import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from signal_forge.api import create_app
from signal_forge.models import BatchRequest
from signal_forge.storage import Store


@pytest.fixture
def payload():
    return {
        "readings": [
            {
                "event_id": "evt-1",
                "sensor_id": "kiln-1",
                "observed_at": "2026-10-22T10:00:00Z",
                "kind": "temperature",
                "value": 176,
                "unit": "F",
            }
        ]
    }


@pytest.fixture
def client(tmp_path):
    with TestClient(create_app(tmp_path / "batches.sqlite3")) as client:
        yield client


def submit(client, payload, key="batch-1"):
    return client.post("/v1/batches", json=payload, headers={"Idempotency-Key": key})


def test_health_and_api_schema(client):
    assert client.get("/health").json() == {"status": "ok"}
    schema = client.get("/openapi.json")
    assert schema.status_code == 200
    assert "/v1/batches/{batch_id}/run" in schema.json()["paths"]
    assert client.get("/docs").status_code == 200


def test_submit_run_and_fetch(client, payload):
    response = submit(client, payload)
    assert response.status_code == 202
    view = response.json()
    assert view["status"] == "pending"
    assert view["reading_count"] == 1
    assert view["completed_at"] is None
    location = response.headers["Location"]
    assert location == f"/v1/batches/{view['id']}"
    assert client.get(location).json() == view
    assert client.get(f"{location}/report").status_code == 409

    result = client.post(f"{location}/run")
    assert result.status_code == 200
    report = result.json()
    assert report["pipeline_version"] == "1"
    assert report["readings"][0]["value"] == 80
    assert report["severity_counts"] == {"normal": 0, "warning": 0, "critical": 1}
    assert client.get(f"{location}/report").json() == report
    completed = client.get(location).json()
    assert completed["status"] == "completed"
    assert completed["completed_at"] is not None
    assert completed["created_at"] == view["created_at"]


def test_identical_retry_returns_existing_batch(client, payload):
    first = submit(client, payload)
    replay = submit(client, payload)
    assert first.status_code == 202
    assert replay.status_code == 200
    assert replay.json() == first.json()
    assert replay.headers["Location"] == first.headers["Location"]


def test_changed_payload_conflicts_without_changing_original(client, payload):
    first = submit(client, payload)
    payload["readings"][0]["value"] = 32
    conflict = submit(client, payload)
    assert conflict.status_code == 409
    assert "different payload" in conflict.json()["detail"]
    report = client.post(first.headers["Location"] + "/run").json()
    assert report["readings"][0]["value"] == 80


def test_new_key_creates_independent_batch(client, payload):
    first = submit(client, payload, "first").json()
    second = submit(client, payload, "second").json()
    assert first["id"] != second["id"]


def test_canonical_request_ignores_json_key_order_and_timezone_offset(client, payload):
    first = submit(client, payload)
    reading = payload["readings"][0]
    reading["observed_at"] = "2026-10-22T12:00:00+02:00"
    reading["value"] = 176.0
    payload["readings"][0] = dict(reversed(list(reading.items())))
    replay = submit(client, payload)
    assert replay.status_code == 200
    assert replay.json()["id"] == first.json()["id"]


def test_completed_batch_is_not_reprocessed(client, payload, monkeypatch):
    location = submit(client, payload).headers["Location"]
    first = client.post(location + "/run").json()
    completed = client.get(location).json()

    def unexpected_run(batch):
        pytest.fail("A completed batch must return its stored report")

    monkeypatch.setattr("signal_forge.storage.run_pipeline", unexpected_run)
    assert client.post(location + "/run").json() == first
    assert client.get(location).json() == completed
    assert submit(client, payload).json() == completed


@pytest.mark.parametrize("suffix", ["", "/report", "/run"])
def test_unknown_batch_returns_404(client, suffix):
    url = f"/v1/batches/{uuid4()}{suffix}"
    response = client.post(url) if suffix == "/run" else client.get(url)
    assert response.status_code == 404
    assert response.json() == {"detail": "Batch not found"}


def test_invalid_batch_identifier_returns_422(client):
    assert client.get("/v1/batches/not-a-uuid").status_code == 422


@pytest.mark.parametrize("key", [None, "", "contains spaces", "x" * 129, "key/with/slash"])
def test_invalid_idempotency_key_returns_422(client, payload, key):
    headers = {} if key is None else {"Idempotency-Key": key}
    response = client.post("/v1/batches", json=payload, headers=headers)
    assert response.status_code == 422


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("observed_at", "2026-10-22T10:00:00"),
        ("observed_at", "yesterday"),
        ("observed_at", "0001-01-01T00:00:00+01:00"),
        ("observed_at", "9999-12-31T23:59:59-01:00"),
        ("kind", "pressure"),
        ("value", "NaN"),
        ("value", "Infinity"),
        ("value", "not-a-number"),
        ("value", True),
        ("value", False),
        ("event_id", ""),
        ("sensor_id", "space not allowed"),
        ("unit", ""),
        ("extra", "unexpected"),
    ],
)
def test_malformed_reading_returns_422(client, payload, field, value):
    payload["readings"][0][field] = value
    assert submit(client, payload).status_code == 422


@pytest.mark.parametrize("literal", ["1e309", "-1e309", "NaN", "Infinity", "-Infinity"])
def test_raw_non_finite_value_returns_serializable_validation_error(client, payload, literal):
    body = json.dumps(payload, separators=(",", ":")).replace('"value":176', f'"value":{literal}')
    response = client.post(
        "/v1/batches",
        content=body,
        headers={"Content-Type": "application/json", "Idempotency-Key": "batch-1"},
    )
    assert response.status_code == 422
    errors = response.json()["detail"]
    assert errors[0]["loc"] == ["body", "readings", 0, "value"]
    assert errors[0]["type"] == "finite_number"
    assert set(errors[0]) == {"loc", "msg", "type"}
    assert submit(client, payload).status_code == 202


@pytest.mark.parametrize("literal", ["-0", "-0.0", "0", "0.0", '"-0"', '"-0.0"', '"0"'])
def test_zero_notation_does_not_change_idempotency(client, payload, literal):
    payload["readings"][0]["value"] = 0
    first = submit(client, payload)
    body = json.dumps(payload, separators=(",", ":")).replace('"value":0', f'"value":{literal}')
    replay = client.post(
        "/v1/batches",
        content=body,
        headers={"Content-Type": "application/json", "Idempotency-Key": "batch-1"},
    )
    assert replay.status_code == 200
    assert replay.json() == first.json()


@pytest.mark.parametrize("count", [0, 501])
def test_batch_size_limits(client, payload, count):
    payload["readings"] *= count
    assert submit(client, payload).status_code == 422


def test_largest_batch_is_accepted(client, payload):
    payload["readings"] = [
        dict(payload["readings"][0], event_id=f"evt-{index}") for index in range(500)
    ]
    response = submit(client, payload)
    assert response.status_code == 202
    report = client.post(response.headers["Location"] + "/run").json()
    assert report["accepted"] == 500
    assert report["quarantined"] == 0


def test_invalid_domain_value_is_quarantined_not_rejected(client, payload):
    payload["readings"][0]["unit"] = "Kelvin"
    response = submit(client, payload)
    assert response.status_code == 202
    report = client.post(response.headers["Location"] + "/run").json()
    assert report["accepted"] == 0
    assert report["quarantined"] == 1
    assert "Unsupported" in report["quarantine"][0]["reason"]


def test_example_batch(client):
    example = Path(__file__).resolve().parents[1] / "examples" / "batch.json"
    response = submit(client, json.loads(example.read_text()))
    assert response.status_code == 202
    report = client.post(response.headers["Location"] + "/run").json()
    assert report["total"] == 5
    assert report["accepted"] == 3
    assert report["quarantined"] == 2
    assert report["severity_counts"] == {"normal": 1, "warning": 1, "critical": 1}


def test_batches_and_reports_survive_app_restart(tmp_path, payload):
    database = tmp_path / "persisted.sqlite3"
    with TestClient(create_app(database)) as first:
        pending_location = submit(first, payload, "pending").headers["Location"]
        completed_location = submit(first, payload, "completed").headers["Location"]
        report = first.post(completed_location + "/run").json()
        completed = first.get(completed_location).json()
    with TestClient(create_app(database)) as second:
        assert second.get(pending_location).json()["status"] == "pending"
        assert second.get(completed_location).json() == completed
        assert second.get(completed_location + "/report").json() == report
        assert second.post(completed_location + "/run").json() == report
        assert submit(second, payload, "completed").json() == completed
        assert second.post(pending_location + "/run").json() == report


def test_failed_run_rolls_back_and_can_be_retried(tmp_path, payload, monkeypatch):
    with TestClient(
        create_app(tmp_path / "failure.sqlite3"), raise_server_exceptions=False
    ) as client:
        location = submit(client, payload).headers["Location"]

        def fail_pipeline(batch):
            raise RuntimeError("Synthetic pipeline failure")

        with monkeypatch.context() as patch:
            patch.setattr("signal_forge.storage.run_pipeline", fail_pipeline)
            assert client.post(location + "/run").status_code == 500
        assert client.get(location).json()["status"] == "pending"
        assert client.get(location + "/report").status_code == 409
        assert client.post(location + "/run").status_code == 200


def test_concurrent_submissions_create_one_batch(tmp_path, payload):
    store = Store(tmp_path / "concurrent.sqlite3")
    store.initialize()
    batch = BatchRequest.model_validate(payload)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: store.submit(batch, "shared-key"), range(24)))
    assert sum(created for _, created in results) == 1
    assert len({view.id for view, _ in results}) == 1


def test_concurrent_runs_process_once(tmp_path, payload, monkeypatch):
    store = Store(tmp_path / "concurrent.sqlite3")
    store.initialize()
    view, _ = store.submit(BatchRequest.model_validate(payload), "key")
    from signal_forge.pipeline import run_pipeline

    calls = []

    def track_pipeline(batch):
        calls.append(batch)
        return run_pipeline(batch)

    monkeypatch.setattr("signal_forge.storage.run_pipeline", track_pipeline)
    with ThreadPoolExecutor(max_workers=8) as pool:
        reports = list(pool.map(lambda _: store.run(view.id), range(16)))
    assert len(calls) == 1
    assert all(report == reports[0] for report in reports)


def test_database_busy_returns_retryable_error(client, payload, monkeypatch):
    def busy(self, batch, key):
        error = sqlite3.OperationalError("database is locked")
        error.sqlite_errorcode = sqlite3.SQLITE_BUSY
        raise error

    monkeypatch.setattr(Store, "submit", busy)
    response = submit(client, payload)
    assert response.status_code == 503
    assert response.headers["Retry-After"] == "1"


def test_database_configuration_from_environment(tmp_path, monkeypatch):
    path = tmp_path / "nested" / "configured.sqlite3"
    monkeypatch.setenv("SIGNAL_FORGE_DB", str(path))
    with TestClient(create_app()) as client:
        assert client.get("/health").status_code == 200
    assert path.is_file()
