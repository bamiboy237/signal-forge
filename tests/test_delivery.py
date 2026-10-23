import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Event
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient

from signal_forge.api import create_app
from signal_forge.delivery import dispatch, send_webhook
from signal_forge.models import BatchRequest
from signal_forge.storage import Store


class Clock:
    def __init__(self):
        self.now = datetime(2030, 1, 1, tzinfo=UTC)

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += timedelta(seconds=seconds)


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "delivery.sqlite3")
    store.initialize()
    return store


def batch(store, value=80, unit="C"):
    request = BatchRequest.model_validate(
        {
            "readings": [
                {
                    "event_id": "event",
                    "sensor_id": "sensor",
                    "observed_at": "2026-01-01T00:00:00Z",
                    "kind": "temperature",
                    "value": value,
                    "unit": unit,
                }
            ]
        }
    )
    view, _ = store.submit(request, str(uuid4()))
    return view.id


def queued(store):
    batch_id = batch(store)
    store.run(batch_id)
    return batch_id


@pytest.mark.parametrize(
    ("value", "unit", "expected"),
    [
        (25, "C", False),
        (60, "C", True),
        (80, "C", True),
        (200, "C", False),
        (80, "K", False),
    ],
)
def test_outbox_eligibility_and_unique_concurrent_runs(store, value, unit, expected):
    batch_id = batch(store, value, unit)
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda _: store.run(batch_id), range(16)))
    delivery = store.delivery(batch_id)
    assert (delivery is not None) == expected
    with store.connect() as connection:
        assert connection.execute("SELECT count(*) FROM deliveries").fetchone()[0] == int(expected)
    if delivery:
        assert delivery.payload.delivery_id == delivery.id
        assert delivery.payload.batch_id == batch_id
        assert delivery.payload.pipeline_version == "1"
        assert delivery.payload.severity_counts == store.report(batch_id).severity_counts


def test_pipeline_and_persistence_rollback(store, monkeypatch):
    batch_id = batch(store)
    with monkeypatch.context() as patch:

        def fail(_):
            raise RuntimeError("pipeline failed")

        patch.setattr("signal_forge.storage.run_pipeline", fail)
        with pytest.raises(RuntimeError):
            store.run(batch_id)
    assert store.get(batch_id).status == "pending"
    assert store.delivery(batch_id) is None
    with store.connect() as connection:
        connection.execute("""CREATE TRIGGER fail_outbox BEFORE INSERT ON deliveries
            BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END""")
    with pytest.raises(sqlite3.IntegrityError):
        store.run(batch_id)
    with store.connect() as connection:
        row = connection.execute("SELECT report, completed_at FROM batches").fetchone()
        assert tuple(row) == (None, None)
        connection.execute("DROP TRIGGER fail_outbox")
    assert store.delivery(batch_id) is None
    store.run(batch_id)
    assert store.delivery(batch_id) is not None


@pytest.mark.parametrize("code", [200, 201, 204, 299])
def test_success_survives_restart(store, code):
    batch_id = queued(store)
    original = store.delivery(batch_id)
    reopened = Store(store.path)
    reopened.initialize()
    assert reopened.delivery(batch_id) == original
    assert (
        dispatch(reopened, "https://example.test", 10, lambda *args: code, Clock()).succeeded == 1
    )
    result = reopened.delivery(batch_id)
    assert result.status == "succeeded"
    assert result.attempt_count == 1
    assert result.last_error is None
    assert result.next_attempt_at is None
    assert (
        dispatch(store, "https://example.test", 10, lambda *args: pytest.fail(), Clock()).attempted
        == 0
    )


@pytest.mark.parametrize("code", [None, 429, 500, 503, 599])
def test_retry_schedule_and_stable_payload(store, code):
    batch_id = queued(store)
    clock = Clock()
    sent = []

    def sender(url, payload, key):
        sent.append((payload, key))
        if code is None:
            raise httpx.ConnectError("https://user:secret@example.test/private?token=hidden")
        return code

    for attempt in range(1, 6):
        result = dispatch(store, "https://example.test", 100, sender, clock)
        assert result.attempted == 1
        delivery = store.delivery(batch_id)
        assert delivery.attempt_count == attempt
        if attempt < 5:
            assert result.retried == 1
            delay = 2 ** (attempt - 1)
            assert delivery.next_attempt_at == clock() + timedelta(seconds=delay)
            clock.advance(delay - 0.01)
            assert dispatch(store, "https://example.test", 100, sender, clock).attempted == 0
            clock.advance(0.01)
        else:
            assert result.dead == 1
            assert delivery.status == "dead"
            assert delivery.next_attempt_at is None
    assert len(set(sent)) == 1
    payload, key = sent[0]
    assert json.loads(payload)["delivery_id"] == key == delivery.id
    assert "secret" not in delivery.model_dump_json()
    clock.advance(100)
    assert dispatch(store, "https://example.test", 100, sender, clock).attempted == 0


@pytest.mark.parametrize("code", [100, 300, 301, 302, 307, 308, 400, 401, 404, 422])
def test_terminal_response(store, code):
    batch_id = queued(store)
    result = dispatch(store, "https://example.test", 10, lambda *args: code, Clock())
    assert result.dead == 1
    assert store.delivery(batch_id).last_error == f"HTTP {code}"
    assert store.delivery(batch_id).attempt_count == 1


def test_concurrent_dispatch_and_no_write_lock_during_http(store):
    batch_id = queued(store)
    entered, release = Event(), Event()

    def sender(*args):
        entered.set()
        assert release.wait(5)
        return 204

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(dispatch, store, "https://example.test", 10, sender, Clock())
        try:
            assert entered.wait(5)
            # A write completes while HTTP is blocked; another dispatcher cannot claim this row.
            batch(store)
            result = dispatch(
                store, "https://example.test", 10, lambda *args: pytest.fail(), Clock()
            )
            assert result.attempted == 0
            assert store.delivery(batch_id).status == "in_flight"
        finally:
            release.set()
        assert first.result().succeeded == 1


def test_expired_claim_restart_and_stale_worker(store):
    batch_id = queued(store)
    clock = Clock()
    old = store.claim(clock(), [])
    clock.advance(59)
    assert store.claim(clock(), []) is None
    clock.advance(1)
    restarted = Store(store.path)
    restarted.initialize()
    new = restarted.claim(clock(), [])
    assert new["claim_id"] != old["claim_id"]
    assert store.finish(old, clock(), 200) == "stale"
    assert restarted.finish(new, clock(), 503) == "retried"
    assert store.finish(old, clock(), 400) == "stale"
    assert store.delivery(batch_id).status == "pending"
    assert store.delivery(batch_id).attempt_count == 2


def test_final_abandoned_attempt_is_bounded(store):
    batch_id = queued(store)
    clock = Clock()
    for _ in range(5):
        assert store.claim(clock(), []) is not None
        clock.advance(60)
    assert store.claim(clock(), []) is None
    assert store.delivery(batch_id).status == "dead"
    assert store.delivery(batch_id).attempt_count == 5


@pytest.mark.parametrize(
    "url", [None, "", "http://user:secret@example.test", "garbage", "https://"]
)
def test_configuration_does_not_consume_attempts(store, monkeypatch, url):
    batch_id = queued(store)
    if url is None:
        monkeypatch.delenv("SIGNAL_FORGE_WEBHOOK_URL", raising=False)
    else:
        monkeypatch.setenv("SIGNAL_FORGE_WEBHOOK_URL", url)
    with TestClient(create_app(store.path)) as client:
        response = client.post("/v1/deliveries/dispatch", json={"limit": 10})
        assert response.status_code == 503
        assert "secret" not in response.text
        delivery = client.get(f"/v1/batches/{batch_id}/delivery").json()
        assert delivery["attempt_count"] == 0
        assert delivery["status"] == "pending"
        assert client.get(f"/v1/batches/{uuid4()}/delivery").status_code == 404
        assert client.get("/v1/batches/invalid/delivery").status_code == 422


@pytest.mark.parametrize(
    "body",
    [
        {"limit": 0},
        {"limit": 101},
        {"limit": 1.5},
        {"limit": True},
        {"limit": "1"},
        {"url": "https://x"},
    ],
)
def test_invalid_dispatch_limits(store, body):
    with TestClient(create_app(store.path)) as client:
        assert client.post("/v1/deliveries/dispatch", json=body).status_code == 422


def test_api_bounded_dispatch_and_inspection(store, monkeypatch):
    for _ in range(3):
        queued(store)
    monkeypatch.setenv("SIGNAL_FORGE_WEBHOOK_URL", "https://user:secret@example.test/?token=hidden")
    # Exercise the actual sender using a mock transport, including HTTP settings and headers.
    real_client = httpx.Client
    sent = []

    def handle(request):
        sent.append(request)
        assert request.headers["Idempotency-Key"] == json.loads(request.content)["delivery_id"]
        return httpx.Response(302, headers={"Location": "https://elsewhere.test"})

    def mock_client(**kwargs):
        assert kwargs == {"timeout": 10, "follow_redirects": False, "trust_env": False}
        return real_client(transport=httpx.MockTransport(handle), **kwargs)

    with TestClient(create_app(store.path)) as client:
        monkeypatch.setattr("signal_forge.delivery.httpx.Client", mock_client)
        response = client.post("/v1/deliveries/dispatch", json={"limit": 2})
        assert response.json() == {
            "attempted": 2,
            "succeeded": 0,
            "retried": 0,
            "dead": 2,
            "stale": 0,
        }
        assert len(sent) == 2
        assert "secret" not in response.text
        result = client.post("/v1/deliveries/dispatch", json={}).json()
        assert result["attempted"] == 1
        assert len(sent) == 3
        batch_id = json.loads(sent[0].content)["batch_id"]
        inspection = client.get(f"/v1/batches/{batch_id}/delivery")
        assert inspection.json()["last_error"] == "HTTP 302"
        assert "secret" not in inspection.text
        assert "hidden" not in inspection.text


def test_sender_network_failure_is_sanitized(store, monkeypatch):
    batch_id = queued(store)
    real_client = httpx.Client

    def fail(request):
        raise httpx.ReadTimeout("https://user:secret@example.test/?token=hidden")

    monkeypatch.setattr(
        "signal_forge.delivery.httpx.Client",
        lambda **kwargs: real_client(transport=httpx.MockTransport(fail), **kwargs),
    )
    result = dispatch(store, "https://user:secret@example.test", 1, send_webhook, Clock())
    assert result.retried == 1
    assert store.delivery(batch_id).last_error == "Network failure"
