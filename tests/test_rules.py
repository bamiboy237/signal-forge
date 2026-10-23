"""Acceptance tests for the versioned alert rules engine (see AGENT_TASK.md)."""

import pytest
from fastapi.testclient import TestClient

from signal_forge.api import create_app

DEFAULT_RULE_IDS = [
    "temp-critical",
    "temp-warning",
    "temp-normal",
    "humidity-low",
    "humidity-high",
    "humidity-normal",
]


@pytest.fixture
def database(tmp_path):
    return tmp_path / "rules.sqlite3"


@pytest.fixture
def client(database):
    with TestClient(create_app(database)) as client:
        yield client


def reading(event_id, value, *, sensor="kiln-1", kind="temperature", unit="C", at="10:00"):
    return {
        "event_id": event_id,
        "sensor_id": sensor,
        "observed_at": f"2026-10-23T{at}:00Z",
        "kind": kind,
        "value": value,
        "unit": unit,
    }


def run(client, readings, key):
    submitted = client.post(
        "/v1/batches", json={"readings": readings}, headers={"Idempotency-Key": key}
    )
    assert submitted.status_code == 202, submitted.text
    batch_id = submitted.json()["id"]
    result = client.post(f"/v1/batches/{batch_id}/run")
    assert result.status_code == 200, result.text
    return batch_id, result.json()


def by_event(report):
    return {line["event_id"]: line for line in report["readings"]}


def rule(rule_id, when, severity="warning", reason="Matched."):
    return {"id": rule_id, "when": when, "severity": severity, "reason": reason}


def create(client, rules, note=None):
    body = {"rules": rules} if note is None else {"rules": rules, "note": note}
    return client.post("/v1/rules", json=body)


def create_and_activate(client, rules):
    created = create(client, rules)
    assert created.status_code == 201, created.text
    version = created.json()["version"]
    assert client.post(f"/v1/rules/{version}/activate").status_code == 200
    return version


def test_version_one_is_the_active_default(client):
    active = client.get("/v1/rules/active")
    assert active.status_code == 200
    body = active.json()
    assert body["version"] == 1
    assert body["active"] is True
    assert body["note"] is None
    assert [r["id"] for r in body["rules"]] == DEFAULT_RULE_IDS
    assert body["rules"][0] == {
        "id": "temp-critical",
        "when": 'kind == "temperature" and value >= 80',
        "severity": "critical",
        "reason": "Temperature is at or above 80 C.",
    }
    listed = client.get("/v1/rules").json()
    assert [v["version"] for v in listed] == [1]
    assert client.get("/v1/rules/1").json() == body


def test_default_rules_keep_current_results(client):
    _, report = run(
        client,
        [
            reading("t-crit", 176, unit="F"),
            reading("t-warn", 65),
            reading("t-norm", 20),
            reading("h-low", 10, kind="humidity", unit="%", sensor="gh-1"),
            reading("h-high", 90, kind="humidity", unit="%", sensor="gh-1"),
            reading("h-norm", 50, kind="humidity", unit="%", sensor="gh-1"),
        ],
        "defaults",
    )
    assert report["pipeline_version"] == "1"
    assert report["ruleset_version"] == 1
    lines = by_event(report)
    assert lines["t-crit"]["severity"] == "critical"
    assert lines["t-crit"]["reason"] == "Temperature is at or above 80 C."
    assert lines["t-warn"]["reason"] == "Temperature is at or above 60 C and below 80 C."
    assert lines["t-norm"]["reason"] == "Temperature is below 60 C."
    assert lines["h-low"]["reason"] == "Humidity is below 20 %."
    assert lines["h-high"]["reason"] == "Humidity is above 80 %."
    assert lines["h-norm"]["reason"] == "Humidity is between 20 % and 80 %, inclusive."
    assert report["severity_counts"] == {"normal": 2, "warning": 3, "critical": 1}


def test_new_version_is_inactive_until_activated(client):
    created = create(client, [rule("hot", "value > 90", "critical", "Too hot.")], note="Tighter")
    assert created.status_code == 201
    assert created.headers["Location"] == "/v1/rules/2"
    body = created.json()
    assert body["version"] == 2
    assert body["active"] is False
    assert body["note"] == "Tighter"
    assert body["created_at"]
    assert client.get("/v1/rules/active").json()["version"] == 1

    activated = client.post("/v1/rules/2/activate")
    assert activated.status_code == 200
    assert activated.json()["active"] is True
    assert client.get("/v1/rules/active").json()["version"] == 2
    assert client.get("/v1/rules/1").json()["active"] is False
    assert [v["active"] for v in client.get("/v1/rules").json()] == [False, True]


def test_unknown_version_is_404(client):
    for response in (
        client.get("/v1/rules/9"),
        client.post("/v1/rules/9/activate"),
        client.post("/v1/rules/9/replay", json={}),
    ):
        assert response.status_code == 404
        assert response.json() == {"detail": "Rule set not found"}


def test_run_uses_the_active_version_and_reruns_keep_their_report(client):
    first_id, first = run(client, [reading("h-1", 50, kind="humidity", unit="%")], "first")
    assert first["ruleset_version"] == 1
    assert by_event(first)["h-1"]["severity"] == "normal"

    create_and_activate(client, [rule("all-humidity", 'kind == "humidity"', "critical")])
    rerun = client.post(f"/v1/batches/{first_id}/run").json()
    assert rerun == first

    _, second = run(client, [reading("h-2", 50, kind="humidity", unit="%")], "second")
    assert second["ruleset_version"] == 2
    assert by_event(second)["h-2"]["severity"] == "critical"
    assert second["severity_counts"] == {"normal": 0, "warning": 0, "critical": 1}


def test_first_matching_rule_wins_and_no_match_is_normal(client):
    create_and_activate(
        client,
        [
            rule("high", "value > 50", "warning", "High."),
            rule("mid", "value > 10", "critical", "Mid."),
        ],
    )
    _, report = run(client, [reading("a", 60), reading("b", 20), reading("c", 5)], "first-match")
    lines = by_event(report)
    assert (lines["a"]["severity"], lines["a"]["reason"]) == ("warning", "High.")
    assert (lines["b"]["severity"], lines["b"]["reason"]) == ("critical", "Mid.")
    assert (lines["c"]["severity"], lines["c"]["reason"]) == ("normal", "No rule matched.")


def test_boolean_operators_follow_precedence(client):
    create_and_activate(
        client,
        [
            rule("not-and", 'not kind == "humidity" and (value > 90 or sensor_id == "kiln-2")'),
            rule(
                "or-and",
                'kind == "humidity" or sensor_id == "nope" and value > 1000',
                "critical",
            ),
        ],
    )
    _, report = run(
        client,
        [
            reading("hot", 95, sensor="kiln-1"),
            reading("kiln-2-cool", 10, sensor="kiln-2"),
            reading("kiln-1-cool", 10, sensor="kiln-1"),
            reading("humid", 50, sensor="kiln-2", kind="humidity", unit="%"),
        ],
        "precedence",
    )
    lines = by_event(report)
    assert lines["hot"]["severity"] == "warning"
    assert lines["kiln-2-cool"]["severity"] == "warning"
    assert lines["kiln-1-cool"]["severity"] == "normal"
    assert lines["humid"]["severity"] == "critical"


def test_matches_is_a_whole_string_glob(client):
    create_and_activate(
        client,
        [
            rule("kilns", 'sensor_id matches "kiln-*"', "critical"),
            rule("labs", 'sensor_id matches "lab-?"', "warning"),
        ],
    )
    _, report = run(
        client,
        [
            reading("k12", 20, sensor="kiln-12"),
            reading("k-bare", 20, sensor="kiln-"),
            reading("l1", 20, sensor="lab-1"),
            reading("l12", 20, sensor="lab-12"),
            reading("xkiln", 20, sensor="xkiln-1"),
        ],
        "glob",
    )
    lines = by_event(report)
    assert lines["k12"]["severity"] == "critical"
    assert lines["k-bare"]["severity"] == "critical"
    assert lines["l1"]["severity"] == "warning"
    assert lines["l12"]["severity"] == "normal"
    assert lines["xkiln"]["severity"] == "normal"


def test_avg_uses_observation_order_per_sensor_and_kind(client):
    create_and_activate(client, [rule("rolling", "avg(value, 3) >= 30", "critical")])
    _, report = run(
        client,
        [
            reading("t3", 30, at="10:03"),
            reading("t1", 10, at="10:01"),
            reading("t4", 40, at="10:04"),
            reading("t2", 20, at="10:02"),
            reading("other-sensor", 100, sensor="kiln-2", at="10:05"),
            reading("other-kind", 100, kind="humidity", unit="%", at="10:05"),
        ],
        "avg",
    )
    lines = by_event(report)
    assert [line["event_id"] for line in report["readings"]][:4] == ["t3", "t1", "t4", "t2"]
    assert lines["t1"]["severity"] == "normal"
    assert lines["t2"]["severity"] == "normal"
    assert lines["t3"]["severity"] == "normal"
    assert lines["t4"]["severity"] == "critical"
    assert lines["other-sensor"]["severity"] == "normal"
    assert lines["other-kind"]["severity"] == "normal"


def test_quarantined_readings_do_not_count_in_windows(client):
    create_and_activate(client, [rule("jump", "delta(value) > 15", "warning")])
    _, report = run(
        client,
        [
            reading("h1", 40, kind="humidity", unit="%", sensor="gh-1", at="10:01"),
            reading("bad", 500, kind="humidity", unit="%", sensor="gh-1", at="10:02"),
            reading("h2", 60, kind="humidity", unit="%", sensor="gh-1", at="10:03"),
            reading("h3", 62, kind="humidity", unit="%", sensor="gh-1", at="10:04"),
        ],
        "delta",
    )
    assert report["quarantined"] == 1
    lines = by_event(report)
    assert lines["h1"]["severity"] == "normal"
    assert lines["h2"]["severity"] == "warning"
    assert lines["h3"]["severity"] == "normal"


def test_comparisons_with_null_are_false(client):
    create_and_activate(
        client,
        [
            rule("changed", "delta(value) != 0", "critical"),
            rule("first", "not delta(value) > 1", "warning"),
        ],
    )
    _, report = run(client, [reading("only", 20)], "null")
    assert by_event(report)["only"]["severity"] == "warning"


def test_converted_values_and_reason_placeholders(client):
    create_and_activate(
        client,
        [rule("hot", "value >= 80", "critical", "{sensor_id} reads {value} {unit}.")],
    )
    _, report = run(
        client,
        [reading("f", 176, unit="F"), reading("c", 81.5, sensor="kiln-9")],
        "placeholders",
    )
    lines = by_event(report)
    assert lines["f"]["reason"] == "kiln-1 reads 80 C."
    assert lines["c"]["reason"] == "kiln-9 reads 81.5 C."


def test_string_literals_support_escapes(client):
    create_and_activate(client, [rule("odd", 'sensor_id == "a\\"b" or unit == "%"', "critical")])
    _, report = run(client, [reading("h", 50, kind="humidity", unit="%")], "escapes")
    assert by_event(report)["h"]["severity"] == "critical"


def test_negative_numbers(client):
    create_and_activate(client, [rule("cold", "value <= -10.5", "warning")])
    _, report = run(client, [reading("cold", -11), reading("mild", -10)], "negative")
    lines = by_event(report)
    assert lines["cold"]["severity"] == "warning"
    assert lines["mild"]["severity"] == "normal"


def error_for(response):
    assert response.status_code == 422, response.text
    return response.json()["detail"][0]


@pytest.mark.parametrize(
    "when",
    [
        "value >=",
        "temp > 3",
        "sensor_id > 5",
        'value matches "a*"',
        "sensor_id matches kind",
        "kind == 3",
        "value",
        "avg(value, 0) > 1",
        "avg(value, 21) > 1",
        "avg(value, 2.5) > 1",
        "max(value) > 1",
        '__import__("os") == 1',
        "value > 1 and",
        "(value > 1",
        "value > 1)",
        'kind == "unterminated',
        "Value > 1",
    ],
)
def test_invalid_expressions_are_rejected_on_the_when_field(client, when):
    response = create(client, [rule("ok", "value > 1"), rule("bad", when)])
    error = error_for(response)
    assert error["loc"] == ["body", "rules", 1, "when"]
    assert client.get("/v1/rules").json()[-1]["version"] == 1


def test_syntax_errors_report_the_position(client):
    response = create(client, [rule("bad", 'kind == "temperature" and and value > 1')])
    error = error_for(response)
    assert "at position 27" in error["msg"]


def test_unknown_reason_placeholder_is_rejected(client):
    error = error_for(create(client, [rule("bad", "value > 1", reason="At {site}.")]))
    assert error["loc"] == ["body", "rules", 0, "reason"]


def test_duplicate_rule_ids_are_rejected(client):
    error = error_for(create(client, [rule("same", "value > 1"), rule("same", "value > 2")]))
    assert error["loc"] == ["body", "rules"]


def test_rule_set_size_and_fields_are_bounded(client):
    assert create(client, []).status_code == 422
    assert create(client, [rule(f"r{i}", "value > 1") for i in range(51)]).status_code == 422
    extra = rule("x", "value > 1") | {"priority": 1}
    assert create(client, [extra]).status_code == 422
    assert create(client, [rule("x", "value > 1", severity="severe")]).status_code == 422
    assert create(client, [rule("x", "v" * 501)]).status_code == 422
    assert create(client, [rule("x", "value > 1")], note="n" * 201).status_code == 422


def test_replay_reports_changes_without_changing_anything(client):
    first_id, first = run(
        client,
        [
            reading("a", 50, kind="humidity", unit="%", sensor="gh-1"),
            reading("b", 25, kind="humidity", unit="%", sensor="gh-1"),
        ],
        "replay-1",
    )
    second_id, second = run(client, [reading("c", 70), reading("d", 20)], "replay-2")
    pending = client.post(
        "/v1/batches", json={"readings": [reading("p", 99)]}, headers={"Idempotency-Key": "p"}
    )
    assert pending.status_code == 202
    delivery_before = client.get(f"/v1/batches/{second_id}/delivery").json()

    created = create(
        client,
        [
            rule("dry", 'kind == "humidity" and value < 30', "critical"),
            rule("warm", "value >= 60", "warning"),
        ],
    )
    version = created.json()["version"]
    replay = client.post(f"/v1/rules/{version}/replay", json={"limit": 10})
    assert replay.status_code == 200
    body = replay.json()
    assert body["version"] == version
    assert [b["batch_id"] for b in body["batches"]] == [second_id, first_id]

    newest, oldest = body["batches"]
    assert newest["ruleset_version"] == 1
    assert newest["before"] == {"normal": 1, "warning": 1, "critical": 0}
    assert newest["after"] == {"normal": 1, "warning": 1, "critical": 0}
    assert newest["changes"] == []
    assert oldest["before"] == {"normal": 2, "warning": 0, "critical": 0}
    assert oldest["after"] == {"normal": 1, "warning": 0, "critical": 1}
    assert oldest["changes"] == [{"event_id": "b", "before": "normal", "after": "critical"}]

    assert client.get(f"/v1/batches/{first_id}/report").json() == first
    assert client.get(f"/v1/batches/{second_id}/report").json() == second
    assert client.get(f"/v1/batches/{second_id}/delivery").json() == delivery_before
    assert client.get("/v1/rules/active").json()["version"] == 1


def test_replay_limit(client):
    ids = [run(client, [reading(f"e{i}", 20)], f"limit-{i}")[0] for i in range(3)]
    replay = client.post("/v1/rules/1/replay", json={"limit": 2}).json()
    assert [b["batch_id"] for b in replay["batches"]] == [ids[2], ids[1]]
    assert len(client.post("/v1/rules/1/replay", json={}).json()["batches"]) == 3
    for bad in (0, 101, "5", 2.0):
        response = client.post("/v1/rules/1/replay", json={"limit": bad})
        assert response.status_code == 422, bad


def test_rule_sets_survive_a_restart(database):
    with TestClient(create_app(database)) as client:
        create_and_activate(client, [rule("hot", "value > 90", "critical")])
    with TestClient(create_app(database)) as client:
        assert client.get("/v1/rules/active").json()["version"] == 2
        assert [v["version"] for v in client.get("/v1/rules").json()] == [1, 2]


def test_rule_routes_are_documented(client):
    paths = client.get("/openapi.json").json()["paths"]
    for path in (
        "/v1/rules",
        "/v1/rules/active",
        "/v1/rules/{version}",
        "/v1/rules/{version}/activate",
        "/v1/rules/{version}/replay",
    ):
        assert path in paths
