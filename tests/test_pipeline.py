from datetime import UTC, datetime
from typing import Literal

import pytest

from signal_forge.models import BatchRequest, Reading
from signal_forge.pipeline import PIPELINE_VERSION, run_pipeline


def make_reading(
    event_id: str = "event-1",
    *,
    kind: Literal["temperature", "humidity"] = "temperature",
    value: float = 25,
    unit: str = "C",
) -> Reading:
    return Reading(
        event_id=event_id,
        sensor_id="sensor-1",
        observed_at=datetime(2026, 1, 1, tzinfo=UTC),
        kind=kind,
        value=value,
        unit=unit,
    )


@pytest.mark.parametrize(
    ("fahrenheit", "celsius", "severity"),
    [
        (32, 0, "normal"),
        (98.6, 37, "normal"),
        (100, 37.777778, "normal"),
        (140, 60, "warning"),
        (176, 80, "critical"),
        (-148, -100, "normal"),
        (302, 150, "critical"),
    ],
)
def test_fahrenheit_conversion(fahrenheit, celsius, severity):
    original = make_reading(value=fahrenheit, unit="F")
    report = run_pipeline(BatchRequest(readings=[original]))

    assert report.accepted == 1
    assert report.quarantine == []
    scored = report.readings[0]
    assert scored.value == celsius
    assert scored.unit == "C"
    assert scored.severity == severity
    assert scored.event_id == original.event_id
    assert scored.sensor_id == original.sensor_id
    assert scored.observed_at == original.observed_at
    assert scored.kind == original.kind


@pytest.mark.parametrize(
    ("value", "severity", "reason"),
    [
        (-100, "normal", "Temperature is below 60 C."),
        (59.999999, "normal", "Temperature is below 60 C."),
        (60, "warning", "Temperature is at or above 60 C and below 80 C."),
        (79.999999, "warning", "Temperature is at or above 60 C and below 80 C."),
        (80, "critical", "Temperature is at or above 80 C."),
        (150, "critical", "Temperature is at or above 80 C."),
    ],
)
def test_temperature_boundaries_and_score_reasons(value, severity, reason):
    report = run_pipeline(BatchRequest(readings=[make_reading(value=value)]))

    assert report.accepted == 1
    assert report.readings[0].value == value
    assert report.readings[0].severity == severity
    assert report.readings[0].reason == reason


@pytest.mark.parametrize(
    ("value", "severity", "reason"),
    [
        (0, "warning", "Humidity is below 20 %."),
        (19.999999, "warning", "Humidity is below 20 %."),
        (20, "normal", "Humidity is between 20 % and 80 %, inclusive."),
        (80, "normal", "Humidity is between 20 % and 80 %, inclusive."),
        (80.000001, "warning", "Humidity is above 80 %."),
        (100, "warning", "Humidity is above 80 %."),
    ],
)
def test_humidity_boundaries_and_score_reasons(value, severity, reason):
    report = run_pipeline(
        BatchRequest(readings=[make_reading(kind="humidity", value=value, unit="%")])
    )

    assert report.accepted == 1
    assert report.readings[0].value == value
    assert report.readings[0].unit == "%"
    assert report.readings[0].severity == severity
    assert report.readings[0].reason == reason


@pytest.mark.parametrize(
    ("kind", "value", "unit", "expected_value", "expected_unit"),
    [
        ("temperature", 25.123456789, " c ", 25.123457, "C"),
        ("temperature", 68, "\t f\n", 20, "C"),
        ("humidity", 50.123456789, " % ", 50.123456789, "%"),
    ],
)
def test_unit_normalization(kind, value, unit, expected_value, expected_unit):
    report = run_pipeline(BatchRequest(readings=[make_reading(kind=kind, value=value, unit=unit)]))

    assert report.accepted == 1
    assert report.readings[0].value == expected_value
    assert report.readings[0].unit == expected_unit


@pytest.mark.parametrize(
    ("kind", "value", "unit", "reason_fragment"),
    [
        ("temperature", -100.000001, "C", "[-100, 150] C"),
        ("temperature", 150.000001, "C", "[-100, 150] C"),
        ("temperature", -149, "F", "[-100, 150] C"),
        ("temperature", 303, "F", "[-100, 150] C"),
        ("temperature", 25, "K", "expected C or F"),
        ("temperature", 25, "   ", "expected C or F"),
        ("humidity", -0.000001, "%", "[0, 100] %"),
        ("humidity", 100.000001, "%", "[0, 100] %"),
        ("humidity", 50, "C", "expected %"),
        ("humidity", 50, "percent", "expected %"),
    ],
)
def test_invalid_domain_data_is_quarantined(kind, value, unit, reason_fragment):
    report = run_pipeline(BatchRequest(readings=[make_reading(kind=kind, value=value, unit=unit)]))

    assert report.readings == []
    assert report.accepted == 0
    assert report.quarantined == 1
    assert report.quarantine[0].index == 0
    assert report.quarantine[0].event_id == "event-1"
    assert reason_fragment in report.quarantine[0].reason


def test_duplicates_preserve_first_occurrence_even_when_invalid():
    batch = BatchRequest(
        readings=[
            make_reading("invalid-unit-first", unit="K"),
            make_reading("valid-first", value=60),
            make_reading("invalid-unit-first", value=80),
            make_reading("valid-first", value=80),
            make_reading("invalid-range-first", value=151),
            make_reading("invalid-range-first", value=25),
            make_reading("last", kind="humidity", unit="%", value=50),
            make_reading("valid-first", unit="K"),
        ]
    )

    report = run_pipeline(batch)

    assert [reading.event_id for reading in report.readings] == ["valid-first", "last"]
    assert report.readings[0].value == 60
    assert [item.index for item in report.quarantine] == [0, 2, 3, 4, 5, 7]
    assert [item.event_id for item in report.quarantine] == [
        "invalid-unit-first",
        "invalid-unit-first",
        "valid-first",
        "invalid-range-first",
        "invalid-range-first",
        "valid-first",
    ]
    assert "Unsupported" in report.quarantine[0].reason
    assert "outside" in report.quarantine[3].reason
    for position in (1, 2, 4, 5):
        assert "Duplicate event_id" in report.quarantine[position].reason
    assert report.total == 8
    assert report.accepted == 2
    assert report.quarantined == 6
    assert report.severity_counts == {"normal": 1, "warning": 1, "critical": 0}


def test_report_counts_and_order():
    batch = BatchRequest(
        readings=[
            make_reading("critical", value=80),
            make_reading("invalid", value=200),
            make_reading("normal", value=25),
            make_reading("warning", kind="humidity", unit="%", value=0),
        ]
    )

    report = run_pipeline(batch)

    assert PIPELINE_VERSION == "1"
    assert report.pipeline_version == PIPELINE_VERSION
    assert report.total == 4
    assert report.accepted == 3
    assert report.quarantined == 1
    assert report.total == report.accepted + report.quarantined
    assert report.severity_counts == {"normal": 1, "warning": 1, "critical": 1}
    assert sum(report.severity_counts.values()) == report.accepted
    assert [reading.event_id for reading in report.readings] == ["critical", "normal", "warning"]
    assert report.quarantine[0].index == 1


def test_all_quarantined_report_includes_zero_severity_counts():
    report = run_pipeline(
        BatchRequest(
            readings=[
                make_reading("bad-unit", unit="K"),
                make_reading("bad-range", kind="humidity", unit="%", value=101),
                make_reading("bad-unit"),
            ]
        )
    )

    assert report.pipeline_version == "1"
    assert report.total == 3
    assert report.accepted == 0
    assert report.quarantined == 3
    assert report.readings == []
    assert report.severity_counts == {"normal": 0, "warning": 0, "critical": 0}
    assert [item.index for item in report.quarantine] == [0, 1, 2]


def test_output_is_deterministic_and_input_is_not_mutated():
    batch = BatchRequest(
        readings=[
            make_reading("converted", value=100, unit=" f "),
            make_reading("invalid", unit=" k "),
            make_reading("converted", value=60),
            make_reading("humidity", kind="humidity", unit=" % ", value=50),
        ]
    )
    original = batch.model_copy(deep=True)
    original_readings = tuple(batch.readings)

    first = run_pipeline(batch)
    run_pipeline(BatchRequest(readings=[make_reading("converted", value=80)]))
    second = run_pipeline(batch)

    assert first == second
    assert first.model_dump_json() == second.model_dump_json()
    assert batch == original
    assert all(
        current is previous
        for current, previous in zip(batch.readings, original_readings, strict=True)
    )

    first.readings[0].value = 999
    first.severity_counts["normal"] = 999
    first.quarantine[0].reason = "changed"
    assert batch == original
    assert run_pipeline(batch) == second
