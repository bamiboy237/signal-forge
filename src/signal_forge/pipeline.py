from signal_forge.models import (
    BatchRequest,
    PipelineReport,
    QuarantinedReading,
    ScoredReading,
    Severity,
)

PIPELINE_VERSION = "1"


def run_pipeline(batch: BatchRequest) -> PipelineReport:
    """Normalize and score readings without changing the batch or using external state."""
    seen_event_ids: set[str] = set()
    readings: list[ScoredReading] = []
    quarantine: list[QuarantinedReading] = []
    severity_counts: dict[Severity, int] = {"normal": 0, "warning": 0, "critical": 0}

    for index, reading in enumerate(batch.readings):
        if reading.event_id in seen_event_ids:
            quarantine.append(
                QuarantinedReading(
                    index=index,
                    event_id=reading.event_id,
                    reason="Duplicate event_id; the first occurrence in the batch wins.",
                )
            )
            continue
        seen_event_ids.add(reading.event_id)

        unit = reading.unit.strip().upper()
        value = reading.value
        invalid_reason = None
        if reading.kind == "temperature":
            if unit not in {"C", "F"}:
                invalid_reason = "Unsupported temperature unit; expected C or F."
            else:
                if unit == "F":
                    value = (value - 32) * 5 / 9
                value = round(value, 6)
                unit = "C"
                if not -100 <= value <= 150:
                    invalid_reason = "Temperature outside the allowed range [-100, 150] C."
        else:
            if unit != "%":
                invalid_reason = "Unsupported humidity unit; expected %."
            elif not 0 <= value <= 100:
                invalid_reason = "Humidity outside the allowed range [0, 100] %."

        if invalid_reason is not None:
            quarantine.append(
                QuarantinedReading(index=index, event_id=reading.event_id, reason=invalid_reason)
            )
            continue

        severity: Severity
        if reading.kind == "temperature":
            if value >= 80:
                severity = "critical"
                reason = "Temperature is at or above 80 C."
            elif value >= 60:
                severity = "warning"
                reason = "Temperature is at or above 60 C and below 80 C."
            else:
                severity = "normal"
                reason = "Temperature is below 60 C."
        elif value < 20:
            severity = "warning"
            reason = "Humidity is below 20 %."
        elif value > 80:
            severity = "warning"
            reason = "Humidity is above 80 %."
        else:
            severity = "normal"
            reason = "Humidity is between 20 % and 80 %, inclusive."

        readings.append(
            ScoredReading(
                event_id=reading.event_id,
                sensor_id=reading.sensor_id,
                observed_at=reading.observed_at,
                kind=reading.kind,
                value=value,
                unit=unit,
                severity=severity,
                reason=reason,
            )
        )
        severity_counts[severity] += 1

    return PipelineReport(
        pipeline_version=PIPELINE_VERSION,
        total=len(batch.readings),
        accepted=len(readings),
        quarantined=len(quarantine),
        severity_counts=severity_counts,
        readings=readings,
        quarantine=quarantine,
    )
