from signal_forge.models import (
    BatchRequest,
    PipelineReport,
    QuarantinedReading,
    Rule,
    ScoredReading,
    Severity,
)

from .rules import DEFAULT_RULES, score_rules

PIPELINE_VERSION = "1"


def run_pipeline(
    batch: BatchRequest, rules: list[Rule] | None = None, ruleset_version: int = 1
) -> PipelineReport:
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

        readings.append(
            ScoredReading(
                event_id=reading.event_id,
                sensor_id=reading.sensor_id,
                observed_at=reading.observed_at,
                kind=reading.kind,
                value=value,
                unit=unit,
                severity="normal",
                reason="No rule matched.",
            )
        )

    selected_rules = rules if rules is not None else [Rule.model_validate(r) for r in DEFAULT_RULES]
    history: list[ScoredReading] = []
    # Stable sorting preserves batch position for equal timestamps; output stays in input order.
    for reading in sorted(readings, key=lambda item: item.observed_at):
        history.append(reading)
        reading.severity, reading.reason = score_rules(selected_rules, reading, history)
        severity_counts[reading.severity] += 1

    return PipelineReport(
        pipeline_version=PIPELINE_VERSION,
        ruleset_version=ruleset_version,
        total=len(batch.readings),
        accepted=len(readings),
        quarantined=len(quarantine),
        severity_counts=severity_counts,
        readings=readings,
        quarantine=quarantine,
    )
