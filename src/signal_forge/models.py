from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

Identifier = Annotated[str, Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9._:-]+$")]
Severity = Literal["normal", "warning", "critical"]


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Reading(Model):
    event_id: Identifier
    sensor_id: Identifier
    observed_at: AwareDatetime
    kind: Literal["temperature", "humidity"]
    value: float = Field(allow_inf_nan=False)
    unit: str = Field(min_length=1, max_length=16)

    @field_validator("value", mode="before")
    @classmethod
    def reject_boolean(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("A reading value must be a number, not a boolean")
        return value

    @field_validator("value")
    @classmethod
    def normalize_zero(cls, value: float) -> float:
        return 0.0 if value == 0 else value

    @field_validator("observed_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        try:
            return value.astimezone(UTC)
        except OverflowError as exc:
            raise ValueError("Timestamp exceeds the supported datetime range in UTC") from exc


class BatchRequest(Model):
    readings: list[Reading] = Field(min_length=1, max_length=500)


class ScoredReading(Model):
    event_id: str
    sensor_id: str
    observed_at: AwareDatetime
    kind: Literal["temperature", "humidity"]
    value: float
    unit: Literal["C", "%"]
    severity: Severity
    reason: str


class QuarantinedReading(Model):
    index: int
    event_id: str
    reason: str


class PipelineReport(Model):
    pipeline_version: str
    ruleset_version: int = 1
    total: int
    accepted: int
    quarantined: int
    severity_counts: dict[Severity, int]
    readings: list[ScoredReading]
    quarantine: list[QuarantinedReading]


class BatchView(Model):
    id: str
    status: Literal["pending", "completed"]
    created_at: AwareDatetime
    completed_at: AwareDatetime | None
    reading_count: int


class Health(Model):
    status: Literal["ok"]


class DeliveryPayload(Model):
    delivery_id: str
    batch_id: str
    pipeline_version: str
    severity_counts: dict[Severity, int]


class DeliveryView(Model):
    id: str
    batch_id: str
    payload: DeliveryPayload
    status: Literal["pending", "in_flight", "succeeded", "dead"]
    attempt_count: int
    next_attempt_at: AwareDatetime | None
    last_error: str | None


class Rule(Model):
    id: Identifier
    when: str = Field(min_length=1, max_length=500)
    severity: Severity
    reason: str = Field(min_length=1, max_length=200)

    @field_validator("when")
    @classmethod
    def validate_expression(cls, value: str) -> str:
        from .rules import parse_expression

        parse_expression(value)
        return value

    @field_validator("reason")
    @classmethod
    def validate_placeholders(cls, value: str) -> str:
        import re

        for placeholder in re.findall(r"\{([^{}]*)\}", value):
            if placeholder not in {"sensor_id", "unit", "value"}:
                raise ValueError(f"Unknown reason placeholder: {placeholder}")
        return value


class RuleSetRequest(Model):
    rules: list[Rule] = Field(min_length=1, max_length=50)
    note: str | None = Field(default=None, max_length=200)

    @field_validator("rules")
    @classmethod
    def unique_ids(cls, rules: list[Rule]) -> list[Rule]:
        if len({rule.id for rule in rules}) != len(rules):
            raise ValueError("Duplicate rule IDs")
        return rules


class RuleSetView(RuleSetRequest):
    version: int
    created_at: AwareDatetime
    active: bool


class ReplayRequest(Model):
    limit: int = Field(default=20, ge=1, le=100, strict=True)


class DispatchRequest(Model):
    limit: int = Field(default=10, ge=1, le=100, strict=True)


class DispatchResult(Model):
    attempted: int = 0
    succeeded: int = 0
    retried: int = 0
    dead: int = 0
    stale: int = 0


class ReplayChange(Model):
    event_id: str
    before: Severity
    after: Severity


class ReplayBatch(Model):
    batch_id: str
    ruleset_version: int
    before: dict[Severity, int]
    after: dict[Severity, int]
    changes: list[ReplayChange]


class ReplayResult(Model):
    version: int
    batches: list[ReplayBatch]
