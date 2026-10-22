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
