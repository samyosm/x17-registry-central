from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, model_validator

from x17_registry.domain.protocol import UINT32_MAX, VF48_FRAME_WORDS, VF48_SLOTS


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


Identifier = Annotated[str, Field(min_length=1, max_length=200, pattern=r"^\S(?:.*\S)?$")]


class Submission[PayloadT: BaseModel](Model):
    schema_version: Literal[1] = 1
    source_instance: Identifier
    source_record_id: Identifier
    observed_at: AwareDatetime

    run_id: Identifier | None = None
    payload: PayloadT


class LogbookEvent(Model):
    id: int = Field(ge=1)
    public_id: str | None = None
    timestamp: AwareDatetime
    user_id: int | None = None
    category: Literal["Beam"]
    payload: dict[str, JsonValue]
    comment: str | None = None
    created_snapshot_id: int | None = None
    is_active: bool
    deactivated_at: AwareDatetime | None = None


class TriggerHistoryEntry(Model):
    id: Identifier
    timestamp: AwareDatetime
    title: str
    note: str
    tmod: str | None
    maj: int | None
    thresholds: dict[str, int] | None
    masks: dict[str, list[bool]] | None
    invert: dict[str, list[bool]] | None
    notes: dict[str, dict[str, str]] | None


class BroadcastFrame(Model):
    protocol: Literal["vf48-v8"] = "vf48-v8"
    epoch: Identifier
    sequence: int = Field(ge=0, strict=True)
    received_at_ns: int = Field(ge=0, strict=True)

    active_slots: list[Annotated[int, Field(ge=0, lt=VF48_SLOTS, strict=True)]] = Field(
        min_length=1, max_length=VF48_SLOTS
    )
    words: list[Annotated[int, Field(ge=0, le=UINT32_MAX, strict=True)]] = Field(
        min_length=VF48_FRAME_WORDS, max_length=VF48_FRAME_WORDS
    )

    @model_validator(mode="after")
    def unique_slots(self) -> "BroadcastFrame":
        if len(self.active_slots) != len(set(self.active_slots)):
            raise ValueError("active_slots must not contain duplicate slot indices")
        return self


class RecordDraft(Model):
    category: Literal["beam", "detector", "run"]
    source_kind: Literal["logbook", "broadcast", "triggerapp"]
    source_instance: str
    source_record_id: str
    observed_at: AwareDatetime
    supplied_run_id: str | None
    original: dict[str, JsonValue]
    processed: dict[str, JsonValue]
    processor_version: str


class StoredRecord(RecordDraft):
    id: str
    revision: int
    content_hash: str
    ingested_at: AwareDatetime


class Receipt(Model):
    id: str
    revision: int
    duplicate: bool
