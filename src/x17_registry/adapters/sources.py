from pydantic import BaseModel, JsonValue

from x17_registry.application.ingestion import (
    BeamDataIngestor,
    DetectorDataIngestor,
    RunDataIngestor,
)
from x17_registry.domain.models import (
    BroadcastFrame,
    LogbookEvent,
    RecordDraft,
    Submission,
    TriggerHistoryEntry,
)
from x17_registry.domain.protocol import (
    HALF_WORD_MASK,
    HEADER_MARKER,
    TRAILER_MARKER,
    VF48_BLOCKS,
    VF48_SLOTS,
    VF48_WORDS_PER_SLOT,
)


def draft[T: BaseModel](
    submission: Submission[T],
    category: str,
    source_kind: str,
    processed: dict[str, JsonValue],
    processor_version: str,
) -> RecordDraft:
    return RecordDraft.model_validate(
        {
            "category": category,
            "source_kind": source_kind,
            "source_instance": submission.source_instance,
            "source_record_id": submission.source_record_id,
            "observed_at": submission.observed_at,
            "supplied_run_id": submission.run_id,
            "original": submission.payload.model_dump(mode="json"),
            "processed": processed,
            "processor_version": processor_version,
        }
    )


class LogbookBeamIngestor(BeamDataIngestor[LogbookEvent]):
    mutable = True

    def process(self, submission: Submission[LogbookEvent]) -> RecordDraft:
        event = submission.payload
        if submission.source_record_id != str(event.id):
            raise ValueError("Logbook source_record_id must equal the SQLite event id.")
        status = event.payload.get("status")
        declared_status = (
            status.lower() if isinstance(status, str) and status in ("ON", "OFF") else "unknown"
        )
        return draft(
            submission,
            "beam",
            "logbook",
            {
                "declared_status": declared_status,
                "evidence": "operator_declaration",
                "is_active": event.is_active,
                "source_timestamp": event.timestamp.isoformat(),
                "comment": event.comment,
            },
            "logbook-event-v1",
        )


class TriggerAppRunIngestor(RunDataIngestor[TriggerHistoryEntry]):
    mutable = True

    def process(self, submission: Submission[TriggerHistoryEntry]) -> RecordDraft:
        entry = submission.payload
        if submission.source_record_id != entry.id:
            raise ValueError("TriggerApp source_record_id must equal the history entry id.")
        return draft(
            submission,
            "run",
            "triggerapp",
            {
                "configuration_title": entry.title,
                "evidence": "saved_history_snapshot",
                "hardware_verified": False,
                "source_timestamp": entry.timestamp.isoformat(),
                "run_boundary": None,
            },
            "triggerapp-history-v1",
        )


class BroadcastDetectorIngestor(DetectorDataIngestor[BroadcastFrame]):
    mutable = False

    def process(self, submission: Submission[BroadcastFrame]) -> RecordDraft:
        frame = submission.payload
        if submission.source_record_id != f"{frame.epoch}:{frame.sequence}":
            raise ValueError("Broadcast source_record_id must equal epoch:sequence.")
        headers: list[JsonValue] = []
        for block in range(VF48_BLOCKS):
            for slot in frame.active_slots:
                start = (block * VF48_SLOTS + slot) * VF48_WORDS_PER_SLOT
                words = frame.words[start : start + VF48_WORDS_PER_SLOT]
                headers.append(
                    {
                        "block": block,
                        "slot": slot,
                        "trignum": ((words[0] & HALF_WORD_MASK) << 16) | (words[1] >> 16),
                        "tstamp": ((words[1] & HALF_WORD_MASK) << 32) | words[2],
                        "livetime": (words[3] << 16) | (words[4] >> 16),
                        "header_marker_valid": words[0] >> 16 == HEADER_MARKER,
                        "trailer_marker_valid": words[-1] & HALF_WORD_MASK == TRAILER_MARKER,
                    }
                )
        return draft(
            submission,
            "detector",
            "broadcast",
            {
                "headers": headers,
                "evidence": "received_frame",
                "time_basis": "collector_receive_time",
                "hardware_tick_seconds": None,
                "filter_applied": False,
            },
            "vf48-v8-headers-v1",
        )
