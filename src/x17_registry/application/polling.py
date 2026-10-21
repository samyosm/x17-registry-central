import logging
import os
from abc import ABC, abstractmethod
from collections.abc import Iterable
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from time import monotonic
from http.client import IncompleteRead, RemoteDisconnected
from typing import Any

CATCH_UP_LAG = timedelta(seconds=120)


class AttemptDeadlineExceeded(TimeoutError):
    """A single read attempt ran past its time budget."""


STREAM_FAILURES = (IncompleteRead, RemoteDisconnected, ConnectionResetError, TimeoutError)


@dataclass(frozen=True)
class SourceRecord:
    source: str
    source_instance: str
    source_id: str
    raw: dict[str, Any]
    event_time: str | None = None
    run_number: str | None = None


class SourceReader(ABC):
    @abstractmethod
    def read(
        self, checkpoint: str | None, now: datetime
    ) -> tuple[Iterable[SourceRecord], str | None]:
        raise NotImplementedError

    def shrink_window(self) -> bool:
        """Narrow the next read after a stream failure; False if it cannot shrink."""
        return False

    def grow_window(self) -> None:
        """Widen the next read back toward its configured size after a success."""
        return None

    def current_window(self) -> tuple[datetime, datetime] | None:
        """The time range of the latest read, for sources that read by window."""
        return None


class SeekableInfluxReader(SourceReader, ABC):
    @abstractmethod
    def read_window(self, start: datetime, stop: datetime) -> Iterable[SourceRecord]:
        raise NotImplementedError

    @abstractmethod
    def next_point_at(self, start: datetime, stop: datetime) -> datetime | None:
        raise NotImplementedError

    @abstractmethod
    def previous_point_before(self, start: datetime, stop: datetime) -> datetime | None:
        raise NotImplementedError


class Processor(ABC):
    @abstractmethod
    def process(self, record: SourceRecord) -> dict[str, Any]:
        raise NotImplementedError


class IdentityProcessor(Processor):
    def process(self, record: SourceRecord) -> dict[str, Any]:
        return deepcopy(record.raw)


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


class PollJob:
    def __init__(
        self,
        readers: dict[str, SourceReader],
        processor: Processor,
        store: Any,
        detector_batch_size: int,
        attempt_seconds: float | None = None,
        interval_seconds: float | None = None,
    ) -> None:
        self.readers = readers
        self.processor = processor
        self.store = store
        self.detector_batch_size = detector_batch_size
        self.attempt_seconds = attempt_seconds
        self.interval_seconds = interval_seconds
        self.behind: set[str] = set()
        self.statuses: dict[str, dict[str, Any]] = {}
        self.started_at = _iso(datetime.now(UTC))

    def _publish(self, name: str, **changes: Any) -> None:
        status = self.statuses.setdefault(name, {"state": "idle", "consecutiveFailures": 0})
        status.update(changes)
        status["updatedAt"] = _iso(datetime.now(UTC))
        try:
            self.store.set_status(name, status)
        except Exception:
            logging.exception("Could not record %s poll status", name)

    def heartbeat(self, state: str, **changes: Any) -> None:
        self._publish(
            "poller",
            state=state,
            pid=os.getpid(),
            startedAt=self.started_at,
            intervalSeconds=self.interval_seconds,
            **changes,
        )

    def run_source(self, name: str, now: datetime) -> int:
        reader = self.readers[name]
        attempt_started = datetime.now(UTC)
        retries = 0
        self._publish(name, state="reading", attemptStartedAt=_iso(attempt_started), retries=0)
        try:
            while True:
                window = reader.current_window()
                try:
                    count, checkpoint = self._read_and_save(name, now)
                    break
                except STREAM_FAILURES as error:
                    if not reader.shrink_window():
                        raise
                    retries += 1
                    logging.warning(
                        "Stream failed for %s (%s); retrying with a smaller window",
                        name,
                        type(error).__name__,
                    )
                    self._publish(
                        name,
                        state="retrying",
                        retries=retries,
                        lastRetryReason=f"{type(error).__name__}: {error}"[:300],
                        failedWindow=[_iso(window[0]), _iso(window[1])] if window else None,
                    )
        except Exception as error:
            failures = int(self.statuses[name].get("consecutiveFailures", 0)) + 1
            self._publish(
                name,
                state="failing",
                consecutiveFailures=failures,
                lastAttemptAt=_iso(datetime.now(UTC)),
                lastError={
                    "type": type(error).__name__,
                    "message": str(error)[:500],
                    "at": _iso(datetime.now(UTC)),
                },
            )
            raise
        reader.grow_window()
        if checkpoint is not None:
            self.store.set_checkpoint(name, checkpoint)
        finished = datetime.now(UTC)
        self.store.mark_synced(name, finished)
        window = reader.current_window()
        behind = checkpoint is not None and window is not None and now - window[1] > CATCH_UP_LAG
        if behind:
            self.behind.add(name)
        else:
            self.behind.discard(name)
        self._publish(
            name,
            state="catching_up" if behind else "idle",
            consecutiveFailures=0,
            lastAttemptAt=_iso(finished),
            lastSuccessAt=_iso(finished),
            lastRecords=count,
            lastDurationSeconds=round((finished - attempt_started).total_seconds(), 1),
            lastWindow=[_iso(window[0]), _iso(window[1])] if window else None,
            retries=retries,
        )
        return count

    def _read_and_save(self, name: str, now: datetime) -> tuple[int, str | None]:
        deadline = (
            monotonic() + self.attempt_seconds
            if name == "influx" and self.attempt_seconds
            else None
        )
        records, checkpoint = self.readers[name].read(self.store.checkpoint(name), now)
        window = self.readers[name].current_window()
        if window:
            self._publish(name, window=[_iso(window[0]), _iso(window[1])])
        count = 0
        detector_batch: list[SourceRecord] = []
        source_batch: list[tuple[SourceRecord, dict[str, Any]]] = []
        for record in records:
            if deadline is not None and monotonic() > deadline:
                raise AttemptDeadlineExceeded(
                    f"read attempt exceeded {self.attempt_seconds:g} s"
                )
            if record.source == "influx":
                detector_batch.append(record)
                if len(detector_batch) >= self.detector_batch_size:
                    self.store.save_detector_batch(detector_batch)
                    detector_batch.clear()
            else:
                source_batch.append((record, self.processor.process(record)))
            count += 1
        if detector_batch:
            self.store.save_detector_batch(detector_batch)
        if source_batch:
            self.store.save_batch(source_batch)
        return count, checkpoint

    def run_once(self, only: Iterable[str] | None = None) -> dict[str, int | str]:
        now = datetime.now(UTC)
        results: dict[str, int | str] = {}
        names = list(self.readers) if only is None else [n for n in self.readers if n in only]
        for name in names:
            try:
                results[name] = self.run_source(name, now)
            except Exception:
                logging.exception("Polling failed for %s", name)
                results[name] = "error"
        return results
