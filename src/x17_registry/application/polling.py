import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


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


class PollJob:
    def __init__(
        self,
        readers: dict[str, SourceReader],
        processor: Processor,
        store: Any,
        detector_batch_size: int,
    ) -> None:
        self.readers = readers
        self.processor = processor
        self.store = store
        self.detector_batch_size = detector_batch_size

    def run_source(self, name: str, now: datetime) -> int:
        records, checkpoint = self.readers[name].read(self.store.checkpoint(name), now)
        count = 0
        detector_batch: list[SourceRecord] = []
        source_batch: list[tuple[SourceRecord, dict[str, Any]]] = []
        for record in records:
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
        if checkpoint is not None:
            self.store.set_checkpoint(name, checkpoint)
        self.store.mark_synced(name, datetime.now(UTC))
        return count

    def run_once(self) -> dict[str, int | str]:
        now = datetime.now(UTC)
        results: dict[str, int | str] = {}
        for name in self.readers:
            try:
                results[name] = self.run_source(name, now)
            except Exception:
                logging.exception("Polling failed for %s", name)
                results[name] = "error"
        return results
