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


class Processor(ABC):
    @abstractmethod
    def process(self, record: SourceRecord) -> dict[str, Any]:
        raise NotImplementedError


class IdentityProcessor(Processor):
    def process(self, record: SourceRecord) -> dict[str, Any]:
        return deepcopy(record.raw)


class PollJob:
    def __init__(self, readers: dict[str, SourceReader], processor: Processor, store: Any) -> None:
        self.readers = readers
        self.processor = processor
        self.store = store

    def run_source(self, name: str, now: datetime) -> int:
        records, checkpoint = self.readers[name].read(self.store.checkpoint(name), now)
        count = 0
        for record in records:
            self.store.save(record, self.processor.process(record))
            count += 1
        if checkpoint is not None:
            self.store.set_checkpoint(name, checkpoint)
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
