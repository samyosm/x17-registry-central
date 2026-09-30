from abc import ABC, abstractmethod

from x17_registry.domain.models import Receipt, RecordDraft, StoredRecord


class RecordRepository(ABC):
    @abstractmethod
    def save(self, record: RecordDraft, *, mutable: bool) -> Receipt:
        raise NotImplementedError

    @abstractmethod
    def get(self, record_id: str) -> StoredRecord | None:
        raise NotImplementedError


class RunQueryService(ABC):
    @abstractmethod
    def search(self) -> object:
        raise NotImplementedError

    @abstractmethod
    def detail(self, run_id: str) -> object:
        raise NotImplementedError

    @abstractmethod
    def download(self, run_id: str, artifact_id: str) -> object:
        raise NotImplementedError
