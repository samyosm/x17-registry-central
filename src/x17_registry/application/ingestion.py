from abc import ABC, abstractmethod

from pydantic import BaseModel

from x17_registry.application.ports import RecordRepository
from x17_registry.domain.models import Receipt, RecordDraft, Submission


class DataIngestor[T: BaseModel](ABC):
    def __init__(self, repository: RecordRepository) -> None:
        self.repository = repository

    @property
    @abstractmethod
    def mutable(self) -> bool:
        raise NotImplementedError

    def receive(self, submission: Submission[T]) -> Receipt:
        return self.store(self.process(submission))

    @abstractmethod
    def process(self, submission: Submission[T]) -> RecordDraft:
        raise NotImplementedError

    def store(self, record: RecordDraft) -> Receipt:
        return self.repository.save(record, mutable=self.mutable)


class BeamDataIngestor[T: BaseModel](DataIngestor[T], ABC):
    pass


class DetectorDataIngestor[T: BaseModel](DataIngestor[T], ABC):
    pass


class RunDataIngestor[T: BaseModel](DataIngestor[T], ABC):
    pass
