from pydantic import BaseModel

from x17_registry.application.ingestion import DetectorDataIngestor
from x17_registry.application.ports import RunQueryService
from x17_registry.domain.errors import IntegrationUnavailable
from x17_registry.domain.models import RecordDraft, Submission


class CaenDetectorIngestor(DetectorDataIngestor[BaseModel]):
    mutable = False

    def process(self, submission: Submission[BaseModel]) -> RecordDraft:
        raise IntegrationUnavailable(
            "CAEN ingestion needs the deployed CoMPASS format, configuration, "
            "and timestamp contract."
        )


class PendingRunQueries(RunQueryService):
    def search(self) -> object:
        raise IntegrationUnavailable(
            "Run search needs verified run identity and source association rules."
        )

    def detail(self, run_id: str) -> object:
        raise IntegrationUnavailable(
            "Run detail needs verified boundaries and configuration/beam correlation."
        )

    def download(self, run_id: str, artifact_id: str) -> object:
        raise IntegrationUnavailable(
            "Artifact download needs verified storage paths and access rules."
        )
