import hashlib
import json
import secrets
import sqlite3
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import date
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from x17_registry.adapters.registry import RegistryStore
from x17_registry.config import Settings


def run_id(source_instance: str, number: str) -> str:
    digest = hashlib.sha256(source_instance.encode()).hexdigest()[:12]
    return f"influx-{digest}-{number}"


def run_summary(stat: dict[str, Any]) -> dict[str, Any]:
    number = stat["run_number"]
    return {
        "id": run_id(stat["source_instance"], number),
        "runNumber": number,
        "experimentId": "unknown",
        "title": f"Run {number}",
        "startedAt": stat["started_at"],
        "beamStatus": "unknown",
        "artifactCount": 1,
    }


def run_detail(stat: dict[str, Any], prefix: str) -> dict[str, Any]:
    summary = run_summary(stat)
    record_id = summary["id"]
    return {
        **summary,
        "titleSource": "InfluxDB measurement",
        "endedAt": None,
        "completeness": "partial",
        "detectorSources": ["VF48"],
        "beam": {
            "status": "unknown",
            "source": "No verified run association",
            "energy": None,
            "current": None,
            "charge": None,
            "note": None,
        },
        "configuration": None,
        "notes": None,
        "artifacts": [
            {
                "id": "detector-records",
                "name": f"run_{stat['run_number']}_detector.json",
                "format": "JSON",
                "source": "VF48 / InfluxDB",
                "sizeBytes": None,
                "state": "ready",
                "downloadUrl": f"{prefix}/runs/{record_id}/artifacts/detector-records/download",
            }
        ],
    }


def export_records(store: RegistryStore, source_instance: str, number: str) -> Iterator[bytes]:
    yield b"["
    first = True
    for record in store.iter_run(source_instance, number):
        if not first:
            yield b","
        yield json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode()
        first = False
    yield b"]"


def create_app(settings: Settings, store: RegistryStore | None = None) -> FastAPI:
    registry = store or RegistryStore(settings.database_path, settings.sqlite_timeout_seconds)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if not settings.api_token.get_secret_value():
            raise RuntimeError("X17_API_TOKEN must be set before serving the API.")
        registry.initialize()
        yield

    app = FastAPI(title="X17 Registry Central", lifespan=lifespan)
    bearer = HTTPBearer(auto_error=False)

    def authenticate(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    ) -> None:
        token = settings.api_token.get_secret_value()
        if credentials is None or not secrets.compare_digest(
            credentials.credentials.encode(), token.encode()
        ):
            raise HTTPException(401, "Invalid bearer token", headers={"WWW-Authenticate": "Bearer"})

    @app.exception_handler(sqlite3.Error)
    async def storage_error(request: Request, error: sqlite3.Error) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={
                "error": {
                    "code": "storage_unavailable",
                    "message": "Registry unavailable",
                }
            },
        )

    @app.get("/health/live")
    def live() -> dict[str, str]:
        return {"status": "alive"}

    api = APIRouter(prefix=settings.api_prefix, dependencies=[Depends(authenticate)])

    @api.get("/records")
    def list_records(
        source: str | None = None,
        page: int = Query(1, ge=1),
        page_size: int | None = Query(None, alias="pageSize", ge=1),
    ) -> dict[str, Any]:
        size = min(page_size or settings.page_size, settings.max_page_size)
        records, total = registry.records(source, page, size)
        return {
            "records": records,
            "total": total,
            "page": page,
            "totalPages": max(1, (total + size - 1) // size),
        }

    @api.get("/records/{record_id}")
    def get_record(record_id: int) -> dict[str, Any]:
        record = registry.get(record_id)
        if record is None:
            raise HTTPException(404, "Record not found")
        return record

    @api.get("/runs")
    def list_runs(
        q: str | None = None,
        from_date: Annotated[date | None, Query(alias="from")] = None,
        to_date: Annotated[date | None, Query(alias="to")] = None,
        sort: Literal["newest", "oldest"] = "newest",
        page: int = Query(1, ge=1),
        page_size: int | None = Query(None, alias="pageSize", ge=1),
    ) -> dict[str, Any]:
        if from_date and to_date and from_date > to_date:
            raise HTTPException(400, "from must be on or before to")
        size = min(page_size or settings.page_size, settings.max_page_size)
        runs = [run_summary(stat) for stat in registry.run_stats()]
        if q:
            term = q.casefold()
            runs = [
                run
                for run in runs
                if term in " ".join((run["id"], run["runNumber"], run["title"])).casefold()
            ]
        if from_date:
            runs = [run for run in runs if run["startedAt"][:10] >= from_date.isoformat()]
        if to_date:
            runs = [run for run in runs if run["startedAt"][:10] <= to_date.isoformat()]
        runs.sort(key=lambda run: (run["startedAt"], run["id"]), reverse=sort == "newest")
        total = len(runs)
        return {
            "runs": runs[(page - 1) * size : page * size],
            "total": total,
            "page": page,
            "totalPages": max(1, (total + size - 1) // size),
        }

    def find_run(identity: str) -> dict[str, Any]:
        for stat in registry.run_stats():
            if run_id(stat["source_instance"], stat["run_number"]) == identity:
                return stat
        raise HTTPException(404, "Run not found")

    @api.get("/runs/{identity}")
    def get_run(identity: str) -> dict[str, Any]:
        return run_detail(find_run(identity), settings.api_prefix)

    @api.get("/runs/{identity}/artifacts/{artifact_id}/download")
    def download(identity: str, artifact_id: str) -> StreamingResponse:
        stat = find_run(identity)
        if artifact_id != "detector-records":
            raise HTTPException(404, "Artifact not found")
        filename = f"run_{stat['run_number']}_detector.json"
        return StreamingResponse(
            export_records(registry, stat["source_instance"], stat["run_number"]),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    app.include_router(api)
    return app
