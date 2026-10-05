import csv
import hashlib
import io
import json
import logging
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
from x17_registry.application.beam import beam_context
from x17_registry.config import Settings


def run_id(source_instance: str, entry_id: str) -> str:
    digest = hashlib.sha256(source_instance.encode()).hexdigest()[:12]
    return f"trigger-{digest}-{entry_id}"


def run_summary(
    record: dict[str, Any],
    detector_origin: dict[str, str] | None,
    beam: dict[str, Any],
    stats: dict[str, int] | None,
) -> dict[str, Any]:
    entry = record["raw"]
    return {
        "id": run_id(record["source_instance"], record["source_id"]),
        "runNumber": record["source_id"],
        "experimentId": "unknown",
        "title": entry.get("title") or f"Trigger write {record['source_id']}",
        "startedAt": record["event_time"],
        "beamStatus": beam["status"],
        "artifactCount": 2 if detector_origin is not None else 0,
        "pointCount": stats["pointCount"] if stats else None,
        "estimatedJsonBytes": stats["estimatedJsonBytes"] if stats else None,
    }


def run_detail(
    record: dict[str, Any],
    end: str | None,
    detector_origin: dict[str, str] | None,
    beam: dict[str, Any],
    stats: dict[str, int] | None,
    prefix: str,
) -> dict[str, Any]:
    summary = run_summary(record, detector_origin, beam, stats)
    entry = record["raw"]
    record_id = summary["id"]
    configuration = {
        "title": entry.get("title"),
        "titleSource": "TriggerApp history title",
        "triggerMode": entry.get("tmod"),
        "majority": entry.get("maj"),
        "channels": sorted((entry.get("thresholds") or {}).keys()),
        "evidence": "Saved TriggerApp configuration; hardware readback unverified",
    }
    artifacts = []
    if detector_origin is not None:
        for format_name in ("csv", "json"):
            artifact_id = f"detector-records-{format_name}"
            artifacts.append(
                {
                    "id": artifact_id,
                    "name": f"{record_id}_detector.{format_name}",
                    "format": format_name.upper(),
                    "source": f"VF48 / InfluxDB v2 / {detector_origin['source_instance']}",
                    "sizeBytes": None,
                    "state": "ready",
                    "downloadUrl": f"{prefix}/runs/{record_id}/artifacts/{artifact_id}/download",
                }
            )
    return {
        **summary,
        "titleSource": "TriggerApp history title",
        "endedAt": end,
        "completeness": "partial",
        "detectorSources": [
            "VF48 / InfluxDB v2"
            f" / {detector_origin['source_instance']}"
            f" / run_{detector_origin['run_number']}"
        ]
        if detector_origin is not None
        else [],
        "beam": beam,
        "configuration": configuration,
        "notes": entry.get("note") or None,
        "artifacts": artifacts,
    }


def export_json(store: RegistryStore, start: str, end: str | None) -> Iterator[bytes]:
    yield b"["
    first = True
    for point in store.iter_detector_points(start, end):
        if not first:
            yield b","
        yield json.dumps(point, ensure_ascii=False, separators=(",", ":")).encode()
        first = False
    yield b"]"


def export_csv(store: RegistryStore, start: str, end: str | None) -> Iterator[bytes]:
    columns = ("_time", "_measurement", "VF48_num", "_field", "_value", "extra_json")
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns)
    writer.writeheader()
    yield buffer.getvalue().encode()
    buffer.seek(0)
    buffer.truncate(0)
    for point in store.iter_detector_points(start, end):
        row = {key: point.get(key, "") for key in columns if key != "extra_json"}
        extra = {key: value for key, value in point.items() if key not in columns}
        row["extra_json"] = json.dumps(extra, ensure_ascii=False, separators=(",", ":"))
        writer.writerow(row)
        yield buffer.getvalue().encode()
        buffer.seek(0)
        buffer.truncate(0)


def run_windows(store: RegistryStore) -> list[tuple[dict[str, Any], str | None]]:
    records = store.trigger_runs()
    next_start: dict[str, str] = {}
    windows = []
    for record in reversed(records):
        source = record["source_instance"]
        windows.append((record, next_start.get(source)))
        next_start[source] = record["event_time"]
    return windows


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
        logging.getLogger(__name__).error(
            "Registry SQLite error", exc_info=(type(error), error, error.__traceback__)
        )
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
        beam: Literal["on", "off", "unknown"] | None = None,
        has_data: Annotated[bool | None, Query(alias="hasData")] = None,
        sort: Literal["newest", "oldest"] = "newest",
        page: int = Query(1, ge=1),
        page_size: int | None = Query(None, alias="pageSize", ge=1),
    ) -> dict[str, Any]:
        if from_date and to_date and from_date > to_date:
            raise HTTPException(400, "from must be on or before to")
        size = min(page_size or settings.page_size, settings.max_page_size)
        beam_events = registry.beam_events()
        windows = run_windows(registry)
        runs = []
        intervals = {}
        for record, end in windows:
            summary = run_summary(
                record,
                None,
                beam_context(beam_events, record["event_time"], end),
                None,
            )
            runs.append(summary)
            intervals[summary["id"]] = (record["event_time"], end)
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
        if beam:
            runs = [run for run in runs if run["beamStatus"] == beam]
        if has_data is not None:
            filtered = []
            for run in runs:
                start, end = intervals[run["id"]]
                origin = registry.influx_origin(start, end)
                if (origin is not None) == has_data:
                    run["artifactCount"] = 2 if origin is not None else 0
                    filtered.append(run)
            runs = filtered
        runs.sort(key=lambda run: (run["startedAt"], run["id"]), reverse=sort == "newest")
        total = len(runs)
        page_runs = runs[(page - 1) * size : page * size]
        for run in page_runs:
            if has_data is None:
                start, end = intervals[run["id"]]
                run["artifactCount"] = 2 if registry.influx_origin(start, end) else 0
        return {
            "runs": page_runs,
            "total": total,
            "page": page,
            "totalPages": max(1, (total + size - 1) // size),
        }

    @api.get("/runs/stats")
    def run_stats(ids: Annotated[list[str], Query()]) -> dict[str, Any]:
        if len(ids) > settings.max_page_size:
            raise HTTPException(400, "Too many run IDs")
        intervals = {
            run_id(record["source_instance"], record["source_id"]): (record["event_time"], end)
            for record, end in run_windows(registry)
        }
        return {
            "stats": {
                identity: registry.detector_window_stats(*intervals[identity])
                for identity in dict.fromkeys(ids)
                if identity in intervals
            }
        }

    @api.get("/runs/suggestions")
    def suggest_runs(q: str = Query(min_length=1, max_length=200)) -> dict[str, Any]:
        term = q.casefold().strip()
        matches = []
        for record in reversed(registry.trigger_runs()):
            title = record["raw"].get("title") or ""
            identity = run_id(record["source_instance"], record["source_id"])
            if term in f"{identity} {record['source_id']} {title}".casefold():
                matches.append({"id": identity, "title": title, "startedAt": record["event_time"]})
            if len(matches) == 8:
                break
        return {"suggestions": matches}

    @api.get("/diagnostics")
    def diagnostics() -> dict[str, Any]:
        return {
            "sources": registry.source_status(),
            "runStarts": [
                {
                    "at": record["event_time"],
                    "id": run_id(record["source_instance"], record["source_id"]),
                    "title": record["raw"].get("title") or "",
                }
                for record in registry.trigger_runs()
            ],
            "beamChanges": [
                {
                    "at": record["event_time"],
                    "status": record["raw"].get("payload", {}).get("status"),
                    "publicId": record["raw"].get("public_id"),
                }
                for record in registry.beam_events()
                if isinstance(record["raw"].get("payload"), dict)
                and record["raw"]["payload"].get("status") in ("ON", "OFF")
            ],
        }

    def find_run(identity: str) -> tuple[dict[str, Any], str | None]:
        for record, end in run_windows(registry):
            if run_id(record["source_instance"], record["source_id"]) == identity:
                return record, end
        raise HTTPException(404, "Run not found")

    @api.get("/runs/{identity}")
    def get_run(identity: str) -> dict[str, Any]:
        record, end = find_run(identity)
        return run_detail(
            record,
            end,
            registry.influx_origin(record["event_time"], end),
            beam_context(registry.beam_events(), record["event_time"], end),
            None,
            settings.api_prefix,
        )

    @api.get("/runs/{identity}/artifacts/{artifact_id}/download")
    def download(identity: str, artifact_id: str) -> StreamingResponse:
        record, end = find_run(identity)
        if artifact_id not in ("detector-records", "detector-records-csv", "detector-records-json"):
            raise HTTPException(404, "Artifact not found")
        if registry.influx_origin(record["event_time"], end) is None:
            raise HTTPException(404, "Artifact not found")
        format_name = (
            "json"
            if artifact_id == "detector-records"
            else artifact_id.removeprefix("detector-records-")
        )
        filename = f"{identity}_detector.{format_name}"
        return StreamingResponse(
            export_csv(registry, record["event_time"], end)
            if format_name == "csv"
            else export_json(registry, record["event_time"], end),
            media_type="text/csv" if format_name == "csv" else "application/json",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    app.include_router(api)
    return app
