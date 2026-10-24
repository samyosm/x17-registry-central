import csv
import hashlib
import io
import json
import logging
import secrets
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from x17_registry.adapters.clickhouse import ClickHouseClient, ClickHouseError
from x17_registry.adapters.registry import RegistryStore
from x17_registry.application import trigger_context
from x17_registry.application.beam import beam_context
from x17_registry.application.channels import PMT_CHANNELS
from x17_registry.application.identification import (
    Identifier,
    IdentifierUnavailable,
    InsufficientData,
    diagnose_period,
    excess_analyzer,
    excess_response,
    identify_excess_period,
    lab_source_rows,
    load_identifier,
)
from x17_registry.config import Settings


def run_id(source_instance: str, entry_id: str) -> str:
    digest = hashlib.sha256(source_instance.encode()).hexdigest()[:12]
    return f"trigger-{digest}-{entry_id}"


def run_summary(
    record: dict[str, Any],
    end: str | None,
    detector_origin: dict[str, str] | None,
    beam: dict[str, Any],
) -> dict[str, Any]:
    entry = record["raw"]
    return {
        "id": run_id(record["source_instance"], record["source_id"]),
        "runNumber": record["source_id"],
        "experimentId": "unknown",
        "title": entry.get("title") or f"Trigger write {record['source_id']}",
        "startedAt": record["event_time"],
        "endedAt": end,
        "beamStatus": beam["status"],
        "artifactCount": 2 if detector_origin is not None else 0,
    }


def run_detail(
    record: dict[str, Any],
    end: str | None,
    detector_origin: dict[str, str] | None,
    beam: dict[str, Any],
    prefix: str,
) -> dict[str, Any]:
    summary = run_summary(record, end, detector_origin, beam)
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
    registry = store or RegistryStore(
        ClickHouseClient(
            settings.clickhouse_url,
            settings.clickhouse_database,
            settings.clickhouse_user,
            settings.clickhouse_password.get_secret_value(),
            settings.clickhouse_timeout_seconds,
        )
    )

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

    @app.exception_handler(ClickHouseError)
    async def storage_error(request: Request, error: ClickHouseError) -> JSONResponse:
        logging.getLogger(__name__).error(
            "Registry ClickHouse error", exc_info=(type(error), error, error.__traceback__)
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
                end,
                None,
                beam_context(beam_events, record["event_time"], end),
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
        sources = registry.source_status()
        statuses = registry.statuses()
        for name, source in sources.items():
            source["poll"] = statuses.get(name)
        return {
            "sources": sources,
            "poller": statuses.get("poller"),
            "backfill": statuses.get("influx_backfill"),
            "checkpoints": registry.checkpoints(),
            "serverTime": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
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

    @api.get("/events")
    def detector_events(
        from_time: Annotated[datetime, Query(alias="from")],
        to_time: Annotated[datetime, Query(alias="to")],
        buckets: int = Query(120, ge=10, le=600),
    ) -> dict[str, Any]:
        """Per-PMT ADC activity in [from, to), summarised into equal time buckets."""
        start = from_time if from_time.tzinfo else from_time.replace(tzinfo=UTC)
        end = to_time if to_time.tzinfo else to_time.replace(tzinfo=UTC)
        if start >= end:
            raise HTTPException(400, "from must be before to")
        start_ns = int(start.timestamp()) * 1_000_000_000 + start.microsecond * 1_000
        end_ns = int(end.timestamp()) * 1_000_000_000 + end.microsecond * 1_000
        width_ns = max(1, -(-(end_ns - start_ns) // buckets))
        series = {
            field: {"channel": label, "end": pmt_end, "events": 0, "points": []}
            for field, (label, pmt_end) in PMT_CHANNELS.items()
        }
        for row in registry.detector_series(start_ns, end_ns, width_ns):
            entry = series.get(row["field"])
            if entry is None:
                continue
            entry["events"] += int(row["n"])
            entry["points"].append(
                {
                    "at": registry._event_at(start_ns + int(row["bucket"]) * width_ns),
                    "count": int(row["n"]),
                    "mean": round(float(row["mean"]), 2),
                    "min": float(row["low"]),
                    "max": float(row["high"]),
                }
            )
        latest = registry.latest_detector_ns()
        return {
            "from": registry._event_at(start_ns),
            "to": registry._event_at(end_ns),
            "bucketSeconds": width_ns / 1e9,
            "latestEventAt": registry._event_at(latest) if latest is not None else None,
            "channels": sorted(
                series.values(), key=lambda item: (item["end"], int(item["channel"][1:]))
            ),
        }

    identifier: list[Identifier] = []

    def loaded_identifier() -> Identifier:
        if not identifier:
            identifier.append(load_identifier(str(settings.isotope_id_config or "")))
        return identifier[0]

    def identification_input(
        from_time: datetime, to_time: datetime
    ) -> tuple[Identifier, int, int]:
        start = from_time if from_time.tzinfo else from_time.replace(tzinfo=UTC)
        end = to_time if to_time.tzinfo else to_time.replace(tzinfo=UTC)
        if start >= end:
            raise HTTPException(400, "from must be before to")
        start_ns = int(start.timestamp()) * 1_000_000_000 + start.microsecond * 1_000
        end_ns = int(end.timestamp()) * 1_000_000_000 + end.microsecond * 1_000
        return loaded_identifier(), start_ns, end_ns

    @app.exception_handler(IdentifierUnavailable)
    async def identifier_unavailable(
        request: Request, error: IdentifierUnavailable
    ) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={"error": {"code": "identifier_unavailable", "message": str(error)}},
        )

    @app.exception_handler(InsufficientData)
    async def insufficient_data(request: Request, error: InsufficientData) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={"error": {"code": "insufficient_data", "message": str(error)}},
        )

    def nanoseconds(moment: datetime) -> int:
        moment = moment if moment.tzinfo else moment.replace(tzinfo=UTC)
        return int(moment.timestamp()) * 1_000_000_000 + moment.microsecond * 1_000

    @api.get("/identification")
    def identification(
        from_time: Annotated[datetime, Query(alias="from")],
        to_time: Annotated[datetime, Query(alias="to")],
        ref_from: Annotated[datetime | None, Query(alias="refFrom")] = None,
        ref_to: Annotated[datetime | None, Query(alias="refTo")] = None,
    ) -> Any:
        """Look for a source in [from, to) as an excess over a reference period.

        The reference is [refFrom, refTo) if given, otherwise the 24 hours (or as long as the
        window) before the window, or after it if the period before holds too little data.
        """
        model, start_ns, end_ns = identification_input(from_time, to_time)
        timeout = settings.identification_timeout_seconds
        analyzer = excess_analyzer(model)
        if (ref_from is None) != (ref_to is None):
            raise HTTPException(400, "refFrom and refTo go together")
        if ref_from is not None and ref_to is not None:
            mode, ref_start, ref_end = "custom", nanoseconds(ref_from), nanoseconds(ref_to)
            if ref_start >= ref_end:
                raise HTTPException(400, "refFrom must be before refTo")
            if ref_start < end_ns and ref_end > start_ns:
                raise HTTPException(400, "The reference must not overlap the window")
        else:
            length = max(end_ns - start_ns, 86_400 * 1_000_000_000)
            history = trigger_context.epochs(registry.trigger_runs())
            current = trigger_context.main_epoch(history, start_ns, end_ns)
            # nearest period with the same trigger configuration that holds enough events
            same = (
                trigger_context.reference_candidates(
                    history, current.config, start_ns, end_ns, length
                )
                if current
                else iter(())
            )
            options = (("before", start_ns - length, start_ns), ("after", end_ns, end_ns + length))
            chosen = next(
                (
                    option
                    for option in same
                    if registry.count_events(option[1], option[2], timeout) >= analyzer.min_events
                ),
                None,
            ) or next(
                (
                    option
                    for option in options
                    if registry.count_events(option[1], option[2], timeout) >= analyzer.min_events
                ),
                options[0],
            )
            mode, ref_start, ref_end = chosen
        result = identify_excess_period(
            registry, model, (start_ns, end_ns), (ref_start, ref_end), timeout,
            registry.enabled_lab_sources(),
        )
        return excess_response(
            result,
            {
                "window": (registry._event_at(start_ns), registry._event_at(end_ns)),
                "reference": (registry._event_at(ref_start), registry._event_at(ref_end)),
                "durations": ((end_ns - start_ns) / 1e9, (ref_end - ref_start) / 1e9),
                "mode": mode,
                "trigger": trigger_context.report(
                    trigger_context.epochs(registry.trigger_runs()),
                    (start_ns, end_ns), (ref_start, ref_end),
                ),
            },
            analyzer,
        )

    class LabSourceSelection(BaseModel):
        enabled: list[int]

    def lab_sources_answer() -> dict[str, Any]:
        analyzer = excess_analyzer(loaded_identifier())
        return {"sources": lab_source_rows(analyzer, registry.enabled_lab_sources())}

    @api.get("/settings/sources")
    def lab_sources() -> Any:
        """The lab's radioactive sources, their activity today and which are reported."""
        return lab_sources_answer()

    @api.put("/settings/sources")
    def choose_lab_sources(selection: LabSourceSelection) -> Any:
        """Choose the lab sources the identification may report (unsupported ones are ignored)."""
        rows = lab_sources_answer()["sources"]
        known = {row["id"] for row in rows}
        unknown = sorted(set(selection.enabled) - known)
        if unknown:
            raise HTTPException(400, f"Unknown lab source ids: {unknown}")
        registry.set_enabled_lab_sources(selection.enabled)
        return lab_sources_answer()

    @api.get("/identification/{scintillator}")
    def identification_diagnostics(
        scintillator: str,
        from_time: Annotated[datetime, Query(alias="from")],
        to_time: Annotated[datetime, Query(alias="to")],
    ) -> Any:
        """Data scale of one scintillator against the templates, over the whole period."""
        model, start_ns, end_ns = identification_input(from_time, to_time)
        timeout = settings.identification_timeout_seconds
        result = diagnose_period(registry, model, scintillator, start_ns, end_ns, timeout)
        if result is None:
            raise HTTPException(404, "Scintillator not found")
        return {
            "scintillator": scintillator,
            "from": registry._event_at(start_ns),
            "to": registry._event_at(end_ns),
            "readings": registry.count_readings(start_ns, end_ns, timeout),
            "threshold": model.threshold,
            **{
                key: value
                for key, value in result.items()
                if key not in ("template_median", "calibration")
            },
            "templateMedian": result["template_median"],
            "calibration": {
                "kevPerUnit": result["calibration"]["kev_per_unit"],
                "gamma": result["calibration"]["gamma"],
                "factor": result["calibration"]["factor"],
            },
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
