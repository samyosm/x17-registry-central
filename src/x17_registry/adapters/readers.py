import csv
import io
import json
import re
import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from x17_registry.application.polling import SeekableInfluxReader, SourceReader, SourceRecord
from x17_registry.config import PollSettings

RUN_MEASUREMENT = re.compile(r"^run_(\d+)$")
LOGBOOK_PUBLIC_ID = re.compile(r"^Run(\d+)_[A-Za-z]\d+$")


def utc_iso(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return value


class TriggerReader(SourceReader):
    def __init__(self, settings: PollSettings) -> None:
        self.settings = settings

    def read(
        self, checkpoint: str | None, now: datetime
    ) -> tuple[Iterable[SourceRecord], str | None]:
        settings = self.settings
        history = read_json(settings.trigger_history_path)
        entries = history.get("entries")
        if not isinstance(entries, list):
            raise ValueError("Trigger history must contain an entries array.")
        records = []
        for entry in entries:
            if not isinstance(entry, dict) or not isinstance(entry.get("id"), str):
                raise ValueError("Trigger history contains an entry without an ID.")
            timestamp = entry.get("timestamp")
            records.append(
                SourceRecord(
                    "trigger_history",
                    settings.trigger_source_instance,
                    entry["id"],
                    entry,
                    utc_iso(timestamp) if isinstance(timestamp, str) else None,
                )
            )
        for kind, path in (
            ("trigger_config", settings.trigger_config_path),
            ("trigger_programmed", settings.trigger_programmed_path),
        ):
            value = read_json(path)
            updated = value.get("updated_at")
            records.append(
                SourceRecord(
                    kind,
                    settings.trigger_source_instance,
                    "current",
                    value,
                    utc_iso(updated) if isinstance(updated, str) else None,
                )
            )
        return records, None


class LogbookReader(SourceReader):
    def __init__(self, settings: PollSettings) -> None:
        self.settings = settings

    def read(
        self, checkpoint: str | None, now: datetime
    ) -> tuple[Iterable[SourceRecord], str | None]:
        settings = self.settings
        uri = f"{settings.logbook_database_path.resolve().as_uri()}?mode=ro"
        with closing(sqlite3.connect(uri, uri=True)) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("BEGIN")
            tables = {
                "logbook_user": connection.execute("SELECT * FROM users").fetchall(),
                "logbook_event": connection.execute("SELECT * FROM condition_events").fetchall(),
                "logbook_snapshot": connection.execute(
                    "SELECT * FROM condition_snapshots"
                ).fetchall(),
            }
        records = []
        for kind, rows in tables.items():
            for row in rows:
                raw = dict(row)
                for field in ("payload", "state"):
                    if field in raw:
                        raw[field] = json.loads(raw[field])
                timestamp = raw.get("timestamp") or raw.get("valid_from") or raw.get("created_at")
                public_id = raw.get("public_id")
                match = (
                    LOGBOOK_PUBLIC_ID.fullmatch(public_id) if isinstance(public_id, str) else None
                )
                run_number = match.group(1) if match else None
                records.append(
                    SourceRecord(
                        kind,
                        settings.logbook_source_instance,
                        str(raw["id"]),
                        raw,
                        utc_iso(timestamp) if isinstance(timestamp, str) else None,
                        run_number,
                    )
                )
        return records, None


def flux_csv_rows(response: Iterable[str]) -> Iterator[dict[str, str]]:
    columns: list[str] = []
    for row in csv.reader(response):
        if not row or row[0].startswith("#"):
            continue
        if "_measurement" in row and "_field" in row and "_value" in row:
            columns = row
            continue
        if columns:
            if len(row) != len(columns):
                raise ValueError("InfluxDB returned a malformed CSV row.")
            yield dict(zip(columns, row, strict=True))


class InfluxReader(SeekableInfluxReader):
    def __init__(self, settings: PollSettings) -> None:
        self.settings = settings

    def read(
        self, checkpoint: str | None, now: datetime
    ) -> tuple[Iterable[SourceRecord], str | None]:
        settings = self.settings
        if settings.influx_start_at is None:
            raise ValueError("InfluxDB start time is required when polling is enabled.")
        start = (
            datetime.fromisoformat(checkpoint.replace("Z", "+00:00"))
            - timedelta(seconds=settings.influx_overlap_seconds)
            if checkpoint
            else settings.influx_start_at
        )
        start = max(start, settings.influx_start_at)
        stop = min(start + timedelta(seconds=settings.influx_window_seconds), now)
        if start >= stop:
            return (), None
        return self.read_window(start, stop), stop.astimezone(UTC).isoformat()

    def read_window(self, start: datetime, stop: datetime) -> Iterator[SourceRecord]:
        return self._query(start, stop)

    def _query(self, start: datetime, stop: datetime) -> Iterator[SourceRecord]:
        settings = self.settings
        if settings.influx_bucket is None or settings.influx_source_instance is None:
            raise ValueError("InfluxDB polling settings are incomplete.")
        bucket = json.dumps(settings.influx_bucket)
        start_text = start.astimezone(UTC).isoformat().replace("+00:00", "Z")
        stop_text = stop.astimezone(UTC).isoformat().replace("+00:00", "Z")
        query = (
            f"from(bucket: {bucket})\n"
            f"  |> range(start: {start_text}, stop: {stop_text})\n"
            "  |> filter(fn: (r) => r._measurement =~ /^run_[0-9]+$/)"
        )
        with urlopen(self._request(query), timeout=settings.influx_timeout_seconds) as response:
            with io.TextIOWrapper(response, encoding="utf-8") as stream:
                for row in flux_csv_rows(stream):
                    measurement = row.get("_measurement", "")
                    match = RUN_MEASUREMENT.fullmatch(measurement)
                    if not match:
                        continue
                    for key in ("_time", "_field", "VF48_num"):
                        if not row.get(key):
                            raise ValueError(f"InfluxDB row has no {key}.")
                    raw = {
                        key: value
                        for key, value in row.items()
                        if key not in ("", "result", "table", "_start", "_stop")
                    }
                    timestamp = utc_iso(row["_time"])
                    identity = json.dumps(
                        [measurement, row["_time"], row["VF48_num"], row["_field"]],
                        separators=(",", ":"),
                    )
                    yield SourceRecord(
                        "influx",
                        settings.influx_source_instance,
                        identity,
                        raw,
                        timestamp,
                        match.group(1),
                    )

    def next_point_at(self, start: datetime, stop: datetime) -> datetime | None:
        settings = self.settings
        if settings.influx_bucket is None:
            raise ValueError("InfluxDB polling settings are incomplete.")
        if start >= stop:
            return None
        start_text = start.astimezone(UTC).isoformat().replace("+00:00", "Z")
        stop_text = stop.astimezone(UTC).isoformat().replace("+00:00", "Z")
        query = (
            f"from(bucket: {json.dumps(settings.influx_bucket)})\n"
            f"  |> range(start: {start_text}, stop: {stop_text})\n"
            "  |> filter(fn: (r) => r._measurement =~ /^run_[0-9]+$/)\n"
            "  |> first()\n"
            '  |> keep(columns: ["_time"])\n'
            "  |> group(columns: [])\n"
            '  |> min(column: "_time")'
        )
        with urlopen(self._request(query), timeout=settings.influx_timeout_seconds) as response:
            with io.TextIOWrapper(response, encoding="utf-8") as stream:
                columns: list[str] = []
                for row in csv.reader(stream):
                    if not row or row[0].startswith("#"):
                        continue
                    if "_time" in row:
                        columns = row
                        continue
                    if columns:
                        values = dict(zip(columns, row, strict=True))
                        timestamp = values.get("_time")
                        if timestamp:
                            return datetime.fromisoformat(utc_iso(timestamp).replace("Z", "+00:00"))
                    else:
                        raise ValueError("InfluxDB next-point response has no _time column.")
        return None

    def _request(self, query: str) -> Request:
        settings = self.settings
        if (
            settings.influx_url is None
            or settings.influx_org is None
            or settings.influx_token is None
        ):
            raise ValueError("InfluxDB polling settings are incomplete.")
        parameters = urlencode({"org": settings.influx_org})
        return Request(
            f"{settings.influx_url.rstrip('/')}/api/v2/query?{parameters}",
            data=query.encode(),
            headers={
                "Authorization": f"Token {settings.influx_token.get_secret_value()}",
                "Accept": "application/csv",
                "Content-Type": "application/vnd.flux",
            },
            method="POST",
        )
