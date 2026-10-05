import hashlib
import json
import re
import time
from calendar import timegm
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from typing import Any

from x17_registry.adapters.clickhouse import ClickHouseClient
from x17_registry.application.polling import SourceRecord


def epoch_ns(value: str) -> int:
    text = value.replace("Z", "+00:00")
    moment = datetime.fromisoformat(text).astimezone(UTC)
    seconds = timegm(moment.utctimetuple())
    match = re.search(r"\.(\d+)", text)
    fraction = match.group(1)[:9] if match else ""
    return seconds * 1_000_000_000 + int(fraction.ljust(9, "0") or "0")


def record_id(source: str, instance: str, identity: str, revision: int) -> int:
    key = json.dumps([source, instance, identity, revision], separators=(",", ":"))
    return int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], "big") & ((1 << 53) - 1)


def latest_records(source: str | None = None) -> str:
    condition = "WHERE source={source:String}" if source else ""
    return (
        "SELECT * FROM collected_records FINAL "
        f"{condition} ORDER BY source,source_instance,source_id,revision DESC "
        "LIMIT 1 BY source,source_instance,source_id"
    )


SCHEMA = (
    """CREATE TABLE IF NOT EXISTS collected_records (
        id UInt64, source LowCardinality(String), source_instance String, source_id String,
        revision UInt32, content_hash String, event_time Nullable(String),
        event_ns Nullable(Int64), run_number Nullable(String), raw_json String,
        processed_json String, collected_at String, version UInt64
    ) ENGINE = ReplacingMergeTree(version)
    ORDER BY (source,source_instance,source_id,revision)""",
    """CREATE TABLE IF NOT EXISTS detector_points (
        source_instance String, source_id String, event_ns Int64,
        event_time String, run_number String, point_json String, version UInt64
    ) ENGINE = ReplacingMergeTree(version)
    ORDER BY (event_ns,source_instance,source_id)""",
    """CREATE TABLE IF NOT EXISTS registry_state (
        kind LowCardinality(String), source String, value String, version UInt64
    ) ENGINE = ReplacingMergeTree(version) ORDER BY (kind,source)""",
)


class RegistryStore:
    def __init__(self, client: ClickHouseClient) -> None:
        self.client = client

    def initialize(self) -> None:
        self.client.execute(
            f"CREATE DATABASE IF NOT EXISTS {self.client.database}", database="default"
        )
        for statement in SCHEMA:
            self.client.execute(statement)

    @staticmethod
    def document(row: dict[str, Any]) -> dict[str, Any]:
        result = dict(row)
        result["raw"] = json.loads(result.pop("raw_json"))
        result["processed"] = json.loads(result.pop("processed_json"))
        result.pop("version", None)
        result.pop("event_ns", None)
        return result

    def save(self, record: SourceRecord, processed: dict[str, Any]) -> bool:
        if record.source == "influx":
            return bool(self.save_detector_batch([record]))
        return self.save_batch([(record, processed)]) > 0

    def save_batch(self, records: Iterable[tuple[SourceRecord, dict[str, Any]]]) -> int:
        batch = list(records)
        if not batch:
            return 0
        if any(record.source == "influx" for record, _ in batch):
            raise ValueError("Detector records require save_detector_batch.")
        previous = {
            (row["source"], row["source_instance"], row["source_id"]): (
                int(row["revision"]), row["content_hash"]
            )
            for row in self.client.query(
                f"SELECT source,source_instance,source_id,revision,content_hash "
                f"FROM ({latest_records()})"
            )
        }
        rows = []
        for record, processed in batch:
            raw_json = json.dumps(record.raw, sort_keys=True, allow_nan=False)
            processed_json = json.dumps(processed, sort_keys=True, allow_nan=False)
            digest = hashlib.sha256(f"{raw_json}\0{processed_json}".encode()).hexdigest()
            key = (record.source, record.source_instance, record.source_id)
            revision, old_digest = previous.get(key, (0, ""))
            if digest == old_digest:
                continue
            revision += 1
            previous[key] = (revision, digest)
            rows.append({
                "id": record_id(*key, revision),
                "source": record.source,
                "source_instance": record.source_instance,
                "source_id": record.source_id,
                "revision": revision,
                "content_hash": digest,
                "event_time": record.event_time,
                "event_ns": epoch_ns(record.event_time) if record.event_time else None,
                "run_number": record.run_number,
                "raw_json": raw_json,
                "processed_json": processed_json,
                "collected_at": datetime.now(UTC).isoformat(),
                "version": time.time_ns(),
            })
        if rows:
            self.client.insert("collected_records", rows)
        return len(rows)

    def save_detector_batch(self, records: Iterable[SourceRecord]) -> int:
        rows = []
        version = time.time_ns()
        for record in records:
            if record.source != "influx" or record.event_time is None or record.run_number is None:
                raise ValueError("Detector batch requires complete Influx points.")
            rows.append({
                "source_instance": record.source_instance,
                "source_id": record.source_id,
                "event_ns": epoch_ns(record.event_time),
                "event_time": record.event_time,
                "run_number": record.run_number,
                "point_json": json.dumps(record.raw, sort_keys=True, allow_nan=False),
                "version": version,
            })
        self.client.insert("detector_points", rows)
        return len(rows)

    def save_detector_backfill_batch(self, records: Iterable[SourceRecord]) -> int:
        return self.save_detector_batch(records)

    def _state(self, kind: str, source: str) -> str | None:
        rows = self.client.query(
            "SELECT value FROM registry_state FINAL WHERE kind={kind:String} "
            "AND source={source:String} LIMIT 1",
            {"kind": kind, "source": source},
        )
        return str(rows[0]["value"]) if rows else None

    def _set_state(self, kind: str, source: str, value: str) -> None:
        self.client.insert(
            "registry_state",
            [{"kind": kind, "source": source, "value": value, "version": time.time_ns()}],
        )

    def checkpoint(self, source: str) -> str | None:
        return self._state("checkpoint", source)

    def set_checkpoint(self, source: str, value: str) -> None:
        self._set_state("checkpoint", source, value)

    def mark_synced(self, source: str, at: datetime) -> None:
        self._set_state("synced", source, at.astimezone(UTC).isoformat().replace("+00:00", "Z"))

    @staticmethod
    def _event_at(value: int) -> str:
        seconds, nanos = divmod(value, 1_000_000_000)
        moment = datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%S")
        fraction = f".{nanos:09d}".rstrip("0") if nanos else ""
        return f"{moment}{fraction}Z"

    def source_status(self) -> dict[str, dict[str, Any]]:
        state = {
            (row["kind"], row["source"]): row["value"]
            for row in self.client.query("SELECT kind,source,value FROM registry_state FINAL")
        }
        sources: dict[str, dict[str, Any]] = {}
        for name, condition in (
            ("trigger", "source='trigger_history'"),
            ("logbook", "startsWith(source,'logbook_')"),
        ):
            rows = self.client.query(
                "SELECT minOrNull(event_ns) AS first_ns,maxOrNull(event_ns) AS last_ns "
                f"FROM ({latest_records()}) WHERE {condition}"
            )
            values = rows[0] if rows else {}
            sources[name] = {
                "earliest": (
                    self._event_at(int(values["first_ns"])) if values.get("first_ns") else None
                ),
                "latest": self._event_at(int(values["last_ns"])) if values.get("last_ns") else None,
                "lastSyncedAt": state.get(("synced", name)),
            }
        point_range = self.client.query(
            "SELECT minOrNull(event_ns) AS first_ns,maxOrNull(event_ns) AS last_ns "
            "FROM detector_points"
        )
        values = point_range[0] if point_range else {}
        reverse_key = next(
            (
                key for kind, key in state
                if kind == "checkpoint" and key.startswith("influx-backfill-reverse:")
            ),
            None,
        )
        sources["influx"] = {
            "earliest": self._event_at(int(values["first_ns"])) if values.get("first_ns") else None,
            "latest": self._event_at(int(values["last_ns"])) if values.get("last_ns") else None,
            "lastSyncedAt": state.get(("synced", "influx")),
            "scannedThrough": state.get(("checkpoint", "influx")),
            "backfillCursor": state.get(("checkpoint", reverse_key)) if reverse_key else None,
            "backfillUpper": (
                state.get(("checkpoint", reverse_key.replace("-reverse:", "-upper:")))
                if reverse_key else None
            ),
            "backfillLastSyncedAt": state.get(("synced", "influx_backfill")),
        }
        return sources

    def get(self, identity: int) -> dict[str, Any] | None:
        rows = self.client.query(
            "SELECT * FROM collected_records FINAL WHERE id={identity:UInt64} LIMIT 1",
            {"identity": identity},
        )
        return self.document(rows[0]) if rows else None

    def records(
        self, source: str | None, page: int, page_size: int
    ) -> tuple[list[dict[str, Any]], int]:
        params: dict[str, str | int] = {"limit": page_size, "offset": (page - 1) * page_size}
        if source is not None:
            params["source"] = source
        latest = latest_records(source)
        total = self.client.query(f"SELECT count() AS total FROM ({latest})", params)[0]["total"]
        rows = self.client.query(
            f"SELECT * FROM ({latest}) ORDER BY event_ns DESC,id DESC "
            "LIMIT {limit:UInt32} OFFSET {offset:UInt64}",
            params,
        )
        return [self.document(row) for row in rows], int(total)

    def trigger_runs(self) -> list[dict[str, Any]]:
        rows = self.client.query(
            f"SELECT * FROM ({latest_records('trigger_history')}) "
            "WHERE event_ns IS NOT NULL ORDER BY event_ns,source_instance,source_id",
            {"source": "trigger_history"},
        )
        return [self.document(row) for row in rows]

    def beam_events(self) -> list[dict[str, Any]]:
        rows = self.client.query(
            f"SELECT * FROM ({latest_records('logbook_event')}) "
            "WHERE event_ns IS NOT NULL ORDER BY event_ns,id",
            {"source": "logbook_event"},
        )
        records = [self.document(row) for row in rows]
        return [
            record for record in records
            if record["raw"].get("category") == "Beam" and record["raw"].get("is_active", True)
        ]

    @staticmethod
    def _interval(start: str, end: str | None) -> tuple[str, dict[str, int]]:
        params = {"start": epoch_ns(start)}
        condition = "event_ns>={start:Int64}"
        if end is not None:
            params["end"] = epoch_ns(end)
            condition += " AND event_ns<{end:Int64}"
        return condition, params

    def influx_origin(self, start: str, end: str | None) -> dict[str, str] | None:
        condition, params = self._interval(start, end)
        rows = self.client.query(
            "SELECT source_instance,run_number FROM detector_points FINAL "
            f"WHERE {condition} LIMIT 1",
            params,
        )
        if not rows:
            return None
        return {
            "source_instance": rows[0]["source_instance"],
            "run_number": rows[0]["run_number"],
        }

    def iter_detector_points(self, start: str, end: str | None) -> Iterator[dict[str, Any]]:
        condition, params = self._interval(start, end)
        for row in self.client.iterate(
            f"SELECT point_json FROM detector_points FINAL WHERE {condition} "
            "ORDER BY event_ns,source_instance,source_id",
            params,
        ):
            yield json.loads(row["point_json"])
