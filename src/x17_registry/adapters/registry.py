import hashlib
import json
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from x17_registry.application.polling import SourceRecord

SCHEMA = """
CREATE TABLE IF NOT EXISTS collected_records (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    source_instance TEXT NOT NULL,
    source_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    event_time TEXT,
    run_number TEXT,
    raw_json TEXT NOT NULL,
    processed_json TEXT NOT NULL,
    collected_at TEXT NOT NULL,
    UNIQUE(source, source_instance, source_id, revision)
);
CREATE INDEX IF NOT EXISTS collected_source
ON collected_records(source, source_instance, source_id, revision);
CREATE INDEX IF NOT EXISTS collected_run ON collected_records(source, run_number, event_time);
CREATE INDEX IF NOT EXISTS collected_event_time ON collected_records(source, event_time);
CREATE TABLE IF NOT EXISTS poll_checkpoints (
    source TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS run_summaries (
    source_instance TEXT NOT NULL,
    run_number TEXT NOT NULL,
    started_at TEXT,
    point_count INTEGER NOT NULL,
    PRIMARY KEY (source_instance, run_number)
);
"""

LATEST = """
SELECT r.* FROM collected_records r
JOIN (
    SELECT source, source_instance, source_id, MAX(revision) AS revision
    FROM collected_records GROUP BY source, source_instance, source_id
) latest
ON r.source=latest.source AND r.source_instance=latest.source_instance
AND r.source_id=latest.source_id AND r.revision=latest.revision
"""


class RegistryStore:
    def __init__(self, path: Path, timeout_seconds: float) -> None:
        self.path = path
        self.timeout_seconds = timeout_seconds

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=self.timeout_seconds)
        connection.row_factory = sqlite3.Row
        return connection

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self.connect()) as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(SCHEMA)
            connection.commit()

    def save(self, record: SourceRecord, processed: dict[str, Any]) -> bool:
        raw_json = json.dumps(record.raw, sort_keys=True, allow_nan=False)
        processed_json = json.dumps(processed, sort_keys=True, allow_nan=False)
        digest = hashlib.sha256(f"{raw_json}\0{processed_json}".encode()).hexdigest()
        key = (record.source, record.source_instance, record.source_id)
        with closing(self.connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            previous = connection.execute(
                "SELECT revision, content_hash, event_time, run_number "
                "FROM collected_records WHERE source=? "
                "AND source_instance=? AND source_id=? ORDER BY revision DESC LIMIT 1",
                key,
            ).fetchone()
            if previous is not None and previous["content_hash"] == digest:
                return False
            revision = previous["revision"] + 1 if previous is not None else 1
            connection.execute(
                "INSERT INTO collected_records (source,source_instance,source_id,revision,"
                "content_hash,event_time,run_number,raw_json,processed_json,collected_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    *key,
                    revision,
                    digest,
                    record.event_time,
                    record.run_number,
                    raw_json,
                    processed_json,
                    datetime.now(UTC).isoformat(),
                ),
            )
            if record.source == "influx" and record.run_number is not None:
                if previous is None:
                    connection.execute(
                        "INSERT INTO run_summaries "
                        "(source_instance,run_number,started_at,point_count) VALUES(?,?,?,1) "
                        "ON CONFLICT(source_instance,run_number) DO UPDATE SET "
                        "started_at=MIN(started_at,excluded.started_at), "
                        "point_count=point_count+1",
                        (record.source_instance, record.run_number, record.event_time),
                    )
                elif (
                    previous["event_time"] != record.event_time
                    or previous["run_number"] != record.run_number
                ):
                    self._refresh_run_summary(
                        connection, record.source_instance, previous["run_number"]
                    )
                    self._refresh_run_summary(connection, record.source_instance, record.run_number)
        return True

    @staticmethod
    def _refresh_run_summary(
        connection: sqlite3.Connection, source_instance: str, run_number: str | None
    ) -> None:
        if run_number is None:
            return
        connection.execute(
            "DELETE FROM run_summaries WHERE source_instance=? AND run_number=?",
            (source_instance, run_number),
        )
        connection.execute(
            "INSERT INTO run_summaries (source_instance,run_number,started_at,point_count) "
            "SELECT source_instance,run_number,MIN(event_time),COUNT(*) "
            "FROM collected_records AS r WHERE source='influx' AND source_instance=? "
            "AND run_number=? AND NOT EXISTS ("
            "SELECT 1 FROM collected_records AS newer WHERE newer.source=r.source "
            "AND newer.source_instance=r.source_instance AND newer.source_id=r.source_id "
            "AND newer.revision>r.revision) GROUP BY source_instance,run_number",
            (source_instance, run_number),
        )

    def rebuild_run_summaries(self) -> int:
        with closing(self.connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("DELETE FROM run_summaries")
            connection.execute(
                "INSERT INTO run_summaries "
                "(source_instance,run_number,started_at,point_count) "
                "SELECT r.source_instance,r.run_number,MIN(r.event_time),COUNT(*) "
                "FROM collected_records AS r WHERE r.source='influx' "
                "AND r.run_number IS NOT NULL AND NOT EXISTS ("
                "SELECT 1 FROM collected_records AS newer WHERE newer.source=r.source "
                "AND newer.source_instance=r.source_instance AND newer.source_id=r.source_id "
                "AND newer.revision>r.revision) "
                "GROUP BY r.source_instance,r.run_number"
            )
            return int(connection.execute("SELECT COUNT(*) FROM run_summaries").fetchone()[0])

    def checkpoint(self, source: str) -> str | None:
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT value FROM poll_checkpoints WHERE source=?", (source,)
            ).fetchone()
        return row["value"] if row else None

    def set_checkpoint(self, source: str, value: str) -> None:
        with closing(self.connect()) as connection, connection:
            connection.execute(
                "INSERT INTO poll_checkpoints(source,value) VALUES(?,?) "
                "ON CONFLICT(source) DO UPDATE SET value=excluded.value",
                (source, value),
            )

    @staticmethod
    def document(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["raw"] = json.loads(result.pop("raw_json"))
        result["processed"] = json.loads(result.pop("processed_json"))
        return result

    def get(self, record_id: int) -> dict[str, Any] | None:
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT * FROM collected_records WHERE id=?", (record_id,)
            ).fetchone()
        return self.document(row) if row else None

    def records(
        self, source: str | None, page: int, page_size: int
    ) -> tuple[list[dict[str, Any]], int]:
        where = " WHERE r.source=?" if source else ""
        params: tuple[Any, ...] = (source,) if source else ()
        with closing(self.connect()) as connection:
            count = connection.execute(
                f"SELECT COUNT(*) FROM ({LATEST}{where})", params
            ).fetchone()[0]
            rows = connection.execute(
                f"{LATEST}{where} ORDER BY r.id DESC LIMIT ? OFFSET ?",
                (*params, page_size, (page - 1) * page_size),
            ).fetchall()
        return [self.document(row) for row in rows], count

    def run_stats(self) -> list[dict[str, Any]]:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                "SELECT source_instance,run_number,started_at,point_count FROM run_summaries"
            ).fetchall()
        return [dict(row) for row in rows]

    def trigger_runs(self) -> list[dict[str, Any]]:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                "SELECT r.* FROM collected_records AS r WHERE r.source='trigger_history' "
                "AND r.event_time IS NOT NULL AND NOT EXISTS ("
                "SELECT 1 FROM collected_records AS newer WHERE newer.source=r.source "
                "AND newer.source_instance=r.source_instance AND newer.source_id=r.source_id "
                "AND newer.revision>r.revision) "
                "ORDER BY r.event_time,r.source_instance,r.source_id"
            ).fetchall()
        return [self.document(row) for row in rows]

    @staticmethod
    def _interval(start: str, end: str | None) -> tuple[str, tuple[str, ...]]:
        condition = "r.event_time>=?"
        parameters: tuple[str, ...] = (start.removesuffix("Z"),)
        if end is not None:
            condition += " AND r.event_time<?"
            parameters += (end.removesuffix("Z"),)
        return condition, parameters

    def influx_origin(self, start: str, end: str | None) -> dict[str, str] | None:
        condition, parameters = self._interval(start, end)
        with closing(self.connect()) as connection:
            row = connection.execute(
                f"SELECT r.source_instance,r.run_number FROM collected_records AS r "
                f"WHERE r.source='influx' "
                f"AND {condition} AND NOT EXISTS ("
                "SELECT 1 FROM collected_records AS newer WHERE newer.source=r.source "
                "AND newer.source_instance=r.source_instance AND newer.source_id=r.source_id "
                "AND newer.revision>r.revision) LIMIT 1",
                parameters,
            ).fetchone()
        return dict(row) if row is not None else None

    def iter_interval(self, start: str, end: str | None) -> Iterator[dict[str, Any]]:
        condition, parameters = self._interval(start, end)
        with closing(self.connect()) as connection:
            cursor = connection.execute(
                "SELECT r.* FROM collected_records AS r WHERE r.source='influx' "
                f"AND {condition} AND NOT EXISTS ("
                "SELECT 1 FROM collected_records AS newer WHERE newer.source=r.source "
                "AND newer.source_instance=r.source_instance AND newer.source_id=r.source_id "
                "AND newer.revision>r.revision) ORDER BY r.event_time,r.id",
                parameters,
            )
            for row in cursor:
                yield self.document(row)

    def iter_run(self, source_instance: str, run_number: str) -> Iterator[dict[str, Any]]:
        with closing(self.connect()) as connection:
            cursor = connection.execute(
                f"{LATEST} WHERE r.source='influx' AND r.source_instance=? "
                "AND r.run_number=? "
                "ORDER BY r.event_time,r.id",
                (source_instance, run_number),
            )
            for row in cursor:
                yield self.document(row)
