import hashlib
import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from x17_registry.application.ports import RecordRepository
from x17_registry.domain.errors import RecordConflict
from x17_registry.domain.models import Receipt, RecordDraft, StoredRecord

SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
    id TEXT PRIMARY KEY,
    source_kind TEXT NOT NULL,
    source_instance TEXT NOT NULL,
    source_record_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    document TEXT NOT NULL,
    UNIQUE(source_kind, source_instance, source_record_id, content_hash),
    UNIQUE(source_kind, source_instance, source_record_id, revision)
);
"""


class SQLiteRecordRepository(RecordRepository):
    def __init__(self, path: Path, timeout_seconds: float) -> None:
        self.path = path
        self.timeout_seconds = timeout_seconds

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, timeout=self.timeout_seconds)

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection, connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(SCHEMA)

    def save(self, record: RecordDraft, *, mutable: bool) -> Receipt:

        content = record.model_dump(mode="json", exclude={"observed_at"})
        digest = hashlib.sha256(
            json.dumps(content, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest()
        key = (record.source_kind, record.source_instance, record.source_record_id)
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            previous = connection.execute(
                "SELECT id, revision FROM records WHERE source_kind=? AND source_instance=? "
                "AND source_record_id=? AND content_hash=?",
                (*key, digest),
            ).fetchone()
            if previous:
                return Receipt(id=previous[0], revision=previous[1], duplicate=True)
            latest = connection.execute(
                "SELECT MAX(revision) FROM records WHERE source_kind=? AND source_instance=? "
                "AND source_record_id=?",
                key,
            ).fetchone()[0]
            if latest and not mutable:
                raise RecordConflict(
                    "Immutable detector record ID was reused with different content."
                )
            stored = StoredRecord(
                **record.model_dump(),
                id=str(uuid4()),
                revision=(latest or 0) + 1,
                content_hash=digest,
                ingested_at=datetime.now(UTC),
            )
            connection.execute(
                "INSERT INTO records VALUES (?, ?, ?, ?, ?, ?, ?)",
                (stored.id, *key, stored.revision, digest, stored.model_dump_json()),
            )
        return Receipt(id=stored.id, revision=stored.revision, duplicate=False)

    def get(self, record_id: str) -> StoredRecord | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT document FROM records WHERE id=?", (record_id,)
            ).fetchone()
        return StoredRecord.model_validate_json(row[0]) if row else None
