import hashlib
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from conftest import ROOT, poll_settings

from x17_registry.adapters.readers import LogbookReader, TriggerReader
from x17_registry.application.polling import IdentityProcessor, PollJob


def test_local_trigger_json_and_logbook_sqlite_are_read_only(store):
    source = ROOT / "x17-ops/x17-logbook/data/logbook.db"
    digest_before = hashlib.sha256(source.read_bytes()).hexdigest()
    settings = poll_settings(store.path)
    trigger, _ = TriggerReader(settings).read(None, datetime.now(UTC))
    logbook, _ = LogbookReader(settings).read(None, datetime.now(UTC))
    trigger_records = list(trigger)
    logbook_records = list(logbook)
    assert len([row for row in trigger_records if row.source == "trigger_history"]) == 200
    assert {row.source for row in trigger_records} == {
        "trigger_history",
        "trigger_config",
        "trigger_programmed",
    }
    assert len([row for row in logbook_records if row.source == "logbook_event"]) == 13
    assert {row.source for row in logbook_records} == {
        "logbook_event",
        "logbook_snapshot",
        "logbook_user",
    }
    assert hashlib.sha256(source.read_bytes()).hexdigest() == digest_before


def test_poll_stores_raw_and_processed_copies_without_duplicate_versions(store):
    settings = poll_settings(store.path)
    job = PollJob(
        {"trigger": TriggerReader(settings), "logbook": LogbookReader(settings)},
        IdentityProcessor(),
        store,
        1000,
    )
    first = job.run_once()
    assert first == {"trigger": 202, "logbook": 28}
    second = job.run_once()
    assert second == first
    status = store.source_status()
    assert status["trigger"]["lastSyncedAt"] is not None
    assert status["logbook"]["lastSyncedAt"] is not None
    assert status["influx"]["lastSyncedAt"] is None
    records, total = store.records("logbook_event", 1, 20)
    assert total == 13
    assert all(row["revision"] == 1 for row in records)
    assert all(row["raw"] == row["processed"] for row in records)
    beam = next(row for row in records if row["raw"]["category"] == "Beam")
    assert beam["raw"]["payload"] == {"status": "ON"}


def test_old_logbook_row_edit_creates_revision(store, tmp_path: Path):
    source = tmp_path / "logbook.db"
    with sqlite3.connect(source) as connection:
        connection.executescript(
            "CREATE TABLE users(id INTEGER PRIMARY KEY, display_name TEXT);"
            "CREATE TABLE condition_snapshots(id INTEGER PRIMARY KEY, valid_from TEXT, state TEXT);"
            "CREATE TABLE condition_events(id INTEGER PRIMARY KEY, public_id TEXT, timestamp TEXT,"
            "category TEXT, payload TEXT, comment TEXT, is_active INTEGER);"
        )
        connection.execute(
            "INSERT INTO condition_events VALUES(1,?,?,?,?,?,?)",
            ("Run4_B1", "2026-09-25 13:00:00", "Beam", '{"status":"ON"}', "", 1),
        )
    settings = poll_settings(store.path, logbook_database_path=source)
    job = PollJob({"logbook": LogbookReader(settings)}, IdentityProcessor(), store, 1000)
    assert job.run_once() == {"logbook": 1}
    with sqlite3.connect(source) as connection:
        connection.execute(
            "UPDATE condition_events SET comment=? WHERE id=1", ("Current I = 3 nA",)
        )
    assert job.run_once() == {"logbook": 1}
    records, count = store.records("logbook_event", 1, 10)
    assert count == 1
    assert records[0]["revision"] == 2
    assert records[0]["processed"]["comment"] == "Current I = 3 nA"
