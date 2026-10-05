from datetime import UTC, datetime

import pytest

from x17_registry.adapters.registry import RegistryStore
from x17_registry.application.polling import SourceRecord


def test_registry_survives_restart_with_raw_processed_and_checkpoint(store):
    record = SourceRecord(
        "influx",
        "test-influx",
        "point-1",
        {"_measurement": "run_80", "_field": "ADCA", "_value": "312"},
        "2026-09-27T14:00:00Z",
        "80",
    )
    assert store.save(record, dict(record.raw))
    store.set_checkpoint("influx", datetime(2026, 9, 27, 15, tzinfo=UTC).isoformat())
    reopened = RegistryStore(store.path, 5)
    reopened.initialize()
    assert reopened.checkpoint("influx") == "2026-09-27T15:00:00+00:00"
    points = list(reopened.iter_run("test-influx", "80"))
    assert len(points) == 1
    assert points[0]["raw"] == points[0]["processed"]
    assert not reopened.save(record, dict(record.raw))
    assert reopened.save(record, {**record.raw, "checked": True})
    assert reopened.records("influx", 1, 10)[0][0]["revision"] == 2
    assert reopened.run_stats() == [
        {
            "source_instance": "test-influx",
            "run_number": "80",
            "started_at": "2026-09-27T14:00:00Z",
            "point_count": 1,
        }
    ]


def test_rebuild_run_summaries_from_existing_records(store):
    for identity, event_time in (
        ("point-1", "2026-09-27T14:00:00Z"),
        ("point-2", "2026-09-27T14:01:00Z"),
    ):
        record = SourceRecord(
            "influx", "test-influx", identity, {"_value": identity}, event_time, "80"
        )
        store.save(record, dict(record.raw))
    with store.connect() as connection:
        connection.execute("DELETE FROM run_summaries")
    assert store.rebuild_run_summaries() == 1
    assert store.run_stats() == [
        {
            "source_instance": "test-influx",
            "run_number": "80",
            "started_at": "2026-09-27T14:00:00Z",
            "point_count": 2,
        }
    ]


def test_save_batch_deduplicates_and_updates_run_summary(store):
    records = [
        SourceRecord(
            "influx",
            "test-influx",
            f"point-{index}",
            {"_value": index},
            f"2026-09-27T14:0{index}:00Z",
            "80",
        )
        for index in range(3)
    ]
    batch = [(record, dict(record.raw)) for record in records]

    assert store.save_batch(batch) == 3
    assert store.save_batch(batch) == 0
    assert store.save_batch([(records[0], {"_value": 0, "checked": True})]) == 1
    assert store.run_stats()[0]["point_count"] == 3
    assert store.records("influx", 1, 10)[1] == 3


def test_save_batch_rolls_back_all_records_if_one_is_invalid(store):
    valid = SourceRecord(
        "influx", "test-influx", "valid", {"_value": 1}, "2026-09-27T14:00:00Z", "80"
    )
    invalid = SourceRecord(
        "influx", "test-influx", "invalid", {"_value": float("nan")}, "2026-09-27T14:01:00Z", "80"
    )

    with pytest.raises(ValueError):
        store.save_batch([(valid, dict(valid.raw)), (invalid, dict(invalid.raw))])

    assert store.records("influx", 1, 10)[1] == 0
    assert store.run_stats() == []


def test_detector_points_have_one_copy_and_legacy_overlap_is_not_duplicated(store):
    old = SourceRecord(
        "influx",
        "test-influx",
        "point-1",
        {"_time": "2026-09-27T14:00:00Z", "_value": "312"},
        "2026-09-27T14:00:00Z",
        "80",
    )
    new = SourceRecord(
        "influx",
        "test-influx",
        "point-2",
        {"_time": "2026-09-27T14:00:01Z", "_value": "313"},
        "2026-09-27T14:00:01Z",
        "80",
    )
    store.save(old, dict(old.raw))

    assert store.save_detector_batch([old, new]) == 1
    assert store.save_detector_batch([old, new]) == 0
    assert store.records("influx", 1, 10)[1] == 1
    assert [
        point["_value"] for point in store.iter_detector_points("2026-09-27T14:00:00Z", None)
    ] == ["312", "313"]
    assert store.run_stats()[0]["point_count"] == 2
    assert store.rebuild_run_summaries() == 1
    assert store.run_stats()[0]["point_count"] == 2


def test_detector_batch_rolls_back_on_invalid_point(store):
    valid = SourceRecord(
        "influx", "test-influx", "valid", {"_value": 1}, "2026-09-27T14:00:00Z", "80"
    )
    invalid = SourceRecord(
        "influx", "test-influx", "invalid", {"_value": float("nan")}, "2026-09-27T14:00:01Z", "80"
    )

    with pytest.raises(ValueError):
        store.save_detector_batch([valid, invalid])

    assert list(store.iter_detector_points("2026-09-27T14:00:00Z", None)) == []


def test_backfill_uses_wal_and_connection_local_sqlite_pragmas(monkeypatch, store):
    statements = []
    connect = store.connect

    def traced_connect():
        connection = connect()
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(store, "connect", traced_connect)
    point = SourceRecord(
        "influx", "test-influx", "point-1", {"_value": "312"}, "2026-09-27T14:00:00Z", "80"
    )
    assert store.save_detector_backfill_batch([point], "NORMAL") == 1
    assert "PRAGMA synchronous=NORMAL" in statements
    assert "PRAGMA temp_store=MEMORY" in statements
    with connect() as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    with pytest.raises(ValueError, match="Unsupported SQLite"):
        store.save_detector_backfill_batch([point], "invalid")


def test_new_detector_time_range_skips_per_point_legacy_lookups(monkeypatch, store):
    old = SourceRecord(
        "influx", "test-influx", "old", {"_value": "1"}, "2026-09-01T00:00:00Z", "80"
    )
    store.save(old, dict(old.raw))
    statements = []
    connect = store.connect

    def traced_connect():
        connection = connect()
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(store, "connect", traced_connect)
    new = SourceRecord(
        "influx", "test-influx", "new", {"_value": "2"}, "2026-10-05T00:00:00Z", "80"
    )
    assert store.save_detector_backfill_batch([new], "NORMAL") == 1
    assert any("SELECT 1 FROM collected_records" in statement for statement in statements)
    assert not any(
        "SELECT raw_json FROM collected_records" in statement for statement in statements
    )


def test_source_failure_does_not_block_other_sources(store):
    class BrokenReader:
        def read(self, checkpoint, now):
            raise ValueError("Bad source")

    class GoodReader:
        def read(self, checkpoint, now):
            return [SourceRecord("trigger_config", "local", "current", {"tmod": "ormaj"})], None

    from x17_registry.application.polling import IdentityProcessor, PollJob

    job = PollJob({"bad": BrokenReader(), "good": GoodReader()}, IdentityProcessor(), store, 1000)
    assert job.run_once() == {"bad": "error", "good": 1}
    assert store.records("trigger_config", 1, 10)[1] == 1
