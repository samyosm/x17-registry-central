import io
from datetime import UTC, datetime, timedelta
from http.client import IncompleteRead
from threading import Event
from time import sleep

import pytest
from conftest import poll_settings

from x17_registry.adapters.readers import InfluxReader
from x17_registry.application.backfill import backfill_influx
from x17_registry.application.polling import SeekableInfluxReader, SourceRecord
from x17_registry.config import BackfillSettings


class SparseReader(SeekableInfluxReader):
    def __init__(self, event_times: list[datetime]) -> None:
        self.event_times = event_times
        self.windows: list[tuple[datetime, datetime]] = []

    def read(self, checkpoint, now):
        raise NotImplementedError

    def read_window(self, start, stop):
        self.windows.append((start, stop))
        return [
            SourceRecord(
                "influx",
                "test-influx",
                f"point-{index}",
                {"_value": "42"},
                event_time.isoformat(),
                "80",
            )
            for index, event_time in enumerate(self.event_times)
            if start <= event_time < stop
        ]

    def next_point_at(self, start, stop):
        raise NotImplementedError

    def previous_point_before(self, start, stop):
        return next((event for event in reversed(self.event_times) if start <= event < stop), None)


class TruncatedReader(SparseReader):
    def read_window(self, start, stop):
        records = super().read_window(start, stop)
        if (stop - start).total_seconds() > 10:
            yield from records[:1]
            raise IncompleteRead(b"")
        yield from records


def settings_for(store, start, **overrides):
    settings = poll_settings(
        store.client.database,
        influx_enabled=True,
        influx_url="http://example.invalid:8086",
        influx_org="UdeM",
        influx_bucket="vf48_test4",
        influx_token="test-token",
        influx_source_instance="test-influx",
        influx_start_at=start,
        influx_window_seconds=300,
        influx_overlap_seconds=60,
    )
    values = {
        **settings.model_dump(),
        "influx_backfill_min_window_seconds": 1,
        "influx_backfill_progress_seconds": 10,
        "influx_backfill_pending_batches": 2,
        "influx_backfill_runs_only": False,
    }
    values.update(overrides)
    return BackfillSettings(_env_file=None, **values)


def test_reverse_backfill_starts_newest_and_skips_empty_years(store):
    older = datetime(2025, 9, 19, 22, tzinfo=UTC)
    newer = datetime.now(UTC) - timedelta(days=1)
    start = datetime(2016, 1, 29, 21, tzinfo=UTC)
    reader = SparseReader([older, newer])
    updates = []

    result = backfill_influx(reader, store, settings_for(store, start), updates.append)

    assert result["skipped_gaps"] == 2
    assert len(list(store.iter_detector_points(start.isoformat(), None))) == 2
    assert reader.windows[0][1] > newer
    assert reader.windows[0][0] > older
    assert store.checkpoint("influx-backfill-reverse:test-influx") == start.isoformat()
    assert store.checkpoint("influx") == store.checkpoint("influx-backfill-upper:test-influx")
    status = store.source_status()["influx"]
    assert status["backfillCursor"] == start.isoformat()
    assert status["backfillUpper"] == store.checkpoint("influx")
    assert status["backfillLastSyncedAt"] is not None
    assert {update["event"] for update in updates} >= {
        "started",
        "window_started",
        "window_completed",
        "skipped_gap",
        "completed",
    }
    assert any(update["percent_time_scanned"] == 100 for update in updates)


def test_reverse_backfill_retries_truncated_stream_without_duplicates(store):
    start = datetime.now(UTC) - timedelta(minutes=10)
    events = [start + timedelta(seconds=1), start + timedelta(seconds=200)]
    updates = []

    backfill_influx(TruncatedReader(events), store, settings_for(store, start), updates.append)

    assert any(update["event"] == "retry_smaller_window" for update in updates)
    assert len(list(store.iter_detector_points(start.isoformat(), None))) == 2


def test_reverse_backfill_resumes_after_failure(store):
    start = datetime.now(UTC) - timedelta(days=2)
    event = start + timedelta(days=1)
    settings = settings_for(store, start)

    with pytest.raises(RuntimeError, match="reverse checkpoint remains"):
        backfill_influx(
            TruncatedReader([event]),
            store,
            settings.model_copy(update={"influx_backfill_min_window_seconds": 300}),
            lambda _: None,
        )
    upper = store.checkpoint("influx-backfill-upper:test-influx")
    cursor = store.checkpoint("influx-backfill-reverse:test-influx")
    assert upper == cursor

    backfill_influx(SparseReader([event]), store, settings, lambda _: None)
    assert store.checkpoint("influx-backfill-upper:test-influx") == upper
    assert store.checkpoint("influx-backfill-reverse:test-influx") == start.isoformat()
    assert len(list(store.iter_detector_points(start.isoformat(), None))) == 1


def test_committed_writer_batch_is_deduplicated_after_stream_retry(store):
    start = datetime.now(UTC) - timedelta(minutes=10)
    event = datetime.now(UTC) - timedelta(seconds=30)
    settings = settings_for(
        store,
        start,
        influx_backfill_batch_size=1,
        influx_backfill_min_window_seconds=300,
    )

    with pytest.raises(RuntimeError, match="reverse checkpoint remains"):
        backfill_influx(TruncatedReader([event]), store, settings, lambda _: None)
    assert len(list(store.iter_detector_points(start.isoformat(), None))) == 1
    checkpoint = store.checkpoint("influx-backfill-reverse:test-influx")
    assert checkpoint is not None
    assert datetime.fromisoformat(checkpoint) > event

    backfill_influx(
        SparseReader([event]),
        store,
        settings.model_copy(update={"influx_backfill_min_window_seconds": 1}),
        lambda _: None,
    )
    assert len(list(store.iter_detector_points(start.isoformat(), None))) == 1


def test_reverse_backfill_reports_heartbeat_during_slow_query(store):
    start = datetime.now(UTC) - timedelta(minutes=10)
    heartbeat_seen = Event()

    class WaitingReader(SparseReader):
        def read_window(self, start, stop):
            assert heartbeat_seen.wait(1)
            return super().read_window(start, stop)

    def progress(update):
        if update["event"] == "heartbeat":
            assert update["phase"] == "reading_influx"
            assert update["phase_seconds"] >= 0
            heartbeat_seen.set()

    backfill_influx(
        WaitingReader([]),
        store,
        settings_for(store, start, influx_backfill_progress_seconds=0.01),
        progress,
    )
    assert heartbeat_seen.is_set()


def test_reverse_backfill_reports_clickhouse_time(monkeypatch, store):
    start = datetime.now(UTC) - timedelta(minutes=10)
    event = start + timedelta(seconds=200)
    save = store.save_detector_backfill_batch
    updates = []

    class SlowReader(SparseReader):
        def read_window(self, start, stop):
            for record in super().read_window(start, stop):
                sleep(0.02)
                yield record

    def slow_save(records):
        sleep(0.02)
        return save(records)

    monkeypatch.setattr(store, "save_detector_backfill_batch", slow_save)
    backfill_influx(
        SlowReader([event]),
        store,
        settings_for(store, start, influx_backfill_batch_size=1),
        updates.append,
    )

    completed = next(
        update
        for update in updates
        if update["event"] == "window_completed" and update["window_records"]
    )
    assert completed["clickhouse_seconds"] >= 0.02
    assert completed["influx_seconds"] >= 0.02


def test_run_limited_backfill_skips_points_before_trigger_history(store):
    first_run = datetime.now(UTC) - timedelta(days=1)
    old_point = first_run - timedelta(days=30)
    recent_point = first_run + timedelta(seconds=1)
    store.save(
        SourceRecord(
            "trigger_history", "test-trigger", "first", {"id": "first"}, first_run.isoformat()
        ),
        {"id": "first"},
    )
    reader = SparseReader([old_point, recent_point])
    updates = []
    settings = settings_for(store, old_point - timedelta(days=1), influx_backfill_runs_only=True)

    backfill_influx(reader, store, settings, updates.append)

    assert updates[0]["oldest_needed"] == first_run.isoformat()
    assert all(window_start >= first_run for window_start, _ in reader.windows)
    assert len(list(store.iter_detector_points(first_run.isoformat(), None))) == 1


def test_run_limited_backfill_requires_trigger_history(store):
    settings = settings_for(
        store, datetime.now(UTC) - timedelta(days=2), influx_backfill_runs_only=True
    )
    with pytest.raises(ValueError, match="TriggerApp runs must be imported"):
        backfill_influx(SparseReader([]), store, settings, lambda _: None)


def test_reader_and_writer_overlap_without_unbounded_queue(monkeypatch, store):
    start = datetime.now(UTC) - timedelta(minutes=10)
    events = [start + timedelta(seconds=200), start + timedelta(seconds=201)]
    writer_started = Event()
    save = store.save_detector_backfill_batch

    class OverlapReader(SparseReader):
        def read_window(self, start, stop):
            for index, record in enumerate(super().read_window(start, stop)):
                if index:
                    assert writer_started.wait(1)
                yield record

    def tracked_save(records):
        writer_started.set()
        return save(records)

    monkeypatch.setattr(store, "save_detector_backfill_batch", tracked_save)
    settings = settings_for(
        store, start, influx_backfill_batch_size=1, influx_backfill_pending_batches=2
    )
    backfill_influx(OverlapReader(events), store, settings, lambda _: None)

    assert writer_started.is_set()
    assert len(list(store.iter_detector_points(start.isoformat(), None))) == 2


def test_writer_failure_keeps_reverse_checkpoint(monkeypatch, store):
    start = datetime.now(UTC) - timedelta(minutes=10)
    event = datetime.now(UTC) - timedelta(seconds=30)
    settings = settings_for(store, start, influx_backfill_batch_size=1)

    def broken_save(records):
        raise OSError("disk unavailable")

    monkeypatch.setattr(store, "save_detector_backfill_batch", broken_save)
    with pytest.raises(RuntimeError, match="ClickHouse backfill batch failed"):
        backfill_influx(SparseReader([event]), store, settings, lambda _: None)

    assert store.checkpoint("influx-backfill-reverse:test-influx") == store.checkpoint(
        "influx-backfill-upper:test-influx"
    )


def test_previous_point_query_uses_last_and_max(monkeypatch, store):
    settings = settings_for(store, datetime(2016, 1, 1, tzinfo=UTC))
    response = (
        b"#group,false,false,true\n"
        b"#datatype,string,long,dateTime:RFC3339\n"
        b"#default,,,\n"
        b",result,table,_time\n"
        b",,0,2026-09-29T21:00:00.599960Z\n"
    )

    def fake_urlopen(request, timeout):
        query = request.data.decode()
        assert "range(start: 2016-01-01T00:00:00Z" in query
        assert "last()" in query
        assert 'r._measurement == "run_80"' in query
        assert 'max(column: "_time")' in query
        assert timeout == settings.influx_timeout_seconds
        return io.BytesIO(response)

    monkeypatch.setattr("x17_registry.adapters.readers.urlopen", fake_urlopen)
    found = InfluxReader(settings).previous_point_before(
        datetime(2016, 1, 1, tzinfo=UTC), datetime(2026, 10, 1, tzinfo=UTC)
    )
    assert found == datetime(2026, 9, 29, 21, 0, 0, 599960, tzinfo=UTC)


def test_point_time_query_rejects_unexpected_response(monkeypatch, store):
    settings = settings_for(store, datetime(2016, 1, 1, tzinfo=UTC))
    monkeypatch.setattr(
        "x17_registry.adapters.readers.urlopen",
        lambda request, timeout: io.BytesIO(b",error\n,query failed\n"),
    )
    with pytest.raises(ValueError, match="no _time column"):
        InfluxReader(settings).previous_point_before(
            datetime(2016, 1, 1, tzinfo=UTC), datetime(2026, 10, 1, tzinfo=UTC)
        )
