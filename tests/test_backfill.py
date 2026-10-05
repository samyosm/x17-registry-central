import io
from datetime import UTC, datetime, timedelta
from http.client import IncompleteRead
from threading import Event

import pytest
from conftest import poll_settings

from x17_registry.adapters.readers import InfluxReader
from x17_registry.application.backfill import backfill_influx
from x17_registry.application.polling import SeekableInfluxReader, SourceRecord


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


def settings_for(store, start):
    return poll_settings(
        store.path,
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


def test_reverse_backfill_starts_newest_and_skips_empty_years(store):
    older = datetime(2025, 9, 19, 22, tzinfo=UTC)
    newer = datetime.now(UTC) - timedelta(days=1)
    start = datetime(2016, 1, 29, 21, tzinfo=UTC)
    reader = SparseReader([older, newer])
    updates = []

    result = backfill_influx(reader, store, settings_for(store, start), 1, 10, 2, updates.append)

    assert result["skipped_gaps"] == 2
    assert len(list(store.iter_detector_points(start.isoformat(), None))) == 2
    assert reader.windows[0][1] > newer
    assert reader.windows[0][0] > older
    assert store.checkpoint("influx-backfill-reverse:test-influx") == start.isoformat()
    assert store.checkpoint("influx") == store.checkpoint("influx-backfill-upper:test-influx")
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

    backfill_influx(
        TruncatedReader(events), store, settings_for(store, start), 1, 10, 2, updates.append
    )

    assert any(update["event"] == "retry_smaller_window" for update in updates)
    assert len(list(store.iter_detector_points(start.isoformat(), None))) == 2
    assert store.run_stats()[0]["point_count"] == 2


def test_reverse_backfill_resumes_after_failure(store):
    start = datetime.now(UTC) - timedelta(days=2)
    event = start + timedelta(days=1)
    settings = settings_for(store, start)

    with pytest.raises(RuntimeError, match="reverse checkpoint remains"):
        backfill_influx(TruncatedReader([event]), store, settings, 300, 10, 2, lambda _: None)
    upper = store.checkpoint("influx-backfill-upper:test-influx")
    cursor = store.checkpoint("influx-backfill-reverse:test-influx")
    assert upper == cursor

    backfill_influx(SparseReader([event]), store, settings, 1, 10, 2, lambda _: None)
    assert store.checkpoint("influx-backfill-upper:test-influx") == upper
    assert store.checkpoint("influx-backfill-reverse:test-influx") == start.isoformat()
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
            heartbeat_seen.set()

    backfill_influx(WaitingReader([]), store, settings_for(store, start), 1, 0.01, 2, progress)
    assert heartbeat_seen.is_set()


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
