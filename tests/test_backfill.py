import io
from datetime import UTC, datetime, timedelta
from http.client import IncompleteRead

import pytest
from conftest import poll_settings

from x17_registry.adapters.readers import InfluxReader
from x17_registry.application.backfill import backfill_influx
from x17_registry.application.polling import SeekableInfluxReader, SourceRecord


class SparseReader(SeekableInfluxReader):
    def __init__(
        self, event_times: list[datetime], window_seconds: int, overlap_seconds: int
    ) -> None:
        self.event_times = event_times
        self.window_seconds = window_seconds
        self.overlap_seconds = overlap_seconds

    def read(self, checkpoint, now):
        start = (
            datetime.fromisoformat(checkpoint) - timedelta(seconds=self.overlap_seconds)
            if checkpoint
            else self.event_times[0] - timedelta(days=20)
        )
        stop = min(start + timedelta(seconds=self.window_seconds), now)
        return self.read_window(start, stop), stop.isoformat()

    def read_window(self, start, stop):
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
        return next((event for event in self.event_times if start <= event < stop), None)


class TruncatedReader(SparseReader):
    def read_window(self, start, stop):
        if (stop - start).total_seconds() > 10:
            yield from super().read_window(start, stop)[:1]
            raise IncompleteRead(b"")
        yield from super().read_window(start, stop)


def test_backfill_skips_empty_years_and_keeps_checkpoint(store):
    event_time = datetime.now(UTC) - timedelta(days=1)
    start = datetime(2016, 1, 29, 21, tzinfo=UTC)
    store.set_checkpoint("influx", start.isoformat())
    settings = poll_settings(
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
    reader = SparseReader([event_time], 300, 60)
    updates = []
    result = backfill_influx(reader, store, settings, 1, updates.append)

    assert result["skipped_gaps"] == 1
    assert result["windows"] < 5
    assert len(list(store.iter_interval(start.isoformat(), None))) == 1
    assert store.checkpoint("influx") == result["checkpoint"]
    assert any("skipped_to" in update for update in updates)


def test_backfill_imports_separate_periods_without_missing_points(store):
    first = datetime.now(UTC) - timedelta(days=3)
    second = first + timedelta(days=2)
    settings = poll_settings(
        store.path,
        influx_enabled=True,
        influx_url="http://example.invalid:8086",
        influx_org="UdeM",
        influx_bucket="vf48_test4",
        influx_token="test-token",
        influx_source_instance="test-influx",
        influx_start_at=first - timedelta(days=20),
        influx_window_seconds=300,
        influx_overlap_seconds=60,
    )
    result = backfill_influx(
        SparseReader([first, second], 300, 60), store, settings, 1, lambda _: None
    )
    assert result["skipped_gaps"] == 2
    assert len(list(store.iter_interval((first - timedelta(days=1)).isoformat(), None))) == 2


def test_backfill_retries_truncated_stream_in_smaller_windows(store):
    start = datetime.now(UTC) - timedelta(minutes=10)
    event_times = [start + timedelta(seconds=1), start + timedelta(seconds=200)]
    store.set_checkpoint("influx", start.isoformat())
    settings = poll_settings(
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
    updates = []
    backfill_influx(TruncatedReader(event_times, 300, 60), store, settings, 1, updates.append)
    assert any(update.get("window_seconds", 300) < 300 for update in updates)
    assert len(list(store.iter_interval(start.isoformat(), None))) == 2
    assert store.run_stats()[0]["point_count"] == 2


def test_next_point_query_returns_earliest_matching_time(monkeypatch, store):
    settings = poll_settings(
        store.path,
        influx_enabled=True,
        influx_url="http://example.invalid:8086",
        influx_org="UdeM",
        influx_bucket="vf48_test4",
        influx_token="test-token",
        influx_source_instance="test-influx",
        influx_start_at=datetime(2016, 1, 1, tzinfo=UTC),
    )
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
        assert "first()" in query
        assert 'min(column: "_time")' in query
        assert timeout == settings.influx_timeout_seconds
        return io.BytesIO(response)

    monkeypatch.setattr("x17_registry.adapters.readers.urlopen", fake_urlopen)
    found = InfluxReader(settings).next_point_at(
        datetime(2016, 1, 1, tzinfo=UTC), datetime(2026, 10, 1, tzinfo=UTC)
    )
    assert found == datetime(2026, 9, 29, 21, 0, 0, 599960, tzinfo=UTC)


def test_next_point_query_rejects_unexpected_response(monkeypatch, store):
    settings = poll_settings(
        store.path,
        influx_enabled=True,
        influx_url="http://example.invalid:8086",
        influx_org="UdeM",
        influx_bucket="vf48_test4",
        influx_token="test-token",
        influx_source_instance="test-influx",
        influx_start_at=datetime(2016, 1, 1, tzinfo=UTC),
    )
    monkeypatch.setattr(
        "x17_registry.adapters.readers.urlopen",
        lambda request, timeout: io.BytesIO(b",error\n,query failed\n"),
    )
    with pytest.raises(ValueError, match="no _time column"):
        InfluxReader(settings).next_point_at(
            datetime(2016, 1, 1, tzinfo=UTC), datetime(2026, 10, 1, tzinfo=UTC)
        )
