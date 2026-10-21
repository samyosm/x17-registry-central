from datetime import datetime, timedelta
from http.client import IncompleteRead
from itertools import count

from x17_registry.application.polling import (
    AttemptDeadlineExceeded,
    IdentityProcessor,
    PollJob,
    SourceReader,
    SourceRecord,
)


class WindowedReader(SourceReader):
    """Reads one window per call; fails while the window is wider than `limit` seconds."""

    def __init__(self, limit: float, width: float = 3600, rows: int = 3) -> None:
        self.limit, self.width, self.rows = limit, width, rows
        self.window = None
        self.calls = 0
        self.ids = count()

    def read(self, checkpoint, now):
        self.calls += 1
        start = datetime.fromisoformat(checkpoint) if checkpoint else now - timedelta(days=1)
        stop = min(start + timedelta(seconds=self.width), now)
        self.window = (start, stop)
        too_wide = self.width > self.limit

        def records():
            for index in range(self.rows):
                yield SourceRecord(
                    "influx", "i", f"p{next(self.ids)}", {"v": index},
                    (start + timedelta(seconds=index)).isoformat(), "80",
                )
                if too_wide and index == 0:
                    raise IncompleteRead(b"")

        return records(), stop.isoformat()

    def shrink_window(self):
        if self.width <= 60:
            return False
        self.width /= 2
        return True

    def grow_window(self):
        self.width = min(self.width * 1.25, 3600)

    def current_window(self):
        return self.window


def job_for(store, reader, **kwargs):
    return PollJob({"influx": reader}, IdentityProcessor(), store, 100, **kwargs)


def test_poll_shrinks_window_and_records_status(store):
    reader = WindowedReader(limit=900)
    job = job_for(store, reader)

    assert job.run_once()["influx"] == 3

    status = store.statuses()["influx"]
    assert status["state"] == "catching_up"
    assert status["retries"] == 2
    assert status["consecutiveFailures"] == 0
    assert "IncompleteRead" in status["lastRetryReason"]
    assert status["lastWindow"] is not None
    assert "influx" in job.behind
    assert store.checkpoint("influx") is not None
    synced = store.source_status()["influx"]["lastSyncedAt"]
    assert status["lastSuccessAt"] == synced


def test_failure_is_recorded_without_advancing_checkpoint(store):
    reader = WindowedReader(limit=1, width=60)
    job = job_for(store, reader)

    assert job.run_once() == {"influx": "error"}
    assert job.run_once() == {"influx": "error"}

    status = store.statuses()["influx"]
    assert status["state"] == "failing"
    assert status["consecutiveFailures"] == 2
    assert status["lastError"]["type"] == "IncompleteRead"
    assert store.checkpoint("influx") is None
    assert store.source_status()["influx"]["lastSyncedAt"] is None


def test_attempt_deadline_counts_as_stream_failure(store, monkeypatch):
    reader = WindowedReader(limit=10_000, rows=5)
    job = job_for(store, reader, attempt_seconds=1)
    ticks = count(0, 10)
    monkeypatch.setattr("x17_registry.application.polling.monotonic", lambda: next(ticks))

    assert job.run_once() == {"influx": "error"}

    assert store.statuses()["influx"]["lastError"]["type"] == AttemptDeadlineExceeded.__name__
    assert reader.width < 3600


def test_heartbeat_and_catching_up_cleared_once_current(store):
    reader = WindowedReader(limit=10_000, width=3600)
    job = job_for(store, reader, interval_seconds=30)
    job.heartbeat("polling")
    for _ in range(40):
        job.run_once(job.behind or None)
        if not job.behind:
            break
    assert not job.behind
    assert store.statuses()["influx"]["state"] == "idle"
    beat = store.statuses()["poller"]
    assert beat["state"] == "polling" and beat["intervalSeconds"] == 30


def test_clear_and_list_checkpoints(store):
    store.set_checkpoint("influx", "2026-10-09T19:13:28+00:00")
    store.set_checkpoint("influx-backfill-reverse:i", "2026-07-29T17:43:20+00:00")
    store.set_checkpoint("influx-backfill-upper:i", "2026-10-05T21:26:25+00:00")

    assert [c["name"] for c in store.checkpoints()] == [
        "influx", "influx-backfill-reverse:i", "influx-backfill-upper:i",
    ]
    assert sorted(store.clear_checkpoints("influx-backfill-")) == [
        "influx-backfill-reverse:i", "influx-backfill-upper:i",
    ]
    assert store.checkpoint("influx-backfill-reverse:i") is None
    assert [c["name"] for c in store.checkpoints()] == ["influx"]
    assert "backfillCursor" in store.source_status()["influx"]
    assert store.source_status()["influx"]["backfillCursor"] is None
    assert store.clear_checkpoints("influx-backfill-") == []


def test_diagnostics_exposes_status_and_checkpoints(client, store):
    store.set_checkpoint("influx", "2026-10-09T19:13:28+00:00")
    store.set_status("poller", {"state": "waiting"})
    store.set_status("influx", {"state": "failing"})
    store.set_status("influx_backfill", {"state": "running"})

    body = client.get("/api/v1/diagnostics").json()

    assert body["poller"] == {"state": "waiting"}
    assert body["backfill"] == {"state": "running"}
    assert body["sources"]["influx"]["poll"] == {"state": "failing"}
    assert body["sources"]["trigger"]["poll"] is None
    assert body["checkpoints"][0]["name"] == "influx"
    assert body["checkpoints"][0]["updatedAt"].endswith("Z")
    assert body["serverTime"].endswith("Z")
