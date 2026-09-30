from datetime import UTC, datetime

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


def test_source_failure_does_not_block_other_sources(store):
    class BrokenReader:
        def read(self, checkpoint, now):
            raise ValueError("Bad source")

    class GoodReader:
        def read(self, checkpoint, now):
            return [SourceRecord("trigger_config", "local", "current", {"tmod": "ormaj"})], None

    from x17_registry.application.polling import IdentityProcessor, PollJob

    job = PollJob({"bad": BrokenReader(), "good": GoodReader()}, IdentityProcessor(), store)
    assert job.run_once() == {"bad": "error", "good": 1}
    assert store.records("trigger_config", 1, 10)[1] == 1
