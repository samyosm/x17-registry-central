import pytest
from embedded_clickhouse import EmbeddedClickHouse

from x17_registry.adapters.registry import RegistryStore
from x17_registry.application.polling import SourceRecord

chdb = pytest.importorskip("chdb")


def test_registry_queries_on_embedded_clickhouse():
    client = EmbeddedClickHouse(chdb)
    try:
        store = RegistryStore(client)
        store.initialize()
        first = SourceRecord(
            "trigger_history", "trigger-162", "one",
            {"title": "First"}, "2026-09-27T14:00:00Z",
        )
        second = SourceRecord(
            "trigger_history", "trigger-162", "two",
            {"title": "Second"}, "2026-09-27T15:00:00Z",
        )
        beam = SourceRecord(
            "logbook_event", "logbook-162", "b1",
            {"category": "Beam", "is_active": True, "payload": {"status": "ON"}},
            "2026-09-27T14:30:00Z",
        )
        assert store.save_batch([(first, first.raw), (second, second.raw), (beam, beam.raw)]) == 3
        assert store.save_batch([(first, first.raw), (second, second.raw), (beam, beam.raw)]) == 0
        assert len(store.trigger_runs()) == 2
        assert len(store.beam_events()) == 1
        assert store.records("logbook_event", 1, 5)[1] == 1
        point = SourceRecord(
            "influx", "vf48-59", "point-1", {"_value": "312"},
            "2026-09-27T14:40:00.123456789Z", "80",
        )
        store.save_detector_batch([point, point])
        assert store.influx_origin(first.event_time, second.event_time) == {
            "source_instance": "vf48-59",
            "run_number": "80",
        }
        assert list(store.iter_detector_points(first.event_time, second.event_time)) == [point.raw]
        assert store.source_status()["influx"]["earliest"] == point.event_time
        store.set_checkpoint("influx", point.event_time)
        assert store.checkpoint("influx") == point.event_time
    finally:
        client.close()
