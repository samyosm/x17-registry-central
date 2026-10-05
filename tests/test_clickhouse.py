import io
import json
from urllib.parse import parse_qs, urlparse

from x17_registry.adapters.clickhouse import ClickHouseClient
from x17_registry.adapters.registry import RegistryStore, epoch_ns
from x17_registry.application.polling import SourceRecord


class CaptureClient:
    database = "x17_test"

    def __init__(self):
        self.inserts = []
        self.queries = []
        self.statements = []
        self.previous = []

    def execute(self, sql, params=None, database=None):
        self.statements.append((sql, database))

    def query(self, sql, params=None):
        self.queries.append((sql, params))
        if "SELECT source,source_instance,source_id,revision,content_hash" in sql:
            return self.previous
        if "FROM registry_state FINAL" in sql and "WHERE" in sql:
            return []
        return []

    def insert(self, table, rows):
        self.inserts.append((table, list(rows)))

    def iterate(self, sql, params=None):
        self.queries.append((sql, params))
        return iter(())


def test_clickhouse_http_request_uses_parameters_and_auth(monkeypatch):
    requests = []

    def respond(request, timeout):
        requests.append((request, timeout))
        return io.BytesIO(b'{"value":"ok"}\n')

    monkeypatch.setattr("x17_registry.adapters.clickhouse.urlopen", respond)
    client = ClickHouseClient("http://127.0.0.1:8123/", "x17_registry", "reader", "secret", 5)
    assert client.query("SELECT {value:String} AS value", {"value": "ok"}) == [{"value": "ok"}]
    request, timeout = requests[0]
    query = parse_qs(urlparse(request.full_url).query)
    assert query["database"] == ["x17_registry"]
    assert query["param_value"] == ["ok"]
    assert request.get_header("Authorization").startswith("Basic ")
    assert "secret" not in request.full_url
    assert timeout == 5


def test_clickhouse_bulk_insert_uses_json_each_row(monkeypatch):
    requests = []

    def respond(request, timeout):
        requests.append(request)
        return io.BytesIO(b"")

    monkeypatch.setattr("x17_registry.adapters.clickhouse.urlopen", respond)
    client = ClickHouseClient("http://127.0.0.1:8123", "x17_registry", "writer", "", 5)
    client.insert("detector_points", [{"source_id": "a"}, {"source_id": "b"}])
    assert parse_qs(urlparse(requests[0].full_url).query)["query"] == [
        "INSERT INTO detector_points FORMAT JSONEachRow"
    ]
    assert [json.loads(line) for line in requests[0].data.splitlines()] == [
        {"source_id": "a"},
        {"source_id": "b"},
    ]


def test_registry_schema_and_detector_query_use_replacing_final():
    client = CaptureClient()
    store = RegistryStore(client)
    store.initialize()
    assert client.statements[0][1] == "default"
    assert all("ReplacingMergeTree" in sql for sql, _ in client.statements[1:])
    store.influx_origin("2026-09-27T14:00:00Z", "2026-09-27T15:00:00Z")
    assert "detector_points FINAL" in client.queries[-1][0]
    assert client.queries[-1][1]["end"] - client.queries[-1][1]["start"] == 3_600_000_000_000


def test_detector_batch_preserves_nanosecond_time_and_raw_json():
    client = CaptureClient()
    store = RegistryStore(client)
    record = SourceRecord(
        "influx", "vf48-59", '["run_80","t",2,"ADCA"]',
        {"_field": "ADCA", "_value": "312"},
        "2026-09-27T14:00:00.123456789Z", "80",
    )
    assert store.save_detector_batch([record]) == 1
    table, rows = client.inserts[0]
    assert table == "detector_points"
    assert rows[0]["event_ns"] == epoch_ns(record.event_time)
    assert rows[0]["point_json"] == '{"_field": "ADCA", "_value": "312"}'


def test_trigger_save_preserves_raw_and_processed_payloads():
    client = CaptureClient()
    store = RegistryStore(client)
    record = SourceRecord(
        "trigger_history", "trigger-162", "entry-1",
        {"title": "Cosmics"}, "2026-09-27T14:00:00Z",
    )
    assert store.save(record, {"title": "Cosmics"}) is True
    row = client.inserts[0][1][0]
    assert row["revision"] == 1
    assert json.loads(row["raw_json"]) == record.raw
    assert json.loads(row["processed_json"]) == record.raw
    assert row["event_ns"] == epoch_ns(record.event_time)


def test_source_batch_inserts_only_changed_revisions():
    client = CaptureClient()
    store = RegistryStore(client)
    first = SourceRecord("trigger_history", "trigger-162", "one", {"title": "First"})
    second = SourceRecord("trigger_history", "trigger-162", "two", {"title": "Second"})
    assert store.save_batch([(first, first.raw), (second, second.raw)]) == 2
    assert len(client.inserts) == 1
    assert len(client.inserts[0][1]) == 2
    rows = client.inserts[0][1]
    client.previous = [
        {
            key: row[key]
            for key in ("source", "source_instance", "source_id", "revision", "content_hash")
        }
        for row in rows
    ]
    assert store.save_batch([(first, first.raw), (second, second.raw)]) == 0
    assert len(client.inserts) == 1


def test_clickhouse_round_trip_when_test_server_is_configured(store):
    first = SourceRecord(
        "trigger_history", "trigger-test", "first",
        {"title": "Calibration"}, "2026-09-27T14:00:00Z",
    )
    assert store.save(first, first.raw)
    assert not store.save(first, first.raw)
    assert store.trigger_runs()[0]["raw"]["title"] == "Calibration"
    store.set_checkpoint("trigger", "2026-09-27T14:00:00Z")
    assert store.checkpoint("trigger") == "2026-09-27T14:00:00Z"
    point = SourceRecord(
        "influx", "vf48-test", "point-1", {"_value": "312"},
        "2026-09-27T14:30:00Z", "80",
    )
    store.save_detector_batch([point, point])
    assert list(store.iter_detector_points("2026-09-27T14:00:00Z", None)) == [point.raw]
