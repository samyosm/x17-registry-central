import io
from datetime import UTC, datetime

import pytest
from conftest import poll_settings
from pydantic import SecretStr

from x17_registry.adapters.readers import InfluxReader, flux_csv_rows
from x17_registry.application.polling import IdentityProcessor, PollJob

CSV = """#datatype,string,long,dateTime:RFC3339,string,string,string,long
#group,false,false,false,true,true,true,false
#default,_result,,,,,,
,result,table,_time,_measurement,VF48_num,_field,_value
,,0,2026-09-27T14:00:00Z,run_80,2,trignum,15
,,0,2026-09-27T14:00:00Z,run_80,2,ADCA,312
"""


def test_influx_annotated_csv_and_checkpoint(monkeypatch, store):
    requests = []

    def local_query(request, timeout):
        requests.append(request)
        return io.BytesIO(CSV.encode())

    monkeypatch.setattr("x17_registry.adapters.readers.urlopen", local_query)
    settings = poll_settings(
        store.path,
        influx_enabled=True,
        influx_url="http://127.0.0.1:18888",
        influx_org="UdeM",
        influx_bucket="vf48_test4",
        influx_token=SecretStr("local-test-token"),
        influx_source_instance="test-influx",
        influx_start_at=datetime(2026, 9, 27, 13, tzinfo=UTC),
    )
    job = PollJob({"influx": InfluxReader(settings)}, IdentityProcessor(), store, 1)
    assert job.run_source("influx", datetime(2026, 9, 27, 15, tzinfo=UTC)) == 2
    assert store.checkpoint("influx") == "2026-09-27T14:00:00+00:00"
    assert requests[0].full_url.endswith("/api/v2/query?org=UdeM")
    assert requests[0].get_header("Authorization") == "Token local-test-token"
    assert b'from(bucket: "vf48_test4")' in requests[0].data
    assert b'r._measurement == "run_80"' in requests[0].data
    assert job.run_source("influx", datetime(2026, 9, 27, 15, tzinfo=UTC)) == 2
    records = list(store.iter_detector_points("2026-09-27T13:00:00Z", None))
    assert len(records) == 2
    assert {record["_field"] for record in records} == {"trignum", "ADCA"}
    assert store.records("influx", 1, 10)[1] == 0
    assert store.run_stats()[0]["run_number"] == "80"


def test_malformed_influx_row_does_not_advance_checkpoint(monkeypatch, store):
    monkeypatch.setattr(
        "x17_registry.adapters.readers.urlopen",
        lambda request, timeout: io.BytesIO(CSV.replace("run_80,2,ADCA", "run_80,,ADCA").encode()),
    )
    settings = poll_settings(
        store.path,
        influx_enabled=True,
        influx_url="http://127.0.0.1:18888",
        influx_org="UdeM",
        influx_bucket="vf48_test4",
        influx_token=SecretStr("local-test-token"),
        influx_source_instance="test-influx",
        influx_start_at=datetime(2026, 9, 27, 13, tzinfo=UTC),
    )
    job = PollJob({"influx": InfluxReader(settings)}, IdentityProcessor(), store, 1)
    with pytest.raises(ValueError, match="VF48_num"):
        job.run_source("influx", datetime(2026, 9, 27, 15, tzinfo=UTC))
    assert store.checkpoint("influx") is None
    assert len(list(store.iter_detector_points("2026-09-27T13:00:00Z", None))) == 1


def test_csv_parser_handles_repeated_tables():
    rows = list(flux_csv_rows((CSV + "\n" + CSV).splitlines()))
    assert len(rows) == 4
