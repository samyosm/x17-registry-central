from x17_registry.application.polling import SourceRecord


def test_interface_run_routes_and_download(client, store):
    store.save(
        SourceRecord(
            "influx",
            "test-influx",
            "point-1",
            {"_measurement": "run_80", "VF48_num": "2", "_field": "ADCA", "_value": "312"},
            "2026-09-27T14:00:00Z",
            "80",
        ),
        {"_measurement": "run_80", "VF48_num": "2", "_field": "ADCA", "_value": "312"},
    )
    search = client.get("/api/v1/runs?q=80&from=2026-09-27&to=2026-09-27")
    assert search.status_code == 200
    assert search.json()["total"] == 1
    found = search.json()["runs"][0]
    assert found["runNumber"] == "80"
    assert found["beamStatus"] == "unknown"
    assert found["experimentId"] == "unknown"
    detail = client.get(f"/api/v1/runs/{found['id']}")
    assert detail.status_code == 200
    body = detail.json()
    assert body["endedAt"] is None
    assert body["configuration"] is None
    assert body["artifacts"][0]["state"] == "ready"
    download = client.get(body["artifacts"][0]["downloadUrl"])
    assert download.status_code == 200
    assert download.json()[0]["raw"]["_value"] == "312"
    assert client.get("/api/v1/runs?q=absent").json()["runs"] == []
    assert client.get("/api/v1/runs/unknown").status_code == 404
    assert client.get("/api/v1/runs?from=2026-09-30&to=2026-09-01").status_code == 400


def test_records_and_authentication(client, store):
    store.save(
        SourceRecord("trigger_config", "local", "current", {"tmod": "2of3"}), {"tmod": "2of3"}
    )
    records = client.get("/api/v1/records?source=trigger_config").json()
    assert records["total"] == 1
    identity = records["records"][0]["id"]
    assert client.get(f"/api/v1/records/{identity}").json()["processed"] == {"tmod": "2of3"}
    assert client.get("/api/v1/records", headers={"Authorization": ""}).status_code == 401
    assert client.get("/health/live", headers={"Authorization": ""}).status_code == 200
