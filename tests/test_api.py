from x17_registry.application.polling import SourceRecord


def test_interface_run_routes_and_download(client, store):
    for entry_id, started_at, title in (
        ("first-write", "2026-09-27T14:00:00Z", "Cosmic calibration"),
        ("second-write", "2026-09-27T15:00:00Z", "Beam on"),
    ):
        store.save(
            SourceRecord(
                "trigger_history",
                "test-trigger",
                entry_id,
                {
                    "id": entry_id,
                    "timestamp": started_at,
                    "title": title,
                    "tmod": "2",
                    "maj": 2,
                    "thresholds": {"F0A": 120},
                    "note": "",
                },
                started_at,
            ),
            {"title": title},
        )
    for point_id, at in (
        ("before", "2026-09-27T13:59:59Z"),
        ("during", "2026-09-27T14:30:00Z"),
        ("boundary", "2026-09-27T15:00:00Z"),
    ):
        store.save(
            SourceRecord(
                "influx",
                "test-influx",
                point_id,
                {"_measurement": "run_80", "_value": point_id},
                at,
                "80",
            ),
            {"_value": point_id},
        )
    search = client.get("/api/v1/runs?q=Cosmic&from=2026-09-27&to=2026-09-27")
    assert search.status_code == 200
    assert search.json()["total"] == 1
    found = search.json()["runs"][0]
    assert found["runNumber"] == "first-write"
    assert found["title"] == "Cosmic calibration"
    assert found["beamStatus"] == "unknown"
    assert found["experimentId"] == "unknown"
    detail = client.get(f"/api/v1/runs/{found['id']}")
    assert detail.status_code == 200
    body = detail.json()
    assert body["endedAt"] == "2026-09-27T15:00:00Z"
    assert body["configuration"]["majority"] == 2
    assert body["detectorSources"] == ["VF48 / InfluxDB v2 / test-influx / run_80"]
    assert body["artifacts"][0]["state"] == "ready"
    assert body["artifacts"][0]["source"] == "VF48 / InfluxDB v2 / test-influx"
    download = client.get(body["artifacts"][0]["downloadUrl"])
    assert download.status_code == 200
    assert [row["raw"]["_value"] for row in download.json()] == ["during"]
    second = client.get("/api/v1/runs?q=Beam on").json()["runs"][0]
    second_detail = client.get(f"/api/v1/runs/{second['id']}").json()
    assert second_detail["endedAt"] is None
    assert [
        row["raw"]["_value"]
        for row in client.get(second_detail["artifacts"][0]["downloadUrl"]).json()
    ] == ["boundary"]
    assert client.get("/api/v1/runs").json()["total"] == 2
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


def test_trigger_only_run_and_late_next_write(client, store):
    first_time = "2026-09-27T14:00:00Z"
    second_time = "2026-09-27T15:00:00Z"
    store.save(
        SourceRecord(
            "trigger_history",
            "test-trigger",
            "first-write",
            {"id": "first-write", "title": "Calibration", "timestamp": first_time},
            first_time,
        ),
        {"title": "Calibration"},
    )
    first = client.get("/api/v1/runs").json()["runs"][0]
    assert first["artifactCount"] == 0
    assert client.get(f"/api/v1/runs/{first['id']}").json()["endedAt"] is None
    assert (
        client.get(f"/api/v1/runs/{first['id']}/artifacts/detector-records/download").status_code
        == 404
    )

    store.save(
        SourceRecord(
            "trigger_history",
            "test-trigger",
            "second-write",
            {"id": "second-write", "title": "Beam", "timestamp": second_time},
            second_time,
        ),
        {"title": "Beam"},
    )
    assert client.get(f"/api/v1/runs/{first['id']}").json()["endedAt"] == second_time
