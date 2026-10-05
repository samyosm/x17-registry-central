import csv
import io

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
    assert found["pointCount"] is None
    stats = client.get(f"/api/v1/runs/stats?ids={found['id']}").json()["stats"][found["id"]]
    assert stats["pointCount"] == 1
    assert stats["estimatedJsonBytes"] > 0
    detail = client.get(f"/api/v1/runs/{found['id']}")
    assert detail.status_code == 200
    body = detail.json()
    assert body["endedAt"] == "2026-09-27T15:00:00Z"
    assert body["configuration"]["majority"] == 2
    assert body["detectorSources"] == ["VF48 / InfluxDB v2 / test-influx / run_80"]
    assert body["artifacts"][0]["state"] == "ready"
    assert body["artifacts"][0]["source"] == "VF48 / InfluxDB v2 / test-influx"
    assert {artifact["format"] for artifact in body["artifacts"]} == {"CSV", "JSON"}
    downloads = {
        artifact["format"]: client.get(artifact["downloadUrl"]) for artifact in body["artifacts"]
    }
    assert all(response.status_code == 200 for response in downloads.values())
    assert [row["_value"] for row in downloads["JSON"].json()] == ["during"]
    assert (
        client.get(f"/api/v1/runs/{found['id']}/artifacts/detector-records/download").json()
        == downloads["JSON"].json()
    )
    assert [row["_value"] for row in csv.DictReader(io.StringIO(downloads["CSV"].text))] == [
        "during"
    ]
    second = client.get("/api/v1/runs?q=Beam on").json()["runs"][0]
    second_detail = client.get(f"/api/v1/runs/{second['id']}").json()
    assert second_detail["endedAt"] is None
    assert [
        row["_value"] for row in client.get(second_detail["artifacts"][1]["downloadUrl"]).json()
    ] == ["boundary"]
    assert client.get("/api/v1/runs").json()["total"] == 2
    assert client.get("/api/v1/runs?hasData=true&pageSize=1").json()["totalPages"] == 2
    assert client.get("/api/v1/runs?hasData=false").json()["total"] == 0
    assert client.get("/api/v1/runs?beam=on").json()["total"] == 0
    suggestions = client.get("/api/v1/runs/suggestions?q=cos").json()["suggestions"]
    assert suggestions[0]["title"] == "Cosmic calibration"
    assert client.get("/api/v1/runs?q=absent").json()["runs"] == []
    assert client.get("/api/v1/runs/unknown").status_code == 404
    assert client.get("/api/v1/runs?from=2026-09-30&to=2026-09-01").status_code == 400


def test_run_list_checks_detector_only_for_visible_rows(client, store, monkeypatch):
    for day in range(1, 21):
        at = f"2026-09-{day:02d}T14:00:00Z"
        store.save(
            SourceRecord(
                "trigger_history",
                "test-trigger",
                str(day),
                {"title": f"Run {day}"},
                at,
            ),
            {},
        )
    checked = []

    def origin(start, end):
        checked.append(start)
        return None

    def expensive_count(start, end):
        raise AssertionError("The run list must not count detector rows")

    monkeypatch.setattr(store, "influx_origin", origin)
    monkeypatch.setattr(store, "detector_window_stats", expensive_count)
    response = client.get("/api/v1/runs?pageSize=5")
    assert response.status_code == 200
    assert response.json()["total"] == 20
    assert len(checked) == 5


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
        client.get(
            f"/api/v1/runs/{first['id']}/artifacts/detector-records-json/download"
        ).status_code
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


def test_download_formats_are_generated_from_new_detector_points(client, store):
    at = "2026-09-27T14:00:00Z"
    store.save(
        SourceRecord(
            "trigger_history",
            "test-trigger",
            "start",
            {"id": "start", "timestamp": at, "title": "Test"},
            at,
        ),
        {"title": "Test"},
    )
    point = SourceRecord(
        "influx",
        "test-influx",
        "point-1",
        {
            "_time": at,
            "_measurement": "run_80",
            "VF48_num": "2",
            "_field": "ADCA",
            "_value": "312",
            "tag": 'a,"b"',
        },
        at,
        "80",
    )
    assert store.save_detector_batch([point]) == 1
    run = client.get("/api/v1/runs").json()["runs"][0]
    assert run["artifactCount"] == 2
    artifacts = client.get(f"/api/v1/runs/{run['id']}").json()["artifacts"]
    exported = {artifact["format"]: client.get(artifact["downloadUrl"]) for artifact in artifacts}
    assert exported["JSON"].json() == [point.raw]
    rows = list(csv.DictReader(io.StringIO(exported["CSV"].text)))
    assert rows[0]["_value"] == "312"
    assert rows[0]["extra_json"] == '{"tag":"a,\\"b\\""}'
    assert store.records("influx", 1, 10)[1] == 0


def test_beam_events_and_measurements_follow_trigger_windows(client, store):
    for entry_id, at in (
        ("first", "2026-09-27T14:00:00Z"),
        ("second", "2026-09-27T15:00:00Z"),
        ("third", "2026-09-27T16:00:00Z"),
    ):
        store.save(
            SourceRecord(
                "trigger_history",
                "test-trigger",
                entry_id,
                {"id": entry_id, "timestamp": at, "title": entry_id},
                at,
            ),
            {"title": entry_id},
        )
    for event_id, at, status, comment, active in (
        (
            1,
            "2026-09-27T14:20:00Z",
            "ON",
            "Beam ON | Beam Energy E = 4.5 MeV | Current I = 3 nA"
            " | Integrated Charge Q = 2 uC",
            True,
        ),
        (2, "2026-09-27T14:40:00Z", "OFF", "Beam OFF", True),
        (3, "2026-09-27T15:10:00Z", "ON", "Beam ON", False),
    ):
        store.save(
            SourceRecord(
                "logbook_event",
                "test-logbook",
                str(event_id),
                {
                    "id": event_id,
                    "public_id": f"Run80_B{event_id}",
                    "timestamp": at,
                    "category": "Beam",
                    "payload": {"status": status},
                    "comment": comment,
                    "is_active": active,
                },
                at,
                "80",
            ),
            {"status": status},
        )

    runs = {run["title"]: run for run in client.get("/api/v1/runs").json()["runs"]}
    assert runs["first"]["beamStatus"] == "on"
    assert runs["second"]["beamStatus"] == "off"
    assert runs["third"]["beamStatus"] == "off"
    first = client.get(f"/api/v1/runs/{runs['first']['id']}").json()["beam"]
    assert first["energy"] == "4.5 MeV"
    assert first["current"] == "3 nA"
    assert first["charge"] == "2 uC"
    assert [event["status"] for event in first["events"]] == ["on", "off"]
    second = client.get(f"/api/v1/runs/{runs['second']['id']}").json()["beam"]
    assert second["energy"] is None
    assert second["events"] == []
    assert client.get("/api/v1/runs?beam=on").json()["total"] == 1
    assert client.get("/api/v1/runs?beam=off").json()["total"] == 2
    diagnostics = client.get("/api/v1/diagnostics").json()
    assert len(diagnostics["runStarts"]) == 3
    assert [change["status"] for change in diagnostics["beamChanges"]] == ["ON", "OFF"]
    assert diagnostics["sources"]["trigger"]["earliest"] == "2026-09-27T14:00:00Z"


def test_beam_session_spanning_run_boundary(client, store):
    for entry_id, at in (
        ("first", "2026-09-27T14:00:00Z"),
        ("second", "2026-09-27T15:00:00Z"),
    ):
        store.save(
            SourceRecord(
                "trigger_history",
                "test-trigger",
                entry_id,
                {"id": entry_id, "timestamp": at, "title": entry_id},
                at,
            ),
            {},
        )
    first = client.get("/api/v1/runs?q=first").json()["runs"][0]
    assert first["beamStatus"] == "unknown"
    store.save(
        SourceRecord(
            "logbook_event",
            "test-logbook",
            "one",
            {
                "category": "Beam",
                "payload": {"status": "ON"},
                "comment": "Beam ON | Beam Energy E = 100 keV",
                "is_active": True,
            },
            "2026-09-27T13:00:00Z",
        ),
        {},
    )
    assert client.get(f"/api/v1/runs/{first['id']}").json()["beam"]["energy"] == "100 keV"
    second = client.get("/api/v1/runs?q=second").json()["runs"][0]
    assert second["beamStatus"] == "on"
