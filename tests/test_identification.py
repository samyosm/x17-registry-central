from datetime import UTC, datetime, timedelta

import numpy as np
import pytest

from x17_registry.application.identification import scintillator_groups
from x17_registry.application.polling import SourceRecord

uproot = pytest.importorskip("uproot")
pytest.importorskip("isotope_id")

EDGES = np.linspace(0, 1200, 201)
CENTERS = (EDGES[:-1] + EDGES[1:]) / 2
START = datetime(2026, 9, 27, 14, 0, tzinfo=UTC)
PARAMS = {"from": "2026-09-27T14:00:00Z", "to": "2026-09-27T14:05:00Z"}


def shapes():
    muons = np.exp(-CENTERS / 300)
    cesium = np.exp(-0.5 * ((CENTERS - 250) / 40) ** 2) + 0.05 * muons
    return {"cosmic-muons": muons, "Cs-137": cesium}


def write_config(directory):
    lines = [
        f'[io]\noutput_dir = "{directory}"\n',
        "[histogram]\nbins = 200\nmin = 0\nmax = 1200\n",
    ]
    for number, (isotope, shape) in enumerate(shapes().items()):
        stem = f"source{number}"
        counts = np.rint(1e6 * shape / shape.sum()).astype(np.int64)
        with uproot.recreate(directory / f"{stem}_digitized_histogram_ref.root") as file:
            file["sqrt_UD"] = (counts, EDGES)
        source = directory / f"{stem}.root"
        lines.append(f'[[sources]]\npath = "{source}"\nisotope = "{isotope}"\n')
    path = directory / "config.toml"
    path.write_text("\n".join(lines))
    return path


def pair_points(field_d, field_u, values, prefix):
    points = []
    for index, value in enumerate(values):
        at = (START + timedelta(microseconds=index + 1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        for field in (field_d, field_u):
            points.append(
                SourceRecord(
                    "influx",
                    "test-influx",
                    f"{prefix}-{index}-{field}",
                    {"_time": at, "_measurement": "run_80", "_field": field, "_value": value},
                    at,
                    "80",
                )
            )
    return points


@pytest.fixture
def isotope_client(tmp_path, store):
    from conftest import server_settings
    from fastapi.testclient import TestClient

    from x17_registry.api.app import create_app

    settings = server_settings(store.client.database).model_copy(
        update={"isotope_id_config": write_config(tmp_path)}
    )
    with TestClient(create_app(settings, store)) as result:
        result.headers["Authorization"] = "Bearer test-token"
        yield result


def test_scintillator_groups_follow_the_events_page():
    groups = scintillator_groups()
    assert len(groups) == 16
    assert groups["0"] == ("FPGA0_ADCA", "FPGA2_ADCA")
    assert groups["15"] == ("FPGA0_ADCB", "FPGA2_ADCB")


def test_identification_classifies_each_scintillator(isotope_client, store):
    rng = np.random.default_rng(1)
    shape = shapes()
    cesium = rng.choice(CENTERS, size=2000, p=shape["Cs-137"] / shape["Cs-137"].sum())
    muons = rng.choice(CENTERS, size=2000, p=shape["cosmic-muons"] / shape["cosmic-muons"].sum())
    store.save_detector_batch(
        pair_points("FPGA0_ADCA", "FPGA2_ADCA", [float(v) for v in cesium], "cs")
        + pair_points("FPGA0_ADCB", "FPGA2_ADCB", [float(v) for v in muons][:50], "mu")
    )
    body = isotope_client.get("/api/v1/identification", params=PARAMS | {"limit": 100_000}).json()
    assert body["truncated"] is False
    assert body["readings"] == 4100
    rows = {item["scintillator"]: item for item in body["scintillators"]}
    assert len(rows) == 16
    assert rows["0"]["pairs"] == 2000
    assert rows["0"]["verdict"] == "Cs-137"
    assert rows["15"]["pairs"] == 50
    assert rows["15"]["verdict"] is None
    assert rows["1"]["pairs"] == 0


def test_identification_limit_truncates_and_validates(isotope_client, store):
    store.save_detector_batch(pair_points("FPGA0_ADCA", "FPGA2_ADCA", [100.0] * 300, "x"))
    body = isotope_client.get("/api/v1/identification", params=PARAMS | {"limit": 200}).json()
    assert body["truncated"] is True
    assert body["readings"] == 200
    rows = {item["scintillator"]: item for item in body["scintillators"]}
    assert rows["0"]["pairs"] == 100
    unlimited = isotope_client.get(
        "/api/v1/identification", params=PARAMS | {"limit": 10_000_000_000}
    ).json()
    assert unlimited["truncated"] is False
    assert unlimited["readings"] == 600
    backwards = isotope_client.get(
        "/api/v1/identification", params={"from": PARAMS["to"], "to": PARAMS["from"]}
    )
    assert backwards.status_code == 400
    too_small = isotope_client.get("/api/v1/identification", params=PARAMS | {"limit": 5})
    assert too_small.status_code == 422


def test_identification_unavailable_without_config(client):
    response = client.get("/api/v1/identification", params=PARAMS)
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "identifier_unavailable"


def test_diagnostics_show_a_scale_mismatch(isotope_client, store):
    rng = np.random.default_rng(2)
    shape = shapes()["cosmic-muons"]
    muons = rng.choice(CENTERS, size=3000, p=shape / shape.sum())
    # Data stored 10x larger than the templates, one D reading never paired.
    values = [float(v) * 10 for v in muons]
    store.save_detector_batch(
        pair_points("FPGA0_ADCA", "FPGA2_ADCA", values, "big")
        + [
            SourceRecord(
                "influx",
                "test-influx",
                "alone",
                {
                    "_time": "2026-09-27T14:02:00.000000Z",
                    "_measurement": "run_80",
                    "_field": "FPGA0_ADCA",
                    "_value": 5.0,
                },
                "2026-09-27T14:02:00.000000Z",
                "80",
            )
        ]
    )
    body = isotope_client.get(
        "/api/v1/identification/0", params=PARAMS | {"limit": 100_000}
    ).json()
    assert body["pairs"] == 3000
    assert body["unpaired"] == 1
    assert body["above"] > 1500
    assert 8 < body["scale"] < 12
    assert body["d"]["n"] == 3001
    assert len(body["observed"]) == len(body["muon"]) == 200
    assert body["fit"]["isotope"] in {"Cs-137"}
    missing = isotope_client.get("/api/v1/identification/99", params=PARAMS)
    assert missing.status_code == 404
