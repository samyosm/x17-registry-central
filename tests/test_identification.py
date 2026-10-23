from datetime import UTC, datetime, timedelta

import numpy as np
import pytest

from x17_registry.adapters.registry import epoch_ns
from x17_registry.application.identification import pair_spec, scintillator_groups
from x17_registry.application.polling import SourceRecord

uproot = pytest.importorskip("uproot")
pytest.importorskip("isotope_id")

EDGES = np.linspace(0, 1200, 201)
CENTERS = (EDGES[:-1] + EDGES[1:]) / 2
START = datetime(2026, 9, 27, 14, 0, tzinfo=UTC)
PARAMS = {"from": "2026-09-27T14:00:00Z", "to": "2026-09-27T15:00:00Z"}
START_NS = epoch_ns("2026-09-27T14:00:00Z")
END_NS = epoch_ns("2026-09-27T15:00:00Z")
GROUPS = scintillator_groups()


def shapes():
    muons = np.exp(-CENTERS / 300)
    cesium = np.exp(-0.5 * ((CENTERS - 250) / 40) ** 2) + 0.05 * muons
    return {"cosmic-muons": muons, "Cs-137": cesium}


def write_config(directory, extra=""):
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
    path.write_text("\n".join(lines) + "\n" + extra)
    return path


FILTERED = '[filter]\nenabled = true\nmode = "both"\nthreshold = 20\n'


def reading(identity, at, field, value):
    return SourceRecord(
        "influx",
        "test-influx",
        identity,
        {"_time": at, "_measurement": "run_80", "_field": field, "_value": value},
        at,
        "80",
    )


def stamp(index):
    return (START + timedelta(microseconds=index + 1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def pair_points(field_d, field_u, values, prefix):
    """A D and a U reading of the same value and time for each value."""
    points = []
    for index, value in enumerate(values):
        for field in (field_d, field_u):
            points.append(reading(f"{prefix}-{index}-{field}", stamp(index), field, value))
    return points


@pytest.fixture
def make_identifier(tmp_path):
    from isotope_id import Identifier

    def make(extra=""):
        return Identifier.from_config(write_config(tmp_path, extra))

    return make


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
    assert len(GROUPS) == 16
    assert GROUPS["0"] == ("FPGA0_ADCA", "FPGA2_ADCA")
    assert GROUPS["15"] == ("FPGA0_ADCB", "FPGA2_ADCB")


def noisy_readings(rng):
    """Baseline noise with some real events, unpaired readings and repeated copies."""
    muon = shapes()["cosmic-muons"]
    points, times, fields, adc = [], [], [], []

    def add(identity, index, field, value, copy=False):
        at = stamp(index)
        record = reading(identity, at, field, float(value))
        points.append(record)
        if copy:
            points.append(record)  # the same point stored twice (overlapping polls)
        times.append(epoch_ns(at))
        fields.append(field)
        adc.append(float(value))

    for index in range(3000):
        real = rng.random() < 0.3
        for number in ("0", "1"):
            d_field, u_field = GROUPS[number]
            if number == "1" and index % 7 == 0:
                add(f"{number}-{index}-d", index, d_field, np.round(rng.normal(3, 2)))  # unpaired
                continue
            base = rng.choice(CENTERS, p=muon / muon.sum()) if real else None
            d = base + 3 if real else np.round(rng.normal(3, 2))
            u = base * rng.uniform(0.8, 1.2) + 5 if real else np.round(rng.normal(5, 3.4))
            copy = index % 11 == 0
            add(f"{number}-{index}-d", index, d_field, d, copy)
            add(f"{number}-{index}-u", index, u_field, u, copy)
    # Readings outside the period must not count.
    outside = reading("late", "2026-09-27T15:30:00.000000Z", GROUPS["0"][0], 500.0)
    return points + [outside], np.array(times), np.array(fields), np.array(adc)


PEDESTALS = '[channels.FPGA0_ADCA]\npedestal = 3.0\n[channels.FPGA2_ADCA]\npedestal = 5.0\n'
CALIBRATION = (
    "[calibration]\nkev_per_template_unit = 40.0\n[calibration.gamma]\n0 = 20.0\n1 = 25.0\n"
)
FILTERS = {
    "disabled": "",
    "both": FILTERED,
    "either": '[filter]\nenabled = true\nmode = "either"\nthreshold = 20\n',
    "both, calibrated": FILTERED + CALIBRATION,
    "disabled, calibrated": CALIBRATION,
}


@pytest.mark.parametrize("name", FILTERS)
def test_database_histograms_equal_the_python_ones(make_identifier, store, name):
    identifier = make_identifier(FILTERS[name] + PEDESTALS)
    points, times, fields, adc = noisy_readings(np.random.default_rng(3))
    store.save_detector_batch(points)

    spec = pair_spec(identifier)
    from_database = store.pair_histograms(spec, START_NS, END_NS)
    assert sum(pairs for _, pairs in from_database.values()) > 0
    for number in GROUPS:
        values, _ = identifier.pair_values(times, fields, adc, *GROUPS[number], number)
        counts, pairs = from_database[number]
        assert counts == np.histogram(values, bins=identifier.edges)[0].tolist()
        assert pairs == len(values)


@pytest.mark.parametrize("calibration", ["", CALIBRATION])
def test_database_diagnostics_equal_the_python_ones(make_identifier, store, calibration):
    identifier = make_identifier(FILTERED + PEDESTALS + calibration)
    points, times, fields, adc = noisy_readings(np.random.default_rng(4))
    store.save_detector_batch(points)

    spec = pair_spec(identifier)
    inputs = store.pair_diagnostics(
        spec, "1", START_NS, END_NS, identifier.raw_start, identifier.raw_bins
    )
    expected = identifier.diagnose(times, fields, adc, *GROUPS["1"], "1")
    scan = inputs.pop("scan")
    result = identifier.diagnose_histogram(**inputs, group="1")
    for key in ("pairs", "matched", "unpaired", "below", "above", "zero", "observed", "raw"):
        assert result[key] == expected[key], key
    for end in ("d", "u"):
        for key in ("n", "negative", "min", "max"):
            assert result[end][key] == expected[end][key], (end, key)
        assert result[end]["p50"] == pytest.approx(expected[end]["p50"], abs=1.5)
    assert result["calibration"] == expected["calibration"]
    assert result["values"]["n"] == expected["values"]["n"]
    u_raw, d_raw = identifier.matched_pairs(times, fields, adc, *GROUPS["1"])
    assert len(scan["thresholds"]) == len(scan["pairs"]) > 5
    for threshold, pairs in zip(scan["thresholds"], scan["pairs"], strict=True):
        assert pairs == int(np.count_nonzero((d_raw >= threshold) & (u_raw >= threshold)))
    assert result["values"]["p50"] == pytest.approx(expected["values"]["p50"], rel=0.1)


def test_calibration_converts_the_real_values_for_the_simulation(make_identifier):
    import config as isotope_config

    defaults = (
        "[defaults]\ngain = 1.0\nspe_relative_sigma = 0.5\npedestal = 4.0\n"
        "electronic_noise_sigma = 2.5\n"
    )
    identifier = make_identifier(defaults + FILTERED + PEDESTALS + CALIBRATION)
    assert identifier.scale("0") == pytest.approx(20.0 / 40.0)
    assert identifier.scale("1") == pytest.approx(25.0 / 40.0)
    assert identifier.scale("9") == pytest.approx(22.5 / 40.0)  # unknown: the mean gamma
    config = identifier.config
    factor = 22.5 / 40.0
    assert config["calibration"]["sim_per_adc"] == pytest.approx(factor)
    # the real pedestal, noise and threshold are given in ADC and simulated in template units
    assert config["filter"]["simulation_threshold"] == pytest.approx(20.0 * factor)
    _, _, pedestal, noise = isotope_config.channel_parameters(config, "FPGA0_ADCA")
    assert pedestal == pytest.approx(3.0 * factor)  # the channel override
    _, _, pedestal, noise = isotope_config.channel_parameters(config, "FPGA1_ADCC")
    assert pedestal == pytest.approx(4.0 * factor)  # the default
    assert noise == pytest.approx(2.5 * factor)
    # the real side keeps the real values
    assert identifier.pedestal("FPGA0_ADCA") == 3.0
    assert identifier.filter["threshold"] == 20

    uncalibrated = make_identifier(FILTERED)
    assert uncalibrated.scale("0") == 1.0
    assert uncalibrated.config["filter"]["simulation_threshold"] == 20.0


def test_identification_covers_the_whole_period(isotope_client, store):
    rng = np.random.default_rng(1)
    shape = shapes()
    cesium = rng.choice(CENTERS, size=2000, p=shape["Cs-137"] / shape["Cs-137"].sum())
    muons = rng.choice(CENTERS, size=50, p=shape["cosmic-muons"] / shape["cosmic-muons"].sum())
    store.save_detector_batch(
        pair_points(*GROUPS["0"], [float(v) for v in cesium], "cs")
        + pair_points(*GROUPS["15"], [float(v) for v in muons], "mu")
    )
    body = isotope_client.get("/api/v1/identification", params=PARAMS).json()
    assert body["readings"] == 4100
    assert body["filter"]["enabled"] is False
    rows = {item["scintillator"]: item for item in body["scintillators"]}
    assert len(rows) == 16
    assert rows["0"]["pairs"] == 2000
    assert rows["0"]["verdict"] == "Cs-137"
    assert rows["15"]["pairs"] == 50
    assert rows["15"]["verdict"] is None
    assert rows["1"]["pairs"] == 0
    assert "limit" not in body
    backwards = isotope_client.get(
        "/api/v1/identification", params={"from": PARAMS["to"], "to": PARAMS["from"]}
    )
    assert backwards.status_code == 400


def test_diagnostics_show_a_scale_mismatch(isotope_client, store):
    rng = np.random.default_rng(2)
    shape = shapes()["cosmic-muons"]
    muons = rng.choice(CENTERS, size=3000, p=shape / shape.sum())
    # Data stored 10x larger than the templates, one D reading never paired.
    store.save_detector_batch(
        pair_points(*GROUPS["0"], [float(v) * 10 for v in muons], "big")
        + [reading("alone", "2026-09-27T14:02:00.000000Z", GROUPS["0"][0], 5.0)]
    )
    body = isotope_client.get("/api/v1/identification/0", params=PARAMS).json()
    assert body["pairs"] == 3000
    assert body["unpaired"] == 1
    assert body["above"] > 1500
    assert 8 < body["scale"] < 12
    assert body["d"]["n"] == 3001
    assert len(body["observed"]) == len(body["muon"]) == 200
    assert body["fit"]["isotope"] in {"Cs-137"}
    assert len(body["raw"]["d"]) == 120
    # The page reads exactly these keys.
    assert body["templateMedian"] > 0
    assert "template_median" not in body
    assert set(body) >= {
        "scintillator", "from", "to", "readings", "threshold", "d", "u", "pairs", "matched",
        "filter", "unpaired", "values", "below", "above", "zero", "edges", "observed", "muon",
        "fit", "templateMedian", "scale", "raw", "scan", "calibration",
    }
    assert set(body["calibration"]) == {"kevPerUnit", "gamma", "factor"}
    assert isotope_client.get("/api/v1/identification/99", params=PARAMS).status_code == 404


def test_identification_unavailable_without_config(client):
    response = client.get("/api/v1/identification", params=PARAMS)
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "identifier_unavailable"
