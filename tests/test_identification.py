import json
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


# --- identification by excess over a reference period ------------------------------------------

BINS, TOP = 20, 40.0  # 2 ADC per bin up to 40 ADC in the tests
BAD_FIELDS = ["FPGA2_ADCG", "FPGA2_ADCH", "FPGA2_ADCF"]
EVENT_STEP_NS = 1_000_000  # one event per millisecond


def write_excess_config(directory):
    """A config with an [excess] section and two invented source shapes: "Soft" and "Hard"."""
    from x17_registry.application.channels import PMT_CHANNELS

    centres = (np.arange(BINS) + 0.5) * TOP / BINS
    # like the real shapes, nothing below the selection (both PMTs at or above 8)
    soft = np.where(centres >= 8, np.exp(-(centres - 8.0) / 8.0), 0.0)
    hard = np.where(centres >= 8, np.exp(-0.5 * ((centres - 26.0) / 6.0) ** 2) + 0.05, 0.0)
    shapes = {
        name: {"shapes": {str(c): {str(b): (shape / shape.sum()).tolist() for b in range(16)}
                          for c in range(9)}}
        for name, shape in (("Soft", soft), ("Hard", hard))
    }
    (directory / "templates.json").write_text(json.dumps({"sources": shapes}))
    pedestals = "\n".join(f"{field} = 0.0" for field in PMT_CHANNELS)
    path = directory / "config.toml"
    path.write_text(f"""
[excess]
threshold = 8
bins = {BINS}
max = {TOP}
templates = "templates.json"
detection_ts = 30.0
margin_ts = 9.0
min_events = 200
min_plausible_activity_bq = 1000
bad_channels = {json.dumps(BAD_FIELDS)}
[half_life_years]
"Soft" = 30.0
"Hard" = 30.0
"Fe-55" = 2.737
[[lab_sources]]
id = 1
isotope = "Soft"
activity_uci = 100.0
reference_date = "2026-01-01"
[[lab_sources]]
id = 2
isotope = "Hard"
activity_uci = 100.0
reference_date = "2026-01-01"
[[lab_sources]]
id = 3
isotope = "Fe-55"
activity_uci = 100.0
reference_date = "2026-01-01"
[excess.pedestals]
{pedestals}
""")
    return path


def stamp_of(ns):
    moment = START + timedelta(microseconds=(ns - START_NS) // 1000)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def events(rng, start_index, count, source_rate=0.0, source_bars=(3, 4), shape="Soft"):
    """`count` events: noise on bars 2 to 5, a trigger number, and in some events a source."""
    points, at = [], START_NS + start_index * EVENT_STEP_NS
    for index in range(count):
        t = at + index * EVENT_STEP_NS
        stamp = stamp_of(t)
        points.append(reading(f"n{t}", stamp, "trignum", float(start_index + index)))
        for bar in (2, 3, 4, 5):
            d_field, u_field = GROUPS[str(bar)]
            hit = rng.random() < source_rate * (1.0 if bar in source_bars else 0.0)
            if hit:  # a source pulse: soft (exponential) or hard (around 26 ADC) in both PMTs
                v = rng.exponential(8.0) + 8 if shape == "Soft" else rng.normal(26, 5)
                d, u = v * rng.uniform(0.9, 1.1), v * rng.uniform(0.9, 1.1)
            elif rng.random() < 0.03:  # a noise pair just above the threshold
                d, u = rng.uniform(8, 14), rng.uniform(8, 14)
            else:
                d, u = rng.integers(0, 6), rng.integers(0, 6)
            points.append(reading(f"{t}-{bar}-d", stamp, d_field, float(d)))
            points.append(reading(f"{t}-{bar}-u", stamp, u_field, float(u)))
    return points


@pytest.fixture
def excess_client(tmp_path, store):
    from conftest import server_settings
    from fastapi.testclient import TestClient

    from x17_registry.api.app import create_app

    settings = server_settings(store.client.database).model_copy(
        update={"isotope_id_config": write_excess_config(tmp_path)}
    )
    with TestClient(create_app(settings, store)) as result:
        result.headers["Authorization"] = "Bearer test-token"
        yield result


def iso_of(index):
    return stamp_of(START_NS + index * EVENT_STEP_NS)


def test_excess_finds_the_source_and_its_bar(excess_client, store):
    rng = np.random.default_rng(5)
    # reference: events 0-3999, no source; window: events 4000-5999, a soft source on bars 3 and 4
    store.save_detector_batch(events(rng, 0, 4000) + events(rng, 4000, 2000, source_rate=0.10))
    body = excess_client.get(
        "/api/v1/identification", params={"from": iso_of(4000), "to": iso_of(6000)}
    ).json()
    assert body["reference"]["mode"] == "before"
    assert body["events"]["window"] == 2000 and body["reference"]["events"] == 4000
    assert body["events"]["scale"] == pytest.approx(0.5)
    bars = {item["scintillator"]: item for item in body["bars"]}
    assert len(bars) == 16
    assert bars["3"]["z"] > 5 and bars["4"]["z"] > 5
    assert abs(bars["2"]["z"]) < 4 and abs(bars["5"]["z"]) < 4
    assert [bars[str(b)]["usable"] for b in (9, 10, 11)] == [False, False, False]
    assert bars["9"]["reason"].startswith("PMT")
    assert body["verdict"]["code"] in ("identified", "detected")
    assert "Soft" in (body["verdict"]["candidates"] + [body["verdict"]["source"]])
    assert body["verdict"]["source"] in ("Soft", None)
    assert body["verdict"]["centre"] in (3, 4)
    assert body["sources"][0]["name"] == "Soft"
    assert body["sources"][0]["ts"] > body["sources"][1]["ts"]
    assert body["detection"]["rateZ"] > 8
    assert len(body["spectrum"]["window"]) == BINS


def test_excess_names_the_hard_source_and_says_none_without_a_source(excess_client, store):
    rng = np.random.default_rng(6)
    store.save_detector_batch(
        events(rng, 0, 4000)
        + events(rng, 4000, 2000, source_rate=0.15, shape="Hard")
        + events(rng, 6000, 2000)
    )
    hard = excess_client.get(
        "/api/v1/identification", params={"from": iso_of(4000), "to": iso_of(6000)}
    ).json()
    assert hard["sources"][0]["name"] == "Hard"
    assert hard["verdict"]["code"] in ("identified", "detected")
    quiet = excess_client.get(
        "/api/v1/identification", params={"from": iso_of(6000), "to": iso_of(8000)}
    ).json()
    assert quiet["verdict"]["code"] == "none"
    assert quiet["verdict"]["centre"] is None
    assert quiet["detection"]["rateZ"] < 4


def test_excess_reference_rules_and_errors(excess_client, store):
    rng = np.random.default_rng(7)
    store.save_detector_batch(events(rng, 0, 3000) + events(rng, 3000, 1000, source_rate=0.1))
    window = {"from": iso_of(0), "to": iso_of(1000)}
    # nothing before the window: the period after is used
    after = excess_client.get("/api/v1/identification", params=window).json()
    assert after["reference"]["mode"] == "after"
    assert after["reference"]["events"] >= 2000
    # a custom reference, and its errors
    custom = excess_client.get(
        "/api/v1/identification", params=window | {"refFrom": iso_of(1000), "refTo": iso_of(3000)}
    )
    assert custom.status_code == 200 and custom.json()["reference"]["mode"] == "custom"
    overlap = excess_client.get(
        "/api/v1/identification", params=window | {"refFrom": iso_of(500), "refTo": iso_of(2000)}
    )
    assert overlap.status_code == 400
    one = excess_client.get("/api/v1/identification", params=window | {"refFrom": iso_of(1000)})
    assert one.status_code == 400
    backwards = excess_client.get(
        "/api/v1/identification", params={"from": iso_of(1000), "to": iso_of(0)}
    )
    assert backwards.status_code == 400
    # too few events in the window
    few = excess_client.get(
        "/api/v1/identification",
        params={
            "from": iso_of(0), "to": iso_of(50), "refFrom": iso_of(1000), "refTo": iso_of(3000),
        },
    )
    assert few.status_code == 422
    assert few.json()["error"]["code"] == "insufficient_data"


def test_excess_warns_when_the_event_rate_changed(excess_client, store):
    rng = np.random.default_rng(8)
    # the reference has half the event rate of the window: the trigger configuration changed
    sparse = [p for i, p in enumerate(events(rng, 0, 2000)) if (i // 9) % 2 == 0]
    store.save_detector_batch(sparse + events(rng, 2000, 2000))
    body = excess_client.get(
        "/api/v1/identification", params={"from": iso_of(2000), "to": iso_of(4000)}
    ).json()
    assert any("event rate differs" in warning for warning in body["warnings"])


def test_excess_unavailable_without_the_section(isotope_client):
    # the default test config has no [excess] section
    response = isotope_client.get("/api/v1/identification", params=PARAMS)
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "identifier_unavailable"
    assert "excess" in response.json()["error"]["message"]


# --- the trigger configuration -------------------------------------------------------------------


def trigger_entry(store, name, at_index, maj):
    config = {"title": name, "tmod": "ormaj", "maj": maj, "thresholds": {"F0A": 4}, "masks": {}}
    store.save(SourceRecord("trigger_history", "test", name, config, iso_of(at_index)), config)


def test_trigger_epochs_merge_equal_configurations_and_compare():
    from x17_registry.application import trigger_context as tc

    def record(name, maj, thr, at):
        config = {"tmod": "ormaj", "maj": maj, "thresholds": {"F0A": thr}, "title": name}
        return {"event_time": at, "raw": config}

    items = tc.epochs([
        record("a", 1, 4, "2026-10-01T00:00:00Z"),
        record("b", 1, 4, "2026-10-02T00:00:00Z"),  # same configuration: merged with a
        record("c", 2, 4, "2026-10-03T00:00:00Z"),
        record("d", 1, 4, "2026-10-04T00:00:00Z"),  # back to the first configuration
    ])
    assert len(items) == 3 and items[0].end_ns == items[1].start_ns
    assert items[2].end_ns == tc.FOREVER
    assert tc.differences(items[0].config, items[1].config) == ["majority 1 against 2"]
    assert tc.differences(items[0].config, items[2].config) == []
    day = 86_400 * 1_000_000_000
    window = (items[2].start_ns + day, items[2].start_ns + 2 * day)
    found = list(tc.reference_candidates(items, items[2].config, *window, day))
    # nearest first: the start of the window's own period, then the first period (the same
    # configuration again), never the middle period; the periods after come last
    assert found[0] == ("before", items[2].start_ns, window[0])
    assert found[1][0] == "before" and found[1][2] == items[1].start_ns
    assert found[-1][0] == "after"
    assert all(not (items[1].start_ns < low < items[1].end_ns) for _, low, _ in found)


def test_excess_reference_follows_the_trigger_configuration(excess_client, store):
    rng = np.random.default_rng(8)
    store.save_detector_batch(
        events(rng, 0, 4000) + events(rng, 4000, 2000, source_rate=0.1) + events(rng, 6000, 4000)
    )
    trigger_entry(store, "first", 0, maj=1)
    trigger_entry(store, "second", 4000, maj=2)
    window = {"from": iso_of(4000), "to": iso_of(6000)}
    body = excess_client.get("/api/v1/identification", params=window).json()
    # before the window the configuration was another one: the period after it is the reference
    assert body["reference"]["mode"] == "after"
    assert body["reference"]["events"] == 4000
    assert body["trigger"]["known"] and body["trigger"]["same"] is True
    assert body["trigger"]["window"]["majority"] == 2
    assert not any("trigger configuration" in warning for warning in body["warnings"])
    # a chosen reference with another configuration is used, with a warning that says what differs
    custom = excess_client.get(
        "/api/v1/identification", params=window | {"refFrom": iso_of(0), "refTo": iso_of(4000)}
    ).json()
    assert custom["trigger"]["same"] is False
    assert custom["trigger"]["differences"] == ["majority 2 against 1"]
    assert any("differs between the window and the reference" in w for w in custom["warnings"])


def test_excess_without_trigger_history_still_works(excess_client, store):
    rng = np.random.default_rng(9)
    store.save_detector_batch(events(rng, 0, 4000) + events(rng, 4000, 2000))
    body = excess_client.get(
        "/api/v1/identification", params={"from": iso_of(4000), "to": iso_of(6000)}
    ).json()
    assert body["trigger"]["known"] is False
    assert any("trigger history does not cover" in warning for warning in body["warnings"])


# --- the lab sources ------------------------------------------------------------------------------


def test_lab_sources_settings(excess_client, store):
    rows = excess_client.get("/api/v1/settings/sources").json()["sources"]
    by_id = {row["id"]: row for row in rows}
    assert [by_id[i]["isotope"] for i in (1, 2, 3)] == ["Soft", "Hard", "Fe-55"]
    assert by_id[1]["supported"] and by_id[1]["enabled"]
    assert not by_id[3]["supported"] and not by_id[3]["enabled"] and by_id[3]["note"]
    assert by_id[1]["activityBq"] < by_id[1]["activityUci"] * 3.7e4  # decayed
    assert excess_client.put("/api/v1/settings/sources", json={"enabled": [9]}).status_code == 400
    # only the chosen sources are fitted and reported
    rng = np.random.default_rng(10)
    store.save_detector_batch(events(rng, 0, 4000) + events(rng, 4000, 2000, source_rate=0.1))
    params = {"from": iso_of(4000), "to": iso_of(6000)}
    assert {s["name"] for s in excess_client.get("/api/v1/identification", params=params).json()[
        "sources"]} == {"Soft", "Hard"}
    changed = excess_client.put("/api/v1/settings/sources", json={"enabled": [2, 3]}).json()
    assert [row["enabled"] for row in changed["sources"]] == [False, True, False]
    only = excess_client.get("/api/v1/identification", params=params).json()
    assert [s["name"] for s in only["sources"]] == ["Hard"]
