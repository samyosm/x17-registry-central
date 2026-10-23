"""ClickHouse queries that turn stored detector readings into identification histograms.

The readings of a period are paired, filtered and histogrammed inside ClickHouse, so only a
few hundred counts per scintillator reach the API, whatever the length of the period. Nothing
is deleted or rejected from the stored data: the acquisition threshold only decides which
stored readings a query uses.

Each stored reading is one row of detector_points; its channel (`field`) and ADC value
(`value`) are the materialized columns of the JSON point (see SCHEMA in registry.py).
A pair is a D-end and a U-end reading of the same scintillator with the same time. It is
kept if the acquisition threshold accepts it, and its value is
sqrt((U - PU) * (D - PD)), with the channel pedestals PU and PD (negative differences count
as 0), exactly as in x17-isotope-id.
"""

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from dataclasses import field as dataclass_field
from typing import Any

# Pairing groups the readings by time, which is the start of the table's sorting key: aggregating
# in that order keeps memory flat, so a whole period never needs the temporary disk.
IN_ORDER = " SETTINGS optimize_aggregation_in_order = 1"
SCAN_THRESHOLDS = (5, 8, 10, 12, 15, 20, 25, 30, 40, 60)
QUANTILES = "(0.01, 0.5, 0.99)"  # approximate (t-digest): 1 %, median and 99 %
SUMMARY_KEYS = ("min", "p1", "p50", "p99", "max", "negative")


@dataclass(frozen=True)
class PairSpec:
    """What the identifier needs the database to compute."""

    groups: Mapping[str, tuple[str, str]]  # group name -> (D channel, U channel)
    low: float
    high: float
    bins: int
    filter_enabled: bool
    filter_mode: str
    filter_threshold: float
    pedestals: Mapping[str, float]  # channel -> pedestal
    # group -> factor on sqrt(U * D), from the calibration of each scintillator
    scales: Mapping[str, float] = dataclass_field(default_factory=dict)


def string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def number(value: float) -> str:
    if not math.isfinite(value):
        raise ValueError("Query numbers must be finite.")
    return repr(float(value))


def numbers(items: Sequence[float]) -> str:
    return "[" + ",".join(number(item) for item in items) + "]"


def strings(items: Sequence[str]) -> str:
    return "[" + ",".join(string(item) for item in items) + "]"


def interval(start_ns: int, end_ns: int) -> str:
    return f"event_ns >= {int(start_ns)} AND event_ns < {int(end_ns)}"


def reading_sql(spec: PairSpec, names: Sequence[str], start_ns: int, end_ns: int) -> str:
    """The readings of the given groups, each with its group index (scint) and end (0 = D)."""
    fields, indexes, ends = [], [], []
    for index, name in enumerate(names):
        for end, field in enumerate(spec.groups[name]):
            fields.append(field)
            indexes.append(index)
            ends.append(end)
    scint = f"transform(field, {strings(fields)}, {numbers(indexes)}, 65535)"
    where = (
        f"{interval(start_ns, end_ns)} AND field IN ({','.join(string(f) for f in fields)}) "
        "AND value IS NOT NULL"
    )
    # Readings that can never be part of an accepted pair are dropped before pairing, so the
    # pairing stays small whatever the length of the period.
    early = ""
    if spec.filter_enabled:
        threshold = number(spec.filter_threshold)
        if spec.filter_mode == "both":
            early = f" AND value >= {threshold}"  # a pair needs both readings at the threshold
        else:
            # a pair needs one reading at the threshold: keep the readings of those events only
            early = (
                f" AND (event_ns, {scint}) IN "
                f"(SELECT event_ns, {scint} FROM detector_points "
                f"WHERE {where} AND value >= {threshold})"
            )
    return (
        "SELECT event_ns, source_id, "
        f"{scint} AS scint, "
        f"transform(field, {strings(fields)}, {numbers(ends)}, 2) AS endp, "
        "toFloat64(value) AS value "
        f"FROM detector_points WHERE {where}{early}"
    )


def pair_sql(spec: PairSpec, names: Sequence[str], start_ns: int, end_ns: int) -> str:
    """One row per accepted pair: scint, d, u (the stored values)."""
    threshold = number(spec.filter_threshold)
    condition = ""
    if spec.filter_enabled:
        condition = (
            f" AND d >= {threshold} AND u >= {threshold}"
            if spec.filter_mode == "both"
            else f" AND (d >= {threshold} OR u >= {threshold})"
        )
    # uniqExact on source_id makes repeated copies of one reading (overlapping polls) count once.
    return (
        "SELECT scint, anyIf(value, endp = 0) AS d, anyIf(value, endp = 1) AS u "
        f"FROM ({reading_sql(spec, names, start_ns, end_ns)}) GROUP BY event_ns, scint "
        "HAVING uniqExactIf(source_id, endp = 0) = 1 AND uniqExactIf(source_id, endp = 1) = 1"
        f"{condition}"
    )


def value_sql(spec: PairSpec, names: Sequence[str], start_ns: int, end_ns: int) -> str:
    """One row per accepted pair: scint and v = scale * sqrt((U - PU) * (D - PD))."""
    indexes = numbers(range(len(names)))
    d_pedestals = numbers([spec.pedestals.get(spec.groups[n][0], 0.0) for n in names])
    u_pedestals = numbers([spec.pedestals.get(spec.groups[n][1], 0.0) for n in names])
    scales = numbers([spec.scales.get(n, 1.0) for n in names])
    return (
        "SELECT scint, sqrt("
        f"greatest(d - transform(scint, {indexes}, {d_pedestals}, 0.), 0) * "
        f"greatest(u - transform(scint, {indexes}, {u_pedestals}, 0.), 0)) "
        f"* transform(scint, {indexes}, {scales}, 1.) AS v "
        f"FROM ({pair_sql(spec, names, start_ns, end_ns)})"
    )


def histogram_sql(spec: PairSpec, names: Sequence[str], start_ns: int, end_ns: int) -> str:
    """Pairs per scint and bin. Bin -1 is above the range, -2 below it."""
    width = (spec.high - spec.low) / spec.bins
    return (
        "SELECT scint, "
        f"multiIf(v < {number(spec.low)}, -2, v > {number(spec.high)}, -1, "
        f"toInt32(least(floor((v - {number(spec.low)}) / {number(width)}), {spec.bins - 1}))) "
        "AS bin, count() AS n "
        f"FROM ({value_sql(spec, names, start_ns, end_ns)}) GROUP BY scint, bin{IN_ORDER}"
    )


def pair_statistics_sql(spec: PairSpec, name: str, start_ns: int, end_ns: int) -> str:
    """Summary of the accepted pair values of one group."""
    return (
        f"SELECT count() AS n, quantilesTDigest{QUANTILES}(v) AS q, min(v) AS low, max(v) AS high, "
        f"countIf(v = 0) AS zero, countIf(v < {number(spec.low)}) AS below, "
        f"countIf(v > {number(spec.high)}) AS above "
        f"FROM ({value_sql(spec, [name], start_ns, end_ns)}){IN_ORDER}"
    )


def threshold_scan_sql(spec: PairSpec, name: str, start_ns: int, end_ns: int) -> str:
    """Pairs of equal time of one group, and how many have both readings at each threshold."""
    unfiltered = replace(spec, filter_enabled=False)
    kept = ", ".join(f"countIf(d >= {t} AND u >= {t}) AS t{t}" for t in SCAN_THRESHOLDS)
    return (
        f"SELECT count() AS matched, {kept} "
        f"FROM ({pair_sql(unfiltered, [name], start_ns, end_ns)}){IN_ORDER}"
    )


def channel_statistics_sql(d_field: str, u_field: str, start_ns: int, end_ns: int) -> str:
    """The raw readings of the two channels of a scintillator, all of them (no threshold)."""
    parts = []
    for tag, field in (("d", string(d_field)), ("u", string(u_field))):
        parts.append(
            f"countIf(field = {field}) AS {tag}_n, "
            f"countIf(field = {field} AND value < 0) AS {tag}_negative, "
            f"minIf(value, field = {field}) AS {tag}_min, "
            f"maxIf(value, field = {field}) AS {tag}_max, "
            f"quantilesTDigestIf{QUANTILES}(value, field = {field}) AS {tag}_q"
        )
    return (
        f"SELECT {', '.join(parts)} "
        f"FROM detector_points WHERE {interval(start_ns, end_ns)} "
        f"AND field IN ({string(d_field)}, {string(u_field)}) AND value IS NOT NULL"
    )


def raw_histogram_sql(
    d_field: str, u_field: str, start_ns: int, end_ns: int, start: int, bins: int
) -> str:
    """The raw readings of the two channels in bins of 1 ADC from `start`."""
    d, u = string(d_field), string(u_field)
    return (
        f"SELECT if(field = {d}, 0, 1) AS endp, toInt32(floor(value)) AS b, count() AS n "
        f"FROM detector_points WHERE {interval(start_ns, end_ns)} "
        f"AND field IN ({d}, {u}) AND value >= {int(start)} AND value < {int(start + bins)} "
        "GROUP BY endp, b"
    )


def events_sql(start_ns: int, end_ns: int) -> str:
    """The triggers (events) in a period: their number (distinct times of the trigger numbers)
    and the first and last time."""
    return (
        "SELECT uniqExact(event_ns) AS n, min(event_ns) AS t_first, max(event_ns) AS t_last "
        "FROM detector_points "
        f"WHERE {interval(start_ns, end_ns)} AND field = 'trignum'"
    )


def readings_sql(start_ns: int, end_ns: int) -> str:
    return f"SELECT count() AS n FROM detector_points WHERE {interval(start_ns, end_ns)}"


def clean(value: Any) -> float | None:
    """A number from ClickHouse; None for the nan or inf of an aggregate over no rows."""
    if value is None:
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def summary(
    count: Any, negative: Any, low: Any, high: Any, quantiles: Sequence[Any]
) -> dict[str, Any]:
    """The summary() shape of x17-isotope-id from the aggregates of a query."""
    if int(count) == 0:
        return {"n": 0} | dict.fromkeys(SUMMARY_KEYS)
    p1, p50, p99 = (clean(q) for q in quantiles)
    return {
        "n": int(count),
        "min": clean(low),
        "p1": p1,
        "p50": p50,
        "p99": p99,
        "max": clean(high),
        "negative": int(negative),
    }
