from collections.abc import Mapping
from typing import Any, Protocol

from x17_registry.adapters.detector_sql import PairSpec
from x17_registry.adapters.registry import RegistryStore
from x17_registry.application.channels import PMT_CHANNELS


class IdentifierUnavailable(RuntimeError):
    pass


class InsufficientData(ValueError):
    """The window or its reference holds too few events to say anything."""


class Identifier(Protocol):
    """The part of x17-isotope-id's Identifier that the registry uses."""

    threshold: float
    min_pairs: int
    filter: Mapping[str, Any]
    raw_start: int
    raw_bins: int

    excess: Any  # the library's ExcessAnalyzer, None if the config has no [excess] section

    def pedestal(self, field: str) -> float: ...

    def scale(self, group: str | None = None) -> float: ...

    def bin_layout(self) -> tuple[float, float, int]: ...

    def identify_histograms(
        self, histograms: Mapping[str, tuple[Any, int]]
    ) -> list[dict[str, Any]]: ...

    def diagnose_histogram(
        self,
        counts: Any,
        pair_summary: dict[str, Any],
        d: dict[str, Any],
        u: dict[str, Any],
        matched: int,
        below: int,
        above: int,
        zero: int,
        raw: dict[str, Any] | None = None,
        group: str | None = None,
    ) -> dict[str, Any]: ...


def scintillator_groups() -> dict[str, tuple[str, str]]:
    """Scintillator number -> (D-end channel, U-end channel), as grouped on the Events page."""
    ends: dict[str, dict[str, str]] = {}
    for field, (label, end) in PMT_CHANNELS.items():
        ends.setdefault(label[1:], {})[end] = field
    return {
        number: (pair["D"], pair["U"])
        for number, pair in sorted(ends.items(), key=lambda item: int(item[0]))
    }


def load_identifier(config_path: str | None) -> Identifier:
    """Load the isotope-id library and its templates; raise IdentifierUnavailable if not set up."""
    if not config_path:
        raise IdentifierUnavailable("X17_ISOTOPE_ID_CONFIG is not set.")
    try:
        from isotope_id import Identifier as Library  # type: ignore[import-untyped]
    except ImportError as error:
        raise IdentifierUnavailable(
            "The x17-isotope-id package is not installed (uv sync --extra isotope)."
        ) from error
    try:
        return Library.from_config(config_path)  # type: ignore[no-any-return]
    except (OSError, KeyError, ValueError) as error:
        raise IdentifierUnavailable(
            f"The identifier templates could not be loaded: {error}"
        ) from error


def pair_spec(identifier: Identifier) -> PairSpec:
    """What the database has to compute for the identifier: bins, acquisition filter, pedestals."""
    groups = scintillator_groups()
    low, high, bins = identifier.bin_layout()
    return PairSpec(
        groups=groups,
        low=low,
        high=high,
        bins=bins,
        filter_enabled=bool(identifier.filter["enabled"]),
        filter_mode=str(identifier.filter["mode"]),
        filter_threshold=float(identifier.filter["threshold"]),
        pedestals={field: identifier.pedestal(field) for pair in groups.values() for field in pair},
        scales={name: identifier.scale(name) for name in groups},
    )


def identify_period(
    store: RegistryStore,
    identifier: Identifier,
    start_ns: int,
    end_ns: int,
    timeout: float | None = None,
) -> list[dict[str, Any]]:
    """One result per scintillator from every reading of the period."""
    histograms = store.pair_histograms(pair_spec(identifier), start_ns, end_ns, timeout)
    return identifier.identify_histograms(histograms)


def diagnose_period(
    store: RegistryStore,
    identifier: Identifier,
    scintillator: str,
    start_ns: int,
    end_ns: int,
    timeout: float | None = None,
) -> dict[str, Any] | None:
    """Data-scale diagnostics of one scintillator; None if the scintillator is unknown."""
    spec = pair_spec(identifier)
    if scintillator not in spec.groups:
        return None
    inputs = store.pair_diagnostics(
        spec, scintillator, start_ns, end_ns, identifier.raw_start, identifier.raw_bins, timeout
    )
    scan = inputs.pop("scan")
    return identifier.diagnose_histogram(**inputs, group=scintillator) | {"scan": scan}


def excess_analyzer(identifier: Identifier) -> Any:
    analyzer = getattr(identifier, "excess", None)
    if analyzer is None:
        raise IdentifierUnavailable(
            "The excess identification is not configured: no [excess] section in config.toml."
        )
    return analyzer


def excess_spec(identifier: Identifier) -> PairSpec:
    """What the database counts for the excess identification: pairs with both PMTs high."""
    analyzer = excess_analyzer(identifier)
    groups = scintillator_groups()
    return PairSpec(
        groups=groups,
        low=0.0,
        high=float(analyzer.high),
        bins=int(analyzer.bins),
        filter_enabled=True,
        filter_mode="both",
        filter_threshold=float(analyzer.threshold),
        pedestals={
            field: float(analyzer.pedestal(field)) for pair in groups.values() for field in pair
        },
    )


def count_period(
    store: RegistryStore, spec: PairSpec, start_ns: int, end_ns: int, timeout: float | None
) -> dict[str, Any]:
    """The events and the pair histogram of every bar in a period."""
    histograms = store.pair_histograms(spec, start_ns, end_ns, timeout)
    events, seconds = store.event_span(start_ns, end_ns, timeout)
    return {
        "events": events,
        "seconds": seconds,
        "histograms": {name: counts for name, (counts, _pairs) in histograms.items()},
    }


def identify_excess_period(
    store: RegistryStore,
    identifier: Identifier,
    window: tuple[int, int],
    reference: tuple[int, int],
    timeout: float | None = None,
) -> dict[str, Any]:
    """Analyse a window against a reference period; the counting happens in the database."""
    analyzer = excess_analyzer(identifier)
    spec = excess_spec(identifier)
    counted_window = count_period(store, spec, *window, timeout)
    counted_reference = count_period(store, spec, *reference, timeout)
    try:
        result: dict[str, Any] = analyzer.analyze(spec.groups, counted_window, counted_reference)
    except ValueError as error:
        if type(error).__name__ == "NotEnoughEvents":
            raise InsufficientData(str(error)) from error
        raise
    result["spans"] = (counted_window["seconds"], counted_reference["seconds"])
    return result


RATE_RATIO = 1.33  # window and reference event rates may differ by this factor before a warning


def excess_response(
    result: dict[str, Any], context: dict[str, Any], analyzer: Any
) -> dict[str, Any]:
    """The API answer (camelCase) from an analysis, the periods and how the reference was chosen."""
    (window_from, window_to), (ref_from, ref_to) = context["window"], context["reference"]
    # Rates are over the time that holds data: a reference reaching back before the first
    # reading must not look slow.
    window_seconds, ref_seconds = (
        span or nominal for span, nominal in zip(result["spans"], context["durations"], strict=True)
    )
    events = result["events"]
    window_rate, ref_rate = events["window"] / window_seconds, events["reference"] / ref_seconds
    warnings = []
    ratio = window_rate / ref_rate if ref_rate else float("inf")
    if not 1 / RATE_RATIO < ratio < RATE_RATIO:
        warnings.append(
            f"The event rate differs ({window_rate:.1f} Hz in the window, {ref_rate:.1f} Hz in the "
            "reference): the trigger configuration may have changed, which makes the comparison "
            "unreliable."
        )
    if result["verdict"].get("warning"):
        warnings.append(result["verdict"]["warning"])
    bad = [bar["bar"] for bar in result["bars"] if not bar["usable"]]
    return {
        "from": window_from,
        "to": window_to,
        "reference": {
            "from": ref_from, "to": ref_to, "mode": context["mode"], "events": events["reference"],
            "rate": ref_rate,
        },
        "events": {"window": events["window"], "rate": window_rate, "scale": events["scale"]},
        "settings": {
            "threshold": analyzer.threshold,
            "binWidth": float(analyzer.high) / int(analyzer.bins),
            "max": float(analyzer.high),
            "detectionTs": analyzer.detection_ts,
            "marginTs": analyzer.margin_ts,
        },
        "bars": [
            {
                "scintillator": str(bar["bar"]), "usable": bar["usable"], "reason": bar["reason"],
                "pairs": bar["pairs"], "expected": bar["expected"],
                "excess": bar["excess"], "z": bar["z"],
            }
            for bar in result["bars"]
        ],
        "unusableBars": bad,
        "detection": {
            "pairs": result["detection"]["pairs"], "expected": result["detection"]["expected"],
            "excess": result["detection"]["excess"], "rateZ": result["detection"]["rate_z"],
            "barsAbove3": result["detection"]["bars_above_3"],
        },
        "sources": [
            {
                "name": source["name"], "ts": source["ts"], "centre": source["centre"],
                "plausible": source["plausible"], "activityBq": source["activity_bq"],
            }
            for source in result["sources"]
        ],
        "verdict": {
            key: result["verdict"][key] for key in ("code", "source", "candidates", "centre")
        },
        "spectrum": {"edges": result["edges"], **result["spectrum"]},
        "warnings": warnings,
    }
