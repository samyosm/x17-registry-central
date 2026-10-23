from collections.abc import Mapping
from typing import Any, Protocol

from x17_registry.adapters.detector_sql import PairSpec
from x17_registry.adapters.registry import RegistryStore
from x17_registry.application.channels import PMT_CHANNELS


class IdentifierUnavailable(RuntimeError):
    pass


class Identifier(Protocol):
    """The part of x17-isotope-id's Identifier that the registry uses."""

    threshold: float
    min_pairs: int
    filter: Mapping[str, Any]
    raw_start: int
    raw_bins: int

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
