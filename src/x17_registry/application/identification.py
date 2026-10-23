from collections.abc import Mapping
from typing import Any, Protocol

import numpy as np

from x17_registry.application.channels import PMT_CHANNELS

MAX_READINGS = 200_000


class IdentifierUnavailable(RuntimeError):
    pass


class Identifier(Protocol):
    threshold: float
    min_pairs: int

    def identify(
        self,
        times: Any,
        fields: Any,
        adc: Any,
        groups: Mapping[str, tuple[str, str]],
    ) -> list[dict[str, Any]]: ...


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


def identify_readings(
    identifier: Identifier, readings: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """One result per scintillator from rows of {event_ns, field, value}."""
    times = np.array([int(row["event_ns"]) for row in readings], dtype=np.int64)
    fields = np.array([row["field"] for row in readings], dtype=str)
    adc = np.array([float(row["value"]) for row in readings], dtype=np.float64)
    return identifier.identify(times, fields, adc, scintillator_groups())
