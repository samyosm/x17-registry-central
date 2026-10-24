"""The trigger configuration over time, from the TriggerApp history.

Each history entry is a configuration written at a time and valid until the next write. The
rate and the spectra of the pairs depend on it (mode, majority, masks, thresholds), so a window is
compared with a reference period recorded with the same configuration. The history is what was saved
by TriggerApp: the hardware readback is not verified.
"""

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from x17_registry.adapters.registry import epoch_ns

FIELDS = ("tmod", "maj", "thresholds", "masks", "invert")
FOREVER = 1 << 62
LABELS = {
    "tmod": "trigger mode",
    "maj": "majority",
    "thresholds": "thresholds",
    "masks": "masks",
    "invert": "inverted channels",
}


@dataclass(frozen=True)
class Epoch:
    start_ns: int
    end_ns: int
    config: dict[str, Any]
    since: str
    title: str | None


def configuration_of(raw: dict[str, Any]) -> dict[str, Any]:
    return {field: raw.get(field) for field in FIELDS}


def epochs(records: list[dict[str, Any]]) -> list[Epoch]:
    """The periods of constant configuration; consecutive entries with the same one are merged."""
    entries = sorted(
        (epoch_ns(record["event_time"]), record) for record in records if record.get("event_time")
    )
    merged: list[Epoch] = []
    for start, record in entries:
        config = configuration_of(record["raw"])
        if merged and merged[-1].config == config:
            continue
        title = record["raw"].get("title")
        merged.append(Epoch(start, FOREVER, config, record["event_time"], title))
    return [
        Epoch(item.start_ns, merged[i + 1].start_ns if i + 1 < len(merged) else FOREVER,
              item.config, item.since, item.title)
        for i, item in enumerate(merged)
    ]


def differences(first: dict[str, Any], second: dict[str, Any]) -> list[str]:
    """What differs between two configurations, in words."""
    found = []
    for field in FIELDS:
        a, b = first.get(field), second.get(field)
        if a == b:
            continue
        if field in ("tmod", "maj"):
            found.append(f"{LABELS[field]} {a} against {b}")
        elif field == "thresholds" and isinstance(a, dict) and isinstance(b, dict):
            changed = sum(a.get(key) != b.get(key) for key in set(a) | set(b))
            found.append(f"thresholds differ on {changed} channels")
        else:
            found.append(f"{LABELS[field]} differ")
    return found


def overlapping(items: list[Epoch], start: int, end: int) -> list[tuple[Epoch, int]]:
    """(epoch, nanoseconds inside [start, end)) of the epochs that overlap the period."""
    found = []
    for item in items:
        inside = min(end, item.end_ns) - max(start, item.start_ns)
        if inside > 0:
            found.append((item, inside))
    return found


def main_epoch(items: list[Epoch], start: int, end: int) -> Epoch | None:
    """The epoch covering most of the period (the first one at a tie)."""
    covering = overlapping(items, start, end)
    return max(covering, key=lambda pair: pair[1])[0] if covering else None


def reference_candidates(
    items: list[Epoch], config: dict[str, Any], start: int, end: int, length: int
) -> Iterator[tuple[str, int, int]]:
    """Reference periods with the configuration `config`, nearest first: before the window
    (going back through earlier periods of that configuration), then after it."""
    same = [(item.start_ns, item.end_ns) for item in items if item.config == config]
    for low, high in sorted(same, reverse=True):
        high = min(high, start)
        if high > low:
            yield "before", max(low, high - length), high
    for low, high in sorted(same):
        low = max(low, end)
        if high > low:
            yield "after", low, min(high, low + length)


def describe(item: Epoch | None) -> dict[str, Any] | None:
    if item is None:
        return None
    thresholds = item.config.get("thresholds")
    values = sorted(set(thresholds.values())) if isinstance(thresholds, dict) else []
    return {
        "since": item.since,
        "title": item.title,
        "mode": item.config.get("tmod"),
        "majority": item.config.get("maj"),
        "thresholds": values,
    }


def report(
    items: list[Epoch], window: tuple[int, int], reference: tuple[int, int]
) -> dict[str, Any]:
    """The trigger configuration of the window and of the reference, and the warnings to show."""
    in_window, in_reference = main_epoch(items, *window), main_epoch(items, *reference)
    warnings = []
    if in_window is None or in_reference is None:
        warnings.append(
            "The trigger history does not cover "
            + ("the window" if in_window is None else "the reference")
            + ": the trigger configuration could not be compared."
        )
        same, found = None, []
    else:
        found = differences(in_window.config, in_reference.config)
        same = not found
        if found:
            warnings.append(
                "The trigger configuration differs between the window and the reference ("
                + "; ".join(found) + "): the comparison is unreliable."
            )
    periods = (("window", window, in_window), ("reference", reference, in_reference))
    for name, (start, end), epoch in periods:
        changes = [item for item, _ in overlapping(items, start, end) if item is not epoch]
        if changes:
            warnings.append(
                f"The trigger configuration changed {len(changes)} time(s) during the {name}: "
                "only the configuration that covers most of it is used."
            )
    return {
        "known": same is not None,
        "same": same,
        "differences": found,
        "window": describe(in_window),
        "reference": describe(in_reference),
        "warnings": warnings,
    }
