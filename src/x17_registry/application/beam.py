import re
from datetime import datetime
from typing import Any

MEASUREMENTS = {
    "energy": re.compile(r"Beam Energy E = (\d+(?:\.\d+)?) (MeV|keV|GeV)"),
    "current": re.compile(r"Current I = (\d+(?:\.\d+)?) (nA|uA|mA|A)"),
    "charge": re.compile(r"Integrated Charge Q = (\d+(?:\.\d+)?) (nC|uC|mC|C)"),
}


def _time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _status(record: dict[str, Any]) -> str | None:
    payload = record["raw"].get("payload")
    if not isinstance(payload, dict):
        return None
    value = payload.get("status")
    return value.lower() if isinstance(value, str) and value.upper() in ("ON", "OFF") else None


def _measurement(comment: str | None, name: str) -> str | None:
    for part in (comment or "").split(" | "):
        match = MEASUREMENTS[name].fullmatch(part.strip())
        if match:
            return f"{match.group(1)} {match.group(2)}"
    return None


def beam_context(
    records: list[dict[str, Any]], start: str, end: str | None
) -> dict[str, Any]:
    beginning = _time(start)
    finish = _time(end) if end is not None else None
    current: dict[str, Any] | None = None
    changes: list[dict[str, Any]] = []
    sessions: list[dict[str, Any]] = []
    observed_off = False
    segment_start = beginning
    current_during_run = False

    for record in records:
        status = _status(record)
        if status is None or not record["event_time"]:
            continue
        at = _time(record["event_time"])
        if at < beginning:
            current = record
            continue
        if finish is not None and at >= finish:
            break
        if current is not None and _status(current) == "on" and at > segment_start:
            sessions.append(current)
        if status == "off":
            observed_off = True
        changes.append(
            {
                "at": record["event_time"],
                "status": status,
                "publicId": record["raw"].get("public_id"),
            }
        )
        current = record
        segment_start = at
        current_during_run = True

    if current_during_run and current is not None and _status(current) == "on" and (
        finish is None or finish > segment_start
    ):
        sessions.append(current)

    active = sessions[-1] if sessions else None
    comment = active["raw"].get("comment") if active else None
    notes = []
    if current is not None:
        if not changes:
            notes.append("No beam change was logged during this run; its status is unknown.")
        if active is not None:
            notes.append("Current and charge describe a beam session and are entered at beam OFF.")
        notes.append("Logbook entries are not instrument telemetry.")
    return {
        "status": "on" if sessions else "off" if observed_off else "unknown",
        "source": (
            f"Control-room logbook / last status entry {current['event_time']}"
            if current is not None
            else "No beam entry in the logbook for this interval"
        ),
        "energy": _measurement(comment, "energy"),
        "current": _measurement(comment, "current"),
        "charge": _measurement(comment, "charge"),
        "note": " ".join(notes) if notes else None,
        "events": changes,
    }
