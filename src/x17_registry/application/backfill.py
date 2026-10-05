from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from http.client import IncompleteRead, RemoteDisconnected

from x17_registry.adapters.registry import RegistryStore
from x17_registry.application.polling import IdentityProcessor, SeekableInfluxReader
from x17_registry.config import PollSettings

STREAM_FAILURES = (IncompleteRead, RemoteDisconnected, ConnectionResetError, TimeoutError)


def backfill_influx(
    reader: SeekableInfluxReader,
    store: RegistryStore,
    settings: PollSettings,
    min_window_seconds: float,
    progress: Callable[[dict[str, object]], None],
) -> dict[str, int | str]:
    if settings.influx_start_at is None:
        raise ValueError("InfluxDB start time is required for backfill.")
    if min_window_seconds <= 0 or min_window_seconds > settings.influx_window_seconds:
        raise ValueError("Backfill minimum window must be positive and at most the query window.")
    until = datetime.now(UTC)
    processor = IdentityProcessor()
    window_seconds = float(settings.influx_window_seconds)
    windows = 0
    skipped = 0
    records = 0
    while True:
        checkpoint = store.checkpoint("influx")
        cursor = (
            datetime.fromisoformat(checkpoint.replace("Z", "+00:00"))
            if checkpoint is not None
            else settings.influx_start_at
        )
        if cursor >= until:
            break
        stop = min(cursor + timedelta(seconds=window_seconds), until)
        count = 0
        try:
            for record in reader.read_window(cursor, stop):
                store.save(record, processor.process(record))
                count += 1
        except STREAM_FAILURES as error:
            if window_seconds <= min_window_seconds:
                raise RuntimeError(
                    f"InfluxDB stream failed at the minimum backfill window "
                    f"({min_window_seconds:g}s); checkpoint remains "
                    f"{checkpoint or cursor.isoformat()}."
                ) from error
            window_seconds = max(window_seconds / 2, min_window_seconds)
            progress({"retry_from": cursor.isoformat(), "window_seconds": window_seconds})
            continue
        store.set_checkpoint("influx", stop.isoformat())
        windows += 1
        records += count
        progress({"window": windows, "records": count, "checkpoint": stop.isoformat()})
        if count:
            continue
        next_point = reader.next_point_at(stop, until)
        if next_point is None:
            store.set_checkpoint("influx", until.isoformat())
            break
        if next_point <= stop or next_point >= until:
            raise ValueError("InfluxDB returned an invalid next point time.")
        store.set_checkpoint("influx", next_point.isoformat())
        window_seconds = float(settings.influx_window_seconds)
        skipped += 1
        progress({"skipped_to": next_point.isoformat()})
    return {
        "windows": windows,
        "skipped_gaps": skipped,
        "records_read": records,
        "checkpoint": store.checkpoint("influx") or until.isoformat(),
    }
