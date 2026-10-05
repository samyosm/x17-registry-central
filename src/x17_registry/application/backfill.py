from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from http.client import IncompleteRead, RemoteDisconnected
from threading import Event, Lock, Thread
from time import monotonic

from x17_registry.adapters.registry import RegistryStore
from x17_registry.application.polling import IdentityProcessor, SeekableInfluxReader
from x17_registry.config import PollSettings

STREAM_FAILURES = (IncompleteRead, RemoteDisconnected, ConnectionResetError, TimeoutError)


def _time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _checkpoint_keys(source_instance: str) -> tuple[str, str]:
    return f"influx-backfill-reverse:{source_instance}", f"influx-backfill-upper:{source_instance}"


def _progress_percent(lower: datetime, upper: datetime, cursor: datetime) -> float:
    total = (upper - lower).total_seconds()
    if total <= 0:
        return 100.0
    scanned = 100 * (upper - cursor).total_seconds() / total
    return round(min(100, max(0, scanned)), 2)


def backfill_influx(
    reader: SeekableInfluxReader,
    store: RegistryStore,
    settings: PollSettings,
    min_window_seconds: float,
    progress_seconds: float,
    progress: Callable[[dict[str, object]], None],
) -> dict[str, int | str]:
    if settings.influx_start_at is None or settings.influx_source_instance is None:
        raise ValueError("InfluxDB start time and source instance are required for backfill.")
    if min_window_seconds <= 0 or min_window_seconds > settings.influx_window_seconds:
        raise ValueError("Backfill minimum window must be positive and at most the query window.")
    if progress_seconds <= 0:
        raise ValueError("Backfill progress interval must be positive.")

    lower = settings.influx_start_at.astimezone(UTC)
    cursor_key, upper_key = _checkpoint_keys(settings.influx_source_instance)
    saved_upper = store.checkpoint(upper_key)
    upper = _time(saved_upper) if saved_upper is not None else datetime.now(UTC)
    if saved_upper is None:
        store.set_checkpoint(upper_key, upper.isoformat())
    saved_cursor = store.checkpoint(cursor_key)
    cursor = _time(saved_cursor) if saved_cursor is not None else upper
    if saved_cursor is None:
        store.set_checkpoint(cursor_key, cursor.isoformat())

    started = monotonic()
    output_lock = Lock()
    stopped = Event()
    state: dict[str, object] = {
        "cursor": cursor.isoformat(),
        "percent_time_scanned": _progress_percent(lower, upper, cursor),
        "window_records": 0,
        "records_saved": 0,
    }

    def report(event: str, **details: object) -> None:
        with output_lock:
            progress(
                {
                    "event": event,
                    "elapsed_seconds": round(monotonic() - started, 1),
                    **state,
                    **details,
                }
            )

    def heartbeat() -> None:
        while not stopped.wait(progress_seconds):
            report("heartbeat")

    monitor = Thread(target=heartbeat, daemon=True)
    monitor.start()
    processor = IdentityProcessor()
    window_seconds = float(settings.influx_window_seconds)
    windows = 0
    skipped = 0
    records_read = 0
    records_saved = 0
    try:
        report("started", oldest_requested=lower.isoformat(), newest=upper.isoformat())
        while cursor > lower:
            start = max(cursor - timedelta(seconds=window_seconds), lower)
            state.update(
                window_start=start.isoformat(),
                window_end=cursor.isoformat(),
                window_records=0,
                window_seconds=window_seconds,
            )
            report("window_started")
            count = 0
            try:
                for record in reader.read_window(start, cursor):
                    if store.save(record, processor.process(record)):
                        records_saved += 1
                    count += 1
                    records_read += 1
                    state["window_records"] = count
                    state["records_saved"] = records_saved
            except STREAM_FAILURES as error:
                if window_seconds <= min_window_seconds:
                    raise RuntimeError(
                        f"InfluxDB stream failed at the minimum backfill window "
                        f"({min_window_seconds:g}s); reverse checkpoint remains "
                        f"{cursor.isoformat()}."
                    ) from error
                window_seconds = max(window_seconds / 2, min_window_seconds)
                report("retry_smaller_window", next_window_seconds=window_seconds)
                continue

            cursor = start
            store.set_checkpoint(cursor_key, cursor.isoformat())
            windows += 1
            state.update(
                cursor=cursor.isoformat(),
                percent_time_scanned=_progress_percent(lower, upper, cursor),
            )
            report("window_completed", records_in_window=count, records_read=records_read)
            if count:
                window_seconds = min(window_seconds * 2, settings.influx_window_seconds)
                continue

            report("finding_previous_point")
            previous = reader.previous_point_before(lower, cursor)
            if previous is None:
                cursor = lower
                store.set_checkpoint(cursor_key, cursor.isoformat())
                state.update(cursor=cursor.isoformat(), percent_time_scanned=100.0)
                break
            next_cursor = min(cursor, previous + timedelta(microseconds=1))
            if next_cursor <= lower or next_cursor >= cursor:
                raise ValueError("InfluxDB returned an invalid previous point time.")
            cursor = next_cursor
            store.set_checkpoint(cursor_key, cursor.isoformat())
            skipped += 1
            window_seconds = float(settings.influx_window_seconds)
            state.update(
                cursor=cursor.isoformat(),
                percent_time_scanned=_progress_percent(lower, upper, cursor),
            )
            report("skipped_gap", previous_point=previous.isoformat())

        forward = store.checkpoint("influx")
        if forward is None or _time(forward) < upper:
            store.set_checkpoint("influx", upper.isoformat())
        report(
            "completed",
            windows=windows,
            skipped_gaps=skipped,
            records_read=records_read,
            records_saved=records_saved,
        )
        return {
            "windows": windows,
            "skipped_gaps": skipped,
            "records_read": records_read,
            "records_saved": records_saved,
            "checkpoint": cursor.isoformat(),
        }
    finally:
        stopped.set()
        monitor.join()
