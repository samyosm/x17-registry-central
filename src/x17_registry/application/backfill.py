from collections import deque
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from http.client import IncompleteRead, RemoteDisconnected
from threading import Event, Lock, Thread
from time import monotonic

from x17_registry.adapters.registry import RegistryStore
from x17_registry.application.polling import SeekableInfluxReader, SourceRecord
from x17_registry.config import BackfillSettings

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
    settings: BackfillSettings,
    progress: Callable[[dict[str, object]], None],
) -> dict[str, int | str]:
    if settings.influx_start_at is None or settings.influx_source_instance is None:
        raise ValueError("InfluxDB start time and source instance are required for backfill.")
    if settings.influx_backfill_min_window_seconds > settings.influx_window_seconds:
        raise ValueError("Backfill minimum window must be positive and at most the query window.")

    requested_lower = settings.influx_start_at.astimezone(UTC)
    lower = requested_lower
    if settings.influx_backfill_runs_only:
        runs = store.trigger_runs()
        if not runs:
            raise ValueError("TriggerApp runs must be imported before run-limited backfill.")
        lower = max(requested_lower, min(_time(run["event_time"]) for run in runs))
    cursor_key, upper_key = _checkpoint_keys(settings.influx_source_instance)
    saved_upper = store.checkpoint(upper_key)
    upper = _time(saved_upper) if saved_upper is not None else datetime.now(UTC)
    if lower > upper:
        raise ValueError("The earliest TriggerApp run starts after the backfill upper bound.")
    if saved_upper is None:
        store.set_checkpoint(upper_key, upper.isoformat())
    saved_cursor = store.checkpoint(cursor_key)
    cursor = _time(saved_cursor) if saved_cursor is not None else upper
    if saved_cursor is None:
        store.set_checkpoint(cursor_key, cursor.isoformat())

    started = monotonic()
    output_lock = Lock()
    stopped = Event()
    timings = {"influx": 0.0, "sqlite": 0.0}
    phase_started = monotonic()
    state: dict[str, object] = {
        "cursor": cursor.isoformat(),
        "percent_time_scanned": _progress_percent(lower, upper, cursor),
        "window_records": 0,
        "records_read": 0,
        "records_saved": 0,
        "pending_batches": 0,
        "write_batches": 0,
        "phase": "starting",
    }

    def report(event: str, **details: object) -> None:
        with output_lock:
            phase_elapsed = monotonic() - phase_started
            progress(
                {
                    "event": event,
                    "elapsed_seconds": round(monotonic() - started, 1),
                    **state,
                    "influx_seconds": round(timings["influx"], 3),
                    "sqlite_seconds": round(timings["sqlite"], 3),
                    "phase_seconds": round(phase_elapsed, 1),
                    **details,
                }
            )

    def phase(name: str) -> None:
        nonlocal phase_started
        state["phase"] = name
        phase_started = monotonic()

    def heartbeat() -> None:
        while not stopped.wait(settings.influx_backfill_progress_seconds):
            report("heartbeat")

    monitor = Thread(target=heartbeat, daemon=True)
    monitor.start()
    window_seconds = float(settings.influx_window_seconds)
    windows = 0
    skipped = 0
    records_read = 0
    records_saved = 0
    write_batches = 0
    writer = ThreadPoolExecutor(max_workers=1)
    pending: deque[Future[tuple[int, float]]] = deque()

    def write_batch(batch: list[SourceRecord]) -> tuple[int, float]:
        began = monotonic()
        saved = store.save_detector_backfill_batch(
            batch, settings.influx_backfill_sqlite_synchronous
        )
        return saved, monotonic() - began

    def collect_one() -> None:
        nonlocal records_saved
        phase("waiting_sqlite")
        try:
            saved, elapsed = pending.popleft().result()
        except Exception as error:
            raise RuntimeError(
                "SQLite backfill batch failed; checkpoint was not advanced."
            ) from error
        records_saved += saved
        timings["sqlite"] += elapsed
        state["records_saved"] = records_saved
        state["pending_batches"] = len(pending)
        phase("reading_influx")

    def submit(batch: list[SourceRecord]) -> None:
        nonlocal write_batches
        if not batch:
            return
        pending.append(writer.submit(write_batch, batch))
        state["pending_batches"] = len(pending)
        write_batches += 1
        state["write_batches"] = write_batches
        if len(pending) >= settings.influx_backfill_pending_batches:
            collect_one()

    def drain() -> None:
        while pending:
            collect_one()

    try:
        report(
            "started",
            oldest_requested=requested_lower.isoformat(),
            oldest_needed=lower.isoformat(),
            newest=upper.isoformat(),
        )
        while cursor > lower:
            start = max(cursor - timedelta(seconds=window_seconds), lower)
            state.update(
                window_start=start.isoformat(),
                window_end=cursor.isoformat(),
                window_records=0,
                window_seconds=window_seconds,
                write_batches=0,
            )
            timings["influx"] = 0.0
            timings["sqlite"] = 0.0
            write_batches = 0
            phase("window_start")
            report("window_started")
            count = 0
            batch: list[SourceRecord] = []
            try:
                phase("reading_influx")
                stream = iter(reader.read_window(start, cursor))
                while True:
                    phase("reading_influx")
                    read_started = monotonic()
                    try:
                        record = next(stream)
                    except StopIteration:
                        timings["influx"] += monotonic() - read_started
                        break
                    timings["influx"] += monotonic() - read_started
                    batch.append(record)
                    count += 1
                    records_read += 1
                    state["window_records"] = count
                    state["records_read"] = records_read
                    if len(batch) >= settings.influx_backfill_batch_size:
                        submit(batch)
                        batch = []
                submit(batch)
                drain()
            except STREAM_FAILURES as error:
                drain()
                if window_seconds <= settings.influx_backfill_min_window_seconds:
                    raise RuntimeError(
                        f"InfluxDB stream failed at the minimum backfill window "
                        f"({settings.influx_backfill_min_window_seconds:g}s); "
                        "reverse checkpoint remains "
                        f"{cursor.isoformat()}."
                    ) from error
                window_seconds = max(
                    window_seconds / 2, settings.influx_backfill_min_window_seconds
                )
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

            phase("finding_previous_point")
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
        writer.shutdown(wait=True)
        stopped.set()
        monitor.join()
