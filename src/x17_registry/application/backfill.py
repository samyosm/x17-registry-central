from collections.abc import Callable
from datetime import UTC, datetime

from x17_registry.adapters.registry import RegistryStore
from x17_registry.application.polling import IdentityProcessor, PollJob, SeekableInfluxReader
from x17_registry.config import PollSettings


def backfill_influx(
    reader: SeekableInfluxReader,
    store: RegistryStore,
    settings: PollSettings,
    progress: Callable[[dict[str, object]], None],
) -> dict[str, int | str]:
    if settings.influx_start_at is None:
        raise ValueError("InfluxDB start time is required for backfill.")
    until = datetime.now(UTC)
    job = PollJob({"influx": reader}, IdentityProcessor(), store)
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
        count = job.run_source("influx", until)
        windows += 1
        records += count
        checkpoint = store.checkpoint("influx")
        if checkpoint is None:
            raise RuntimeError("Influx backfill did not advance its checkpoint.")
        progress({"window": windows, "records": count, "checkpoint": checkpoint})
        if count:
            continue
        cursor = datetime.fromisoformat(checkpoint.replace("Z", "+00:00"))
        next_point = reader.next_point_at(cursor, until)
        if next_point is None:
            store.set_checkpoint("influx", until.isoformat())
            break
        if next_point <= cursor or next_point >= until:
            raise ValueError("InfluxDB returned an invalid next point time.")
        store.set_checkpoint("influx", next_point.isoformat())
        skipped += 1
        progress({"skipped_to": next_point.isoformat()})
    return {
        "windows": windows,
        "skipped_gaps": skipped,
        "records_read": records,
        "checkpoint": store.checkpoint("influx") or until.isoformat(),
    }
