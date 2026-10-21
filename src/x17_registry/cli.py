import argparse
import json
import logging
import signal
import threading
from datetime import UTC, datetime
from time import monotonic
from typing import Any

import uvicorn

from x17_registry.adapters.clickhouse import ClickHouseClient
from x17_registry.adapters.readers import InfluxReader, LogbookReader, TriggerReader
from x17_registry.adapters.registry import RegistryStore
from x17_registry.api.app import create_app
from x17_registry.application.backfill import backfill_influx
from x17_registry.application.polling import IdentityProcessor, PollJob, SourceReader
from x17_registry.config import BackfillSettings, CommonSettings, PollSettings, Settings

BACKFILL_CHECKPOINT_PREFIX = "influx-backfill-"
ALWAYS_RECORDED = {"started", "retry_smaller_window", "finding_previous_point"}


def registry_store(settings: CommonSettings) -> RegistryStore:
    return RegistryStore(
        ClickHouseClient(
            settings.clickhouse_url,
            settings.clickhouse_database,
            settings.clickhouse_user,
            settings.clickhouse_password.get_secret_value(),
            settings.clickhouse_timeout_seconds,
        )
    )


def build_job(settings: PollSettings) -> PollJob:
    store = registry_store(settings)
    store.initialize()
    readers: dict[str, SourceReader] = {
        "trigger": TriggerReader(settings),
        "logbook": LogbookReader(settings),
    }
    if settings.influx_enabled:
        readers["influx"] = InfluxReader(settings)
    return PollJob(
        readers,
        IdentityProcessor(),
        store,
        settings.influx_backfill_batch_size,
        attempt_seconds=settings.influx_attempt_seconds,
        interval_seconds=settings.poll_interval_seconds,
    )


def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def parse_time(text: str) -> datetime:
    if text == "now":
        return datetime.now(UTC)
    moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def run_poller(settings: PollSettings, once: bool) -> None:
    job = build_job(settings)
    if once:
        result = job.run_once()
        print(json.dumps(result))
        if "error" in result.values():
            raise SystemExit(1)
        return
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    job.heartbeat("starting", lastCycleAt=None)
    try:
        while not stop.is_set():
            job.heartbeat("polling")
            result = job.run_once()
            logging.info("Poll result: %s", result)
            # While Influx is behind, keep reading without waiting between windows.
            while job.behind and "error" not in result.values() and not stop.is_set():
                job.heartbeat("catching_up", behind=sorted(job.behind))
                result = job.run_once(job.behind)
                logging.info("Catch-up poll: %s", result)
            job.heartbeat("waiting", lastCycleAt=utc_now(), lastResult=result, behind=[])
            stop.wait(settings.poll_interval_seconds)
    except KeyboardInterrupt:
        stop.set()
    finally:
        job.heartbeat("stopped", stoppedAt=utc_now())


def run_backfill(settings: BackfillSettings, since: str | None, restart: bool) -> None:
    if not settings.influx_enabled:
        raise SystemExit("X17_INFLUX_ENABLED must be true for backfill.")
    store = registry_store(settings)
    store.initialize()
    if since:
        settings = settings.model_copy(update={"influx_start_at": parse_time(since)})
    if restart:
        cleared = store.clear_checkpoints(BACKFILL_CHECKPOINT_PREFIX)
        print(json.dumps({"event": "reset", "cleared": cleared}), flush=True)

    started = utc_now()
    last_written = 0.0
    latest: dict[str, Any] = {}

    def record(state: str, **extra: object) -> None:
        store.set_status(
            "influx_backfill",
            {
                "state": state,
                "startedAt": started,
                "updatedAt": utc_now(),
                "since": settings.influx_start_at.isoformat() if settings.influx_start_at else None,
                "event": latest.get("event"),
                "phase": latest.get("phase"),
                "cursor": latest.get("cursor"),
                "percentScanned": latest.get("percent_time_scanned"),
                "windowStart": latest.get("window_start"),
                "windowEnd": latest.get("window_end"),
                "windowSeconds": latest.get("window_seconds"),
                "recordsRead": latest.get("records_read"),
                "recordsSaved": latest.get("records_saved"),
                "oldestNeeded": latest.get("oldest_needed"),
                "newest": latest.get("newest"),
                "elapsedSeconds": latest.get("elapsed_seconds"),
                **extra,
            },
        )

    def progress(update: dict[str, object]) -> None:
        nonlocal last_written
        print(json.dumps(update), flush=True)
        latest.update(update)
        if update["event"] == "completed":
            return
        if update["event"] in ALWAYS_RECORDED or monotonic() - last_written >= 5:
            last_written = monotonic()
            record("running")

    record("running")
    try:
        result = backfill_influx(InfluxReader(settings), store, settings, progress)
    except BaseException as error:
        record("failed", endedAt=utc_now(), error=f"{type(error).__name__}: {error}"[:500])
        raise
    record("completed", endedAt=utc_now())
    print(json.dumps(result))


def show_status(store: RegistryStore) -> None:
    print(
        json.dumps(
            {"checkpoints": store.checkpoints(), "status": store.statuses()},
            indent=2,
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="X17 registry service")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("serve", help="Run the HTTP API.")
    commands.add_parser("poll", help="Poll all sources continuously.")
    commands.add_parser("poll-once", help="Poll all sources one time.")
    backfill = commands.add_parser(
        "backfill-influx", help="Import historical detector points, newest first."
    )
    backfill.add_argument(
        "--since",
        metavar="TIME",
        help="Scan back only to this ISO time (e.g. 2026-10-09), overriding X17_INFLUX_START_AT.",
    )
    backfill.add_argument(
        "--restart",
        action="store_true",
        help="Forget the saved backfill position first, so it starts again from now.",
    )
    commands.add_parser("status", help="Print stored checkpoints and poller/backfill status.")
    commands.add_parser(
        "reset-backfill", help="Forget the saved backfill position (no data is deleted)."
    )
    move = commands.add_parser(
        "set-checkpoint", help="Move a checkpoint, for example the forward Influx scan."
    )
    move.add_argument("name", help="Checkpoint name as listed by the status command.")
    move.add_argument("value", help="ISO time, or 'now'.")
    args = parser.parse_args()
    command = args.command

    if command == "serve":
        settings = Settings()
        logging.basicConfig(level=settings.log_level.upper())
        uvicorn.run(
            create_app(settings),
            host=settings.host,
            port=settings.port,
            log_level=settings.log_level,
        )
    elif command == "backfill-influx":
        backfill_settings = BackfillSettings()
        logging.basicConfig(level=backfill_settings.log_level.upper())
        run_backfill(backfill_settings, args.since, args.restart)
    elif command in ("poll", "poll-once"):
        poll_settings = PollSettings()
        logging.basicConfig(level=poll_settings.log_level.upper())
        run_poller(poll_settings, once=command == "poll-once")
    else:
        common = CommonSettings()
        store = registry_store(common)
        store.initialize()
        if command == "status":
            show_status(store)
        elif command == "reset-backfill":
            print(json.dumps({"cleared": store.clear_checkpoints(BACKFILL_CHECKPOINT_PREFIX)}))
        else:
            value = parse_time(args.value).isoformat()
            known = {entry["name"] for entry in store.checkpoints()}
            if args.name not in known:
                raise SystemExit(f"Unknown checkpoint {args.name!r}; known: {sorted(known)}")
            store.set_checkpoint(args.name, value)
            print(json.dumps({"name": args.name, "value": value}))


if __name__ == "__main__":
    main()
