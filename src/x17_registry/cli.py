import argparse
import json
import logging
import threading

import uvicorn

from x17_registry.adapters.readers import InfluxReader, LogbookReader, TriggerReader
from x17_registry.adapters.registry import RegistryStore
from x17_registry.api.app import create_app
from x17_registry.application.polling import IdentityProcessor, PollJob, SourceReader
from x17_registry.config import CommonSettings, PollSettings, Settings


def build_job(settings: PollSettings) -> PollJob:
    store = RegistryStore(settings.database_path, settings.sqlite_timeout_seconds)
    store.initialize()
    readers: dict[str, SourceReader] = {
        "trigger": TriggerReader(settings),
        "logbook": LogbookReader(settings),
    }
    if settings.influx_enabled:
        readers["influx"] = InfluxReader(settings)
    return PollJob(readers, IdentityProcessor(), store)


def main() -> None:
    parser = argparse.ArgumentParser(description="X17 registry service")
    parser.add_argument("command", choices=("serve", "poll", "poll-once", "rebuild-run-summaries"))
    command = parser.parse_args().command
    if command == "serve":
        settings = Settings()
        logging.basicConfig(level=settings.log_level.upper())
        uvicorn.run(
            create_app(settings),
            host=settings.host,
            port=settings.port,
            log_level=settings.log_level,
        )
        return
    if command == "rebuild-run-summaries":
        common_settings = CommonSettings()
        store = RegistryStore(common_settings.database_path, common_settings.sqlite_timeout_seconds)
        store.initialize()
        print(json.dumps({"runs": store.rebuild_run_summaries()}))
        return
    poll_settings = PollSettings()
    logging.basicConfig(level=poll_settings.log_level.upper())
    job = build_job(poll_settings)
    if command == "poll-once":
        result = job.run_once()
        print(json.dumps(result))
        if "error" in result.values():
            raise SystemExit(1)
        return
    stop = threading.Event()
    try:
        while not stop.is_set():
            logging.info("Poll result: %s", job.run_once())
            stop.wait(poll_settings.poll_interval_seconds)
    except KeyboardInterrupt:
        stop.set()
