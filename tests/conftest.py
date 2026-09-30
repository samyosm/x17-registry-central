from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from x17_registry.adapters.registry import RegistryStore
from x17_registry.api.app import create_app
from x17_registry.config import PollSettings, Settings

ROOT = Path(__file__).resolve().parents[2]


def server_settings(database_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        database_path=database_path,
        sqlite_timeout_seconds=5,
        log_level="info",
        host="127.0.0.1",
        port=8000,
        api_prefix="/api/v1",
        api_token=SecretStr("test-token"),
        page_size=5,
        max_page_size=100,
    )


def poll_settings(database_path: Path, **overrides) -> PollSettings:
    values = {
        "database_path": database_path,
        "sqlite_timeout_seconds": 5,
        "log_level": "info",
        "poll_interval_seconds": 5,
        "trigger_history_path": ROOT / "triggerApp/trigger/data/trigger_history.json",
        "trigger_config_path": ROOT / "triggerApp/trigger/data/trigger_config.json",
        "trigger_programmed_path": ROOT / "triggerApp/trigger/data/trigger_programmed.json",
        "trigger_source_instance": "trigger-copy",
        "logbook_database_path": ROOT / "x17-ops/x17-logbook/data/logbook.db",
        "logbook_source_instance": "logbook-copy",
        "influx_enabled": False,
        "influx_url": None,
        "influx_org": None,
        "influx_bucket": None,
        "influx_token": None,
        "influx_source_instance": None,
        "influx_start_at": None,
        "influx_overlap_seconds": 60,
        "influx_window_seconds": 3600,
        "influx_timeout_seconds": 5,
    }
    values.update(overrides)
    return PollSettings(_env_file=None, **values)


@pytest.fixture
def store(tmp_path: Path) -> RegistryStore:
    result = RegistryStore(tmp_path / "registry.sqlite3", 5)
    result.initialize()
    return result


@pytest.fixture
def client(store: RegistryStore):
    with TestClient(create_app(server_settings(store.path), store)) as result:
        result.headers["Authorization"] = "Bearer test-token"
        yield result
