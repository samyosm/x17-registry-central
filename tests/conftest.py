import os
from pathlib import Path
from uuid import uuid4

import pytest
from embedded_clickhouse import EmbeddedClickHouse
from fastapi.testclient import TestClient
from pydantic import SecretStr

from x17_registry.adapters.clickhouse import ClickHouseClient
from x17_registry.adapters.registry import RegistryStore
from x17_registry.api.app import create_app
from x17_registry.config import PollSettings, Settings

ROOT = Path(__file__).resolve().parents[2]


def server_settings(database: str) -> Settings:
    return Settings(
        _env_file=None,
        clickhouse_url=os.environ.get("X17_TEST_CLICKHOUSE_URL", "http://127.0.0.1:8123"),
        clickhouse_database=database,
        clickhouse_user=os.environ.get("X17_TEST_CLICKHOUSE_USER", "default"),
        clickhouse_password=SecretStr(os.environ.get("X17_TEST_CLICKHOUSE_PASSWORD", "")),
        clickhouse_timeout_seconds=5,
        log_level="info",
        host="127.0.0.1",
        port=8000,
        api_prefix="/api/v1",
        api_token=SecretStr("test-token"),
        page_size=5,
        max_page_size=100,
    )


def poll_settings(database: str, **overrides) -> PollSettings:
    values = {
        "clickhouse_url": os.environ.get("X17_TEST_CLICKHOUSE_URL", "http://127.0.0.1:8123"),
        "clickhouse_database": f"x17_test_{uuid4().hex}",
        "clickhouse_user": os.environ.get("X17_TEST_CLICKHOUSE_USER", "default"),
        "clickhouse_password": SecretStr(os.environ.get("X17_TEST_CLICKHOUSE_PASSWORD", "")),
        "clickhouse_timeout_seconds": 5,
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
        "influx_measurement": "run_80",
        "influx_token": None,
        "influx_source_instance": None,
        "influx_start_at": None,
        "influx_overlap_seconds": 60,
        "influx_window_seconds": 3600,
        "influx_timeout_seconds": 5,
        "influx_backfill_batch_size": 1000,
    }
    values.update(overrides)
    return PollSettings(_env_file=None, **values)


@pytest.fixture
def store(tmp_path: Path) -> RegistryStore:
    url = os.environ.get("X17_TEST_CLICKHOUSE_URL")
    if not url:
        chdb = pytest.importorskip("chdb")
        client = EmbeddedClickHouse(chdb)
    else:
        client = ClickHouseClient(
            url, f"x17_test_{uuid4().hex}",
            os.environ.get("X17_TEST_CLICKHOUSE_USER", "default"),
            os.environ.get("X17_TEST_CLICKHOUSE_PASSWORD", ""), 5,
        )
    result = RegistryStore(client)
    result.initialize()
    yield result
    if not url:
        client.close()


@pytest.fixture
def client(store: RegistryStore):
    with TestClient(create_app(server_settings(store.client.database), store)) as result:
        result.headers["Authorization"] = "Bearer test-token"
        yield result
