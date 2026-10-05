# X17 Registry Central

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/). Run commands from this directory.

1. Run `uv sync --locked`.
1. Copy `.env.example` to `.env`. Set `X17_API_TOKEN`.
1. Run `uv run x17-registry serve` for the API.
1. Run `uv run x17-registry poll` in a second process to fetch sources at `X17_POLL_INTERVAL_SECONDS` intervals. Use `uv run x17-registry poll-once` from an external cron job if preferred.

For historical InfluxDB data, import TriggerApp history first, stop the regular poller, and set `X17_INFLUX_BACKFILL_BATCH_SIZE=10000`, `X17_INFLUX_BACKFILL_PENDING_BATCHES=4`, `X17_INFLUX_BACKFILL_RUNS_ONLY=true`, and `X17_INFLUX_BACKFILL_SQLITE_SYNCHRONOUS=NORMAL` in `.env`. Keep the minimum window and progress settings from `.env.example`; run `uv run x17-registry backfill-influx`. A bounded writer thread overlaps SQLite writes with Influx reads. Run-limited mode stops at the first retained TriggerApp start; it still copies all detector fields inside covered runs. The reverse checkpoint survives interruption. Progress shows per-window `influx_seconds`, `sqlite_seconds`, and `pending_batches`. Restart the regular poller when backfill finishes. WAL mode is already enabled. `NORMAL` preserves WAL consistency but can lose the latest committed batches after power failure; `OFF` can corrupt the registry database, so do not use it on the shared registry without a recoverable backup. The API generates CSV and JSON exports when requested.

The example paths point to the copied TriggerApp JSON and logbook SQLite files. Set `X17_INFLUX_ENABLED=true` and fill the Influx settings to query the detector store. The poller reads source files and queries InfluxDB; it writes only to `X17_DATABASE_PATH`.

The API uses `Authorization: Bearer <X17_API_TOKEN>`. Its routes and schemas are available at `http://127.0.0.1:8000/docs` with the example bind settings.

After upgrading a registry database created before run summaries existed, stop the API and poller, run `uv run x17-registry rebuild-run-summaries` once, then restart them. This updates only the registry database.

Run `uv run pytest`, `uv run ruff check .`, `uv run ruff format --check .`, and `uv run mypy` for checks.
