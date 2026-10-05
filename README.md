# X17 Registry Central

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/). Run commands from this directory.

1. Run `uv sync --locked`.
1. Copy `.env.example` to `.env`. Set `X17_API_TOKEN`.
1. Run `uv run x17-registry serve` for the API.
1. Run `uv run x17-registry poll` in a second process to fetch sources at `X17_POLL_INTERVAL_SECONDS` intervals. Use `uv run x17-registry poll-once` from an external cron job if preferred.

For historical InfluxDB data, stop the regular poller and run `uv run x17-registry backfill-influx`. It skips empty time gaps, imports populated windows without the polling delay, and resumes from the saved Influx checkpoint if interrupted. Restart the regular poller when the command finishes. Keep `X17_INFLUX_ENABLED=true` and `X17_INFLUX_START_AT` at or before the earliest data to import.

The example paths point to the copied TriggerApp JSON and logbook SQLite files. Set `X17_INFLUX_ENABLED=true` and fill the Influx settings to query the detector store. The poller reads source files and queries InfluxDB; it writes only to `X17_DATABASE_PATH`.

The API uses `Authorization: Bearer <X17_API_TOKEN>`. Its routes and schemas are available at `http://127.0.0.1:8000/docs` with the example bind settings.

After upgrading a registry database created before run summaries existed, stop the API and poller, run `uv run x17-registry rebuild-run-summaries` once, then restart them. This updates only the registry database.

Run `uv run pytest`, `uv run ruff check .`, `uv run ruff format --check .`, and `uv run mypy` for checks.
