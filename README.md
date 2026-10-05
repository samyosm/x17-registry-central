# X17 Registry Central

Requires Python 3.12+, uv, and ClickHouse. Start `clickhouse-server.service` as described in `../DEPLOYMENT.md` before running these commands.

1. Run `uv sync --locked`.
2. Copy `.env.example` to `.env`. Set the ClickHouse connection, source paths, and `X17_API_TOKEN`.
3. Run `uv run x17-registry poll-once` to import the copied TriggerApp and logbook data.
4. Run `uv run x17-registry poll` and `uv run x17-registry serve` as separate processes.

Enable the Influx settings before detector polling. Run `uv run x17-registry backfill-influx` with the poller stopped to import historical detector points. Repeated polling and backfill windows are deduplicated when queried. CSV and JSON downloads are generated on demand.

Run `uv run --with chdb pytest -q` to test the API, polling, and backfill with an in-memory ClickHouse engine. Set `X17_TEST_CLICKHOUSE_URL` to run the same suite against a disposable ClickHouse HTTP service.
