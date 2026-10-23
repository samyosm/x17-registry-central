# X17 Registry Central

Requires Python 3.12+, uv, and ClickHouse. Start `clickhouse-server.service` as described in `../DEPLOYMENT.md` before running these commands.

1. Run `uv sync --locked`.
2. Copy `.env.example` to `.env`. Set the ClickHouse connection, source paths, and `X17_API_TOKEN`.
3. Run `uv run x17-registry poll-once` to import the copied TriggerApp and logbook data.
4. Run `uv run x17-registry poll` and `uv run x17-registry serve` as separate processes.

Enable the Influx settings before detector polling. Run `uv run x17-registry backfill-influx` with the poller stopped to import historical detector points. Repeated polling and backfill windows are deduplicated when queried. CSV and JSON downloads are generated on demand.

Run `uv run --with chdb pytest -q` to test the API, polling, and backfill with an in-memory ClickHouse engine. Set `X17_TEST_CLICKHOUSE_URL` to run the same suite against a disposable ClickHouse HTTP service.

## Keeping the Influx poller current

`poll` reads at most `X17_INFLUX_WINDOW_SECONDS` of detector data per attempt. If an attempt fails mid-stream or runs longer than `X17_INFLUX_ATTEMPT_SECONDS`, the window is halved and retried (down to twice `X17_INFLUX_OVERLAP_SECONDS`), then grows back after successes. While the poller is more than two minutes behind, it reads window after window without waiting for the poll interval. This is how it catches up after an outage, so `backfill-influx` is only needed for history before the forward checkpoint.

The poller records its state (reading, retrying, failing, catching up), the current window, retry reason, last error, and consecutive failures in the registry. The diagnostics page shows them next to every checkpoint.

## Inspecting and moving checkpoints

No SQL is needed:

```sh
uv run x17-registry status                                  # checkpoints plus poller/backfill status as JSON
uv run x17-registry backfill-influx --since 2026-10-09      # scan back only to this time
uv run x17-registry backfill-influx --restart --since 2026-10-09
uv run x17-registry reset-backfill                          # forget the saved backfill position; no data is deleted
uv run x17-registry set-checkpoint influx now               # move the forward scan (skips unread data)
```

`backfill-influx` resumes where it stopped unless `--restart` is given. `set-checkpoint` accepts an ISO time or `now` and only for checkpoints that exist.

## Source identification

`GET /identification?from=…&to=…&limit=10000` builds one histogram per scintillator from the first `limit` ADC readings of the period and classifies it with the `x17-isotope-id` library (pairing U and D readings of equal time, template fit). It is off until the library and its templates are set up:

```sh
uv sync --locked --extra isotope      # installs ../x17-isotope-id
```

Build the templates in `x17-isotope-id` (`python scripts/initialize.py`), then set `X17_ISOTOPE_ID_CONFIG` to its `config.toml`. Without them the endpoint answers 503 `identifier_unavailable`. No calibration is applied yet.

The library is installed as a copy, not linked: after changing `x17-isotope-id`, run `uv sync --reinstall-package x17-isotope-id --extra isotope` (or add `--reinstall-package x17-isotope-id` to `uv run`).
