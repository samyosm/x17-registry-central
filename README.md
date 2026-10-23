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

`GET /identification?from=…&to=…[&refFrom=…&refTo=…]` looks for a source as an **excess over a reference period** and returns a verdict (`none`, `detected`, `identified`), the source and the scintillator it is next to, the per-scintillator excess and the spectrum of the excess. The reference is `refFrom`/`refTo` if both are given (it must not overlap the window), otherwise the 24 hours (or as long as the window) before the window, or after it if there is too little data. The data is a random-trigger acquisition, so the number of triggers is the exposure. A warning is returned when the trigger rate differs between window and reference. Needs the `[excess]` and `[response]` sections of `x17-isotope-id/config.toml` and `templates/excess_templates.json` (built by `scripts/build_excess_templates.py`). `GET /identification/{scintillator}` still shows the data scale behind one scintillator (raw readings, threshold scan, spectrum against the older templates). There is no limit on the period or the number of readings.

The work happens inside ClickHouse (`adapters/detector_sql.py`): the readings are paired, filtered with the selection threshold (`[excess] threshold`, or `[filter]` for the diagnostics endpoint), and histogrammed there, so only a few hundred counts per scintillator reach the API. The stored points are never changed or removed; the threshold only decides which readings a query uses. On a synthetic table of 48 million readings, the whole-period identification took about 0.4 s with the threshold in `both` mode, and the worst case (threshold disabled, every pair counted) 16 s without using the temporary disk.

Set it up:

```sh
uv sync --locked --extra isotope      # installs ../x17-isotope-id (editable)
```

Build the templates in `x17-isotope-id` (`uv run --group scripts python scripts/initialize.py`), then set `X17_ISOTOPE_ID_CONFIG` to its `config.toml`. Without them the endpoints answer 503 `identifier_unavailable`. Restart `x17-registry serve` after changing the library, its config or the templates. All templates must use the same bins.

Starting the API adds two stored columns to `detector_points`, `field` and `value` (the channel and ADC value of each point, derived from its JSON). Nothing is deleted. Rows stored before that are computed from the JSON when queried, which is already fast; to make them faster still (about 10×), rewrite the two columns once, in the background:

```sh
uv run x17-registry materialize-detector-columns
# progress: SELECT table, command, is_done FROM system.mutations WHERE table = 'detector_points'
```

`X17_IDENTIFICATION_TIMEOUT_SECONDS` (default 280) is how long one ClickHouse query of these endpoints may run; keep it below the 290 s the website waits.
