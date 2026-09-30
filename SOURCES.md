# Source formats observed in the local copies

This is an implementation note for the three readers. It describes the supplied copies, not a verified live deployment.

| Source | Persisted output | Reader | Source identity |
| --- | --- | --- | --- |
| VF48 | InfluxDB v2 `run_<run_number>` measurements; tag `VF48_num`; fields include `trignum`, `tstamp`, `livetime`, `wall_clock_ns` and mapped ADC/pattern values. The point time is assigned in the subscriber after its queue. | `InfluxReader` queries `/api/v2/query` for individual fields. | Measurement, exact Influx time, board tag and field. |
| TriggerApp | `trigger_history.json` contains `{version, entries}` and retains at most 200 entries. Each entry has `id`, UTC `timestamp`, `title`, `note`, `tmod`, `maj`, `thresholds`, `masks`, `invert`, and `notes`. `trigger_config.json` stores editor state; `trigger_programmed.json` stores the last programmed baseline. | `TriggerReader` reads all three JSON files. | History entry ID or `current` for each state file. |
| Control room logbook | `data/logbook.db` has `users`, `condition_events`, and `condition_snapshots`. Events carry `id`, `public_id`, `timestamp`, `category`, JSON `payload`, `comment`, active flag and operator link. Snapshots have `valid_from`, `valid_to`, and JSON `state`. | `LogbookReader` reads all three tables through a read-only SQLite connection each poll. | Table and integer row ID. |

The processor currently copies each source record unchanged. The registry stores both the source record and the processed copy. Edits to an old JSON entry or logbook row create another revision. A completed Influx query advances its checkpoint; an interrupted query is replayed and deduplicated. `X17_INFLUX_START_AT` defines the first time range, and subsequent queries overlap by `X17_INFLUX_OVERLAP_SECONDS`.

The interface run list is currently based only on the observed Influx measurement name and earliest stored point. End time, experiment identity, beam state, and TriggerApp association are unknown. A logbook `Run` event sets operator context; it does not confirm detector acquisition start. The TriggerApp history records successful writes but not a verified run boundary. No ADC-to-MeV calibration is available in the inspected source, so the processor does not invent one.

The copied TriggerApp history is capped. A poller cannot recover entries that disappeared between polls unless another source such as the app's backup files is added. The current reader does not import those backups. The Influx reader targets the documented v2 API and needs deployment values in `.env`; it has not queried a laboratory server.
