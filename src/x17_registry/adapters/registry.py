import hashlib
import json
import re
import time
from calendar import timegm
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from typing import Any

from x17_registry.adapters import detector_sql
from x17_registry.adapters.clickhouse import ClickHouseClient
from x17_registry.application.polling import SourceRecord


def epoch_ns(value: str) -> int:
    text = value.replace("Z", "+00:00")
    moment = datetime.fromisoformat(text).astimezone(UTC)
    seconds = timegm(moment.utctimetuple())
    match = re.search(r"\.(\d+)", text)
    fraction = match.group(1)[:9] if match else ""
    return seconds * 1_000_000_000 + int(fraction.ljust(9, "0") or "0")


def record_id(source: str, instance: str, identity: str, revision: int) -> int:
    key = json.dumps([source, instance, identity, revision], separators=(",", ":"))
    return int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], "big") & ((1 << 53) - 1)


def latest_records(source: str | None = None) -> str:
    condition = "WHERE source={source:String}" if source else ""
    return (
        "SELECT * FROM collected_records FINAL "
        f"{condition} ORDER BY source,source_instance,source_id,revision DESC "
        "LIMIT 1 BY source,source_instance,source_id"
    )


SCHEMA = (
    """CREATE TABLE IF NOT EXISTS collected_records (
        id UInt64, source LowCardinality(String), source_instance String, source_id String,
        revision UInt32, content_hash String, event_time Nullable(String),
        event_ns Nullable(Int64), run_number Nullable(String), raw_json String,
        processed_json String, collected_at String, version UInt64
    ) ENGINE = ReplacingMergeTree(version)
    ORDER BY (source,source_instance,source_id,revision)""",
    """CREATE TABLE IF NOT EXISTS detector_points (
        source_instance String, source_id String, event_ns Int64,
        event_time String, run_number String, point_json String, version UInt64
    ) ENGINE = ReplacingMergeTree(version)
    ORDER BY (event_ns,source_instance,source_id)""",
    # The channel and ADC value of a point, stored beside its JSON so that analysis queries do not
    # parse the JSON of every row. Rows stored before these columns exist are computed when read
    # until `x17-registry materialize-detector-columns` has rewritten them.
    "ALTER TABLE detector_points ADD COLUMN IF NOT EXISTS field LowCardinality(String) "
    "MATERIALIZED JSONExtractString(point_json,'_field')",
    "ALTER TABLE detector_points ADD COLUMN IF NOT EXISTS value Nullable(Float64) "
    "MATERIALIZED toFloat64OrNull(trim(BOTH '\"' FROM JSONExtractRaw(point_json,'_value')))",
    """CREATE TABLE IF NOT EXISTS registry_state (
        kind LowCardinality(String), source String, value String, version UInt64
    ) ENGINE = ReplacingMergeTree(version) ORDER BY (kind,source)""",
)


class RegistryStore:
    def __init__(self, client: ClickHouseClient) -> None:
        self.client = client

    def initialize(self) -> None:
        self.client.execute(
            f"CREATE DATABASE IF NOT EXISTS {self.client.database}", database="default"
        )
        for statement in SCHEMA:
            self.client.execute(statement)

    def materialize_detector_columns(self) -> None:
        """Start rewriting `field` and `value` for rows stored before the columns existed.

        Runs as a background ClickHouse mutation that rewrites only these two columns and
        leaves the stored points untouched; until it has finished, queries still work.
        """
        for column in ("field", "value"):
            self.client.execute(f"ALTER TABLE detector_points MATERIALIZE COLUMN {column}")

    @staticmethod
    def document(row: dict[str, Any]) -> dict[str, Any]:
        result = dict(row)
        result["raw"] = json.loads(result.pop("raw_json"))
        result["processed"] = json.loads(result.pop("processed_json"))
        result.pop("version", None)
        result.pop("event_ns", None)
        return result

    def save(self, record: SourceRecord, processed: dict[str, Any]) -> bool:
        if record.source == "influx":
            return bool(self.save_detector_batch([record]))
        return self.save_batch([(record, processed)]) > 0

    def save_batch(self, records: Iterable[tuple[SourceRecord, dict[str, Any]]]) -> int:
        batch = list(records)
        if not batch:
            return 0
        if any(record.source == "influx" for record, _ in batch):
            raise ValueError("Detector records require save_detector_batch.")
        previous = {
            (row["source"], row["source_instance"], row["source_id"]): (
                int(row["revision"]), row["content_hash"]
            )
            for row in self.client.query(
                f"SELECT source,source_instance,source_id,revision,content_hash "
                f"FROM ({latest_records()})"
            )
        }
        rows = []
        for record, processed in batch:
            raw_json = json.dumps(record.raw, sort_keys=True, allow_nan=False)
            processed_json = json.dumps(processed, sort_keys=True, allow_nan=False)
            digest = hashlib.sha256(f"{raw_json}\0{processed_json}".encode()).hexdigest()
            key = (record.source, record.source_instance, record.source_id)
            revision, old_digest = previous.get(key, (0, ""))
            if digest == old_digest:
                continue
            revision += 1
            previous[key] = (revision, digest)
            rows.append({
                "id": record_id(*key, revision),
                "source": record.source,
                "source_instance": record.source_instance,
                "source_id": record.source_id,
                "revision": revision,
                "content_hash": digest,
                "event_time": record.event_time,
                "event_ns": epoch_ns(record.event_time) if record.event_time else None,
                "run_number": record.run_number,
                "raw_json": raw_json,
                "processed_json": processed_json,
                "collected_at": datetime.now(UTC).isoformat(),
                "version": time.time_ns(),
            })
        if rows:
            self.client.insert("collected_records", rows)
        return len(rows)

    def save_detector_batch(self, records: Iterable[SourceRecord]) -> int:
        rows = []
        version = time.time_ns()
        for record in records:
            if record.source != "influx" or record.event_time is None or record.run_number is None:
                raise ValueError("Detector batch requires complete Influx points.")
            rows.append({
                "source_instance": record.source_instance,
                "source_id": record.source_id,
                "event_ns": epoch_ns(record.event_time),
                "event_time": record.event_time,
                "run_number": record.run_number,
                "point_json": json.dumps(record.raw, sort_keys=True, allow_nan=False),
                "version": version,
            })
        self.client.insert("detector_points", rows)
        return len(rows)

    def save_detector_backfill_batch(self, records: Iterable[SourceRecord]) -> int:
        return self.save_detector_batch(records)

    def _state(self, kind: str, source: str) -> str | None:
        rows = self.client.query(
            "SELECT value FROM registry_state FINAL WHERE kind={kind:String} "
            "AND source={source:String} LIMIT 1",
            {"kind": kind, "source": source},
        )
        return str(rows[0]["value"]) or None if rows else None

    def _set_state(self, kind: str, source: str, value: str) -> None:
        self.client.insert(
            "registry_state",
            [{"kind": kind, "source": source, "value": value, "version": time.time_ns()}],
        )

    def checkpoint(self, source: str) -> str | None:
        return self._state("checkpoint", source)

    def set_checkpoint(self, source: str, value: str) -> None:
        self._set_state("checkpoint", source, value)

    def clear_checkpoints(self, prefix: str) -> list[str]:
        """Forget every checkpoint whose name starts with prefix; returns the cleared names."""
        names = [
            row["source"]
            for row in self.client.query(
                "SELECT source FROM registry_state FINAL WHERE kind='checkpoint' "
                "AND startsWith(source,{prefix:String}) AND value!=''",
                {"prefix": prefix},
            )
        ]
        for name in names:
            self._set_state("checkpoint", name, "")
        return names

    def enabled_lab_sources(self) -> list[int] | None:
        """Ids of the lab sources chosen for the reports, or None when nothing was chosen yet."""
        value = self._state("lab_sources", "enabled")
        return None if value is None else [int(item) for item in json.loads(value)]

    def set_enabled_lab_sources(self, ids: list[int]) -> None:
        self._set_state("lab_sources", "enabled", json.dumps(sorted(set(ids))))

    def checkpoints(self) -> list[dict[str, str]]:
        return [
            {
                "name": row["source"],
                "value": row["value"],
                "updatedAt": self._event_at(int(row["version"])),
            }
            for row in self.client.query(
                "SELECT source,value,version FROM registry_state FINAL "
                "WHERE kind='checkpoint' AND value!='' ORDER BY source"
            )
        ]

    def set_status(self, source: str, status: dict[str, Any]) -> None:
        self._set_state("status", source, json.dumps(status, separators=(",", ":")))

    def statuses(self) -> dict[str, dict[str, Any]]:
        return {
            row["source"]: json.loads(row["value"])
            for row in self.client.query(
                "SELECT source,value FROM registry_state FINAL "
                "WHERE kind='status' AND value!=''"
            )
        }

    def mark_synced(self, source: str, at: datetime) -> None:
        self._set_state("synced", source, at.astimezone(UTC).isoformat().replace("+00:00", "Z"))

    @staticmethod
    def _event_at(value: int) -> str:
        seconds, nanos = divmod(value, 1_000_000_000)
        moment = datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%S")
        fraction = f".{nanos:09d}".rstrip("0") if nanos else ""
        return f"{moment}{fraction}Z"

    def source_status(self) -> dict[str, dict[str, Any]]:
        state = {
            (row["kind"], row["source"]): row["value"]
            for row in self.client.query(
                "SELECT kind,source,value FROM registry_state FINAL WHERE value!=''"
            )
        }
        sources: dict[str, dict[str, Any]] = {}
        for name, condition in (
            ("trigger", "source='trigger_history'"),
            ("logbook", "startsWith(source,'logbook_')"),
        ):
            rows = self.client.query(
                "SELECT minOrNull(event_ns) AS first_ns,maxOrNull(event_ns) AS last_ns "
                f"FROM ({latest_records()}) WHERE {condition}"
            )
            values = rows[0] if rows else {}
            sources[name] = {
                "earliest": (
                    self._event_at(int(values["first_ns"])) if values.get("first_ns") else None
                ),
                "latest": self._event_at(int(values["last_ns"])) if values.get("last_ns") else None,
                "lastSyncedAt": state.get(("synced", name)),
            }
        point_range = self.client.query(
            "SELECT minOrNull(event_ns) AS first_ns,maxOrNull(event_ns) AS last_ns "
            "FROM detector_points"
        )
        values = point_range[0] if point_range else {}
        reverse_key = next(
            (
                key for kind, key in state
                if kind == "checkpoint" and key.startswith("influx-backfill-reverse:")
            ),
            None,
        )
        sources["influx"] = {
            "earliest": self._event_at(int(values["first_ns"])) if values.get("first_ns") else None,
            "latest": self._event_at(int(values["last_ns"])) if values.get("last_ns") else None,
            "lastSyncedAt": state.get(("synced", "influx")),
            "scannedThrough": state.get(("checkpoint", "influx")),
            "backfillCursor": state.get(("checkpoint", reverse_key)) if reverse_key else None,
            "backfillUpper": (
                state.get(("checkpoint", reverse_key.replace("-reverse:", "-upper:")))
                if reverse_key else None
            ),
            "backfillLastSyncedAt": state.get(("synced", "influx_backfill")),
        }
        return sources

    def get(self, identity: int) -> dict[str, Any] | None:
        rows = self.client.query(
            "SELECT * FROM collected_records FINAL WHERE id={identity:UInt64} LIMIT 1",
            {"identity": identity},
        )
        return self.document(rows[0]) if rows else None

    def records(
        self, source: str | None, page: int, page_size: int
    ) -> tuple[list[dict[str, Any]], int]:
        params: dict[str, str | int] = {"limit": page_size, "offset": (page - 1) * page_size}
        if source is not None:
            params["source"] = source
        latest = latest_records(source)
        total = self.client.query(f"SELECT count() AS total FROM ({latest})", params)[0]["total"]
        rows = self.client.query(
            f"SELECT * FROM ({latest}) ORDER BY event_ns DESC,id DESC "
            "LIMIT {limit:UInt32} OFFSET {offset:UInt64}",
            params,
        )
        return [self.document(row) for row in rows], int(total)

    def trigger_runs(self) -> list[dict[str, Any]]:
        rows = self.client.query(
            f"SELECT * FROM ({latest_records('trigger_history')}) "
            "WHERE event_ns IS NOT NULL ORDER BY event_ns,source_instance,source_id",
            {"source": "trigger_history"},
        )
        return [self.document(row) for row in rows]

    def beam_events(self) -> list[dict[str, Any]]:
        rows = self.client.query(
            f"SELECT * FROM ({latest_records('logbook_event')}) "
            "WHERE event_ns IS NOT NULL ORDER BY event_ns,id",
            {"source": "logbook_event"},
        )
        records = [self.document(row) for row in rows]
        return [
            record for record in records
            if record["raw"].get("category") == "Beam" and record["raw"].get("is_active", True)
        ]

    @staticmethod
    def _interval(start: str, end: str | None) -> tuple[str, dict[str, int]]:
        params = {"start": epoch_ns(start)}
        condition = "event_ns>={start:Int64}"
        if end is not None:
            params["end"] = epoch_ns(end)
            condition += " AND event_ns<{end:Int64}"
        return condition, params

    def influx_origin(self, start: str, end: str | None) -> dict[str, str] | None:
        condition, params = self._interval(start, end)
        rows = self.client.query(
            "SELECT source_instance,run_number FROM detector_points FINAL "
            f"WHERE {condition} LIMIT 1",
            params,
        )
        if not rows:
            return None
        return {
            "source_instance": rows[0]["source_instance"],
            "run_number": rows[0]["run_number"],
        }

    def latest_detector_ns(self) -> int | None:
        rows = self.client.query("SELECT maxOrNull(event_ns) AS last_ns FROM detector_points")
        return int(rows[0]["last_ns"]) if rows and rows[0].get("last_ns") is not None else None

    def detector_series(self, start_ns: int, end_ns: int, width_ns: int) -> list[dict[str, Any]]:
        """Per-field time buckets (count, mean, min, max) for ADC fields in [start, end)."""
        return self.client.query(
            "SELECT field, intDiv(event_ns-{start:Int64},{width:Int64}) AS bucket, "
            "count() AS n, avg(value) AS mean, min(value) AS low, max(value) AS high "
            "FROM detector_points FINAL WHERE event_ns>={start:Int64} AND event_ns<{end:Int64} "
            "AND value IS NOT NULL AND startsWith(field,'FPGA') AND position(field,'_ADC')>0 "
            "GROUP BY field, bucket ORDER BY field, bucket",
            {"start": start_ns, "end": end_ns, "width": width_ns},
        )

    def count_readings(self, start_ns: int, end_ns: int, timeout: float | None = None) -> int:
        rows = self.client.query(detector_sql.readings_sql(start_ns, end_ns), timeout=timeout)
        return int(rows[0]["n"])

    def count_events(self, start_ns: int, end_ns: int, timeout: float | None = None) -> int:
        """The triggers (events) in [start, end): the exposure of a random-trigger acquisition."""
        return self.event_span(start_ns, end_ns, timeout)[0]

    def event_span(
        self, start_ns: int, end_ns: int, timeout: float | None = None
    ) -> tuple[int, float]:
        """(triggers, seconds from the first to the last): the rate is over the time with data."""
        rows = self.client.query(detector_sql.events_sql(start_ns, end_ns), timeout=timeout)
        count = int(rows[0]["n"])
        if count < 2:
            return count, 0.0
        return count, (int(rows[0]["t_last"]) - int(rows[0]["t_first"])) / 1e9

    def pair_histograms(
        self,
        spec: detector_sql.PairSpec,
        start_ns: int,
        end_ns: int,
        timeout: float | None = None,
        names: list[str] | None = None,
    ) -> dict[str, tuple[list[int], int]]:
        """{group: (accepted pairs per histogram bin, accepted pairs in all)} over the period."""
        names = names or list(spec.groups)
        counts = {name: [0] * spec.bins for name in names}
        pairs = dict.fromkeys(names, 0)
        for row in self.client.query(
            detector_sql.histogram_sql(spec, names, start_ns, end_ns), timeout=timeout
        ):
            name = names[int(row["scint"])]
            pairs[name] += int(row["n"])
            if int(row["bin"]) >= 0:
                counts[name][int(row["bin"])] += int(row["n"])
        return {name: (counts[name], pairs[name]) for name in names}

    def pair_diagnostics(
        self,
        spec: detector_sql.PairSpec,
        name: str,
        start_ns: int,
        end_ns: int,
        raw_start: int,
        raw_bins: int,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """The inputs of Identifier.diagnose_histogram for one group over the period."""
        d_field, u_field = spec.groups[name]
        counts, _ = self.pair_histograms(spec, start_ns, end_ns, timeout, [name])[name]
        stats = self.client.query(
            detector_sql.pair_statistics_sql(spec, name, start_ns, end_ns), timeout=timeout
        )[0]
        channels = self.client.query(
            detector_sql.channel_statistics_sql(d_field, u_field, start_ns, end_ns),
            timeout=timeout,
        )[0]
        raw: dict[str, Any] = {
            "start": raw_start,
            "d": [0] * raw_bins,
            "u": [0] * raw_bins,
        }
        for row in self.client.query(
            detector_sql.raw_histogram_sql(d_field, u_field, start_ns, end_ns, raw_start, raw_bins),
            timeout=timeout,
        ):
            raw["du"[int(row["endp"])]][int(row["b"]) - raw_start] = int(row["n"])
        d = detector_sql.summary(
            channels["d_n"], channels["d_negative"], channels["d_min"], channels["d_max"],
            channels["d_q"],
        )
        u = detector_sql.summary(
            channels["u_n"], channels["u_negative"], channels["u_min"], channels["u_max"],
            channels["u_q"],
        )
        scan = self.client.query(
            detector_sql.threshold_scan_sql(spec, name, start_ns, end_ns), timeout=timeout
        )[0]
        return {
            "counts": counts,
            "pair_summary": detector_sql.summary(
                stats["n"], 0, stats["low"], stats["high"], stats["q"]
            ),
            "d": d,
            "u": u,
            "matched": int(scan["matched"]),
            "below": int(stats["below"]),
            "above": int(stats["above"]),
            "zero": int(stats["zero"]),
            "raw": raw,
            "scan": {
                "thresholds": list(detector_sql.SCAN_THRESHOLDS),
                "pairs": [int(scan[f"t{t}"]) for t in detector_sql.SCAN_THRESHOLDS],
            },
        }

    def iter_detector_points(self, start: str, end: str | None) -> Iterator[dict[str, Any]]:
        condition, params = self._interval(start, end)
        for row in self.client.iterate(
            f"SELECT point_json FROM detector_points FINAL WHERE {condition} "
            "ORDER BY event_ns,source_instance,source_id",
            params,
        ):
            yield json.loads(row["point_json"])
