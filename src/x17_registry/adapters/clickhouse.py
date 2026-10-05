import base64
import json
import re
from collections.abc import Iterable, Iterator, Mapping
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class ClickHouseError(RuntimeError):
    pass


class ClickHouseClient:
    def __init__(
        self, url: str, database: str, user: str, password: str, timeout_seconds: float
    ) -> None:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database):
            raise ValueError("ClickHouse database name must be a simple identifier.")
        self.url = url.rstrip("/")
        self.database = database
        self.timeout_seconds = timeout_seconds
        self.authorization = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()

    def _request(
        self, sql: str, params: Mapping[str, str | int] | None = None,
        body: bytes | None = None, database: str | None = None,
        wait_for_end: bool = True
    ) -> Request:
        query = {"database": database or self.database, "query": sql}
        if wait_for_end:
            query["wait_end_of_query"] = "1"
        query.update({f"param_{key}": str(value) for key, value in (params or {}).items()})
        headers = {"Authorization": self.authorization}
        return Request(
            f"{self.url}/?{urlencode(query)}",
            data=body if body is not None else b"",
            headers=headers,
            method="POST",
        )

    def _open(self, request: Request) -> Any:
        try:
            return urlopen(request, timeout=self.timeout_seconds)
        except HTTPError as error:
            detail = error.read(1000).decode("utf-8", errors="replace")
            raise ClickHouseError(f"ClickHouse HTTP {error.code}: {detail}") from error
        except (URLError, TimeoutError) as error:
            raise ClickHouseError(f"ClickHouse connection failed: {error}") from error

    def execute(
        self, sql: str, params: Mapping[str, str | int] | None = None,
        database: str | None = None
    ) -> None:
        with self._open(self._request(sql, params, database=database)) as response:
            response.read()

    def query(
        self, sql: str, params: Mapping[str, str | int] | None = None
    ) -> list[dict[str, Any]]:
        return list(self.iterate(sql, params))

    def iterate(
        self, sql: str, params: Mapping[str, str | int] | None = None
    ) -> Iterator[dict[str, Any]]:
        with self._open(
            self._request(f"{sql} FORMAT JSONEachRow", params, wait_for_end=False)
        ) as response:
            for line in response:
                yield json.loads(line)

    def insert(self, table: str, rows: Iterable[dict[str, Any]]) -> None:
        if table not in {"collected_records", "detector_points", "registry_state"}:
            raise ValueError("Unknown ClickHouse table.")
        body = b"".join(
            json.dumps(row, separators=(",", ":"), allow_nan=False).encode() + b"\n"
            for row in rows
        )
        if not body:
            return
        request = self._request(
            f"INSERT INTO {table} FORMAT JSONEachRow", body=body
        )
        with self._open(request) as response:
            response.read()
