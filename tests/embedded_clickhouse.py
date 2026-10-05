import json
import re


class EmbeddedClickHouse:
    database = "default"

    def __init__(self, chdb):
        self.session = chdb.session.Session()

    def close(self):
        self.session.close()

    def execute(self, sql, params=None, database=None):
        self.session.query(self._expand(sql, params))

    @staticmethod
    def _expand(sql, params):
        for key, value in (params or {}).items():
            token = re.compile(r"\{" + key + r":(?:String|Int64|UInt32|UInt64)\}")
            literal = "'" + value.replace("'", "''") + "'" if isinstance(value, str) else str(value)
            sql = token.sub(literal, sql)
        return sql

    def query(self, sql, params=None):
        result = str(self.session.query(self._expand(sql, params), "JSONEachRow"))
        return [json.loads(line) for line in result.splitlines() if line]

    def iterate(self, sql, params=None):
        return iter(self.query(sql, params))

    def insert(self, table, rows):
        values = "\n".join(json.dumps(row) for row in rows)
        if values:
            self.session.query(f"INSERT INTO {table} FORMAT JSONEachRow\n{values}")
