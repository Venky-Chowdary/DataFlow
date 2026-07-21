"""Snowflake identifier case resolution — legacy lowercase quoted tables."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.snowflake_conn import (
    resolve_or_fold_snowflake_table,
    resolve_snowflake_table_name,
    snowflake_qualified_table,
)


class _FakeCursor:
    """Minimal cursor that answers information_schema table lookups."""

    def __init__(self, tables: dict[str, list[str]]):
        self._tables = {k.upper(): list(v) for k, v in tables.items()}
        self._result: list[tuple[Any, ...]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        params = params or ()
        sql_u = " ".join(sql.split()).upper()
        self._result = []
        if "FROM INFORMATION_SCHEMA.TABLES" not in sql_u:
            return
        schema = str(params[0])
        name = str(params[1])
        stored = self._tables.get(schema.upper(), [])
        if "UPPER(TABLE_NAME) = UPPER" in sql_u:
            for t in stored:
                if t.upper() == name.upper():
                    self._result = [(t,)]
                    return
        else:
            for t in stored:
                if t == name:
                    self._result = [(t,)]
                    return

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._result[0] if self._result else None


def test_resolve_finds_legacy_lowercase_when_ui_asks_upper():
    cur = _FakeCursor({"PUBLIC": ["csvtestfile"]})
    assert resolve_snowflake_table_name(cur, "PUBLIC", "CSVTESTFILE") == "csvtestfile"
    assert resolve_snowflake_table_name(cur, "public", "csvtestfile") == "csvtestfile"
    assert resolve_or_fold_snowflake_table(cur, "PUBLIC", "CSVTESTFILE") == "csvtestfile"
    assert snowflake_qualified_table("PUBLIC", "csvtestfile") == '"PUBLIC"."csvtestfile"'


def test_resolve_missing_folds_to_upper_for_create():
    cur = _FakeCursor({"PUBLIC": []})
    assert resolve_snowflake_table_name(cur, "PUBLIC", "new_table") is None
    assert resolve_or_fold_snowflake_table(cur, "PUBLIC", "new_table") == "NEW_TABLE"


@pytest.mark.skipif(
    __import__("importlib").util.find_spec("fakesnow") is None,
    reason="requires optional Snowflake test dependency",
)
def test_live_fakesnow_lowercase_table_preview():
    import uuid

    from connectors.snowflake_conn import get_connection
    from connectors.snowflake_reader import count_table_rows, read_table_batch

    suffix = uuid.uuid4().hex[:8]
    table = f"csvtestfile_{suffix}"

    conn = get_connection(
        account="localhost",
        username="test",
        password="test",
        database="dataflow",
        schema="public",
        warehouse="",
        connection_string="",
    )
    try:
        cur = conn.cursor()
        cur.execute(f'CREATE TABLE "{table}" (id INT, column_2 VARCHAR)')
        cur.execute(f'INSERT INTO "{table}" (id, column_2) VALUES (1, \'a\'), (2, \'b\')')
        conn.commit()

        n = count_table_rows(
            host="localhost",
            port=443,
            database="dataflow",
            username="test",
            password="test",
            schema="PUBLIC",
            connection_string="",
            warehouse="",
            table=table.upper(),
        )
        assert n == 2
        batch = read_table_batch(
            host="localhost",
            port=443,
            database="dataflow",
            username="test",
            password="test",
            schema="public",
            connection_string="",
            warehouse="",
            table=table.upper(),
            limit=10,
            known_total_rows=2,
        )
        assert len(batch.rows) == 2
    finally:
        try:
            conn.cursor().execute(f'DROP TABLE IF EXISTS "{table}"')
            conn.commit()
        except Exception:
            pass
        conn.close()
