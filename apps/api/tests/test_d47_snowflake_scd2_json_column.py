"""D47 — SCD2 into Snowflake failed after a green preflight when the table had
a JSON column.

The SCD2 history table is created and loaded through SQLAlchemy. Its
Snowflake dialect cannot render DataFlow's JSON column type (``Compiler ...
can't render element of type _ExactJSON``), so the CREATE failed; and the
versions were inserted with ``INSERT ... VALUES`` binds, which Snowflake
refuses for a VARIANT column ("Expression type does not match column data
type, expecting VARIANT but got VARCHAR") while a ``VALUES`` clause cannot call
``PARSE_JSON`` either. fakesnow accepts text in a VARIANT column, so that
Snowflake rule is restored here on top of the strict-identifier fixture.

Mirror with a JSON column is covered too: it already goes through the
Snowflake writer.
"""

from __future__ import annotations

import json
import re
import uuid
from typing import Any

import pytest

from tests import test_d120_snowflake_strict_e2e as _strict
from tests.test_d47_snowflake_scd2_mirror import _dst, _pg, _run

engine_env = _strict.engine_env
snowflake_semantics = _strict.snowflake_semantics

_INSERT_VALUES = re.compile(
    r"^\s*INSERT\s+INTO\s+(?P<table>[^\s(]+)\s*\((?P<cols>[^)]*)\)\s*VALUES\b", re.I
)


@pytest.fixture()
def variant_binds_refused(monkeypatch, snowflake_semantics):
    """Snowflake: text bound by ``INSERT ... VALUES`` never lands in a VARIANT."""
    import snowflake.connector
    from fakesnow.cursor import FakeSnowflakeCursor

    strict_execute = FakeSnowflakeCursor.execute

    def execute(self, command: str, *args: Any, **kwargs: Any) -> Any:
        hit = _INSERT_VALUES.match(str(command or ""))
        if hit:
            table = hit["table"].split(".")[-1].strip('"')
            documents = {
                str(r[0]).upper()
                for r in self._duck_conn.cursor().execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE upper(table_name) = upper(?) AND data_type = 'JSON'",
                    [table],
                ).fetchall()
            }
            listed = {c.strip().strip('"').upper() for c in hit["cols"].split(",")}
            if documents & listed:
                raise snowflake.connector.errors.ProgrammingError(
                    msg="Expression type does not match column data type, "
                    "expecting VARIANT but got VARCHAR(16777216)",
                    errno=2023,
                    sqlstate="22000",
                )
        return strict_execute(self, command, *args, **kwargs)

    monkeypatch.setattr(FakeSnowflakeCursor, "execute", execute)


@pytest.fixture()
def pg_documents():
    conn = _pg()
    table = "d47j_src_" + uuid.uuid4().hex[:8]
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, name TEXT, doc JSONB)")
        cur.execute(
            f"INSERT INTO {table} VALUES "
            "(1, 'a', '{\"x\": 3.0, \"k\": [1, 2]}'), (2, 'b', NULL), (3, 'c', '{\"y\": \"z\"}')"
        )
    try:
        yield conn, table
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()


def _documents(rows: list[tuple]) -> list[tuple]:
    return [
        tuple(json.loads(v) if isinstance(v, str) else v for v in row) for row in rows
    ]


def test_scd2_into_snowflake_with_a_json_column(
    engine_env, variant_binds_refused, pg_documents
):
    conn, src = pg_documents
    dst = "d47j_scd2_" + uuid.uuid4().hex[:8]
    first = _run(engine_env, src, dst, "scd2")
    assert first.records_transferred == 3
    with conn.cursor() as cur:
        cur.execute(f"UPDATE {src} SET doc = '{{\"x\": 4}}' WHERE id = 2")
    _run(engine_env, src, dst, "scd2")
    history = _documents(_dst(dst, '"ID", "DOC"'))
    assert history == [(1, {"x": 3, "k": [1, 2]}), (2, None), (2, {"x": 4}), (3, {"y": "z"})]
    current = _documents(_dst(dst, '"ID", "DOC"', 'WHERE "IS_CURRENT"'))
    assert current == [(1, {"x": 3, "k": [1, 2]}), (2, {"x": 4}), (3, {"y": "z"})]


def test_mirror_into_snowflake_with_a_json_column(
    engine_env, variant_binds_refused, pg_documents
):
    conn, src = pg_documents
    dst = "d47j_mirror_" + uuid.uuid4().hex[:8]
    _run(engine_env, src, dst, "mirror", delete_mode="hard")
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {src} WHERE id = 3")
        cur.execute(f"UPDATE {src} SET doc = '{{\"x\": 5}}' WHERE id = 1")
    _run(engine_env, src, dst, "mirror", delete_mode="hard")
    assert _documents(_dst(dst, '"ID", "DOC"')) == [(1, {"x": 5}), (2, None)]
