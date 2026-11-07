"""Create-new materialization must not ALTER an operator's existing column.

Postgres materializes a logical ``INTEGER`` as ``BIGINT`` when it invents a
new table. Feeding that invention to the widen pass made every backfill write
ALTER an operator's existing ``INTEGER`` column to ``BIGINT`` although the
source column had not drifted at all — an unapproved physical DDL change with
FK/index consequences. A widen is only legitimate when the existing carrier
cannot hold the incoming source type.
"""

from __future__ import annotations

import os
import uuid

import pytest

from connectors.schema_drift import widen_existing_columns_native


class _FakeCursor:
    """Answers the information_schema probe, records executed DDL."""

    def __init__(self, columns: list[tuple]):
        self._columns = columns
        self.executed: list[str] = []

    def execute(self, statement, params=None):  # noqa: ARG002
        text = str(statement)
        if "information_schema.columns" in text:
            self._pending = self._columns
        else:
            self.executed.append(text)
            self._pending = []

    def fetchall(self):
        return getattr(self, "_pending", [])


def test_undrifted_integer_carrier_is_not_widened_to_bigint():
    cursor = _FakeCursor([("id", "integer", None, 32, 0)])
    suppressed: dict[str, str] = {}
    ddl = widen_existing_columns_native(
        cursor,
        "postgresql",
        "public",
        "t",
        ["id"],
        ["BIGINT"],
        backfill=True,
        source_types={"id": "INTEGER"},
        suppressed_out=suppressed,
    )
    assert ddl == []
    assert cursor.executed == []
    assert suppressed == {"id": "INTEGER"}


def test_genuinely_narrow_carrier_still_widens():
    cursor = _FakeCursor([("name", "character varying", 10, None, None)])
    ddl = widen_existing_columns_native(
        cursor,
        "postgresql",
        "public",
        "t",
        ["name"],
        ["TEXT"],
        backfill=True,
        source_types={"name": "TEXT"},
    )
    assert ddl and "TEXT" in ddl[0]


def test_unknown_source_type_keeps_previous_widen_behaviour():
    cursor = _FakeCursor([("id", "integer", None, 32, 0)])
    ddl = widen_existing_columns_native(
        cursor,
        "postgresql",
        "public",
        "t",
        ["id"],
        ["BIGINT"],
        backfill=True,
        source_types={},
    )
    assert ddl, "no source evidence must not silently disable widen"


PG = {
    "host": "localhost",
    "port": 5433,
    "database": "dataflow",
    "username": "postgres",
    "password": "postgres",  # nosec B106 - local test container
}


def _connect():
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(
            host=PG["host"],
            port=PG["port"],
            dbname=PG["database"],
            user=PG["username"],
            password=PG["password"],
            connect_timeout=3,
        )
    except psycopg2.Error:  # pragma: no cover - env without local postgres
        pytest.skip("local postgres unavailable")
    conn.autocommit = True
    return conn


@pytest.mark.skipif(
    os.environ.get("SKIP_LIVE_PG") == "1", reason="live postgres disabled"
)
def test_live_backfill_write_leaves_existing_integer_column_intact():
    from connectors.postgresql_writer import write_mapped_rows

    conn = _connect()
    table = f"widen_{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table}" (id INTEGER, name VARCHAR(10))')
    try:
        result = write_mapped_rows(
            host=PG["host"],
            port=PG["port"],
            database=PG["database"],
            username=PG["username"],
            password=PG["password"],
            schema="public",
            connection_string="",
            ssl=False,
            table_name=table,
            headers=["id", "name"],
            data_rows=[["1", "short"]],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "name", "target": "name"},
            ],
            column_types={"id": "INTEGER", "name": "VARCHAR(10)"},
            create_table=False,
            backfill_new_fields=True,
            job_id=f"job-{uuid.uuid4().hex[:8]}",
        )
        assert result.ok, result.error
        with conn.cursor() as cur:
            cur.execute(
                """SELECT column_name, data_type FROM information_schema.columns
                   WHERE table_schema='public' AND table_name=%s
                   ORDER BY ordinal_position""",
                (table,),
            )
            physical = dict(cur.fetchall())
        assert physical["id"] == "integer", (
            f"existing INTEGER carrier was altered to {physical['id']}"
        )
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        conn.close()
