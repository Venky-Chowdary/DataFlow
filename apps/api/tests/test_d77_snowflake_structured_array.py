"""D77 — typed PostgreSQL arrays could not be inserted into Snowflake.

DataFlow creates ``ARRAY(NUMBER(38,0))`` / ``ARRAY(VARCHAR)`` for ``int[]`` /
``text[]``. Both write paths parse the JSON text with ``PARSE_JSON``, which is a
VARIANT, and Snowflake does not assign a VARIANT to a structured ARRAY column
without a cast. fakesnow has no structured types, so the SQL itself is pinned
here and the arrays are also loaded end to end to prove the cast is accepted.
"""

from __future__ import annotations

import csv
import uuid
from pathlib import Path

import psycopg2
import pytest

from connectors import snowflake_writer as sf
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests import test_d120_snowflake_strict_e2e as _strict
from tests.test_d120_snowflake_strict_e2e import PG_CFG

snowflake_semantics = _strict.snowflake_semantics
engine_env = _strict.engine_env


class _Cur:
    def __init__(self) -> None:
        self.sql: list[str] = []
        self.staged: list[list[str]] = []
        self._rows: list[tuple] = []

    def execute(self, sql: str, params=None):  # noqa: ANN001
        text = " ".join(sql.split())
        self.sql.append(text)
        if text.startswith("PUT file://"):
            with Path(text[len("PUT file://"):].split(" ")[0]).open(newline="") as fh:
                self.staged = list(csv.reader(fh))
        elif text.startswith("COPY INTO"):
            n = len(self.staged) - 1
            self._rows = [("f", "LOADED", n, n, 1, 0, None)]
        return self

    def fetchall(self):
        return self._rows


def test_insert_casts_parsed_json_to_the_structured_carrier():
    cur = _Cur()
    sf._batch_insert_rows(
        cur, "T", ["ID", "A", "V", "ANY_ARR"],
        ["NUMBER(38,0)", "ARRAY(NUMBER(38,0))", "VARIANT", "ARRAY"],
        [("1", "[1,2]", "{}", "[3]")],
    )
    assert "PARSE_JSON(column2)::ARRAY(NUMBER(38,0))" in cur.sql[0]
    # Unstructured carriers take the parsed value as is.
    assert "PARSE_JSON(column3)," in cur.sql[0]
    assert "PARSE_JSON(column4) FROM" in cur.sql[0]


def test_copy_casts_parsed_json_to_the_structured_carrier():
    cur = _Cur()
    rows = [(str(i), "[1,2]") for i in range(sf.COPY_THRESHOLD)]
    method = sf._load_rows_into_table(
        cur, "T", ["ID", "A"], ["NUMBER(38,0)", "ARRAY(VARCHAR)"], rows,
        prefer_copy=True, conn=object(), policy="fail",
    )
    assert method == "copy_into"
    copy_sql = next(s for s in cur.sql if s.startswith("COPY INTO"))
    assert "PARSE_JSON($2)::ARRAY(VARCHAR)" in copy_sql


@pytest.fixture()
def pg_arrays():
    try:
        conn = psycopg2.connect(
            host=PG_CFG["host"], port=PG_CFG["port"], dbname=PG_CFG["database"],
            user=PG_CFG["username"], password=PG_CFG["password"], connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    table = "d77_src_" + uuid.uuid4().hex[:8]
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, nums INT[], tags TEXT[])")
        cur.execute(
            f"INSERT INTO {table} VALUES (1, '{{1,2,3}}', '{{a,b}}'), (2, NULL, NULL)"
        )
    try:
        yield table
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()


@pytest.mark.parametrize("sync_mode", ["full_refresh_overwrite", "upsert"])
def test_pg_arrays_reach_snowflake(engine_env, pg_arrays, sync_mode):
    dst = "d77_dst_" + uuid.uuid4().hex[:8]
    job_id = "d77" + uuid.uuid4().hex[:16]
    engine_env.update_job_status(job_id, "pending", transfer_request={})
    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql", schema="public",
            table=pg_arrays, **PG_CFG,
        ),
        destination=EndpointConfig(
            kind="database", format="snowflake", host="localhost", port=443,
            database="dataflow", username="test", password="test",
            schema="public", table=dst,
        ),
        sync_mode=sync_mode,
        stream_contracts=[{
            "name": pg_arrays, "sync_mode": sync_mode,
            "primary_key": "id", "selected": True,
        }],
    )
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success is True, result.error
    assert (result.reconciliation or {}).get("passed") is True, result.reconciliation
    rows = _snowflake_rows_columns(dst)
    assert rows[1] == ("[1,2,3]", '["a","b"]')
    assert rows[2] == (None, None)


def _snowflake_rows_columns(table: str) -> dict[int, tuple]:
    import json

    from connectors.snowflake_conn import get_connection

    conn = get_connection(
        account="localhost", username="test", password="test",
        database="dataflow", schema="PUBLIC", warehouse="", connection_string="",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT "ID", "NUMS", "TAGS" FROM "PUBLIC"."{table.upper()}"')
            return {
                int(r[0]): tuple(
                    None if v is None else json.dumps(json.loads(v), separators=(",", ":"))
                    if isinstance(v, str) else json.dumps(v, separators=(",", ":"))
                    for v in r[1:]
                )
                for r in cur.fetchall()
            }
    finally:
        conn.close()

