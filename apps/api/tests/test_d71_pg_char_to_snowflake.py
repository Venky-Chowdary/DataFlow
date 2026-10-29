"""D71 — PostgreSQL CHAR(n) -> Snowflake was blocked as a fidelity collapse.

Snowflake's CHAR is a synonym for VARCHAR that never blank-pads, so the
planner's ``VARCHAR(n)`` is the only carrier the dialect can spell, and the
pad PostgreSQL returns (``'ab   '``) is carried in the value. The CHAR->VARCHAR
pad-polarity rule read that as a collapse and preflight refused every
CHAR(n) column. The NULL row is the D106 CHAR arm (a NULL in a short CHAR
must not be length-checked as the sentinel).
"""

from __future__ import annotations

import uuid

import psycopg2
import pytest

from services.type_system import fixed_width_pad_polarity_loss
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests import test_d120_snowflake_strict_e2e as _strict
from tests.test_d120_snowflake_strict_e2e import PG_CFG, _snowflake_rows

# Real-Snowflake rules over fakesnow (see test_d120_snowflake_strict_e2e).
snowflake_semantics = _strict.snowflake_semantics
engine_env = _strict.engine_env


def test_d71_char_to_varchar_is_not_a_pad_collapse_on_snowflake():
    assert not fixed_width_pad_polarity_loss("CHAR(5)", "VARCHAR(5)", dest_db="snowflake")
    assert not fixed_width_pad_polarity_loss("VARCHAR(5)", "CHAR(5)", dest_db="snowflake")
    # Engines with a real blank-padded CHAR keep the rule.
    assert fixed_width_pad_polarity_loss("CHAR(5)", "VARCHAR(5)", dest_db="postgresql")
    assert fixed_width_pad_polarity_loss("CHAR(5)", "VARCHAR(5)", dest_db="mysql")


@pytest.fixture()
def pg_char_source():
    try:
        conn = psycopg2.connect(
            host=PG_CFG["host"], port=PG_CFG["port"], dbname=PG_CFG["database"],
            user=PG_CFG["username"], password=PG_CFG["password"], connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    table = "d71_src_" + uuid.uuid4().hex[:8]
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE {table} (id INT PRIMARY KEY, code CHAR(5), "
            "label TEXT, amt NUMERIC(12,2))"
        )
        cur.execute(
            f"INSERT INTO {table} VALUES "
            "(1, 'ab', 'x', 1), (2, NULL, 'y', 2), (3, 'abcde', 'z', 3)"
        )
    try:
        yield table
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()


def test_d71_pg_char_lands_in_snowflake_with_pad(engine_env, pg_char_source):
    dst = "d71_dst_" + uuid.uuid4().hex[:8]
    job_id = "d71" + uuid.uuid4().hex[:16]
    engine_env.update_job_status(job_id, "pending", transfer_request={})
    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql", schema="public",
            table=pg_char_source, **PG_CFG,
        ),
        destination=EndpointConfig(
            kind="database", format="snowflake", host="localhost", port=443,
            database="dataflow", username="test", password="test",
            schema="public", table=dst,
        ),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": pg_char_source, "sync_mode": "full_refresh_overwrite",
            "primary_key": "id", "selected": True,
        }],
    )
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success is True, result.error
    recon = result.reconciliation or {}
    assert recon.get("passed") is True, recon
    rows = _snowflake_rows(dst)
    assert rows[1][0] == "ab   "
    assert rows[2][0] is None
    assert rows[3][0] == "abcde"
