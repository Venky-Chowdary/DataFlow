"""D49 — a Snowflake table with timestamps loads out without a risk contract.

Snowflake spells three timestamp kinds: TIMESTAMP_NTZ (wall clock), TIMESTAMP_TZ
(instant + offset) and TIMESTAMP_LTZ (instant, session-relative). Reading them
out was refused as a lossy collapse until the operator signed a contract, even
into a destination whose carrier keeps the same polarity (PostgreSQL
``timestamp`` / ``timestamptz``).

fakesnow stands in for Snowflake; the destination is live PostgreSQL.
"""

from __future__ import annotations

import os
import uuid

import psycopg2
import pytest

pytest.importorskip("fakesnow")

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

PG = dict(host="127.0.0.1", port=5432, database="dataflow", username="dataflow", password="dataflow")


@pytest.fixture()
def env(tmp_path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _pg(sql: str):
    try:
        conn = psycopg2.connect(
            host=PG["host"], port=PG["port"], dbname=PG["database"],
            user=PG["username"], password=PG["password"], connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall() if cur.description else []
    finally:
        conn.close()


def _seed_snowflake(table: str) -> None:
    from connectors.snowflake_conn import get_connection

    conn = get_connection(
        account="localhost", username="test", password="test",
        database="dataflow", schema="PUBLIC", warehouse="", connection_string="",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE OR REPLACE TABLE "PUBLIC"."{table}" '
                "(ID INT, TS_NTZ TIMESTAMP_NTZ, TS_TZ TIMESTAMP_TZ, TS_LTZ TIMESTAMP_LTZ)"
            )
            cur.execute(
                f'INSERT INTO "PUBLIC"."{table}" VALUES '
                "(1, '2024-03-01 10:15:00', '2024-03-01 10:15:00', '2024-03-01 10:15:00'), "
                "(2, '2024-06-30 23:59:59', '2024-06-30 23:59:59', '2024-06-30 23:59:59')"
            )
    finally:
        conn.close()


def test_snowflake_timestamps_load_to_postgres_without_a_contract(env):
    src = "D49_SRC_" + uuid.uuid4().hex[:8].upper()
    dst = "d49_dst_" + uuid.uuid4().hex[:8]
    _seed_snowflake(src)
    job_id = "d49" + uuid.uuid4().hex[:16]
    env.update_job_status(job_id, "pending", transfer_request={})
    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="snowflake", host="localhost", port=443,
            database="dataflow", username="test", password="test",
            schema="public", table=src,
        ),
        destination=EndpointConfig(
            kind="database", format="postgresql", schema="public", table=dst, **PG
        ),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
    )
    try:
        result = UniversalTransferEngine().execute_tracked(request, job_id)
        assert result.success is True, result.error
        assert result.records_transferred == 2
        types = {
            name.lower(): kind
            for name, kind in _pg(
                "SELECT column_name, data_type FROM information_schema.columns "
                f"WHERE table_schema = 'public' AND table_name = '{dst}'"
            )
        }
        assert types["ts_ntz"] == "timestamp without time zone"
        assert types["ts_tz"] == "timestamp with time zone"
        assert types["ts_ltz"] == "timestamp with time zone"
        landed = _pg(
            f'SELECT "ID", "TS_NTZ"::text, ("TS_TZ" AT TIME ZONE \'UTC\')::text '
            f'FROM public."{dst}" ORDER BY "ID"'
        )
        assert landed == [
            (1, "2024-03-01 10:15:00", "2024-03-01 10:15:00"),
            (2, "2024-06-30 23:59:59", "2024-06-30 23:59:59"),
        ]
    finally:
        _pg(f'DROP TABLE IF EXISTS public."{dst}"')


@pytest.mark.parametrize(
    "population,narrows",
    [
        (["2024-03-01 10:15:00.123456", "2024-03-01 10:15:00"], False),
        (["2024-03-01 10:15:00.123456789"], True),
        ([None, ""], True),  # nothing measured proves nothing
        (None, True),
    ],
    ids=["fits-microseconds", "holds-nanoseconds", "only-null", "unmeasured"],
)
def test_declared_nanoseconds_narrow_only_when_the_rows_use_them(population, narrows):
    from services.type_system import is_lossy_coercion, temporal_precision_would_narrow

    assert (
        temporal_precision_would_narrow(
            "TIMESTAMP_NTZ(9)", "DATETIME(6)", dest_db="mysql", population=population
        )
        is narrows
    )
    assert (
        is_lossy_coercion(
            "TIMESTAMP_NTZ(9)", "DATETIME(6)", dest_db="mysql", population=population
        )
        is narrows
    )
