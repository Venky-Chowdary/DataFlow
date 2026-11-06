"""D288 — a schema-qualified Snowflake source table is the schema and the table.

Prod (pf_bf107469b052): source_table ``PUBLIC.HQ0609_SF`` was quoted as one
identifier by the g3f population probe, so preflight failed "table does not
exist". Root: ``connectors.snowflake_reader._table_ref`` quoted the qualified
name whole; every Snowflake reader goes through it. fakesnow stands in for
Snowflake (no server here).
"""

from __future__ import annotations

import os
import sqlite3
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

CFG = {"type": "snowflake", "host": "localhost", "port": 443, "database": "DATAFLOW",
       "username": "test", "password": "test", "schema": "PUBLIC", "warehouse": "WH"}


def _fakesnow_connection():
    """A connection through the connector's own opener.

    ``snowflake_conn._open_connection`` installs fakesnow for the current
    ``FAKESNOW_DB_PATH`` (re-pointing a patch another test left installed), so
    the fixture and the engine read the same emulated database.
    """
    from connectors.snowflake_conn import _open_connection

    return _open_connection(
        account="localhost", username="test", password="test", database="DATAFLOW",
        schema="PUBLIC", warehouse="WH", connection_string="",
    )


@pytest.fixture()
def snow(monkeypatch, tmp_path):
    pytest.importorskip("fakesnow")
    path = tmp_path / "fakesnow"
    path.mkdir()
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(path))
    conn = _fakesnow_connection()
    try:
        cur = conn.cursor()
        cur.execute("CREATE OR REPLACE TABLE HQ0609_SF (ID INT, AMT NUMBER(12,2), NAME VARCHAR(20))")
        cur.execute("INSERT INTO HQ0609_SF VALUES (1, 1.5, 'a'), (2, NULL, '東京 ✓'), (3, 0, ''), (4, 2, '  ')")
    finally:
        conn.close()
    return path


@pytest.mark.parametrize("table", ["PUBLIC.HQ0609_SF", "HQ0609_SF"])
def test_g3f_population_walk_reads_the_qualified_table(snow, table):
    from services.preflight_population import _iter_table_population_for_preflight

    rows = _iter_table_population_for_preflight(
        source_kind="database", source_format="snowflake", source_connector_id="",
        source_config=CFG, source_table=table,
        mappings=[{"source": "AMT", "target": "AMT", "target_type": "DECIMAL(4,2)"},
                  {"source": "NAME", "target": "NAME", "target_type": "VARCHAR(1)"}],
        column_types={"AMT": "DECIMAL(12,2)", "NAME": "VARCHAR(20)"},
        dest_types={"AMT": "DECIMAL(4,2)", "NAME": "VARCHAR(1)"}, dest_db="postgresql",
    )
    got = list(rows)
    assert len(got) == 4
    assert {r["NAME"] for r in got} == {"a", "東京 ✓", "", "  "}


def test_execute_reads_the_qualified_source_table(snow, tmp_path, monkeypatch):
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    db = str(tmp_path / "dst.db")
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="snowflake", table="PUBLIC.HQ0609_SF",
                              **{k: v for k, v in CFG.items() if k != "type"}),
        destination=EndpointConfig(kind="database", format="sqlite", database=db, table="hq0609_sf"),
        sync_mode="full_refresh_overwrite", skip_preflight=False, validation_mode="strict",
    )
    jid = uuid.uuid4().hex[:24]
    fake.update_job_status(jid, "pending", transfer_request={})
    res = UniversalTransferEngine().execute_tracked(req, jid)
    assert res.success, res.error
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM hq0609_sf").fetchone()[0] == 4


def test_every_snowflake_name_site_splits_the_qualifier():
    from connectors.snowflake_conn import snowflake_schema_and_table

    assert snowflake_schema_and_table("PUBLIC", "PUBLIC.HQ0609_SF") == ("PUBLIC", "HQ0609_SF")
    assert snowflake_schema_and_table("PUBLIC", "staging.orders") == ("STAGING", "orders")
    assert snowflake_schema_and_table("PUBLIC", "HQ0609_SF") == ("PUBLIC", "HQ0609_SF")
    assert snowflake_schema_and_table("", "orders") == ("PUBLIC", "orders")
    # A quoted dotted name is one identifier and is not split.
    assert snowflake_schema_and_table("PUBLIC", '"a.b"') == ("PUBLIC", "a.b")


def test_desc_table_reads_the_qualified_name(snow):
    from connectors.snowflake_conn import _snowflake_desc_column_rows

    conn = _fakesnow_connection()
    try:
        with conn.cursor() as cur:
            rows, missing = _snowflake_desc_column_rows(cur, "PUBLIC", "PUBLIC.HQ0609_SF")
    finally:
        conn.close()
    assert not missing and {str(r[0]).upper() for r in rows} >= {"ID", "AMT", "NAME"}


def test_resolver_and_qualifier_split_inside_the_seam(snow):
    """F6: the sites that call resolve_*/snowflake_qualified_table (ladder read-back,
    COUNT, DESC, writer, table manager) get the split without touching each."""
    from connectors.snowflake_conn import (
        resolve_or_fold_snowflake_table,
        resolve_snowflake_table_name,
        snowflake_qualified_table,
    )

    conn = _fakesnow_connection()
    try:
        with conn.cursor() as cur:
            assert resolve_snowflake_table_name(cur, "PUBLIC", "PUBLIC.HQ0609_SF") == "HQ0609_SF"
            assert resolve_or_fold_snowflake_table(cur, "PUBLIC", "PUBLIC.HQ0609_SF") == "HQ0609_SF"
            # A different qualifier comes back re-qualified so the quoted ref keeps it.
            assert resolve_or_fold_snowflake_table(cur, "PUBLIC", "STAGING.NEW_T") == "STAGING.NEW_T"
            cur.execute(f"SELECT COUNT(*) FROM {snowflake_qualified_table('PUBLIC', 'PUBLIC.HQ0609_SF')}")
            assert cur.fetchone()[0] == 4
    finally:
        conn.close()
    assert snowflake_qualified_table("PUBLIC", "PUBLIC.HQ0609_SF") == '"PUBLIC"."HQ0609_SF"'
    assert snowflake_qualified_table("PUBLIC", "STAGING.NEW_T") == '"STAGING"."NEW_T"'
    assert snowflake_qualified_table("PUBLIC", "HQ0609_SF") == '"PUBLIC"."HQ0609_SF"'


def test_ladder_read_back_of_a_qualified_destination(snow, tmp_path, monkeypatch):
    """F6: Snowflake as the destination with a schema-qualified table — the
    Gate-8 read-back SELECT (reconcile_step) addresses schema and table."""
    import sqlite3

    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    src_db = tmp_path / "src.db"
    with sqlite3.connect(str(src_db)) as c:
        c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        c.executemany("INSERT INTO t VALUES (?, ?)", [(1, "a"), (2, None), (3, ""), (4, "東京 ✓")])
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    dest_table = "PUBLIC.D288_OUT_" + uuid.uuid4().hex[:6].upper()
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src_db), table="t"),
        destination=EndpointConfig(kind="database", format="snowflake", table=dest_table,
                                   **{k: v for k, v in CFG.items() if k != "type"}),
        sync_mode="full_refresh_overwrite", skip_preflight=True, validation_mode="strict",
    )
    jid = uuid.uuid4().hex[:24]
    fake.update_job_status(jid, "pending", transfer_request={})
    res = UniversalTransferEngine().execute_tracked(req, jid)
    assert res.success, res.error
    assert res.reconciliation.get("passed") is True and res.reconciliation.get("target_rows") == 4
