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


@pytest.fixture()
def snow(monkeypatch, tmp_path):
    pytest.importorskip("fakesnow")
    import fakesnow
    import snowflake.connector as snc

    path = tmp_path / "fakesnow"
    path.mkdir()
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(path))
    with fakesnow.patch(db_path=str(path)):
        conn = snc.connect(account="x", user="test", password="test", database="DATAFLOW", schema="PUBLIC")
        cur = conn.cursor()
        cur.execute("CREATE TABLE HQ0609_SF (ID INT, AMT NUMBER(12,2), NAME VARCHAR(20))")
        cur.execute("INSERT INTO HQ0609_SF VALUES (1, 1.5, 'a'), (2, NULL, '東京 ✓'), (3, 0, ''), (4, 2, '  ')")
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
