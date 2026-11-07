"""QA D252: a SQL Server CDC run created MySQL text columns on the server default.

QA (production 38eae820): run 1 of QAENV-SQLServer-CDC.qaenv_seed -> MySQL
``sync_mode=cdc`` created ``name`` as ``utf8mb4_0900_ai_ci`` (accent-insensitive)
while the source is ``SQL_Latin1_General_CP1_CI_AS`` (accent-sensitive). The
rerun then refused, correctly, on an accent-polarity change DataFlow itself had
made. ``full_refresh_append`` on the same route creates ``utf8mb4_0900_as_ci``:
the CDC create path never handed the writer the source schema catalog, so the
collation carry was skipped.

Route: MCP ``start_transfer`` -> ``/api/v1/copilot/confirm`` -> job, real
SQL Server 2022 with the Agent running and native CDC enabled, into real MySQL 8.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h
from tests.test_d223_cdc_schema_qualified_source_e2e import (  # noqa: F401
    _agent_running,
    _drop_cdc_source,
    _ms,
    client,
)

pytestmark = pytest.mark.timeout(900)

_ROWS = [(1, "café"), (2, "cafe"), (3, "naïve 中文")]


def _create_source(table: str) -> None:
    conn = _ms()
    try:
        conn.execute(
            "IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME() "
            "AND is_cdc_enabled = 1) EXEC sys.sp_cdc_enable_db"
        )
        conn.execute(
            f"CREATE TABLE dbo.[{table}] (id INT NOT NULL PRIMARY KEY, "
            "note NVARCHAR(40) COLLATE SQL_Latin1_General_CP1_CI_AS NULL)"
        )
        conn.cursor().executemany(
            f"INSERT INTO dbo.[{table}] (id, note) VALUES (?, ?)", _ROWS
        )
        conn.execute(
            "EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', "
            f"@source_name = N'{table}', @role_name = NULL, @supports_net_changes = 0"
        )
    finally:
        conn.close()


def test_cdc_create_new_carries_accent_sensitive_collation_and_rerun_stages(client):
    if not h.port_up(3306):
        pytest.skip("MySQL is not running on 3306")
    if not _agent_running():
        pytest.skip("SQL Server Agent is not running (native CDC capture job)")
    from services.connector_store import create_connector

    token = uuid.uuid4().hex[:8]
    table, dst_t = f"d252_cdc_{token}", f"d252_my_{token}"
    _create_source(table)
    try:
        src = create_connector({"name": f"d252-s-{token}", "role": "source",
                                "workspace_id": "", "ssl": False,
                                **h.dest_config("sqlserver"), "schema": "dbo"})
        dst = create_connector({"name": f"d252-d-{token}", "role": "destination",
                                "workspace_id": "", "ssl": False,
                                **h.dest_config("mysql")})
        args = {
            "source_connector_id": src.id, "source_table": table,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "cdc", "primary_key": "id",
        }
        ok, staged = h.mcp_call(client, "start_transfer", args)
        assert ok, {"error": staged}
        assert not h.blockers_of(staged), h.blockers_of(staged)
        job = h.confirm(client, staged["ack_id"], reason="d252")
        assert str(job.get("status")) == "completed", str(job)[:2000]
        got = h.query(
            "mysql",
            "SELECT collation_name FROM information_schema.columns "
            f"WHERE table_schema = DATABASE() AND table_name = '{dst_t}' "
            "AND column_name = 'note'",
        )
        assert got and str(got[0][0]).lower() == "utf8mb4_0900_as_ci", got
        # Accent-sensitive equality kept: café and cafe stay two rows.
        assert sorted(h.query("mysql", f"SELECT id, note FROM `{dst_t}`")) == sorted(_ROWS)
        # The rerun reads the table this run created: no polarity refusal.
        ok, again = h.mcp_call(client, "start_transfer", args)
        assert ok, {"error": again}
        assert not h.blockers_of(again), h.blockers_of(again)
    finally:
        _drop_cdc_source(table)
        h.drop_table("mysql", dst_t)


def test_cdc_eos_create_ddl_carries_source_collation_without_services():
    """Hermetic: the EOS create-new DDL takes the planner's COLLATE clause."""
    from connectors.cdc_eos_sa import (
        _eos_collation_clauses,
        _eos_write_shape,
        _with_collation,
        _col_sql_type,
        _logical_for,
    )
    from src.transfer.adapters_introspect import source_schema_catalog_payload

    cols = ["id", "note"]
    catalog = source_schema_catalog_payload(
        "sqlserver",
        cols,
        (
            {"id": "INT", "note": "NVARCHAR(40) COLLATE SQL_Latin1_General_CP1_CI_AS"},
            {"id": False, "note": True},
            {"primary_key": ["id"]},
        ),
    )
    maps = [{"source": c, "target": c, "confidence": 1.0} for c in cols]
    shape_maps, _, target_cols, logical_by = _eos_write_shape(
        "mysql", maps, {"id": "integer", "note": "string"}
    )
    clauses = _eos_collation_clauses(
        "mysql", catalog, shape_maps, target_cols, ["id"], logical_by
    )
    ddl = _with_collation(
        _col_sql_type("mysql", "note", ["id"], _logical_for("note", logical_by)),
        "note",
        clauses,
    )
    assert "COLLATE utf8mb4_0900_as_ci" in ddl, ddl
    assert ddl.upper().count("CHARACTER SET") <= 1, ddl
    assert _eos_collation_clauses(
        "mysql", None, shape_maps, target_cols, ["id"], logical_by
    ) == {}
