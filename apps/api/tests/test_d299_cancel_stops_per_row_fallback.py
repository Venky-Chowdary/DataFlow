"""D299: a cancel stops the row-by-row fallback writer within one row.

Repro (QA, Postgres -> new SQL Server, incremental_upsert): a CHECK
constraint makes a batch fail, the writer falls back to one MERGE + COMMIT
per row (about 40 rows/min), and the job is cancelled. The batch loop polls
the cancel probe before every sub-chunk (D168), but the per-row salvage
loop inside a failed sub-chunk never did: rows kept landing for minutes
after the job showed 'cancelled', and the cancel's committed-row count
missed every row the fallback had written.

These tests drive the real writers against the local engines; each fails on
the pre-fix code (all rows of the chunk land after the cancel).
"""

from __future__ import annotations

import pytest

from tests.r13_mapping_harness import (
    drop,
    mssql_cfg,
    mysql_cfg,
    run_sql,
    token,
)


def _cancelling_probe(cancel_after_rows: int, written_rows: list[int]):
    """Probe that 'presses cancel' once ``cancel_after_rows`` rows landed."""
    from services.cancel_check import cancel_check_for
    from services.mongodb_service import MemoryMongoDBService

    mongo = MemoryMongoDBService()
    job_id = mongo.create_transfer_job({"_id": f"d299-{token()}", "status": "running"})
    real_check = cancel_check_for(mongo, job_id)

    def probe(rows_written=None):
        if written_rows and written_rows[0] >= cancel_after_rows:
            mongo.request_job_cancel(job_id)
        real_check(rows_written)

    return probe


def _rows(n: int) -> list[list[str]]:
    return [[str(i), f"name ✓ é {i}"] for i in range(1, n + 1)]


def _mappings() -> list[dict]:
    return [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}]


def test_sqlserver_per_row_fallback_stops_after_cancel(monkeypatch):
    cfg = mssql_cfg()
    pytest.importorskip("pyodbc")
    import connectors.generic_sql as gs
    from services.cancel_check import set_cancel_probe
    from services.error_handling import TransferCancelled

    table = f"d299_{token()}"
    drop("sqlserver", table)
    run_sql(
        "sqlserver",
        f"CREATE TABLE [{table}] (id INT NOT NULL PRIMARY KEY, name NVARCHAR(100))",
        # One poison row (id 1) fails the whole MERGE batch -> per-row fallback.
        f"ALTER TABLE [{table}] WITH NOCHECK ADD CONSTRAINT ck_{table} CHECK (id <> 1)",
    )
    landed: list[int] = [0]
    real_upsert = gs._upsert_batch

    def counting_upsert(conn, table_obj, batch, *a, **k):
        n = real_upsert(conn, table_obj, batch, *a, **k)
        if len(batch) == 1:  # a per-row fallback write
            landed[0] += 1
        return n

    monkeypatch.setattr(gs, "_upsert_batch", counting_upsert)
    set_cancel_probe(_cancelling_probe(3, landed))
    try:
        with pytest.raises(TransferCancelled) as ei:
            gs.write_mapped_rows(
                host=cfg["host"], port=cfg["port"], database=cfg["database"],
                username=cfg["username"], password=cfg["password"], schema="dbo",
                connection_string="", ssl=False, type="sqlserver",
                extra=cfg["extra"], table_name=table, headers=["id", "name"],
                data_rows=_rows(30), mappings=_mappings(),
                column_types={"id": "INT", "name": "NVARCHAR(100)"},
                create_table=False, write_mode="upsert", conflict_columns=["id"],
                error_policy="quarantine",
            )
        count = run_sql("sqlserver", f"SELECT COUNT(*) FROM [{table}]")[0][0]  # nosec B608
        # Cancel pressed after the 3rd per-row write (row 1 is quarantined):
        # the loop stops before the next row, not after all 29 good rows.
        assert count <= 3, count
        assert ei.value.rows_written == count
        # The session is released: no open transaction holds the table.
        assert run_sql(
            "sqlserver",
            "SELECT COUNT(*) FROM sys.dm_tran_locks l JOIN sys.partitions p "
            "ON l.resource_associated_entity_id = p.hobt_id "
            f"WHERE p.object_id = OBJECT_ID('dbo.{table}')",
        )[0][0] == 0
    finally:
        set_cancel_probe(None)
        drop("sqlserver", table)


def test_mysql_per_row_fallback_stops_after_cancel(monkeypatch):
    cfg = mysql_cfg()
    import connectors.mysql_writer as mw
    from services.cancel_check import set_cancel_probe
    from services.error_handling import TransferCancelled

    table = f"d299_{token()}"
    drop("mysql", table)
    run_sql(
        "mysql",
        f"CREATE TABLE `{table}` (id INT NOT NULL PRIMARY KEY, name VARCHAR(100), "
        f"CONSTRAINT ck_{table} CHECK (id <> 1)) CHARACTER SET utf8mb4",
    )
    landed: list[int] = [0]
    probe = _cancelling_probe(3, landed)

    def counting_probe(rows_written=None):
        if rows_written is not None:
            landed[0] = rows_written
        probe(rows_written)

    set_cancel_probe(counting_probe)
    try:
        with pytest.raises(TransferCancelled) as ei:
            mw.write_mapped_rows(
                host=cfg["host"], port=cfg["port"], database=cfg["database"],
                username=cfg["username"], password=cfg["password"], schema="",
                connection_string="", ssl=False, table_name=table,
                headers=["id", "name"], data_rows=_rows(30), mappings=_mappings(),
                column_types={"id": "INT", "name": "VARCHAR(100)"},
                create_table=False, write_mode="insert", error_policy="quarantine",
            )
        count = run_sql("mysql", f"SELECT COUNT(*) FROM `{table}`")[0][0]  # nosec B608
        assert count <= 3, count
        assert ei.value.rows_written == count
    finally:
        set_cancel_probe(None)
        drop("mysql", table)
