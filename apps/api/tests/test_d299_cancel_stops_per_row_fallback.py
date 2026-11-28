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


def test_sqlite_per_row_fallback_stops_after_cancel(tmp_path, monkeypatch):
    """The same per-row loop SQL Server uses, on a local file, so CI can prove it."""
    import sqlite3

    import connectors.generic_sql as gs
    from services.cancel_check import set_cancel_probe
    from services.error_handling import TransferCancelled

    monkeypatch.setenv("DATAFLOW_CONFINE_LOCAL_FILES", "0")
    db = tmp_path / "d299.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, CHECK (id <> 1))")
    conn.commit()
    conn.close()
    landed: list[int] = [0]
    real_upsert = gs._upsert_batch

    def counting_upsert(connection, table_obj, batch, *args, **kwargs):
        written = real_upsert(connection, table_obj, batch, *args, **kwargs)
        if len(batch) == 1:
            landed[0] += 1
        return written

    def probe(rows_written=None):
        if landed[0] >= 3:
            raise TransferCancelled(
                "cancelled",
                rows_written=rows_written if rows_written is not None else landed[0],
            )

    monkeypatch.setattr(gs, "_upsert_batch", counting_upsert)
    set_cancel_probe(probe)
    try:
        with pytest.raises(TransferCancelled) as caught:
            gs.write_mapped_rows(
                host="", port=0, database=str(db), username="", password="",
                schema="", connection_string="", ssl=False, type="sqlite",
                table_name="t", headers=["id", "name"],
                data_rows=_rows(20), mappings=_mappings(),
                column_types={"id": "INTEGER", "name": "TEXT"},
                create_table=False, write_mode="upsert", conflict_columns=["id"],
                error_policy="quarantine",
            )
        count = sqlite3.connect(db).execute("SELECT COUNT(*) FROM t").fetchone()[0]
        assert count <= 3, count
        assert caught.value.rows_written == count
    finally:
        set_cancel_probe(None)


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
    at_cancel: list[int] = []

    def counting_probe(rows_written=None):
        if rows_written is not None:
            landed[0] = rows_written
        try:
            probe(rows_written)
        except TransferCancelled:
            at_cancel.append(landed[0])
            raise

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
        # The salvage splits the failed chunk (connectors.batch_salvage) and
        # polls before every attempt, so nothing lands after the poll that saw
        # the cancel: the table holds exactly what had landed then, the cancel
        # reports it, and the chunk's other rows never land.
        assert at_cancel and count == at_cancel[0], (count, at_cancel)
        assert 3 <= count < 29, count
        assert ei.value.rows_written == count
    finally:
        set_cancel_probe(None)
        drop("mysql", table)


def test_postgresql_salvage_discards_its_chunk_after_cancel(monkeypatch):
    """PostgreSQL salvages a failed chunk row by row under SAVEPOINTs and commits
    once at the end of the chunk. A cancel pressed during that salvage must
    discard the chunk: before the fix every salvaged row of the chunk was
    committed after the job already said cancelled (a whole chunk late)."""
    from tests.r13_mapping_harness import pg_cfg

    cfg = pg_cfg()
    import connectors.postgresql_writer as pw
    from services.cancel_check import set_cancel_probe
    from services.error_handling import TransferCancelled

    table = f"d299_{token()}"
    drop("postgresql", table)
    run_sql(
        "postgresql",
        f'CREATE TABLE "{table}" (id INT PRIMARY KEY, name VARCHAR(100), '
        "CHECK (id <> 1))",
    )
    salvaging: list[bool] = [False]
    real_is_data_error = pw.is_sql_data_error

    def spying_is_data_error(exc):
        # The batch INSERT failed on row 1: the writer is about to salvage.
        salvaging[0] = True
        return real_is_data_error(exc)

    def probe(rows_written=None):
        if salvaging[0]:
            raise TransferCancelled("cancelled", rows_written=rows_written)

    monkeypatch.setattr(pw, "is_sql_data_error", spying_is_data_error)
    set_cancel_probe(probe)
    try:
        with pytest.raises(TransferCancelled) as caught:
            pw.write_mapped_rows(
                host=cfg["host"], port=cfg["port"], database=cfg["database"],
                username=cfg["username"], password=cfg["password"], schema="public",
                connection_string="", ssl=False, table_name=table,
                headers=["id", "name"], data_rows=_rows(30), mappings=_mappings(),
                column_types={"id": "INTEGER", "name": "VARCHAR(100)"},
                create_table=False, write_mode="insert", error_policy="quarantine",
            )
        count = run_sql("postgresql", f'SELECT COUNT(*) FROM "{table}"')[0][0]  # nosec B608
        assert count == 0, count
        assert caught.value.rows_written == 0
        # The cancelled chunk left no open transaction behind on the table.
        assert run_sql(
            "postgresql",
            "SELECT COUNT(*) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation "
            f"WHERE c.relname = '{table}'",  # nosec B608 - generated test name
        )[0][0] == 0
    finally:
        set_cancel_probe(None)
        drop("postgresql", table)


def _pg_salvage_cancel_after(monkeypatch, salvaged_before_cancel: int, seen: dict):
    """Cancel once the PostgreSQL salvage has released ``salvaged_before_cancel`` rows.

    The guard polls before each salvaged row (throttled in production; every
    row here), so poll ``n`` runs after rows ``1..n-1`` of the chunk were
    tried. Rows already salvaged sit in the open chunk transaction.
    """
    import functools

    import connectors.postgresql_writer as pw
    from services.cancel_check import set_cancel_probe
    from services.error_handling import TransferCancelled

    monkeypatch.setattr(pw, "write_chunk_size", lambda *_a, **_kw: 10)
    monkeypatch.setattr(pw, "SalvageCancelGuard", functools.partial(pw.SalvageCancelGuard, interval=0.0))
    salvaging = [False]
    polls = [0]
    real_is_data_error = pw.is_sql_data_error

    def spying_is_data_error(exc):
        salvaging[0] = True
        return real_is_data_error(exc)

    def probe(rows_written=None):
        if not salvaging[0]:
            return
        polls[0] += 1
        # Poll n runs before the chunk's n-th row: rows 1..n-1 were tried.
        if polls[0] == salvaged_before_cancel + 1:
            seen["rows_written_at_cancel"] = rows_written
            on_cancel = seen.get("on_cancel")
            if on_cancel:
                on_cancel()
            raise TransferCancelled("cancelled", rows_written=rows_written)

    monkeypatch.setattr(pw, "is_sql_data_error", spying_is_data_error)
    set_cancel_probe(probe)


def test_postgresql_salvage_rolls_back_rows_it_already_salvaged_on_cancel(monkeypatch):
    """D299: chunk 1 (ids 1-10) commits; chunk 2 fails on id 15 and is salvaged
    (split into savepointed batches, then single rows). A cancel after some of
    ids 11-14 were released under their savepoints must roll those back too:
    the table keeps exactly chunk 1, and the cancel reports the 10 committed
    rows."""
    import psycopg2

    from tests.r13_mapping_harness import pg_cfg

    cfg = pg_cfg()
    import connectors.postgresql_writer as pw
    from services.cancel_check import set_cancel_probe
    from services.error_handling import TransferCancelled

    table = f"d299_{token()}"
    drop("postgresql", table)
    run_sql("postgresql", f'CREATE TABLE "{table}" (id INT PRIMARY KEY, name VARCHAR(100), CHECK (id <> 15))')
    conn = psycopg2.connect(
        host=cfg["host"], port=cfg["port"], dbname=cfg["database"], user=cfg["username"], password=cfg["password"]
    )
    seen: dict = {}

    def in_flight():
        # The writer's own transaction, at the moment of the cancel.
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{table}" WHERE id > 10')  # nosec B608 - generated test name
            seen["salvaged_uncommitted"] = cur.fetchone()[0]

    seen["on_cancel"] = in_flight
    _pg_salvage_cancel_after(monkeypatch, 3, seen)
    try:
        with pytest.raises(TransferCancelled) as caught:
            pw.write_mapped_rows(
                host=cfg["host"], port=cfg["port"], database=cfg["database"],
                username=cfg["username"], password=cfg["password"], schema="public",
                connection_string="", ssl=False, table_name=table,
                headers=["id", "name"], data_rows=_rows(30), mappings=_mappings(),
                column_types={"id": "INTEGER", "name": "VARCHAR(100)"},
                create_table=False, write_mode="insert", error_policy="quarantine",
                connection=conn, close_connection=False,
            )
        # Rows of chunk 2 really were salvaged into the open chunk when cancel
        # came (the split lands ids 11-12 as one batch before the poll that
        # cancels)...
        assert seen["salvaged_uncommitted"] >= 1, seen
        # ...and none of them committed afterwards.
        ids = [r[0] for r in run_sql("postgresql", f'SELECT id FROM "{table}" ORDER BY id')]  # nosec B608
        assert ids == list(range(1, 11)), ids
        assert caught.value.rows_written == 10
        assert seen["rows_written_at_cancel"] == 10
        assert run_sql(
            "postgresql",
            "SELECT COUNT(*) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation "
            f"WHERE c.relname = '{table}'",  # nosec B608 - generated test name
        )[0][0] == 0
    finally:
        set_cancel_probe(None)
        conn.close()
        drop("postgresql", table)


def test_postgresql_salvage_cancel_in_a_held_overwrite_discards_only_its_chunk(monkeypatch):
    """A full-refresh overwrite holds TRUNCATE and every chunk in one
    transaction. A cancel mid-salvage rolls back to the chunk savepoint
    (df_ow_chunk): the salvaged rows go, the transaction stays usable, and
    the caller's rollback of the held overwrite leaves the old table intact."""
    import psycopg2

    from connectors.write_resilience import finish_held_overwrite
    from tests.r13_mapping_harness import pg_cfg

    cfg = pg_cfg()
    import connectors.postgresql_writer as pw
    from services.cancel_check import set_cancel_probe
    from services.error_handling import TransferCancelled

    table = f"d299_{token()}"
    drop("postgresql", table)
    run_sql("postgresql", f'CREATE TABLE "{table}" (id INT PRIMARY KEY, name VARCHAR(100), CHECK (id <> 15))')
    run_sql("postgresql", f"INSERT INTO \"{table}\" (id, name) VALUES (100, 'old-a'), (101, 'old-b')")  # nosec B608
    conn = psycopg2.connect(
        host=cfg["host"], port=cfg["port"], dbname=cfg["database"], user=cfg["username"], password=cfg["password"]
    )
    conn.autocommit = False
    seen: dict = {}

    def in_flight():
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{table}" WHERE id > 10 AND id < 100')  # nosec B608
            seen["salvaged_uncommitted"] = cur.fetchone()[0]

    seen["on_cancel"] = in_flight
    _pg_salvage_cancel_after(monkeypatch, 3, seen)
    try:
        with pytest.raises(TransferCancelled):
            pw.write_mapped_rows(
                host=cfg["host"], port=cfg["port"], database=cfg["database"],
                username=cfg["username"], password=cfg["password"], schema="public",
                connection_string="", ssl=False, table_name=table,
                headers=["id", "name"], data_rows=_rows(30), mappings=_mappings(),
                column_types={"id": "INTEGER", "name": "VARCHAR(100)"},
                create_table=False, write_mode="insert", error_policy="quarantine",
                connection=conn, close_connection=False, pg_overwrite_in_transaction=True,
            )
        assert seen["salvaged_uncommitted"] >= 1, seen
        # Still inside the held overwrite: chunk 1 is there, the salvaged
        # chunk-2 rows are gone, the old rows were truncated, and the
        # transaction is not aborted (a full rollback would have dropped
        # chunk 1 and the TRUNCATE too).
        with conn.cursor() as cur:
            cur.execute(f'SELECT id FROM "{table}" ORDER BY id')  # nosec B608
            assert [r[0] for r in cur.fetchall()] == list(range(1, 11))
        finish_held_overwrite(conn, success=False)
        rows = run_sql("postgresql", f'SELECT id, name FROM "{table}" ORDER BY id')  # nosec B608
        assert [tuple(r) for r in rows] == [(100, "old-a"), (101, "old-b")]
    finally:
        set_cancel_probe(None)
        try:
            conn.rollback()
        finally:
            conn.close()
        drop("postgresql", table)
