from __future__ import annotations

import time
import uuid

import pytest

from connectors.sqlserver_cdc_native import SqlServerNativeCdc
from connectors.sqlserver_change_stream import SqlServerChangeTrackingCdc
from services.cdc_cursor_gap import CdcCtGapError, CdcLsnGapError
from test_cdc_sqlserver_ct_integration import (
    CFG as CT_CFG,
    _sqlserver_ct_ready,
)
from test_cdc_sqlserver_native_integration import (
    CFG,
    _enable_cdc_on_table,
    _sqlserver_native_ready,
    _wait_capture,
)


def _assert_poll_fails_closed(
    reader,
    error_type: type[Exception],
    *,
    object_name: str,
    remedy: str,
) -> None:
    try:
        batches = list(reader.poll())
    except Exception as exc:
        if not isinstance(exc, error_type):
            pytest.fail(
                f"poll raised {type(exc).__name__} instead of "
                f"{error_type.__name__}: {exc}"
            )
        message = str(exc)
        assert object_name in message, message
        assert remedy.lower() in message.lower(), message
        return
    pytest.fail(
        f"poll returned {len(batches)} batch(es) instead of failing closed: "
        f"{[(b.table, b.total_changes, b.resume_token) for b in batches]!r}"
    )


def _prepare_native_reader(tables: list[str]) -> tuple[SqlServerNativeCdc, SqlServerNativeCdc]:
    holder = f"m2-capture-{uuid.uuid4().hex[:8]}"
    cfg = {**CFG, "lease_holder_id": holder, "job_id": holder}
    bootstrap = SqlServerNativeCdc(
        cfg,
        table="cdc_native_orders",
        primary_key="id",
        schema="dbo",
    )
    with bootstrap._conn() as conn:
        with conn.cursor() as cur:
            for table in tables:
                _enable_cdc_on_table(cur, table)
        conn.commit()

    reader = SqlServerNativeCdc(
        cfg,
        table=tables if len(tables) > 1 else tables[0],
        primary_key="id",
        primary_keys={table: "id" for table in tables},
        schema="dbo",
        batch_size=20,
        cursor_key=f"m2:capture-removal:{holder}",
    )
    _wait_capture(reader, tables[0])
    if len(tables) > 1:
        deadline = time.monotonic() + 20
        ready = False
        while time.monotonic() < deadline:
            reader.force_cdc_scan()
            with reader._conn() as conn:
                with conn.cursor() as cur:
                    ready = True
                    for table in tables:
                        cur.execute(
                            """
                            SELECT ct.capture_instance
                            FROM cdc.change_tables ct
                            JOIN sys.tables t ON t.object_id = ct.source_object_id
                            JOIN sys.schemas s ON s.schema_id = t.schema_id
                            WHERE t.name = %s AND s.name = N'dbo'
                            """,
                            (table,),
                        )
                        row = cur.fetchone()
                        if not row or not reader._min_lsn_for(cur, str(row[0])):
                            ready = False
                            break
            if ready:
                break
            time.sleep(0.25)
        assert ready, f"CDC capture instances did not become ready for {tables!r}"
    return bootstrap, reader


def _cleanup_native(
    bootstrap: SqlServerNativeCdc,
    reader: SqlServerNativeCdc,
    tables: list[str],
) -> None:
    try:
        reader.close()
    except Exception:
        pass
    with bootstrap._conn() as conn:
        with conn.cursor() as cur:
            for table in tables:
                try:
                    cur.execute(
                        """
                        EXEC sys.sp_cdc_disable_table
                            @source_schema = N'dbo',
                            @source_name = %s,
                            @capture_instance = N'all';
                        """,
                        (table,),
                    )
                except Exception:
                    pass
                cur.execute(f"IF OBJECT_ID(N'dbo.{table}') IS NOT NULL DROP TABLE dbo.[{table}]")
        conn.commit()


@pytest.mark.skipif(
    not _sqlserver_native_ready(),
    reason="SQL Server native CDC is not reachable on localhost:1433",
)
def test_native_cdc_single_table_missing_capture_fails_closed() -> None:
    table = "m2_cdc_single_" + uuid.uuid4().hex[:8]
    bootstrap, reader = _prepare_native_reader([table])
    try:
        list(reader.snapshot())
        assert reader.phase == "streaming"
        assert reader.start_lsn
        capture = reader.capture_instance
        assert capture

        with bootstrap._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    EXEC sys.sp_cdc_disable_table
                        @source_schema = N'dbo',
                        @source_name = %s,
                        @capture_instance = N'all';
                    """,
                    (table,),
                )
            conn.commit()

        _assert_poll_fails_closed(
            reader,
            CdcLsnGapError,
            object_name=capture,
            remedy="re-enable CDC",
        )
    finally:
        _cleanup_native(bootstrap, reader, [table])


@pytest.mark.skipif(
    not _sqlserver_native_ready(),
    reason="SQL Server native CDC is not reachable on localhost:1433",
)
def test_native_cdc_shared_reader_missing_capture_fails_closed() -> None:
    tables = [
        "m2_cdc_shared_a_" + uuid.uuid4().hex[:8],
        "m2_cdc_shared_b_" + uuid.uuid4().hex[:8],
    ]
    bootstrap, reader = _prepare_native_reader(tables)
    try:
        list(reader.snapshot())
        assert reader.phase == "streaming"
        assert reader.start_lsn

        with reader._conn() as conn:
            with conn.cursor() as cur:
                capture = reader._resolve_capture_for_table(cur, tables[1])
        assert capture

        with bootstrap._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    EXEC sys.sp_cdc_disable_table
                        @source_schema = N'dbo',
                        @source_name = %s,
                        @capture_instance = N'all';
                    """,
                    (tables[1],),
                )
            conn.commit()

        _assert_poll_fails_closed(
            reader,
            CdcLsnGapError,
            object_name=capture,
            remedy="re-enable CDC",
        )
    finally:
        _cleanup_native(bootstrap, reader, tables)


@pytest.mark.skipif(
    not _sqlserver_ct_ready(),
    reason="SQL Server Change Tracking is not reachable on localhost:1433",
)
def test_change_tracking_disabled_table_fails_closed() -> None:
    table = "m2_ct_disabled_" + uuid.uuid4().hex[:8]
    holder = f"m2-ct-{uuid.uuid4().hex[:8]}"
    cfg = {**CT_CFG, "lease_holder_id": holder, "job_id": holder}
    bootstrap = SqlServerChangeTrackingCdc(
        cfg, table="cdc_orders", primary_key="id", schema="dbo"
    )
    with bootstrap._conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                IF OBJECT_ID(N'dbo.{table}') IS NOT NULL DROP TABLE dbo.[{table}];
                CREATE TABLE dbo.[{table}] (
                    id INT NOT NULL PRIMARY KEY,
                    amount DECIMAL(12,2) NOT NULL
                );
                ALTER TABLE dbo.[{table}] ENABLE CHANGE_TRACKING
                    WITH (TRACK_COLUMNS_UPDATED = ON);
                INSERT INTO dbo.[{table}] (id, amount) VALUES (1, 10.00);
                """
            )
        conn.commit()

    reader = SqlServerChangeTrackingCdc(
        cfg,
        table=table,
        primary_key="id",
        schema="dbo",
        batch_size=20,
        cursor_key=f"m2:ct-capture-removal:{holder}",
    )
    try:
        list(reader.snapshot())
        assert reader.phase == "streaming"
        assert reader.version > 0

        with bootstrap._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"ALTER TABLE dbo.[{table}] DISABLE CHANGE_TRACKING")
            conn.commit()

        _assert_poll_fails_closed(
            reader,
            CdcCtGapError,
            object_name=table,
            remedy="re-enable change tracking",
        )
    finally:
        try:
            reader.close()
        except Exception:
            pass
        with bootstrap._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"IF OBJECT_ID(N'dbo.{table}') IS NOT NULL DROP TABLE dbo.[{table}]")
            conn.commit()
