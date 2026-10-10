"""SQL Server CDC polling must surface native read failures."""

from __future__ import annotations

from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pytest

from connectors.sqlserver_cdc_native import SqlServerNativeCdc
from connectors.sqlserver_change_stream import SqlServerChangeTrackingCdc
from connectors.write_resilience import is_connection_lost


CFG = {
    "host": "localhost",
    "database": "dataflow",
    "username": "sa",
    "password": "not-used-by-fakes",
}


def _operational_error_313() -> Exception:
    try:
        import pymssql
    except ImportError:
        class OperationalError(Exception):
            pass

        return OperationalError((313, b"An insufficient number of arguments..."))
    return pymssql.OperationalError((313, b"An insufficient number of arguments..."))


def _connection(cur):
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn


def _native_reader(*, shared: bool = False) -> SqlServerNativeCdc:
    return SqlServerNativeCdc(
        CFG,
        table=["orders", "users"] if shared else "orders",
        primary_key="id",
        primary_keys={"orders": "id", "users": "id"} if shared else None,
        schema="dbo",
        batch_size=2,
    )


def _native_poll(
    reader: SqlServerNativeCdc,
    cur,
    *,
    shared: bool = False,
) -> list:
    conn = _connection(cur)
    reader.phase = "streaming"
    reader.start_lsn = "00000000000000000010"
    reader.capture_instance = "dbo_orders"
    reader._capture_resolved = True

    with ExitStack() as stack:
        stack.enter_context(patch.object(reader, "_conn", return_value=conn))
        stack.enter_context(patch.object(reader, "_acquire_cdc_lease"))
        if shared:
            def resolve_captures(_cur):
                reader._captures = {"orders": "dbo_orders", "users": "dbo_users"}

            stack.enter_context(
                patch.object(reader, "_resolve_all_captures", side_effect=resolve_captures)
            )
            stack.enter_context(patch.object(reader, "_min_lsn_for", return_value=reader.start_lsn))
            stack.enter_context(
                patch.object(reader, "_max_lsn", return_value="00000000000000000011")
            )
            stack.enter_context(
                patch.object(
                    reader,
                    "_changes_tvf_for",
                    side_effect=lambda capture: f"cdc.fn_cdc_get_all_changes_{capture}",
                )
            )
            return list(reader._poll_shared_multi())

        stack.enter_context(
            patch(
                "services.cdc_incremental_runner.interleave_incremental_snapshot",
                return_value=iter(()),
            )
        )
        stack.enter_context(patch.object(reader, "_resolve_capture_instance"))
        stack.enter_context(patch.object(reader, "_maybe_record_capture_schema"))
        stack.enter_context(patch.object(reader, "_min_lsn", return_value=reader.start_lsn))
        stack.enter_context(
            patch.object(reader, "_max_lsn", return_value="00000000000000000011")
        )
        stack.enter_context(
            patch.object(reader, "_changes_tvf", return_value="cdc.fn_cdc_get_all_changes_dbo_orders")
        )
        return list(reader.poll())


def _assert_new_read_error(poll, expected_error: type[Exception]) -> Exception:
    from services.cdc_cursor_gap import CdcLsnGapError

    try:
        batches = list(poll())
    except CdcLsnGapError as exc:
        pytest.fail(f"poll raised CdcLsnGapError instead of a read error: {exc}")
    except expected_error as exc:
        return exc
    pytest.fail(f"poll swallowed the read error and returned {batches!r}")


def test_native_poll_surfaces_cdc_tvf_error_313() -> None:
    from connectors.sqlserver_cdc_native import SqlServerCdcReadError

    cur = MagicMock()
    cur.execute.side_effect = _operational_error_313()
    reader = _native_reader()
    error = _assert_new_read_error(
        lambda: _native_poll(reader, cur),
        SqlServerCdcReadError,
    )
    assert error.error_number == 313
    assert error.capture_instance == "dbo_orders"
    assert error.cursor_key == "mssql-cdc:dataflow:dbo.orders"
    assert error.__cause__ is not None
    assert error.transient is is_connection_lost(error.__cause__)
    assert "CDC capture job" in str(error)
    assert "sp_cdc_help_change_data_capture" in str(error)
    assert "permissions" in str(error)
    assert CFG["password"] not in str(error)


def test_native_poll_emits_heartbeat_for_empty_tvf() -> None:
    cur = MagicMock()
    cur.description = [
        ("__$start_lsn",),
        ("__$seqval",),
        ("__$operation",),
        ("id",),
    ]
    cur.fetchall.return_value = []
    batches = _native_poll(_native_reader(), cur)

    assert len(batches) == 1
    assert batches[0].total_changes == 0
    assert batches[0].resume_token


def test_shared_poll_surfaces_cdc_tvf_error_313() -> None:
    from connectors.sqlserver_cdc_native import SqlServerCdcReadError

    cur = MagicMock()
    cur.execute.side_effect = _operational_error_313()
    reader = _native_reader(shared=True)
    error = _assert_new_read_error(
        lambda: _native_poll(reader, cur, shared=True),
        SqlServerCdcReadError,
    )
    assert error.error_number == 313
    assert error.capture_instance == "dbo_orders"
    assert error.cursor_key == reader.cursor_key


def test_change_tracking_poll_surfaces_query_error_313() -> None:
    from connectors.sqlserver_cdc_native import SqlServerCdcReadError

    reader = SqlServerChangeTrackingCdc(
        CFG,
        table="orders",
        primary_key="id",
        schema="dbo",
        batch_size=2,
    )
    reader.phase = "streaming"
    reader.version = 10
    cur = MagicMock()
    cur.execute.side_effect = _operational_error_313()
    conn = _connection(cur)

    with patch.object(reader, "_conn", return_value=conn), patch.object(
        reader, "_acquire_cdc_lease"
    ), patch.object(reader, "_assert_version_within_retention"):
        error = _assert_new_read_error(lambda: reader.poll(), SqlServerCdcReadError)
    assert error.error_number == 313
    assert error.capture_instance == ""
    assert error.cursor_key == "mssql-ct:dataflow:dbo.orders"
    assert "table 'dbo.orders'" in str(error)
