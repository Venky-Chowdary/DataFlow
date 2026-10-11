"""Poll reconnects retry from the same CDC cursor."""

from __future__ import annotations

from contextlib import ExitStack
import logging
from unittest.mock import MagicMock, patch

import pytest

from connectors.oracle_logminer import (
    OracleLogMinerCdc,
    OracleLogMinerReadError,
    _is_oracle_connection_lost,
)
from connectors.sqlserver_cdc_native import (
    SqlServerCdcReadError,
    SqlServerNativeCdc,
)
from connectors.sqlserver_change_stream import (
    SqlServerChangeTrackingCdc,
    encode_sqlserver_resume_token,
)
CFG = {
    "host": "localhost",
    "database": "dataflow",
    "username": "sa",
    "password": "not-used-by-fakes",
}
RESUME_LSN = "00000000000000000010"


def _connection(cursor: MagicMock) -> MagicMock:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn


def _native_reader() -> SqlServerNativeCdc:
    reader = SqlServerNativeCdc(
        CFG,
        table="orders",
        primary_key="id",
        schema="dbo",
    )
    reader.phase = "streaming"
    reader.start_lsn = RESUME_LSN
    reader.capture_instance = "dbo_orders"
    reader._capture_resolved = True
    reader._resume_inclusive = False
    return reader


def test_native_poll_retries_transient_connection_loss_without_advancing() -> None:
    reader = _native_reader()
    cursor = MagicMock()
    conn = _connection(cursor)
    with ExitStack() as stack:
        connect = stack.enter_context(
            patch.object(
                reader,
                "_conn",
                side_effect=[ConnectionError("connection reset"), conn],
            )
        )
        stack.enter_context(patch.object(reader, "_acquire_cdc_lease"))
        stack.enter_context(
            patch.object(reader, "_resolve_capture_instance", return_value="dbo_orders")
        )
        stack.enter_context(
            patch.object(reader, "_capture_instance_exists", return_value=True)
        )
        stack.enter_context(patch.object(reader, "_maybe_record_capture_schema"))
        stack.enter_context(patch.object(reader, "_min_lsn", return_value=RESUME_LSN))
        stack.enter_context(patch.object(reader, "_max_lsn", return_value=RESUME_LSN))
        stack.enter_context(
            patch(
                "connectors.sqlserver_cdc_native.reconnect_backoff_seconds",
                return_value=0,
            )
        )
        batches = list(reader.poll())

    assert connect.call_count == 2
    assert len(batches) == 1
    assert not batches[0].inserts
    assert reader.start_lsn == RESUME_LSN


def test_native_shared_poll_retries_without_advancing() -> None:
    tables = ["orders", "customers"]
    reader = SqlServerNativeCdc(
        CFG,
        table=tables,
        primary_key="id",
        primary_keys={table: "id" for table in tables},
        schema="dbo",
    )
    reader.phase = "streaming"
    reader.start_lsn = RESUME_LSN
    reader._resume_inclusive = False
    conn = _connection(MagicMock())

    def resolve_captures(_cursor: MagicMock) -> None:
        reader._captures = {
            "orders": "dbo_orders",
            "customers": "dbo_customers",
        }

    with ExitStack() as stack:
        connect = stack.enter_context(
            patch.object(
                reader,
                "_conn",
                side_effect=[ConnectionError("connection reset"), conn],
            )
        )
        stack.enter_context(patch.object(reader, "_acquire_cdc_lease"))
        stack.enter_context(
            patch.object(reader, "_resolve_all_captures", side_effect=resolve_captures)
        )
        stack.enter_context(
            patch.object(reader, "_capture_instance_exists", return_value=True)
        )
        stack.enter_context(patch.object(reader, "_min_lsn_for", return_value=RESUME_LSN))
        stack.enter_context(patch.object(reader, "_max_lsn", return_value=RESUME_LSN))
        stack.enter_context(
            patch(
                "connectors.sqlserver_cdc_native.reconnect_backoff_seconds",
                return_value=0,
            )
        )
        batches = list(reader.poll())

    assert connect.call_count == 2
    assert len(batches) == 1
    assert reader.start_lsn == RESUME_LSN


def test_change_tracking_poll_retries_transient_connection_loss() -> None:
    reader = SqlServerChangeTrackingCdc(
        CFG,
        table="orders",
        primary_key="id",
        schema="dbo",
        resume_token=encode_sqlserver_resume_token(
            10, table="orders", phase="streaming"
        ),
    )
    reader.phase = "streaming"
    reader.version = 10
    conn = _connection(MagicMock())
    with (
        patch.object(
            reader,
            "_conn",
            side_effect=[ConnectionError("connection reset"), conn],
        ) as connect,
        patch.object(reader, "_acquire_cdc_lease"),
        patch.object(reader, "_assert_version_within_retention"),
        patch(
            "connectors.sqlserver_cdc_native.reconnect_backoff_seconds",
            return_value=0,
        ),
    ):
        batches = list(reader.poll())

    assert connect.call_count == 2
    assert len(batches) == 1
    assert reader.version == 10


def test_oracle_poll_retries_transient_connection_loss(
    monkeypatch,
) -> None:
    monkeypatch.delenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", raising=False)
    reader = OracleLogMinerCdc(
        CFG,
        table="orders",
        primary_key="id",
        schema="dbo",
        resume_token='{"kind":"oracle-logminer","table":"orders","phase":"streaming","scn":100}',
    )
    reader.phase = "streaming"
    reader.scn = 100
    cursor = MagicMock()
    cursor.fetchone.return_value = (100,)
    conn = _connection(cursor)
    with ExitStack() as stack:
        connect = stack.enter_context(
            patch.object(
                reader,
                "_mining_conn",
                side_effect=[ConnectionError("connection reset"), conn],
            )
        )
        stack.enter_context(patch.object(reader, "_acquire_cdc_lease"))
        stack.enter_context(
            patch(
                "services.cdc_incremental_runner.interleave_incremental_snapshot",
                return_value=iter(()),
            )
        )
        stack.enter_context(
            patch("connectors.oracle_logminer.fetch_oldest_available_scn", return_value=100)
        )
        stack.enter_context(
            patch("connectors.oracle_logminer.fetch_redo_inventory", return_value=[])
        )
        stack.enter_context(
            patch("connectors.oracle_logminer.assert_resume_scn_in_redo")
        )
        stack.enter_context(
            patch("connectors.oracle_logminer.assert_redo_continuity")
        )
        stack.enter_context(
            patch("connectors.oracle_logminer.start_logminer_session")
        )
        stack.enter_context(
            patch.object(reader, "_fetch_logminer_rows_visible", return_value=([], 100))
        )
        batches = list(reader.poll())

    assert connect.call_count == 2
    assert len(batches) == 1
    assert reader.scn == 100


def test_oracle_shared_poll_retries_transient_connection_loss(
    monkeypatch,
) -> None:
    monkeypatch.delenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", raising=False)
    tables = ["orders", "customers"]
    reader = OracleLogMinerCdc(
        CFG,
        table=tables,
        primary_key="id",
        primary_keys={table: "id" for table in tables},
        schema="dbo",
    )
    reader.phase = "streaming"
    reader.scn = 100
    cursor = MagicMock()
    cursor.fetchone.return_value = (100,)
    conn = _connection(cursor)
    with ExitStack() as stack:
        connect = stack.enter_context(
            patch.object(
                reader,
                "_mining_conn",
                side_effect=[ConnectionError("connection reset"), conn],
            )
        )
        stack.enter_context(patch.object(reader, "_acquire_cdc_lease"))
        stack.enter_context(
            patch("connectors.oracle_logminer.fetch_oldest_available_scn", return_value=100)
        )
        stack.enter_context(
            patch("connectors.oracle_logminer.fetch_redo_inventory", return_value=[])
        )
        stack.enter_context(
            patch("connectors.oracle_logminer.assert_resume_scn_in_redo")
        )
        stack.enter_context(
            patch("connectors.oracle_logminer.assert_redo_continuity")
        )
        stack.enter_context(
            patch("connectors.oracle_logminer.start_logminer_session")
        )
        stack.enter_context(
            patch(
                "connectors.oracle_logminer.reconnect_backoff_seconds",
                return_value=0,
            )
        )
        batches = list(reader.poll())

    assert connect.call_count == 2
    assert len(batches) == 1
    assert reader.scn == 100


def test_oracle_transaction_buffer_poll_retries_before_buffering_rows(
    monkeypatch,
) -> None:
    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1")
    reader = OracleLogMinerCdc(
        CFG,
        table="orders",
        primary_key="id",
        schema="dbo",
        resume_token='{"kind":"oracle-logminer","table":"orders","phase":"streaming","scn":100}',
    )
    reader.phase = "streaming"
    reader.scn = 100
    cursor = MagicMock()
    cursor.fetchone.return_value = (100,)
    conn = _connection(cursor)
    with ExitStack() as stack:
        connect = stack.enter_context(
            patch.object(
                reader,
                "_mining_conn",
                side_effect=[ConnectionError("connection reset"), conn],
            )
        )
        stack.enter_context(patch.object(reader, "_acquire_cdc_lease"))
        stack.enter_context(
            patch("connectors.oracle_logminer.fetch_oldest_available_scn", return_value=100)
        )
        stack.enter_context(
            patch("connectors.oracle_logminer.fetch_redo_inventory", return_value=[])
        )
        stack.enter_context(
            patch("connectors.oracle_logminer.assert_resume_scn_in_redo")
        )
        stack.enter_context(
            patch("connectors.oracle_logminer.assert_redo_continuity")
        )
        stack.enter_context(
            patch("connectors.oracle_logminer.start_logminer_session")
        )
        stack.enter_context(
            patch(
                "connectors.oracle_logminer.reconnect_backoff_seconds",
                return_value=0,
            )
        )
        batches = list(reader.poll())

    assert connect.call_count == 2
    assert len(batches) == 1
    assert reader.scn == 100


def test_native_poll_exhaustion_raises_transient_error_and_logs_attempts(
    monkeypatch,
    caplog,
) -> None:
    reader = _native_reader()
    attempts = 0

    def connection_lost():
        nonlocal attempts
        attempts += 1
        raise ConnectionError("connection reset")

    with (
        patch.object(reader, "_conn", side_effect=connection_lost),
        patch.object(reader, "_acquire_cdc_lease"),
        patch(
            "connectors.oracle_logminer.reconnect_backoff_seconds",
            return_value=0,
        ),
        caplog.at_level(logging.WARNING),
    ):
        with pytest.raises(SqlServerCdcReadError) as exc:
            list(reader.poll())

    assert exc.value.transient is True
    assert attempts > 1
    assert "attempt=1" in caplog.text


def test_native_poll_does_not_retry_nontransient_read_error() -> None:
    reader = _native_reader()
    attempts = 0

    def invalid_query():
        nonlocal attempts
        attempts += 1
        raise ValueError("invalid query")

    with (
        patch.object(reader, "_conn", side_effect=invalid_query),
        patch.object(reader, "_acquire_cdc_lease"),
    ):
        with pytest.raises(SqlServerCdcReadError) as exc:
            list(reader.poll())

    assert exc.value.transient is False
    assert attempts == 1


def test_oracle_poll_exhaustion_raises_transient_read_error(
    monkeypatch, caplog
) -> None:
    monkeypatch.delenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", raising=False)
    reader = OracleLogMinerCdc(
        CFG,
        table="orders",
        primary_key="id",
        schema="dbo",
        resume_token='{"kind":"oracle-logminer","table":"orders","phase":"streaming","scn":100}',
    )
    reader.phase = "streaming"
    reader.scn = 100
    attempts = 0

    def connection_lost():
        nonlocal attempts
        attempts += 1
        raise ConnectionError("connection reset")

    with (
        patch.object(reader, "_mining_conn", side_effect=connection_lost),
        patch.object(reader, "_acquire_cdc_lease"),
        patch(
            "services.cdc_incremental_runner.interleave_incremental_snapshot",
            return_value=iter(()),
        ),
        patch(
            "connectors.oracle_logminer.reconnect_backoff_seconds",
            return_value=0,
        ),
        caplog.at_level(logging.WARNING),
    ):
        with pytest.raises(OracleLogMinerReadError) as exc:
            list(reader.poll())

    assert exc.value.transient is True
    assert CFG["password"] not in caplog.text


def test_oracle_poll_does_not_retry_after_yielding_a_batch() -> None:
    from services.cdc_engine import ChangeBatch

    reader = OracleLogMinerCdc(
        CFG,
        table="orders",
        primary_key="id",
        schema="dbo",
        resume_token='{"kind":"oracle-logminer","table":"orders","phase":"streaming","scn":100}',
    )
    reader.phase = "streaming"
    reader.scn = 100
    attempts = 0
    batch = ChangeBatch(inserts=[{"ID": "1"}], resume_token="token")

    def yield_then_disconnect():
        nonlocal attempts
        attempts += 1
        yield batch
        raise ConnectionError("connection reset")

    with patch.object(reader, "_poll_once", side_effect=yield_then_disconnect):
        poll = reader.poll()
        assert next(poll) is batch
        with pytest.raises(OracleLogMinerReadError) as exc:
            next(poll)

    assert exc.value.transient is True
    assert attempts == 1


def test_oracle_poll_does_not_retry_nontransient_read_error(monkeypatch) -> None:
    monkeypatch.delenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", raising=False)
    reader = OracleLogMinerCdc(
        CFG,
        table="orders",
        primary_key="id",
        schema="dbo",
        resume_token='{"kind":"oracle-logminer","table":"orders","phase":"streaming","scn":100}',
    )
    reader.phase = "streaming"
    reader.scn = 100
    attempts = 0

    def invalid_query():
        nonlocal attempts
        attempts += 1
        raise ValueError("invalid query")

    with (
        patch.object(reader, "_mining_conn", side_effect=invalid_query),
        patch.object(reader, "_acquire_cdc_lease"),
        patch(
            "services.cdc_incremental_runner.interleave_incremental_snapshot",
            return_value=iter(()),
        ),
    ):
        with pytest.raises(OracleLogMinerReadError) as exc:
            list(reader.poll())

    assert exc.value.transient is False
    assert attempts == 1


def test_oracle_closed_pdb_startup_error_is_connection_lost() -> None:
    from connectors.oracle_logminer import _is_oracle_connection_lost

    assert _is_oracle_connection_lost(
        RuntimeError('ORA-16331: container "FREEPDB1" is not open')
    )


def test_sqlserver_poll_does_not_retry_after_yielding_a_batch() -> None:
    from connectors.sqlserver_cdc_native import _retry_sqlserver_poll
    from services.cdc_engine import ChangeBatch

    attempts = 0
    batch = ChangeBatch(inserts=[{"ID": "1"}], resume_token="token")

    def yield_then_disconnect():
        nonlocal attempts
        attempts += 1
        yield batch
        raise ConnectionError("connection reset")

    with patch(
        "connectors.sqlserver_cdc_native.reconnect_backoff_seconds",
        return_value=0,
    ):
        poll = _retry_sqlserver_poll(
            yield_then_disconnect,
            component="SQL Server CDC",
            cursor_key="test",
            table="dbo.orders",
        )
        assert next(poll) is batch
        with pytest.raises(SqlServerCdcReadError) as exc:
            next(poll)

    assert exc.value.transient is True
    assert attempts == 1


@pytest.mark.parametrize("end_scn", [100, 101])
def test_oracle_nonempty_window_keeps_last_composite_cursor(end_scn: int) -> None:
    reader = OracleLogMinerCdc(
        CFG,
        table="orders",
        primary_key="id",
        schema="dbo",
    )
    reader.scn = 100
    reader.rs_id = "0x000017.0000b4a2.0010"
    reader.ssn = 7

    reader._advance_offset(
        last_scn=100,
        last_rs_id=reader.rs_id,
        last_ssn=reader.ssn,
        end_scn=end_scn,
        fetched=1,
        limit=2,
    )

    assert (reader.scn, reader.rs_id, reader.ssn) == (
        100,
        "0x000017.0000b4a2.0010",
        7,
    )


@pytest.mark.parametrize("tables", [("orders",), ("orders", "customers")])
def test_snapshot_handoff_stays_before_same_scn_redo(tables: tuple[str, ...]) -> None:
    from connectors.oracle_logminer import decode_logminer_token
    from services.cdc_exactly_once import batch_lsn

    reader = OracleLogMinerCdc(
        CFG,
        table=list(tables) if len(tables) > 1 else tables[0],
        primary_key="id" if len(tables) == 1 else "",
        primary_keys={table: "id" for table in tables} if len(tables) > 1 else None,
        schema="dbo",
    )
    cursor = MagicMock()
    cursor.fetchone.return_value = (12,)
    conn = _connection(cursor)
    with (
        patch.object(reader, "_conn", return_value=conn),
        patch.object(reader, "_acquire_cdc_lease"),
        patch.object(reader, "_iter_snapshot_table", return_value=iter(())),
    ):
        batches = list(reader.snapshot())

    token = decode_logminer_token(batches[-1].resume_token)
    assert token["scn"] == 11
    assert token["rs_id"] == ""
    assert token["ssn"] == 0
    assert batch_lsn(batches[-1].resume_token) == "scn:11.c"


@pytest.mark.parametrize(
    "message",
    [
        "DPY-4011: the database or network closed the connection",
        "ORA-01033: initialization or shutdown in progress",
        "ORA-01034: ORACLE not available",
        "ORA-03113: end-of-file on communication channel",
        "ORA-03114: not connected to ORACLE",
        "ORA-03135: connection lost contact",
        "ORA-01109: database not open",
    ],
)
def test_oracle_driver_disconnect_codes_are_connection_losses(message: str) -> None:
    assert _is_oracle_connection_lost(message)
