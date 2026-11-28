"""D427: a transient connection loss is reported as a connection problem.

QA round 65: a SQL Server CDC read whose session died (DB-Library 20047,
"DBPROCESS is dead") failed the run with "CDC cursor gap (retention /
failover)": the wrapper text says "CDC/Change Tracking" and a text rule
matched those words. Rounds 68-71: the same drop read as "couldn't identify
the cause ... (20047, Unknown error)", and during a tunnel outage the capture
probe answered "CDC is not enabled", so the run was refused "the server's
change log is off". The next run caught up exactly every time: there was no
gap.

The driver code now decides first (services.connection_lost_codes): a
dropped connection is said as one, with its code, and is retriable; a cursor
gap is still reported when CdcCursorGapError proves it. These tests use the
real pymssql / pymysql exception classes; the probe test dials a real port
where no SQL Server listens.
"""

from __future__ import annotations

import socket

import pytest

pymssql = pytest.importorskip("pymssql")

_DEAD = b"DB-Lib error message 20047, severity 9:\nDBPROCESS is dead or not enabled\n"
_GAP_WORDS = ("retention", "failover", "cursor gap", "re-snapshot", "snapshot_mode")


def _dead_session() -> BaseException:
    return pymssql.exceptions.OperationalError(20047, _DEAD)


def _wrapped_cdc_read_error() -> BaseException:
    from connectors.sqlserver_cdc_native import SqlServerCdcReadError

    try:
        try:
            raise _dead_session()
        except Exception as inner:
            raise SqlServerCdcReadError(
                inner, capture_instance="dbo_r65_cdc_ms_v1", table="dbo.r65_cdc_ms"
            ) from inner
    except SqlServerCdcReadError as outer:
        return outer


def _assert_connection_lost(failure: dict, code: str) -> None:
    assert failure["code"].startswith("network."), failure
    assert failure["retriable"] is True
    text = f"{failure['title']} {failure['message']} {failure.get('fix', '')}".lower()
    assert code.lower() in text
    for word in _GAP_WORDS:
        assert word not in text, (word, failure)
    assert "couldn't identify" not in text


def test_a_dead_sql_server_session_during_a_cdc_read_is_a_lost_connection():
    from services.error_handling import humanize_transfer_failure

    error = _wrapped_cdc_read_error()
    failure = humanize_transfer_failure(error, side="source")
    _assert_connection_lost(failure, "20047")
    assert failure["title"] == "Connection lost"
    # The job record keeps only text on some paths; the code still decides.
    _assert_connection_lost(humanize_transfer_failure(str(error)), "20047")


def test_the_raw_driver_tuple_is_not_an_unidentified_cause():
    from services.error_handling import humanize_transfer_failure

    _assert_connection_lost(humanize_transfer_failure(_dead_session()), "20047")
    _assert_connection_lost(
        humanize_transfer_failure("(20047, b'DB-Lib error message 20047, severity 9:')"), "20047"
    )


def test_mysql_lost_connection_is_a_connection_problem_not_a_binlog_gap():
    import pymysql

    from services.error_handling import humanize_transfer_failure

    error = pymysql.err.OperationalError(2013, "Lost connection to MySQL server during query")
    try:
        raise RuntimeError("MySQL binlog read failed") from error
    except RuntimeError as wrapped:
        failure = humanize_transfer_failure(wrapped, side="source")
    assert failure["code"].startswith("network.") and failure["retriable"] is True
    assert "gap" not in failure["message"].lower()


@pytest.mark.parametrize(
    "error",
    [
        pymssql.exceptions.OperationalError(229, b"The SELECT permission was denied on the object"),
        RuntimeError("relation \"orders\" does not exist"),
        RuntimeError("DPY-6005: cannot connect to database. ORA-12514: listener does not know the service"),
    ],
    ids=["mssql-permission", "missing-table", "oracle-wrong-service"],
)
def test_other_driver_errors_are_not_read_as_a_dropped_connection(error):
    from services.connection_lost_codes import connection_lost_code

    assert connection_lost_code(error) == ""


def test_a_proven_cursor_gap_is_still_a_cursor_gap():
    from services.cdc_cursor_gap import CdcLsnGapError
    from services.error_handling import humanize_transfer_failure

    gap = CdcLsnGapError(
        "resume LSN is before the capture instance minimum",
        resume_lsn="00000010000000100001",
        min_lsn="00000020000000200001",
    )
    failure = humanize_transfer_failure(gap)
    assert "cursor gap" in failure["title"].lower()
    assert failure["retriable"] is False


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.mark.parametrize("reader", ["native", "change_tracking"])
def test_an_unreachable_capture_probe_is_not_cdc_switched_off(reader):
    """The real driver dials a closed port (DB-Library 20009): the probe must
    say the connection failed, not "CDC is not enabled"."""
    from connectors.sqlserver_cdc_native import SqlServerCdcReadError, SqlServerNativeCdc
    from connectors.sqlserver_change_stream import SqlServerChangeTrackingCdc
    from services.error_handling import humanize_transfer_failure

    cfg = {
        "type": "sqlserver", "host": "127.0.0.1", "port": _free_port(), "database": "dataflow",
        "username": "sa", "password": "unused", "job_id": "", "lease_holder_id": "",
        "login_timeout": 2,
    }
    cls = SqlServerNativeCdc if reader == "native" else SqlServerChangeTrackingCdc
    probe = cls(cfg, table="orders", primary_key="id", schema="dbo")
    with pytest.raises(SqlServerCdcReadError) as caught:
        probe.is_available()
    failure = humanize_transfer_failure(caught.value, side="source")
    assert failure["code"].startswith("network.") and failure["retriable"] is True
    assert "not enabled" not in failure["message"] and "change log is off" not in failure["message"]


def test_the_preflight_probe_stays_undecided_when_the_source_is_unreachable():
    from services.cdc_log_capture_probe import probe_log_capture

    cfg = {
        "type": "sqlserver", "host": "127.0.0.1", "port": _free_port(), "database": "dataflow",
        "username": "sa", "password": "unused", "login_timeout": 2,
    }
    probe = probe_log_capture("sqlserver", cfg, table="dbo.orders", primary_key="id")
    assert probe.available is None  # connectivity has its own gate; never "log is off"


@pytest.mark.parametrize(
    "error",
    [
        pymssql.exceptions.OperationalError(
            18456,
            b"Login failed for user 'sa'.DB-Lib error message 20018, severity 14:\n"
            b"General SQL Server error: Check messages from the SQL Server\n"
            b"DB-Lib error message 20002, severity 9:\nAdaptive Server connection failed\n",
        ),
        "(18456, b\"Login failed for user 'sa'.DB-Lib error message 20002, severity 9:\")",
        "SQL Server CDC/Change Tracking read failed for table 'dbo.t' (SQL Server error 4060).",
    ],
    ids=["login-failed", "login-failed-text", "database-missing-wrapper"],
)
def test_the_main_driver_code_decides_not_a_trailing_one(error):
    """A login failure ends with "DB-Lib error message 20002": it is a refused
    login (fix the credentials), not a connection to retry."""
    from services.connection_lost_codes import connection_lost_code

    assert connection_lost_code(error) == ""
