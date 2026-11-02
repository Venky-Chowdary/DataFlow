from datetime import datetime

import pytest

from cdc_sqlserver_live_helpers import (
    _probe_sqlserver_log_reader,
    wait_for_sqlserver_capture_job_healthy,
    wait_for_sqlserver_capture_job_stopped,
)


class _FakeSqlError(Exception):
    pass


class _FakeCursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, statement, params=None):
        self.connection.calls.append((statement, params))
        if (
            statement == "EXEC sys.sp_cdc_scan"
            and self.connection.scan_error is not None
        ):
            raise self.connection.scan_error

    def fetchone(self):
        if self.connection.fetchone_results:
            return self.connection.fetchone_results.pop(0)
        return None

    def fetchall(self):
        if self.connection.fetchall_results:
            return self.connection.fetchall_results.pop(0)
        return []


class _FakeConnection:
    def __init__(
        self,
        *,
        scan_error=None,
        fetchone_results=None,
        fetchall_results=None,
    ):
        self.scan_error = scan_error
        self.fetchone_results = list(fetchone_results or [])
        self.fetchall_results = list(fetchall_results or [])
        self.calls = []
        self.commits = 0
        self.invalidated = False
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.commits += 1

    def invalidate(self):
        self.invalidated = True

    def close(self):
        self.closed = True


class _FakeReader:
    cfg = {
        "host": "localhost",
        "port": 1433,
        "database": "dataflow",
        "username": "sa",
        "password": "test",
    }

    def _conn(self):
        raise AssertionError("ownership helpers must not use pooled reader._conn()")


def _mock_pymssql_connect(monkeypatch, connections):
    import pymssql

    pending = iter(connections)
    calls = []

    def connect(**kwargs):
        calls.append(kwargs)
        return next(pending)

    monkeypatch.setattr(pymssql, "connect", connect)
    return calls


def _agent_owner_error(session_id=53):
    return _FakeSqlError(
        22903,
        f"Another connection with session ID {session_id} is already running "
        "'sp_replcmds' for Change Data Capture.".encode(),
    )


def test_log_reader_probe_accepts_sql_agent_owner(monkeypatch):
    owner = (53, "SQLAgent - TSQL JobStep (Job 0x123 : Step 2)", "running", 0)
    connection = _FakeConnection(
        scan_error=_agent_owner_error(),
        fetchone_results=[(117,), owner],
    )
    _mock_pymssql_connect(monkeypatch, [connection])
    probe_history = []

    outcome = _probe_sqlserver_log_reader(_FakeReader(), probe_history)

    assert outcome["status"] == "agent_owner"
    assert outcome["probe_spid"] == 117
    assert outcome["owner"] == {
        "session_id": 53,
        "program_name": owner[1],
        "status": "running",
        "open_transaction_count": 0,
    }
    assert len(connection.calls) == 3
    assert connection.calls[0][0] == "SELECT @@SPID"
    assert connection.calls[1][0] == "EXEC sys.sp_cdc_scan"
    assert "FROM sys.dm_exec_sessions" in connection.calls[2][0]
    assert probe_history == [
        {
            "probe_spid": 117,
            "outcome": "agent_owner",
            "owner_spid": 53,
            "owner_program_name": owner[1],
            "error": (
                "22903 Another connection with session ID 53 is already running "
                "'sp_replcmds' for Change Data Capture."
            ),
        }
    ]
    assert connection.closed


def test_log_reader_probe_rejects_non_agent_owner(monkeypatch):
    owner = (53, "pymssql=2.4.4", "sleeping", 1)
    connection = _FakeConnection(
        scan_error=_agent_owner_error(),
        fetchone_results=[(117,), owner],
    )
    _mock_pymssql_connect(monkeypatch, [connection])

    outcome = _probe_sqlserver_log_reader(_FakeReader())

    assert outcome["status"] == "other_owner"
    assert outcome["probe_spid"] == 117
    assert outcome["owner"]["session_id"] == 53
    assert outcome["owner"]["program_name"] == "pymssql=2.4.4"
    assert outcome["owner"]["status"] == "sleeping"
    assert outcome["owner"]["open_transaction_count"] == 1
    assert connection.closed


def test_successful_log_reader_probe_flushes_and_commits(monkeypatch):
    connection = _FakeConnection(fetchone_results=[(117,)])
    _mock_pymssql_connect(monkeypatch, [connection])

    outcome = _probe_sqlserver_log_reader(_FakeReader())

    assert outcome["status"] == "unowned"
    assert [call[0] for call in connection.calls] == [
        "SELECT @@SPID",
        "EXEC sys.sp_cdc_scan",
        "EXEC sys.sp_replflush",
    ]
    assert connection.commits == 1
    assert connection.closed


def test_log_reader_probe_records_non_22903_error(monkeypatch):
    connection = _FakeConnection(
        scan_error=_FakeSqlError(50001, b"scan failed"),
        fetchone_results=[(117,)],
    )
    _mock_pymssql_connect(monkeypatch, [connection])

    outcome = _probe_sqlserver_log_reader(_FakeReader())

    assert outcome["status"] == "scan_error"
    assert outcome["probe_spid"] == 117
    assert outcome["error_type"] == "_FakeSqlError"
    assert outcome["error"] == "50001 scan failed"
    assert [call[0] for call in connection.calls] == [
        "SELECT @@SPID",
        "EXEC sys.sp_cdc_scan",
    ]
    assert connection.closed


def test_log_reader_probe_uses_dedicated_pymssql_connection(monkeypatch):
    connection = _FakeConnection(fetchone_results=[(117,)])
    calls = _mock_pymssql_connect(monkeypatch, [connection])
    reader = _FakeReader()
    reader.cfg = {
        **reader.cfg,
        "connect_timeout": 5,
        "encrypt": "yes",
        "trust_server_certificate": True,
    }

    outcome = _probe_sqlserver_log_reader(reader)

    assert outcome["status"] == "unowned"
    assert calls == [
        {
            "server": "localhost",
            "port": 1433,
            "database": "dataflow",
            "user": "sa",
            "password": "test",
            "autocommit": False,
            "login_timeout": 5,
            "encryption": "require",
        }
    ]
    assert connection.closed


def test_healthy_wait_requires_active_job_and_sql_agent_owner(monkeypatch):
    now = datetime(2026, 10, 12, 3, 0)
    activity = (14003, "cdc.dataflow_capture", None, now, None)
    owner = (53, "SQLAgent - TSQL JobStep (Job 0x123 : Step 2)", "running", 0)
    _mock_pymssql_connect(
        monkeypatch,
        [
            _FakeConnection(fetchone_results=[activity]),
            _FakeConnection(
                scan_error=_agent_owner_error(),
                fetchone_results=[(117,), owner],
            ),
        ],
    )

    wait_for_sqlserver_capture_job_healthy(_FakeReader(), since=now, timeout=0)


def test_stopped_wait_requires_stopped_job_and_successful_released_probe(
    monkeypatch,
):
    now = datetime(2026, 10, 12, 3, 0)
    activity = (14003, "cdc.dataflow_capture", None, now, now)
    server_time_connection = _FakeConnection(fetchone_results=[(now,)])
    activity_connection = _FakeConnection(fetchone_results=[activity])
    probe_connection = _FakeConnection(fetchone_results=[(117,)])
    _mock_pymssql_connect(
        monkeypatch,
        [
            server_time_connection,
            activity_connection,
            probe_connection,
        ],
    )

    wait_for_sqlserver_capture_job_stopped(_FakeReader(), timeout=0)

    assert probe_connection.commits == 1
    assert [call[0] for call in probe_connection.calls] == [
        "SELECT @@SPID",
        "EXEC sys.sp_cdc_scan",
        "EXEC sys.sp_replflush",
    ]
    assert server_time_connection.closed
    assert activity_connection.closed
    assert probe_connection.closed


def test_healthy_timeout_reports_non_agent_owner_and_recent_errors(monkeypatch):
    now = datetime(2026, 10, 12, 3, 0)
    activity = (14003, "cdc.dataflow_capture", None, now, None)
    owner = (53, "pymssql=2.4.4", "sleeping", 1)
    activity_connection = _FakeConnection(fetchone_results=[activity])
    probe_connection = _FakeConnection(
        scan_error=_agent_owner_error(),
        fetchone_results=[(117,), owner],
    )
    recent_errors = [(10142, now, 22903, "pymssql owns sp_replcmds")]
    errors_connection = _FakeConnection(fetchall_results=[recent_errors])
    _mock_pymssql_connect(
        monkeypatch,
        [
            activity_connection,
            probe_connection,
            errors_connection,
        ],
    )

    with pytest.raises(AssertionError) as exc:
        wait_for_sqlserver_capture_job_healthy(_FakeReader(), since=now, timeout=0)

    message = str(exc.value)
    assert "last_probe_outcome=" in message
    assert "'probe_spid': 117" in message
    assert "'owner_spid': 53" in message
    assert "pymssql=2.4.4" in message
    assert "open_transaction_count': 1" in message
    assert "pymssql owns sp_replcmds" in message
    assert errors_connection.calls[0][1] == (now,)
    assert activity_connection.closed
    assert probe_connection.closed
    assert errors_connection.closed
