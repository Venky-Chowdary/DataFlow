"""Round-3 review: real driver and dialect regressions.

pymssql, pymongo, the Oracle SQLAlchemy dialect, sqlite3, and psycopg are the
libraries the product dials. These tests use those libraries. They do not
substitute a hand-written error string for the driver's own exception.
"""

from __future__ import annotations

import sqlite3

import pymssql
import sqlalchemy as sa
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pymongo.uri_parser import parse_uri
from sqlalchemy.dialects.oracle import CLOB
from starlette.exceptions import HTTPException as StarletteHTTPException


def test_structured_detail_stays_json_and_unknown_routes_have_a_reference():
    from src.main import http_exception_reference

    app = FastAPI()
    app.add_exception_handler(StarletteHTTPException, http_exception_reference)

    @app.get("/conflict")
    def _conflict():
        from fastapi import HTTPException

        raise HTTPException(
            status_code=409,
            detail={"code": "plan_version_conflict", "message": "stale", "current_version": 4},
        )

    client = TestClient(app)
    body = client.get("/conflict").json()
    assert body["detail"] == {
        "code": "plan_version_conflict",
        "message": "stale",
        "current_version": 4,
    }
    assert body["error"]["reference"]
    missing = client.get("/no-such-route")
    assert missing.status_code == 404
    assert missing.json()["detail"] == "Not Found"
    assert missing.json()["error"]["reference"]


def test_pymssql_208_is_a_missing_table_and_20009_is_the_network():
    from connectors.generic_sql import _is_not_found_error
    from services.error_taxonomy import sqlserver_is_network_failure
    from src.ai.copilot.transfer_tools import _source_read_error

    missing = pymssql.ProgrammingError(
        208,
        b"Invalid object name 'dbo.r43_ms'.DB-Lib error message 20018, severity 16:\n"
        b"General SQL Server error: Check messages from the SQL Server\n",
    )
    down = pymssql.OperationalError(
        20009,
        b"DB-Lib error message 20009, severity 9:\n"
        b"Unable to connect: Adaptive Server is unavailable or does not exist (host)\n",
    )
    column = pymssql.ProgrammingError(
        207,
        b"Invalid column name 'qty'.DB-Lib error message 20018, severity 16:\n",
    )
    login = pymssql.OperationalError(
        18456,
        b"Login failed for user 'sa'.DB-Lib error message 20018, severity 14:\n",
    )
    assert _is_not_found_error(missing) is True
    assert sqlserver_is_network_failure(str(missing)) is False
    assert "unreachable" not in _source_read_error(
        {"name": "QAR41-MS", "host": "db.example", "port": 1433}, "r43_ms", str(missing)
    )
    assert _is_not_found_error(down) is False
    assert sqlserver_is_network_failure(str(down)) is True
    assert "unreachable" in _source_read_error(
        {"name": "QAR41-MS", "host": "db.example", "port": 1433}, "r43_ms", str(down)
    )
    assert sqlserver_is_network_failure(str(column)) is False
    assert sqlserver_is_network_failure(str(login)) is False


def test_pymongo_parses_a_multi_host_uri_and_an_srv_name():
    from src.ai.copilot.tool_deadline import describe_target, mongo_dial_label

    multi = "mongodb://reader:secret@h1:38403,h2:38403/app"
    # The same parser the driver uses. urlparse().port raises on this URI.
    parsed = parse_uri(multi)
    assert parsed["nodelist"] == [("h1", 38403), ("h2", 38403)]
    label = mongo_dial_label(multi)
    assert label == "h1:38403,h2:38403"
    assert "secret" not in label
    srv = describe_target({
        "type": "mongodb",
        "port": 27017,
        "connection_string": "mongodb+srv://reader:secret@cluster.example.net/app",
    })
    assert "cluster.example.net" in srv
    assert "27017" not in srv
    assert "secret" not in srv


def test_oracle_lob_substr_stays_inside_the_varchar2_byte_limit():
    from services.null_marker_probe import _exact_in

    dialect = sa.dialects.oracle.dialect()
    values = sa.bindparam("m", value=["__df_sql_null__"], expanding=True, literal_execute=True)
    expr = _exact_in(sa.column("NOTE", CLOB()), "oracle", values)
    sql = str(expr.compile(dialect=dialect))
    assert "DBMS_LOB.SUBSTR" in sql.upper() or "dbms_lob.substr" in sql.lower()
    amounts = [arg.value for arg in expr.left.clauses if getattr(arg, "value", None) is not None]
    assert 1000 in amounts
    assert 4000 not in amounts


def test_percent_encoded_and_braced_path_drivers_are_refused():
    from services.connector_store import server_side_dsn_refusal

    encoded = server_side_dsn_refusal("postgresql://db.example/app?serv%69ce=prod")
    assert "service" in encoded
    path = server_side_dsn_refusal("Driver={/tmp/x.so};Server=db;")
    assert "driver" in path
    nested = server_side_dsn_refusal({"extra": {"dsn": "DSN=Prod;UID=sa;"}})
    assert "dsn" in nested
    allowed = server_side_dsn_refusal(
        "Driver={ODBC Driver 18 for SQL Server};Server=db;"
    )
    assert allowed == ""


def test_the_word_nan_is_text_and_float_nan_is_not():
    from services.row_filter import apply_row_filter

    rows = [
        {"id": 1, "qty": float("nan"), "note": "Nan"},
        {"id": 2, "qty": 5, "note": "nan"},
    ]
    kept = apply_row_filter(rows, {"column": "note", "operator": "eq", "value": "Nan"})
    assert [r["id"] for r in kept] == [1]
    numbers = apply_row_filter(rows, {"column": "qty", "operator": "lt", "value": 10})
    assert [r["id"] for r in numbers] == [2]


def test_restore_rolls_back_before_the_alter():
    from connectors.column_widen_restore import _RESTORE_BUCKET, settle_widen_restore

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id INTEGER)")
    conn.execute("BEGIN")
    conn.execute("INSERT INTO t VALUES (1)")
    seen: list[str] = []
    conn.set_trace_callback(seen.append)
    token = _RESTORE_BUCKET.set(["CREATE TABLE restored (id INTEGER)"])
    try:
        settle_widen_restore(conn, rows_written=0, failed=True)
    finally:
        _RESTORE_BUCKET.reset(token)
    script = " ; ".join(seen).upper()
    assert script.index("ROLLBACK") < script.index("CREATE TABLE RESTORED")
    assert conn.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 0


def test_folder_keep_last_orders_by_modified_time_then_name():
    from services.file_pickup import RemoteFile, ordered_files

    files = [
        RemoteFile(name="b.csv", size=1, mtime="2", checksum=""),
        RemoteFile(name="a.csv", size=1, mtime="2", checksum=""),
        RemoteFile(name="c.csv", size=1, mtime="1", checksum=""),
    ]
    assert [item.name for item in ordered_files(files)] == ["c.csv", "a.csv", "b.csv"]


def test_a_stamped_workspace_job_is_in_that_workspaces_list():
    from services.confirmed_transfer import transfer_request_from_ack
    from services.mongodb_service import MemoryMongoDBService

    request = transfer_request_from_ack({
        "workspace_id": "ws-9",
        "source": {"kind": "database", "connector_id": "src", "format": "postgresql"},
        "destination": {"kind": "database", "connector_id": "dst", "format": "postgresql"},
    })
    store = MemoryMongoDBService()
    own = store.create_transfer_job({
        "workspace_id": request.workspace_id,
        "status": "failed",
        "name": "mcp",
    })
    store.create_transfer_job({"workspace_id": "other", "status": "failed", "name": "else"})
    listed = store.list_jobs(workspace_id="ws-9", status="failed")
    assert [job["_id"] for job in listed] == [own]


def test_probe_and_reconcile_share_one_mysql_login(monkeypatch):
    import connectors.mysql_conn as mysql_conn
    from services.endpoint_session import endpoint_session_scope

    opened = {"n": 0}

    class _Conn:
        def ping(self, reconnect=False):
            return None

        def rollback(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr(mysql_conn, "dial_pymysql", lambda *_a, **_k: opened.__setitem__("n", opened["n"] + 1) or _Conn())
    monkeypatch.setattr("services.egress_guard.assert_connector_dial_allowed", lambda *_a, **_k: None)
    monkeypatch.setattr("connectors.write_resilience.apply_mysql_session_guards", lambda *_a, **_k: None)
    monkeypatch.setattr(
        mysql_conn, "resolve_sql_endpoint",
        lambda **_kw: {"host": "db.example", "port": 3306, "username": "u", "password": "p", "database": "app"},
    )
    with endpoint_session_scope(engines=("mysql",)):
        mysql_conn.get_connection(
            host="db.example", port=3306, database="app", username="u", password="p",
            connection_string="", ssl=False, purpose="probe",
        ).close()
        mysql_conn.get_connection(
            host="db.example", port=3306, database="app", username="u", password="p",
            connection_string="", ssl=False, purpose="reconcile",
        ).close()
    assert opened["n"] == 1


def test_elapsed_includes_the_time_before_the_first_read():
    from datetime import datetime, timedelta, timezone

    from services.phase_profile import job_phase_timings

    started = datetime(2026, 11, 16, tzinfo=timezone.utc)
    timings = job_phase_timings({
        "created_at": started.isoformat(),
        "completed_at": (started + timedelta(seconds=133)).isoformat(),
        "destination_summary": {"phase_profile": {"elapsed_seconds": 39, "phases": []}},
    })
    assert timings["elapsed_seconds"] == 133.0


def test_replay_refuses_a_lossy_reversible_job():
    from services.reversibility_schedule import schedule_reversibility_block

    request = {
        "require_reversibility": True,
        "mappings": [{"source": "note", "target": "note", "transform": "mask_pii"}],
        "destination": {"table": "q3_rev_out", "format": "postgresql"},
    }
    assert "q3_rev_out.note" in schedule_reversibility_block(request)


def test_rows_skipped_is_on_get_job():
    from services.job_status import promote_rows_skipped
    from services.mongodb_service import MemoryMongoDBService

    store = MemoryMongoDBService()
    job_id = store.create_transfer_job({"status": "running", "name": "skip"})
    updates = {"destination_summary": {"rows_skipped": 30, "rows_written": 0}}
    promote_rows_skipped(updates, None)
    store.update_job_status(job_id, "completed", **updates)
    job = store.get_job(job_id)
    assert job["rows_skipped"] == 30
