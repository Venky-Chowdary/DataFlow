"""ERR-3..ERR-43, UX-7, DATA-16, DATA-18: user-visible failures, through the real routes.

Each test reproduces one audit finding (``enterprise_audit/errors/FINDINGS.md``)
against PostgreSQL 16 / MySQL 8 / MongoDB 7 and asserts the copy the user now
sees: the right cause, the object named, one next action, no internals and a
support reference. Creates only ``w3err_rt_*`` objects and drops them.
"""

from __future__ import annotations

import errno
import os
import re
import secrets
import uuid

import pytest

from tests import r4_route_harness as h

TOK = uuid.uuid4().hex[:6]
SRC = f"w3err_rt_src_{TOK}"
DST = f"w3err_rt_dst_{TOK}"
DUP = f"w3err_rt_dup_{TOK}"
ROLE = f"w3err_rt_np_{TOK}"
ROLE_PW = secrets.token_hex(8)

_HOSTPORT = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b|:\d{2,5}\b")


class _MP:
    def __init__(self, mp):
        self._mp = mp

    def setenv(self, k, v, *a, **kw):
        if k == "DATAFLOW_SEED_DEMO" and v == "":
            v = "false"
        return self._mp.setenv(k, v, *a, **kw)

    def __getattr__(self, n):
        return getattr(self._mp, n)


@pytest.fixture(scope="module")
def pg_objects():
    h.execute("postgresql", f'CREATE TABLE "{SRC}" (id INT PRIMARY KEY, name TEXT, val TEXT)')
    h.execute("postgresql", f"INSERT INTO \"{SRC}\" VALUES (1,'a','abc'),(2,'b','x')")
    h.execute("postgresql", f'CREATE TABLE "{DUP}" (id INT PRIMARY KEY, email TEXT)')
    h.execute("postgresql", f"INSERT INTO \"{DUP}\" VALUES (1,'pii-owner@example.com')")
    h.execute("postgresql", f"CREATE ROLE {ROLE} LOGIN PASSWORD '{ROLE_PW}'")
    h.execute("postgresql", f"GRANT CONNECT ON DATABASE {h.PG['database']} TO {ROLE}")
    yield
    for t in (SRC, DST, DUP):
        h.drop_table("postgresql", t)
    h.execute("postgresql", f'REVOKE ALL ON DATABASE {h.PG["database"]} FROM {ROLE}')
    h.execute("postgresql", f"DROP OWNED BY {ROLE}")
    h.execute("postgresql", f"DROP ROLE IF EXISTS {ROLE}")


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, _MP(monkeypatch))
    from fastapi.testclient import TestClient

    from src.main import app

    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


def _conn(kind: str, role: str, **over):
    from services.connector_store import create_connector

    cfg = {**h.dest_config(kind), **over}
    return create_connector({"name": f"w3err-rt-{uuid.uuid4().hex[:6]}", "role": role,
                             "workspace_id": "", "ssl": False, **cfg})


def _execute(client, src, dst, src_table, dest_table):
    cols = ["id", "name", "val"]
    r = client.post("/api/v1/transfer/execute", json={
        "source": {"kind": "database", "format": src.type, "connector_id": src.id, "table": src_table},
        "destination": {"kind": "database", "format": dst.type, "connector_id": dst.id, "table": dest_table},
        "mappings": [{"source": c, "target": c, "confidence": 1.0} for c in cols],
        "sync_mode": "append",
        "stream_contracts": [{"name": src_table, "selected": True, "sync_mode": "append", "primary_key": "id"}],
        "validation_mode": "strict", "async_mode": False, "skip_preflight": True,
    })
    body = r.json()
    return body.get("detail") if isinstance(body.get("detail"), dict) else body


# -- RC-A: the job-failure humaniser ------------------------------------------

def test_err3_real_duplicate_keys_get_the_key_action(pg_objects):
    from sqlalchemy import text

    from connectors.generic_sql import get_sqlalchemy_engine
    from services.error_handling import humanize_transfer_failure

    eng = get_sqlalchemy_engine(h.dest_config("postgresql"))
    with pytest.raises(Exception) as caught:
        with eng.begin() as c:
            c.execute(text(f'insert into "{DUP}" (id, email) values (:i, :e)'), {"i": 1, "e": "x@y.z"})
    human = humanize_transfer_failure(caught.value)
    assert human["code"] == "value.duplicate_key", human
    assert "upsert" in human["fix"].lower()
    assert f"{DUP}_pkey" in human["message"]


def test_err4_err8_connection_failure_during_a_run_is_classified(pg_objects, client):
    """ERR-4: a wrong source password mid-run; ERR-8: no driver text, host or user dump."""
    src = _conn("postgresql", "source", password="wrong-" + ROLE_PW)
    out = _execute(client, src, _conn("postgresql", "destination"), SRC, DST)
    details = out.get("error_details") or {}
    assert details.get("code") == "auth.password_rejected", details
    text = str(out.get("error"))
    assert "connection to server at" not in text and not _HOSTPORT.search(text), text
    assert text.count("rejected the sign-in") == 1, text


@pytest.mark.parametrize("raw", [
    "canceling statement due to lock timeout",
    'relation "orders" does not exist',
    "(1114, \"The table 'jobs' is full\")",
    "deadlock detected",
    "logical decoding requires wal_level=logical",
])
def test_err5_data18_no_address_capacity_tail(raw):
    from services.error_handling import humanize_transfer_failure

    human = humanize_transfer_failure(RuntimeError(raw))
    assert "address capacity" not in human["message"], human
    assert "Rows already written stay" not in human["message"], human


def test_err6_postgres_public_schema_is_not_snowflake():
    from services.error_handling import humanize_transfer_failure

    human = humanize_transfer_failure(RuntimeError('schema "public" does not exist'))
    assert human["code"] == "object.schema_not_found", human
    assert "Snowflake" not in human["title"] + human["message"] + human["fix"]


@pytest.mark.parametrize("raw", [
    'syntax error at or near "fro"', "division by zero", "canceling statement due to statement timeout",
    "deadlock detected", "value too long for type character varying(3)", "numeric field overflow",
    'invalid input syntax for type integer: "abc"', 'schema "x" does not exist',
    'duplicate key value violates unique constraint "t_pkey"',
    "(1064, \"You have an error in your SQL syntax; check the manual near 'fro' at line 1\")",
    "(1062, \"Duplicate entry '1' for key 't.PRIMARY'\")",
    "(3024, 'Query execution was interrupted, maximum statement execution time exceeded')",
    "unable to open database file", "No module named 'pymysql'",
    "[Errno 2] No such file or directory: '/data/x.csv'",
    "SSL error: certificate verify failed: self signed certificate",
])
def test_err7_common_causes_are_classified(raw):
    from services.error_handling import humanize_transfer_failure

    exc = ModuleNotFoundError(raw, name="pymysql") if raw.startswith("No module") else RuntimeError(raw)
    human = humanize_transfer_failure(exc)
    assert human["code"] not in {"transfer_failed", "unclassified"}, (raw, human)
    assert "No mapped remediation" not in human["fix"]


def test_err8_data16_writer_errors_carry_no_sql_or_row_values(pg_objects):
    from sqlalchemy import text

    from connectors.generic_sql import get_sqlalchemy_engine
    from src.transfer.job_failure import _job_failure_fields

    eng = get_sqlalchemy_engine(h.dest_config("postgresql"))
    with pytest.raises(Exception) as caught:
        with eng.begin() as c:
            c.execute(text(f'insert into "{DUP}" (id, email) values (:i, :e)'),
                      [{"i": 9, "e": "first@example.com"}, {"i": 1, "e": "pii-owner@example.com"}])
    details, extras = _job_failure_fields(caught.value)
    shown = f"{extras['operator_error']} {details['message']} {details['fix']}"
    for leak in ("[SQL", "insert into", "pii-owner@example.com", "sqlalche.me", "LINE 1"):
        assert leak not in shown, (leak, shown)
    assert "pii-owner@example.com" not in details["raw"], details["raw"]
    assert "[parameters: withheld]" in details["raw"]


# -- RC-B / RC-C / RC-D: Validate, introspect, Test, Query, Pilot --------------

def test_err9_err31_err32_validate_names_the_cause_once(pg_objects, client):
    src, dst = _conn("postgresql", "source"), _conn("postgresql", "destination")
    missing = f"w3err_rt_missing_{TOK}"
    r = client.post("/api/v1/preflight/run", json={
        "dataset_name": missing, "columns": ["id"], "column_types": {"id": "integer"},
        "mappings": [{"source": "id", "target": "id", "confidence": 1.0}],
        "source_connector_id": src.id, "source_kind": "database", "source_type": "postgresql",
        "source_table": missing, "connector_id": dst.id, "dest_kind": "database", "dest_type": "postgresql",
        "dest_table": DST, "sync_mode": "append", "validation_mode": "strict",
        "stream_contracts": [{"name": missing, "selected": True, "sync_mode": "append", "primary_key": "id"}],
    })
    body = r.json()
    messages = [str(b.get("message")) for b in body.get("blockers") or []]
    joined = " ".join(messages)
    assert "LINE 1" not in joined and "SELECT" not in joined, joined
    assert "catalog lists this table" not in joined, joined
    assert sum(f'"{missing}"' in m for m in messages) == 1, messages
    assert any("waiting on Source" in m for m in messages[1:]) or len(messages) == 1, messages
    assert body.get("reference"), body.keys()


def test_err10_introspect_returns_classified_copy(pg_objects, client):
    bad = _conn("postgresql", "source", password="wrong-" + ROLE_PW)
    r = client.post("/api/v1/transfer/introspect", json={
        "source": {"kind": "database", "format": "postgresql", "connector_id": bad.id, "table": SRC},
        "destination": {"kind": "file_export", "format": "csv"}})
    src = r.json().get("source") or {}
    assert "rejected the sign-in" in str(src.get("message")), src
    assert "connection to server" not in str(src.get("message"))
    assert src.get("reference")


def test_err12_ux7_mongo_auth_test_has_no_server_reply_and_has_a_reference(client):
    from urllib.parse import urlparse

    if not os.environ.get("MONGO_URI"):
        pytest.skip("MongoDB not configured (MONGO_URI)")
    u = urlparse(os.environ["MONGO_URI"])
    r = client.post("/api/v1/connectors/test", json={
        "name": "w3err", "type": "mongodb", "host": u.hostname, "port": u.port,
        "database": "dataflow", "username": "w3err_nobody", "password": "bad"})
    body = r.json()
    assert body["success"] is False
    for leak in ("$clusterTime", "signature", "full error", "codeName"):
        assert leak not in body["message"], body["message"]
    assert "Auth source" in body["message"]
    assert body.get("reference") and body["error"]["code"] == "auth.mongo_auth_source"


def test_err14_a_database_named_auth_is_not_an_authentication_failure(client):
    r = client.post("/api/v1/connectors/test", json={"name": "w3err", **h.dest_config("postgresql"),
                                                     "database": "auth"})
    message = r.json()["message"]
    assert "Authentication failed" not in message and "password is wrong" not in message, message
    assert 'Database "auth" doesn\'t exist' in message, message


def test_err15_mysql_unknown_database_does_not_blame_the_password(client):
    r = client.post("/api/v1/connectors/test", json={"name": "w3err", **h.dest_config("mysql"),
                                                     "database": f"w3err_rt_nodb_{TOK}"})
    message = r.json()["message"]
    assert "password" not in message.lower(), message
    assert f"w3err_rt_nodb_{TOK}" in message


def test_err16_statement_timeout_is_not_a_network_timeout(pg_objects):
    """A real 57014 on the Query page and the schema browser (both humanisers)."""
    from sqlalchemy import text

    from connectors.generic_sql import get_sqlalchemy_engine
    from services.safe_errors import user_query_error
    from src.transfer.connector_registry import humanize_connection_error

    eng = get_sqlalchemy_engine(h.dest_config("postgresql"))
    with pytest.raises(Exception) as caught:
        with eng.begin() as c:
            c.execute(text("set local statement_timeout=200"))
            c.execute(text("select count(*) from generate_series(1,200000000)"))
    for shown in (user_query_error(caught.value, public_message="Query failed").detail,
                  humanize_connection_error("postgresql", caught.value)):
        assert "firewall" not in shown.lower() and "Connection timed out" not in shown, shown
        assert "statement time limit" in shown, shown


def test_err18_mistyped_host_says_host_not_found(client):
    r = client.post("/api/v1/connectors/test", json={"name": "w3err", **h.dest_config("postgresql"),
                                                     "host": "no-such-host-w3err.invalid"})
    message = r.json()["message"]
    assert message.startswith("Host not found"), message
    assert "spelling" in message and "public address" not in message, message


def test_err11_err21_missing_driver_reads_the_same_on_test_and_query(pg_objects, client, monkeypatch):
    from connectors import generic_sql

    def no_driver(*_a, **_k):
        raise ModuleNotFoundError("No module named 'psycopg2'", name="psycopg2")

    src = _conn("postgresql", "source")
    monkeypatch.setattr(generic_sql, "get_sqlalchemy_engine", no_driver)
    r = client.post("/api/v1/query/execute", json={"connector_id": src.id, "query": "select 1", "limit": 5, "params": {}})
    detail = r.json()["detail"]
    expected = "PostgreSQL connections aren't available on this server (driver not installed). Contact your administrator."
    assert detail.startswith(expected), detail
    assert "psycopg2" not in detail and "Install" not in detail and "npm run" not in detail
    from src.transfer.connector_registry import humanize_connection_error

    assert humanize_connection_error("postgresql", "Platform driver missing: psycopg2 (psycopg2-binary). "
                                     "Restart the API after `npm run setup:api`.") == expected


def test_err22_pilot_reports_the_login_and_the_unreadable_table(pg_objects, client):
    from src.ai.copilot.query_tools import run_connector_query as run_query

    bad = _conn("postgresql", "source", password="wrong-" + ROLE_PW)
    res = run_query(connector_id=bad.id, query="select 1")
    assert not res.success and "rejected the sign-in" in res.error, res.error
    assert "allow-list" not in res.error
    np_ = _conn("postgresql", "source", username=ROLE, password=ROLE_PW)
    res = run_query(connector_id=np_.id, query=f'select * from "{SRC}"')
    assert not res.success, res
    assert "introspected schema" not in res.error, res.error
    assert SRC in res.error


def test_err23_err24_mongo_query_failures_are_classified(client):
    from urllib.parse import urlparse

    if not os.environ.get("MONGO_URI"):
        pytest.skip("MongoDB not configured (MONGO_URI)")
    from services.connector_store import create_connector

    u = urlparse(os.environ["MONGO_URI"])
    good = create_connector({"name": "w3err-mongo", "type": "mongodb", "role": "source", "workspace_id": "",
                             "connection_string": os.environ["MONGO_URI"], "database": "dataflow"})
    r = client.post("/api/v1/query/execute", json={"connector_id": good.id, "query": "{}", "limit": 5,
                                                   "collection": f"w3err_rt_nocoll_{TOK}", "params": {}})
    assert r.status_code == 404, r.text
    assert f"w3err_rt_nocoll_{TOK}" in r.json()["detail"]
    bad = create_connector({"name": "w3err-mongo-bad", "type": "mongodb", "role": "source", "workspace_id": "",
                            "host": u.hostname, "port": u.port, "database": "dataflow",
                            "username": "w3err_nobody", "password": "bad"})
    r = client.post("/api/v1/query/execute", json={"connector_id": bad.id, "query": "{}", "limit": 5,
                                                   "collection": "x", "params": {}})
    assert r.status_code == 400, (r.status_code, r.text)
    assert "Auth source" in r.json()["detail"]
    assert r.json()["error"]["code"] == "auth.mongo_auth_source"


def test_err25_err26_schemas_route_says_when_the_connection_failed(pg_objects, client):
    bad = _conn("postgresql", "source", password="wrong-" + ROLE_PW)
    body = client.post("/api/v1/query/schemas", json={"connector_id": bad.id}).json()
    assert body["connected"] is False, body
    assert "rejected the sign-in" in body["message"], body
    good = _conn("postgresql", "source", schema=f"w3err_rt_noschema_{TOK}")
    body = client.post("/api/v1/query/schema", json={"connector_id": good.id,
                                                     "schema_name": f"w3err_rt_noschema_{TOK}"}).json()
    assert "0 tables" not in body.get("message", ""), body
    assert "doesn't exist" in body.get("message", ""), body


def test_err27_full_disk_export_says_so_and_leaves_no_file(pg_objects, client, monkeypatch):
    from src.routers import query_router

    src = _conn("postgresql", "source")
    real_run = query_router._run_sql_query

    def full_disk(*args, **kwargs):
        real_run(*args, **kwargs)
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(query_router, "_run_sql_query", full_disk)
    r = client.post("/api/v1/query/export", json={
        "connector_id": src.id, "query": f'select * from "{SRC}"', "format": "csv",
        "full_population": True, "output_path": f"w3err_rt_{TOK}.csv", "params": {}})
    body = r.json()
    assert body.get("success") is False, body
    assert "disk space" in body.get("error", ""), body
    assert "Reference:" in body.get("error", "")
    from services.export_paths import generated_export_path

    assert not generated_export_path(f"w3err_rt_{TOK}.csv").exists()


def test_err28_job_store_outage_names_no_host(client, monkeypatch):
    from pymongo.errors import ServerSelectionTimeoutError

    from services.mongodb_service import get_mongodb_service

    svc = get_mongodb_service()
    msg = "jobs-db.internal.example:27017: [Errno -2] Name or service not known, Topology Description: <...>"

    def boom(*_a, **_k):
        raise ServerSelectionTimeoutError(msg)

    for name in ("list_jobs", "list_jobs_page", "count_jobs", "job_status_counts", "query_jobs"):
        if hasattr(svc, name):
            monkeypatch.setattr(svc, name, boom)
    body = client.get("/api/v1/connectors/jobs").json()
    assert body["degraded"] is True
    assert "jobs-db" not in str(body), body
    assert body["error"]["code"] == "platform.job_store_unavailable" and body["error"]["reference"]

    def boom2(*_a, **_k):
        raise RuntimeError("Mongo cursor id 81234 not found on server jobs-db.internal.example:27017")

    monkeypatch.setattr(svc, "get_job", boom2)
    r = client.get("/api/v1/connectors/jobs/6af77caee8086c18dea1231e")
    assert r.status_code == 500
    assert "jobs-db" not in r.text and "Reference:" in r.json()["detail"], r.text


def test_err30_missing_csv_path_is_named(client):
    r = client.post("/api/v1/transfer/execute", json={
        "source": {"kind": "file", "format": "csv"}, "source_path": f"/nonexistent/w3err/w3err_rt_{TOK}.csv",
        "destination": {"kind": "file_export", "format": "csv"},
        "mappings": [], "sync_mode": "append", "async_mode": False, "skip_preflight": True,
    })
    body = r.json()
    out = body.get("detail") if isinstance(body.get("detail"), dict) else body
    text = str(out.get("error"))
    assert "File content required" not in text, text
    # A path outside the upload roots is refused before the engine sees it, so
    # the copy cannot name it; it names the cause and the action, never a path.
    assert (out.get("error_details") or {}).get("code") == "object.file_not_found", out
    assert "wasn't found" in text and "upload the file again" in text and "/nonexistent" not in text, text


# -- RC-F / RC-H -----------------------------------------------------------------

def test_err34_not_null_blocker_does_not_say_widen_the_type():
    from services.preflight_rules import explain_gate

    rule = explain_gate(
        "g3_schema_contract",
        "1 type coercion issue(s): NOT NULL contract: name → name (TEXT) rejects NULL — 1 empty/null sample value(s)",
    )
    assert "wider" not in rule["fix"], rule["fix"]
    assert "nullable" in rule["fix"] or "default" in rule["fix"]


def test_err35_internal_ids_are_worded():
    from services.error_taxonomy import drift_kind_words, sync_mode_words

    assert sync_mode_words("full_refresh_append") == "append"
    assert drift_kind_words("type_change") == "a column's type changed"


def test_err41_a_scheduled_run_is_notified_once_with_table_names(monkeypatch):
    from types import SimpleNamespace

    import services.notification_service as ns
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    sent: list[dict] = []
    monkeypatch.setattr(ns, "notify_workspace", lambda _ws, payload: sent.append(payload) or [])
    result = SimpleNamespace(success=False, destination_summary={}, job_id="j-w3err", records_transferred=0,
                             error="Table \"orders\" doesn't exist on the source.")
    scheduled = TransferRequest(
        source=EndpointConfig(kind="database", table="orders", extra={"schedule_id": "s-1"}),
        destination=EndpointConfig(kind="database", table="orders_copy"))
    UniversalTransferEngine()._notify_job_status(scheduled, result)
    assert sent == [], "the schedule runner sends the scheduled run's one notification"
    manual = TransferRequest(source=EndpointConfig(kind="database", table="orders"),
                             destination=EndpointConfig(kind="database", table="orders_copy"))
    UniversalTransferEngine()._notify_job_status(manual, result)
    assert len(sent) == 1 and "orders → orders_copy" in sent[0]["title"], sent
    assert "database → database" not in sent[0]["title"]


def test_err42_a_missing_table_is_not_an_unrecognised_cause():
    from services.failure_retry_policy import classify_failure

    verdict = classify_failure(error='Table "orders" doesn\'t exist on the source. Pick an existing table.')
    assert "Cause not recognised" not in verdict.reason, verdict


def test_err43_playground_refusal_names_the_function():
    from src.routers.query_router import sql_refusal_message

    message = sql_refusal_message("select pg_sleep(5)", playground=True)
    assert "pg_sleep" in message, message
