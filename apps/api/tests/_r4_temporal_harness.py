"""Shared live-engine harness for the round-4 temporal family tests.

Routes QA drives: MCP ``start_transfer`` -> ``/api/v1/copilot/confirm`` -> job,
on real PostgreSQL / MySQL / SQL Server / MongoDB / SQLite. Each engine is
skipped when it is unreachable on this host.
"""

from __future__ import annotations

import os
import socket
import time
import uuid
from typing import Any

import pytest

PG = {
    "host": os.environ.get("PG_HOST") or os.environ.get("PGHOST") or "127.0.0.1",
    "port": int(os.environ.get("PG_PORT") or os.environ.get("PGPORT") or 5432),
    "user": os.environ.get("PG_USER") or "dataflow",
    "password": os.environ.get("PG_PASSWORD") or "dataflow",
    "dbname": os.environ.get("PG_DB") or "dataflow",
}
MYSQL = {
    "host": os.environ.get("DATAFLOW_TEST_MYSQL_HOST") or "127.0.0.1",
    "port": int(os.environ.get("DATAFLOW_TEST_MYSQL_PORT") or 3306),
    "user": os.environ.get("DATAFLOW_TEST_MYSQL_USER") or "dataflow",
    "password": os.environ.get("DATAFLOW_TEST_MYSQL_PASSWORD") or "dataflow",
    "database": os.environ.get("DATAFLOW_TEST_MYSQL_DB") or "dataflow",
}
MSSQL = {
    "host": os.environ.get("DATAFLOW_TEST_MSSQL_HOST") or "127.0.0.1",
    "port": int(os.environ.get("DATAFLOW_TEST_MSSQL_PORT") or 1433),
    "user": os.environ.get("DATAFLOW_TEST_MSSQL_USER") or "sa",
    "password": os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD") or "",
    "database": "dataflow",
}
MONGO_HOST, MONGO_PORT = "127.0.0.1", 27017


def up(host: str, port: int) -> bool:
    try:
        socket.create_connection((host, port), timeout=1).close()
        return True
    except OSError:
        return False


def tag(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


# ----------------------------------------------------------------- engines


def pg_conn():
    psycopg2 = pytest.importorskip("psycopg2")
    if not up(PG["host"], PG["port"]):
        pytest.skip("PostgreSQL not reachable")
    try:
        conn = psycopg2.connect(**PG)
    except Exception as exc:  # environmental: wrong credentials on this host
        pytest.skip(f"PostgreSQL login failed: {exc}")
    conn.autocommit = True
    return conn


def pg_exec(sql: str, params: Any = None) -> list[tuple]:
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall()) if cur.description else []
    finally:
        conn.close()


def mysql_conn():
    pymysql = pytest.importorskip("pymysql")
    if not up(MYSQL["host"], MYSQL["port"]):
        pytest.skip("MySQL not reachable")
    try:
        return pymysql.connect(autocommit=True, **MYSQL)
    except Exception as exc:
        pytest.skip(f"MySQL login failed: {exc}")


def mysql_exec(sql: str, params: Any = None) -> list[tuple]:
    conn = mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall()) if cur.description else []
    finally:
        conn.close()


def mssql_conn():
    pyodbc = pytest.importorskip("pyodbc")
    if not MSSQL["password"] or not up(MSSQL["host"], MSSQL["port"]):
        pytest.skip("SQL Server not reachable")
    try:
        return pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={MSSQL['host']},{MSSQL['port']};DATABASE={MSSQL['database']};"
            f"UID={MSSQL['user']};PWD={MSSQL['password']};TrustServerCertificate=yes",
            autocommit=True,
        )
    except Exception as exc:
        pytest.skip(f"SQL Server login failed: {exc}")


def _datetimeoffset(raw: bytes):
    """pyodbc has no DATETIMEOFFSET (-155) decode; the documented struct is."""
    import struct
    from datetime import datetime, timedelta, timezone

    y, mo, d, h, mi, sec, ns, oh, om = struct.unpack("<6hI2h", raw)
    return datetime(y, mo, d, h, mi, sec, ns // 1000,
                    timezone(timedelta(hours=oh, minutes=om)))


def mssql_exec(sql: str, *params: Any) -> list[tuple]:
    conn = mssql_conn()
    conn.add_output_converter(-155, _datetimeoffset)
    try:
        cur = conn.cursor()
        cur.execute(sql, *params)
        return [tuple(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


def mongo_db():
    pymongo = pytest.importorskip("pymongo")
    if not up(MONGO_HOST, MONGO_PORT):
        pytest.skip("MongoDB not reachable")
    client = pymongo.MongoClient(
        f"mongodb://{MONGO_HOST}:{MONGO_PORT}", serverSelectionTimeoutMS=3000, tz_aware=True,
    )
    return client, client["dataflow_test"]


# ---------------------------------------------------------------- connectors


def connector(kind: str, role: str, **extra: Any) -> str:
    from services.connector_store import create_connector

    base: dict[str, Any] = {
        "name": tag(f"r4t-{kind}-{role}"), "role": role, "workspace_id": "", "ssl": False,
    }
    if kind == "postgresql":
        base.update(type="postgresql", host=PG["host"], port=PG["port"], database=PG["dbname"],
                    username=PG["user"], password=PG["password"], schema="public")
    elif kind == "mysql":
        base.update(type="mysql", host=MYSQL["host"], port=MYSQL["port"],
                    database=MYSQL["database"], username=MYSQL["user"],
                    password=MYSQL["password"])
    elif kind == "sqlserver":
        base.update(type="sqlserver", host=MSSQL["host"], port=MSSQL["port"],
                    database=MSSQL["database"], username=MSSQL["user"],
                    password=MSSQL["password"], schema="dbo",
                    trust_server_certificate=True, encrypt="yes")
    elif kind == "mongodb":
        base.update(type="mongodb", host=MONGO_HOST, port=MONGO_PORT, database="dataflow_test")
    elif kind == "sqlite":
        base.update(type="sqlite")
    elif kind == "elasticsearch":
        base.update(type="elasticsearch", host="127.0.0.1", port=9200, username="", password="")
    base.update(extra)
    return str(create_connector(base).id)


# ------------------------------------------------------------------ client


@pytest.fixture
def transfer_client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "test-pii-key")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "")
    sqlite_root = tmp_path / "sqlite"
    sqlite_root.mkdir()
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(sqlite_root))
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    monkeypatch.setenv("DATAFLOW_UPLOAD_DIR", str(uploads))
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: False)

    import services.connector_store as connector_store
    import services.mongodb_service as mongo_mod
    import src.ai.copilot.ack_ledger as ack_mod
    import src.ai.copilot.data_analyst as data_analyst
    from fastapi.testclient import TestClient
    from services.mongodb_service import MemoryMongoDBService
    from src.ai.copilot import tools as tools_mod
    from src.main import app

    memory = MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongo_mod, "_mongodb_service", memory)
    monkeypatch.setattr(connector_store, "_backend_choice", None)
    monkeypatch.setattr(ack_mod, "_ledger", None)
    monkeypatch.setattr(tools_mod, "_tools", None)
    from src.ai.training.universal_data_feeder import UniversalDataFeeder

    analyst = data_analyst.CopilotDataAnalyst()
    analyst.feeder = UniversalDataFeeder([str(uploads)])
    monkeypatch.setattr(data_analyst, "_analyst", analyst)

    with TestClient(app) as client:
        client.sqlite_root = sqlite_root  # type: ignore[attr-defined]
        client.uploads = uploads  # type: ignore[attr-defined]
        yield client


def wait_job(job_id: str, timeout_s: float = 180.0) -> dict:
    from services.mongodb_service import get_mongodb_service

    deadline = time.monotonic() + timeout_s
    last: dict = {}
    while time.monotonic() < deadline:
        job = get_mongodb_service().get_job(job_id) or {}
        last = job
        status = str(job.get("status") or "")
        if status in {"failed", "partial", "cancelled"} or status.startswith("completed"):
            return job
        time.sleep(0.25)
    return last


def stage(**kwargs: Any):
    from src.ai.copilot.transfer_tools import start_transfer

    kwargs.setdefault("validation_mode", "strict")
    return start_transfer(**kwargs)


def staged_summary(staged: Any) -> dict:
    out = staged.output or {}
    return {
        "error": staged.error,
        "blockers": ((out.get("preflight") or {}).get("blockers")
                     or ((out.get("plan") or {}).get("preflight") or {}).get("blockers")),
        "mappings": [
            {k: m.get(k) for k in ("source", "source_type", "target_type", "transform")}
            for m in out.get("engine_mappings") or []
        ],
    }


def confirm(client, staged: Any) -> dict:
    assert staged.success, staged_summary(staged)
    response = client.post(
        "/api/v1/copilot/confirm",
        json={"ack_id": staged.output["ack_id"], "reason": "r4-temporal"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body.get("ok") is True, body
    return wait_job(str(body.get("job_id") or ""))


def run(client, **kwargs: Any) -> dict:
    """``start_transfer`` -> ``/copilot/confirm`` -> finished job."""
    job = confirm(client, stage(**kwargs))
    assert str(job.get("status") or "").startswith("completed"), {
        "status": job.get("status"),
        "error": job.get("error") or job.get("message"),
        "headline": job.get("failure_class") or job.get("headline"),
    }
    return job


def engine_mappings(staged: Any) -> list[dict]:
    from src.ai.copilot.ack_ledger import get_ack_ledger

    payload, _why = get_ack_ledger().get_pending_payload(staged.output["ack_id"])
    return list((payload or {}).get("mappings") or [])


# ------------------------------------------------------- Elasticsearch fake
#
# No Elasticsearch runs on this host (and the disk cannot hold its image), so
# the ES leg drives DataFlow's real reader / writer / planner against this
# in-memory transport. It stands in for the HTTP cluster only: documents are
# stored as the JSON the client would send, mappings as declared.


class _FakeIndices:
    def __init__(self, store: dict):
        self._s = store

    def exists(self, index, **_):
        return index in self._s

    def create(self, index, body=None, **kw):
        body = body or {}
        props = ((body.get("mappings") or kw.get("mappings") or {}).get("properties") or {})
        self._s[index] = {"props": dict(props), "docs": {}}
        return {"acknowledged": True}

    def get_mapping(self, index, **_):
        if index not in self._s:
            from elasticsearch import NotFoundError  # noqa: F401

            raise KeyError(index)
        return {index: {"mappings": {"properties": dict(self._s[index]["props"])}}}

    def put_mapping(self, index, body=None, **kw):
        props = ((body or {}).get("properties") or kw.get("properties") or {})
        self._s.setdefault(index, {"props": {}, "docs": {}})["props"].update(props)
        return {"acknowledged": True}

    def refresh(self, index=None, **_):
        return {}

    def delete(self, index, **_):
        self._s.pop(index, None)
        return {"acknowledged": True}

    def get(self, index, **_):
        return {index: {"mappings": {"properties": self._s[index]["props"]}}}

    def get_alias(self, index="*", **_):
        return {name: {"aliases": {}} for name in self._s}


class _FakeSecurity:
    def has_privileges(self, index=None, **_):
        names = [n for spec in (index or []) for n in (spec.get("names") or [])]
        privs = ["create", "create_doc", "create_index", "index", "manage", "write", "read"]
        return {
            "username": "fake", "has_all_requested": True, "cluster": {},
            "index": {n: {p: True for p in privs} for n in names}, "application": {},
        }


class _FakeCluster:
    def health(self, **_):
        return {"status": "green"}


class _FakeCat:
    def __init__(self, store: dict):
        self._s = store

    def indices(self, **_):
        return [{"index": name} for name in self._s]


class FakeElasticsearch:
    store: dict = {}

    def __init__(self, *args, **kwargs):
        self.indices = _FakeIndices(FakeElasticsearch.store)
        self.security = _FakeSecurity()
        self.cluster = _FakeCluster()
        self.cat = _FakeCat(FakeElasticsearch.store)

    def options(self, **_):
        return self

    def ping(self, **_):
        return True

    def info(self, **_):
        return {"version": {"number": "8.15.0"}, "cluster_name": "fake"}

    def count(self, index, **_):
        return {"count": len(self.store.get(index, {}).get("docs", {}))}

    def search(self, index, body=None, **_):
        body = body or {}
        docs = list(self.store.get(index, {}).get("docs", {}).items())
        start = int((body.get("search_after") or [-1])[0]) + 1
        size = int(body.get("size") or 10)
        hits = [
            {"_id": doc_id, "_source": src, "sort": [i]}
            for i, (doc_id, src) in enumerate(docs)
        ][start:start + size]
        return {"hits": {"total": {"value": len(docs)}, "hits": hits}}

    def close(self):
        return None


def fake_bulk(client, actions, raise_on_error=False, **_):
    import json

    from services.value_serializer import json_default

    written = 0
    for action in actions:
        index = action["_index"]
        src = action.get("_source") if "_source" in action else action.get("doc")
        doc = json.loads(json.dumps(src, default=json_default))
        FakeElasticsearch.store.setdefault(index, {"props": {}, "docs": {}})["docs"][
            str(action["_id"])
        ] = doc
        written += 1
    return written, []


@pytest.fixture
def fake_es(monkeypatch):
    elasticsearch = pytest.importorskip("elasticsearch")
    import elasticsearch.helpers as helpers

    FakeElasticsearch.store = {}
    monkeypatch.setattr(elasticsearch, "Elasticsearch", FakeElasticsearch)
    monkeypatch.setattr(helpers, "bulk", fake_bulk)
    return FakeElasticsearch.store


def mcp(client, name: str, arguments: dict) -> tuple[bool, Any]:
    """One MCP ``tools/call``: ``(is_error, payload)``."""
    import json

    response = client.post(
        "/api/v1/mcp",
        headers={"Accept": "application/json, text/event-stream"},
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/call",
              "params": {"name": name, "arguments": arguments}},
    )
    assert response.status_code == 200, response.text
    result = response.json()["result"]
    text = result["content"][0]["text"]
    if result["isError"]:
        return True, text
    return False, json.loads(text)


def connector_name(connector_id: str) -> str:
    from services.connector_store import get_connector

    return str(get_connector(connector_id).name)
