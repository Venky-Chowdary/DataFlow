"""Upsert identity across engines, end to end through QA's route (round 4).

QA drives ``start_transfer`` (plan + Validate) -> ``/api/v1/copilot/confirm`` ->
job, on real engines: PostgreSQL, MySQL 8, MariaDB 11, SQL Server 2022, Oracle
Free, a real Kafka broker, and fakesnow for Snowflake (no real Snowflake here).

The family: "how does this engine enforce upsert identity?" was answered in
three places that disagreed. One answer now lives in the capability registry
(``connectors.merge_registry.upsert_identity``) and ``services.upsert_identity``
turns it into the exact DDL the writer runs (or a plain refusal), read by
Validate's disclosure, the MySQL writer and the root-cause remediation:

* D182 - Snowflake run 2 into the table DataFlow created (JSONB -> VARIANT) in
  overwrite and in upsert, no overrides; MERGE on the key and no index DDL.
* D101 - an upsert into a pre-existing keyless table: PG/MySQL/MariaDB disclose
  the index and the MCP ``ddl_acknowledgement`` approves it; SQL Server and
  Snowflake MERGE with no index and nothing to approve.
* CR-D126 - PG TEXT key -> MySQL/MariaDB: never a prefix index (keys sharing
  191 characters stay distinct); a JSON key is refused plainly at Validate.
* D53 - ``''`` is a key value wherever the destination stores it; Oracle stores
  ``''`` as NULL and Validate says so before any row moves.
* D89 - a Kafka changelog repeating keys lands last-write-wins by offset.
"""

from __future__ import annotations

import json
import os
import socket
import time
import uuid
from decimal import Decimal
from typing import Any

import pytest

psycopg2 = pytest.importorskip("psycopg2")

_PG = {
    "host": os.environ.get("PGHOST", "127.0.0.1"),
    "port": int(os.environ.get("PGPORT", "5432")),
    "database": os.environ.get("PGDATABASE", "dataflow"),
    "username": os.environ.get("PGUSER", "dataflow"),
    "password": os.environ.get("PGPASSWORD", ""),
}
_KAFKA = {"host": "127.0.0.1", "port": 9092}


def _up(port: int, host: str = "127.0.0.1") -> bool:
    try:
        socket.create_connection((host, port), timeout=1).close()
        return True
    except OSError:
        return False


def _pg_conn():
    if not _PG["password"] or not _up(_PG["port"], _PG["host"]):
        pytest.skip("PostgreSQL not configured (PG* env)")
    try:
        conn = psycopg2.connect(
            host=_PG["host"], port=_PG["port"], user=_PG["username"],
            password=_PG["password"], dbname=_PG["database"], connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    return conn


@pytest.fixture(scope="module")
def _fakesnow_path(tmp_path_factory):
    # One fakesnow catalog for the module: the cached Snowflake engine keeps
    # the file it first opened, so a per-test path loses tables between tests.
    return str(tmp_path_factory.mktemp("fakesnow") / "db")


@pytest.fixture
def client(tmp_path, monkeypatch, _fakesnow_path):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "test-pii-key")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "")
    monkeypatch.setenv("FAKESNOW_DB_PATH", _fakesnow_path)
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: False)

    import services.connector_store as connector_store
    import services.mongodb_service as mongo_mod
    import services.sync_cursor as sc
    import src.ai.copilot.ack_ledger as ack_mod
    from fastapi.testclient import TestClient
    from services.mongodb_service import MemoryMongoDBService
    from src.main import app

    memory = MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongo_mod, "_mongodb_service", memory)
    monkeypatch.setattr(connector_store, "_backend_choice", None)
    monkeypatch.setattr(ack_mod, "_ledger", None)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)

    with TestClient(app) as c:
        yield c


# --------------------------------------------------------------------------- #
# Destinations
# --------------------------------------------------------------------------- #


def _cfg(kind: str) -> dict[str, Any]:
    if kind == "postgresql":
        _pg_conn().close()
        return dict(type="postgresql", host=_PG["host"], port=_PG["port"],
                    database=_PG["database"], username=_PG["username"],
                    password=_PG["password"], schema="public")
    if kind == "mysql":
        pw = os.environ.get("MYSQL_PASSWORD", "")
        if not pw or not _up(3306):
            pytest.skip("MySQL not configured (MYSQL_PASSWORD, :3306)")
        return dict(type="mysql", host="127.0.0.1", port=3306,
                    database=os.environ.get("MYSQL_DATABASE", "dataflow"),
                    username=os.environ.get("MYSQL_USER", "dataflow"), password=pw)
    if kind == "mariadb":
        pw = os.environ.get("DATAFLOW_TEST_MARIADB_PASSWORD", "")
        port = int(os.environ.get("DATAFLOW_TEST_MARIADB_PORT", "3307"))
        if not pw or not _up(port):
            pytest.skip("MariaDB not configured (DATAFLOW_TEST_MARIADB_*, :3307)")
        return dict(type="mariadb", host="127.0.0.1", port=port,
                    database=os.environ.get("DATAFLOW_TEST_MARIADB_DB", "dataflow"),
                    username=os.environ.get("DATAFLOW_TEST_MARIADB_USER", "dataflow"),
                    password=pw)
    if kind == "sqlserver":
        pw = os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD", "")
        if not pw or not _up(1433):
            pytest.skip("SQL Server not configured (DATAFLOW_TEST_MSSQL_PASSWORD, :1433)")
        pytest.importorskip("pyodbc")
        return dict(type="sqlserver", host="127.0.0.1", port=1433, database="dataflow",
                    username=os.environ.get("DATAFLOW_TEST_MSSQL_USER", "sa"), password=pw,
                    extra={"trust_server_certificate": True})
    if kind == "snowflake":
        pytest.importorskip("fakesnow")
        return dict(type="snowflake", host="localhost", port=443, database="dataflow",
                    username="test", password="test", schema="public")
    if kind == "oracle":
        pw = os.environ.get("DATAFLOW_TEST_ORACLE_PASSWORD", "")
        if not pw or not _up(1521):
            pytest.skip("Oracle not configured (DATAFLOW_TEST_ORACLE_PASSWORD, :1521)")
        pytest.importorskip("oracledb")
        svc = os.environ.get("DATAFLOW_TEST_ORACLE_SERVICE", "FREEPDB1")
        return dict(type="oracle", host="127.0.0.1", port=1521, database=svc,
                    service_name=svc,
                    username=os.environ.get("DATAFLOW_TEST_ORACLE_USER", "system"),
                    password=pw,
                    schema=os.environ.get("DATAFLOW_TEST_ORACLE_SCHEMA", "SYSTEM"))
    raise AssertionError(kind)


def _connect(kind: str):
    if kind == "postgresql":
        return _pg_conn()
    if kind in {"mysql", "mariadb"}:
        import pymysql

        c = _cfg(kind)
        return pymysql.connect(host=c["host"], port=c["port"], user=c["username"],
                               password=c["password"], database=c["database"],
                               charset="utf8mb4", autocommit=True)
    if kind == "sqlserver":
        import pyodbc

        return pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,1433;DATABASE=dataflow;"
            f"UID={os.environ.get('DATAFLOW_TEST_MSSQL_USER', 'sa')};"
            f"PWD={os.environ['DATAFLOW_TEST_MSSQL_PASSWORD']};TrustServerCertificate=yes",
            timeout=10, autocommit=True,
        )
    if kind == "snowflake":
        import connectors.snowflake_conn as sc

        return sc.get_connection(
            account="localhost", username="test", password="test", database="dataflow",
            schema="public", warehouse="", connection_string="",
        )
    if kind == "oracle":
        import oracledb

        c = _cfg(kind)
        return oracledb.connect(user=c["username"], password=c["password"],
                                dsn=f"127.0.0.1:1521/{c['service_name']}")
    raise AssertionError(kind)


def _q(kind: str, table: str) -> str:
    if kind == "postgresql":
        return f'public."{table}"'
    if kind in {"mysql", "mariadb"}:
        return f"`{table}`"
    if kind == "sqlserver":
        return f"[{table}]"
    if kind == "oracle":
        return f'"{table}"'
    return table


def _exec(kind: str, sql: str) -> None:
    conn = _connect(kind)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        if kind == "oracle":
            conn.commit()
    finally:
        conn.close()


def _select(kind: str, sql: str) -> list[tuple]:
    conn = _connect(kind)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return [tuple(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _drop(kind: str, table: str) -> None:
    try:
        if kind == "oracle":
            _exec(kind, f'BEGIN EXECUTE IMMEDIATE \'DROP TABLE "{table}" PURGE\'; '
                        "EXCEPTION WHEN OTHERS THEN NULL; END;")
        else:
            _exec(kind, f"DROP TABLE IF EXISTS {_q(kind, table)}")  # nosec B608
    except Exception:  # pragma: no cover - best-effort cleanup
        pass


def _unique_index_count(kind: str, table: str) -> int:
    if kind == "postgresql":
        return len(_select(kind, "SELECT 1 FROM pg_indexes WHERE tablename = "
                                 f"'{table}' AND indexdef ILIKE '%UNIQUE%'"))
    if kind in {"mysql", "mariadb"}:
        return len(_select(kind, "SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS "
                                 f"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table}' "
                                 "AND NON_UNIQUE = 0"))
    if kind == "sqlserver":
        return len(_select(kind, "SELECT 1 FROM sys.indexes WHERE is_unique = 1 AND "
                                 f"object_id = OBJECT_ID('dbo.{table}')"))
    return 0


def _decode(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "read"):
        value = value.read()
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        return json.loads(value)
    return value


def _plain(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_plain(v) for v in value]
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    return value


# --------------------------------------------------------------------------- #
# QA's route
# --------------------------------------------------------------------------- #


def _wait_job(job_id: str, timeout_s: float = 180.0) -> dict:
    from services.mongodb_service import get_mongodb_service

    deadline = time.monotonic() + timeout_s
    job: dict = {}
    while time.monotonic() < deadline:
        job = get_mongodb_service().get_job(job_id) or {}
        if str(job.get("status") or "") in {"completed", "failed", "partial", "cancelled"}:
            return job
        time.sleep(0.25)
    return job


def _stage(src_id: str, dst_id: str, table: str, dest_table: str, mode: str, **extra):
    from src.ai.copilot.transfer_tools import start_transfer

    kw: dict[str, Any] = dict(
        source_connector_id=src_id, source_table=table, dest_connector_id=dst_id,
        dest_table=dest_table, sync_mode=mode, validation_mode="strict",
        source_timezone="UTC",
    )
    kw.update(extra)
    staged = start_transfer(**kw)
    plan = (staged.output or {}).get("plan") or staged.output or {}
    preflight = plan.get("preflight") or {}
    blockers = [
        f"{b.get('gate') or b.get('id')}: {b.get('message')}"
        for b in (preflight.get("blockers") or [])
    ]
    return staged, plan, blockers


def _confirm(client, staged) -> dict:
    resp = client.post(
        "/api/v1/copilot/confirm",
        json={"ack_id": staged.output["ack_id"], "reason": "r4-upsert-identity"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("ok") is True, body
    return _wait_job(str(body.get("job_id") or ""))


def _run_ok(client, src_id, dst_id, table, dest_table, mode, **extra) -> dict:
    staged, plan, blockers = _stage(src_id, dst_id, table, dest_table, mode, **extra)
    assert staged.success, {"error": staged.error, "blockers": blockers}
    assert not blockers, blockers
    job = _confirm(client, staged)
    assert str(job.get("status")) == "completed", {
        "status": job.get("status"), "error": job.get("error") or job.get("message"),
    }
    return {"plan": plan, "job": job}


def _keyed(mode: str) -> dict[str, Any]:
    if mode == "full_refresh_overwrite":
        return {}
    return dict(primary_key="id", cursor_column="updated_at",
                cursor_semantics="monotonic_sequence")


def _connectors(token: str, kind: str):
    from services.connector_store import create_connector

    dest_cfg = _cfg(kind)
    src = create_connector(
        {"name": f"r4u-pg-{token}", "type": "postgresql", "role": "source",
         "workspace_id": "", "ssl": False, **_PG, "schema": "public"}
    )
    dst = create_connector(
        {"name": f"r4u-{kind}-{token}", "role": "destination", "workspace_id": "",
         "ssl": False, **dest_cfg}
    )
    return src, dst


def _no_index_ddl_offered(plan: dict) -> None:
    text = json.dumps(plan, default=str).upper()
    assert "CREATE UNIQUE INDEX" not in text, "index DDL offered on a MERGE engine"


# --------------------------------------------------------------------------- #
# D182 (+ family): run 1 creates, tamper, run 2 identical parameters.
# --------------------------------------------------------------------------- #

_DOCS: dict[int, Any] = {
    1: {"a": {"b": [1, 2, {"c": "x"}]}, "n": None},
    2: [1, "two", None],
    3: {"name": "Łukasz 山田 🎉"},
    4: None,
    5: {},
}
_NOTES: dict[int, Any] = {1: "plain", 2: "", 3: "Łukasz 山田", 4: None, 5: "   "}
_TAMPER: dict[int, Any] = {2: {"changed": ["x", {"y": "中文"}]}, 6: {"added": True}}


def _seed_json_source(pg, table: str) -> None:
    cur = pg.cursor()
    cur.execute(
        f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload JSONB, note TEXT, "  # nosec B608
        "amount NUMERIC(12,3), seen_at TIMESTAMPTZ, updated_at BIGINT NOT NULL)"
    )
    for i, doc in _DOCS.items():
        cur.execute(
            f"INSERT INTO {table} VALUES (%s, %s, %s, %s, %s, %s)",  # nosec B608
            (i, None if doc is None else json.dumps(doc), _NOTES.get(i),
             None if i == 4 else Decimal(f"{i}.125"),
             None if i == 4 else f"2024-0{i}-0{i} 1{i}:00:00+05:30", i),
        )


def _tamper(pg, table: str) -> None:
    cur = pg.cursor()
    for i, doc in _TAMPER.items():
        cur.execute(
            f"INSERT INTO {table} (id, payload, note, updated_at) VALUES (%s, %s, %s, %s) "  # nosec B608
            "ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload, "
            "updated_at = EXCLUDED.updated_at",
            (i, json.dumps(doc), f"n{i}", 100 + i),
        )


@pytest.mark.parametrize("mode", ["full_refresh_overwrite", "incremental_upsert"])
@pytest.mark.parametrize("kind", ["postgresql", "mysql", "mariadb", "sqlserver", "snowflake"])
def test_d182_run2_into_own_table_with_json_needs_no_override(client, kind, mode):
    pg = _pg_conn()
    token = uuid.uuid4().hex[:8]
    table = f"r4u_j_{token}"
    dest_table = f"r4u_j_{kind[:4]}_{mode[:4]}_{token}"
    try:
        _seed_json_source(pg, table)
        src, dst = _connectors(token, kind)
        first = _run_ok(client, src.id, dst.id, table, dest_table, mode, **_keyed(mode))
        _tamper(pg, table)
        second = _run_ok(client, src.id, dst.id, table, dest_table, mode, **_keyed(mode))
        if kind == "snowflake":
            _no_index_ddl_offered(first["plan"])
            _no_index_ddl_offered(second["plan"])
        rows = _select(kind, f"SELECT id, payload, note FROM {_q(kind, dest_table)}")  # nosec B608
        got = {int(r[0]): (_plain(_decode(r[1])), r[2]) for r in rows}
        expected = {**_DOCS, **_TAMPER}
        assert {i: v[0] for i, v in got.items()} == expected
        for i, note in _NOTES.items():
            assert got[i][1] == note, (kind, i, got[i][1], note)
    finally:
        pg.cursor().execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.close()
        _drop(kind, dest_table)


# --------------------------------------------------------------------------- #
# D101: upsert into a pre-existing table that has no unique key.
# --------------------------------------------------------------------------- #

_KEYLESS_DDL = {
    "postgresql": "CREATE TABLE {t} (id BIGINT, payload JSONB, note TEXT, updated_at BIGINT)",
    "mysql": "CREATE TABLE {t} (id BIGINT, payload JSON, note TEXT, updated_at BIGINT)",
    "mariadb": "CREATE TABLE {t} (id BIGINT, payload LONGTEXT, note TEXT, updated_at BIGINT)",
    "sqlserver": "CREATE TABLE {t} (id BIGINT, payload NVARCHAR(MAX), note NVARCHAR(MAX), "
                 "updated_at BIGINT)",
    "snowflake": "CREATE TABLE {t} (id BIGINT, payload VARIANT, note VARCHAR, updated_at BIGINT)",
}


def _seed_keyless(pg, table: str) -> None:
    cur = pg.cursor()
    cur.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload JSONB, "  # nosec B608
                "note TEXT, updated_at BIGINT NOT NULL)")
    for i in range(1, 5):
        cur.execute(f"INSERT INTO {table} VALUES (%s, %s, %s, %s)",  # nosec B608
                    (i, json.dumps({"i": i, "u": "山田"}), "" if i == 2 else f"n{i}", i))


def _keyless_rows(kind: str, dest_table: str) -> dict[int, Any]:
    rows = _select(kind, f"SELECT id, note FROM {_q(kind, dest_table)}")  # nosec B608
    return {int(r[0]): r[1] for r in rows}


@pytest.mark.parametrize("kind", ["postgresql", "mysql", "mariadb"])
def test_d101_index_engine_discloses_ddl_and_mcp_approval_lands_it(client, kind):
    pg = _pg_conn()
    token = uuid.uuid4().hex[:8]
    table = f"r4u_k_{token}"
    dest_table = f"r4u_k_{kind[:4]}_{token}"
    try:
        _seed_keyless(pg, table)
        _exec(kind, _KEYLESS_DDL[kind].format(t=_q(kind, dest_table)))
        src, dst = _connectors(token, kind)
        keyed = _keyed("incremental_upsert")

        staged, _plan, blockers = _stage(src.id, dst.id, table, dest_table,
                                         "incremental_upsert", **keyed)
        assert not staged.success
        text = " ".join(blockers)
        assert "CREATE UNIQUE INDEX" in text and "uidx_" in text, blockers
        assert _keyless_rows(kind, dest_table) == {}

        approval = {"approved_by": "qa-operator", "reason": "approve the disclosed unique index"}
        _run_ok(client, src.id, dst.id, table, dest_table, "incremental_upsert",
                ddl_acknowledgement=approval, **keyed)
        assert _keyless_rows(kind, dest_table) == {1: "n1", 2: "", 3: "n3", 4: "n4"}
        assert _unique_index_count(kind, dest_table) == 1

        pg.cursor().execute(f"UPDATE {table} SET note = 'v2', updated_at = 50 WHERE id = 3")  # nosec B608
        _run_ok(client, src.id, dst.id, table, dest_table, "incremental_upsert",
                ddl_acknowledgement=approval, **keyed)
        assert _keyless_rows(kind, dest_table) == {1: "n1", 2: "", 3: "v2", 4: "n4"}
    finally:
        pg.cursor().execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.close()
        _drop(kind, dest_table)


@pytest.mark.parametrize("kind", ["sqlserver", "snowflake"])
def test_d101_merge_engine_upserts_keyless_table_without_any_index(client, kind):
    pg = _pg_conn()
    token = uuid.uuid4().hex[:8]
    table = f"r4u_m_{token}"
    dest_table = f"r4u_m_{kind[:4]}_{token}"
    try:
        _seed_keyless(pg, table)
        _exec(kind, _KEYLESS_DDL[kind].format(t=_q(kind, dest_table)))
        src, dst = _connectors(token, kind)
        keyed = _keyed("incremental_upsert")
        first = _run_ok(client, src.id, dst.id, table, dest_table, "incremental_upsert", **keyed)
        _no_index_ddl_offered(first["plan"])
        pg.cursor().execute(f"UPDATE {table} SET note = 'v2', updated_at = 50 WHERE id = 3")  # nosec B608
        _run_ok(client, src.id, dst.id, table, dest_table, "incremental_upsert", **keyed)
        assert _keyless_rows(kind, dest_table) == {1: "n1", 2: "", 3: "v2", 4: "n4"}
        assert _unique_index_count(kind, dest_table) == 0
    finally:
        pg.cursor().execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.close()
        _drop(kind, dest_table)


# --------------------------------------------------------------------------- #
# CR-D126 + D53: TEXT keys, 191-char shared prefixes, '' and unicode keys.
# --------------------------------------------------------------------------- #

_TEXT_KEYS = ["a", "", "Łukasz 山田 🎉", "x" * 191 + "A", "x" * 191 + "B", "B"]


def _seed_text_keys(pg, table: str, keytype: str = "TEXT") -> None:
    cur = pg.cursor()
    cur.execute(f"CREATE TABLE {table} (id {keytype} PRIMARY KEY, note TEXT, "  # nosec B608
                "updated_at BIGINT NOT NULL)")
    for i, k in enumerate(_TEXT_KEYS):
        cur.execute(f"INSERT INTO {table} VALUES (%s, %s, %s)", (k, f"n{i}", i))  # nosec B608


def _text_rows(kind: str, dest_table: str) -> dict[str, Any]:
    rows = _select(kind, f"SELECT id, note FROM {_q(kind, dest_table)}")  # nosec B608
    return {r[0]: r[1] for r in rows}


@pytest.mark.parametrize("kind", ["postgresql", "mysql", "mariadb", "sqlserver", "snowflake"])
def test_text_keys_create_then_upsert_keep_every_key_distinct(client, kind):
    """PG TEXT PK -> new table, incremental_upsert twice (QA's CR-D126 route).

    MySQL/MariaDB used to fail with error 1170 (TEXT in a key) after Validate
    passed; a 191-char prefix index would merge the two ``x…`` keys; ``''``
    must land as its own key (D53)."""
    pg = _pg_conn()
    token = uuid.uuid4().hex[:8]
    table = f"r4u_t_{token}"
    dest_table = f"r4u_t_{kind[:4]}_{token}"
    try:
        _seed_text_keys(pg, table)
        src, dst = _connectors(token, kind)
        keyed = _keyed("incremental_upsert")
        _run_ok(client, src.id, dst.id, table, dest_table, "incremental_upsert", **keyed)
        assert _text_rows(kind, dest_table) == {k: f"n{i}" for i, k in enumerate(_TEXT_KEYS)}
        cur = pg.cursor()
        for k in ("", "x" * 191 + "B", "Łukasz 山田 🎉"):
            cur.execute(f"UPDATE {table} SET note = %s, updated_at = updated_at + 100 "  # nosec B608
                        "WHERE id = %s", (f"v2-{len(k)}", k))
        _run_ok(client, src.id, dst.id, table, dest_table, "incremental_upsert", **keyed)
        expected = {k: f"n{i}" for i, k in enumerate(_TEXT_KEYS)}
        for k in ("", "x" * 191 + "B", "Łukasz 山田 🎉"):
            expected[k] = f"v2-{len(k)}"
        assert _text_rows(kind, dest_table) == expected
    finally:
        pg.cursor().execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.close()
        _drop(kind, dest_table)


@pytest.mark.parametrize("kind", ["mysql", "mariadb"])
def test_cr_d126_existing_text_key_table_is_made_full_width_never_prefixed(client, kind):
    pg = _pg_conn()
    token = uuid.uuid4().hex[:8]
    table = f"r4u_p_{token}"
    dest_table = f"r4u_p_{kind[:4]}_{token}"
    try:
        _seed_text_keys(pg, table)
        _exec(kind, f"CREATE TABLE {_q(kind, dest_table)} (id TEXT, note TEXT, updated_at BIGINT)")
        src, dst = _connectors(token, kind)
        keyed = _keyed("incremental_upsert")
        staged, _plan, blockers = _stage(src.id, dst.id, table, dest_table,
                                         "incremental_upsert", **keyed)
        text = " ".join(blockers)
        # What Validate discloses is what the writer runs: TEXT -> VARCHAR(768)
        # then the full-width unique index. Never a prefix length.
        assert "ALTER TABLE" in text and "VARCHAR(768)" in text, blockers
        assert "(191)" not in text
        _run_ok(client, src.id, dst.id, table, dest_table, "incremental_upsert",
                ddl_acknowledgement={"approved_by": "qa", "reason": "approve disclosed key DDL"},
                **keyed)
        assert _text_rows(kind, dest_table) == {k: f"n{i}" for i, k in enumerate(_TEXT_KEYS)}
        subparts = _select(kind, "SELECT SUB_PART FROM INFORMATION_SCHEMA.STATISTICS WHERE "
                                 f"TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{dest_table}' "
                                 "AND NON_UNIQUE = 0")
        assert subparts and all(r[0] is None for r in subparts), subparts
    finally:
        pg.cursor().execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.close()
        _drop(kind, dest_table)


def test_cr_d126_json_key_is_refused_plainly_at_validate(client):
    kind = "mysql"
    pg = _pg_conn()
    token = uuid.uuid4().hex[:8]
    table = f"r4u_jk_{token}"
    dest_table = f"r4u_jk_{token}_out"
    try:
        cur = pg.cursor()
        cur.execute(f"CREATE TABLE {table} (id JSONB, note TEXT, updated_at BIGINT NOT NULL)")  # nosec B608
        cur.execute(f"INSERT INTO {table} VALUES ('{{\"k\": 1}}', 'a', 1)")  # nosec B608
        _exec(kind, f"CREATE TABLE {_q(kind, dest_table)} (id JSON, note TEXT, updated_at BIGINT)")
        src, dst = _connectors(token, kind)
        staged, _plan, blockers = _stage(
            src.id, dst.id, table, dest_table, "incremental_upsert",
            ddl_acknowledgement={"approved_by": "qa", "reason": "approve disclosed key DDL"},
            **_keyed("incremental_upsert"),
        )
        assert not staged.success
        text = " ".join(blockers)
        assert "cannot index" in text and "prefix" in text, blockers
        assert "CREATE UNIQUE INDEX" not in text
        assert _text_rows(kind, dest_table) == {}
    finally:
        pg.cursor().execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.close()
        _drop(kind, dest_table)


def test_d53_oracle_stores_empty_key_as_null_and_validate_says_so(client):
    pg = _pg_conn()
    token = uuid.uuid4().hex[:8]
    table = f"r4u_o_{token}"
    dest_table = f"R4U_O_{token}".upper()
    try:
        cur = pg.cursor()
        cur.execute(f"CREATE TABLE {table} (id VARCHAR(40) PRIMARY KEY, note TEXT, "  # nosec B608
                    "updated_at BIGINT NOT NULL)")
        for i, k in enumerate(["a", "", "b"]):
            cur.execute(f"INSERT INTO {table} VALUES (%s, %s, %s)", (k, f"n{i}", i))  # nosec B608
        src, dst = _connectors(token, "oracle")
        staged, _plan, blockers = _stage(src.id, dst.id, table, dest_table,
                                         "incremental_upsert", **_keyed("incremental_upsert"))
        assert not staged.success, "'' cannot be an Oracle key: it is stored as NULL"
        assert any("null" in b.lower() for b in blockers), blockers
    finally:
        pg.cursor().execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.close()
        _drop("oracle", dest_table)


# --------------------------------------------------------------------------- #
# D89: Kafka changelog with repeated keys -> PG, last write wins by offset.
# --------------------------------------------------------------------------- #


@pytest.fixture
def topic():
    if not _up(_KAFKA["port"], _KAFKA["host"]):
        pytest.skip("Kafka 127.0.0.1:9092 not reachable")
    pytest.importorskip("kafka")
    name = f"r4u_{uuid.uuid4().hex[:10]}"
    yield name
    from services.copy_kafka_common import kafka_admin

    admin = kafka_admin(_KAFKA)
    try:
        admin.delete_topics([name])
    except Exception:
        pass
    finally:
        admin.close()


def _produce(topic: str, rows: list[dict]) -> None:
    from connectors.kafka_writer import _producer

    producer = _producer(_KAFKA)
    try:
        for row in rows:
            producer.send(topic, value=row, key=str(row["id"]))
        producer.flush(10)
    finally:
        producer.close()


@pytest.mark.parametrize("keys", ["bigint", "text"])
def test_d89_kafka_changelog_repeated_keys_last_write_wins_into_pg(client, topic, keys):
    from services.connector_store import create_connector

    def k(v: Any) -> Any:
        return v if keys == "bigint" else {1: "a", 2: "", 3: "Łukasz 山田", 4: "z"}[v]

    dest_table = f"r4u_kf_{uuid.uuid4().hex[:8]}"
    src = create_connector({"name": f"k-{dest_table}", "type": "kafka", "role": "source",
                            "workspace_id": "", **_KAFKA})
    dst = create_connector({"name": f"p-{dest_table}", "type": "postgresql",
                            "role": "destination", "workspace_id": "", "ssl": False,
                            **_PG, "schema": "public"})
    keyed = dict(primary_key="id", cursor_column="_kafka_offset",
                 cursor_semantics="monotonic_sequence")
    try:
        _produce(topic, [
            {"id": k(1), "amount": 10, "ver": 1}, {"id": k(2), "amount": 20, "ver": 1},
            {"id": k(3), "amount": 30, "ver": 1}, {"id": k(1), "amount": 999, "ver": 2},
            {"id": k(2), "amount": 21, "ver": 2},
        ])
        _run_ok(client, src.id, dst.id, topic, dest_table, "incremental_upsert", **keyed)
        _produce(topic, [
            {"id": k(2), "amount": 222, "ver": 3}, {"id": k(2), "amount": 2222, "ver": 4},
            {"id": k(4), "amount": 40, "ver": 1},
        ])
        _run_ok(client, src.id, dst.id, topic, dest_table, "incremental_upsert", **keyed)
        rows = _select("postgresql",
                       f'SELECT id, amount, ver, _kafka_offset FROM public."{dest_table}"')  # nosec B608
        got = {r[0]: (int(r[1]), int(r[2])) for r in rows}
        assert got == {k(1): (999, 2), k(2): (2222, 4), k(3): (30, 1), k(4): (40, 1)}
        offsets = {r[0]: int(r[3]) for r in rows}
        # The surviving image is the one with the highest offset for its key.
        assert offsets[k(2)] == max(offsets.values()) - 1
    finally:
        _drop("postgresql", dest_table)
