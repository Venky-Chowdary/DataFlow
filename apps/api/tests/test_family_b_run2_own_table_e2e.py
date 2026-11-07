"""Family B (D182 JSON half, D198, D45, D144) — run 2 into the table DataFlow created.

QA (production ac612203): run 1 creates the destination table; the source is
tampered; run 2 with identical parameters is blocked because the destination type
DataFlow chose on run 1 (JSONB / JSON / NVARCHAR(MAX) / CLOB / VARIANT ...) is read
back and graded as if a user had created it: "Mapping confidence below floor: j",
"JSONB -> TEXT fidelity collapse; schema_drift narrow_type", "payload below the
Map confidence floor".

QA's acceptance, driven here through QA's own route on real engines
(``start_transfer`` -> ``/api/v1/copilot/confirm`` -> job):

  for each destination (PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, MongoDB,
  Snowflake[fakesnow emulator]):
    1. run 1 (full_refresh_overwrite) creates the table,
    2. the source is tampered (updates, NULLs written over values, values written
       over NULLs, a new row),
    3. run 2 in full_refresh_overwrite and, separately, run 2 in
       incremental_upsert (key id, cursor updated_at, source_timezone UTC)
       succeed with no contract, no risk acceptance and no acknowledgement flag,
  and every cell (jsonb, text, numeric, timestamptz, NULLs) reads back equal.

Each destination reads its credentials from environment variables and skips when
they are absent. Snowflake runs on the fakesnow emulator only: that leg is an
emulator result, not a live-Snowflake claim.
"""

from __future__ import annotations

import json
import os
import socket
import time
import uuid
from datetime import datetime, timezone
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

_COLS = ("id", "j", "note", "amt", "ts", "updated_at")


def _t(s: str) -> datetime:
    return datetime.fromisoformat(s).astimezone(timezone.utc)


# id -> (j, note, amt, ts). updated_at is set per run.
_SEED: dict[int, tuple[Any, Any, Any, Any]] = {
    1: ({"a": {"b": [1, 2, {"c": "x"}]}, "n": None, "ok": True}, "plain",
        Decimal("12.3400"), _t("2024-01-15T10:30:00.123+05:30")),
    2: ([1, "two", None, {"k": []}], "Łukasz 山田 🎉", Decimal("-0.0001"),
        _t("1999-12-31T23:59:59.500+00:00")),
    3: (None, None, None, None),
    4: ({}, "   ", Decimal("0"), _t("2024-03-10T07:30:00-05:00")),
    5: ({"name": "é ü 中文", "emoji": "👩‍💻"}, "tab\tand\nnewline",
        Decimal("99999999.9999"), _t("2038-01-19T03:14:08+00:00")),
    6: ([], "", Decimal("1.5"), _t("1970-01-01T00:00:00+00:00")),
}

# Run 2: values over NULLs, NULLs over values, changed documents, one new row.
_TAMPER: dict[int, tuple[Any, Any, Any, Any]] = {
    2: ({"changed": ["x", {"y": "中文"}]}, "Łukasz 山田 🎉", Decimal("-0.0001"),
        _t("1999-12-31T23:59:59.500+00:00")),
    3: ({"was": "null"}, "now set", Decimal("42.4200"), _t("2025-06-01T12:00:00+02:00")),
    4: (None, None, None, None),
    7: ({"added": True, "list": [1.5, -2, 0]}, "new row", Decimal("-123.4560"),
        _t("2026-10-29T23:59:59.999+00:00")),
}

_KINDS = ["postgresql", "mysql", "mariadb", "sqlserver", "oracle", "mongodb", "snowflake"]


def _up(port: int, host: str = "127.0.0.1") -> bool:
    try:
        socket.create_connection((host, port), timeout=1).close()
        return True
    except OSError:
        return False


def _pg_conn():
    if not _PG["password"] or not _up(_PG["port"], _PG["host"]):
        pytest.skip("PostgreSQL source not configured (PG* env)")
    try:
        conn = psycopg2.connect(
            host=_PG["host"], port=_PG["port"], user=_PG["username"],
            password=_PG["password"], dbname=_PG["database"], connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    return conn


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "test-pii-key")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "")
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
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


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _dest_config(kind: str) -> dict[str, Any]:
    if kind == "postgresql":
        return dict(type="postgresql", host=_PG["host"], port=_PG["port"],
                    database=_PG["database"], username=_PG["username"],
                    password=_PG["password"], schema="public")
    if kind == "mysql":
        pw = _env("MYSQL_PASSWORD")
        if not pw or not _up(3306):
            pytest.skip("MySQL not configured (MYSQL_PASSWORD, :3306)")
        return dict(type="mysql", host="127.0.0.1", port=3306,
                    database=_env("MYSQL_DATABASE", "dataflow"),
                    username=_env("MYSQL_USER", "dataflow"), password=pw)
    if kind == "mariadb":
        pw = _env("DATAFLOW_TEST_MARIADB_PASSWORD")
        port = int(_env("DATAFLOW_TEST_MARIADB_PORT", "3307"))
        host = _env("DATAFLOW_TEST_MARIADB_HOST", "127.0.0.1")
        if not pw or not _up(port, host):
            pytest.skip("MariaDB not configured (DATAFLOW_TEST_MARIADB_*)")
        return dict(type="mariadb", host=host, port=port,
                    database=_env("DATAFLOW_TEST_MARIADB_DB", "dataflow"),
                    username=_env("DATAFLOW_TEST_MARIADB_USER", "dataflow"), password=pw)
    if kind == "sqlserver":
        pw = _env("DATAFLOW_TEST_MSSQL_PASSWORD")
        if not pw or not _up(1433):
            pytest.skip("SQL Server not configured (DATAFLOW_TEST_MSSQL_PASSWORD, :1433)")
        pytest.importorskip("pyodbc")
        return dict(type="sqlserver", host="127.0.0.1", port=1433, database="dataflow",
                    username=_env("DATAFLOW_TEST_MSSQL_USER", "sa"), password=pw,
                    extra={"trust_server_certificate": True})
    if kind == "oracle":
        pw = _env("DATAFLOW_TEST_ORACLE_PASSWORD")
        host = _env("DATAFLOW_TEST_ORACLE_HOST", "127.0.0.1")
        port = int(_env("DATAFLOW_TEST_ORACLE_PORT", "1521"))
        if not pw or not _up(port, host):
            pytest.skip("Oracle not configured (DATAFLOW_TEST_ORACLE_*)")
        pytest.importorskip("oracledb")
        user = _env("DATAFLOW_TEST_ORACLE_USER")
        return dict(type="oracle", host=host, port=port,
                    database=_env("DATAFLOW_TEST_ORACLE_SERVICE", "FREEPDB1"),
                    username=user, password=pw, schema=user.upper())
    if kind == "mongodb":
        if not _up(27017):
            pytest.skip("MongoDB not reachable on :27017")
        pytest.importorskip("pymongo")
        return dict(type="mongodb", host="127.0.0.1", port=27017, database="dataflow_test")
    if kind == "snowflake":
        pytest.importorskip("fakesnow")
        return dict(type="snowflake", host="localhost", port=443, database="dataflow",
                    username="test", password="test", schema="public")
    raise AssertionError(kind)


def _mssql_datetimeoffset(raw: bytes) -> datetime:
    import struct
    from datetime import timedelta

    y, mo, d, h, mi, s, ns, oh, om = struct.unpack("<6hI2h", raw)
    tz = timezone(timedelta(hours=oh, minutes=om))
    return datetime(y, mo, d, h, mi, s, ns // 1000, tzinfo=tz)


def _sql_rows(kind: str, table: str) -> list[tuple]:
    cols = ", ".join(_COLS)
    if kind == "postgresql":
        conn = _pg_conn()
        cur = conn.cursor()
        cur.execute(f'SELECT {cols} FROM public."{table}"')  # nosec B608
        rows = cur.fetchall()
        conn.close()
        return rows
    if kind in {"mysql", "mariadb"}:
        import pymysql

        if kind == "mysql":
            cfg = dict(host="127.0.0.1", port=3306, user=_env("MYSQL_USER", "dataflow"),
                       password=_env("MYSQL_PASSWORD"), database=_env("MYSQL_DATABASE", "dataflow"))
        else:
            cfg = dict(host=_env("DATAFLOW_TEST_MARIADB_HOST", "127.0.0.1"),
                       port=int(_env("DATAFLOW_TEST_MARIADB_PORT", "3307")),
                       user=_env("DATAFLOW_TEST_MARIADB_USER", "dataflow"),
                       password=_env("DATAFLOW_TEST_MARIADB_PASSWORD"),
                       database=_env("DATAFLOW_TEST_MARIADB_DB", "dataflow"))
        conn = pymysql.connect(charset="utf8mb4", **cfg)
        cur = conn.cursor()
        cur.execute(f"SELECT {cols} FROM `{table}`")  # nosec B608
        rows = cur.fetchall()
        conn.close()
        return list(rows)
    if kind == "sqlserver":
        import pyodbc

        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,1433;DATABASE=dataflow;"
            f"UID={_env('DATAFLOW_TEST_MSSQL_USER', 'sa')};"
            f"PWD={_env('DATAFLOW_TEST_MSSQL_PASSWORD')};TrustServerCertificate=yes",
            timeout=10,
        )
        conn.add_output_converter(-155, _mssql_datetimeoffset)
        rows = [tuple(r) for r in conn.execute(f"SELECT {cols} FROM [{table}]").fetchall()]  # nosec B608
        conn.close()
        return rows
    if kind == "oracle":
        import oracledb

        conn = oracledb.connect(
            user=_env("DATAFLOW_TEST_ORACLE_USER"), password=_env("DATAFLOW_TEST_ORACLE_PASSWORD"),
            dsn=f"{_env('DATAFLOW_TEST_ORACLE_HOST', '127.0.0.1')}:"
                f"{_env('DATAFLOW_TEST_ORACLE_PORT', '1521')}/"
                f"{_env('DATAFLOW_TEST_ORACLE_SERVICE', 'FREEPDB1')}",
        )
        cur = conn.cursor()
        quoted = ", ".join(f'"{c.upper()}"' for c in _COLS)
        cur.execute(f'SELECT {quoted} FROM "{table.upper()}"')  # nosec B608
        rows = []
        for r in cur.fetchall():
            rows.append(tuple(v.read() if hasattr(v, "read") else v for v in r))
        conn.close()
        return rows
    if kind == "snowflake":
        import connectors.snowflake_conn as sc

        conn = sc.get_connection(
            account="localhost", username="test", password="test", database="dataflow",
            schema="public", warehouse="", connection_string="",
        )
        cur = conn.cursor()
        cur.execute(f"SELECT {cols} FROM {table}")  # nosec B608
        rows = cur.fetchall()
        conn.close()
        return rows
    raise AssertionError(kind)


def _drop(kind: str, table: str) -> None:
    try:
        if kind == "postgresql":
            conn = _pg_conn()
            conn.cursor().execute(f'DROP TABLE IF EXISTS public."{table}"')  # nosec B608
            conn.close()
        elif kind in {"mysql", "mariadb"}:
            import pymysql

            cfg = _dest_config(kind)
            conn = pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["username"],
                                   password=cfg["password"], database=cfg["database"])
            conn.cursor().execute(f"DROP TABLE IF EXISTS `{table}`")  # nosec B608
            conn.commit()
            conn.close()
        elif kind == "sqlserver":
            import pyodbc

            conn = pyodbc.connect(
                "DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,1433;DATABASE=dataflow;"
                f"UID={_env('DATAFLOW_TEST_MSSQL_USER', 'sa')};"
                f"PWD={_env('DATAFLOW_TEST_MSSQL_PASSWORD')};TrustServerCertificate=yes",
                timeout=10,
            )
            conn.execute(f"DROP TABLE IF EXISTS [{table}]")  # nosec B608
            conn.commit()
            conn.close()
        elif kind == "oracle":
            import oracledb

            cfg = _dest_config(kind)
            conn = oracledb.connect(user=cfg["username"], password=cfg["password"],
                                    dsn=f"{cfg['host']}:{cfg['port']}/{cfg['database']}")
            cur = conn.cursor()
            cur.execute(
                "SELECT table_name FROM user_tables WHERE table_name LIKE :p",
                p=table.upper() + "%",
            )
            for (name,) in cur.fetchall():
                cur.execute(f'DROP TABLE "{name}" PURGE')  # nosec B608
            conn.close()
        elif kind == "mongodb":
            import pymongo

            client = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
            client["dataflow_test"][table].drop()
            client.close()
    except Exception:  # pragma: no cover - best-effort cleanup
        pass


# --------------------------------------------------------------------------- #
# Cell normalisation: each engine's driver hands values back in its own shape.
# --------------------------------------------------------------------------- #


def _num(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "to_decimal"):  # bson Decimal128
        value = value.to_decimal()
    return Decimal(str(value)).normalize()


def _json_plain(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _json_plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_plain(v) for v in value]
    if hasattr(value, "to_decimal"):
        value = value.to_decimal()
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    return value


def _doc(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        value = json.loads(value)
    return _json_plain(value)


def _ts(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value.strip().replace("Z", "+00:00").replace(" ", "T", 1))
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)  # stored as UTC wall clock
    return value.astimezone(timezone.utc)


def _cells(row: tuple) -> tuple:
    rid, j, note, amt, ts = row[:5]
    return int(rid), _doc(j), note, _num(amt), _ts(ts)


def _read_back(kind: str, table: str) -> dict[int, tuple]:
    if kind == "mongodb":
        import pymongo

        client = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
        try:
            docs = list(client["dataflow_test"][table].find({}))
        finally:
            client.close()
        rows = [
            (d.get("id", d.get("_id")), d.get("j"), d.get("note"), d.get("amt"), d.get("ts"))
            for d in docs
        ]
    else:
        rows = _sql_rows(kind, table)
    out: dict[int, tuple] = {}
    for r in rows:
        c = _cells(r)
        out[c[0]] = c[1:]
    return out


def _expected(data: dict[int, tuple], kind: str) -> dict[int, tuple]:
    def text(note: Any) -> Any:
        # Oracle has no empty string: '' IS NULL in every Oracle text type.
        return None if kind == "oracle" and note == "" else note

    return {
        i: (_json_plain(j), text(note), _num(amt), _ts(ts))
        for i, (j, note, amt, ts) in data.items()
    }


# --------------------------------------------------------------------------- #
# QA's route: start_transfer -> confirm -> job.
# --------------------------------------------------------------------------- #


def _wait_job(job_id: str, timeout_s: float = 240.0) -> dict:
    from services.mongodb_service import get_mongodb_service

    deadline = time.monotonic() + timeout_s
    job: dict = {}
    while time.monotonic() < deadline:
        job = get_mongodb_service().get_job(job_id) or {}
        if str(job.get("status") or "") in {"completed", "failed", "partial", "cancelled"}:
            return job
        time.sleep(0.25)
    return job


def _run(client, src_id: str, dst_id: str, table: str, dest_table: str, mode: str) -> dict:
    from src.ai.copilot.transfer_tools import start_transfer

    kwargs: dict[str, Any] = dict(
        source_connector_id=src_id,
        source_table=table,
        dest_connector_id=dst_id,
        dest_table=dest_table,
        sync_mode=mode,
        validation_mode="strict",
        source_timezone="UTC",
    )
    if mode == "incremental_upsert":
        kwargs.update(primary_key="id", cursor_column="updated_at",
                      cursor_semantics="modification_timestamp")
    # No contract, no risk acceptance, no schema-drift or shared-destination ack.
    staged = start_transfer(**kwargs)
    plan = (staged.output or {}).get("plan") or staged.output or {}
    preflight = plan.get("preflight") or {}
    blockers = [
        f"{b.get('gate') or b.get('id')}: {b.get('message')}"
        for b in (preflight.get("blockers") or [])
    ]
    assert staged.success, {"mode": mode, "error": staged.error, "blockers": blockers}
    assert not blockers, blockers
    # Every column maps onto its own name, above the Map floor, needing no review.
    mappings = plan.get("mappings") or []
    assert {str(m.get("source")) for m in mappings} >= set(_COLS), mappings
    for m in mappings:
        assert str(m.get("target") or "").casefold() == str(m.get("source")).casefold(), m
        assert float(m.get("confidence") or 0) >= 0.85, m
        assert not m.get("requires_review"), m
        assert not m.get("requires_risk_contract"), m
    resp = client.post(
        "/api/v1/copilot/confirm",
        json={"ack_id": staged.output["ack_id"], "reason": "family-b"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("ok") is True, body
    job = _wait_job(str(body.get("job_id") or ""))
    assert str(job.get("status")) == "completed", {
        "mode": mode,
        "status": job.get("status"),
        "error": job.get("error") or job.get("message"),
    }
    return job


def _write_source(cur, table: str, data: dict[int, tuple], stamp: datetime) -> None:
    for i, (j, note, amt, ts) in data.items():
        cur.execute(
            f"INSERT INTO {table} VALUES (%s, %s, %s, %s, %s, %s) "  # nosec B608
            "ON CONFLICT (id) DO UPDATE SET j = EXCLUDED.j, note = EXCLUDED.note, "
            "amt = EXCLUDED.amt, ts = EXCLUDED.ts, updated_at = EXCLUDED.updated_at",
            (i, None if j is None else json.dumps(j), note, amt, ts, stamp),
        )


@pytest.mark.parametrize("run2_mode", ["full_refresh_overwrite", "incremental_upsert"])
@pytest.mark.parametrize("kind", _KINDS)
def test_run2_into_own_table_needs_no_contract_and_matches_cell_by_cell(client, kind, run2_mode):
    from services.connector_store import create_connector

    dest_cfg = _dest_config(kind)
    pg = _pg_conn()
    token = uuid.uuid4().hex[:8]
    table = f"famb_{kind[:5]}_{token}"
    dest_table = table + "_out"
    cur = pg.cursor()
    try:
        cur.execute(
            f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, j JSONB, note TEXT, "  # nosec B608
            "amt NUMERIC(12,4), ts TIMESTAMPTZ, updated_at TIMESTAMPTZ NOT NULL)"
        )
        _write_source(cur, table, _SEED, _t("2026-01-01T00:00:00+00:00"))
        src = create_connector(
            {"name": f"famb-pg-{token}", "type": "postgresql", "role": "source",
             "workspace_id": "", "ssl": False, **_PG, "schema": "public"}
        )
        dst = create_connector(
            {"name": f"famb-{kind}-{token}", "role": "destination", "workspace_id": "",
             "ssl": False, **dest_cfg}
        )

        # Run 1 creates the table.
        _run(client, src.id, dst.id, table, dest_table, "full_refresh_overwrite")
        assert _read_back(kind, dest_table) == _expected(_SEED, kind)

        # Tamper the source.
        _write_source(cur, table, _TAMPER, _t("2026-02-01T00:00:00+00:00"))

        # Run 2 into DataFlow's own table, no contract and no flags.
        _run(client, src.id, dst.id, table, dest_table, run2_mode)
        got = _read_back(kind, dest_table)
        want = _expected({**_SEED, **_TAMPER}, kind)
        assert sorted(got) == sorted(want)
        for i in sorted(want):
            for col, g, w in zip(_COLS[1:], got[i], want[i]):
                assert g == w, (kind, run2_mode, i, col, g, w)
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.close()
        _drop(kind, dest_table)
