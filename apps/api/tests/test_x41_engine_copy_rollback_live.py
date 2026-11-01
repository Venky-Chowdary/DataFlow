"""X-41 LIVE: engine-copy rollback honesty — the undo must classify the
pre-run state correctly and the message must say what happened to the table.

QA round 12 changed the contract for a failed *validation* on an overwrite:
the table this run created is kept for inspection (it used to be dropped),
and the message says where the data is.

Live evidence (loop probes on 6669447d): on the in-engine copy route
("Row count verified by engine copy") — PG→MSSQL, PG→MySQL, PG→SQLite —
a post-write tamper correctly failed verification, but the undo
misclassified the pre-run state: message read "The destination was empty
before this run… table is empty again" while the created table was left
behind empty, and records_transferred reported 5 on the rolled-back job.
PG→DuckDB goes through the row path and already reports correctly
("no such table before this run … table was dropped").

Root cause: the engine-copy routes never stamped
``dest_table_existed_before`` — ``_try_copy_fast_path`` now takes the
pre-DDL existence probe once, up front, for every source/dest pair, and a
failed run's undo drops a run-created table and says so.

This test is the live regression: real PG source → {MSSQL, MySQL, SQLite}
engine-copy runs plus a PG→DuckDB row-path control, each parametrized over
the checksum-failure (tamper a value) and sample/census-failure (delete a
row) branches. It SKIPS when engines are unreachable.
"""

from __future__ import annotations

import os
import socket
import sys
import tempfile
from pathlib import Path
from typing import Any

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

pytestmark = pytest.mark.skipif(
    not os.getenv("DATAFLOW_X41_LIVE"),
    reason="live X-41 check: set DATAFLOW_X41_LIVE=1 with reachable engines",
)

_VALUES = ["1.5", "-0.0000000001", "0", None, "999.25"]


class _FakeMongo:
    def __init__(self):
        self.jobs: dict[str, dict] = {}

    def get_job(self, job_id: str) -> dict | None:
        return self.jobs.get(job_id)

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        self.jobs.setdefault(job_id, {})
        self.jobs[job_id].update(kwargs)

    def update_job_fields(self, job_id: str, fields: dict) -> bool:
        self.jobs.setdefault(job_id, {}).update(fields)
        return True


@pytest.fixture(autouse=True)
def _patch_mongodb_service(monkeypatch):
    fake = _FakeMongo()
    # execute_tracked demands a registered pending job as its durable resume
    # point — an unregistered id fails with CheckpointPersistenceError before
    # the tamper hook can fire.
    fake.update_job_status(
        "0000000000000000000000aa", "pending", transfer_request={}
    )
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)


def _cfg(prefix: str, default_port: int) -> dict[str, Any] | None:
    host = os.getenv(f"{prefix}_HOST", "")
    if not host:
        return None
    return {
        "host": host,
        "port": int(os.getenv(f"{prefix}_PORT", str(default_port))),
        "database": os.getenv(f"{prefix}_DB", ""),
        "username": os.getenv(f"{prefix}_USER", ""),
        "password": os.getenv(f"{prefix}_PASSWORD", ""),
        "connection_string": os.getenv(f"{prefix}_CONNECTION_STRING", ""),
        "ssl": False,
        **({"encrypt": os.getenv(f"{prefix}_ENCRYPT")} if os.getenv(f"{prefix}_ENCRYPT") else {}),
        "trust_server_certificate": os.getenv(
            f"{prefix}_TRUST_SERVER_CERTIFICATE", "yes"
        ) in {"1", "true", "yes", "y", "on"},
    }


def _reachable(host: str, port: int) -> bool:
    try:
        socket.create_connection((host, int(port)), timeout=3).close()
        return True
    except OSError:
        return False


def _pg_conn(pg: dict[str, Any]):
    import psycopg2

    return psycopg2.connect(
        host=pg["host"], port=int(pg["port"]), user=pg["username"],
        password=pg["password"], dbname=pg["database"],
    )


def _seed_pg_source(pg: dict[str, Any], table: str) -> None:
    conn = _pg_conn(pg)
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        cur.execute(
            f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, "
            "t NUMERIC(38,10), label VARCHAR(50))"
        )
        for i, v in enumerate(_VALUES):
            cur.execute(
                f"INSERT INTO {table} (id, t, label) VALUES (%s, %s, %s)",  # nosec B608
                (i + 1, v, f"lbl{i + 1}"),
            )
    finally:
        conn.close()


def _drop_pg_source(pg: dict[str, Any], table: str) -> None:
    conn = _pg_conn(pg)
    try:
        conn.autocommit = True
        conn.cursor().execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
    finally:
        conn.close()


def _seed_mysql_source(mysql: dict[str, Any], table: str) -> None:
    conn = _mysql_conn(mysql)
    try:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        cur.execute(
            f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, "
            "t VARCHAR(50), label VARCHAR(50))"
        )
        for i, v in enumerate(_VALUES):
            cur.execute(
                f"INSERT INTO {table} (id, t, label) VALUES (%s, %s, %s)",  # nosec B608
                (i + 1, v, f"lbl{i + 1}"),
            )
    finally:
        conn.close()


def _drop_mysql_source(mysql: dict[str, Any], table: str) -> None:
    conn = _mysql_conn(mysql)
    try:
        conn.cursor().execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
    finally:
        conn.close()


def _dest_exists_sqlite(path: str, table: str) -> bool:
    import sqlite3

    conn = sqlite3.connect(path)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def _dest_exists_duckdb(path: str, table: str) -> bool:
    import duckdb

    conn = duckdb.connect(path)
    try:
        rows = conn.execute("SHOW TABLES").fetchall()
        return any(str(r[0]).lower() == table.lower() for r in rows)
    finally:
        conn.close()


def _mysql_conn(cfg: dict[str, Any]):
    import pymysql

    return pymysql.connect(
        host=cfg["host"], port=int(cfg["port"]), user=cfg["username"],
        password=cfg["password"], database=cfg["database"], autocommit=True,
    )


def _mssql_conn(cfg: dict[str, Any]):
    import pyodbc

    cs = cfg.get("connection_string") or (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={cfg['host']},{cfg['port']};"
        f"DATABASE={cfg['database']};"
        f"UID={cfg['username']};PWD={cfg['password']};"
        f"TrustServerCertificate={'yes' if cfg.get('trust_server_certificate') else 'no'};"
        f"Encrypt={cfg.get('encrypt') or 'yes'};"
    )
    return pyodbc.connect(cs, timeout=10)


def _dest_exists(engine: str, cfg: dict[str, Any], table: str) -> bool:
    if engine == "sqlite":
        return _dest_exists_sqlite(cfg["database"], table)
    if engine == "duckdb":
        return _dest_exists_duckdb(cfg["database"], table)
    if engine == "mysql":
        conn = _mysql_conn(cfg)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT TABLE_NAME FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s",
                (cfg["database"], table),
            )
            return cur.fetchone() is not None
        finally:
            conn.close()
    conn = _mssql_conn(cfg)
    try:
        cur = conn.cursor()
        cur.execute("SELECT OBJECT_ID(?, 'U')", (f"dbo.{table}",))
        return cur.fetchone()[0] is not None
    finally:
        conn.close()


def _drop_dest(engine: str, cfg: dict[str, Any], table: str) -> None:
    """The kept-for-inspection table must not leak into the next case."""
    if engine in {"sqlite", "duckdb"}:
        return  # per-test tmp_path file
    if engine == "mysql":
        conn = _mysql_conn(cfg)
        try:
            conn.cursor().execute(f"DROP TABLE IF EXISTS `{table}`")
            conn.commit()
        finally:
            conn.close()
        return
    conn = _mssql_conn(cfg)
    try:
        conn.cursor().execute(f"DROP TABLE IF EXISTS dbo.[{table}]")
        conn.commit()
    finally:
        conn.close()


def _tamper_dest(engine: str, cfg: dict[str, Any], table: str, branch: str) -> None:
    """Inject the post-write fault the verify step must catch."""
    if engine == "sqlite":
        import sqlite3

        conn = sqlite3.connect(cfg["database"])
        try:
            if branch == "checksum":
                conn.execute(f"UPDATE {table} SET t=1.5 WHERE id=1")  # nosec B608
            else:
                conn.execute(f"DELETE FROM {table} WHERE id=2")  # nosec B608
            conn.commit()
        finally:
            conn.close()
        return
    if engine == "duckdb":
        import duckdb

        conn = duckdb.connect(cfg["database"])
        try:
            if branch == "checksum":
                conn.execute(f"UPDATE {table} SET t=1.5 WHERE id=1")  # nosec B608
            else:
                conn.execute(f"DELETE FROM {table} WHERE id=2")  # nosec B608
        finally:
            conn.close()
        return
    if engine == "mysql":
        conn = _mysql_conn(cfg)
        try:
            cur = conn.cursor()
            if branch == "checksum":
                cur.execute(f"UPDATE {table} SET t=1.5 WHERE id=1")  # nosec B608
            else:
                cur.execute(f"DELETE FROM {table} WHERE id=2")  # nosec B608
        finally:
            conn.close()
        return
    conn = _mssql_conn(cfg)
    try:
        cur = conn.cursor()
        if branch == "checksum":
            cur.execute(f"UPDATE [dbo].[{table}] SET t=1.5 WHERE id=1")  # nosec B608
        else:
            cur.execute(f"DELETE FROM [dbo].[{table}] WHERE id=2")  # nosec B608
        conn.commit()
    finally:
        conn.close()


def _endpoint(engine: str, cfg: dict[str, Any], table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format=engine,
        host=str(cfg.get("host") or ""),
        port=int(cfg.get("port") or 0),
        database=str(cfg.get("database") or ""),
        table=table,
        username=str(cfg.get("username") or ""),
        password=str(cfg.get("password") or ""),
        connection_string=str(cfg.get("connection_string") or ""),
        ssl=bool(cfg.get("ssl", False)),
        extra={
            **({"encrypt": cfg["encrypt"]} if cfg.get("encrypt") else {}),
            **(
                {"trust_server_certificate": cfg["trust_server_certificate"]}
                if cfg.get("trust_server_certificate") is not None
                else {}
            ),
        },
    )


@pytest.mark.parametrize(
    "src_engine,dest_engine",
    [
        ("postgresql", "sqlite"),
        ("postgresql", "mysql"),
        ("postgresql", "mssql"),
        ("postgresql", "duckdb"),
        # X-41 reproduces on the MySQL-source engine-copy route too
        # (loop probe on f92c09e0: table left behind, "was empty
        # before" wording) — same pre-DDL probe must cover it.
        ("mysql", "mssql"),
    ],
)
@pytest.mark.parametrize("branch", ["checksum", "sample"])
def test_x41_engine_copy_created_table_kept_on_failed_verify(
    src_engine: str, dest_engine: str, branch: str, monkeypatch, tmp_path
):
    """A tampered engine-copy run must fail and keep the table it created
    for inspection (QA round 12: a failed validation no longer drops it),
    say where, and report the rows it holds — the row-path (duckdb)
    control asserts the identical contract."""
    if src_engine == "postgresql":
        src_cfg = _cfg("DATAFLOW_TEST_PG", 5432)
        if not src_cfg or not _reachable(src_cfg["host"], src_cfg["port"]):
            pytest.skip("set DATAFLOW_TEST_PG_* (reachable)")
    else:
        src_cfg = _cfg("DATAFLOW_TEST_MYSQL", 3306)
        if not src_cfg or not _reachable(src_cfg["host"], src_cfg["port"]):
            pytest.skip("set DATAFLOW_TEST_MYSQL_* (reachable)")

    dest_table = f"x41_{dest_engine}_{branch}"
    if dest_engine == "sqlite":
        dest_cfg = {"database": str(tmp_path / "x41_dest.db"), "type": "sqlite"}
    elif dest_engine == "duckdb":
        dest_cfg = {"database": str(tmp_path / "x41_dest.duckdb"), "type": "duckdb"}
    elif dest_engine == "mysql":
        c = _cfg("DATAFLOW_TEST_MYSQL", 3306)
        if not c or not _reachable(c["host"], c["port"]):
            pytest.skip("set DATAFLOW_TEST_MYSQL_* (reachable)")
        dest_cfg = c
    else:
        c = _cfg("DATAFLOW_TEST_MSSQL", 1433)
        if not c or not _reachable(c["host"], c["port"]):
            pytest.skip("set DATAFLOW_TEST_MSSQL_* (reachable)")
        dest_cfg = c

    _drop_dest(dest_engine, dest_cfg, dest_table)
    src_table = f"x41src_{src_engine}"
    if src_engine == "postgresql":
        _seed_pg_source(src_cfg, src_table)
    else:
        _seed_mysql_source(src_cfg, src_table)
    tampered = {"done": False}
    orig_recon = engine_mod.run_reconciliation

    def _tampering_recon(*args, **kwargs):
        if not tampered["done"]:
            _tamper_dest(dest_engine, dest_cfg, dest_table, branch)
            tampered["done"] = True
        return orig_recon(*args, **kwargs)

    monkeypatch.setattr(engine_mod, "run_reconciliation", _tampering_recon)
    try:
        request = TransferRequest(
            source=_endpoint(src_engine, src_cfg, src_table),
            destination=_endpoint(dest_engine, dest_cfg, dest_table),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
        )
        result = UniversalTransferEngine().execute_tracked(
            request, "0000000000000000000000aa"
        )
    finally:
        if src_engine == "postgresql":
            _drop_pg_source(src_cfg, src_table)
        else:
            _drop_mysql_source(src_cfg, src_table)

    assert tampered["done"], "tamper hook never fired before reconcile"
    assert result.success is False, (
        f"tampered run ({branch}) should fail verify, got success: {result}"
    )
    # X-41 / QA round 12: the table this run created is kept for inspection
    # and the message says where — never "dropped", never "was empty before".
    assert _dest_exists(dest_engine, dest_cfg, dest_table), (
        f"{dest_engine}: table {dest_table} created by this run was dropped"
    )
    _drop_dest(dest_engine, dest_cfg, dest_table)
    text = f"{getattr(result, 'message', '')} {getattr(result, 'error', '')}"
    assert "data kept at" in text.lower() and dest_table in text, (
        f"expected kept-for-inspection wording, got: {text[:300]}"
    )
    assert "was dropped" not in text.lower(), text[:300]
    assert "was empty before" not in text.lower(), (
        f"rollback misread a run-created table as pre-existing-empty: {text[:300]}"
    )
    assert getattr(result, "records_transferred", None) is not None and int(
        result.records_transferred
    ) > 0, (
        "a failed job whose rows were kept reports the rows the table holds: "
        f"{getattr(result, 'records_transferred', None)!r}"
    )
