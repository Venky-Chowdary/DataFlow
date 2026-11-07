"""X-43 LIVE: MySQL DECIMAL(38,10) -> SQLite must store exact decimal text,
not scientific notation (false verification failure on -0.0000000001 -> -1E-10).

Live evidence (loop probe N on f92c09e0): the MySQL->SQLite engine-copy route
bound Decimal via ``str(value)``, which renders small magnitudes in scientific
notation ('-1E-10'). The stored cell then differed literally from the source
text '-0.0000000001' and the L4/L5 key-aligned sample check flagged a real
looking "mismatch" on a clean run — the job failed and dropped the table.
PG->SQLite already stores exact TEXT and passes; MySQL->DuckDB stores a real
DECIMAL and passes.

Fix: the MySQL/Oracle/Mongo/SQLServer/Iceberg -> SQLite copy converters now
bind ``connectors.sqlite_common.sqlite_decimal_bind_text`` — the same
dest-canonical, scale-preserving text the row path binds — never str()/float.
The sample comparator on a TEXT carrier stays strict on purpose: once the
stored text is canonical, strict text equality IS the decimal equality, and a
true tamper still fails.

Coverage per the request:
- MySQL DECIMAL(38,10) -> SQLite clean: passes; exact read-back of all five
  values incl. the storage type of ``t`` (TEXT, plain decimal text).
- MySQL DECIMAL(38,10) -> SQLite tamper: fails, names culprit key 1 column t,
  drops the run-created table (X-41 contract).
- Controls with the same five values: PG NUMERIC(38,10) -> SQLite and
  MySQL DECIMAL(38,10) -> DuckDB, clean + tamper.

Skips when the engines are unreachable. Set DATAFLOW_X43_LIVE=1 plus
DATAFLOW_TEST_{MYSQL,PG}_* to run live.
"""

from __future__ import annotations

import os
import socket
import sys
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
    not os.getenv("DATAFLOW_X43_LIVE"),
    reason="live X-43 check: set DATAFLOW_X43_LIVE=1 with reachable engines",
)

_VALUES = [
    "1234567890123456789012345678.0123456789",
    "-0.0000000001",
    "0",
    None,
    "-9999999999999999999999999999.9999999999",
]


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
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _FakeMongo())


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


def _mysql_conn(cfg: dict[str, Any]):
    import pymysql

    return pymysql.connect(
        host=cfg["host"], port=int(cfg["port"]), user=cfg["username"],
        password=cfg["password"], database=cfg["database"], autocommit=True,
    )


def _pg_conn(cfg: dict[str, Any]):
    import psycopg2

    return psycopg2.connect(
        host=cfg["host"], port=int(cfg["port"]), user=cfg["username"],
        password=cfg["password"], dbname=cfg["database"],
    )


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
        extra={},
    )


def _seed_source(engine: str, cfg: dict[str, Any], table: str) -> None:
    if engine == "mysql":
        conn = _mysql_conn(cfg)
        try:
            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
            cur.execute(
                f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, "
                "t DECIMAL(38,10), label VARCHAR(50))"
            )
            for i, v in enumerate(_VALUES):
                cur.execute(
                    f"INSERT INTO {table} (id, t, label) VALUES (%s, %s, %s)",  # nosec B608
                    (i + 1, v, f"lbl{i + 1}"),
                )
        finally:
            conn.close()
        return
    conn = _pg_conn(cfg)
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


def _drop_source(engine: str, cfg: dict[str, Any], table: str) -> None:
    try:
        if engine == "mysql":
            conn = _mysql_conn(cfg)
            try:
                conn.cursor().execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
            finally:
                conn.close()
            return
        conn = _pg_conn(cfg)
        try:
            conn.autocommit = True
            conn.cursor().execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        finally:
            conn.close()
    except Exception:
        pass


def _read_sqlite(path: str, table: str):
    import sqlite3

    conn = sqlite3.connect(path)
    try:
        rows = conn.execute(
            f"SELECT id, t, label, typeof(t) FROM {table} ORDER BY id"  # nosec B608
        ).fetchall()
        return rows
    finally:
        conn.close()


def _read_duckdb(path: str, table: str):
    import duckdb

    conn = duckdb.connect(path)
    try:
        return conn.execute(
            f"SELECT id, t, label FROM {table} ORDER BY id"  # nosec B608
        ).fetchall()
    finally:
        conn.close()


def _dest_exists(engine: str, cfg: dict[str, Any], table: str) -> bool:
    if engine == "sqlite":
        import sqlite3

        conn = sqlite3.connect(cfg["database"])
        try:
            return conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone() is not None
        finally:
            conn.close()
    import duckdb

    conn = duckdb.connect(cfg["database"])
    try:
        rows = conn.execute("SHOW TABLES").fetchall()
        return any(str(r[0]).lower() == table.lower() for r in rows)
    finally:
        conn.close()


def _tamper_sqlite(path: str, table: str) -> None:
    import sqlite3

    conn = sqlite3.connect(path)
    try:
        conn.execute(f"UPDATE {table} SET t=1.5 WHERE id=1")  # nosec B608
        conn.commit()
    finally:
        conn.close()


def _tamper_duckdb(path: str, table: str) -> None:
    import duckdb

    conn = duckdb.connect(path)
    try:
        conn.execute(f"UPDATE {table} SET t=1.5 WHERE id=1")  # nosec B608
    finally:
        conn.close()


@pytest.mark.parametrize("src_engine", ["mysql", "postgresql"])
def test_x43_sqlite_stores_exact_decimal_text_clean(src_engine, tmp_path):
    """Clean DEC(38,10) -> SQLite must pass with exact plain-text read-back."""
    cfg = _cfg(
        "DATAFLOW_TEST_MYSQL" if src_engine == "mysql" else "DATAFLOW_TEST_PG",
        3306 if src_engine == "mysql" else 5432,
    )
    if not cfg or not _reachable(cfg["host"], cfg["port"]):
        pytest.skip(f"set DATAFLOW_TEST_{'MYSQL' if src_engine == 'mysql' else 'PG'}_* (reachable)")

    src_table = f"x43src_{src_engine}"
    dest_db = tmp_path / "x43_dest.db"
    dest_table = "x43_out"
    _seed_source(src_engine, cfg, src_table)
    try:
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=_endpoint(src_engine, cfg, src_table),
                destination=_endpoint(
                    "sqlite", {"database": str(dest_db)}, dest_table
                ),
                sync_mode="full_refresh_overwrite",
                skip_preflight=True,
            ),
            "0000000000000000000000a1",
        )
    finally:
        _drop_source(src_engine, cfg, src_table)

    assert result.success is True, (
        f"clean {src_engine}->SQLite decimal copy must pass: "
        f"{getattr(result, 'error', '') or getattr(result, 'message', '')}"
    )
    rows = _read_sqlite(str(dest_db), dest_table)
    assert len(rows) == 5
    for i, (rid, t_val, label, t_type) in enumerate(rows):
        assert rid == i + 1
        expected = _VALUES[i]
        if expected is None:
            assert t_val is None
        else:
            # Exact plain decimal text — never scientific notation, never float.
            assert t_type == "text", (
                f"row {rid}: DECIMAL must be stored as exact TEXT, got "
                f"{t_type} value {t_val!r}"
            )
            assert str(t_val) == expected, (
                f"row {rid}: stored {t_val!r} != source {expected!r}"
            )


@pytest.mark.parametrize("src_engine", ["mysql", "postgresql"])
def test_x43_sqlite_tamper_fails_names_key_drops_table(src_engine, monkeypatch, tmp_path):
    """Tampered DEC(38,10) -> SQLite must fail with the culprit named and the
    run-created table dropped — and the failure cause must lead the message
    (X-44): never 'Row count verified …' before the mismatch."""
    cfg = _cfg(
        "DATAFLOW_TEST_MYSQL" if src_engine == "mysql" else "DATAFLOW_TEST_PG",
        3306 if src_engine == "mysql" else 5432,
    )
    if not cfg or not _reachable(cfg["host"], cfg["port"]):
        pytest.skip(f"set DATAFLOW_TEST_{'MYSQL' if src_engine == 'mysql' else 'PG'}_* (reachable)")

    src_table = f"x43tam_{src_engine}"
    dest_db = tmp_path / "x43_tamper.db"
    dest_table = "x43_tampered"
    _seed_source(src_engine, cfg, src_table)
    tampered = {"done": False}
    orig_recon = engine_mod.run_reconciliation

    def _tampering_recon(*args, **kwargs):
        if not tampered["done"]:
            _tamper_sqlite(str(dest_db), dest_table)
            tampered["done"] = True
        return orig_recon(*args, **kwargs)

    monkeypatch.setattr(engine_mod, "run_reconciliation", _tampering_recon)
    try:
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=_endpoint(src_engine, cfg, src_table),
                destination=_endpoint(
                    "sqlite", {"database": str(dest_db)}, dest_table
                ),
                sync_mode="full_refresh_overwrite",
                skip_preflight=True,
            ),
            "0000000000000000000000a2",
        )
    finally:
        _drop_source(src_engine, cfg, src_table)

    assert tampered["done"], "tamper hook never fired before reconcile"
    assert result.success is False, (
        f"tampered {src_engine}->SQLite run must fail verify: {result}"
    )
    text = f"{getattr(result, 'message', '')} {getattr(result, 'error', '')}"
    assert "id='1'" in text or "'id': 1" in text or "key 1" in text.lower(), (
        f"culprit key 1 must be named: {text[:300]}"
    )
    assert not _dest_exists("sqlite", {"database": str(dest_db)}, dest_table), (
        f"{src_engine}: table created by the failed run was left behind"
    )
    # X-44: the failure cause leads; a count-verified note may follow but
    # never precede the mismatch.
    head = text[:160]
    assert "row count verified" not in head.lower(), (
        f"job error must open with the failure cause, got: {head!r}"
    )


@pytest.mark.parametrize("tamper", [False, True])
def test_x43_mysql_duckdb_control(src_engine_control, tamper, monkeypatch, tmp_path):
    """MySQL DEC(38,10) -> DuckDB control: clean passes exact; tamper fails
    naming key 1 and drops the table (X-41 contract on a non-SQLite dest)."""
    cfg = _cfg("DATAFLOW_TEST_MYSQL", 3306)
    if not cfg or not _reachable(cfg["host"], cfg["port"]):
        pytest.skip("set DATAFLOW_TEST_MYSQL_* (reachable)")

    src_table = "x43duck_mysql"
    dest_db = tmp_path / "x43_dest.duckdb"
    dest_table = "x43_duck_out"
    _seed_source("mysql", cfg, src_table)
    tampered = {"done": False}
    orig_recon = engine_mod.run_reconciliation

    def _tampering_recon(*args, **kwargs):
        if tamper and not tampered["done"]:
            _tamper_duckdb(str(dest_db), dest_table)
            tampered["done"] = True
        return orig_recon(*args, **kwargs)

    monkeypatch.setattr(engine_mod, "run_reconciliation", _tampering_recon)
    try:
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=_endpoint("mysql", cfg, src_table),
                destination=_endpoint(
                    "duckdb", {"database": str(dest_db)}, dest_table
                ),
                sync_mode="full_refresh_overwrite",
                skip_preflight=True,
            ),
            "0000000000000000000000a3",
        )
    finally:
        _drop_source("mysql", cfg, src_table)

    if not tamper:
        assert result.success is True, (
            f"clean MySQL->DuckDB control must pass: "
            f"{getattr(result, 'error', '') or getattr(result, 'message', '')}"
        )
        rows = _read_duckdb(str(dest_db), dest_table)
        assert len(rows) == 5
        for i, (rid, t_val, _label) in enumerate(rows):
            expected = _VALUES[i]
            if expected is None:
                assert t_val is None
            else:
                from decimal import Decimal

                assert Decimal(str(t_val)) == Decimal(expected), (
                    f"row {rid}: stored {t_val!r} != source {expected!r}"
                )
        return
    assert tampered["done"]
    assert result.success is False
    text = f"{getattr(result, 'message', '')} {getattr(result, 'error', '')}"
    assert "id='1'" in text or "key 1" in text.lower()
    assert not _dest_exists("duckdb", {"database": str(dest_db)}, dest_table)


@pytest.fixture
def src_engine_control():
    return "mysql"
