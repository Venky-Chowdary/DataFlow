"""X-46/X-47 LIVE: zero-length binary is a real value, and binary culprits
render hex.

X-46 (loop probe on f92c09e0): MySQL VARBINARY with a ``b''`` row failed the
whole job on every engine — "Empty value cannot coerce to binary … FAIL_JOB",
records 0. Zero-length bytes are valid on VARBINARY/BLOB/bytea and must write
as ``b''``, distinct from NULL. Root cause: ``cell_to_string(b'')`` serializes
to ``""`` (empty base64) and the per-cell coerce rejected ``""`` for the typed
``binary`` transform — same "empty ⇒ missing" collapse as the empty-string
class it was built to stop. The coerce now accepts the truly-empty wire for
binary (whitespace still refuses — it is not a base64 payload), and raw
``bytes`` serialize via ``cell_to_string`` instead of ``str()`` repr.

X-47: binary culprit cells rendered base64 (``AAEC/w==``); operator messages
now render ``0x`` hex for binary-ddl columns.

Live regression: MySQL VARBINARY(32) and PG bytea sources with
``0x000102FF``, ``b''``, NULL, ``0x00..0x1F``, UTF-8 'héllo' bytes and
``0xDEADBEEF`` → {MSSQL, SQLite, PG, DuckDB}. Clean runs assert byte-exact
read-back (b'' == b'', NULL is NULL); tamper runs swap ``b''``→NULL and
NULL→``b''`` and must fail naming the row, with hex values in the message.
Skips when engines are unreachable (DATAFLOW_X46_LIVE).
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

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

pytestmark = pytest.mark.skipif(
    not os.getenv("DATAFLOW_X46_LIVE"),
    reason="live X-46 check: set DATAFLOW_X46_LIVE=1 with reachable engines",
)

_BLOB = bytes(range(0x20))
_VALUES = [b"\x00\x01\x02\xff", b"", None, _BLOB, "héllo".encode(), b"\xde\xad\xbe\xef"]


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


def _pg_conn(pg):
    import psycopg2

    return psycopg2.connect(
        host=pg["host"], port=int(pg["port"]), user=pg["username"],
        password=pg["password"], dbname=pg["database"],
    )


def _mysql_conn(cfg):
    import pymysql

    return pymysql.connect(
        host=cfg["host"], port=int(cfg["port"]), user=cfg["username"],
        password=cfg["password"], database=cfg["database"], autocommit=True,
    )


def _mssql_conn(cfg):
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


def _seed_source(engine: str, cfg: dict[str, Any], table: str) -> None:
    if engine == "postgresql":
        conn = _pg_conn(cfg)
        try:
            cur = conn.cursor()
            cur.execute(f'DROP TABLE IF EXISTS {table}')  # nosec B608
            cur.execute(f'CREATE TABLE {table} (id BIGINT PRIMARY KEY, t BYTEA)')  # nosec B608
            for i, v in enumerate(_VALUES):
                cur.execute(
                    f"INSERT INTO {table} (id, t) VALUES (%s, %s)",  # nosec B608
                    (i + 1, v),
                )
            conn.commit()
        finally:
            conn.close()
        return
    conn = _mysql_conn(cfg)
    try:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        cur.execute(
            f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, t VARBINARY(64))"  # nosec B608
        )
        for i, v in enumerate(_VALUES):
            cur.execute(
                f"INSERT INTO {table} (id, t) VALUES (%s, %s)",  # nosec B608
                (i + 1, v),
            )
    finally:
        conn.close()


def _drop_source(engine: str, cfg: dict[str, Any], table: str) -> None:
    if engine == "postgresql":
        conn = _pg_conn(cfg)
        try:
            conn.cursor().execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
            conn.commit()
        finally:
            conn.close()
        return
    conn = _mysql_conn(cfg)
    try:
        conn.cursor().execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
    finally:
        conn.close()


def _dest_cfg(engine: str, tmp_path: Path) -> dict[str, Any] | None:
    if engine == "sqlite":
        return {
            "host": "", "port": 0, "username": "", "password": "",
            "connection_string": "",
            "database": str(tmp_path / "x46.sqlite"),
        }
    if engine == "duckdb":
        return {
            "host": "", "port": 0, "username": "", "password": "",
            "connection_string": "",
            "database": str(tmp_path / "x46.duckdb"),
        }
    prefix, port = (
        ("DATAFLOW_TEST_MYSQL", 3306) if engine == "mysql"
        else ("DATAFLOW_TEST_PG", 5432) if engine == "postgresql"
        else ("DATAFLOW_TEST_MSSQL", 1433)
    )
    cfg = _cfg(prefix, port)
    if not cfg or not _reachable(cfg["host"], cfg["port"]):
        return None
    return cfg


def _read_t(engine: str, cfg: dict[str, Any], table: str) -> dict[int, Any]:
    """Dest read-back as {id: t} — bytes preserved, NULL distinct from b''."""
    if engine == "sqlite":
        import sqlite3

        conn = sqlite3.connect(cfg["database"])
        try:
            return {
                int(r[0]): (bytes(r[1]) if isinstance(r[1], (bytes, bytearray)) else r[1])
                for r in conn.execute(f"SELECT id, t FROM {table} ORDER BY id")  # nosec B608
            }
        finally:
            conn.close()
    if engine == "duckdb":
        import duckdb

        conn = duckdb.connect(cfg["database"])
        try:
            return {
                int(r[0]): (bytes(r[1]) if isinstance(r[1], (bytes, bytearray)) else r[1])
                for r in conn.execute(f"SELECT id, t FROM {table} ORDER BY id").fetchall()  # nosec B608
            }
        finally:
            conn.close()
    if engine == "mysql":
        conn = _mysql_conn(cfg)
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT id, t FROM {table} ORDER BY id")  # nosec B608
            return {int(r[0]): r[1] for r in cur.fetchall()}
        finally:
            conn.close()
    if engine == "postgresql":
        conn = _pg_conn(cfg)
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT id, t FROM {table} ORDER BY id")  # nosec B608
            return {int(r[0]): (bytes(r[1]) if isinstance(r[1], memoryview) else r[1]) for r in cur.fetchall()}
        finally:
            conn.close()
    conn = _mssql_conn(cfg)
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT id, t FROM [dbo].[{table}] ORDER BY id")  # nosec B608
        return {
            int(r[0]): (bytes(r[1]) if isinstance(r[1], (bytes, bytearray)) else r[1])
            for r in cur.fetchall()
        }
    finally:
        conn.close()


def _tamper_b64_to_null(engine: str, cfg: dict[str, Any], table: str, *, row: int, to_null: bool) -> None:
    """row's t: b''→NULL or NULL→b'' — the two directions X-46 must catch."""
    new = "NULL" if to_null else "X''"
    qmarks = {
        "sqlite": ("sqlite3", f"UPDATE {table} SET t={new} WHERE id={row}"),
        "duckdb": ("duckdb", f"UPDATE {table} SET t={new} WHERE id={row}"),
        "mysql": ("mysql", f"UPDATE {table} SET t={new} WHERE id={row}"),
        "postgresql": ("pg", f"UPDATE {table} SET t={new} WHERE id={row}"),
        "mssql": ("mssql", f"UPDATE [dbo].[{table}] SET t={new} WHERE id={row}"),
    }
    kind, sql = qmarks[engine]
    if kind == "sqlite3":
        import sqlite3

        conn = sqlite3.connect(cfg["database"])
        conn.execute(sql)  # nosec B608
        conn.commit()
        conn.close()
    elif kind == "duckdb":
        import duckdb

        conn = duckdb.connect(cfg["database"])
        conn.execute(sql)  # nosec B608
        conn.close()
    elif kind == "mysql":
        conn = _mysql_conn(cfg)
        conn.cursor().execute(sql)  # nosec B608
        conn.close()
    elif kind == "pg":
        conn = _pg_conn(cfg)
        conn.cursor().execute(sql)  # nosec B608
        conn.commit()
        conn.close()
    else:
        conn = _mssql_conn(cfg)
        cur = conn.cursor()
        cur.execute(sql)  # nosec B608
        conn.commit()
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


def _run(engine_mod_args: dict[str, Any], tmp_path: Path, dest_table: str) -> Any:
    eng = UniversalTransferEngine()
    return eng.execute_tracked(
        TransferRequest(
            job_id=f"x46-{dest_table}",
            mapping={"auto_map": True},
            sync_mode="overwrite",
            **engine_mod_args,
        ),
        job_id=f"x46-{dest_table}",
    )


@pytest.mark.parametrize("src_engine", ["mysql", "postgresql"])
@pytest.mark.parametrize("dest_engine", ["mssql", "sqlite", "postgresql", "duckdb"])
def test_x46_empty_binary_writes_and_compares_exact(
    src_engine: str, dest_engine: str, tmp_path
):
    """Clean run: b'' writes as zero-length bytes on every engine and the
    job verifies — NULL stays NULL, b'' is not NULL."""
    src_prefix, src_port = (
        ("DATAFLOW_TEST_MYSQL", 3306) if src_engine == "mysql"
        else ("DATAFLOW_TEST_PG", 5432)
    )
    src_cfg = _cfg(src_prefix, src_port)
    if not src_cfg or not _reachable(src_cfg["host"], src_cfg["port"]):
        pytest.skip(f"set {src_prefix}_* (reachable)")
    dest_cfg = _dest_cfg(dest_engine, tmp_path)
    if dest_cfg is None:
        pytest.skip(f"dest {dest_engine} unreachable")

    src_table = f"x46src_{src_engine}"
    dest_table = f"x46_{src_engine}_{dest_engine}"
    _seed_source(src_engine, src_cfg, src_table)
    try:
        res = _run(
            {
                "source": _endpoint(src_engine, src_cfg, src_table),
                "destination": _endpoint(dest_engine, dest_cfg, dest_table),
            },
            tmp_path,
            dest_table,
        )
        assert getattr(res, "success", False), (
            f"clean {src_engine}->{dest_engine} run failed: "
            f"{getattr(res, 'error', None) or getattr(res, 'message', None)}"
        )
        rows = _read_t(dest_engine, dest_cfg, dest_table)
        assert set(rows) == set(range(1, len(_VALUES) + 1)), rows
        for i, v in enumerate(_VALUES):
            got = rows[i + 1]
            if v is None:
                assert got is None, f"row {i + 1}: NULL stored as {got!r}"
            else:
                assert isinstance(got, (bytes, bytearray)), (
                    f"row {i + 1}: expected bytes, got {type(got).__name__} {got!r}"
                )
                assert bytes(got) == v, (
                    f"row {i + 1}: {bytes(got).hex()} != {v.hex()}"
                )
        # The X-46 value: zero-length bytes is a real cell, not a NULL.
        assert rows[2] == b"" and rows[2] is not None
        assert rows[3] is None
    finally:
        _drop_source(src_engine, src_cfg, src_table)


@pytest.mark.parametrize("src_engine", ["mysql", "postgresql"])
@pytest.mark.parametrize("dest_engine", ["mssql", "sqlite", "postgresql", "duckdb"])
@pytest.mark.parametrize("direction", ["empty_to_null", "null_to_empty"])
def test_x46_binary_tamper_detected_and_named(
    src_engine: str, dest_engine: str, direction: str, tmp_path
):
    """Tamper b''→NULL (row 2) or NULL→b'' (row 3) post-write: the job must
    fail verification naming that row — the two values are distinct (X-46),
    and the culprit text renders hex, not base64 (X-47)."""
    src_prefix, src_port = (
        ("DATAFLOW_TEST_MYSQL", 3306) if src_engine == "mysql"
        else ("DATAFLOW_TEST_PG", 5432)
    )
    src_cfg = _cfg(src_prefix, src_port)
    if not src_cfg or not _reachable(src_cfg["host"], src_cfg["port"]):
        pytest.skip(f"set {src_prefix}_* (reachable)")
    dest_cfg = _dest_cfg(dest_engine, tmp_path)
    if dest_cfg is None:
        pytest.skip(f"dest {dest_engine} unreachable")

    src_table = f"x46tsrc_{src_engine}"
    dest_table = f"x46t_{src_engine}_{dest_engine}_{direction}"
    _seed_source(src_engine, src_cfg, src_table)
    tampered = {"empty_to_null": (2, True), "null_to_empty": (3, False)}[direction]

    real_recon = engine_mod.run_reconciliation

    def _tampering_recon(*a: Any, **kw: Any):
        _tamper_b64_to_null(
            dest_engine, dest_cfg, dest_table,
            row=tampered[0], to_null=tampered[1],
        )
        return real_recon(*a, **kw)

    try:
        eng = UniversalTransferEngine()
        with __import__("unittest").mock.patch.object(
            engine_mod, "run_reconciliation", _tampering_recon
        ):
            res = eng.execute_tracked(
                TransferRequest(
                    job_id=f"x46-{dest_table}",
                    mapping={"auto_map": True},
                    sync_mode="overwrite",
                    source=_endpoint(src_engine, src_cfg, src_table),
                    destination=_endpoint(dest_engine, dest_cfg, dest_table),
                ),
                job_id=f"x46-{dest_table}",
            )
        assert not getattr(res, "success", True), (
            f"tampered {src_engine}->{dest_engine} run reported success"
        )
        text = str(getattr(res, "error", "") or getattr(res, "message", ""))
        assert str(tampered[0]) in text, f"culprit row not named: {text}"
        # X-47: binary cells render 0x-hex for the operator, not base64.
        assert "0x" in text or tampered[1], (
            f"binary culprit not rendered as hex: {text}"
        )
    finally:
        _drop_source(src_engine, src_cfg, src_table)
