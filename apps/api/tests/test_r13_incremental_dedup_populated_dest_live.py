"""R-13 LIVE: incremental_deduped into a populated destination, with
row-by-row read-back.

Regression for the window-B defect (first red at f4e9157c): a second
incremental run against a destination that already held the first load
failed verification — the verify digested the WHOLE populated destination
against the incremental source slice and reported the untouched id=1 as a
value mismatch ("dest_before 500 + inserts 1 … 501 rows for 1 source row").
That is a false failure on a correct sync: a cursor incremental appends the
new key and leaves existing rows untouched by contract.

Legs:
- SQLite -> SQLite (always live): run 1 loads 100 rows; run 2 appends
  ids 101-105 after an in-place UPDATE below the watermark — the honest
  contract is id=1 stays stale, ids 101-105 land, the job SUCCEEDS, and
  every destination row reads back equal to its expected value.
- MySQL -> MySQL (DATAFLOW_R13_LIVE=1 + DATAFLOW_TEST_MYSQL_*): identical
  scenario on a real MySQL 8 pair.

Row-by-row read-back: assert dest[id] == expected for EVERY id — for id=1
the expected value is the pre-update amount (cursor semantics), for all
other ids the source amount. count == distinct ids.
"""

from __future__ import annotations

import os
import socket
import sqlite3
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
from services import sync_cursor as sync_cursor_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


class _FakeMongo:
    def __init__(self):
        self.jobs: dict[str, dict] = {}

    def get_job(self, job_id: str) -> dict | None:
        return self.jobs.get(job_id)

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        self.jobs.setdefault(job_id, {})
        self.jobs[job_id].update(kwargs)
        self.jobs[job_id]["status"] = status
        return True

    def update_job_fields(self, job_id: str, fields: dict) -> bool:
        self.jobs.setdefault(job_id, {}).update(fields)
        return True


@pytest.fixture(autouse=True)
def _patch_mongodb_service(monkeypatch):
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _FakeMongo())


def _request(src_ep, dst_ep, table: str) -> TransferRequest:
    return TransferRequest(
        source=src_ep,
        destination=dst_ep,
        sync_mode="incremental_deduped",
        stream_contracts=[
            {
                "selected": True,
                "name": table,
                "sync_mode": "incremental_deduped",
                "primary_key": "id",
                "cursor_field": "id",
            }
        ],
        skip_preflight=True,
    )


def _expected_map(
    rows: int, updated_id1: bool, *, dest_engine: str
) -> dict[int, tuple[str, Any]]:
    out: dict[int, tuple[str, Any]] = {}

    def _active(i: int):
        # SQLite coerces declared-bool 'true'/'false' to INTEGER 1/0;
        # MySQL keeps the VARCHAR literal.
        v = "true" if i % 2 == 1 else "false"
        return (1 if v == "true" else 0) if dest_engine == "sqlite" else v

    for i in range(1, rows + 1):
        if updated_id1 and i == 1:
            # Below-watermark UPDATE is invisible to the id cursor — the
            # honest contract keeps the pre-update value.
            out[1] = ("1.50", _active(1))
        else:
            out[i] = (f"{i * 1.5:.2f}", _active(i))
    return out


def _isolate_cursor_store(tmp_path: Path) -> None:
    sync_cursor_mod.STORE_PATH = tmp_path / "sync_cursors.json"
    if sync_cursor_mod.STORE_PATH.exists():
        sync_cursor_mod.STORE_PATH.unlink()


# ---------------------------------------------------------------- sqlite leg


def test_r13_sqlite_incremental_dedup_into_populated_dest(tmp_path):
    _isolate_cursor_store(tmp_path)
    src = tmp_path / "r13_src.db"
    dst = tmp_path / "r13_dst.db"
    conn = sqlite3.connect(src)
    conn.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount TEXT, active TEXT)"
    )
    for i in range(1, 101):
        conn.execute(
            "INSERT INTO orders VALUES (?, ?, ?)",
            (i, f"{i * 1.5:.2f}", "true" if i % 2 == 1 else "false"),
        )
    conn.commit()
    conn.close()

    source = EndpointConfig(
        kind="database", format="sqlite", database=str(src), table="orders"
    )
    destination = EndpointConfig(
        kind="database", format="sqlite", database=str(dst), table="orders_out"
    )
    engine = UniversalTransferEngine()

    r1 = engine.execute_tracked(
        _request(source, destination, "orders"),
        "0000000000000000000000b1",
    )
    assert r1.success is True, f"initial incremental load failed: {r1.error}"

    # Mutate the source: new keys above the watermark + an in-place update
    # below it (cursor-honest: the update is NOT re-read).
    conn = sqlite3.connect(src)
    conn.execute("UPDATE orders SET amount = '999999.99' WHERE id = 1")
    for i in range(101, 106):
        conn.execute(
            "INSERT INTO orders VALUES (?, ?, ?)",
            (i, f"{i * 1.5:.2f}", "true" if i % 2 == 1 else "false"),
        )
    conn.commit()
    conn.close()

    r2 = engine.execute_tracked(
        _request(source, destination, "orders"),
        "0000000000000000000000b1",
    )
    assert r2.success is True, (
        "incremental dedup into a populated destination must not false-fail "
        f"(R-13 window B): {getattr(r2, 'error', '') or getattr(r2, 'message', '')}"
    )

    # Row-by-row read-back.
    conn = sqlite3.connect(dst)
    rows = {
        r[0]: (r[1], r[2])
        for r in conn.execute(
            "SELECT id, amount, active FROM orders_out"
        ).fetchall()
    }
    conn.close()
    expected = _expected_map(105, updated_id1=True, dest_engine="sqlite")
    assert len(rows) == 105, f"dest count {len(rows)} != 105"
    for rid, (amount, active) in expected.items():
        assert rid in rows, f"id {rid} missing from dest"
        assert rows[rid] == (amount, active), (
            f"id {rid}: dest {rows[rid]!r} != expected {(amount, active)!r}"
        )


# ------------------------------------------------------------------ mysql leg


def _mysql_cfg() -> dict[str, Any] | None:
    host = os.getenv("DATAFLOW_TEST_MYSQL_HOST", "")
    if not host:
        return None
    return {
        "host": host,
        "port": int(os.getenv("DATAFLOW_TEST_MYSQL_PORT", "3306")),
        "database": os.getenv("DATAFLOW_TEST_MYSQL_DB", ""),
        "username": os.getenv("DATAFLOW_TEST_MYSQL_USER", ""),
        "password": os.getenv("DATAFLOW_TEST_MYSQL_PASSWORD", ""),
        "connection_string": os.getenv("DATAFLOW_TEST_MYSQL_CONNECTION_STRING", ""),
        "ssl": False,
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


def test_r13_mysql_incremental_dedup_into_populated_dest(tmp_path):
    if not os.getenv("DATAFLOW_R13_LIVE"):
        pytest.skip("live MySQL check: set DATAFLOW_R13_LIVE=1")
    cfg = _mysql_cfg()
    if not cfg or not _reachable(cfg["host"], cfg["port"]):
        pytest.skip("set DATAFLOW_TEST_MYSQL_* (reachable)")

    _isolate_cursor_store(tmp_path)
    src_t = "r13_src_mysql"
    dst_t = "r13_dst_mysql"
    conn = _mysql_conn(cfg)
    try:
        cur = conn.cursor()
        for t in (src_t, dst_t):
            cur.execute(f"DROP TABLE IF EXISTS {t}")  # nosec B608
        cur.execute(
            f"CREATE TABLE {src_t} (id BIGINT PRIMARY KEY, "  # nosec B608
            "amount VARCHAR(50), active VARCHAR(10))"
        )
        for i in range(1, 101):
            cur.execute(
                f"INSERT INTO {src_t} VALUES (%s, %s, %s)",  # nosec B608
                (i, f"{i * 1.5:.2f}", "true" if i % 2 == 1 else "false"),
            )
    finally:
        conn.close()

    source = EndpointConfig(
        kind="database",
        format="mysql",
        host=cfg["host"], port=int(cfg["port"]),
        database=cfg["database"], table=src_t,
        username=cfg["username"], password=cfg["password"],
        connection_string=cfg["connection_string"], ssl=False,
    )
    destination = EndpointConfig(
        kind="database",
        format="mysql",
        host=cfg["host"], port=int(cfg["port"]),
        database=cfg["database"], table=dst_t,
        username=cfg["username"], password=cfg["password"],
        connection_string=cfg["connection_string"], ssl=False,
    )
    engine = UniversalTransferEngine()

    try:
        r1 = engine.execute_tracked(
            _request(source, destination, src_t),
            "0000000000000000000000b2",
        )
        assert r1.success is True, f"initial MySQL load failed: {r1.error}"

        conn = _mysql_conn(cfg)
        try:
            cur = conn.cursor()
            cur.execute(
                f"UPDATE {src_t} SET amount='999999.99' WHERE id=1"  # nosec B608
            )
            for i in range(101, 106):
                cur.execute(
                    f"INSERT INTO {src_t} VALUES (%s, %s, %s)",  # nosec B608
                    (i, f"{i * 1.5:.2f}", "true" if i % 2 == 1 else "false"),
                )
        finally:
            conn.close()

        r2 = engine.execute_tracked(
            _request(source, destination, src_t),
            "0000000000000000000000b2",
        )
        assert r2.success is True, (
            "incremental dedup into a populated MySQL destination must not "
            f"false-fail (R-13 window B): "
            f"{getattr(r2, 'error', '') or getattr(r2, 'message', '')}"
        )

        conn = _mysql_conn(cfg)
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT id, amount, active FROM {dst_t}"  # nosec B608
            )
            rows = {r[0]: (str(r[1]), str(r[2])) for r in cur.fetchall()}
        finally:
            conn.close()
        expected = _expected_map(105, updated_id1=True, dest_engine="mysql")
        assert len(rows) == 105, f"dest count {len(rows)} != 105"
        for rid, (amount, active) in expected.items():
            assert int(rid) in rows, f"id {rid} missing from dest"
            assert rows[int(rid)] == (amount, active), (
                f"id {rid}: dest {rows[int(rid)]!r} != expected "
                f"{(amount, active)!r}"
            )
    finally:
        conn = _mysql_conn(cfg)
        try:
            cur = conn.cursor()
            for t in (src_t, dst_t):
                cur.execute(f"DROP TABLE IF EXISTS {t}")  # nosec B608
        finally:
            conn.close()
