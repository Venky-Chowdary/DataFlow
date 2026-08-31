"""Live `mysql -> mysql` CDC on one instance does not wait on its own snapshot.

The snapshot's `FLUSH TABLES WITH READ LOCK` freezes every write on the server.
When the destination lives on that same server, a lock held into the dump blocks
the run's own writes until `lock_wait_timeout` — `(1205, 'Lock wait timeout
exceeded')`. This proves the lock is gone before the dump on the happy path
*and* when pinning the read view fails, and reads the destination back on a
connection the transfer engine never touched.

Skips when the compose MySQL with ROW binlog is not reachable.
"""

from __future__ import annotations

import socket
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.mysql_conn import get_connection  # noqa: E402
from src.transfer.cdc_transfer import run_cdc_database_transfer  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402
from tests.helpers.live_env import mysql_creds  # noqa: E402

ROWS = 5_000

CFG = mysql_creds()


def _connect():
    return get_connection(
        host=CFG["host"],
        port=int(CFG["port"]),
        database=CFG["database"],
        username=CFG["username"],
        password=CFG["password"],
        connection_string="",
        ssl=False,
    )


def _mysql_binlog_ready() -> bool:
    try:
        import pymysqlreplication  # noqa: F401
    except ImportError:
        return False
    try:
        with socket.create_connection((CFG["host"], int(CFG["port"])), timeout=1):
            pass
    except OSError:
        return False
    try:
        conn = _connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW VARIABLES LIKE 'binlog_format'")
                row = cur.fetchone()
                return bool(row) and str(row[1]).upper() == "ROW"
        finally:
            conn.close()
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _mysql_binlog_ready(),
    reason="compose MySQL with ROW binlog + pymysqlreplication not reachable",
)


def _exec(sql: str, params: Any = None, many: bool = False) -> None:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            if many:
                cur.executemany(sql, params)
            elif params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def _seed(table: str) -> None:
    _exec(f"DROP TABLE IF EXISTS {table}")
    _exec(
        f"CREATE TABLE {table} (id INT PRIMARY KEY, name VARCHAR(64), "
        "amount DECIMAL(12,2))"
    )
    _exec(
        f"INSERT INTO {table} (id, name, amount) VALUES (%s, %s, %s)",
        [(i, f"n{i:06d}", f"{i / 7:.2f}") for i in range(ROWS)],
        many=True,
    )


def _independent_count(table: str) -> int:
    """Count on a connection the transfer engine never used."""
    import pymysql

    conn = pymysql.connect(
        host=CFG["host"],
        port=int(CFG["port"]),
        user=CFG["username"],
        password=CFG["password"],
        database=CFG["database"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            row = cur.fetchone()
            return int(row[0]) if row else 0
    finally:
        conn.close()


def _run_transfer(src_table: str, dst_table: str, job_id: str) -> tuple[int, list[str]]:
    src = EndpointConfig(
        kind="database",
        format="mysql",
        table=src_table,
        host=CFG["host"],
        port=int(CFG["port"]),
        database=CFG["database"],
        username=CFG["username"],
        password=CFG["password"],
        connection_string="",
        ssl=False,
    )
    dst = EndpointConfig(
        kind="database",
        format="mysql",
        table=dst_table,
        host=CFG["host"],
        port=int(CFG["port"]),
        database=CFG["database"],
        username=CFG["username"],
        password=CFG["password"],
        connection_string="",
        ssl=False,
    )
    mappings = [
        {"source": "id", "target": "id", "source_type": "INT", "target_type": "INT"},
        {
            "source": "name",
            "target": "name",
            "source_type": "VARCHAR(64)",
            "target_type": "VARCHAR(64)",
        },
        {
            "source": "amount",
            "target": "amount",
            "source_type": "DECIMAL(12,2)",
            "target_type": "DECIMAL(12,2)",
        },
    ]
    schema = {"id": "INT", "name": "VARCHAR(64)", "amount": "DECIMAL(12,2)"}
    stream = [
        {
            "name": src_table,
            "selected": True,
            "snapshot_mode": "initial",
            "primary_key": "id",
            "sync_mode": "cdc",
        }
    ]
    rows, ddl, _summary, _cols = run_cdc_database_transfer(
        src,
        dst,
        mappings,
        schema,
        sync_mode="cdc",
        stream_contracts=stream,
        job_id=job_id,
    )
    return rows, ddl


def _cleanup(*tables: str) -> None:
    for table in tables:
        try:
            _exec(f"DROP TABLE IF EXISTS {table}")
        except Exception:
            pass


def test_mysql_to_mysql_cdc_snapshot_does_not_block_its_own_destination() -> None:
    suffix = uuid.uuid4().hex[:8]
    src_table = f"cdc_same_src_{suffix}"
    dst_table = f"cdc_same_dst_{suffix}"
    _seed(src_table)
    try:
        rows, ddl = _run_transfer(src_table, dst_table, f"mysql-same-{suffix}")
        assert any("CDC(binlog)" in line for line in ddl), ddl
        assert rows == ROWS, f"snapshot wrote {rows} of {ROWS}"
        assert _independent_count(dst_table) == ROWS
    finally:
        _cleanup(src_table, dst_table)


def test_a_concurrent_writer_is_not_frozen_by_the_snapshot() -> None:
    """A third session keeps writing while the snapshot runs."""
    suffix = uuid.uuid4().hex[:8]
    src_table = f"cdc_free_src_{suffix}"
    dst_table = f"cdc_free_dst_{suffix}"
    other_table = f"cdc_free_other_{suffix}"
    _seed(src_table)
    _exec(f"DROP TABLE IF EXISTS {other_table}")
    _exec(f"CREATE TABLE {other_table} (id INT PRIMARY KEY)")

    stop = threading.Event()
    worst = 0.0
    writes = 0
    errors: list[str] = []

    def writer() -> None:
        nonlocal worst, writes
        import pymysql

        conn = pymysql.connect(
            host=CFG["host"],
            port=int(CFG["port"]),
            user=CFG["username"],
            password=CFG["password"],
            database=CFG["database"],
            autocommit=True,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SET SESSION lock_wait_timeout = 5")
            i = 0
            while not stop.is_set():
                started = time.time()
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"INSERT INTO {other_table} (id) VALUES (%s)", (i,)
                        )
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"{type(exc).__name__}: {exc}")
                    return
                worst = max(worst, time.time() - started)
                writes += 1
                i += 1
                time.sleep(0.01)
        finally:
            conn.close()

    thread = threading.Thread(target=writer, daemon=True)
    thread.start()
    try:
        rows, _ddl = _run_transfer(src_table, dst_table, f"mysql-free-{suffix}")
        assert rows == ROWS
    finally:
        stop.set()
        thread.join(timeout=30)
        _cleanup(src_table, dst_table, other_table)

    assert not errors, f"concurrent writer was refused: {errors[:2]}"
    assert writes > 0, "concurrent writer never ran"
    # A snapshot that held the global read lock through the dump would push this
    # into seconds (and into 1205 once lock_wait_timeout is short).
    assert worst < 2.0, f"a concurrent write waited {worst:.2f}s on the snapshot"


def test_snapshot_completes_when_the_read_view_cannot_be_pinned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The lock is released even when `START TRANSACTION` fails on the server."""
    from connectors import mysql_change_stream as mcs

    real_get_connection = mcs.get_connection

    class RefusingTxnConnection:
        """Real connection; only the read-view pin is made to fail."""

        def __init__(self, inner: Any) -> None:
            self._inner = inner

        def cursor(self) -> Any:
            inner_cursor = self._inner.cursor()

            class Cursor:
                def __enter__(self) -> Any:
                    inner_cursor.__enter__()
                    return self

                def __exit__(self, *exc: object) -> None:
                    inner_cursor.__exit__(*exc)

                def execute(self, sql: str, *args: Any) -> Any:
                    if sql.strip().upper().startswith("START TRANSACTION"):
                        raise RuntimeError("injected: read view cannot be pinned")
                    return inner_cursor.execute(sql, *args)

                def __getattr__(self, name: str) -> Any:
                    return getattr(inner_cursor, name)

            return Cursor()

        def __getattr__(self, name: str) -> Any:
            return getattr(self._inner, name)

    def wrapped(**kwargs: Any) -> Any:
        return RefusingTxnConnection(real_get_connection(**kwargs))

    monkeypatch.setattr(mcs, "get_connection", wrapped)

    suffix = uuid.uuid4().hex[:8]
    src_table = f"cdc_nopin_src_{suffix}"
    dst_table = f"cdc_nopin_dst_{suffix}"
    _seed(src_table)
    try:
        rows, _ddl = _run_transfer(src_table, dst_table, f"mysql-nopin-{suffix}")
        assert rows == ROWS, f"snapshot wrote {rows} of {ROWS}"
        assert _independent_count(dst_table) == ROWS
    finally:
        _cleanup(src_table, dst_table)
