"""PG -> MySQL-family COPY into a fresh table completes while another session counts it.

The bug (D302 follow-up): the PG -> MySQL COPY route ran ``LOAD DATA LOCAL``
with ``unique_checks=0`` and ``foreign_key_checks=0``. On MariaDB 10.6+ that,
into a new empty InnoDB table, starts the empty-table bulk insert mode
(MDEV-515 / MDEV-24621). A concurrent ``SELECT COUNT(*)`` on that table (a BI
tool, the UI row count, another job) then hung both statements for good:
``KILL`` did nothing and only a server restart cleared it.

Isolation: the MariaDB leg starts its OWN ``mariadbd`` (own datadir, port and
socket in a fresh temp dir) and never touches a shared server, so a regression
can only freeze that throwaway server, which teardown kills with SIGKILL.
Every client socket has a read timeout and the load is joined with a deadline,
so a regression fails in about ``LOAD_DEADLINE_S`` seconds, not a hung job.
The MySQL leg runs on the CI MySQL 8 service (``MYSQL_*``) and refuses to run
against a MariaDB there.
"""

from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Iterator

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

pytestmark = pytest.mark.timeout(300)

pymysql = pytest.importorskip("pymysql")
psycopg2 = pytest.importorskip("psycopg2")

SOURCE_ROWS = 100_000
#: Each round's load takes ~2-4s here; a wedged one never returns.
LOAD_DEADLINE_S = 45
_PAIRS = [("id", "id"), ("label", "label"), ("n", "n"), ("ts", "ts"), ("amt", "amt")]
_DDLS = ["BIGINT", "VARCHAR(40)", "BIGINT", "DATETIME(6)", "DECIMAL(12,2)"]


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def _pg_settings() -> dict[str, Any]:
    return {
        "host": os.environ.get("PGHOST") or "127.0.0.1",
        "port": int(os.environ.get("PGPORT") or 5432),
        "username": os.environ.get("PGUSER") or "dataflow",
        "password": os.environ.get("PGPASSWORD") or "dataflow",
        "database": os.environ.get("PGDATABASE") or "dataflow",
    }


@pytest.fixture
def pg_source() -> Iterator[dict[str, Any]]:
    cfg = _pg_settings()
    if not _reachable(cfg["host"], cfg["port"]):
        pytest.skip(f"PostgreSQL not reachable on {cfg['host']}:{cfg['port']} (PGHOST/PGPORT)")
    table = "bulk_rd_src_" + uuid.uuid4().hex[:8]
    conn = psycopg2.connect(
        host=cfg["host"], port=cfg["port"], user=cfg["username"],
        password=cfg["password"], dbname=cfg["database"], connect_timeout=5,
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, label varchar(40), '
                "n bigint, ts timestamp, amt numeric(12,2))"
            )
            cur.execute(
                f'INSERT INTO "{table}" SELECT i, \'label-\' || i, i %% 97, '
                "timestamp '2024-01-01' + i * interval '1 second', (i %% 1000) / 7.0 "
                "FROM generate_series(1, %s) AS s(i)",
                (SOURCE_ROWS,),
            )
        yield {**cfg, "table": table}
    finally:
        with conn.cursor() as cur:
            # A wedged load keeps its source snapshot open; never wait on it.
            cur.execute(
                "SELECT pg_terminate_backend(l.pid) FROM pg_locks l "
                "JOIN pg_class c ON c.oid = l.relation "
                "WHERE c.relname = %s AND l.pid <> pg_backend_pid()",
                (table,),
            )
            cur.execute("SET lock_timeout = '10s'")
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.close()


def _mariadb_binaries() -> tuple[str, str] | None:
    bindir = (os.environ.get("DATAFLOW_TEST_MARIADB_BINDIR") or "").strip()
    search = os.pathsep.join(p for p in (bindir, "/usr/sbin", "/usr/bin", os.environ.get("PATH", "")) if p)
    server = shutil.which("mariadbd", path=search)
    install = shutil.which("mariadb-install-db", path=search) or shutil.which(
        "mysql_install_db", path=search
    )
    if not server or not install:
        return None
    return server, install


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture
def private_mariadb() -> Iterator[dict[str, Any]]:
    """A throwaway mariadbd that only this test talks to."""
    found = _mariadb_binaries()
    if found is None:
        pytest.skip(
            "MariaDB not configured (DATAFLOW_TEST_MARIADB_BINDIR): no mariadbd + "
            "mariadb-install-db to start a private server for the bulk-load reader test"
        )
    server, install = found
    # A short path: a unix socket path must fit in 107 bytes.
    root = Path(tempfile.mkdtemp(prefix="dwmdb_", dir="/tmp" if os.path.isdir("/tmp") else None))
    datadir = root / "data"
    port = _free_port()
    sock = root / "mdb.sock"
    cnf = root / "my.cnf"
    lines = [
        "[mariadbd]",
        f"datadir={datadir}",
        f"socket={sock}",
        f"pid-file={root / 'mdb.pid'}",
        f"port={port}",
        "bind-address=127.0.0.1",
        "local-infile=1",
        "innodb_buffer_pool_size=64M",
        "innodb_log_file_size=16M",
        "skip-log-bin",
        f"log-error={root / 'mdb.err'}",
    ]
    if os.geteuid() == 0:
        lines.append("user=root")
    cnf.write_text("\n".join(lines) + "\n")
    init = subprocess.run(
        [install, f"--defaults-file={cnf}", "--auth-root-authentication-method=normal",
         "--skip-test-db"],
        capture_output=True, text=True, timeout=120,
    )
    if init.returncode != 0:
        pytest.fail(f"mariadb-install-db failed: {init.stderr[-500:] or init.stdout[-500:]}")
    proc = subprocess.Popen(
        [server, f"--defaults-file={cnf}"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
    )
    try:
        admin = None
        for _ in range(120):
            if proc.poll() is not None:
                err = (root / "mdb.err").read_text(errors="replace")[-800:]
                pytest.fail(f"private mariadbd exited early: {err}")
            try:
                admin = pymysql.connect(
                    unix_socket=str(sock), user="root", password="", autocommit=True,
                    connect_timeout=2, read_timeout=30,
                )
                break
            except pymysql.err.OperationalError:
                time.sleep(0.25)
        if admin is None:
            pytest.fail("private mariadbd did not accept connections within 30s")
        with admin.cursor() as cur:
            cur.execute("SELECT VERSION()")
            version = str(cur.fetchone()[0])
            cur.execute("CREATE DATABASE dw_bulk_reader")
            for host in ("localhost", "127.0.0.1", "%"):
                cur.execute(f"CREATE USER 'dataflow'@'{host}' IDENTIFIED BY 'dataflow'")
                cur.execute(f"GRANT ALL ON dw_bulk_reader.* TO 'dataflow'@'{host}'")
        admin.close()
        yield {
            "host": "127.0.0.1",
            "port": port,
            "database": "dw_bulk_reader",
            "username": "dataflow",
            "password": "dataflow",
            "version": version,
        }
    finally:
        # SIGKILL on purpose: a wedged server ignores a clean shutdown.
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            proc.kill()
        proc.wait(timeout=30)
        shutil.rmtree(root, ignore_errors=True)


@pytest.fixture
def ci_mysql() -> dict[str, Any]:
    cfg = {
        "host": os.environ.get("MYSQL_HOST") or "127.0.0.1",
        "port": int(os.environ.get("MYSQL_PORT") or 3306),
        "username": os.environ.get("MYSQL_USER") or "dataflow",
        "password": os.environ.get("MYSQL_PASSWORD") or "dataflow",
        "database": os.environ.get("MYSQL_DATABASE") or "dataflow",
    }
    if not _reachable(cfg["host"], cfg["port"]):
        pytest.skip(f"MySQL not reachable on {cfg['host']}:{cfg['port']} (MYSQL_HOST/MYSQL_PORT)")
    try:
        conn = pymysql.connect(
            host=cfg["host"], port=cfg["port"], user=cfg["username"], password=cfg["password"],
            database=cfg["database"], connect_timeout=5, read_timeout=30,
        )
    except pymysql.err.OperationalError as exc:
        pytest.skip(f"MySQL not reachable as {cfg['username']} on {cfg['port']}: {exc}")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT VERSION(), @@GLOBAL.local_infile")
            version, local_infile = cur.fetchone()
    finally:
        conn.close()
    if "mariadb" in str(version).lower():
        # Never race a reader against a shared MariaDB: a regression would
        # freeze it for everyone. The MariaDB leg runs on its own server.
        pytest.skip(
            f"MySQL leg needs MySQL on {cfg['port']}, found MariaDB {version}; "
            "the MariaDB leg runs on a private server"
        )
    if str(local_infile).strip().lower() not in {"1", "on", "true"}:
        pytest.fail(f"MySQL on {cfg['port']} has local_infile=OFF; the COPY route needs it")
    return {**cfg, "version": str(version)}


def _load_while_counting(source: dict[str, Any], dest: dict[str, Any]) -> dict[str, Any]:
    """One COPY into a new table while another session polls COUNT(*) on it."""
    from services.copy_pg_mysql import copy_postgres_to_mysql

    table = "bulk_rd_" + uuid.uuid4().hex[:8]
    stop = threading.Event()
    seen = {"counts": 0, "missing": 0, "reader_error": "", "result": None, "error": None}

    def _reader() -> None:
        conn = pymysql.connect(
            host=dest["host"], port=dest["port"], user=dest["username"],
            password=dest["password"], database=dest["database"], autocommit=True,
            connect_timeout=5, read_timeout=LOAD_DEADLINE_S + 15,
        )
        try:
            # Tight loop: the reader is on the empty table when LOAD DATA starts,
            # which is the moment the MariaDB bulk insert mode deadlocked with it.
            while not stop.is_set():
                try:
                    with conn.cursor() as cur:
                        cur.execute(f"SELECT COUNT(*) FROM `{table}`")  # nosec B608
                        cur.fetchone()
                    seen["counts"] += 1
                except pymysql.err.ProgrammingError:
                    seen["missing"] += 1  # not created yet
        except Exception as exc:  # noqa: BLE001 - reported by the assertion below
            seen["reader_error"] = f"{type(exc).__name__}: {exc}"
        finally:
            conn.close()

    def _load() -> None:
        try:
            seen["result"] = copy_postgres_to_mysql(
                source_cfg={k: source[k] for k in ("host", "port", "username", "password", "database")},
                source_schema="public",
                source_table=source["table"],
                dest_cfg={k: dest[k] for k in ("host", "port", "username", "password", "database")},
                dest_table=table,
                pairs=_PAIRS,
                mysql_ddls=_DDLS,
                replace_destination=True,
            )
        except BaseException as exc:  # noqa: BLE001 - reported by the assertion below
            seen["error"] = exc

    reader = threading.Thread(target=_reader, daemon=True)
    loader = threading.Thread(target=_load, daemon=True)
    reader.start()
    time.sleep(0.2)
    started = time.monotonic()
    loader.start()
    loader.join(LOAD_DEADLINE_S)
    elapsed = time.monotonic() - started
    stop.set()
    if loader.is_alive():
        pytest.fail(
            f"COPY into new table `{table}` did not finish in {LOAD_DEADLINE_S}s while "
            f"another session ran SELECT COUNT(*) on it ({seen['counts']} counts done): "
            f"the server is wedged ({dest['version']}). On MariaDB 10.6+ this is the "
            "empty-table bulk insert mode (unique_checks=0 + foreign_key_checks=0)."
        )
    reader.join(LOAD_DEADLINE_S)
    assert not reader.is_alive(), "COUNT(*) reader still blocked after the load finished"
    if seen["error"] is not None:
        raise AssertionError(f"COPY failed: {seen['error']!r}") from seen["error"]
    assert not seen["reader_error"], seen["reader_error"]
    result = seen["result"]
    assert result.source_rows == SOURCE_ROWS
    assert result.target_rows == SOURCE_ROWS
    # The reader must actually have overlapped the load, or this proves nothing.
    assert seen["counts"] > 0, "reader never counted the destination table"

    conn = pymysql.connect(
        host=dest["host"], port=dest["port"], user=dest["username"], password=dest["password"],
        database=dest["database"], autocommit=True, connect_timeout=5, read_timeout=30,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")  # nosec B608
            assert int(cur.fetchone()[0]) == SOURCE_ROWS
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
    finally:
        conn.close()
    return {"elapsed_s": elapsed, "counts": seen["counts"], "workers": result.source_snapshot.get("copy_workers")}


#: One worker is the deterministic trigger (3/3 wedged before the fix on
#: MariaDB 10.11.14); the auto run covers the parallel-shard route.
_ROUNDS = ("1", "1", "auto")


def test_mariadb_copy_into_new_table_completes_under_concurrent_count(
    pg_source, private_mariadb, monkeypatch
):
    for workers in _ROUNDS:
        monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", workers)
        _load_while_counting(pg_source, private_mariadb)


def test_mysql_copy_into_new_table_completes_under_concurrent_count(
    pg_source, ci_mysql, monkeypatch
):
    for workers in _ROUNDS:
        monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", workers)
        _load_while_counting(pg_source, ci_mysql)
