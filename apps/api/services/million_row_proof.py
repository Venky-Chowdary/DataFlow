"""Million-row Execute proof — one owner for conservation + OLTP discovery.

A rows/s number is not a claim without destination ``COUNT(*)``. This module
is the shared algorithm the PG→MySQL bench, pytest smoke, and proof artifact
all call. It never invents a green COUNT.

Default discovery order matches this product's local pair first
(``127.0.0.1:5432`` / ``3306``), then the older bench ports (``5433`` / ``3307``).
"""

from __future__ import annotations

import json
import os
import socket
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.atomic_file import write_json_atomic

PROOF_SCHEMA = "million_row_proof.v1"


def tcp_open(host: str, port: int, timeout: float = 0.4) -> bool:
    sock = socket.socket()
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def mongo_reachable() -> bool:
    host = os.environ.get("MONGO_HOST", "127.0.0.1")
    port = int(os.environ.get("MONGO_PORT", "27017"))
    return tcp_open(host, port)


def ensure_memory_job_store_if_mongo_down() -> str:
    """Stream refuses to run without a job shell. Use memory when Mongo is down."""
    if mongo_reachable():
        return "mongo"
    os.environ["DATAFLOW_JOB_STORE"] = "memory"
    try:
        import services.mongodb_service as mongo_mod

        existing = getattr(mongo_mod, "_mongodb_service", None)
        if existing is not None and type(existing).__name__ != "MemoryMongoDBService":
            mongo_mod._mongodb_service = None
    except Exception:
        pass
    return "memory"


def _try_pg(host: str, port: int, user: str, password: str, dbname: str) -> dict[str, Any] | None:
    if not tcp_open(host, port):
        return None
    try:
        import psycopg2
    except ImportError:
        return None
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname,
            connect_timeout=3,
        )
        conn.close()
    except Exception:
        return None
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "dbname": dbname,
    }


def _try_mysql(host: str, port: int, user: str, password: str, database: str) -> dict[str, Any] | None:
    if not tcp_open(host, port):
        return None
    try:
        import pymysql
    except ImportError:
        return None
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            connect_timeout=3,
        )
        conn.close()
    except Exception:
        return None
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
    }


def discover_oltp_pair() -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Return (pg, mysql) configs for the first reachable pair, or None."""
    pg_host = os.environ.get("PGHOST", "127.0.0.1")
    mysql_host = os.environ.get("MYSQL_HOST", "127.0.0.1")
    pg_user = os.environ.get("PGUSER", "dataflow")
    pg_password = os.environ.get("PGPASSWORD", "dataflow")
    pg_db = os.environ.get("PGDATABASE", "dataflow")
    mysql_user = os.environ.get("MYSQL_USER", "dataflow")
    mysql_password = os.environ.get("MYSQL_PASSWORD", "dataflow")
    mysql_db = os.environ.get("MYSQL_DATABASE", "dataflow")

    pg_ports = []
    if os.environ.get("PGPORT"):
        pg_ports.append(int(os.environ["PGPORT"]))
    for port in (5432, 5433):
        if port not in pg_ports:
            pg_ports.append(port)

    mysql_ports = []
    if os.environ.get("MYSQL_PORT"):
        mysql_ports.append(int(os.environ["MYSQL_PORT"]))
    for port in (3306, 3307):
        if port not in mysql_ports:
            mysql_ports.append(port)

    pg_users = [(pg_user, pg_password)]
    if (pg_user, pg_password) != ("postgres", os.environ.get("PGPASSWORD", "postgres")):
        pg_users.append(("postgres", os.environ.get("PGPASSWORD", "postgres")))
        pg_users.append(("dataflow", "dataflow"))

    mysql_users = [(mysql_user, mysql_password)]
    if (mysql_user, mysql_password) != ("root", os.environ.get("MYSQL_PASSWORD", "dataflow")):
        mysql_users.append(("root", os.environ.get("MYSQL_PASSWORD", "dataflow")))
        mysql_users.append(("dataflow", "dataflow"))

    pg = None
    for port in pg_ports:
        for user, password in pg_users:
            pg = _try_pg(pg_host, port, user, password, pg_db)
            if pg:
                break
        if pg:
            break

    mysql = None
    for port in mysql_ports:
        for user, password in mysql_users:
            mysql = _try_mysql(mysql_host, port, user, password, mysql_db)
            if mysql:
                break
        if mysql:
            break

    if not pg or not mysql:
        return None
    return pg, mysql


def skip_reason_if_unreachable() -> str | None:
    pair = discover_oltp_pair()
    if pair:
        return None
    pg_open = tcp_open("127.0.0.1", int(os.environ.get("PGPORT", "5432")))
    my_open = tcp_open("127.0.0.1", int(os.environ.get("MYSQL_PORT", "3306")))
    return (
        "PG→MySQL pair unreachable — "
        f"pg5432={tcp_open('127.0.0.1', 5432)} pg5433={tcp_open('127.0.0.1', 5433)} "
        f"my3306={tcp_open('127.0.0.1', 3306)} my3307={tcp_open('127.0.0.1', 3307)} "
        f"env_pg_open={pg_open} env_mysql_open={my_open}. "
        "No invented COUNT(*)."
    )


def row_conservation(
    *,
    source_rows: int,
    dest_count: int,
    rejected_rows: int = 0,
) -> dict[str, Any]:
    """Fail-closed conservation for a clean append fixture.

    ``clean`` is dest == source and rejected == 0 (the 1M honesty bar).
    ``balanced`` is dest + rejected == source (quarantine-aware, not silent drop).
    """
    source_rows = int(source_rows)
    dest_count = int(dest_count)
    rejected_rows = int(rejected_rows or 0)
    accounted = dest_count + rejected_rows
    clean = dest_count == source_rows and rejected_rows == 0
    balanced = accounted == source_rows
    return {
        "source_rows": source_rows,
        "dest_count": dest_count,
        "rejected_rows": rejected_rows,
        "accounted": accounted,
        "clean": clean,
        "balanced": balanced,
        "verdict": "OK" if clean else ("BALANCED_QUARANTINE" if balanced else "MISMATCH"),
    }


def assert_clean_conservation(report: dict[str, Any]) -> None:
    if not report.get("clean"):
        raise AssertionError(
            "row conservation failed: "
            f"source={report.get('source_rows')} dest={report.get('dest_count')} "
            f"rejected={report.get('rejected_rows')} verdict={report.get('verdict')}"
        )


def write_million_proof(path: Path | str, payload: dict[str, Any]) -> Path:
    dest = Path(path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    body = {
        "schema": PROOF_SCHEMA,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        **payload,
    }
    write_json_atomic(dest, body)
    return dest
