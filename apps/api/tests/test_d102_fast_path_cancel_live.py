"""D102 — cancel_job reported cancelled while a COPY fast path kept writing.

The shared cancel probe (``services.cancel_check``) was polled by the row
writers and several COPY fast paths, but MySQL→SQLite, MySQL/SQLite→SQL
Server and SQL Server→PG/MySQL/SQLite streamed the whole source without ever
asking. A job cancelled mid-copy landed every row.

Live MySQL / SQL Server: cancel at the first batch boundary must stop the
copy, roll the destination back, and land no rows.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import uuid
from pathlib import Path

import pytest

from services.cancel_check import set_cancel_probe
from services.dest_precount import destination_row_count
from services.error_handling import TransferCancelled


def _reachable(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            return True
    except OSError:
        return False


def _seed_sqlite(path: Path, table: str, rows: int) -> None:
    conn = sqlite3.connect(path)
    conn.execute(f'CREATE TABLE "{table}" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)')
    conn.executemany(
        f'INSERT INTO "{table}" (id, label) VALUES (?, ?)',
        [(i, f"r{i}") for i in range(1, rows + 1)],
    )
    conn.commit()
    conn.close()


@pytest.fixture()
def cancel_at_first_batch():
    calls = {"n": 0}

    def probe(rows_written=None):
        calls["n"] += 1
        raise TransferCancelled("Transfer cancelled by user", rows_written=rows_written)

    set_cancel_probe(probe)
    try:
        yield calls
    finally:
        set_cancel_probe(None)


def _mysql_cfg() -> dict:
    return {
        "type": "mysql",
        "host": "127.0.0.1",
        "port": 3306,
        "database": "dataflow",
        "username": os.environ.get("MYSQL_USER", "dataflow"),
        "password": os.environ.get("MYSQL_PASSWORD", "dataflow"),
    }


def test_mysql_to_sqlite_copy_stops_on_cancel(tmp_path, cancel_at_first_batch):
    if not _reachable(3306):
        pytest.skip("MySQL 3306 not reachable")
    pymysql = pytest.importorskip("pymysql")
    from services.copy_mysql_sqlite import copy_mysql_to_sqlite

    cfg = _mysql_cfg()
    src = f"d102_src_{uuid.uuid4().hex[:8]}"
    conn = pymysql.connect(
        host=cfg["host"], port=cfg["port"], user=cfg["username"],
        password=cfg["password"], database=cfg["database"], autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE `{src}` (id BIGINT PRIMARY KEY, label TEXT)")
            cur.executemany(
                f"INSERT INTO `{src}` VALUES (%s, %s)",
                [(i, f"r{i}") for i in range(1, 501)],
            )
        dest_db = tmp_path / "dst.db"
        with pytest.raises(TransferCancelled):
            copy_mysql_to_sqlite(
                source_cfg={**cfg, "table": src},
                source_table=src,
                dest_cfg={"type": "sqlite", "database": str(dest_db), "table": "dst_t"},
                dest_table="dst_t",
                pairs=[("id", "id"), ("label", "label")],
                sqlite_ddls=["INTEGER", "TEXT"],
                replace_destination=True,
            )
        assert cancel_at_first_batch["n"] >= 1
        landed = sqlite3.connect(dest_db).execute(
            "SELECT name FROM sqlite_master WHERE name='dst_t'"
        ).fetchall()
        if landed:
            assert sqlite3.connect(dest_db).execute(
                "SELECT COUNT(*) FROM dst_t"
            ).fetchone()[0] == 0
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        conn.close()


def _ss_cfg() -> dict:
    return {
        "type": "sqlserver",
        "host": "127.0.0.1",
        "port": 1433,
        "database": "dataflow",
        "username": "sa",
        "password": os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD", ""),
        "schema": "dbo",
        "trust_server_certificate": True,
        "encrypt": "yes",
    }


def _ss_drop(table: str) -> None:
    pyodbc = pytest.importorskip("pyodbc")
    cfg = _ss_cfg()
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,1433;"
        f"DATABASE=dataflow;UID=sa;PWD={cfg['password']};TrustServerCertificate=yes",
        autocommit=True,
    )
    try:
        conn.execute(
            f"IF OBJECT_ID(N'dbo.{table}', 'U') IS NOT NULL DROP TABLE dbo.[{table}]"
        )
    finally:
        conn.close()


def test_sqlite_to_sqlserver_copy_stops_on_cancel(tmp_path, cancel_at_first_batch):
    if not _reachable(1433) or not os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD"):
        pytest.skip("SQL Server 1433 not reachable")
    from services.copy_sqlite_sqlserver import copy_sqlite_to_sqlserver

    src_db = tmp_path / "src.db"
    _seed_sqlite(src_db, "src_t", 500)
    dest = f"d102_dst_{uuid.uuid4().hex[:8]}"
    try:
        with pytest.raises(TransferCancelled):
            copy_sqlite_to_sqlserver(
                source_cfg={"type": "sqlite", "database": str(src_db), "table": "src_t"},
                source_table="src_t",
                dest_cfg=_ss_cfg(),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                sqlserver_ddls=["BIGINT", "NVARCHAR(MAX)"],
                replace_destination=True,
            )
        assert cancel_at_first_batch["n"] >= 1
        n = destination_row_count("sqlserver", _ss_cfg(), schema="dbo", table_name=dest)
        assert not n  # table rolled back / dropped, or present with 0 rows
    finally:
        _ss_drop(dest)


def test_sqlite_to_mongo_copy_stops_on_cancel(tmp_path, cancel_at_first_batch):
    """Every route into Mongo lands through insert_many_documents, so one poll
    there covers SQLite/MySQL/PG/SQL Server/Oracle/Iceberg/S3 -> Mongo."""
    if not _reachable(27017):
        pytest.skip("MongoDB 27017 not reachable")
    pymongo = pytest.importorskip("pymongo")
    from services.copy_sqlite_mongo import copy_sqlite_to_mongo

    src_db = tmp_path / "src.db"
    _seed_sqlite(src_db, "src_t", 500)
    dest = f"d102_mongo_{uuid.uuid4().hex[:8]}"
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
    try:
        with pytest.raises(TransferCancelled):
            copy_sqlite_to_mongo(
                source_cfg={"type": "sqlite", "format": "sqlite",
                            "database": str(src_db), "table": "src_t"},
                source_table="src_t",
                dest_cfg={"type": "mongodb", "host": "127.0.0.1", "port": 27017,
                          "database": "dataflow", "table": dest, "collection": dest},
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                mongo_ddls=["INTEGER", "TEXT"],
                replace_destination=True,
            )
        assert cancel_at_first_batch["n"] >= 1
        assert client["dataflow"][dest].count_documents({}) == 0
    finally:
        client["dataflow"][dest].drop()
        client.close()
