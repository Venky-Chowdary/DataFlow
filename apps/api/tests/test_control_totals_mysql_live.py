"""Live MySQL proof for G21 control totals.

The Postgres matrix proved the ledger comparison; it could not prove the SQL
that collects it is portable. MySQL has no ``TEXT`` cast target, so a single
Postgres-shaped ``CAST(... AS TEXT)`` turns a declared control total into
"unproven" on every MySQL destination — a gate failing for our syntax, not for
the customer's data. These cases open their own pymysql connections.
"""

from __future__ import annotations

import os
import uuid
from decimal import Decimal

import pytest

os.environ.setdefault("MYSQL_HOST", "127.0.0.1")
os.environ.setdefault("MYSQL_PORT", "3306")
os.environ.setdefault("MYSQL_DATABASE", "dataflow")
os.environ.setdefault("MYSQL_USER", "dataflow")
os.environ.setdefault("MYSQL_PASSWORD", "dataflow")
os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from tests.helpers.live_env import mysql_creds, mysql_up

pytestmark = pytest.mark.skipif(not mysql_up(), reason="MySQL not authenticating")


def _connect():
    import pymysql

    creds = mysql_creds()
    return pymysql.connect(
        host=str(creds["host"]),
        port=int(creds["port"]),
        user=str(creds["username"]),
        password=str(creds["password"]),
        database=str(creds["database"]),
        connect_timeout=5,
        autocommit=True,
    )


def _exec(sql: str) -> None:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
    finally:
        conn.close()


def _fetch_one(sql: str):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return None if row is None else row[0]
    finally:
        conn.close()


def _cfg() -> dict:
    creds = mysql_creds()
    return {
        "type": "mysql",
        "host": creds["host"],
        "port": creds["port"],
        "database": creds["database"],
        "username": creds["username"],
        "password": creds["password"],
    }


def _amount_mappings() -> list[dict]:
    return [
        {
            "source": "id",
            "target": "id",
            "confidence": 0.99,
            "transform": "none",
            "target_type": "INT",
        },
        {
            "source": "amount",
            "target": "amount",
            "confidence": 0.99,
            "transform": "none",
            "target_type": "DECIMAL(12,2)",
            "control_total": True,
        },
    ]


def test_live_mysql_independent_sum_is_available() -> None:
    from services.control_totals import independent_column_sum

    table = f"g21_my_{uuid.uuid4().hex[:8]}"
    creds = mysql_creds()
    _exec(f"CREATE TABLE `{table}` (id INT PRIMARY KEY, amount DECIMAL(12,2) NOT NULL)")
    _exec(f"INSERT INTO `{table}` (id, amount) VALUES (1, 10.25), (2, 608.50)")
    try:
        out = independent_column_sum(
            "mysql",
            _cfg(),
            schema=str(creds["database"]),
            table=table,
            column="amount",
        )
        assert out["available"] is True, out["reason"]
        assert Decimal(str(out["sum"])) == Decimal("618.75")
        assert "AS TEXT" not in str(out.get("scan_sql") or "")
        # Independent reread on a connection this module opened itself.
        assert Decimal(
            str(_fetch_one(f"SELECT CAST(COALESCE(SUM(amount), 0) AS CHAR) FROM `{table}`"))
        ) == Decimal("618.75")
    finally:
        _exec(f"DROP TABLE IF EXISTS `{table}`")


def test_live_mysql_declared_control_total_is_proven() -> None:
    from services.control_totals import verify_control_totals

    creds = mysql_creds()
    suffix = uuid.uuid4().hex[:8]
    src = f"g21_my_src_{suffix}"
    dst = f"g21_my_dst_{suffix}"
    _exec(f"CREATE TABLE `{src}` (id INT PRIMARY KEY, amount DECIMAL(12,2) NOT NULL)")
    _exec(f"CREATE TABLE `{dst}` (id INT PRIMARY KEY, amount DECIMAL(12,2) NOT NULL)")
    _exec(f"INSERT INTO `{src}` (id, amount) VALUES (1, 10.25), (2, 608.50)")
    _exec(f"INSERT INTO `{dst}` (id, amount) VALUES (1, 10.25), (2, 608.50)")
    try:
        report, gate = verify_control_totals(
            mappings=_amount_mappings(),
            source_db_type="mysql",
            source_cfg=_cfg(),
            source_schema=str(creds["database"]),
            source_table=src,
            dest_db_type="mysql",
            dest_cfg=_cfg(),
            dest_schema=str(creds["database"]),
            dest_table=dst,
            phase="execute",
        )
        assert gate["status"] == "pass", (gate.get("message"), report)
        column = report["columns"][0]
        assert column["proven"] is True, column["reason"]
        assert Decimal(column["source_sum"]) == Decimal("618.75")
        assert Decimal(column["dest_sum"]) == Decimal("618.75")
        assert report["evidence"] == "exact"
    finally:
        _exec(f"DROP TABLE IF EXISTS `{dst}`")
        _exec(f"DROP TABLE IF EXISTS `{src}`")


def test_live_mysql_control_total_mismatch_blocks_with_same_count() -> None:
    from services.control_totals import verify_control_totals

    creds = mysql_creds()
    suffix = uuid.uuid4().hex[:8]
    src = f"g21_myx_src_{suffix}"
    dst = f"g21_myx_dst_{suffix}"
    _exec(f"CREATE TABLE `{src}` (id INT PRIMARY KEY, amount DECIMAL(12,2) NOT NULL)")
    _exec(f"CREATE TABLE `{dst}` (id INT PRIMARY KEY, amount DECIMAL(12,2) NOT NULL)")
    _exec(f"INSERT INTO `{src}` (id, amount) VALUES (1, 10.25), (2, 608.50)")
    # One cent short, same cardinality.
    _exec(f"INSERT INTO `{dst}` (id, amount) VALUES (1, 10.25), (2, 608.49)")
    try:
        assert int(str(_fetch_one(f"SELECT COUNT(*) FROM `{src}`"))) == int(
            str(_fetch_one(f"SELECT COUNT(*) FROM `{dst}`"))
        )
        report, gate = verify_control_totals(
            mappings=_amount_mappings(),
            source_db_type="mysql",
            source_cfg=_cfg(),
            source_schema=str(creds["database"]),
            source_table=src,
            dest_db_type="mysql",
            dest_cfg=_cfg(),
            dest_schema=str(creds["database"]),
            dest_table=dst,
            phase="execute",
        )
        assert gate["status"] == "block"
        assert report["any_mismatch"] is True
        assert Decimal(report["columns"][0]["dest_sum"]) == Decimal("618.74")
    finally:
        _exec(f"DROP TABLE IF EXISTS `{dst}`")
        _exec(f"DROP TABLE IF EXISTS `{src}`")
