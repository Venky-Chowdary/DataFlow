"""A Unicode source column lands in a SQL Server carrier that can hold it (live).

SQL Server ``VARCHAR`` stores one byte per character in the column collation's
code page, and the compose server's database collation is
``SQL_Latin1_General_CP1_CI_AS`` (cp1252). ``語`` has no cp1252 code point, so a
create-new run that stamps ``VARCHAR`` either quarantines every CJK row or
writes ``?`` — the choice of carrier is the defect, not the refusal.

MySQL is the source that proves it: its character set is declared per column, so
one table carries a ``utf8mb4`` column (every scalar) beside a ``latin1`` column
(single byte). The first must land national; the second must keep its own
polarity, because promoting it would report a widen the source never had.
"""

from __future__ import annotations

from typing import Any

import pytest

from tests.typed_fidelity_helpers import (
    mysql_endpoint,
    require_ports,
    require_sqlserver_drivers,
    run_typed_transfer,
    sqlserver_endpoint,
    uniq,
)

CJK = "語"
EMOJI = "😀"
LATIN = "café"


def _mysql_conn() -> Any:
    import pymysql

    return pymysql.connect(
        host="localhost",
        port=3306,
        user="dataflow",
        password="dataflow",
        database="dataflow",
        charset="utf8mb4",
        autocommit=True,
    )


def _seed_mysql_mixed_charset(table: str) -> None:
    conn = _mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(
                f"""
                CREATE TABLE `{table}` (
                  id INT PRIMARY KEY,
                  code VARCHAR(32) CHARACTER SET utf8mb4 NOT NULL,
                  note LONGTEXT CHARACTER SET utf8mb4 NULL,
                  legacy VARCHAR(32) CHARACTER SET latin1 NULL
                )
                """
            )
            cur.executemany(
                f"INSERT INTO `{table}` (id, code, note, legacy) VALUES (%s,%s,%s,%s)",
                [
                    (1, CJK, f"{CJK}{EMOJI} mixed", LATIN),
                    (2, "plain", "ascii note", "plain"),
                ],
            )
    finally:
        conn.close()


def _drop_mysql(table: str) -> None:
    conn = _mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
    finally:
        conn.close()


def _sqlserver_conn() -> Any:
    import pyodbc

    return pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;"
        "DATABASE=dataflow;UID=sa;PWD=DataFlow_CDC_2022!;"
        "Encrypt=yes;TrustServerCertificate=yes",
        timeout=15,
        autocommit=True,
    )


def _sqlserver_column_types(table: str) -> dict[str, str]:
    conn = _sqlserver_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT c.name, t.name, c.max_length, c.collation_name
            FROM sys.columns c
            JOIN sys.types t ON t.user_type_id = c.user_type_id
            WHERE c.object_id = OBJECT_ID(?)
            """,
            f"dbo.{table}",
        )
        return {str(r[0]): f"{str(r[1]).lower()}({r[2]})|{r[3] or ''}" for r in cur.fetchall()}
    finally:
        conn.close()


def _drop_sqlserver(table: str) -> None:
    try:
        conn = _sqlserver_conn()
    except Exception:  # noqa: BLE001 - cleanup must not mask the test's verdict
        return
    try:
        cur = conn.cursor()
        cur.execute(f"IF OBJECT_ID(N'dbo.[{table}]', N'U') IS NOT NULL DROP TABLE dbo.[{table}]")
    finally:
        conn.close()


def test_mysql_utf8mb4_source_lands_national_on_sqlserver() -> None:
    require_ports(3306, 1433)
    require_sqlserver_drivers()
    pytest.importorskip("pymysql")
    pytest.importorskip("pyodbc")

    src = uniq("d2_my_src")
    dst = uniq("d2_mssql_dst")
    _seed_mysql_mixed_charset(src)
    try:
        result = run_typed_transfer(mysql_endpoint(src), sqlserver_endpoint(dst))
        assert result.success is True, result.error
        assert result.records_transferred == 2

        types = _sqlserver_column_types(dst)
        assert types, f"destination table {dst} not created"
        assert types["code"].startswith("nvarchar"), types
        assert types["note"].startswith("nvarchar"), types
        # A latin1 source column is genuinely single-byte: keep its polarity.
        assert types["legacy"].startswith("varchar"), types

        conn = _sqlserver_conn()
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT code, note, legacy FROM dbo.[{dst}] WHERE id = 1")
            row = cur.fetchone()
        finally:
            conn.close()
        assert row is not None, "row id=1 missing on the destination"
        assert row[0] == CJK, ascii(row[0])
        assert row[1] == f"{CJK}{EMOJI} mixed", ascii(row[1])
        assert row[2] == LATIN, ascii(row[2])

        conn = _mysql_conn()
        try:
            with conn.cursor() as cur2:
                cur2.execute(f"SELECT code, note, legacy FROM `{src}` WHERE id = 1")
                src_row = cur2.fetchone()
        finally:
            conn.close()
        assert src_row == (CJK, f"{CJK}{EMOJI} mixed", LATIN), ascii(src_row)
    finally:
        _drop_mysql(src)
        _drop_sqlserver(dst)


def test_varchar_on_this_server_really_cannot_hold_the_scalar() -> None:
    """The defect's cost, measured on the live server rather than asserted."""
    require_ports(1433)
    pytest.importorskip("pyodbc")

    table = uniq("d2_cp1252_probe")
    conn = _sqlserver_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE dbo.[{table}] (id INT, code VARCHAR(32), ncode NVARCHAR(32))")
        cur.execute(
            f"INSERT INTO dbo.[{table}] (id, code, ncode) VALUES (1, ?, ?)",
            CJK,
            CJK,
        )
        cur.execute(f"SELECT code, ncode FROM dbo.[{table}] WHERE id = 1")
        code, ncode = cur.fetchone()
        # cp1252 has no code point for 語: the VARCHAR column keeps a '?' while
        # the national column re-reads the scalar exactly.
        assert code == "?", ascii(code)
        assert ncode == CJK, ascii(ncode)
    finally:
        try:
            cur.execute(f"DROP TABLE dbo.[{table}]")
        finally:
            conn.close()
