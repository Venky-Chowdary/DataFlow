"""A refused MySQL full_refresh_overwrite must put the prior rows back.

TRUNCATE auto-commits, so emptying the live table and then refusing the load
published an empty destination. The overwrite copies the prior rows to
``<table>__df_bak`` and empties the live table in place (D195: renaming it
aside let the writer re-create it without its constraints); the failure path
copies the rows back, the success path drops the backup.
"""

from __future__ import annotations

import pytest

from connectors.mysql_conn import get_connection
from src.transfer.engine import _settle_overwrite_backup
from src.transfer.engine_destination import _drop_destination_table
from src.transfer.models import EndpointConfig
from tests.helpers.live_env import mysql_creds


def _connect():
    cfg = mysql_creds()
    try:
        return get_connection(
            host=cfg["host"],
            port=int(cfg["port"]),
            database=cfg["database"],
            username=cfg["username"],
            password=cfg["password"],
            connection_string="",
            ssl=False,
        )
    except Exception as exc:
        pytest.fail(f"MySQL is required for the overwrite atomicity regression: {exc}")


def _endpoint(table: str) -> EndpointConfig:
    cfg = mysql_creds()
    return EndpointConfig(
        kind="database",
        format="mysql",
        host=cfg["host"],
        port=int(cfg["port"]),
        database=cfg["database"],
        username=cfg["username"],
        password=cfg["password"],
        table=table,
    )


def _exec(conn, sql: str, args=None) -> None:
    with conn.cursor() as cur:
        cur.execute(sql, args or ())
    conn.commit()


def _count(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM `{table}`")
        found = int(cur.fetchone()[0])
    conn.commit()
    return found


def _names(conn, table: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT name FROM `{table}` ORDER BY id")
        found = [str(row[0]) for row in cur.fetchall()]
    conn.commit()
    return found


def _absent(conn, table: str) -> bool:
    try:
        _count(conn, table)
    except Exception:
        conn.rollback()
        return True
    return False


def _reset(conn, table: str) -> None:
    _exec(conn, f"DROP TABLE IF EXISTS `{table}`")
    _exec(conn, f"DROP TABLE IF EXISTS `{table}__df_bak`")


def test_refused_overwrite_restores_prior_mysql_rows():
    table = "mx_fro_refuse"
    conn = _connect()
    try:
        _reset(conn, table)
        _exec(
            conn,
            f"CREATE TABLE `{table}` (id INT PRIMARY KEY, name VARCHAR(32) NOT NULL)",
        )
        _exec(
            conn,
            f"INSERT INTO `{table}` (id, name) VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')",
        )
        assert _count(conn, table) == 3

        dest = _endpoint(table)
        assert _drop_destination_table(dest) is True
        assert dest.extra.get("overwrite_backup") == f"{table}__df_bak"
        # The live table stays (with its constraints), emptied in place.
        assert _count(conn, table) == 0
        assert _count(conn, f"{table}__df_bak") == 3

        # The refused load wrote part of its batch before it aborted.
        _exec(conn, f"INSERT INTO `{table}` (id, name) VALUES (7, 'partial')")

        assert _settle_overwrite_backup(dest, restore=True) is True
        assert _count(conn, table) == 3
        assert _names(conn, table) == ["alpha", "beta", "gamma"]
        assert _absent(conn, f"{table}__df_bak")
    finally:
        try:
            _reset(conn, table)
        finally:
            conn.close()


def test_successful_overwrite_discards_the_mysql_backup():
    table = "mx_fro_commit"
    conn = _connect()
    try:
        _reset(conn, table)
        _exec(conn, f"CREATE TABLE `{table}` (id INT PRIMARY KEY, name VARCHAR(32))")
        _exec(conn, f"INSERT INTO `{table}` (id, name) VALUES (1, 'old')")
        dest = _endpoint(table)
        assert _drop_destination_table(dest) is True
        _exec(conn, f"INSERT INTO `{table}` (id, name) VALUES (9, 'new')")
        assert _settle_overwrite_backup(dest, restore=False) is True
        assert _names(conn, table) == ["new"]
        assert _absent(conn, f"{table}__df_bak")
    finally:
        try:
            _reset(conn, table)
        finally:
            conn.close()
