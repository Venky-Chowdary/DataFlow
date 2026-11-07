"""CR-D126 — a TEXT upsert key must be unique on its whole value on MySQL.

Create-new already sizes a LOB key to ``VARCHAR(n)`` (or refuses), but an
upsert into an *existing* table whose key column is TEXT still got
``CREATE UNIQUE INDEX ... (col(191))``: two distinct keys sharing their first
191 characters collided and ``ON DUPLICATE KEY UPDATE`` overwrote the other
row. A ``json`` key failed with 3152. The key column is now converted to a
full-width ``VARCHAR(n)`` when every stored value fits, and anything else is
refused with a clear message — never a prefix.

Live MySQL 8 on :3306.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

from connectors.mysql_writer import write_mapped_rows

MY = dict(
    host=os.environ.get("DATAFLOW_TEST_MYSQL_HOST", "127.0.0.1"),
    port=int(os.environ.get("DATAFLOW_TEST_MYSQL_PORT", "3306")),
    database=os.environ.get("DATAFLOW_TEST_MYSQL_DB", "dataflow"),
    username=os.environ.get("DATAFLOW_TEST_MYSQL_USER", "dataflow"),
    password=os.environ.get("DATAFLOW_TEST_MYSQL_PASSWORD", "dataflow"),
)


def _reachable() -> bool:
    try:
        with socket.create_connection((MY["host"], MY["port"]), timeout=1.5):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _reachable(), reason="MySQL not reachable")


def _query(sql: str, params: tuple = ()) -> list[tuple]:
    import pymysql

    conn = pymysql.connect(
        host=MY["host"], port=MY["port"], user=MY["username"],
        password=MY["password"], database=MY["database"], autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall())
    finally:
        conn.close()


PREFIX = "k" * 191
KEY_A, KEY_B = PREFIX + "A" * 9, PREFIX + "B" * 9


def _write(table: str, rows: list[list[str]], key_type: str = "TEXT"):
    return write_mapped_rows(
        **MY,
        schema="",
        connection_string="",
        ssl=False,
        table_name=table,
        headers=["k", "v"],
        data_rows=rows,
        mappings=[{"source": "k", "target": "k"}, {"source": "v", "target": "v"}],
        column_types={"k": key_type, "v": "TEXT"},
        write_mode="upsert",
        conflict_columns=["k"],
    )


@pytest.fixture()
def table():
    name = f"crd126_{uuid.uuid4().hex[:10]}"
    yield name
    _query(f"DROP TABLE IF EXISTS `{name}`")


def _existing(table: str, key_type: str, rows: list[tuple[str, str]]) -> None:
    _query(
        f"CREATE TABLE `{table}` (k {key_type}, v TEXT) "
        "CHARACTER SET utf8mb4 COMMENT='dataflow:managed'"
    )
    for k, v in rows:
        _query(f"INSERT INTO `{table}` VALUES (%s, %s)", (k, v))


def test_keys_sharing_a_191_char_prefix_stay_distinct(table):
    _existing(table, "TEXT", [(KEY_A, "a1")])
    res = _write(table, [[KEY_B, "b1"]])
    assert res.ok, res.error
    res = _write(table, [[KEY_B, "b2"]])
    assert res.ok, res.error

    assert _query(f"SELECT k, v FROM `{table}` ORDER BY v") == [(KEY_A, "a1"), (KEY_B, "b2")]
    (col_type,) = _query(
        "SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = 'k'",
        (table,),
    )[0]
    assert col_type.lower() == "varchar(768)"


def test_key_longer_than_an_index_can_hold_is_refused(table):
    _existing(table, "TEXT", [("x" * 800, "long")])
    res = _write(table, [["y", "1"]])
    assert not res.ok
    assert "768" in str(res.error) and "prefix" in str(res.error), res.error
    assert _query(f"SELECT COUNT(*) FROM `{table}`") == [(1,)]


def test_json_key_is_refused_with_a_reason(table):
    _existing(table, "JSON", [('{"id": 1}', "one")])
    res = _write(table, [['{"id": 2}', "two"]], key_type="JSON")
    assert not res.ok
    assert "json" in str(res.error).lower(), res.error
    assert _query(f"SELECT COUNT(*) FROM `{table}`") == [(1,)]
