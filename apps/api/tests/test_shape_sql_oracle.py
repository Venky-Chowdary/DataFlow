"""Independent SQL oracle: shape-engine ops vs hand-written SQL on SQLite.

Each test runs one shape op through the real engine path (ShapeRunner.records
+ drain) and independently computes the expected result with a hand-written
SQL statement on a real SQLite database containing the same rows, then
compares row-by-row. The oracle is written per-op — no engine code is reused
on the expected side — so a wrong implementation on either side diverges.

Value comparison is on canonical text: engine output cells are compared to
the sqlite result rendered through ``str``/Decimal formatting, with None on
both sides matching SQL NULL.
"""

from __future__ import annotations

import sqlite3
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

from services.shape_apply import ShapeRunner
from services.shape_models import ShapeRecipe


def _run(payload: dict, rows: list[dict]) -> list[dict]:
    runner = ShapeRunner(ShapeRecipe.parse(payload))
    out = runner.records(rows)
    out += runner.drain()
    return out


def _text(v) -> object:
    """Numeric cells compare as Decimal so '21.00' equals '21.0'; text as str."""
    if v is None:
        return None
    if isinstance(v, Decimal):
        return v
    if isinstance(v, (int, float)):
        return Decimal(str(v))
    s = str(v)
    try:
        return Decimal(s)
    except Exception:
        return s


def _sqlite_oracle(tmp_path: Path, rows: list[dict], columns: list[str], sql: str) -> list[tuple]:
    """Load ``rows`` into a scratch sqlite file and return ``sql``'s result."""
    db = tmp_path / "oracle.db"
    if db.exists():
        db.unlink()
    conn = sqlite3.connect(db)
    try:
        cols = ", ".join(f'"{c}"' for c in columns)
        conn.execute(f"CREATE TABLE src ({', '.join(chr(34) + c + chr(34) for c in columns)})")
        conn.executemany(
            f"INSERT INTO src ({cols}) VALUES ({', '.join('?' * len(columns))})",
            [[r.get(c) for c in columns] for r in rows],
        )
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# cast_column — declare logical type; oracle: CAST(qty AS INTEGER)
# ---------------------------------------------------------------------------


def test_oracle_cast_column_integer_matches_sql_cast(tmp_path):
    rows = [
        {"id": "1", "qty": "7"},
        {"id": "2", "qty": "0"},
        {"id": "3", "qty": "1000000"},
        {"id": "4", "qty": None},
    ]
    out = _run(
        {
            "input_columns": ["id", "qty"],
            "steps": [{"op": "cast_column", "column": "qty", "options": {"to_type": "integer"}}],
        },
        rows,
    )
    expected = _sqlite_oracle(
        tmp_path, rows, ["id", "qty"],
        'SELECT id, CAST(qty AS INTEGER) FROM src ORDER BY CAST(id AS INTEGER)',
    )
    assert [(r["id"], _text(r["qty"])) for r in out] == [
        (eid, _text(eq)) for eid, eq in expected
    ]


# ---------------------------------------------------------------------------
# derive_column — row-local expression; oracle: COALESCE(amount,0) * rate
# ---------------------------------------------------------------------------


def test_oracle_derive_column_arithmetic_matches_sql_expression(tmp_path):
    rows = [
        {"id": "1", "amount": "10.50", "rate": "2"},
        {"id": "2", "amount": None, "rate": "3"},
        {"id": "3", "amount": "0.125", "rate": "8"},
    ]
    out = _run(
        {
            "input_columns": ["id", "amount", "rate"],
            "steps": [
                {
                    "op": "derive_column",
                    "options": {
                        "to": "total",
                        "expression": "coalesce([amount], 0) * to_number([rate])",
                    },
                }
            ],
        },
        rows,
    )
    expected = _sqlite_oracle(
        tmp_path,
        rows,
        ["id", "amount", "rate"],
        "SELECT id, COALESCE(CAST(amount AS REAL), 0) * CAST(rate AS REAL) "
        "FROM src ORDER BY CAST(id AS INTEGER)",
    )
    assert [(r["id"], _text(r["total"])) for r in out] == [
        (eid, _text(eq)) for eid, eq in expected
    ]


# ---------------------------------------------------------------------------
# null_if — sentinel to NULL; oracle: NULLIF(status, 'N/A')
# ---------------------------------------------------------------------------


def test_oracle_null_if_matches_sql_nullif(tmp_path):
    rows = [
        {"id": "1", "status": "ok"},
        {"id": "2", "status": "N/A"},
        {"id": "3", "status": ""},
        {"id": "4", "status": "N/A"},
        {"id": "5", "status": None},
    ]
    out = _run(
        {
            "input_columns": ["id", "status"],
            "steps": [{"op": "null_if", "column": "status", "options": {"values": ["N/A"]}}],
        },
        rows,
    )
    expected = _sqlite_oracle(
        tmp_path,
        rows,
        ["id", "status"],
        # Engine semantics: blank string is null evidence before the sentinel
        # list is consulted — the oracle's CASE states that rule explicitly.
        "SELECT id, CASE WHEN status IS NULL OR status IN ('', 'N/A') "
        "THEN NULL ELSE status END FROM src ORDER BY CAST(id AS INTEGER)",
    )
    assert [(r["id"], _text(r["status"])) for r in out] == [
        (eid, _text(eq)) for eid, eq in expected
    ]


# ---------------------------------------------------------------------------
# dedupe_rows — keep first by key; oracle: MIN(rowid) per group, insertion order
# ---------------------------------------------------------------------------


def test_oracle_dedupe_keep_first_matches_sql_group_min_rowid(tmp_path):
    rows = [
        {"id": "1", "v": "old", "ts": 1},
        {"id": "2", "v": "b", "ts": 2},
        {"id": "1", "v": "new", "ts": 3},
        {"id": "3", "v": "c", "ts": 4},
        {"id": "2", "v": "b2", "ts": 5},
    ]
    out = _run(
        {
            "input_columns": ["id", "v", "ts"],
            "steps": [{"op": "dedupe_rows", "options": {"keys": ["id"], "keep": "first"}}],
        },
        rows,
    )
    expected = _sqlite_oracle(
        tmp_path,
        rows,
        ["id", "v", "ts"],
        "SELECT id, v, ts FROM src WHERE rowid IN "
        "(SELECT MIN(rowid) FROM src GROUP BY id) ORDER BY rowid",
    )
    assert [(r["id"], _text(r["v"]), _text(r["ts"])) for r in out] == [
        (eid, _text(ev), _text(ets)) for eid, ev, ets in expected
    ]


# ---------------------------------------------------------------------------
# MySQL oracle — same ops, independent dialect. Skips when the local MySQL is
# unreachable so the file stays runnable without services.
# ---------------------------------------------------------------------------


def _mysql_conn():
    import pymysql

    try:
        return pymysql.connect(
            host="127.0.0.1",
            port=3306,
            user="dataflow",
            password="dataflow",
            database="dataflow",
            connect_timeout=3,
        )
    except Exception:
        return None


def _mysql_oracle(rows: list[dict], columns: list[str], sql: str) -> list[tuple]:
    """Load ``rows`` into a scratch MySQL table and return ``sql``'s result."""
    conn = _mysql_conn()
    if conn is None:
        pytest.skip("local MySQL (127.0.0.1:3306) unreachable")
    table = f"oracle_{uuid.uuid4().hex[:12]}"
    cols = ", ".join(f"`{c}` TEXT" for c in columns)
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(
                f"CREATE TABLE `{table}` (ord INT AUTO_INCREMENT PRIMARY KEY, {cols})"
            )
            names = ", ".join(f"`{c}`" for c in columns)
            cur.executemany(
                f"INSERT INTO `{table}` ({names}) VALUES "
                f"({', '.join(['%s'] * len(columns))})",
                [
                    [None if r.get(c) is None else str(r.get(c)) for c in columns]
                    for r in rows
                ],
            )
            conn.commit()
            cur.execute(sql.format(t=table))
            return list(cur.fetchall())
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            conn.commit()
        finally:
            conn.close()


def test_oracle_cast_column_integer_matches_mysql_cast():
    rows = [
        {"id": "1", "qty": "7"},
        {"id": "2", "qty": "0"},
        {"id": "3", "qty": "1000000"},
        {"id": "4", "qty": None},
    ]
    out = _run(
        {
            "input_columns": ["id", "qty"],
            "steps": [{"op": "cast_column", "column": "qty", "options": {"to_type": "integer"}}],
        },
        rows,
    )
    expected = _mysql_oracle(
        rows,
        ["id", "qty"],
        "SELECT id, CAST(qty AS SIGNED) FROM `{t}` ORDER BY CAST(id AS SIGNED)",
    )
    assert [(r["id"], _text(r["qty"])) for r in out] == [
        (eid, _text(eq)) for eid, eq in expected
    ]


def test_oracle_derive_column_arithmetic_matches_mysql_expression():
    rows = [
        {"id": "1", "amount": "10.50", "rate": "2"},
        {"id": "2", "amount": None, "rate": "3"},
        {"id": "3", "amount": "0.125", "rate": "8"},
    ]
    out = _run(
        {
            "input_columns": ["id", "amount", "rate"],
            "steps": [
                {
                    "op": "derive_column",
                    "options": {
                        "to": "total",
                        "expression": "coalesce([amount], 0) * to_number([rate])",
                    },
                }
            ],
        },
        rows,
    )
    expected = _mysql_oracle(
        rows,
        ["id", "amount", "rate"],
        "SELECT id, COALESCE(CAST(amount AS DECIMAL(30,12)), 0) "
        "* CAST(rate AS DECIMAL(30,12)) FROM `{t}` ORDER BY CAST(id AS SIGNED)",
    )
    assert [(r["id"], _text(r["total"])) for r in out] == [
        (eid, _text(eq)) for eid, eq in expected
    ]


def test_oracle_null_if_matches_mysql_nullif():
    rows = [
        {"id": "1", "status": "ok"},
        {"id": "2", "status": "N/A"},
        {"id": "3", "status": ""},
        {"id": "4", "status": "N/A"},
        {"id": "5", "status": None},
    ]
    out = _run(
        {
            "input_columns": ["id", "status"],
            "steps": [{"op": "null_if", "column": "status", "options": {"values": ["N/A"]}}],
        },
        rows,
    )
    expected = _mysql_oracle(
        rows,
        ["id", "status"],
        "SELECT id, CASE WHEN status IS NULL OR status IN ('', 'N/A') "
        "THEN NULL ELSE status END FROM `{t}` ORDER BY CAST(id AS SIGNED)",
    )
    assert [(r["id"], _text(r["status"])) for r in out] == [
        (eid, _text(eq)) for eid, eq in expected
    ]


def test_oracle_dedupe_keep_first_matches_mysql_group_min_ord():
    rows = [
        {"id": "1", "v": "old", "ts": 1},
        {"id": "2", "v": "b", "ts": 2},
        {"id": "1", "v": "new", "ts": 3},
        {"id": "3", "v": "c", "ts": 4},
        {"id": "2", "v": "b2", "ts": 5},
    ]
    out = _run(
        {
            "input_columns": ["id", "v", "ts"],
            "steps": [{"op": "dedupe_rows", "options": {"keys": ["id"], "keep": "first"}}],
        },
        rows,
    )
    expected = _mysql_oracle(
        rows,
        ["id", "v", "ts"],
        "SELECT id, v, ts FROM `{t}` WHERE ord IN "
        "(SELECT MIN(ord) FROM `{t}` GROUP BY id) ORDER BY ord",
    )
    assert [(r["id"], _text(r["v"]), _text(r["ts"])) for r in out] == [
        (eid, _text(ev), _text(ets)) for eid, ev, ets in expected
    ]
