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


# ---------------------------------------------------------------------------
# New-ops batch: filter_rows / concat_columns / split_column / rename_column /
# keep_columns / parse_date / lookup — independent SQL on SQLite, MySQL mirrors
# where the dialect matters.
# ---------------------------------------------------------------------------


def test_oracle_filter_rows_matches_sql_where(tmp_path):
    rows = [
        {"id": "1", "qty": "5"},
        {"id": "2", "qty": "15"},
        {"id": "3", "qty": "10"},
        {"id": "4", "qty": "25"},
        {"id": "5", "qty": None},
    ]
    out = _run(
        {
            "input_columns": ["id", "qty"],
            "steps": [
                {"op": "filter_rows", "options": {"condition": "to_number([qty]) > 10"}}
            ],
        },
        rows,
    )
    expected = _sqlite_oracle(
        tmp_path,
        rows,
        ["id", "qty"],
        "SELECT id, qty FROM src WHERE CAST(qty AS REAL) > 10 "
        "ORDER BY CAST(id AS INTEGER)",
    )
    assert [(r["id"], _text(r["qty"])) for r in out] == [
        (eid, _text(eq)) for eid, eq in expected
    ]


def test_oracle_concat_columns_matches_sql_concat_ws(tmp_path):
    rows = [
        {"id": "1", "first": "Ada", "last": "Lovelace"},
        {"id": "2", "first": None, "last": "Hopper"},
        {"id": "3", "first": "Alan", "last": None},
        {"id": "4", "first": None, "last": None},
    ]
    out = _run(
        {
            "input_columns": ["id", "first", "last"],
            "steps": [
                {
                    "op": "concat_columns",
                    "options": {"to": "full", "columns": ["first", "last"], "separator": ":"},
                }
            ],
        },
        rows,
    )
    expected = _sqlite_oracle(
        tmp_path,
        rows,
        ["id", "first", "last"],
        # Engine joins only the non-NULL parts — the oracle strips the edge
        # separators a missing part would leave.
        "SELECT id, TRIM(COALESCE(first, '') || ':' || COALESCE(last, ''), ':') "
        "FROM src ORDER BY CAST(id AS INTEGER)",
    )
    assert [(r["id"], _text(r["full"])) for r in out] == [
        (eid, _text(eq)) for eid, eq in expected
    ]


def test_oracle_split_column_matches_sql_substr(tmp_path):
    rows = [
        {"id": "1", "code": "US-CA"},
        {"id": "2", "code": "GB-LDN"},
        {"id": "3", "code": "NODASH"},
        {"id": "4", "code": None},
    ]
    out = _run(
        {
            "input_columns": ["id", "code"],
            "steps": [
                {
                    "op": "split_column",
                    "column": "code",
                    "options": {"separator": "-", "into": ["country", "region"]},
                }
            ],
        },
        rows,
    )
    expected = _sqlite_oracle(
        tmp_path,
        rows,
        ["id", "code"],
        "SELECT id, "
        "CASE WHEN code IS NULL THEN NULL WHEN INSTR(code,'-')>0 "
        "THEN SUBSTR(code,1,INSTR(code,'-')-1) ELSE code END, "
        "CASE WHEN code IS NOT NULL AND INSTR(code,'-')>0 "
        "THEN SUBSTR(code,INSTR(code,'-')+1) ELSE NULL END "
        "FROM src ORDER BY CAST(id AS INTEGER)",
    )
    assert [(r["id"], _text(r["country"]), _text(r["region"])) for r in out] == [
        (eid, _text(ec), _text(er)) for eid, ec, er in expected
    ]


def test_oracle_rename_column_matches_sql_aliased_projection(tmp_path):
    rows = [
        {"id": "1", "qty": "7"},
        {"id": "2", "qty": "0"},
        {"id": "3", "qty": None},
    ]
    out = _run(
        {
            "input_columns": ["id", "qty"],
            "steps": [{"op": "rename_column", "column": "qty", "options": {"to": "quantity"}}],
        },
        rows,
    )
    expected = _sqlite_oracle(
        tmp_path,
        rows,
        ["id", "qty"],
        "SELECT id, qty FROM src ORDER BY CAST(id AS INTEGER)",
    )
    assert [(r["id"], _text(r["quantity"]), "qty" in r) for r in out] == [
        (eid, _text(eq), False) for eid, eq in expected
    ]


def test_oracle_keep_columns_matches_sql_projection(tmp_path):
    rows = [
        {"id": "1", "a": "x", "b": "y", "dropme": "z"},
        {"id": "2", "a": None, "b": "w", "dropme": None},
    ]
    out = _run(
        {
            "input_columns": ["id", "a", "b", "dropme"],
            "steps": [{"op": "keep_columns", "options": {"columns": ["id", "a", "b"]}}],
        },
        rows,
    )
    expected = _sqlite_oracle(
        tmp_path,
        rows,
        ["id", "a", "b", "dropme"],
        "SELECT id, a, b FROM src ORDER BY CAST(id AS INTEGER)",
    )
    assert [(r["id"], _text(r.get("a")), _text(r.get("b")), sorted(r)) for r in out] == [
        (eid, _text(ea), _text(eb), ["a", "b", "id"]) for eid, ea, eb in expected
    ]


def test_oracle_parse_date_matches_sql_reformat(tmp_path):
    rows = [
        {"id": "1", "d": "01/15/2026"},
        {"id": "2", "d": "12/31/2025"},
        {"id": "3", "d": "07/04/2026"},
        {"id": "4", "d": None},
    ]
    out = _run(
        {
            "input_columns": ["id", "d"],
            "steps": [
                {
                    "op": "parse_date",
                    "column": "d",
                    "options": {"format": "%m/%d/%Y", "output_format": "%Y-%m-%d"},
                }
            ],
        },
        rows,
    )
    expected = _sqlite_oracle(
        tmp_path,
        rows,
        ["id", "d"],
        # Engine REFUSES unparseable values (on_error=refuse) — the oracle
        # only models the valid inputs; NULL passes through as NULL.
        "SELECT id, CASE WHEN d IS NULL THEN NULL "
        "ELSE SUBSTR(d,7,4)||'-'||SUBSTR(d,1,2)||'-'||SUBSTR(d,4,2) "
        "END FROM src ORDER BY CAST(id AS INTEGER)",
    )
    assert [(r["id"], str(r["d"]) if r.get("d") is not None else None) for r in out] == [
        (eid, ed) for eid, ed in expected
    ]


def test_oracle_lookup_matches_sql_left_join(tmp_path):
    rows = [
        {"id": "1", "ccy": "USD"},
        {"id": "2", "ccy": "EUR"},
        {"id": "3", "ccy": "XXX"},
        {"id": "4", "ccy": None},
    ]
    out = _run(
        {
            "input_columns": ["id", "ccy"],
            "steps": [
                {
                    "op": "lookup",
                    "column": "ccy",
                    "options": {
                        "lookup_key": "code",
                        "rows": [
                            {"code": "USD", "name": "US Dollar"},
                            {"code": "EUR", "name": "Euro"},
                        ],
                        "map": {"name": "ccy_name"},
                        "unmatched": "null",
                    },
                }
            ],
        },
        rows,
    )
    db = tmp_path / "oracle_lookup.db"
    conn = sqlite3.connect(db)
    try:
        conn.execute("CREATE TABLE src (id TEXT, ccy TEXT)")
        conn.execute("CREATE TABLE ref (code TEXT, name TEXT)")
        conn.executemany(
            "INSERT INTO src VALUES (?, ?)", [(r["id"], r["ccy"]) for r in rows]
        )
        conn.executemany(
            "INSERT INTO ref VALUES (?, ?)",
            [("USD", "US Dollar"), ("EUR", "Euro")],
        )
        expected = conn.execute(
            "SELECT s.id, r.name FROM src s LEFT JOIN ref r ON r.code = s.ccy "
            "ORDER BY CAST(s.id AS INTEGER)"
        ).fetchall()
    finally:
        conn.close()
    assert [(r["id"], _text(r["ccy_name"])) for r in out] == [
        (eid, _text(en)) for eid, en in expected
    ]


def test_oracle_filter_rows_matches_mysql_where():
    rows = [
        {"id": "1", "qty": "5"},
        {"id": "2", "qty": "15"},
        {"id": "3", "qty": "10"},
        {"id": "4", "qty": "25"},
        {"id": "5", "qty": None},
    ]
    out = _run(
        {
            "input_columns": ["id", "qty"],
            "steps": [
                {"op": "filter_rows", "options": {"condition": "to_number([qty]) > 10"}}
            ],
        },
        rows,
    )
    expected = _mysql_oracle(
        rows,
        ["id", "qty"],
        "SELECT id, qty FROM `{t}` WHERE CAST(qty AS DECIMAL(30,12)) > 10 "
        "ORDER BY CAST(id AS SIGNED)",
    )
    assert [(r["id"], _text(r["qty"])) for r in out] == [
        (eid, _text(eq)) for eid, eq in expected
    ]


def test_oracle_concat_columns_matches_mysql_concat_ws():
    rows = [
        {"id": "1", "first": "Ada", "last": "Lovelace"},
        {"id": "2", "first": None, "last": "Hopper"},
        {"id": "3", "first": "Alan", "last": None},
        {"id": "4", "first": None, "last": None},
    ]
    out = _run(
        {
            "input_columns": ["id", "first", "last"],
            "steps": [
                {
                    "op": "concat_columns",
                    "options": {"to": "full", "columns": ["first", "last"], "separator": ":"},
                }
            ],
        },
        rows,
    )
    # CONCAT_WS natively skips NULL parts — exactly the engine's rule.
    expected = _mysql_oracle(
        rows,
        ["id", "first", "last"],
        "SELECT id, CONCAT_WS(':', first, last) FROM `{t}` "
        "ORDER BY CAST(id AS SIGNED)",
    )
    assert [(r["id"], _text(r["full"])) for r in out] == [
        (eid, _text(eq)) for eid, eq in expected
    ]


def test_oracle_split_column_matches_mysql_substring_index():
    rows = [
        {"id": "1", "code": "US-CA"},
        {"id": "2", "code": "GB-LDN"},
        {"id": "3", "code": "NODASH"},
        {"id": "4", "code": None},
    ]
    out = _run(
        {
            "input_columns": ["id", "code"],
            "steps": [
                {
                    "op": "split_column",
                    "column": "code",
                    "options": {"separator": "-", "into": ["country", "region"]},
                }
            ],
        },
        rows,
    )
    expected = _mysql_oracle(
        rows,
        ["id", "code"],
        "SELECT id, "
        "CASE WHEN code IS NULL THEN NULL WHEN INSTR(code,'-')>0 "
        "THEN SUBSTRING_INDEX(code,'-',1) ELSE code END, "
        "CASE WHEN code IS NOT NULL AND INSTR(code,'-')>0 "
        "THEN SUBSTRING_INDEX(code,'-',-1) ELSE NULL END "
        "FROM `{t}` ORDER BY CAST(id AS SIGNED)",
    )
    assert [(r["id"], _text(r["country"]), _text(r["region"])) for r in out] == [
        (eid, _text(ec), _text(er)) for eid, ec, er in expected
    ]


def test_oracle_lookup_matches_mysql_left_join():
    rows = [
        {"id": "1", "ccy": "USD"},
        {"id": "2", "ccy": "EUR"},
        {"id": "3", "ccy": "XXX"},
        {"id": "4", "ccy": None},
    ]
    out = _run(
        {
            "input_columns": ["id", "ccy"],
            "steps": [
                {
                    "op": "lookup",
                    "column": "ccy",
                    "options": {
                        "lookup_key": "code",
                        "rows": [
                            {"code": "USD", "name": "US Dollar"},
                            {"code": "EUR", "name": "Euro"},
                        ],
                        "map": {"name": "ccy_name"},
                        "unmatched": "null",
                    },
                }
            ],
        },
        rows,
    )
    conn = _mysql_conn()
    if conn is None:
        pytest.skip("local MySQL (127.0.0.1:3306) unreachable")
    t_src = f"oracle_lk_{uuid.uuid4().hex[:10]}"
    t_ref = f"oracle_lr_{uuid.uuid4().hex[:10]}"
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE `{t_src}` (ord INT AUTO_INCREMENT PRIMARY KEY, id TEXT, ccy TEXT)")
            cur.execute(f"CREATE TABLE `{t_ref}` (code TEXT, name TEXT)")
            cur.executemany(
                f"INSERT INTO `{t_src}` (id, ccy) VALUES (%s, %s)",
                [(r["id"], r["ccy"]) for r in rows],
            )
            cur.executemany(
                f"INSERT INTO `{t_ref}` (code, name) VALUES (%s, %s)",
                [("USD", "US Dollar"), ("EUR", "Euro")],
            )
            conn.commit()
            cur.execute(
                f"SELECT s.id, r.name FROM `{t_src}` s LEFT JOIN `{t_ref}` r "
                f"ON r.code = s.ccy ORDER BY s.ord"
            )
            expected = list(cur.fetchall())
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{t_src}`")
                cur.execute(f"DROP TABLE IF EXISTS `{t_ref}`")
            conn.commit()
        finally:
            conn.close()
    assert [(r["id"], _text(r["ccy_name"])) for r in out] == [
        (eid, _text(en)) for eid, en in expected
    ]
