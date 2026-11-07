"""Uniqueness-probe identity cells use cell_to_string, not str(value).

str(True) invented True. str(Decimal('1E+2')) invented scientific.
str(bytes) invented a Python b'...' repr. None used a private NUL token,
then findings collapsed it to '' so Validate could not tell NULL from empty.
"""

from __future__ import annotations

import sqlite3
import sys
from collections import Counter
from datetime import datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.source_duplicate_probe import (  # noqa: E402
    _counter_findings,
    _identity_cell,
    _normalize_key_cell,
    probe_source_duplicate_keys_result,
)
from services.value_serializer import SQL_NULL_SENTINEL, cell_to_string  # noqa: E402

LONG = "1.234567890123456789"
BLOB = bytes([0xFF, 0xFE, 0x00])
TS = datetime(2024, 1, 2, 3, 4, 5)


def test_identity_cell_matches_sql_reader():
    assert _identity_cell(None) == SQL_NULL_SENTINEL
    assert _identity_cell("") == ""
    assert _identity_cell(None) != _identity_cell("")
    assert _identity_cell(True) == "true"
    assert _identity_cell(True) != str(True)
    assert _identity_cell(Decimal(LONG)) == LONG
    assert _identity_cell(Decimal("1E+2")) == "100"
    assert str(Decimal("1E+2")) == "1E+2"
    assert _identity_cell(TS) == "2024-01-02T03:04:05"
    assert _identity_cell(BLOB) == cell_to_string(BLOB, preserve_sql_null=True)
    assert _normalize_key_cell(None) == SQL_NULL_SENTINEL
    assert _normalize_key_cell(None) != _normalize_key_cell("")


def test_counter_findings_keep_sql_null_distinct_from_empty():
    counts = Counter(
        {
            (SQL_NULL_SENTINEL,): 3,
            ("",): 2,
        }
    )
    findings = _counter_findings(counts, ["id"], limit=5)
    by_value = {f["value"]: f["count"] for f in findings}
    assert by_value[SQL_NULL_SENTINEL] == 3
    assert by_value[""] == 2
    assert SQL_NULL_SENTINEL != ""


def test_sqlite_null_keys_are_not_shown_as_empty(tmp_path: Path):
    db = tmp_path / "null_keys.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE jobs (id TEXT, name TEXT)")
    conn.executemany(
        "INSERT INTO jobs VALUES (?, ?)",
        [(None, "A"), (None, "B"), ("", "C"), ("", "D"), ("x", "E")],
    )
    conn.commit()
    conn.close()
    result = probe_source_duplicate_keys_result(
        source_config={
            "type": "sqlite",
            "database": str(db),
            "connection_string": f"sqlite:///{db}",
            "host": "",
            "port": 0,
            "username": "",
            "password": "",
            "schema": "",
            "ssl": False,
        },
        source_table="jobs",
        primary_key="id",
    )
    assert result.ran
    by_value = {f["value"]: f["count"] for f in result.findings}
    assert by_value.get(SQL_NULL_SENTINEL) == 2
    assert by_value.get("") == 2
    assert SQL_NULL_SENTINEL not in ("", None)
