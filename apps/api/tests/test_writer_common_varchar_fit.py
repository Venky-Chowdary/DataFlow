"""Bounded VARCHAR/NVARCHAR/CHAR write-path fit + quarantine."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.writer_common import (  # noqa: E402
    fits_varchar,
    quarantine_unfit_strings,
    string_storage_units,
)
from services.ddl_compatibility import parse_varchar_width  # noqa: E402


def test_parse_varchar_width_bounded_and_unbounded():
    assert parse_varchar_width("VARCHAR(10)") == 10
    assert parse_varchar_width("NVARCHAR(50)") == 50
    assert parse_varchar_width("VARCHAR2(4000)") == 4000
    assert parse_varchar_width("CHAR(3)") == 3
    assert parse_varchar_width("CHARACTER VARYING(20)") == 20
    assert parse_varchar_width("NVARCHAR(MAX)") is None
    assert parse_varchar_width("VARCHAR(MAX)") is None
    assert parse_varchar_width("TEXT") is None
    assert parse_varchar_width("STRING") is None
    assert parse_varchar_width("VARCHAR") is None  # bare = unlimited/unknown
    assert parse_varchar_width("DECIMAL(10,2)") is None


def test_fits_varchar_basic():
    assert fits_varchar("abc", 3, "VARCHAR(3)") is True
    assert fits_varchar("abcd", 3, "VARCHAR(3)") is False
    assert fits_varchar(None, 3, "VARCHAR(3)") is True


def test_nvarchar_counts_utf16_code_units():
    # U+1F600 😀 is one Python code point but two UTF-16 code units.
    emoji = "😀"
    assert len(emoji) == 1
    assert string_storage_units(emoji, "NVARCHAR(1)") == 2
    assert fits_varchar(emoji, 1, "NVARCHAR(1)") is False
    assert fits_varchar(emoji, 2, "NVARCHAR(2)") is True


def test_oracle_byte_semantics_counts_utf8_bytes():
    # “你” is one code point / 3 UTF-8 bytes — fits CHAR(1) but not BYTE(1).
    han = "你"
    assert string_storage_units(han, "VARCHAR(1 CHAR)") == 1
    assert fits_varchar(han, 1, "VARCHAR(1 CHAR)") is True
    assert string_storage_units(han, "VARCHAR(1 BYTE)") == 3
    assert fits_varchar(han, 1, "VARCHAR(1 BYTE)") is False
    assert fits_varchar(han, 3, "VARCHAR(3 BYTE)") is True
    assert parse_varchar_width("VARCHAR2(10 BYTE)") == 10
    assert parse_varchar_width("VARCHAR(10 CHAR)") == 10


def test_redshift_varchar_counts_utf8_bytes():
    # AWS Redshift VARCHAR(n) is byte-length — emoji is 4 UTF-8 bytes.
    emoji = "😀"
    assert string_storage_units(emoji, "VARCHAR(4)", dialect_label="Redshift VARCHAR") == 4
    assert fits_varchar(emoji, 4, "VARCHAR(4)", dialect_label="Redshift VARCHAR") is True
    assert fits_varchar(emoji, 3, "VARCHAR(3)", dialect_label="Redshift VARCHAR") is False
    # PostgreSQL stays on code points.
    assert fits_varchar(emoji, 1, "VARCHAR(1)", dialect_label="PostgreSQL VARCHAR") is True


def test_quarantine_holds_out_oversized_string():
    rows = [("too-long-value", "ok"), ("short", "fine")]
    details: list[dict] = []
    out = quarantine_unfit_strings(
        rows,
        ["name", "label"],
        ["VARCHAR(5)", "VARCHAR(50)"],
        details,
        policy="quarantine",
        dialect_label="VARCHAR",
    )
    assert out == [("short", "fine")]
    assert details and "exceeds VARCHAR(5)" in details[0]["reason"]


def test_coerce_null_nulls_oversized_cell():
    rows = [("toolong", "keep")]
    details: list[dict] = []
    out = quarantine_unfit_strings(
        rows,
        ["name", "label"],
        ["NVARCHAR(4)", "NVARCHAR(50)"],
        details,
        policy="coerce_null",
    )
    assert out == [(None, "keep")]
    assert details


def test_unlimited_carrier_skips_quarantine():
    rows = [("x" * 10_000,)]
    details: list[dict] = []
    out = quarantine_unfit_strings(
        rows,
        ["body"],
        ["NVARCHAR(MAX)"],
        details,
        policy="quarantine",
    )
    assert out == rows
    assert details == []


def test_apply_mssql_session_guards_sets_ansi_warnings():
    from connectors.write_resilience import apply_mssql_session_guards

    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur

    apply_mssql_session_guards(conn)

    executed = " ".join(str(c.args[0]) for c in cur.execute.call_args_list if c.args)
    assert "ANSI_WARNINGS ON" in executed
    assert "ANSI_PADDING ON" in executed
