"""MySQL / InnoDB create-table limits, checked before CREATE (DATA-17).

A create-new MySQL table that is too wide used to pass Validate and then die
in the driver with 1117 "Too many columns" or 1118 "Row size too large". The
three limits below are the ones MySQL 8.0 InnoDB enforces at CREATE TABLE
(DYNAMIC row format, 16 KiB pages, strict mode); each was measured on a live
server (``tests/test_w2dat_data17_mysql_row_limits.py``):

* columns: InnoDB stops at 1,017 user columns (MySQL's own cap is 4,096);
* the server row: the declared maximum of every column, BLOB/TEXT counting
  9-12 bytes, must stay within 65,535 bytes;
* the InnoDB record: a column of 256 bytes or more can move off-page (a
  40-byte local prefix stays), everything else is stored inline, and the
  record must stay below 8,126 bytes (half a page).
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Sequence

MYSQL_MAX_COLUMNS = 4096
INNODB_MAX_COLUMNS = 1017
MYSQL_MAX_ROW_BYTES = 65535
INNODB_MAX_RECORD_BYTES = 8126

CODE_TOO_MANY_COLUMNS = "MYSQL_TOO_MANY_COLUMNS"
CODE_ROW_TOO_LARGE = "MYSQL_ROW_TOO_LARGE"

_CHARSET_BYTES = {"latin1": 1, "ascii": 1, "binary": 1, "utf8mb3": 3, "utf8": 3, "ucs2": 2,
                  "utf16": 4, "utf16le": 4, "utf32": 4, "utf8mb4": 4}
_FIXED = {"TINYINT": 1, "BOOL": 1, "BOOLEAN": 1, "SMALLINT": 2, "MEDIUMINT": 3, "INT": 4,
          "INTEGER": 4, "BIGINT": 8, "SERIAL": 8, "FLOAT": 4, "DOUBLE": 8, "REAL": 8, "DATE": 3,
          "YEAR": 1}
_LOB = {"TINYTEXT": 9, "TINYBLOB": 9, "TEXT": 10, "BLOB": 10, "MEDIUMTEXT": 11, "MEDIUMBLOB": 11,
        "LONGTEXT": 12, "LONGBLOB": 12, "JSON": 12, "GEOMETRY": 12, "POINT": 12, "LINESTRING": 12,
        "POLYGON": 12, "MULTIPOINT": 12, "MULTILINESTRING": 12, "MULTIPOLYGON": 12,
        "GEOMETRYCOLLECTION": 12, "GEOMCOLLECTION": 12}
_DIGIT_BYTES = (0, 1, 1, 2, 2, 3, 3, 4, 4, 4)


def _decimal_bytes(precision: int, scale: int) -> int:
    def part(digits: int) -> int:
        return (digits // 9) * 4 + _DIGIT_BYTES[digits % 9]

    return part(max(precision - scale, 0)) + part(scale)


def _args(text: str) -> list[int]:
    m = re.search(r"\(([^)]*)\)", text)
    if not m:
        return []
    return [int(x) for x in re.findall(r"\d+", m.group(1))]


def _charset_bytes(text: str) -> int:
    m = re.search(r"(?:CHARACTER\s+SET|CHARSET)\s+(\w+)", text, re.I) or re.search(
        r"COLLATE\s+([a-z0-9]+?)_", text, re.I
    )
    return _CHARSET_BYTES.get(m.group(1).lower(), 4) if m else 4


def column_shape(ddl_type: str) -> tuple[int, int | None, bool]:
    """``(server_bytes, fixed_inline_bytes or None, is_lob)`` for one MySQL column type."""
    text = " ".join(str(ddl_type or "").split())
    base = re.sub(r"\s*\(.*$|\s+.*$", "", text).upper()
    args = _args(text)
    if base in _LOB:
        return _LOB[base], None, True
    if base in _FIXED:
        return _FIXED[base], _FIXED[base], False
    if base in {"DECIMAL", "NUMERIC", "DEC", "FIXED"}:
        p = args[0] if args else 10
        s = args[1] if len(args) > 1 else 0
        n = _decimal_bytes(p, s)
        return n, n, False
    if base in {"DATETIME", "TIMESTAMP", "TIME"}:
        fsp = math.ceil((args[0] if args else 0) / 2)
        n = {"DATETIME": 5, "TIMESTAMP": 4, "TIME": 3}[base] + fsp
        return n, n, False
    if base == "BIT":
        n = math.ceil((args[0] if args else 1) / 8)
        return n, n, False
    if base in {"ENUM", "SET"}:
        return 2, 2, False
    if base in {"CHAR", "CHARACTER", "NCHAR", "BINARY"}:
        per = 1 if base == "BINARY" else (3 if base == "NCHAR" else _charset_bytes(text))
        n = (args[0] if args else 1) * per
        return n, (n if per == 1 else None), False
    if base in {"VARCHAR", "NVARCHAR", "VARBINARY", "CHARACTER VARYING"}:
        per = 1 if base == "VARBINARY" else (3 if base == "NVARCHAR" else _charset_bytes(text))
        n = (args[0] if args else 255) * per
        return n + (1 if n < 256 else 2), None, False
    return 8, 8, False


def mysql_create_limit_refusal(
    target_cols: Sequence[str],
    target_types: Sequence[str],
    *,
    key_cols: Iterable[str] = (),
    table: str = "",
) -> dict[str, object] | None:
    """Why MySQL would refuse this CREATE TABLE, or ``None`` when it fits.

    Returns ``{"code", "message", "columns"}``; ``columns`` names the widest
    variable-width columns (the ones to map to TEXT).
    """
    cols = list(target_cols)
    types = [str(t or "") for t in target_types]
    keys = {str(k) for k in key_cols}
    name = f"table `{table}`" if table else "this table"
    n = len(cols)
    if n > INNODB_MAX_COLUMNS:
        return {
            "code": CODE_TOO_MANY_COLUMNS,
            "message": (
                f"MySQL cannot create {name} with {n:,} columns: InnoDB tables hold at most "
                f"{INNODB_MAX_COLUMNS:,} columns (MySQL's own cap is {MYSQL_MAX_COLUMNS:,}). "
                "Select fewer columns, or split the table."
            ),
            "columns": [],
        }
    nullable = sum(1 for c in cols if c not in keys)
    null_bytes = math.ceil(nullable / 8)
    server = null_bytes
    record = 5 + null_bytes + 13 + (0 if keys else 6)
    wide: list[tuple[int, int, str]] = []
    for pos, (col, ddl) in enumerate(zip(cols, types)):
        size, fixed, lob = column_shape(ddl)
        server += size
        if fixed is not None:
            record += fixed
            continue
        raw = size - (0 if lob else (1 if size <= 256 else 2))
        record += 41 if (lob or raw >= 256) else raw + 1
        if not lob:
            wide.append((-raw, pos, col))
    wide.sort()
    widest = [c for _, _, c in wide[:5]]
    hint = (
        f" Map the widest VARCHAR columns ({', '.join(widest)}"
        f"{', …' if len(wide) > 5 else ''}) to TEXT, which counts only 10 bytes toward the limit,"
        " or select fewer columns."
        if widest else " Select fewer columns, or map some of them to TEXT."
    )
    if server > MYSQL_MAX_ROW_BYTES:
        return {
            "code": CODE_ROW_TOO_LARGE,
            "message": (
                f"MySQL cannot create {name}: a row could take {server:,} bytes and MySQL's "
                f"row limit is {MYSQL_MAX_ROW_BYTES:,} bytes (TEXT and BLOB columns count 9-12 "
                f"bytes).{hint}"
            ),
            "columns": widest,
        }
    if record >= INNODB_MAX_RECORD_BYTES:
        return {
            "code": CODE_ROW_TOO_LARGE,
            "message": (
                f"MySQL cannot create {name}: InnoDB would store up to {record:,} bytes of "
                f"each row in the page, and the limit is {INNODB_MAX_RECORD_BYTES:,} bytes. Columns "
                "of 256 bytes or more and TEXT keep 41 bytes there; smaller and fixed-width ones "
                f"are stored whole.{hint}"
            ),
            "columns": widest,
        }
    return None
