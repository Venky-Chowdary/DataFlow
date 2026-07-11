"""Full-file CSV validation — RFC-aware parsing with type consistency checks."""

from __future__ import annotations

import csv
import io
import re
from pathlib import Path
from typing import Any

from services.csv_profiler import detect_delimiter, detect_encoding
from services.transform_engine import _parse_boolean, _parse_date, _parse_datetime, _parse_integer

_INT_RE = re.compile(r"^-?\d+$")
_DEC_RE = re.compile(r"^-?\d+(\.\d+)?([eE][+-]?\d+)?$")
_BOOL_VALUES = frozenset({"true", "false", "yes", "no", "1", "0", "y", "n", "t", "f"})


def _check_value(value: str, inferred: str) -> str | None:
    v = (value or "").strip()
    if not v:
        return None
    t = (inferred or "VARCHAR").upper()
    if t in ("INTEGER", "INT", "BIGINT", "NUMBER"):
        if _parse_integer(v) is None:
            return f"expected integer, got {v[:40]!r}"
    elif t in ("DECIMAL", "NUMERIC", "FLOAT", "DOUBLE"):
        cleaned = v.replace("$", "").replace(",", "").replace("€", "").strip()
        if not _DEC_RE.match(cleaned) and _parse_integer(cleaned) is None:
            return f"expected number, got {v[:40]!r}"
    elif t in ("BOOLEAN", "BOOL"):
        if _parse_boolean(v) is None and v.lower() not in _BOOL_VALUES:
            return f"expected boolean, got {v[:40]!r}"
    elif t in ("DATE",):
        if not _parse_date(v):
            return f"expected date, got {v[:40]!r}"
    elif t in ("TIMESTAMP", "DATETIME"):
        if not _parse_datetime(v):
            return f"expected datetime, got {v[:40]!r}"
    elif t == "JSON":
        if v[0:1] not in ("{", "["):
            return f"expected JSON, got {v[:40]!r}"
    return None


def _parse_csv_rows(text: str, delimiter: str) -> tuple[list[str], list[list[str]]]:
    """RFC 4180-aware CSV parsing via stdlib csv module."""
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    rows = list(reader)
    if not rows:
        return [], []
    header = [h.strip() for h in rows[0]]
    data = [row for row in rows[1:] if any(cell.strip() for cell in row)]
    return header, data


def validate_csv_content(
    content: bytes,
    headers: list[str],
    schema: dict[str, str],
    *,
    max_issues: int = 50,
    scan_limit_rows: int | None = None,
) -> dict[str, Any]:
    """Scan CSV rows for type violations vs inferred schema."""
    text = content.decode(detect_encoding(content), errors="replace")
    if not text.strip():
        return {"ok": False, "rows_scanned": 0, "issues": ["File is empty"], "issue_count": 1}

    lines = text.splitlines()
    delim = detect_delimiter(lines[0]) if lines else ","
    header_row, data_rows = _parse_csv_rows(text, delim)

    if not header_row:
        return {"ok": False, "rows_scanned": 0, "issues": ["No header row"], "issue_count": 1}

    col_index = {h: i for i, h in enumerate(header_row)}
    issues: list[str] = []
    limit = scan_limit_rows if scan_limit_rows is not None else len(data_rows)
    rows_scanned = 0

    for line_no, parts in enumerate(data_rows[:limit], start=2):
        if not any(p.strip() for p in parts):
            continue
        rows_scanned += 1
        for col, inferred in schema.items():
            idx = col_index.get(col)
            if idx is None or idx >= len(parts):
                continue
            err = _check_value(parts[idx], inferred)
            if err and len(issues) < max_issues:
                issues.append(f"row {line_no} · {col}: {err}")
        if len(issues) >= max_issues:
            break

    total_data_rows = len(data_rows)
    return {
        "ok": len(issues) == 0,
        "rows_scanned": rows_scanned,
        "total_rows": total_data_rows,
        "full_scan": scan_limit_rows is None or scan_limit_rows >= total_data_rows,
        "issues": issues,
        "issue_count": len(issues),
        "parser": "csv_stdlib",
    }


def validate_csv_file_path(
    path: Path,
    headers: list[str],
    schema: dict[str, str],
    *,
    max_issues: int = 50,
) -> dict[str, Any]:
    content = path.read_bytes()
    text = content.decode(detect_encoding(content), errors="replace")
    line_count = text.count("\n")
    scan_limit = None if line_count <= 500_000 else 500_000
    return validate_csv_content(content, headers, schema, max_issues=max_issues, scan_limit_rows=scan_limit)
