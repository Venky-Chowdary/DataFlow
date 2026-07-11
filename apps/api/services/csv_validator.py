"""Full-file CSV validation — type consistency beyond preview sample."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from services.csv_profiler import detect_delimiter, detect_encoding

_INT_RE = re.compile(r"^-?\d+$")
_DEC_RE = re.compile(r"^-?\d+\.\d+$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")
_BOOL_VALUES = frozenset({"true", "false", "yes", "no", "1", "0", "y", "n"})


def _check_value(value: str, inferred: str) -> str | None:
    v = (value or "").strip()
    if not v:
        return None
    t = (inferred or "VARCHAR").upper()
    if t in ("INTEGER", "INT", "BIGINT", "NUMBER"):
        if not _INT_RE.match(v.replace(",", "")):
            return f"expected integer, got {v[:40]!r}"
    elif t in ("DECIMAL", "NUMERIC", "FLOAT", "DOUBLE"):
        cleaned = v.replace("$", "").replace(",", "")
        if not (_INT_RE.match(cleaned) or _DEC_RE.match(cleaned)):
            return f"expected number, got {v[:40]!r}"
    elif t in ("BOOLEAN", "BOOL"):
        if v.lower() not in _BOOL_VALUES:
            return f"expected boolean, got {v[:40]!r}"
    elif t in ("DATE", "TIMESTAMP", "DATETIME"):
        if not _DATE_RE.match(v) and "T" not in v:
            return f"expected date/time, got {v[:40]!r}"
    elif t == "JSON":
        if v[0:1] not in ("{", "["):
            return f"expected JSON, got {v[:40]!r}"
    return None


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
    lines = text.splitlines()
    if not lines:
        return {"ok": False, "rows_scanned": 0, "issues": ["File is empty"], "issue_count": 1}

    delim = detect_delimiter(lines[0])
    header_row = [h.strip() for h in lines[0].split(delim)]
    col_index = {h: i for i, h in enumerate(header_row)}

    issues: list[str] = []
    rows_scanned = 0
    limit = scan_limit_rows if scan_limit_rows is not None else len(lines) - 1

    for line_no, line in enumerate(lines[1 : limit + 1], start=2):
        if not line.strip():
            continue
        rows_scanned += 1
        parts = line.split(delim)
        for col, inferred in schema.items():
            idx = col_index.get(col)
            if idx is None or idx >= len(parts):
                continue
            err = _check_value(parts[idx], inferred)
            if err and len(issues) < max_issues:
                issues.append(f"row {line_no} · {col}: {err}")
        if len(issues) >= max_issues:
            break

    total_data_rows = max(0, len(lines) - 1)
    return {
        "ok": len(issues) == 0,
        "rows_scanned": rows_scanned,
        "total_rows": total_data_rows,
        "full_scan": scan_limit_rows is None or scan_limit_rows >= total_data_rows,
        "issues": issues,
        "issue_count": len(issues),
    }


def validate_csv_file_path(
    path: Path,
    headers: list[str],
    schema: dict[str, str],
    *,
    max_issues: int = 50,
) -> dict[str, Any]:
    content = path.read_bytes()
    # For very large files, scan up to 500k rows then extrapolate
    text = content.decode(detect_encoding(content), errors="replace")
    line_count = text.count("\n")
    scan_limit = None if line_count <= 500_000 else 500_000
    return validate_csv_content(content, headers, schema, max_issues=max_issues, scan_limit_rows=scan_limit)
