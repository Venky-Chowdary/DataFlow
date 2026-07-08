"""Infer column types from sample values — enterprise-grade majority voting."""

from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime
from typing import Any

# Logical types emitted to mapping / preflight / DDL layers
LOGICAL_TYPES = frozenset({
    "INTEGER", "DECIMAL", "BOOLEAN", "DATE", "TIMESTAMP",
    "VARCHAR", "TEXT", "UUID", "JSON", "BINARY",
})

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.I,
)
_BASE64_RE = re.compile(r"^[A-Za-z0-9+/]+={0,2}$")


def _is_base64(value: str) -> bool:
    s = value.strip()
    if len(s) < 12 or len(s) % 4 != 0:
        return False
    if not _BASE64_RE.match(s):
        return False
    # Avoid classifying long plain-text runs as base64
    if len(s) > 64 and len(set(s)) <= 3:
        return False
    if s.isalpha() and len(s) > 32:
        return False
    return True
_EPOCH_MS_RE = re.compile(r"^\d{13}$")
_EPOCH_S_RE = re.compile(r"^\d{10}$")
_YYYYMMDD_RE = re.compile(r"^\d{8}$")
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _classify_value(value: str) -> str:
    s = value.strip()
    if not s:
        return "VARCHAR"

    if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
        try:
            json.loads(s)
            return "JSON"
        except json.JSONDecodeError:
            pass

    if _UUID_RE.match(s):
        return "UUID"

    if _is_base64(s):
        return "BINARY"

    low = s.lower()
    if low in {"true", "false", "yes", "no", "y", "n"}:
        return "BOOLEAN"
    if low in {"0", "1"} and len(s) == 1:
        return "BOOLEAN"

    if _EPOCH_MS_RE.match(s):
        return "TIMESTAMP"
    if _EPOCH_S_RE.match(s):
        return "TIMESTAMP"

    if _YYYYMMDD_RE.match(s):
        try:
            datetime.strptime(s, "%Y%m%d")
            return "DATE"
        except ValueError:
            pass

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
    ):
        try:
            datetime.strptime(s.replace("Z", "+0000"), fmt.replace("Z", "+0000"))
            return "TIMESTAMP"
        except ValueError:
            continue

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            datetime.strptime(s, fmt)
            return "DATE"
        except ValueError:
            continue

    cleaned = s.replace(",", "")
    if re.match(r"^-?\d+$", cleaned):
        return "INTEGER"
    try:
        float(cleaned)
        if "." in cleaned or "e" in cleaned.lower():
            return "DECIMAL"
        return "INTEGER"
    except ValueError:
        pass

    if len(s) > 255:
        return "TEXT"
    if _EMAIL_RE.match(s):
        return "VARCHAR"
    return "VARCHAR"


def infer_type(samples: list[str], *, threshold: float = 0.85) -> str:
    """Majority-vote type inference across sample values."""
    non_empty = [s.strip() for s in samples if s and str(s).strip()]
    if not non_empty:
        return "VARCHAR"

    counts: Counter[str] = Counter(_classify_value(s) for s in non_empty)
    best_type, best_count = counts.most_common(1)[0]
    ratio = best_count / len(non_empty)

    if ratio >= threshold:
        return best_type

    # Mixed column — prefer safer wider types
    if "TEXT" in counts and max(len(s) for s in non_empty) > 255:
        return "TEXT"
    if counts.get("DECIMAL", 0) + counts.get("INTEGER", 0) >= len(non_empty) * 0.66:
        return "DECIMAL" if counts.get("DECIMAL", 0) > 0 else "INTEGER"
    if counts.get("TIMESTAMP", 0) + counts.get("DATE", 0) >= len(non_empty) * 0.7:
        return "TIMESTAMP" if counts.get("TIMESTAMP", 0) >= counts.get("DATE", 0) else "DATE"

    return best_type if ratio >= 0.6 else "VARCHAR"


def infer_columns_from_rows(headers: list[str], rows: list[list[Any]], *, max_samples: int = 50) -> list[dict]:
    columns = []
    sample_rows = rows[:max_samples]
    for i, name in enumerate(headers):
        samples = [str(row[i]) if i < len(row) else "" for row in sample_rows]
        columns.append(
            {
                "name": name.strip() or f"column_{i + 1}",
                "inferred_type": infer_type(samples),
                "nullable": any(not str(s).strip() for s in samples),
                "samples": [s for s in samples[:5] if str(s).strip()],
            }
        )
    return columns
