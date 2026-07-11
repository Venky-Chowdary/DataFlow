"""Infer column types from sample values — enterprise-grade majority voting."""

from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime
from typing import Any

from services.transform_engine import (
    _parse_boolean,
    _parse_date,
    _parse_datetime,
    _parse_decimal,
    NULL_SENTINELS,
)

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
    if not s or s.lower() in NULL_SENTINELS:
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

    if _parse_boolean(s) is not None:
        return "BOOLEAN"

    if _parse_date(s) is not None:
        return "DATE"

    if _parse_datetime(s) is not None:
        return "TIMESTAMP"

    decimal_parsed = _parse_decimal(s)
    if decimal_parsed is not None:
        if "." in decimal_parsed or "e" in s.lower():
            return "DECIMAL"
        return "INTEGER"

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
