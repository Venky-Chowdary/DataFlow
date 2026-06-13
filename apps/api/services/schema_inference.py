"""Infer column types from sample values."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any


def infer_type(samples: list[str]) -> str:
    non_empty = [s.strip() for s in samples if s and s.strip()]
    if not non_empty:
        return "VARCHAR"

    if all(_is_int(s) for s in non_empty):
        return "INTEGER"
    if all(_is_float(s) for s in non_empty):
        return "DECIMAL"
    if all(_is_bool(s) for s in non_empty):
        return "BOOLEAN"
    if all(_is_date(s) for s in non_empty):
        return "DATE"
    if all(_is_datetime(s) for s in non_empty):
        return "TIMESTAMP"

    max_len = max(len(s) for s in non_empty)
    if max_len > 255:
        return "TEXT"
    return "VARCHAR"


def _is_int(value: str) -> bool:
    try:
        int(value.replace(",", ""))
        return True
    except ValueError:
        return False


def _is_float(value: str) -> bool:
    try:
        float(value.replace(",", ""))
        return "." in value or "e" in value.lower()
    except ValueError:
        return False


def _is_bool(value: str) -> bool:
    return value.lower() in {"true", "false", "0", "1", "yes", "no", "y", "n"}


def _is_date(value: str) -> bool:
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            datetime.strptime(value, fmt)
            return True
        except ValueError:
            continue
    return False


def _is_datetime(value: str) -> bool:
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
    ):
        try:
            datetime.strptime(value.replace("Z", ""), fmt.replace("Z", ""))
            return True
        except ValueError:
            continue
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}", value))


def infer_columns_from_rows(headers: list[str], rows: list[list[Any]]) -> list[dict]:
    columns = []
    for i, name in enumerate(headers):
        samples = [str(row[i]) if i < len(row) else "" for row in rows[:20]]
        columns.append(
            {
                "name": name.strip() or f"column_{i + 1}",
                "inferred_type": infer_type(samples),
                "nullable": any(not s.strip() for s in samples),
                "samples": samples[:5],
            }
        )
    return columns
