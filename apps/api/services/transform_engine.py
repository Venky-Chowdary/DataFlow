"""Deterministic transform execution for dry-run and write paths."""

from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

DATE_PATTERNS = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%d/%m/%Y",
    "%Y/%m/%d",
    "%m-%d-%Y",
    "%Y%m%d",
)


def _parse_date(value: str) -> str | None:
    text = value.strip()
    if not text:
        return None
    for fmt in DATE_PATTERNS:
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _parse_decimal(value: str) -> str | None:
    text = value.strip().replace(",", "")
    for sym in ("$", "€", "£", "¥"):
        text = text.replace(sym, "")
    text = text.strip()
    if not text:
        return None
    try:
        return str(Decimal(text))
    except InvalidOperation:
        return None


def infer_transform(source_col: str, target_col: str, inferred_type: str) -> str:
    src = source_col.upper()
    tgt = target_col.lower()
    if "amount" in tgt or "total" in tgt or "weight" in tgt or src in {"AMT", "PAY_AMT", "PAYMENT_AMT", "VALUE"}:
        return "decimal"
    if "date" in tgt or "dt" in src.lower() or inferred_type.upper() in {"DATE", "TIMESTAMP"}:
        return "date"
    if tgt.endswith("_id") or tgt.endswith("id") or src.endswith("_ID"):
        return "trim_id"
    if "qty" in tgt or "quantity" in tgt:
        return "decimal"
    return "trim"


def apply_transform(raw: str | None, transform: str) -> tuple[Any, str | None]:
    """Returns (value, error). Empty string becomes None for nullable columns."""
    if raw is None:
        return None, None
    text = str(raw).strip()
    if text == "":
        return None, None

    if transform == "decimal":
        parsed = _parse_decimal(text)
        if parsed is None:
            return None, f"Invalid decimal: {text!r}"
        return parsed, None

    if transform == "date":
        parsed = _parse_date(text)
        if parsed is None:
            return None, f"Invalid date: {text!r}"
        return parsed, None

    if transform in {"trim", "trim_id"}:
        cleaned = re.sub(r"\s+", " ", text)
        return cleaned, None

    return text, None


def dry_run_sample(
    *,
    headers: list[str],
    sample_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    sample_size: int = 100,
) -> tuple[bool, list[str]]:
    errors: list[str] = []
    source_idx = {h: i for i, h in enumerate(headers)}

    for m in mappings:
        transform = m.get("transform") or infer_transform(
            m["source"], m["target"], column_types.get(m["source"], "VARCHAR")
        )
        idx = source_idx.get(m["source"])
        if idx is None:
            errors.append(f"Source column missing: {m['source']}")
            continue
        checked = 0
        for row in sample_rows[:sample_size]:
            raw = row[idx] if idx < len(row) else ""
            _, err = apply_transform(raw, transform)
            if err:
                errors.append(f"{m['source']}→{m['target']}: {err}")
                break
            checked += 1
        if checked == 0 and sample_rows:
            errors.append(f"No sample values for {m['source']}")

    return len(errors) == 0, errors[:25]
