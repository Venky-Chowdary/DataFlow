"""Deterministic transform execution for dry-run and write paths."""

from __future__ import annotations

import base64
import json
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

DATETIME_PATTERNS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f%z",
)


def _parse_datetime(value: str) -> str | None:
    text = value.strip()
    if _EPOCH_MS_RE.match(text):
        from datetime import timezone
        ms = int(text)
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if re.match(r"^\d{10}$", text):
        from datetime import timezone
        return datetime.fromtimestamp(int(text), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        iso = text.replace("Z", "+00:00")
        return datetime.fromisoformat(iso).strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        pass
    for fmt in DATETIME_PATTERNS:
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue
    parsed = _parse_date(text)
    return f"{parsed}T00:00:00Z" if parsed else None


_EPOCH_MS_RE = re.compile(r"^\d{13}$")


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
    if text.startswith("(") and text.endswith(")"):
        text = f"-{text[1:-1].strip()}"
    if not text:
        return None
    try:
        return str(Decimal(text))
    except InvalidOperation:
        return None


def _parse_integer(value: str) -> int | None:
    text = value.strip().replace(",", "")
    if not text:
        return None
    try:
        dec = Decimal(text)
    except InvalidOperation:
        return None
    if dec != dec.to_integral_value():
        return None
    return int(dec)


def _parse_boolean(value: str) -> bool | None:
    text = value.strip().lower()
    if text in {"true", "t", "yes", "y", "1"}:
        return True
    if text in {"false", "f", "no", "n", "0"}:
        return False
    return None


def _parse_json(value: str) -> str | None:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return None
    return json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))


def _parse_binary(value: str) -> str | None:
    text = value.strip()
    if not text:
        return None
    try:
        base64.b64decode(text, validate=True)
        return text
    except Exception:
        return base64.b64encode(text.encode("utf-8")).decode("ascii")


def infer_transform(source_col: str, target_col: str, inferred_type: str) -> str:
    src = source_col.upper()
    tgt = target_col.lower()
    logical = inferred_type.upper()
    if logical in {"JSON", "ARRAY", "OBJECT"}:
        return "json"
    if logical in {"BINARY", "BLOB", "BYTEA"}:
        return "binary"
    if logical in {"BOOLEAN", "BOOL"}:
        return "boolean"
    if logical in {"INTEGER", "BIGINT", "INT"}:
        return "integer"
    if logical == "UUID":
        return "uuid"
    if "amount" in tgt or "total" in tgt or "weight" in tgt or src in {"AMT", "PAY_AMT", "PAYMENT_AMT", "VALUE"}:
        return "decimal"
    if "date" in tgt or "dt" in src.lower() or inferred_type.upper() in {"DATE", "TIMESTAMP"}:
        return "datetime" if inferred_type.upper() == "TIMESTAMP" or "epoch" in src.lower() else "date"
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

    if transform == "integer":
        parsed_int = _parse_integer(text)
        if parsed_int is None:
            return None, f"Invalid integer: {text!r}"
        return parsed_int, None

    if transform == "boolean":
        parsed_bool = _parse_boolean(text)
        if parsed_bool is None:
            return None, f"Invalid boolean: {text!r}"
        return parsed_bool, None

    if transform == "date":
        parsed = _parse_date(text)
        if parsed is None:
            return None, f"Invalid date: {text!r}"
        return parsed, None

    if transform == "datetime":
        parsed = _parse_datetime(text)
        if parsed is None:
            return None, f"Invalid datetime: {text!r}"
        return parsed, None

    if transform == "json":
        parsed_json = _parse_json(text)
        if parsed_json is None:
            return None, f"Invalid JSON: {text!r}"
        return parsed_json, None

    if transform == "binary":
        parsed_binary = _parse_binary(text)
        if parsed_binary is None:
            return None, f"Invalid binary: {text!r}"
        return parsed_binary, None

    if transform in {"trim", "trim_id", "uuid"}:
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
