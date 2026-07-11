"""Deterministic transform execution for dry-run and write paths."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import unicodedata
import uuid as uuid_lib
from datetime import datetime, timezone
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


def _to_utc_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_datetime(value: str) -> str | None:
    text = value.strip()
    if _EPOCH_MS_RE.match(text):
        ms = int(text)
        return _to_utc_z(datetime.fromtimestamp(ms / 1000, tz=timezone.utc))
    if re.match(r"^\d{10}$", text):
        return _to_utc_z(datetime.fromtimestamp(int(text), tz=timezone.utc))
    try:
        iso = text.replace("Z", "+00:00")
        return _to_utc_z(datetime.fromisoformat(iso))
    except ValueError:
        pass
    for fmt in DATETIME_PATTERNS:
        try:
            parsed = datetime.strptime(text, fmt)
            return _to_utc_z(parsed)
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


def _normalize_numeric_text(value: str) -> str:
    """Normalize unicode spaces, accounting formats, and percent suffixes."""
    text = unicodedata.normalize("NFKC", value)
    for ch in ("\u00a0", "\u2007", "\u202f", "\u2009"):
        text = text.replace(ch, "")
    text = text.strip()
    if text.endswith("%"):
        text = text[:-1].strip()
    # Accounting negative: (1,234.56) or 1,234.56-
    if text.startswith("(") and text.endswith(")"):
        text = f"-{text[1:-1].strip()}"
    if text.endswith("-") and text[:-1].strip():
        text = f"-{text[:-1].strip()}"
    return text


def _parse_decimal(value: str) -> str | None:
    text = _normalize_numeric_text(value.strip())
    for sym in ("$", "€", "£", "¥", "₹", "₩"):
        text = text.replace(sym, "")
    text = text.replace(",", "").strip()
    if not text:
        return None
    # Scientific notation: 1.5e3, 2E-4
    if re.match(r"^-?\d+(\.\d+)?[eE][+-]?\d+$", text):
        try:
            return str(Decimal(text))
        except InvalidOperation:
            return None
    try:
        return str(Decimal(text))
    except InvalidOperation:
        return None


def _parse_integer(value: str) -> int | None:
    text = _normalize_numeric_text(value.strip()).replace(",", "")
    if not text:
        return None
    if re.match(r"^-?\d+(\.\d+)?[eE][+-]?\d+$", text):
        try:
            dec = Decimal(text)
            if dec != dec.to_integral_value():
                return None
            return int(dec)
        except (InvalidOperation, ValueError):
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


def _parse_uuid(value: str) -> str | None:
    text = value.strip()
    try:
        return str(uuid_lib.UUID(text))
    except ValueError:
        return None


def _hash_pii(value: str) -> str:
    secret = os.getenv("DATAFLOW_PII_HASH_KEY", os.getenv("DATAFLOW_SECRET", "dataflow-dev-key"))
    digest = hmac.new(secret.encode("utf-8"), value.encode("utf-8"), hashlib.sha256).hexdigest()
    return digest[:32]


KNOWN_TRANSFORMS = frozenset({
    "decimal", "integer", "boolean", "date", "datetime", "json", "binary",
    "trim", "trim_id", "uuid", "upper", "lower", "hash_pii", "none", "identity",
})


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
    return infer_transform_for_mapping(source_col, target_col, inferred_type, None)


def infer_transform_for_mapping(
    source_col: str,
    target_col: str,
    source_type: str,
    target_type: str | None = None,
) -> str:
    """Pick transform from source/target logical types and column semantics."""
    from services.type_system import normalize_logical_type

    src = normalize_logical_type(source_type)
    tgt = normalize_logical_type(target_type) if target_type else None
    src_upper = source_type.upper()
    tgt_name = target_col.lower()

    if tgt == "integer" or (tgt is None and src_upper in {"INTEGER", "BIGINT", "INT", "SMALLINT"}):
        return "integer"
    if tgt == "decimal" or (tgt is None and src_upper in {"DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL"}):
        return "decimal"
    if tgt == "boolean" or (tgt is None and src_upper in {"BOOLEAN", "BOOL"}):
        return "boolean"
    if tgt == "json" or src_upper in {"JSON", "JSONB", "ARRAY", "OBJECT", "VARIANT"}:
        return "json"
    if tgt == "binary" or src_upper in {"BINARY", "BLOB", "BYTEA", "BYTES"}:
        return "binary"
    if tgt == "datetime" or src_upper in {"TIMESTAMP", "DATETIMETZ", "TIMESTAMP_TZ"}:
        return "datetime"
    if tgt == "date" or src_upper == "DATE":
        return "date"
    if tgt == "uuid" or src_upper == "UUID":
        return "uuid"

    src_col = source_col.upper()
    if "amount" in tgt_name or "total" in tgt_name or "weight" in tgt_name or src_col in {
        "AMT", "PAY_AMT", "PAYMENT_AMT", "VALUE",
    }:
        return "decimal"
    src_lower = source_col.lower()
    if (
        "date" in tgt_name
        or tgt_name.endswith("_dt")
        or src_col.endswith("_DT")
        or src_col in {"TXN_DT", "PAY_DT", "PAYMENT_DT", "TRANS_DT"}
        or src_upper in {"DATE", "TIMESTAMP"}
    ):
        return "datetime" if src_upper == "TIMESTAMP" or "epoch" in src_lower else "date"
    if tgt_name.endswith("_id") or tgt_name.endswith("id") or src_col.endswith("_ID"):
        return "trim_id"
    if "qty" in tgt_name or "quantity" in tgt_name:
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

    if transform in {"trim", "trim_id"}:
        cleaned = re.sub(r"\s+", " ", text)
        return cleaned, None

    if transform == "uuid":
        parsed = _parse_uuid(text)
        if parsed is None:
            return None, f"Invalid UUID: {text!r}"
        return parsed, None

    if transform in {"none", "identity"}:
        return text, None

    if transform == "upper":
        return text.upper(), None

    if transform == "lower":
        return text.lower(), None

    if transform == "hash_pii":
        return _hash_pii(text), None

    if transform not in KNOWN_TRANSFORMS:
        return None, f"Unknown transform: {transform!r}"

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
        transform = m.get("transform") or infer_transform_for_mapping(
            m["source"],
            m["target"],
            column_types.get(m["source"], "VARCHAR"),
            m.get("target_type") or column_types.get(m["target"]),
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
