"""Infer column types from sample values — enterprise-grade majority voting."""

from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime
from typing import Any

from services.transform_engine import (
    NULL_SENTINELS,
    _parse_boolean,
    _parse_date,
    _parse_datetime,
    _parse_decimal,
)

_BOOLEAN_FIELD_RE = re.compile(
    r"(?:^|_)(?:is|has|was|are|can|should|will|do|does|did|enable|enabled|disabled|active|flag|bool|status|"
    r"confirmed|verified|valid|success|passed|failed|locked|paid|sent|done|toggled|archived|deleted|"
    r"subscribed|premium|hidden|visible|public|private|readonly|read_only|write|approved|rejected|"
    r"processed|resolved|completed|cancelled|canceled|cancel|on|off|yes|no|true|false)"
    r"\d*(?:$|_)",
    re.I,
)

# Values accepted as boolean when the field name looks boolean
_BOOLEAN_STRINGS = {
    "0", "1", "true", "false", "yes", "no", "y", "n", "t", "f",
}


def _is_boolean_field_name(name: str) -> bool:
    return bool(_BOOLEAN_FIELD_RE.search(name or ""))

# Logical types emitted to mapping / preflight / DDL layers
LOGICAL_TYPES = frozenset({
    "INTEGER", "DECIMAL", "BOOLEAN", "DATE", "TIMESTAMP", "TIME",
    "VARCHAR", "TEXT", "UUID", "JSON", "BINARY",
})

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.I,
)
_BASE64_RE = re.compile(r"^[A-Za-z0-9+/]+={0,2}$")
_YYYYMMDD_RE = re.compile(r"^\d{8}$")
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


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
    # Pure hex strings (e.g. ObjectId, hashes) are not base64 payloads.
    if all(c in "0123456789abcdefABCDEF" for c in s):
        return False
    return True


_BINARY_FIELD_RE = re.compile(
    r"(?:^|_)(?:payload|binary|blob|bytea|bytes|b64|base64|data|file|doc|content|image|audio|video|pdf|signature|hash|checksum|key|secret|token|cipher|iv|salt|nonce|encoded|raw|dump|attachment)(?:_?\d*)?(?:$|_)",
    re.I,
)


def _is_binary_field_name(name: str) -> bool:
    return bool(_BINARY_FIELD_RE.search(name or ""))


# Date / time / timestamp field-name heuristics used to disambiguate long integer
# strings (10-13 digits) from Unix-epoch timestamps.  Without this, a phone
# number such as 5558675309 is misclassified as a timestamp.
_TIMESTAMP_FIELD_RE = re.compile(
    r"(?:^|_)(?:time|date|timestamp|datetime|epoch|unix|created|updated|modified|"
    r"logged|expires|occurred|at|on|dt|utc|timezone|scheduled|start|end|from|to)"
    r"\d*(?:$|_)",
    re.I,
)


def _is_timestamp_field_name(name: str) -> bool:
    return bool(_TIMESTAMP_FIELD_RE.search(name or ""))


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

    boolean_parsed = _parse_boolean(s)
    if boolean_parsed is not None:
        # Defer 0/1 disambiguation to infer_type, where the field name is known.
        if s in {"0", "1"}:
            return "INTEGER"
        return "BOOLEAN"

    if _parse_date(s) is not None:
        return "DATE"

    if _parse_datetime(s) is not None:
        return "TIMESTAMP"

    for fmt in ("%H:%M:%S", "%H:%M:%S.%f", "%H:%M:%S%z"):
        try:
            datetime.strptime(s.replace("Z", "+0000"), fmt.replace("Z", "+0000"))
            return "TIME"
        except ValueError:
            continue

    decimal_parsed = _parse_decimal(s)
    if decimal_parsed is not None:
        if "." in decimal_parsed or "e" in s.lower():
            return "DECIMAL"
        # MongoDB / BSON only supports signed 64-bit ints. Treat larger whole
        # numbers as strings so they are not silently mangled or rejected.
        try:
            iv = int(decimal_parsed)
        except (ValueError, TypeError):
            return "VARCHAR"
        if iv > 2**63 - 1 or iv < -(2**63):
            return "VARCHAR"
        return "INTEGER"

    if len(s) > 255:
        return "TEXT"
    if _EMAIL_RE.match(s):
        return "VARCHAR"
    return "VARCHAR"


def infer_type(
    samples: list[str], *, threshold: float = 0.85, field_name: str | None = None
) -> str:
    """Majority-vote type inference across sample values."""
    non_empty = [s.strip() for s in samples if s and str(s).strip()]
    if not non_empty:
        return "VARCHAR"

    counts: Counter[str] = Counter(_classify_value(s) for s in non_empty)
    best_type, best_count = counts.most_common(1)[0]
    ratio = best_count / len(non_empty)

    if ratio >= threshold:
        inferred = best_type
    else:
        # Mixed column — prefer safer wider types
        if "TEXT" in counts and max(len(s) for s in non_empty) > 255:
            inferred = "TEXT"
        elif counts.get("DECIMAL", 0) + counts.get("INTEGER", 0) >= len(non_empty) * 0.66:
            inferred = "DECIMAL" if counts.get("DECIMAL", 0) > 0 else "INTEGER"
        elif counts.get("TIMESTAMP", 0) + counts.get("DATE", 0) + counts.get("TIME", 0) >= len(non_empty) * 0.7:
            if counts.get("TIMESTAMP", 0) >= counts.get("DATE", 0) and counts.get("TIMESTAMP", 0) >= counts.get("TIME", 0):
                inferred = "TIMESTAMP"
            elif counts.get("DATE", 0) >= counts.get("TIME", 0):
                inferred = "DATE"
            else:
                inferred = "TIME"
        else:
            # Mixed columns that are not clearly numeric or date/time should stay
            # as VARCHAR.  This prevents aggressive boolean/date inference from
            # turning values like "pending" or partial dates into transform errors.
            inferred = "VARCHAR"

    # Disambiguate 0/1 numeric columns from boolean flags using the field name
    if (
        inferred in {"INTEGER", "VARCHAR"}
        and field_name
        and _is_boolean_field_name(field_name)
        and all(v.lower() in _BOOLEAN_STRINGS for v in non_empty)
    ):
        return "BOOLEAN"

    # Short base64 payloads in fields like payload, binary, b64, blob, etc.
    if field_name and _is_binary_field_name(field_name):
        valid = 0
        for v in non_empty:
            s = v.strip()
            if len(s) >= 4 and len(s) % 4 == 0 and _BASE64_RE.match(s):
                try:
                    import base64

                    base64.b64decode(s, validate=True)
                    valid += 1
                except Exception:
                    pass
        if valid == len(non_empty):
            return "BINARY"

    # A column of long-integer strings (phone numbers, IDs, SSNs, etc.) must not
    # be inferred as a Unix-epoch timestamp unless the field name indicates a
    # date/time value.  YYYYMMDD values still come through as DATE above and are
    # left unchanged.
    if (
        inferred == "TIMESTAMP"
        and field_name
        and not _is_timestamp_field_name(field_name)
    ):
        if all(re.match(r"^[+\-]?\d+$", v) for v in non_empty):
            # If every value is a valid integer, prefer INTEGER; otherwise keep
            # it as a safe VARCHAR so no semantic transform is misapplied.
            try:
                for v in non_empty:
                    int(v)
                return "INTEGER"
            except ValueError:
                return "VARCHAR"

    return inferred


def infer_columns_from_rows(headers: list[str], rows: list[list[Any]], *, max_samples: int = 50) -> list[dict]:
    columns = []
    sample_rows = rows[:max_samples]
    for i, name in enumerate(headers):
        samples = [str(row[i]) if i < len(row) else "" for row in sample_rows]
        columns.append(
            {
                "name": name.strip() or f"column_{i + 1}",
                "inferred_type": infer_type(samples, field_name=name),
                "nullable": any(not str(s).strip() for s in samples),
                "samples": [s for s in samples[:5] if str(s).strip()],
            }
        )
    return columns
