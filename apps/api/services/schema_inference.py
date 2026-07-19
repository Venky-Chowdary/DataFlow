"""Top-level schema intelligence engine for DataFlow.

Universal contract
------------------
Every connector path (Mongo, SQL, files, warehouses) must type columns through
this module before Map / preflight / CREATE TABLE. Rules are fail-safe:

1. **Values beat name guesses.** A typed logical type is emitted only when
   every non-empty sample parses as that type.
2. **Booleans are true/false family only** (true/false/yes/no/0/1/t/f/y/n/on/off).
   Words like ``active`` / ``inactive`` / ``pending`` / ``invalidated`` are
   **string enums**, never booleans.
3. **Name heuristics only disambiguate** (e.g. 0/1 on ``is_active`` → BOOLEAN;
   epoch digits on ``created_at`` → TIMESTAMP). Names never invent a type that
   samples contradict.
4. **Widen to VARCHAR/TEXT** on mixed or ambiguous columns — never invent a
   tight type that will fail dry-run on the next unseen value.

Public API: ``infer_type``, ``infer_column``, ``infer_columns_from_rows``.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime
from typing import Any

from services.transform_engine import (
    NULL_SENTINELS,
    _STRICT_BOOL_FALSE,
    _STRICT_BOOL_TRUE,
    _parse_boolean,
    _parse_date,
    _parse_datetime,
    _parse_decimal,
)

# Logical types emitted to mapping / preflight / DDL layers
LOGICAL_TYPES = frozenset({
    "INTEGER", "DECIMAL", "BOOLEAN", "DATE", "TIMESTAMP", "TIME",
    "VARCHAR", "TEXT", "UUID", "JSON", "BINARY",
})

# Tokens that may become BOOLEAN only when the field name looks like a flag.
_BOOLEAN_STRINGS = _STRICT_BOOL_TRUE | _STRICT_BOOL_FALSE

# Status / lifecycle vocabulary — never treat as boolean literals.
_STATUS_ENUM_TOKENS = frozenset({
    "active", "inactive", "enabled", "disabled", "pending", "invalidated",
    "approved", "rejected", "completed", "cancelled", "canceled", "draft",
    "published", "archived", "deleted", "suspended", "locked", "unlocked",
    "open", "closed", "new", "old", "success", "failure", "failed", "passed",
    "processing", "processed", "queued", "running", "stopped", "paused",
    "ok", "error", "warning", "info", "unknown", "n/a", "na", "none",
    "positive", "negative", "aye", "nope",
})


def _is_boolean_field_name(name: str) -> bool:
    """True only for flag-shaped names — not bare status/lifecycle words.

    Matches: is_active, has_flag, deviceVerified, email_verified, enabled, *_bool.
    Rejects: status, state, active, completed, approved (those are usually enums).
    """
    n = (name or "").strip()
    if not n:
        return False
    # camelCase / snake prefix flags
    if re.search(r"(?:^|_)(is|has|can|should|was|are|do|does|did)(?:[A-Z_]|_)", n):
        return True
    if re.search(r"(?:^|_)(?:is|has)_[a-z0-9]", n, re.I):
        return True
    # Explicit flag/bool suffix or enabled/disabled/verified/confirmed as the
    # whole name or trailing token (deviceVerified, email_verified).
    if re.search(r"(?:^|_|[a-z])(?:flag|bool|enabled|disabled|verified|confirmed)$", n, re.I):
        return True
    if re.fullmatch(r"(?:enabled|disabled|verified|confirmed|flag|bool)", n, re.I):
        return True
    return False


_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.I,
)
_BASE64_RE = re.compile(r"^[A-Za-z0-9+/]+={0,2}$")
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_ENUM_TOKEN_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_\-./]{0,63}$")


def _is_base64(value: str) -> bool:
    s = value.strip()
    if len(s) < 12 or len(s) % 4 != 0:
        return False
    if not _BASE64_RE.match(s):
        return False
    if len(s) > 64 and len(set(s)) <= 3:
        return False
    if s.isalpha() and len(s) > 32:
        return False
    if all(c in "0123456789abcdefABCDEF" for c in s):
        return False
    return True


# Binary payloads only — not generic "data"/"key"/"token" (those are often IDs/JWTs).
_BINARY_FIELD_RE = re.compile(
    r"(?:^|_)(?:payload|binary|blob|bytea|bytes|b64|base64|image|audio|video|pdf|"
    r"attachment|thumbnail|avatar_bytes|file_bytes|raw_bytes)(?:_?\d*)?(?:$|_)",
    re.I,
)


def _is_binary_field_name(name: str) -> bool:
    return bool(_BINARY_FIELD_RE.search(name or ""))


_TIMESTAMP_FIELD_RE = re.compile(
    r"(?:^|_)(?:time|date|timestamp|datetime|epoch|unix|created|updated|modified|"
    r"logged|expires|occurred|scheduled|started|ended|birth)"
    r"\d*(?:$|_)"
    r"|(?:^|_)(?:created|updated|modified|logged|expires|occurred|started|ended)_(?:at|on)$"
    r"|_at$|_dt$",
    re.I,
)

_DATE_FIELD_RE = re.compile(
    r"(?:^|_)(?:date|day|dob|birth|yyyymmdd|txn_dt|pay_dt|trans_dt)(?:$|_)"
    r"|_date$|_dt$",
    re.I,
)


def _is_timestamp_field_name(name: str) -> bool:
    return bool(_TIMESTAMP_FIELD_RE.search(name or ""))


def _is_date_field_name(name: str) -> bool:
    return bool(_DATE_FIELD_RE.search(name or "")) or _is_timestamp_field_name(name)


def _valid_yyyymmdd(text: str) -> bool:
    if not re.fullmatch(r"\d{8}", text):
        return False
    try:
        dt = datetime.strptime(text, "%Y%m%d")
    except ValueError:
        return False
    return 1900 <= dt.year <= 2100


def _looks_like_string_enum(samples: list[str]) -> bool:
    """Low-cardinality symbolic tokens that are not strict booleans."""
    vals = [s.strip() for s in samples if s and str(s).strip()]
    if len(vals) < 1:
        return False
    distinct = {v.lower() for v in vals}
    if not distinct:
        return False
    if distinct <= _BOOLEAN_STRINGS:
        return False
    if len(distinct) > 32:
        return False
    if not all(_ENUM_TOKEN_RE.match(v) for v in vals):
        return False
    # Any status-vocabulary token, or >2 distinct labels → enum
    if distinct & _STATUS_ENUM_TOKENS:
        return True
    if len(distinct) >= 2 and all(not v.isdigit() for v in distinct):
        # Short alphabetic labels (pending/active/…)
        if all(len(v) <= 32 and v.replace("_", "").replace("-", "").isalpha() for v in distinct):
            return True
    return False


def _classify_value(value: str, *, field_name: str | None = None) -> str:
    s = value.strip()
    if not s or s.lower() in NULL_SENTINELS:
        return "VARCHAR"

    # Status vocabulary is always text — never boolean/date.
    if s.lower() in _STATUS_ENUM_TOKENS:
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
        # Defer 0/1 disambiguation to infer_type (field name known).
        if s in {"0", "1"}:
            return "INTEGER"
        return "BOOLEAN"

    # YYYYMMDD only when calendar-valid and field looks temporal (avoids SKUs/zips).
    if re.fullmatch(r"\d{8}", s):
        if _valid_yyyymmdd(s) and (field_name is None or _is_date_field_name(field_name) or "yyyymmdd" in (field_name or "").lower()):
            return "DATE"
        # Without a date-ish name, keep as integer/string later
        if _valid_yyyymmdd(s) and field_name is None:
            return "DATE"

    if _parse_date(s) is not None and not re.fullmatch(r"\d{8}", s):
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
    """Infer a single logical type for a column from sample values."""
    return str(infer_column(samples, field_name=field_name)["logical_type"])


def infer_schema_map(
    samples_by_field: dict[str, list[str]],
) -> tuple[dict[str, str], dict[str, dict[str, Any]]]:
    """Canonical choke point: field → logical type + full intelligence records.

    All introspect paths (Mongo, SQL sample refine, files) should call this
    instead of ad-hoc ``infer_type`` loops so semantic_role / notes stay attached.
    """
    schema: dict[str, str] = {}
    intel: dict[str, dict[str, Any]] = {}
    for field, samples in samples_by_field.items():
        rec = infer_column(samples, field_name=field)
        schema[field] = str(rec["logical_type"])
        intel[field] = rec
    return schema, intel


def samples_fit_logical_type(samples: list[str], logical_type: str, *, field_name: str | None = None) -> bool:
    """True when every non-empty sample coerces cleanly to ``logical_type``."""
    from services.transform_engine import apply_transform, infer_transform_for_mapping

    non_empty = [str(s).strip() for s in samples if s is not None and str(s).strip()]
    if not non_empty:
        return True
    lt = (logical_type or "VARCHAR").upper()
    if lt in {"VARCHAR", "TEXT", "STRING", "CHAR"}:
        return True
    # Re-infer; if engine widens away from proposed type, samples do not fit.
    inferred = infer_type(non_empty, field_name=field_name)
    if inferred in {"VARCHAR", "TEXT"} and lt not in {"VARCHAR", "TEXT", "STRING"}:
        return False
    transform = infer_transform_for_mapping(
        field_name or "col",
        field_name or "col",
        inferred,
        lt,
    )
    typed = {"boolean", "integer", "decimal", "date", "datetime", "time", "uuid", "json", "binary"}
    if transform not in typed and lt in {"BOOLEAN", "INTEGER", "DECIMAL", "DATE", "TIMESTAMP", "TIME", "UUID", "JSON", "BINARY"}:
        # Explicit typed DDL with only trim — verify via apply_transform alias
        engine_t = {
            "BOOLEAN": "boolean",
            "INTEGER": "integer",
            "DECIMAL": "decimal",
            "DATE": "date",
            "TIMESTAMP": "datetime",
            "TIME": "time",
            "UUID": "uuid",
            "JSON": "json",
            "BINARY": "binary",
        }.get(lt)
        if not engine_t:
            return True
        transform = engine_t
    if transform not in typed:
        return True
    for raw in non_empty[:200]:
        _val, err = apply_transform(raw, transform)
        if err:
            return False
    return True


def safe_ddl_logical_type(
    proposed: str,
    samples: list[str] | None,
    *,
    field_name: str | None = None,
    source_type: str | None = None,
) -> str:
    """For new-table DDL: never emit a tight type samples cannot all coerce to."""
    proposed_u = (proposed or source_type or "VARCHAR").upper()
    if proposed_u in {"STRING", "CHAR", "CHARACTER", "CHARACTER VARYING"}:
        proposed_u = "VARCHAR"
    if not samples:
        # Prefer source type when no samples; still avoid BOOLEAN without evidence.
        if proposed_u == "BOOLEAN" and source_type and str(source_type).upper() in {"VARCHAR", "TEXT", "STRING"}:
            return "VARCHAR"
        return proposed_u if proposed_u in LOGICAL_TYPES else "VARCHAR"
    if samples_fit_logical_type(samples, proposed_u, field_name=field_name):
        return proposed_u if proposed_u in LOGICAL_TYPES else "VARCHAR"
    # Widen using fresh inference (string enums → VARCHAR, etc.)
    return infer_type(samples, field_name=field_name)


def infer_column(
    samples: list[str], *, field_name: str | None = None
) -> dict[str, Any]:
    """Full schema-intelligence record for one column.

    Returns keys: logical_type, semantic_role, confidence, notes, samples.
    """
    non_empty = [s.strip() for s in samples if s and str(s).strip()]
    notes: list[str] = []
    if not non_empty:
        return {
            "name": field_name or "",
            "logical_type": "VARCHAR",
            "semantic_role": "unknown",
            "confidence": 0.5,
            "notes": ["no samples — default VARCHAR"],
            "samples": [],
        }

    # Explicit string-enum short-circuit (status=active/invalidated, state=pending, …)
    if _looks_like_string_enum(non_empty):
        notes.append("string enum vocabulary — VARCHAR (not BOOLEAN)")
        return {
            "name": field_name or "",
            "logical_type": "VARCHAR",
            "semantic_role": "string_enum",
            "confidence": 0.95,
            "notes": notes,
            "samples": non_empty[:8],
        }

    counts: Counter[str] = Counter(_classify_value(s, field_name=field_name) for s in non_empty)
    types = set(counts.keys())

    if types <= {"INTEGER", "DECIMAL"}:
        inferred = "DECIMAL" if "DECIMAL" in types else "INTEGER"
        role = "numeric"
    elif types <= {"DATE", "TIMESTAMP", "TIME"}:
        if counts.get("TIMESTAMP", 0) >= counts.get("DATE", 0) and counts.get("TIMESTAMP", 0) >= counts.get("TIME", 0):
            inferred = "TIMESTAMP"
        elif counts.get("DATE", 0) >= counts.get("TIME", 0):
            inferred = "DATE"
        else:
            inferred = "TIME"
        role = "temporal"
    elif len(types) == 1:
        inferred = next(iter(types))
        role = {
            "BOOLEAN": "boolean_flag",
            "UUID": "identifier",
            "JSON": "semi_structured",
            "BINARY": "binary",
            "TEXT": "text",
            "VARCHAR": "text",
        }.get(inferred, "unknown")
    else:
        if "TEXT" in counts and max(len(s) for s in non_empty if _classify_value(s, field_name=field_name) == "TEXT") > 255:
            inferred = "TEXT"
        else:
            inferred = "VARCHAR"
        role = "text"
        notes.append("mixed sample types — widened to lossless text")

    # 0/1 → BOOLEAN only on flag-shaped names
    if (
        inferred in {"INTEGER", "VARCHAR"}
        and field_name
        and _is_boolean_field_name(field_name)
        and all(v.lower() in _BOOLEAN_STRINGS for v in non_empty)
    ):
        inferred = "BOOLEAN"
        role = "boolean_flag"
        notes.append("0/1 on flag-shaped field name → BOOLEAN")

    # Never keep BOOLEAN if any sample is status vocabulary
    if inferred == "BOOLEAN" and any(v.lower() in _STATUS_ENUM_TOKENS for v in non_empty):
        inferred = "VARCHAR"
        role = "string_enum"
        notes.append("status vocabulary present — demoted BOOLEAN → VARCHAR")

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
                    # Invalid base64 padding/alphabet — treat as non-binary below.
                    continue
        if valid == len(non_empty):
            inferred = "BINARY"
            role = "binary"

    if (
        inferred == "TIMESTAMP"
        and field_name
        and not _is_timestamp_field_name(field_name)
    ):
        if all(re.match(r"^[+\-]?\d+$", v) for v in non_empty):
            try:
                for v in non_empty:
                    int(v)
                inferred = "INTEGER"
                role = "numeric"
                notes.append("long digits without temporal name — INTEGER not TIMESTAMP")
            except ValueError:
                inferred = "VARCHAR"
                role = "text"

    # YYYYMMDD without date-ish name → INTEGER/VARCHAR, not DATE
    if inferred == "DATE" and field_name and not _is_date_field_name(field_name):
        if all(re.fullmatch(r"\d{8}", v) for v in non_empty):
            inferred = "INTEGER"
            role = "numeric"
            notes.append("8-digit values without date-ish name — not DATE")

    confidence = 0.99 if len(types) == 1 and not notes else 0.85
    if role == "string_enum":
        confidence = 0.95

    return {
        "name": field_name or "",
        "logical_type": inferred,
        "semantic_role": role,
        "confidence": confidence,
        "notes": notes,
        "samples": non_empty[:8],
    }


def infer_columns_from_rows(headers: list[str], rows: list[list[Any]], *, max_samples: int = 50) -> list[dict]:
    columns = []
    sample_rows = rows[:max_samples]
    for i, name in enumerate(headers):
        samples = [str(row[i]) if i < len(row) else "" for row in sample_rows]
        intel = infer_column(samples, field_name=name)
        columns.append(
            {
                "name": name.strip() or f"column_{i + 1}",
                "inferred_type": intel["logical_type"],
                "semantic_role": intel["semantic_role"],
                "confidence": intel["confidence"],
                "notes": intel["notes"],
                "nullable": any(not str(s).strip() for s in samples),
                "samples": [s for s in samples[:5] if str(s).strip()],
            }
        )
    return columns
