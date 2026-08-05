"""BigQuery type mapping for canonical schema discovery."""

from __future__ import annotations

import re
from typing import Any


def bq_to_logical(
    dtype: str,
    *,
    precision: int | None = None,
    scale: int | None = None,
    max_length: int | None = None,
) -> str:
    """Map BigQuery dtype strings to logical carriers (never invent TEXT).

    Nested ``ARRAY<T>`` / ``STRUCT<…>`` / ``RANGE<T>`` must be parsed *before*
    scalar token checks — otherwise ``RANGE<TIMESTAMP>`` falsely becomes
    ``TIMESTAMPTZ`` via a substring trap (Airbyte/Fivetran-class fidelity).

    ``BIGNUMERIC`` stays distinct from ``NUMERIC``/``DECIMAL`` so create-new and
    preflight can honor the (76,38) vs (38,9) contract (Google SQL docs).
    """
    raw = (dtype or "").strip()
    if not raw:
        return "TEXT"
    upper = raw.upper()

    # --- Nested / RANGE carriers (before any "TIMESTAMP" substring match) ---
    if (upper.startswith("ARRAY<") or upper.startswith("LIST<")) and raw.endswith(">"):
        inner = raw[raw.index("<") + 1 : -1].strip()
        if not inner:
            return "ARRAY"
        return f"ARRAY<{bq_to_logical(inner)}>"
    if upper in {"ARRAY", "LIST"}:
        return "ARRAY"

    if (upper.startswith("STRUCT<") or upper.startswith("RECORD<")) and raw.endswith(">"):
        from services.type_system import parse_struct_fields

        fields = parse_struct_fields(raw)
        if not fields:
            return "STRUCT"
        parts = [f"{name}:{bq_to_logical(typ)}" for name, typ in fields]
        return f"STRUCT<{', '.join(parts)}>"
    if upper in {"RECORD", "STRUCT"}:
        # Fielded nested — distinct from opaque JSON (G3 nested→document collapse).
        return "STRUCT"

    if upper.startswith("RANGE<") and raw.endswith(">"):
        inner = raw[raw.index("<") + 1 : -1].strip().upper()
        # BigQuery RANGE ↔ PostgreSQL range twins (no Snowflake native RANGE).
        if inner == "DATE":
            return "DATERANGE"
        if inner == "DATETIME":
            return "TSRANGE"
        if inner in {"TIMESTAMP", "TIMESTAMPTZ"}:
            return "TSTZRANGE"
        return f"RANGE<{inner}>" if inner else "RANGE"
    if upper == "RANGE":
        return "RANGE"

    # Parametric numeric from dtype string (INFORMATION_SCHEMA / DDL paste).
    m_num = re.match(
        r"^(NUMERIC|DECIMAL|BIGNUMERIC|BIGDECIMAL)\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)$",
        raw,
        re.I,
    )
    if m_num:
        kind = m_num.group(1).upper()
        p = int(m_num.group(2))
        s = int(m_num.group(3)) if m_num.group(3) is not None else None
        if kind in {"BIGNUMERIC", "BIGDECIMAL"}:
            return f"BIGNUMERIC({p},{s})" if s is not None else f"BIGNUMERIC({p})"
        return f"DECIMAL({p},{s})" if s is not None else f"DECIMAL({p})"

    d = upper
    if d in {"INT64", "INTEGER", "SMALLINT", "BIGINT", "TINYINT", "BYTEINT"}:
        return "INTEGER"
    if d in {"BIGNUMERIC", "BIGDECIMAL"}:
        if precision is not None and scale is not None:
            return f"BIGNUMERIC({int(precision)},{int(scale)})"
        if precision is not None:
            return f"BIGNUMERIC({int(precision)})"
        return "BIGNUMERIC"
    if d in {"NUMERIC", "DECIMAL"}:
        if precision is not None and scale is not None:
            return f"DECIMAL({int(precision)},{int(scale)})"
        if precision is not None:
            return f"DECIMAL({int(precision)})"
        return "DECIMAL"
    if d in {"FLOAT64", "FLOAT", "DOUBLE"}:
        return "FLOAT"
    if d == "BOOL":
        return "BOOLEAN"
    if d == "DATE":
        return "DATE"
    if d == "TIME":
        return "TIME"
    # BigQuery DATETIME is wall-clock NTZ; TIMESTAMP is UTC instant (TZ-aware).
    if d == "DATETIME":
        return "TIMESTAMP_NTZ"
    if d == "TIMESTAMP":
        return "TIMESTAMPTZ"
    if d == "INTERVAL":
        return "INTERVAL"
    if d == "BYTES" or d.startswith("BYTES("):
        if d.startswith("BYTES("):
            m_len = re.match(r"^BYTES\((\d+)\)$", d)
            if m_len:
                return f"BINARY({int(m_len.group(1))})"
        if max_length is not None and int(max_length) > 0:
            return f"BINARY({int(max_length)})"
        return "BINARY"
    if d == "JSON":
        return "JSON"
    if d == "GEOGRAPHY":
        return "GEOGRAPHY"
    if d == "STRING" or d.startswith("STRING("):
        if d.startswith("STRING("):
            m_len = re.match(r"^STRING\((\d+)\)$", d)
            if m_len:
                return f"STRING({int(m_len.group(1))})"
        if max_length is not None and int(max_length) > 0:
            return f"STRING({int(max_length)})"
        return "TEXT"
    return "TEXT"


def bq_field_to_logical(field: Any) -> str:
    """Preserve BigQuery RECORD/ARRAY nesting — never collapse child types to bare JSON."""
    precision = getattr(field, "precision", None)
    scale = getattr(field, "scale", None)
    max_length = getattr(field, "max_length", None)
    ftype = str(getattr(field, "field_type", "") or "")
    mode = str(getattr(field, "mode", "NULLABLE") or "NULLABLE").upper()
    children = list(getattr(field, "fields", None) or [])

    if ftype.upper() in {"RECORD", "STRUCT"} and children:
        parts: list[str] = []
        for child in children:
            child_mode = str(getattr(child, "mode", "NULLABLE") or "NULLABLE").upper()
            child_t = bq_field_to_logical(child)
            # Avoid double ARRAY<> when child is already REPEATED.
            if child_mode == "REPEATED" and not child_t.upper().startswith("ARRAY<"):
                child_t = f"ARRAY<{child_t}>"
            parts.append(f"{child.name}:{child_t}")
        base = f"STRUCT<{', '.join(parts)}>"
    else:
        base = bq_to_logical(
            ftype,
            precision=precision if isinstance(precision, int) else None,
            scale=scale if isinstance(scale, int) else None,
            max_length=max_length if isinstance(max_length, int) else None,
        )

    if mode == "REPEATED" and not base.upper().startswith("ARRAY<"):
        return f"ARRAY<{base}>"
    return base
