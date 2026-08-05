"""Snowflake type mapping for canonical schema discovery."""

from __future__ import annotations

import re
from typing import Any


def snowflake_to_logical(
    dtype: str,
    *,
    character_maximum_length: Any = None,
    numeric_precision: Any = None,
    numeric_scale: Any = None,
    datetime_precision: Any = None,
) -> str:
    """Map Snowflake data types, preserving VECTOR / NUMBER(p,s) / VARCHAR(n).

    INFORMATION_SCHEMA often returns bare ``TEXT`` / ``NUMBER`` / ``BINARY``;
    fold CHARACTER_MAXIMUM_LENGTH / NUMERIC_* / DATETIME_PRECISION so Map/G3
    and write quarantine see real physical capacity (Airbyte catalog-width class).
    """
    raw = (dtype or "").strip()
    d = raw.upper()

    def _as_int(value: Any) -> int | None:
        try:
            if value is None:
                return None
            n = int(value)
            return n if n > 0 else None
        except (TypeError, ValueError):
            return None

    char_len = _as_int(character_maximum_length)
    num_prec = _as_int(numeric_precision)
    num_scale = (
        int(numeric_scale)
        if numeric_scale is not None
        and str(numeric_scale).strip() != ""
        and str(numeric_scale).lstrip("-").isdigit()
        else None
    )
    dt_prec = _as_int(datetime_precision)

    # Structured nested FIRST — ``OBJECT(... NUMBER ...)`` / ``MAP(..., INT)``
    # must never hit the NUMBER/INT substring traps below (Snowflake structured
    # types docs; Iceberg list/struct/map ↔ SF ARRAY/OBJECT/MAP).
    from services.type_system import (
        parse_array_element,
        parse_map_key_value,
        parse_struct_fields,
    )

    arr_el = parse_array_element(raw)
    if arr_el is not None:
        return f"ARRAY<{snowflake_to_logical(arr_el)}>"
    if d == "ARRAY":
        return "ARRAY"

    if d.startswith("OBJECT(") and raw.endswith(")"):
        fields = parse_struct_fields(raw)
        if not fields:
            return "STRUCT"
        parts = [f"{name}:{snowflake_to_logical(typ)}" for name, typ in fields]
        return f"STRUCT<{', '.join(parts)}>"
    # Bare OBJECT is semi-structured (VARIANT-shaped); structured OBJECT(...) above.
    if d == "OBJECT":
        return "JSON"

    map_kv = parse_map_key_value(raw)
    if map_kv is not None:
        key_t, val_t = map_kv
        return f"MAP<{snowflake_to_logical(key_t)},{snowflake_to_logical(val_t)}>"
    if d == "MAP":
        return "MAP"

    if d == "VARIANT":
        return "JSON"

    if "VECTOR" in d:
        return raw  # keep VECTOR(FLOAT, n) carrier
    if "GEOGRAPHY" in d:
        return "GEOGRAPHY"
    if "GEOMETRY" in d:
        return "GEOMETRY"
    if "INTERVAL" in d:
        if "YEAR" in d and "MONTH" in d:
            return "INTERVAL YEAR TO MONTH"
        if "DAY" in d or "SECOND" in d:
            return "INTERVAL DAY TO SECOND"
        return "INTERVAL"
    if d == "BINARY" or d.startswith("BINARY("):
        m_bin = re.match(r"^BINARY\s*\(\s*(\d+)\s*\)$", d)
        width = int(m_bin.group(1)) if m_bin else char_len
        return f"BINARY({width})" if width else "BINARY"
    if d == "TIME" or d.startswith("TIME("):
        m_time = re.match(r"^TIME\s*\(\s*(\d+)\s*\)$", d)
        prec = int(m_time.group(1)) if m_time else dt_prec
        return f"TIME({prec})" if prec is not None else "TIME"
    # NUMBER(p,0) → INTEGER when p ≤ 18; else DECIMAL(p,0) (never silent BIGINT overflow).
    m = re.match(r"^(NUMBER|DECIMAL|NUMERIC)\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)$", d)
    if m:
        from services.type_system import zero_scale_numeric_carrier

        if m.group(3) is not None and int(m.group(3)) == 0:
            return zero_scale_numeric_carrier(int(m.group(2)))
        if m.group(3) is not None:
            return f"DECIMAL({m.group(2)},{m.group(3)})"
        return f"DECIMAL({m.group(2)})"
    if d in {"NUMBER", "DECIMAL", "NUMERIC"}:
        if num_prec is not None:
            from services.type_system import zero_scale_numeric_carrier

            scale = 0 if num_scale is None else int(num_scale)
            if scale == 0:
                return zero_scale_numeric_carrier(num_prec)
            return f"DECIMAL({num_prec},{scale})"
        return "DECIMAL"
    # Token-anchored integers only — never ``"INT" in d`` (breaks MAP/OBJECT).
    if re.match(
        r"^(INT|INTEGER|BIGINT|SMALLINT|TINYINT|BYTEINT)(\s*\(|$)",
        d,
    ):
        return "INTEGER"
    # Snowflake FLOAT / DOUBLE / REAL — approximate IEEE, not NUMBER.
    if d in {"FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL"} or d.startswith("FLOAT"):
        return "FLOAT"
    if "BOOLEAN" in d:
        return "BOOLEAN"
    if d == "DATE":
        return "DATE"
    # Preserve LTZ vs TZ polarity (Snowflake Openflow / Airbyte #80914 class).
    # TIMESTAMP_LTZ ≈ session-relative instant; TIMESTAMP_TZ pins per-row offset.
    if "TIMESTAMP_LTZ" in d:
        m_ltz = re.match(r"^TIMESTAMP_LTZ\s*\(\s*(\d+)\s*\)$", d)
        prec = int(m_ltz.group(1)) if m_ltz else dt_prec
        return f"TIMESTAMP_LTZ({prec})" if prec is not None else "TIMESTAMP_LTZ"
    if "TIMESTAMP_TZ" in d:
        m_tz = re.match(r"^TIMESTAMP_TZ\s*\(\s*(\d+)\s*\)$", d)
        prec = int(m_tz.group(1)) if m_tz else dt_prec
        return f"TIMESTAMP_TZ({prec})" if prec is not None else "TIMESTAMP_TZ"
    if "TIMESTAMP_NTZ" in d:
        if dt_prec is not None and "(" not in d:
            return f"TIMESTAMP_NTZ({dt_prec})"
        return "TIMESTAMP_NTZ"
    if "TIMESTAMP" in d:
        if dt_prec is not None and "(" not in d:
            return f"TIMESTAMP_NTZ({dt_prec})"
        return "TIMESTAMP_NTZ"
    # TEXT / VARCHAR / CHAR — fold CHARACTER_MAXIMUM_LENGTH (SF catalog often
    # returns bare TEXT even when the column was created as VARCHAR(n)).
    if d in {"TEXT", "VARCHAR", "CHAR", "CHARACTER", "STRING"} or d.startswith(
        ("VARCHAR(", "CHAR(", "CHARACTER(", "TEXT(")
    ):
        m_str = re.match(
            r"^(?:VARCHAR|CHAR|CHARACTER(?:\s+VARYING)?|TEXT|STRING)\s*\(\s*(\d+)\s*\)$",
            d,
        )
        width = int(m_str.group(1)) if m_str else char_len
        if width:
            return f"VARCHAR({width})"
        return "TEXT"
    if char_len:
        return f"VARCHAR({char_len})"
    return "TEXT"
