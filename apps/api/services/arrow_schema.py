"""Arrow / Parquet native schema → DataFlow logical carriers.

Prefer writer schema over sample inference so nested types, decimals, and
TIMESTAMPTZ polarity survive (Airbyte-class honesty gap when pandas flattens).
"""

from __future__ import annotations

from typing import Any


def arrow_type_to_logical(arrow_type: Any) -> str:
    """Map a ``pyarrow.DataType`` to a DataFlow logical / structural carrier."""
    try:
        import pyarrow.types as pat
    except ImportError:
        return "TEXT"

    t = arrow_type
    if pat.is_null(t):
        return "TEXT"
    if pat.is_boolean(t):
        return "BOOLEAN"
    if pat.is_int8(t) or pat.is_int16(t) or pat.is_int32(t) or pat.is_int64(t):
        return "INTEGER"
    if pat.is_uint64(t):
        # Unsigned 64-bit exceeds signed BIGINT — keep as DECIMAL.
        return "DECIMAL(20,0)"
    if pat.is_integer(t):
        return "INTEGER"
    if pat.is_decimal(t):
        return f"DECIMAL({t.precision},{t.scale})"
    if pat.is_float16(t) or pat.is_float32(t) or pat.is_float64(t):
        return "FLOAT"
    if pat.is_date(t):
        return "DATE"
    if pat.is_time(t):
        return "TIME"
    if pat.is_timestamp(t):
        return "TIMESTAMPTZ" if getattr(t, "tz", None) else "TIMESTAMP_NTZ"
    if pat.is_duration(t) or pat.is_interval(t):
        return "INTERVAL"
    if pat.is_binary(t) or pat.is_large_binary(t) or pat.is_fixed_size_binary(t):
        return "BINARY"
    if pat.is_string(t) or pat.is_large_string(t):
        return "TEXT"
    if getattr(pat, "is_uuid", None) and pat.is_uuid(t):
        return "UUID"
    if pat.is_list(t) or pat.is_large_list(t) or pat.is_fixed_size_list(t):
        value_type = getattr(t, "value_type", None)
        if value_type is not None:
            return f"ARRAY<{arrow_type_to_logical(value_type)}>"
        return "ARRAY"
    if pat.is_struct(t):
        parts: list[str] = []
        for i in range(t.num_fields):
            field = t.field(i)
            parts.append(f"{field.name}:{arrow_type_to_logical(field.type)}")
        return f"STRUCT<{', '.join(parts)}>" if parts else "JSON"
    if pat.is_map(t):
        key_t = arrow_type_to_logical(t.key_type)
        item_t = arrow_type_to_logical(t.item_type)
        return f"MAP<{key_t},{item_t}>"
    if pat.is_dictionary(t):
        return arrow_type_to_logical(t.value_type)
    return "TEXT"


def schema_from_arrow(schema: Any) -> dict[str, str]:
    """Return ``{column: logical_carrier}`` from a ``pyarrow.Schema``."""
    out: dict[str, str] = {}
    for field in schema:
        out[str(field.name)] = arrow_type_to_logical(field.type)
    return out


def columns_from_arrow_schema(schema: Any) -> list[dict[str, Any]]:
    """Column records suitable for upload / Map (native types, not sample-inferred)."""
    cols: list[dict[str, Any]] = []
    for field in schema:
        cols.append({
            "name": str(field.name),
            "inferred_type": arrow_type_to_logical(field.type),
            "nullable": bool(getattr(field, "nullable", True)),
            "source": "arrow_schema",
        })
    return cols
