"""Avro writer-schema → DataFlow logical carriers.

Prefer the embedded Avro contract over first-record key inference so empty
defaults, decimals, and nested types survive Map / preflight (Airbyte-class gap).
"""

from __future__ import annotations

from typing import Any


def _unwrap_union(avro_type: Any) -> tuple[Any, bool]:
    """Return ``(non_null_branch, nullable)`` for Avro unions like ``["null", "string"]``."""
    if not isinstance(avro_type, list):
        return avro_type, False
    non_null = [t for t in avro_type if not (isinstance(t, str) and t == "null")]
    nullable = len(non_null) < len(avro_type)
    if len(non_null) == 1:
        return non_null[0], nullable
    if not non_null:
        return "null", True
    # Multi-branch union — keep as JSON (honest polymorphic carrier).
    return {"type": "union", "branches": non_null}, nullable


def avro_type_to_logical(avro_type: Any) -> str:
    """Map an Avro type (string, dict, or union list) to a DataFlow logical carrier."""
    avro_type, _nullable = _unwrap_union(avro_type)

    if isinstance(avro_type, str):
        base = avro_type.lower()
        return {
            "null": "TEXT",
            "boolean": "BOOLEAN",
            "int": "INTEGER",
            "long": "INTEGER",
            "float": "FLOAT",
            "double": "FLOAT",
            "bytes": "BINARY",
            "string": "TEXT",
        }.get(base, "TEXT")

    if not isinstance(avro_type, dict):
        return "TEXT"

    type_name = str(avro_type.get("type") or "").lower()
    logical = str(avro_type.get("logicalType") or "").lower()

    if logical in {"decimal"}:
        precision = int(avro_type.get("precision") or 38)
        scale = int(avro_type.get("scale") or 0)
        return f"DECIMAL({precision},{scale})"
    if logical in {"uuid"}:
        return "UUID"
    if logical in {"date"}:
        return "DATE"
    if logical in {"time-millis", "time-micros"}:
        return "TIME"
    if logical in {"timestamp-millis", "timestamp-micros", "local-timestamp-millis", "local-timestamp-micros"}:
        if "local" in logical:
            return "TIMESTAMP_NTZ"
        return "TIMESTAMPTZ"
    if logical in {"duration"}:
        return "INTERVAL"

    if type_name in {"boolean", "int", "long", "float", "double", "bytes", "string", "null"}:
        return avro_type_to_logical(type_name)

    if type_name == "enum":
        return "TEXT"
    if type_name == "fixed":
        if logical == "decimal":
            precision = int(avro_type.get("precision") or 38)
            scale = int(avro_type.get("scale") or 0)
            return f"DECIMAL({precision},{scale})"
        return "BINARY"
    if type_name == "array":
        items = avro_type.get("items", "string")
        return f"ARRAY<{avro_type_to_logical(items)}>"
    if type_name == "map":
        values = avro_type.get("values", "string")
        return f"MAP<TEXT,{avro_type_to_logical(values)}>"
    if type_name == "record":
        parts: list[str] = []
        for field in avro_type.get("fields") or []:
            if not isinstance(field, dict):
                continue
            name = str(field.get("name") or "")
            if not name:
                continue
            parts.append(f"{name}:{avro_type_to_logical(field.get('type'))}")
        return f"STRUCT<{', '.join(parts)}>" if parts else "JSON"
    if type_name == "union":
        return "JSON"
    return "TEXT"


def schema_map_from_avro(schema: Any) -> dict[str, str]:
    """Return ``{field: logical}`` for a top-level Avro record schema."""
    schema, _ = _unwrap_union(schema)
    if isinstance(schema, str):
        try:
            import json

            schema = json.loads(schema)
        except Exception:
            return {}
    if not isinstance(schema, dict):
        return {}
    if str(schema.get("type") or "").lower() != "record":
        # Named type reference — treat whole payload as one column.
        return {"value": avro_type_to_logical(schema)}
    out: dict[str, str] = {}
    for field in schema.get("fields") or []:
        if not isinstance(field, dict):
            continue
        name = str(field.get("name") or "")
        if not name:
            continue
        out[name] = avro_type_to_logical(field.get("type"))
    return out


def columns_from_avro_schema(schema: Any) -> list[dict[str, Any]]:
    """Column records suitable for upload / Map (native Avro contract)."""
    schema_u, _ = _unwrap_union(schema)
    if isinstance(schema_u, str):
        try:
            import json

            schema_u = json.loads(schema_u)
        except Exception:
            schema_u = schema
    cols: list[dict[str, Any]] = []
    schema_map = schema_map_from_avro(schema_u)
    field_nullability: dict[str, bool] = {}
    if isinstance(schema_u, dict):
        for field in schema_u.get("fields") or []:
            if not isinstance(field, dict):
                continue
            name = str(field.get("name") or "")
            _, nullable = _unwrap_union(field.get("type"))
            # Default present also implies optional at write time.
            if "default" in field:
                nullable = True
            field_nullability[name] = nullable
    for name, logical in schema_map.items():
        cols.append({
            "name": name,
            "inferred_type": logical,
            "nullable": field_nullability.get(name, True),
            "source": "avro_schema",
        })
    return cols
