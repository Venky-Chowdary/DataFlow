"""Map inferred column types to destination-native DDL types."""

from __future__ import annotations

from ..ai.knowledge.type_conversions import suggest_type_conversion

# Inferred schema types from file parser → canonical
CANONICAL_TYPES = {
    "string": "string",
    "integer": "integer",
    "number": "decimal",
    "decimal": "decimal",
    "boolean": "boolean",
    "null": "string",
    "array": "array",
    "object": "json",
    "datetime": "datetime",
    "date": "date",
}

DDL_TYPES = {
    "postgresql": {
        "string": "TEXT",
        "integer": "BIGINT",
        "decimal": "NUMERIC(18,4)",
        "boolean": "BOOLEAN",
        "datetime": "TIMESTAMPTZ",
        "date": "DATE",
        "json": "JSONB",
        "array": "JSONB",
    },
    "snowflake": {
        "string": "VARCHAR",
        "integer": "NUMBER(38,0)",
        "decimal": "NUMBER(18,4)",
        "boolean": "BOOLEAN",
        "datetime": "TIMESTAMP_TZ",
        "date": "DATE",
        "json": "VARIANT",
        "array": "VARIANT",
    },
    "mongodb": {
        "string": "string",
        "integer": "int",
        "decimal": "double",
        "boolean": "bool",
        "datetime": "date",
        "date": "date",
        "json": "object",
        "array": "array",
    },
}


def normalize_inferred(inferred: str) -> str:
    return CANONICAL_TYPES.get(inferred.lower(), "string")


def ddl_type(db_type: str, inferred: str) -> str:
    canonical = normalize_inferred(inferred)
    return DDL_TYPES.get(db_type.lower(), {}).get(canonical, "TEXT" if db_type == "postgresql" else "VARCHAR")


def build_column_types(columns: list[str], schema: dict[str, str]) -> dict[str, str]:
    return {col: normalize_inferred(schema.get(col, "string")) for col in columns}


def default_mappings(columns: list[str]) -> list[dict]:
    return [
        {"source": c, "target": c, "confidence": 0.95, "reason": "Direct mapping"}
        for c in columns
    ]
