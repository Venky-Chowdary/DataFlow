"""Universal logical type system and destination DDL mapping.

The transfer layer uses these logical types as a pivot between files, SQL
databases, warehouses, document stores, and exports. Unsupported or ambiguous
types intentionally fall back to lossless text/JSON representations.
"""

from __future__ import annotations

import re
from typing import Final

LOGICAL_STRING = "string"
LOGICAL_TEXT = "text"
LOGICAL_INTEGER = "integer"
LOGICAL_DECIMAL = "decimal"
LOGICAL_BOOLEAN = "boolean"
LOGICAL_DATE = "date"
LOGICAL_DATETIME = "datetime"
LOGICAL_TIME = "time"
LOGICAL_UUID = "uuid"
LOGICAL_JSON = "json"
LOGICAL_ARRAY = "array"
LOGICAL_BINARY = "binary"

LOSSLESS_TEXT_TYPES: Final[set[str]] = {
    LOGICAL_STRING,
    LOGICAL_TEXT,
    LOGICAL_UUID,
}

CANONICAL_TYPES: Final[dict[str, str]] = {
    "": LOGICAL_STRING,
    "null": LOGICAL_STRING,
    "none": LOGICAL_STRING,
    "varchar": LOGICAL_STRING,
    "char": LOGICAL_STRING,
    "character": LOGICAL_STRING,
    "character varying": LOGICAL_STRING,
    "string": LOGICAL_STRING,
    "str": LOGICAL_STRING,
    "text": LOGICAL_TEXT,
    "clob": LOGICAL_TEXT,
    "longtext": LOGICAL_TEXT,
    "mediumtext": LOGICAL_TEXT,
    "int": LOGICAL_INTEGER,
    "int2": LOGICAL_INTEGER,
    "int4": LOGICAL_INTEGER,
    "int8": LOGICAL_INTEGER,
    "integer": LOGICAL_INTEGER,
    "bigint": LOGICAL_INTEGER,
    "smallint": LOGICAL_INTEGER,
    "tinyint": LOGICAL_INTEGER,
    "serial": LOGICAL_INTEGER,
    "bigserial": LOGICAL_INTEGER,
    "number": LOGICAL_DECIMAL,
    "numeric": LOGICAL_DECIMAL,
    "decimal": LOGICAL_DECIMAL,
    "double": LOGICAL_DECIMAL,
    "double precision": LOGICAL_DECIMAL,
    "float": LOGICAL_DECIMAL,
    "float64": LOGICAL_DECIMAL,
    "real": LOGICAL_DECIMAL,
    "bool": LOGICAL_BOOLEAN,
    "boolean": LOGICAL_BOOLEAN,
    "date": LOGICAL_DATE,
    "datetime": LOGICAL_DATETIME,
    "timestamp": LOGICAL_DATETIME,
    "timestamp_tz": LOGICAL_DATETIME,
    "timestamp_ltz": LOGICAL_DATETIME,
    "timestamptz": LOGICAL_DATETIME,
    "time": LOGICAL_TIME,
    "timetz": LOGICAL_TIME,
    "uuid": LOGICAL_UUID,
    "guid": LOGICAL_UUID,
    "json": LOGICAL_JSON,
    "jsonb": LOGICAL_JSON,
    "object": LOGICAL_JSON,
    "variant": LOGICAL_JSON,
    "record": LOGICAL_JSON,
    "struct": LOGICAL_JSON,
    "array": LOGICAL_ARRAY,
    "list": LOGICAL_ARRAY,
    "binary": LOGICAL_BINARY,
    "bytes": LOGICAL_BINARY,
    "bytea": LOGICAL_BINARY,
    "blob": LOGICAL_BINARY,
    "varbinary": LOGICAL_BINARY,
}

DDL_TYPES: Final[dict[str, dict[str, str]]] = {
    "postgresql": {
        LOGICAL_STRING: "TEXT",
        LOGICAL_TEXT: "TEXT",
        LOGICAL_INTEGER: "BIGINT",
        LOGICAL_DECIMAL: "NUMERIC(38,10)",
        LOGICAL_BOOLEAN: "BOOLEAN",
        LOGICAL_DATE: "DATE",
        LOGICAL_DATETIME: "TIMESTAMPTZ",
        LOGICAL_TIME: "TIME",
        LOGICAL_UUID: "UUID",
        LOGICAL_JSON: "JSONB",
        LOGICAL_ARRAY: "JSONB",
        LOGICAL_BINARY: "BYTEA",
    },
    "mysql": {
        LOGICAL_STRING: "TEXT",
        LOGICAL_TEXT: "LONGTEXT",
        LOGICAL_INTEGER: "BIGINT",
        LOGICAL_DECIMAL: "DECIMAL(38,10)",
        LOGICAL_BOOLEAN: "BOOLEAN",
        LOGICAL_DATE: "DATE",
        LOGICAL_DATETIME: "DATETIME(6)",
        LOGICAL_TIME: "TIME",
        LOGICAL_UUID: "CHAR(36)",
        LOGICAL_JSON: "JSON",
        LOGICAL_ARRAY: "JSON",
        LOGICAL_BINARY: "LONGBLOB",
    },
    "snowflake": {
        LOGICAL_STRING: "VARCHAR",
        LOGICAL_TEXT: "VARCHAR",
        LOGICAL_INTEGER: "NUMBER(38,0)",
        LOGICAL_DECIMAL: "NUMBER(38,10)",
        LOGICAL_BOOLEAN: "BOOLEAN",
        LOGICAL_DATE: "DATE",
        LOGICAL_DATETIME: "TIMESTAMP_TZ",
        LOGICAL_TIME: "TIME",
        LOGICAL_UUID: "VARCHAR",
        LOGICAL_JSON: "VARIANT",
        LOGICAL_ARRAY: "VARIANT",
        LOGICAL_BINARY: "BINARY",
    },
    "bigquery": {
        LOGICAL_STRING: "STRING",
        LOGICAL_TEXT: "STRING",
        LOGICAL_INTEGER: "INT64",
        LOGICAL_DECIMAL: "BIGNUMERIC",
        LOGICAL_BOOLEAN: "BOOL",
        LOGICAL_DATE: "DATE",
        LOGICAL_DATETIME: "TIMESTAMP",
        LOGICAL_TIME: "TIME",
        LOGICAL_UUID: "STRING",
        LOGICAL_JSON: "JSON",
        LOGICAL_ARRAY: "JSON",
        LOGICAL_BINARY: "BYTES",
    },
    "mongodb": {
        LOGICAL_STRING: "string",
        LOGICAL_TEXT: "string",
        LOGICAL_INTEGER: "long",
        LOGICAL_DECIMAL: "decimal",
        LOGICAL_BOOLEAN: "bool",
        LOGICAL_DATE: "date",
        LOGICAL_DATETIME: "date",
        LOGICAL_TIME: "string",
        LOGICAL_UUID: "string",
        LOGICAL_JSON: "object",
        LOGICAL_ARRAY: "array",
        LOGICAL_BINARY: "binData",
    },
    "redshift": {
        LOGICAL_STRING: "VARCHAR(65535)",
        LOGICAL_TEXT: "VARCHAR(65535)",
        LOGICAL_INTEGER: "BIGINT",
        LOGICAL_DECIMAL: "DECIMAL(38,10)",
        LOGICAL_BOOLEAN: "BOOLEAN",
        LOGICAL_DATE: "DATE",
        LOGICAL_DATETIME: "TIMESTAMP",
        LOGICAL_TIME: "TIME",
        LOGICAL_UUID: "VARCHAR(36)",
        LOGICAL_JSON: "SUPER",
        LOGICAL_ARRAY: "SUPER",
        LOGICAL_BINARY: "VARBYTE",
    },
}

DEFAULT_DDL: Final[dict[str, str]] = {
    "postgresql": "TEXT",
    "mysql": "TEXT",
    "snowflake": "VARCHAR",
    "bigquery": "STRING",
    "mongodb": "string",
    "redshift": "VARCHAR(65535)",
}


def normalize_logical_type(inferred: str | None) -> str:
    """Return a canonical logical type for parser, DB, and warehouse types."""
    key = (inferred or "").strip().lower()
    key = re.sub(r"\([^)]*\)", "", key).strip()
    key = key.replace("_", " ")
    return CANONICAL_TYPES.get(key, CANONICAL_TYPES.get(key.replace(" ", "_"), LOGICAL_STRING))


def ddl_type(db_type: str, inferred: str | None) -> str:
    """Map a logical source type to a destination-native DDL type."""
    db = (db_type or "").strip().lower()
    logical = normalize_logical_type(inferred)
    return DDL_TYPES.get(db, {}).get(logical, DEFAULT_DDL.get(db, "TEXT"))


def is_structural_type(inferred: str | None) -> bool:
    return normalize_logical_type(inferred) in {LOGICAL_JSON, LOGICAL_ARRAY}


def is_binary_type(inferred: str | None) -> bool:
    return normalize_logical_type(inferred) == LOGICAL_BINARY


def is_lossy_coercion(source_type: str, target_type: str) -> bool:
    """True when converting source→target may lose precision or fail silently."""
    src = normalize_logical_type(source_type)
    tgt = normalize_logical_type(target_type)
    if src == tgt:
        return False
    if tgt in LOSSLESS_TEXT_TYPES or tgt == LOGICAL_JSON:
        return False
    if src in {LOGICAL_JSON, LOGICAL_ARRAY} and tgt in {LOGICAL_INTEGER, LOGICAL_DECIMAL, LOGICAL_BOOLEAN}:
        return True
    if src == LOGICAL_BINARY and tgt != LOGICAL_BINARY:
        return True
    if src in {LOGICAL_DATETIME, LOGICAL_DATE} and tgt == LOGICAL_DATE and src == LOGICAL_DATETIME:
        return True
    if src == LOGICAL_DECIMAL and tgt == LOGICAL_INTEGER:
        return True
    if src in LOSSLESS_TEXT_TYPES and tgt in {
        LOGICAL_INTEGER, LOGICAL_DECIMAL, LOGICAL_BOOLEAN, LOGICAL_DATE, LOGICAL_DATETIME, LOGICAL_BINARY,
    }:
        return True
    if src == LOGICAL_DATETIME and tgt == LOGICAL_DATE:
        return True
    return False


def build_column_types(columns: list[str], schema: dict[str, str]) -> dict[str, str]:
    """Return uppercase logical types for writer compatibility."""
    return {col: normalize_logical_type(schema.get(col, "string")).upper() for col in columns}


def default_mappings(columns: list[str]) -> list[dict]:
    return [
        {"source": c, "target": c, "confidence": 0.95, "reason": "Direct mapping"}
        for c in columns
    ]
