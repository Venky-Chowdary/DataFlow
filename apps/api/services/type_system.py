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
    "tinyblob": LOGICAL_BINARY,
    "mediumblob": LOGICAL_BINARY,
    "longblob": LOGICAL_BINARY,
    "image": LOGICAL_BINARY,
    "raw": LOGICAL_BINARY,
    "long raw": LOGICAL_BINARY,
    "binary varying": LOGICAL_BINARY,
    "rowversion": LOGICAL_BINARY,
    "money": LOGICAL_DECIMAL,
    "smallmoney": LOGICAL_DECIMAL,
    "dec": LOGICAL_DECIMAL,
    "num": LOGICAL_DECIMAL,
    "decfloat": LOGICAL_DECIMAL,
    "float4": LOGICAL_DECIMAL,
    "float8": LOGICAL_DECIMAL,
    "binary_float": LOGICAL_DECIMAL,
    "binary_double": LOGICAL_DECIMAL,
    "single": LOGICAL_DECIMAL,
    "int16": LOGICAL_INTEGER,
    "int32": LOGICAL_INTEGER,
    "int64": LOGICAL_INTEGER,
    "int8": LOGICAL_INTEGER,
    "uint8": LOGICAL_INTEGER,
    "uint16": LOGICAL_INTEGER,
    "uint32": LOGICAL_INTEGER,
    "uint64": LOGICAL_INTEGER,
    "mediumint": LOGICAL_INTEGER,
    "mediumint unsigned": LOGICAL_INTEGER,
    "tinyint unsigned": LOGICAL_INTEGER,
    "smallint unsigned": LOGICAL_INTEGER,
    "int unsigned": LOGICAL_INTEGER,
    "bigint unsigned": LOGICAL_INTEGER,
    "serial": LOGICAL_INTEGER,
    "smallserial": LOGICAL_INTEGER,
    "bigserial": LOGICAL_INTEGER,
    "year": LOGICAL_INTEGER,
    "bit": LOGICAL_BOOLEAN,
    "year_month": LOGICAL_STRING,
    "interval": LOGICAL_STRING,
    "enum": LOGICAL_STRING,
    "set": LOGICAL_STRING,
    "inet": LOGICAL_STRING,
    "cidr": LOGICAL_STRING,
    "macaddr": LOGICAL_STRING,
    "macaddr8": LOGICAL_STRING,
    "geometry": LOGICAL_STRING,
    "geography": LOGICAL_STRING,
    "point": LOGICAL_STRING,
    "linestring": LOGICAL_STRING,
    "polygon": LOGICAL_STRING,
    "multipoint": LOGICAL_STRING,
    "multilinestring": LOGICAL_STRING,
    "multipolygon": LOGICAL_STRING,
    "geometrycollection": LOGICAL_STRING,
    "hstore": LOGICAL_JSON,
    "map": LOGICAL_JSON,
    "xml": LOGICAL_TEXT,
    "xmltype": LOGICAL_TEXT,
    "tsvector": LOGICAL_TEXT,
    "tsquery": LOGICAL_TEXT,
    "jsonpath": LOGICAL_TEXT,
    "uniqueidentifier": LOGICAL_UUID,
    "sql_variant": LOGICAL_STRING,
    "cursor": LOGICAL_STRING,
    "refcursor": LOGICAL_STRING,
    "oid": LOGICAL_INTEGER,
    "xid": LOGICAL_INTEGER,
    "tid": LOGICAL_INTEGER,
    "cid": LOGICAL_INTEGER,
    "vector": LOGICAL_STRING,
    "pg_lsn": LOGICAL_STRING,
    "character large object": LOGICAL_TEXT,
    "national character varying": LOGICAL_STRING,
    "national character": LOGICAL_STRING,
    "nchar": LOGICAL_STRING,
    "nvarchar": LOGICAL_STRING,
    "nvarchar2": LOGICAL_STRING,
    "varchar2": LOGICAL_STRING,
    "ntext": LOGICAL_TEXT,
    "tinytext": LOGICAL_TEXT,
    "mediumtext": LOGICAL_TEXT,
    "long varchar": LOGICAL_TEXT,
    "national character large object": LOGICAL_TEXT,
    "timestamp with time zone": LOGICAL_DATETIME,
    "timestamp without time zone": LOGICAL_DATETIME,
    "time with time zone": LOGICAL_TIME,
    "time without time zone": LOGICAL_TIME,
    "datetime2": LOGICAL_DATETIME,
    "smalldatetime": LOGICAL_DATETIME,
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
    "sqlite": {
        LOGICAL_STRING: "TEXT",
        LOGICAL_TEXT: "TEXT",
        LOGICAL_INTEGER: "INTEGER",
        LOGICAL_DECIMAL: "REAL",
        LOGICAL_BOOLEAN: "INTEGER",
        LOGICAL_DATE: "TEXT",
        LOGICAL_DATETIME: "TEXT",
        LOGICAL_TIME: "TEXT",
        LOGICAL_UUID: "TEXT",
        LOGICAL_JSON: "TEXT",
        LOGICAL_ARRAY: "TEXT",
        LOGICAL_BINARY: "BLOB",
    },
    "generic_sql": {
        LOGICAL_STRING: "TEXT",
        LOGICAL_TEXT: "TEXT",
        LOGICAL_INTEGER: "BIGINT",
        LOGICAL_DECIMAL: "NUMERIC(38,10)",
        LOGICAL_BOOLEAN: "BOOLEAN",
        LOGICAL_DATE: "DATE",
        LOGICAL_DATETIME: "TIMESTAMP",
        LOGICAL_TIME: "TIME",
        LOGICAL_UUID: "UUID",
        LOGICAL_JSON: "JSON",
        LOGICAL_ARRAY: "JSON",
        LOGICAL_BINARY: "BLOB",
    },
}

DEFAULT_DDL: Final[dict[str, str]] = {
    "postgresql": "TEXT",
    "mysql": "TEXT",
    "snowflake": "VARCHAR",
    "bigquery": "STRING",
    "mongodb": "string",
    "redshift": "VARCHAR(65535)",
    "sqlite": "TEXT",
    "generic_sql": "TEXT",
}


def normalize_logical_type(inferred: str | None) -> str:
    """Return a canonical logical type for parser, DB, and warehouse types."""
    raw = (inferred or "").strip()
    if not raw:
        return LOGICAL_STRING

    # Numeric DDL such as NUMBER(38,0) or DECIMAL(10,0) is integer when scale is 0,
    # while NUMBER(38,10) stays decimal.
    m = re.match(r"^([A-Za-z_ ]+?)\s*\(\s*\d+\s*(?:,\s*(\d+))?\s*\)$", raw)
    if m:
        base = m.group(1).strip().lower()
        if base in {"number", "numeric", "decimal"}:
            scale = m.group(2)
            if scale is not None and int(scale) == 0:
                return LOGICAL_INTEGER
            return LOGICAL_DECIMAL

    key = re.sub(r"\([^)]*\)", "", raw).strip().lower()
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
