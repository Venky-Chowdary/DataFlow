"""Universal logical type system and destination DDL mapping.

ETL contract (Informatica / Airbyte / Fivetran class)
----------------------------------------------------
1. **Native → logical → native.** Source native types coerce into these logical
   types; writers map logical → destination DDL. Never carry Postgres/Oracle
   physical naming into another engine (see ``dialect_profiles.py``).
2. **Fail-fast preflight, quarantine at write.** Incompatible coercions are
   rejected or quarantined — never silently dropped.
3. **High-precision numerics.** Industry rule (Informatica high-precision /
   Databricks overflow guidance): values that exceed safe DECIMAL capacity are
   kept as exact *text* (scientific or fixed), not cast through float64, and
   must not abort an entire load when policy is quarantine.
4. **Extend here** — add logical types / DDL maps / decimal budgets in this
   module (or ``dialect_profiles``). Do not sprinkle one-off connector patches.
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
# Specialty types — native DDL where the engine supports them, else lossless text.
LOGICAL_INTERVAL = "interval"
LOGICAL_GEOGRAPHY = "geography"
LOGICAL_VECTOR = "vector"

# ---------------------------------------------------------------------------
# Decimal / integer wire budgets (shared by serializer + transform engine)
# ---------------------------------------------------------------------------
# Modest values use fixed-point text so scale is preserved (0.10 stays "0.10").
# Beyond these budgets we keep short scientific form — same idea as Informatica
# "store as string when precision exceeds platform DECIMAL" and Databricks
# guidance for Oracle NUMBER overflow. Never expand 1e+1000000 into a
# million-character string (that path raises decimal.Overflow mid-transfer).
DECIMAL_MAX_FIXED_ABS_EXP: Final[int] = 100
DECIMAL_MAX_FIXED_DIGITS: Final[int] = 512
# ~NUMBER(38) class integer digit budget for typed INTEGER transforms.
INTEGER_MAX_DIGITS: Final[int] = 38 * 2  # 76 ≈ allow slightly over common DDL

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
    "timestamp_ntz": LOGICAL_DATETIME,
    "timestamptz": LOGICAL_DATETIME,
    "timestamp ntz": LOGICAL_DATETIME,
    "timestamp ltz": LOGICAL_DATETIME,
    "timestamp tz": LOGICAL_DATETIME,
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
    "uint8": LOGICAL_INTEGER,
    "uint16": LOGICAL_INTEGER,
    "uint32": LOGICAL_INTEGER,
    "uint64": LOGICAL_DECIMAL,  # full unsigned 64-bit range — DECIMAL, not signed BIGINT
    "mediumint": LOGICAL_INTEGER,
    "mediumint unsigned": LOGICAL_INTEGER,
    "tinyint unsigned": LOGICAL_INTEGER,
    "smallint unsigned": LOGICAL_INTEGER,
    "int unsigned": LOGICAL_INTEGER,
    "bigint unsigned": LOGICAL_DECIMAL,  # MySQL BIGINT UNSIGNED → DECIMAL (fidelity)
    "smallserial": LOGICAL_INTEGER,
    "year": LOGICAL_INTEGER,
    "bit": LOGICAL_BOOLEAN,
    "year_month": LOGICAL_INTERVAL,
    "interval": LOGICAL_INTERVAL,
    "interval year to month": LOGICAL_INTERVAL,
    "interval day to second": LOGICAL_INTERVAL,
    "enum": LOGICAL_STRING,
    "set": LOGICAL_STRING,
    "inet": LOGICAL_STRING,
    "cidr": LOGICAL_STRING,
    "macaddr": LOGICAL_STRING,
    "macaddr8": LOGICAL_STRING,
    "geometry": LOGICAL_GEOGRAPHY,
    "geography": LOGICAL_GEOGRAPHY,
    "point": LOGICAL_GEOGRAPHY,
    "linestring": LOGICAL_GEOGRAPHY,
    "polygon": LOGICAL_GEOGRAPHY,
    "multipoint": LOGICAL_GEOGRAPHY,
    "multilinestring": LOGICAL_GEOGRAPHY,
    "multipolygon": LOGICAL_GEOGRAPHY,
    "geometrycollection": LOGICAL_GEOGRAPHY,
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
    "vector": LOGICAL_VECTOR,
    "halfvec": LOGICAL_VECTOR,
    "sparsevec": LOGICAL_VECTOR,
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
    "long varchar": LOGICAL_TEXT,
    "national character large object": LOGICAL_TEXT,
    "timestamp with time zone": LOGICAL_DATETIME,
    "timestamp without time zone": LOGICAL_DATETIME,
    "time with time zone": LOGICAL_TIME,
    "time without time zone": LOGICAL_TIME,
    "datetime2": LOGICAL_DATETIME,
    "smalldatetime": LOGICAL_DATETIME,
    "datetimeoffset": LOGICAL_DATETIME,
    "sysname": LOGICAL_STRING,
    "hierarchyid": LOGICAL_STRING,
    "nclob": LOGICAL_TEXT,
    "bfile": LOGICAL_BINARY,
    # Bare "long" is the lakehouse/Java 64-bit integer (Iceberg/Spark). Oracle's
    # deprecated LONG text LOB is rare; prefer integer fidelity for universal transfers.
    "long": LOGICAL_INTEGER,
    "fixed": LOGICAL_DECIMAL,  # MySQL FIXED synonym for DECIMAL
    "bit varying": LOGICAL_BINARY,
    "varbit": LOGICAL_BINARY,
    "citext": LOGICAL_STRING,
    "regclass": LOGICAL_STRING,
    "regtype": LOGICAL_STRING,
    "regproc": LOGICAL_STRING,
    "regnamespace": LOGICAL_STRING,
    "numrange": LOGICAL_STRING,
    "int4range": LOGICAL_STRING,
    "int8range": LOGICAL_STRING,
    "tsrange": LOGICAL_STRING,
    "tstzrange": LOGICAL_STRING,
    "daterange": LOGICAL_STRING,
    "uint128": LOGICAL_DECIMAL,
    "int128": LOGICAL_DECIMAL,
    "uint256": LOGICAL_DECIMAL,
    "int256": LOGICAL_DECIMAL,
    "hugeint": LOGICAL_DECIMAL,  # DuckDB
    "uhugeint": LOGICAL_DECIMAL,
    "super": LOGICAL_JSON,  # Redshift SUPER
    "document": LOGICAL_JSON,
    "bson": LOGICAL_JSON,
    "rowid": LOGICAL_STRING,
    "urowid": LOGICAL_STRING,
    "currency": LOGICAL_DECIMAL,
    "halfvec": LOGICAL_VECTOR,
    "sparsevec": LOGICAL_VECTOR,
}

DDL_TYPES: Final[dict[str, dict[str, str]]] = {
    "postgresql": {
        LOGICAL_STRING: "TEXT",
        LOGICAL_TEXT: "TEXT",
        LOGICAL_INTEGER: "BIGINT",
        LOGICAL_DECIMAL: "NUMERIC",
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
        LOGICAL_DECIMAL: "DECIMAL(38,15)",
        LOGICAL_BOOLEAN: "BOOLEAN",
        LOGICAL_DATE: "DATE",
        LOGICAL_DATETIME: "DATETIME(6)",
        LOGICAL_TIME: "TIME",
        LOGICAL_UUID: "CHAR(36)",
        LOGICAL_JSON: "JSON",
        LOGICAL_ARRAY: "JSON",
        LOGICAL_BINARY: "LONGBLOB",
    },
    "sqlserver": {
        LOGICAL_STRING: "NVARCHAR(MAX)",
        LOGICAL_TEXT: "NVARCHAR(MAX)",
        LOGICAL_INTEGER: "BIGINT",
        LOGICAL_DECIMAL: "DECIMAL(38,10)",
        LOGICAL_BOOLEAN: "BIT",
        LOGICAL_DATE: "DATE",
        LOGICAL_DATETIME: "DATETIME2",
        LOGICAL_TIME: "TIME",
        LOGICAL_UUID: "UNIQUEIDENTIFIER",
        LOGICAL_JSON: "NVARCHAR(MAX)",
        LOGICAL_ARRAY: "NVARCHAR(MAX)",
        LOGICAL_BINARY: "VARBINARY(MAX)",
    },
    "oracle": {
        LOGICAL_STRING: "VARCHAR2(4000)",
        LOGICAL_TEXT: "CLOB",
        LOGICAL_INTEGER: "NUMBER(38,0)",
        LOGICAL_DECIMAL: "NUMBER(38,10)",
        LOGICAL_BOOLEAN: "NUMBER(1)",
        LOGICAL_DATE: "DATE",
        LOGICAL_DATETIME: "TIMESTAMP WITH TIME ZONE",
        LOGICAL_TIME: "VARCHAR2(32)",
        LOGICAL_UUID: "VARCHAR2(36)",
        LOGICAL_JSON: "CLOB",
        LOGICAL_ARRAY: "CLOB",
        LOGICAL_BINARY: "BLOB",
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
        LOGICAL_DECIMAL: "DECIMAL(38,15)",
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
        # SQLite has no true fixed-point type; store decimals as TEXT to avoid
        # IEEE-754 precision loss for high-precision values.
        LOGICAL_DECIMAL: "TEXT",
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
        LOGICAL_DECIMAL: "NUMERIC(38,15)",
        LOGICAL_BOOLEAN: "BOOLEAN",
        LOGICAL_DATE: "DATE",
        LOGICAL_DATETIME: "TIMESTAMP",
        LOGICAL_TIME: "TIME",
        LOGICAL_UUID: "UUID",
        LOGICAL_JSON: "JSON",
        LOGICAL_ARRAY: "JSON",
        LOGICAL_BINARY: "BLOB",
    },
    # Databricks / Spark SQL lakehouse (Unity Catalog tables, Delta).
    "databricks": {
        LOGICAL_STRING: "STRING",
        LOGICAL_TEXT: "STRING",
        LOGICAL_INTEGER: "BIGINT",
        LOGICAL_DECIMAL: "DECIMAL(38,10)",
        LOGICAL_BOOLEAN: "BOOLEAN",
        LOGICAL_DATE: "DATE",
        LOGICAL_DATETIME: "TIMESTAMP",
        LOGICAL_TIME: "STRING",
        LOGICAL_UUID: "STRING",
        LOGICAL_JSON: "STRING",
        LOGICAL_ARRAY: "STRING",
        LOGICAL_BINARY: "BINARY",
    },
    # Apache Iceberg table schema (writer/catalog native).
    "iceberg": {
        LOGICAL_STRING: "string",
        LOGICAL_TEXT: "string",
        LOGICAL_INTEGER: "long",
        LOGICAL_DECIMAL: "decimal(38,10)",
        LOGICAL_BOOLEAN: "boolean",
        LOGICAL_DATE: "date",
        LOGICAL_DATETIME: "timestamptz",
        LOGICAL_TIME: "time",
        LOGICAL_UUID: "uuid",
        LOGICAL_JSON: "string",
        LOGICAL_ARRAY: "list",
        LOGICAL_BINARY: "binary",
    },
    # Schemaless / document / KV — wire as string; no SQL DDL contract.
    "redis": {
        LOGICAL_STRING: "string",
        LOGICAL_TEXT: "string",
        LOGICAL_INTEGER: "string",
        LOGICAL_DECIMAL: "string",
        LOGICAL_BOOLEAN: "string",
        LOGICAL_DATE: "string",
        LOGICAL_DATETIME: "string",
        LOGICAL_TIME: "string",
        LOGICAL_UUID: "string",
        LOGICAL_JSON: "string",
        LOGICAL_ARRAY: "string",
        LOGICAL_BINARY: "string",
    },
    "dynamodb": {
        LOGICAL_STRING: "S",
        LOGICAL_TEXT: "S",
        LOGICAL_INTEGER: "N",
        LOGICAL_DECIMAL: "N",
        LOGICAL_BOOLEAN: "BOOL",
        LOGICAL_DATE: "S",
        LOGICAL_DATETIME: "S",
        LOGICAL_TIME: "S",
        LOGICAL_UUID: "S",
        LOGICAL_JSON: "M",
        LOGICAL_ARRAY: "L",
        LOGICAL_BINARY: "B",
    },
    "elasticsearch": {
        LOGICAL_STRING: "text",
        LOGICAL_TEXT: "text",
        LOGICAL_INTEGER: "long",
        LOGICAL_DECIMAL: "double",
        LOGICAL_BOOLEAN: "boolean",
        LOGICAL_DATE: "date",
        LOGICAL_DATETIME: "date",
        LOGICAL_TIME: "keyword",
        LOGICAL_UUID: "keyword",
        LOGICAL_JSON: "object",
        LOGICAL_ARRAY: "object",
        LOGICAL_BINARY: "binary",
    },
    # Engines reached via generic_sql — keep DDL honest for preflight.
    "duckdb": {
        LOGICAL_STRING: "VARCHAR",
        LOGICAL_TEXT: "VARCHAR",
        LOGICAL_INTEGER: "BIGINT",
        LOGICAL_DECIMAL: "DECIMAL(38,15)",
        LOGICAL_BOOLEAN: "BOOLEAN",
        LOGICAL_DATE: "DATE",
        LOGICAL_DATETIME: "TIMESTAMP",
        LOGICAL_TIME: "TIME",
        LOGICAL_UUID: "UUID",
        LOGICAL_JSON: "JSON",
        LOGICAL_ARRAY: "JSON",
        LOGICAL_BINARY: "BLOB",
    },
    "clickhouse": {
        LOGICAL_STRING: "String",
        LOGICAL_TEXT: "String",
        LOGICAL_INTEGER: "Int64",
        LOGICAL_DECIMAL: "Decimal(38, 15)",
        LOGICAL_BOOLEAN: "UInt8",
        LOGICAL_DATE: "Date",
        LOGICAL_DATETIME: "DateTime64(3)",
        LOGICAL_TIME: "String",
        LOGICAL_UUID: "UUID",
        LOGICAL_JSON: "String",
        LOGICAL_ARRAY: "String",
        LOGICAL_BINARY: "String",
    },
    "trino": {
        LOGICAL_STRING: "varchar",
        LOGICAL_TEXT: "varchar",
        LOGICAL_INTEGER: "bigint",
        LOGICAL_DECIMAL: "decimal(38,15)",
        LOGICAL_BOOLEAN: "boolean",
        LOGICAL_DATE: "date",
        LOGICAL_DATETIME: "timestamp(3) with time zone",
        LOGICAL_TIME: "time(3)",
        LOGICAL_UUID: "uuid",
        LOGICAL_JSON: "json",
        LOGICAL_ARRAY: "json",
        LOGICAL_BINARY: "varbinary",
    },
    "presto": {
        LOGICAL_STRING: "varchar",
        LOGICAL_TEXT: "varchar",
        LOGICAL_INTEGER: "bigint",
        LOGICAL_DECIMAL: "decimal(38,15)",
        LOGICAL_BOOLEAN: "boolean",
        LOGICAL_DATE: "date",
        LOGICAL_DATETIME: "timestamp",
        LOGICAL_TIME: "time",
        LOGICAL_UUID: "varchar",
        LOGICAL_JSON: "json",
        LOGICAL_ARRAY: "json",
        LOGICAL_BINARY: "varbinary",
    },
}

# Native specialty DDL where the engine supports the type; otherwise lossless text.
# Applied after base maps so every destination has interval/geography/vector keys.
#
# VECTOR entries here are *non-parametric sinks* (ARRAY/TEXT/SUPER). Engines that
# require an explicit dimension (PostgreSQL pgvector, Snowflake VECTOR) are
# resolved in ``_vector_ddl_for_dest`` — never invent a default like 1536.
_NATIVE_SPECIALTY_DDL: Final[dict[str, dict[str, str]]] = {
    "postgresql": {
        LOGICAL_INTERVAL: "INTERVAL",
        LOGICAL_GEOGRAPHY: "GEOMETRY",
        # Dimensional form emitted by _vector_ddl_for_dest; bare VECTOR → TEXT.
        LOGICAL_VECTOR: "TEXT",
    },
    "mysql": {
        LOGICAL_INTERVAL: "TEXT",
        LOGICAL_GEOGRAPHY: "GEOMETRY",
        LOGICAL_VECTOR: "TEXT",
    },
    "sqlserver": {
        LOGICAL_INTERVAL: "NVARCHAR(MAX)",
        LOGICAL_GEOGRAPHY: "GEOGRAPHY",
        LOGICAL_VECTOR: "NVARCHAR(MAX)",
    },
    "oracle": {
        LOGICAL_INTERVAL: "INTERVAL DAY TO SECOND",
        LOGICAL_GEOGRAPHY: "SDO_GEOMETRY",
        LOGICAL_VECTOR: "CLOB",
    },
    "snowflake": {
        LOGICAL_INTERVAL: "VARCHAR",
        LOGICAL_GEOGRAPHY: "GEOGRAPHY",
        # Dimensional form emitted by _vector_ddl_for_dest; bare VECTOR → VARCHAR.
        LOGICAL_VECTOR: "VARCHAR",
    },
    "bigquery": {
        LOGICAL_INTERVAL: "STRING",
        LOGICAL_GEOGRAPHY: "GEOGRAPHY",
        LOGICAL_VECTOR: "STRING",
    },
    "redshift": {
        LOGICAL_INTERVAL: "VARCHAR(65535)",
        LOGICAL_GEOGRAPHY: "GEOMETRY",
        LOGICAL_VECTOR: "SUPER",
    },
    "databricks": {
        LOGICAL_INTERVAL: "STRING",
        LOGICAL_GEOGRAPHY: "STRING",
        LOGICAL_VECTOR: "ARRAY<FLOAT>",
    },
    "iceberg": {
        LOGICAL_INTERVAL: "string",
        LOGICAL_GEOGRAPHY: "string",
        LOGICAL_VECTOR: "list",
    },
    "clickhouse": {
        LOGICAL_INTERVAL: "String",
        LOGICAL_GEOGRAPHY: "String",
        LOGICAL_VECTOR: "Array(Float32)",
    },
    "duckdb": {
        LOGICAL_INTERVAL: "INTERVAL",
        LOGICAL_GEOGRAPHY: "VARCHAR",
        LOGICAL_VECTOR: "FLOAT[]",
    },
    "trino": {
        LOGICAL_INTERVAL: "interval day to second",
        LOGICAL_GEOGRAPHY: "varchar",
        LOGICAL_VECTOR: "array(real)",
    },
    "presto": {
        LOGICAL_INTERVAL: "interval day to second",
        LOGICAL_GEOGRAPHY: "varchar",
        LOGICAL_VECTOR: "array(real)",
    },
}

for _dest, _map in DDL_TYPES.items():
    _native = _NATIVE_SPECIALTY_DDL.get(_dest, {})
    _fallback = {
        "redis": "string",
        "mongodb": "string",
        "dynamodb": "S",
        "elasticsearch": "keyword",
        "sqlite": "TEXT",
        "generic_sql": "TEXT",
    }.get(_dest, "TEXT")
    for _logical in (LOGICAL_INTERVAL, LOGICAL_GEOGRAPHY, LOGICAL_VECTOR):
        _map[_logical] = _native.get(_logical, _fallback)

DEFAULT_DDL: Final[dict[str, str]] = {
    "postgresql": "TEXT",
    "mysql": "TEXT",
    "sqlserver": "NVARCHAR(MAX)",
    "oracle": "VARCHAR2(4000)",
    "snowflake": "VARCHAR",
    "bigquery": "STRING",
    "mongodb": "string",
    "redshift": "VARCHAR(65535)",
    "sqlite": "TEXT",
    "generic_sql": "TEXT",
    "databricks": "STRING",
    "iceberg": "string",
    "redis": "string",
    "dynamodb": "S",
    "elasticsearch": "text",
    "duckdb": "VARCHAR",
    "clickhouse": "String",
    "trino": "varchar",
    "presto": "varchar",
}

# Destination fixed-point caps (precision, scale). When source scale exceeds the
# destination scale cap we fall back to a lossless text type — never silently
# truncate fractional digits (financial / scientific fidelity).
_DECIMAL_CAPS: Final[dict[str, tuple[int, int]]] = {
    "mysql": (38, 30),
    "sqlserver": (38, 38),
    "oracle": (38, 127),
    "snowflake": (38, 37),
    "redshift": (38, 37),
    "generic_sql": (38, 37),
    "databricks": (38, 37),
    "iceberg": (38, 37),
    "duckdb": (38, 38),
    "clickhouse": (76, 38),
    "trino": (38, 37),
    "presto": (38, 37),
    # BigQuery NUMERIC is (38,9); BIGNUMERIC is (76,38). We emit BIGNUMERIC
    # for DECIMAL logicals; caps used when source params force a check.
    "bigquery": (76, 38),
}

# DDL templates that accept (precision, scale). Bare NUMERIC / BIGNUMERIC /
# schemaless "decimal" stay as-is (no silent scale floor).
_DECIMAL_PARAM_TEMPLATES: Final[dict[str, str]] = {
    "mysql": "DECIMAL({p},{s})",
    "sqlserver": "DECIMAL({p},{s})",
    "oracle": "NUMBER({p},{s})",
    "snowflake": "NUMBER({p},{s})",
    "redshift": "DECIMAL({p},{s})",
    "generic_sql": "NUMERIC({p},{s})",
    "databricks": "DECIMAL({p},{s})",
    "iceberg": "decimal({p},{s})",
    "duckdb": "DECIMAL({p},{s})",
    "clickhouse": "Decimal({p}, {s})",
    "trino": "decimal({p},{s})",
    "presto": "decimal({p},{s})",
}

_DECIMAL_DEFAULT_SCALE: Final[dict[str, int]] = {
    "mysql": 15,
    "sqlserver": 10,
    "oracle": 10,
    "snowflake": 10,
    "redshift": 15,
    "generic_sql": 15,
    "databricks": 10,
    "iceberg": 10,
    "duckdb": 15,
    "clickhouse": 15,
    "trino": 15,
    "presto": 15,
}


def parse_numeric_precision_scale(inferred: str | None) -> tuple[int | None, int | None]:
    """Extract (precision, scale) from NUMBER(p,s) / DECIMAL(p,s) / NUMERIC(p)."""
    raw = (inferred or "").strip()
    if not raw:
        return None, None
    m = re.match(
        r"^[A-Za-z_ ]+?\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)$",
        raw,
    )
    if not m:
        return None, None
    precision = int(m.group(1))
    scale = int(m.group(2)) if m.group(2) is not None else None
    return precision, scale


# Engines that emit a true vector DDL type only when dimension is known.
_VECTOR_PARAM_TEMPLATES: Final[dict[str, str]] = {
    "postgresql": "vector({n})",
    "snowflake": "VECTOR(FLOAT, {n})",
}

# Platform upper bounds for declared vector dimensions (fail closed → text).
_VECTOR_DIM_CAPS: Final[dict[str, int]] = {
    "postgresql": 16000,  # pgvector practical upper bound
    "snowflake": 4096,
}


def parse_vector_dimension(inferred: str | None) -> int | None:
    """Extract embedding dimension from VECTOR / HALFVEC type strings.

    Accepted carriers (same spirit as DECIMAL(p,s) — params live in the type string):

    * ``VECTOR(1536)`` / ``vector(1536)`` / ``HALFVEC(768)``
    * ``VECTOR(FLOAT, 1536)`` / ``VECTOR(INT, 768)`` (Snowflake-style)
    """
    raw = (inferred or "").strip()
    if not raw:
        return None
    # VECTOR(FLOAT, n) / VECTOR(INT, n)
    m = re.match(
        r"^(?:half)?vec(?:tor)?\s*\(\s*[A-Za-z_][A-Za-z0-9_]*\s*,\s*(\d+)\s*\)$",
        raw,
        re.IGNORECASE,
    )
    if m:
        dim = int(m.group(1))
        return dim if dim > 0 else None
    # VECTOR(n) / HALFVEC(n) / SPARSEVEC(n)
    m = re.match(
        r"^(?:half|sparse)?vec(?:tor)?\s*\(\s*(\d+)\s*\)$",
        raw,
        re.IGNORECASE,
    )
    if m:
        dim = int(m.group(1))
        return dim if dim > 0 else None
    return None


def _vector_ddl_for_dest(db: str, inferred: str | None) -> str:
    """Emit destination VECTOR DDL with source dimension when the engine needs it.

    Never invents a default dimension (historically Snowflake used 1536). When the
    dimension is unknown or exceeds the platform cap, fall back to the destination
    lossless text sink — CREATE TABLE must not invent a wrong embedding width.
    """
    fallback = DDL_TYPES.get(db, {}).get(LOGICAL_VECTOR, DEFAULT_DDL.get(db, "TEXT"))
    template = _VECTOR_PARAM_TEMPLATES.get(db)
    if not template:
        return fallback

    dim = parse_vector_dimension(inferred)
    if dim is None:
        return DEFAULT_DDL.get(db, "TEXT")

    cap = _VECTOR_DIM_CAPS.get(db, 65535)
    if dim > cap:
        return DEFAULT_DDL.get(db, "TEXT")
    return template.format(n=dim)


def normalize_logical_type(inferred: str | None) -> str:
    """Return a canonical logical type for parser, DB, and warehouse types."""
    raw = (inferred or "").strip()
    if not raw:
        return LOGICAL_STRING

    # Parametric types — preserve precision semantics before stripping ().
    m = re.match(r"^([A-Za-z_ ]+?)\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)$", raw)
    if m:
        base = m.group(1).strip().lower()
        p1 = int(m.group(2))
        scale = m.group(3)
        if base in {"number", "numeric", "decimal", "fixed"}:
            if scale is not None and int(scale) == 0:
                return LOGICAL_INTEGER
            return LOGICAL_DECIMAL
        # SQL Server BIT is boolean; PostgreSQL BIT(n>1) / BIT VARYING is a bitstring.
        if base == "bit":
            return LOGICAL_BOOLEAN if p1 <= 1 else LOGICAL_BINARY
        # MySQL TINYINT(1) is the conventional boolean display width.
        if base == "tinyint" and p1 == 1:
            return LOGICAL_BOOLEAN
        if base in {"vector", "halfvec", "sparsevec"}:
            return LOGICAL_VECTOR

    # Snowflake-style VECTOR(FLOAT, n) — first param is element type, not digits.
    if re.match(
        r"^(?:half|sparse)?vec(?:tor)?\s*\(\s*[A-Za-z_][A-Za-z0-9_]*\s*,\s*\d+\s*\)$",
        raw,
        re.IGNORECASE,
    ):
        return LOGICAL_VECTOR

    key = re.sub(r"\([^)]*\)", "", raw).strip().lower()
    key = key.replace("_", " ")
    # Transfer fidelity: unsigned 64-bit integers must not land as signed BIGINT.
    if "unsigned" in key and ("bigint" in key or key in {"uint64", "ubyte8"}):
        return LOGICAL_DECIMAL
    if key in {"uint64", "uint128", "uint256", "int128", "int256", "hugeint", "uhugeint"}:
        return LOGICAL_DECIMAL
    return CANONICAL_TYPES.get(key, CANONICAL_TYPES.get(key.replace(" ", "_"), LOGICAL_STRING))


def _decimal_ddl_for_dest(db: str, inferred: str | None) -> str:
    """Emit destination DECIMAL preserving source scale when possible.

    If source scale exceeds the destination platform cap, return the destination
    lossless text type instead of truncating fractional digits.
    """
    template = _DECIMAL_PARAM_TEMPLATES.get(db)
    default_ddl = DDL_TYPES.get(db, {}).get(LOGICAL_DECIMAL, DEFAULT_DDL.get(db, "TEXT"))
    if not template:
        return default_ddl

    precision, scale = parse_numeric_precision_scale(inferred)
    cap_p, cap_s = _DECIMAL_CAPS.get(db, (38, 37))

    # No source params → use documented platform default scale (not silent truncate).
    if precision is None and scale is None:
        default_s = min(_DECIMAL_DEFAULT_SCALE.get(db, 10), cap_s)
        return template.format(p=cap_p, s=default_s)

    src_p = precision if precision is not None else cap_p
    src_s = scale if scale is not None else 0

    if src_s > cap_s:
        # Preserve digits as text rather than silently truncating scale.
        return DEFAULT_DDL.get(db, "TEXT")

    out_s = min(src_s, cap_s)
    out_p = min(max(src_p, out_s), cap_p)
    if out_p < out_s:
        out_p = out_s
    return template.format(p=out_p, s=out_s)


def _normalize_dest_db(db_type: str | None) -> str:
    """Canonical destination engine id for DDL / cap lookups."""
    db = (db_type or "").strip().lower()
    if db in {"spark", "delta", "delta_lake", "databricks_sql", "unity_catalog"}:
        return "databricks"
    if db in {"apache_iceberg", "iceberg_rest", "nessie"}:
        return "iceberg"
    if db in {"opensearch", "amazon_elasticsearch", "elastic_cloud"}:
        return "elasticsearch"
    if db in {"amazon_dynamodb"}:
        return "dynamodb"
    if db in {"redis-kv", "redis_kv"}:
        return "redis"
    if db in {"ch", "clickhouse_cloud"}:
        return "clickhouse"
    return db


def ddl_type(db_type: str, inferred: str | None) -> str:
    """Map a logical source type to a destination-native DDL type.

    For DECIMAL sources with ``NUMBER(p,s)`` / ``DECIMAL(p,s)``, precision and
    scale are propagated within destination caps. Scale that exceeds the
    destination platform falls back to a lossless text type — never silent
    truncation of fractional digits.

    For VECTOR sources with ``VECTOR(n)`` / ``VECTOR(FLOAT, n)``, dimension is
    propagated on engines that require it (PostgreSQL pgvector, Snowflake).
    Missing or oversized dimensions fall back to lossless text — never invent
    a default width such as 1536.
    """
    db = _normalize_dest_db(db_type)
    logical = normalize_logical_type(inferred)
    if logical == LOGICAL_DECIMAL and db in _DECIMAL_PARAM_TEMPLATES:
        return _decimal_ddl_for_dest(db, inferred)
    if logical == LOGICAL_VECTOR:
        return _vector_ddl_for_dest(db, inferred)
    return DDL_TYPES.get(db, {}).get(logical, DEFAULT_DDL.get(db, "TEXT"))


def decimal_scale_would_truncate(source_type: str | None, dest_db_type: str | None) -> bool:
    """True when mapping source DECIMAL(p,s) onto dest would truncate scale."""
    db = _normalize_dest_db(dest_db_type)
    if normalize_logical_type(source_type) != LOGICAL_DECIMAL:
        return False
    if db not in _DECIMAL_CAPS:
        return False
    _, scale = parse_numeric_precision_scale(source_type)
    if scale is None:
        return False
    return scale > _DECIMAL_CAPS[db][1]


def vector_dim_mismatch(source_type: str | None, target_type: str | None) -> bool:
    """True when both sides declare a vector dimension and they differ.

    Used by G3 / DDL compatibility to fail closed on embedding-width drift
    (e.g. ``VECTOR(768)`` → ``VECTOR(FLOAT, 1536)``). Unknown dims on either
    side return False — that case is handled by ``ddl_type`` falling back to
    text rather than inventing a width.
    """
    if normalize_logical_type(source_type) != LOGICAL_VECTOR:
        return False
    if normalize_logical_type(target_type) != LOGICAL_VECTOR:
        return False
    src_dim = parse_vector_dimension(source_type)
    tgt_dim = parse_vector_dimension(target_type)
    if src_dim is None or tgt_dim is None:
        return False
    return src_dim != tgt_dim


def vector_dim_unknown_for_native(source_type: str | None, dest_db_type: str | None) -> bool:
    """True when dest requires a VECTOR dim but the source type does not declare one.

    Maps that would CREATE a native vector column without a known width are
    unsafe — operators must supply ``VECTOR(n)`` (or accept the text sink).
    """
    db = _normalize_dest_db(dest_db_type)
    if db not in _VECTOR_PARAM_TEMPLATES:
        return False
    if normalize_logical_type(source_type) != LOGICAL_VECTOR:
        return False
    return parse_vector_dimension(source_type) is None


def is_structural_type(inferred: str | None) -> bool:
    return normalize_logical_type(inferred) in {LOGICAL_JSON, LOGICAL_ARRAY}


def is_binary_type(inferred: str | None) -> bool:
    return normalize_logical_type(inferred) == LOGICAL_BINARY


def is_lossy_coercion(source_type: str, target_type: str) -> bool:
    """True when converting source→target may lose precision, fail silently, or
    change the semantic meaning of a value.

    The allow-list below captures the widening / reversible conversions that the
    transform engine can perform without losing the original value:

      * any value → string/text/json/array (structural serialization)
      * integer → decimal/string/text/json
      * decimal → string/text/json
      * boolean → string/text/json/integer/decimal
      * date → datetime/string/text/json
      * datetime/time → string/text/json
      * uuid → string/text/json
      * json/array → string/text/json/array
      * string/text/uuid/json/array → binary (base64 reversible)
      * binary → string/text/json (base64 reversible)

    Everything else is considered lossy and should be surfaced in preflight.
    """
    src = normalize_logical_type(source_type)
    tgt = normalize_logical_type(target_type)
    if src == tgt:
        return False

    safe: set[tuple[str, str]] = {
        # text / structural containers are universal sinks
        (LOGICAL_STRING, LOGICAL_TEXT),
        (LOGICAL_TEXT, LOGICAL_STRING),
        (LOGICAL_STRING, LOGICAL_JSON),
        (LOGICAL_TEXT, LOGICAL_JSON),
        (LOGICAL_JSON, LOGICAL_STRING),
        (LOGICAL_JSON, LOGICAL_TEXT),
        (LOGICAL_JSON, LOGICAL_JSON),
        (LOGICAL_ARRAY, LOGICAL_STRING),
        (LOGICAL_ARRAY, LOGICAL_TEXT),
        (LOGICAL_ARRAY, LOGICAL_JSON),
        (LOGICAL_ARRAY, LOGICAL_ARRAY),
        (LOGICAL_JSON, LOGICAL_ARRAY),
        # numeric widening and text renderings
        (LOGICAL_INTEGER, LOGICAL_DECIMAL),
        (LOGICAL_INTEGER, LOGICAL_STRING),
        (LOGICAL_INTEGER, LOGICAL_TEXT),
        (LOGICAL_INTEGER, LOGICAL_JSON),
        (LOGICAL_DECIMAL, LOGICAL_STRING),
        (LOGICAL_DECIMAL, LOGICAL_TEXT),
        (LOGICAL_DECIMAL, LOGICAL_JSON),
        # boolean renderings and scalar widenings
        (LOGICAL_BOOLEAN, LOGICAL_STRING),
        (LOGICAL_BOOLEAN, LOGICAL_TEXT),
        (LOGICAL_BOOLEAN, LOGICAL_JSON),
        (LOGICAL_BOOLEAN, LOGICAL_INTEGER),
        (LOGICAL_BOOLEAN, LOGICAL_DECIMAL),
        # date/time renderings and date→datetime widening
        (LOGICAL_DATE, LOGICAL_DATETIME),
        (LOGICAL_DATE, LOGICAL_STRING),
        (LOGICAL_DATE, LOGICAL_TEXT),
        (LOGICAL_DATE, LOGICAL_JSON),
        (LOGICAL_DATETIME, LOGICAL_STRING),
        (LOGICAL_DATETIME, LOGICAL_TEXT),
        (LOGICAL_DATETIME, LOGICAL_JSON),
        (LOGICAL_TIME, LOGICAL_STRING),
        (LOGICAL_TIME, LOGICAL_TEXT),
        (LOGICAL_TIME, LOGICAL_JSON),
        # uuid renderings
        (LOGICAL_UUID, LOGICAL_STRING),
        (LOGICAL_UUID, LOGICAL_TEXT),
        (LOGICAL_UUID, LOGICAL_JSON),
        # binary ↔ text reversible (base64)
        (LOGICAL_BINARY, LOGICAL_STRING),
        (LOGICAL_BINARY, LOGICAL_TEXT),
        (LOGICAL_BINARY, LOGICAL_JSON),
        (LOGICAL_STRING, LOGICAL_BINARY),
        (LOGICAL_TEXT, LOGICAL_BINARY),
        (LOGICAL_UUID, LOGICAL_BINARY),
        (LOGICAL_JSON, LOGICAL_BINARY),
        (LOGICAL_ARRAY, LOGICAL_BINARY),
        # Specialty → lossless text / JSON (never invent a fake numeric cast)
        (LOGICAL_INTERVAL, LOGICAL_STRING),
        (LOGICAL_INTERVAL, LOGICAL_TEXT),
        (LOGICAL_INTERVAL, LOGICAL_JSON),
        (LOGICAL_GEOGRAPHY, LOGICAL_STRING),
        (LOGICAL_GEOGRAPHY, LOGICAL_TEXT),
        (LOGICAL_GEOGRAPHY, LOGICAL_JSON),
        (LOGICAL_VECTOR, LOGICAL_STRING),
        (LOGICAL_VECTOR, LOGICAL_TEXT),
        (LOGICAL_VECTOR, LOGICAL_JSON),
        (LOGICAL_VECTOR, LOGICAL_ARRAY),
    }

    if (src, tgt) in safe:
        return False
    return True


def build_column_types(columns: list[str], schema: dict[str, str]) -> dict[str, str]:
    """Return uppercase logical types for writer compatibility."""
    return {col: normalize_logical_type(schema.get(col, "string")).upper() for col in columns}


def default_mappings(columns: list[str]) -> list[dict]:
    return [
        {"source": c, "target": c, "confidence": 0.95, "reason": "Direct mapping"}
        for c in columns
    ]


def decimal_needs_scientific_wire(*, digit_count: int, abs_exponent: int) -> bool:
    """True when fixed-point expansion would violate DECIMAL wire budgets."""
    return (
        abs_exponent > DECIMAL_MAX_FIXED_ABS_EXP
        or (digit_count + abs_exponent) > DECIMAL_MAX_FIXED_DIGITS
    )


def integer_within_wire_budget(*, digit_count: int, exponent: int) -> bool:
    """True when a finite integral Decimal fits INTEGER transform budgets."""
    magnitude_digits = digit_count + max(exponent, 0)
    return magnitude_digits <= INTEGER_MAX_DIGITS
