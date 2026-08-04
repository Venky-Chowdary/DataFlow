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
from typing import Any, Final

LOGICAL_STRING = "string"
LOGICAL_TEXT = "text"
LOGICAL_INTEGER = "integer"
LOGICAL_DECIMAL = "decimal"
LOGICAL_FLOAT = "float"
LOGICAL_BOOLEAN = "boolean"
LOGICAL_DATE = "date"
LOGICAL_DATETIME = "datetime"
LOGICAL_TIME = "time"
LOGICAL_UUID = "uuid"
LOGICAL_JSON = "json"
LOGICAL_ARRAY = "array"
LOGICAL_STRUCT = "struct"
LOGICAL_MAP = "map"
LOGICAL_BINARY = "binary"
# Specialty types — native DDL where the engine supports them, else lossless text.
LOGICAL_INTERVAL = "interval"
LOGICAL_GEOGRAPHY = "geography"
LOGICAL_VECTOR = "vector"
LOGICAL_OBJECTID = "objectid"  # MongoDB ObjectId — first-class, not STRING+specialty dual path

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
    "smallserial": LOGICAL_INTEGER,
    # SQL Server IDENTITY(…) — integer identity carrier (not open STRING).
    "identity": LOGICAL_INTEGER,
    "number": LOGICAL_DECIMAL,
    "numeric": LOGICAL_DECIMAL,
    "decimal": LOGICAL_DECIMAL,
    # BigQuery BIGNUMERIC / BIGDECIMAL — (76,38) class; keep DECIMAL logical
    # while ddl_type preserves the BIGNUMERIC carrier token.
    "bignumeric": LOGICAL_DECIMAL,
    "bigdecimal": LOGICAL_DECIMAL,
    # Approximate IEEE floats — distinct from fixed-point DECIMAL/NUMBER.
    "double": LOGICAL_FLOAT,
    "double precision": LOGICAL_FLOAT,
    "float": LOGICAL_FLOAT,
    "float64": LOGICAL_FLOAT,
    "float32": LOGICAL_FLOAT,
    "real": LOGICAL_FLOAT,
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
    # Oracle / ANSI — must not fall through to STRING (wave 71).
    "timestamp with time zone": LOGICAL_DATETIME,
    "timestamp with local time zone": LOGICAL_DATETIME,
    "timestamp without time zone": LOGICAL_DATETIME,
    "time": LOGICAL_TIME,
    "timetz": LOGICAL_TIME,
    "time tz": LOGICAL_TIME,
    "time with time zone": LOGICAL_TIME,
    "time without time zone": LOGICAL_TIME,
    "uuid": LOGICAL_UUID,
    "guid": LOGICAL_UUID,
    "objectid": LOGICAL_OBJECTID,  # MongoDB ObjectId — first-class logical
    "object id": LOGICAL_OBJECTID,
    "object_id": LOGICAL_STRING,
    "json": LOGICAL_JSON,
    "jsonb": LOGICAL_JSON,
    # Opaque semi-structured document carriers (Airbyte Destinations V2 JSON path).
    "object": LOGICAL_JSON,
    "variant": LOGICAL_JSON,
    "super": LOGICAL_JSON,
    # Fielded nested carriers — distinct from opaque JSON so G3 can enforce shape.
    "record": LOGICAL_STRUCT,
    "struct": LOGICAL_STRUCT,
    "row": LOGICAL_STRUCT,  # Trino/Presto ROW ↔ STRUCT
    "nested": LOGICAL_ARRAY,  # ClickHouse Nested → array-of-struct shape
    "map": LOGICAL_MAP,
    "array": LOGICAL_ARRAY,
    "list": LOGICAL_ARRAY,
    "void": LOGICAL_STRING,  # Spark/Databricks VOID — carrier kept via ddl early
    # Elasticsearch / OpenSearch specialty field types.
    "keyword": LOGICAL_STRING,
    "scaled_float": LOGICAL_FLOAT,
    "geo_point": LOGICAL_GEOGRAPHY,
    "geo_shape": LOGICAL_GEOGRAPHY,
    "dense_vector": LOGICAL_VECTOR,
    "sparse_vector": LOGICAL_VECTOR,
    "flattened": LOGICAL_JSON,
    "ip": LOGICAL_STRING,  # specialty IP via ddl early → INET
    "version": LOGICAL_STRING,
    "completion": LOGICAL_STRING,
    "search_as_you_type": LOGICAL_STRING,
    "token_count": LOGICAL_INTEGER,
    "rank_feature": LOGICAL_FLOAT,
    "rank_features": LOGICAL_JSON,
    "binary": LOGICAL_BINARY,
    "bytes": LOGICAL_BINARY,
    "bytea": LOGICAL_BINARY,
    "blob": LOGICAL_BINARY,
    "varbinary": LOGICAL_BINARY,
    "varbyte": LOGICAL_BINARY,
    "bindata": LOGICAL_BINARY,
    "bin data": LOGICAL_BINARY,
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
    "decimal128": LOGICAL_DECIMAL,
    "float4": LOGICAL_FLOAT,
    "float8": LOGICAL_FLOAT,
    "binary_float": LOGICAL_FLOAT,
    "binary_double": LOGICAL_FLOAT,
    "single": LOGICAL_FLOAT,
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
    "ring": LOGICAL_GEOGRAPHY,  # ClickHouse Ring → geometry subtype
    # Oracle Spatial / Esri ST_GEOMETRY — must not collapse to string.
    "sdo geometry": LOGICAL_GEOGRAPHY,
    "st geometry": LOGICAL_GEOGRAPHY,
    "hstore": LOGICAL_JSON,
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
    "timestamp with local time zone": LOGICAL_DATETIME,
    "timestamp without time zone": LOGICAL_DATETIME,
    "time with time zone": LOGICAL_TIME,
    "time without time zone": LOGICAL_TIME,
    "datetime2": LOGICAL_DATETIME,
    "smalldatetime": LOGICAL_DATETIME,
    "datetimeoffset": LOGICAL_DATETIME,
    # ClickHouse — DateTime64(p[, tz]) must not fall through to STRING.
    "datetime64": LOGICAL_DATETIME,
    "tuple": LOGICAL_STRUCT,
    "ipv4": LOGICAL_STRING,  # specialty IPv4 carrier via ddl_type / bind
    "ipv6": LOGICAL_STRING,
    "sysname": LOGICAL_STRING,
    "hierarchyid": LOGICAL_STRING,  # carrier HIERARCHYID kept via ddl_type early path
    "nclob": LOGICAL_TEXT,
    "bfile": LOGICAL_BINARY,
    # Bare "long" is ambiguous: Spark/Iceberg INT64 vs Oracle text LOB.
    # Logical stays integer for lakehouse; Oracle LONG invent is fail-closed via
    # oracle_long_numeric_invent + ddl_type text stamp off-Oracle relational.
    "long": LOGICAL_INTEGER,
    "half": LOGICAL_FLOAT,
    "halffloat": LOGICAL_FLOAT,
    "float16": LOGICAL_FLOAT,
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
    "document": LOGICAL_JSON,
    "bson": LOGICAL_JSON,
    "rowid": LOGICAL_STRING,
    "urowid": LOGICAL_STRING,
    "currency": LOGICAL_DECIMAL,
}

DDL_TYPES: Final[dict[str, dict[str, str]]] = {
    "postgresql": {
        LOGICAL_STRING: "TEXT",
        LOGICAL_TEXT: "TEXT",
        LOGICAL_INTEGER: "BIGINT",
        LOGICAL_DECIMAL: "NUMERIC",
        LOGICAL_BOOLEAN: "BOOLEAN",
        LOGICAL_DATE: "DATE",
        # Wall-clock TIMESTAMP — bare datetime must not invent TIMESTAMPTZ/UTC.
        # Explicit TIMESTAMPTZ / TIMESTAMP WITH TIME ZONE still map via polarity.
        LOGICAL_DATETIME: "TIMESTAMP",
        LOGICAL_TIME: "TIME",
        LOGICAL_UUID: "UUID",
        LOGICAL_JSON: "JSONB",
        # Bare ARRAY → JSONB document; typed ARRAY<T> / T[] → native T[] via nested DDL.
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
        # Match DATETIME(6): bare TIME is MySQL FSP 0 and false-collapses TIME(6).
        LOGICAL_TIME: "TIME(6)",
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
        # SQL Server defaults DATETIME2/TIME to precision 7 — bare stamps false-collapse.
        LOGICAL_DATETIME: "DATETIME2(7)",
        LOGICAL_TIME: "TIME(7)",
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
        # Wall-clock TIMESTAMP — bare datetime must not invent WITH TIME ZONE.
        LOGICAL_DATETIME: "TIMESTAMP",
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
        # Wall-clock NTZ — bare datetime must not invent TIMESTAMP_TZ.
        LOGICAL_DATETIME: "TIMESTAMP_NTZ",
        LOGICAL_TIME: "TIME",
        # Width-safe carrier — bare VARCHAR collapses UUID polarity in preflight.
        LOGICAL_UUID: "VARCHAR(36)",
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
        # Wall-clock DATETIME — bare logical datetime must not invent UTC as TIMESTAMP.
        # Explicit TIMESTAMPTZ / instant carriers still map to TIMESTAMP via ddl_type().
        LOGICAL_DATETIME: "DATETIME",
        LOGICAL_TIME: "TIME",
        # BigQuery has no UUID type; STRING holds the value but drops polarity.
        # create_new_mapping_target_type stamps physical STRING so Validate warns
        # (never silent-green UUID→UUID while writers emit STRING).
        LOGICAL_UUID: "STRING",
        LOGICAL_JSON: "JSON",
        LOGICAL_ARRAY: "JSON",
        LOGICAL_BINARY: "BYTES",
    },
    # Google Cloud Spanner — INT64/NUMERIC/TIMESTAMP only. No DATETIME, TIME,
    # BIGNUMERIC, or GEOGRAPHY. Wall-clock NTZ lands on STRING (fail-closed).
    "spanner": {
        LOGICAL_STRING: "STRING(MAX)",
        LOGICAL_TEXT: "STRING(MAX)",
        LOGICAL_INTEGER: "INT64",
        LOGICAL_DECIMAL: "NUMERIC",
        LOGICAL_BOOLEAN: "BOOL",
        LOGICAL_DATE: "DATE",
        LOGICAL_DATETIME: "STRING(30)",
        LOGICAL_TIME: "STRING(18)",
        LOGICAL_UUID: "STRING(36)",
        LOGICAL_JSON: "JSON",
        LOGICAL_ARRAY: "JSON",
        LOGICAL_BINARY: "BYTES(MAX)",
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
        # Wall-clock TIMESTAMP — bare datetime must not invent TIMESTAMPTZ.
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
        # Native nested — never collapse ARRAY/STRUCT to STRING (fidelity vs Airbyte).
        LOGICAL_ARRAY: "ARRAY<STRING>",
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
        # Wall-clock timestamp — bare datetime must not invent timestamptz/UTC.
        LOGICAL_DATETIME: "timestamp",
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
        LOGICAL_DECIMAL: "keyword",  # scaled_float requires scaling_factor; writer stores decimal strings
        LOGICAL_FLOAT: "double",
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
        # Never map bare DECIMAL → DOUBLE (IEEE loss). Prefer parametric DECIMAL.
        LOGICAL_DECIMAL: "DECIMAL(38,15)",
        LOGICAL_BOOLEAN: "BOOLEAN",
        LOGICAL_DATE: "DATE",
        LOGICAL_DATETIME: "TIMESTAMP",
        LOGICAL_TIME: "TIME",
        LOGICAL_UUID: "UUID",
        # VARCHAR preserves the exact JSON text (no DuckDB native re-spacing)
        # and lets Python None bind as SQL NULL instead of the JSON null literal.
        LOGICAL_JSON: "VARCHAR",
        LOGICAL_ARRAY: "VARCHAR",
        LOGICAL_BINARY: "BLOB",
    },
    "clickhouse": {
        LOGICAL_STRING: "String",
        LOGICAL_TEXT: "String",
        LOGICAL_INTEGER: "Int64",
        LOGICAL_DECIMAL: "Decimal(38, 15)",
        LOGICAL_BOOLEAN: "Bool",
        LOGICAL_DATE: "Date",
        LOGICAL_DATETIME: "DateTime64(3)",
        LOGICAL_TIME: "String",
        LOGICAL_UUID: "UUID",
        LOGICAL_JSON: "String",
        LOGICAL_ARRAY: "Array(String)",
        LOGICAL_BINARY: "String",
    },
    "trino": {
        LOGICAL_STRING: "varchar",
        LOGICAL_TEXT: "varchar",
        LOGICAL_INTEGER: "bigint",
        LOGICAL_DECIMAL: "decimal(38,15)",
        LOGICAL_BOOLEAN: "boolean",
        LOGICAL_DATE: "date",
        # Wall-clock timestamp — bare datetime must not invent with-time-zone/UTC.
        LOGICAL_DATETIME: "timestamp(3)",
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


# ObjectId is first-class — stamp into every dialect map (avoid STRING+specialty dual path).
_OBJECTID_DDL_DEFAULTS: Final[dict[str, str]] = {
    "mongodb": "objectId",
    "dynamodb": "S",
    "redis": "string",
    "elasticsearch": "keyword",
    # Width-safe hex wires — bare STRING drops ObjectId polarity (false Validate).
    "bigquery": "STRING(24)",
    "spanner": "STRING(24)",
    "databricks": "VARCHAR(24)",
    "sqlite": "TEXT",
    "oracle": "VARCHAR2(24)",
    "sqlserver": "CHAR(24)",
    "mysql": "CHAR(24)",
    "presto": "varchar",
    "trino": "varchar",
    "iceberg": "string",
}
for _db_key, _map in DDL_TYPES.items():
    _map.setdefault(LOGICAL_OBJECTID, _OBJECTID_DDL_DEFAULTS.get(_db_key, "VARCHAR(24)"))

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
        # Bare INTERVAL has no unqualified native — never invent DAY TO SECOND.
        LOGICAL_INTERVAL: "VARCHAR2(64)",
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
        LOGICAL_INTERVAL: "INTERVAL",
        LOGICAL_GEOGRAPHY: "GEOGRAPHY",
        LOGICAL_VECTOR: "STRING",
    },
    "spanner": {
        # No native INTERVAL / GEOGRAPHY / VECTOR — lossless text, never invent BQ types.
        LOGICAL_INTERVAL: "STRING(64)",
        LOGICAL_GEOGRAPHY: "STRING(MAX)",
        LOGICAL_VECTOR: "STRING(MAX)",
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
        LOGICAL_VECTOR: "list<float>",
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

# Approximate float DDL — never silently rewrite FLOAT → NUMBER(38,10).
_FLOAT_DDL: Final[dict[str, str]] = {
    "postgresql": "DOUBLE PRECISION",
    "mysql": "DOUBLE",
    "sqlserver": "FLOAT",
    "oracle": "BINARY_DOUBLE",
    "snowflake": "FLOAT",
    "bigquery": "FLOAT64",
    "spanner": "FLOAT64",
    "mongodb": "double",
    "redshift": "DOUBLE PRECISION",
    "sqlite": "REAL",
    "generic_sql": "DOUBLE PRECISION",
    "databricks": "DOUBLE",
    "iceberg": "double",
    "redis": "string",
    "dynamodb": "N",
    "elasticsearch": "double",
    "duckdb": "DOUBLE",
    "clickhouse": "Float64",
    "trino": "double",
    "presto": "double",
}
for _dest, _map in DDL_TYPES.items():
    _map[LOGICAL_FLOAT] = _FLOAT_DDL.get(_dest, "DOUBLE PRECISION")

DEFAULT_DDL: Final[dict[str, str]] = {
    "postgresql": "TEXT",
    "mysql": "TEXT",
    "sqlserver": "NVARCHAR(MAX)",
    "oracle": "VARCHAR2(4000)",
    "snowflake": "VARCHAR",
    "bigquery": "STRING",
    "spanner": "STRING(MAX)",
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
    # Postgres NUMERIC precision max 1000; typmod scale 0..precision.
    "postgresql": (1000, 1000),
    # BigQuery NUMERIC is (38,9); BIGNUMERIC is (76,38). We emit BIGNUMERIC
    # for DECIMAL logicals; caps used when source params force a check.
    "bigquery": (76, 38),
    # Spanner NUMERIC is fixed (38,9) — never invent BIGNUMERIC.
    "spanner": (38, 9),
}

# DDL templates that accept (precision, scale). Bare NUMERIC / BIGNUMERIC /
# only when the source has no (p,s) — never strip known source scale.
_DECIMAL_PARAM_TEMPLATES: Final[dict[str, str]] = {
    "mysql": "DECIMAL({p},{s})",
    "sqlserver": "DECIMAL({p},{s})",
    "oracle": "NUMBER({p},{s})",
    "snowflake": "NUMBER({p},{s})",
    "redshift": "DECIMAL({p},{s})",
    "generic_sql": "NUMERIC({p},{s})",
    "databricks": "DECIMAL({p},{s})",
    "iceberg": "decimal({p},{s})",
    "clickhouse": "Decimal({p}, {s})",
    "trino": "decimal({p},{s})",
    "presto": "decimal({p},{s})",
    "duckdb": "DECIMAL({p},{s})",
    "postgresql": "NUMERIC({p},{s})",
    "bigquery": "BIGNUMERIC({p},{s})",
    "spanner": "NUMERIC({p},{s})",
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
    "postgresql": 0,
    "bigquery": 38,
    "spanner": 9,
}


def parse_numeric_precision_scale(inferred: str | None) -> tuple[int | None, int | None]:
    """Extract (precision, scale) from NUMBER(p,s) / DECIMAL(p,s) / NUMERIC(p).

    ClickHouse ``Decimal128(S)`` / ``Decimal64(S)`` pass scale only — precision is
    implied by the Decimal* width (9/18/38/76).

    ``DECFLOAT(n)`` is IEEE decimal-*float* digit count — not fixed-point (p,s).
    """
    raw = strip_identity_qualifier(inferred)
    if not raw:
        return None, None
    upper = raw.upper().replace(" ", "")
    # DECFLOAT(16|34) is not DECIMAL(p) — never invent scale 0 from digit count.
    if upper == "DECFLOAT" or upper.startswith("DECFLOAT("):
        return None, None
    if upper in {"MONEY", "CURRENCY"}:
        return 19, 4
    if upper == "SMALLMONEY":
        return 10, 4
    m_ch = re.match(r"^DECIMAL(32|64|128|256)\((\d+)\)$", upper)
    if m_ch:
        prec = {"32": 9, "64": 18, "128": 38, "256": 76}[m_ch.group(1)]
        return prec, int(m_ch.group(2))
    m = re.match(
        r"^[A-Za-z_ ]+?\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)$",
        raw,
    )
    if not m:
        return None, None
    precision = int(m.group(1))
    scale = int(m.group(2)) if m.group(2) is not None else None
    return precision, scale


# Signed BIGINT holds at most ~18 decimal digits (2^63-1 ≈ 9.22e18).
# DECIMAL(p,0) with p > 18 must stay DECIMAL — never collapse to INTEGER/BIGINT
# (SQL Server NUMERIC(38,0) / Oracle NUMBER(38,0) / Snowflake NUMBER(38,0)).
SIGNED_BIGINT_SAFE_PRECISION: Final[int] = 18


def zero_scale_fits_signed_bigint(precision: int | None) -> bool:
    """True when DECIMAL(p,0) / NUMBER(p,0) is safely representable as signed BIGINT."""
    if precision is None:
        return False
    return int(precision) <= SIGNED_BIGINT_SAFE_PRECISION


def zero_scale_numeric_carrier(precision: int) -> str:
    """Introspect carrier for zero-scale numerics — preserve wide DECIMAL(p,0)."""
    if zero_scale_fits_signed_bigint(precision):
        return "INTEGER"
    return f"DECIMAL({int(precision)},0)"


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
    PostgreSQL preserves HALFVEC/SPARSEVEC encoding (never invent dense vector).
    """
    fallback = DDL_TYPES.get(db, {}).get(LOGICAL_VECTOR, DEFAULT_DDL.get(db, "TEXT"))
    template = _VECTOR_PARAM_TEMPLATES.get(db)
    # Engines without native vector templates keep DDL_TYPES sink (ARRAY/STRING/…).
    if not template and db != "postgresql":
        return fallback

    dim = parse_vector_dimension(inferred)
    if dim is None:
        return DEFAULT_DDL.get(db, "TEXT")

    cap = _VECTOR_DIM_CAPS.get(db, 65535)
    if dim > cap:
        return DEFAULT_DDL.get(db, "TEXT")

    enc = vector_encoding_polarity(inferred)
    if db == "postgresql":
        if enc == "half":
            return f"halfvec({dim})"
        if enc == "sparse":
            return f"sparsevec({dim})"
        return f"vector({dim})"

    if not template:
        return fallback
    return template.format(n=dim)


# DynamoDB AttributeValue wire codes (DocumentClient / low-level API).
_DYNAMODB_ATTR_LOGICAL: Final[dict[str, str]] = {
    "S": LOGICAL_STRING,
    "N": LOGICAL_DECIMAL,
    "B": LOGICAL_BINARY,
    "BOOL": LOGICAL_BOOLEAN,
    "NULL": LOGICAL_JSON,  # typed null envelope — not invent VARCHAR
    "M": LOGICAL_JSON,
    "L": LOGICAL_ARRAY,
    "SS": LOGICAL_ARRAY,
    "NS": LOGICAL_ARRAY,
    "BS": LOGICAL_ARRAY,
}

# Attribute codes whose spelling only ever means DynamoDB. "BOOL" and "NULL" are
# also generic SQL/Avro/JSON-Schema aliases, so CANONICAL_TYPES stays their one
# owner and a DynamoDB *destination* still round-trips them via ddl_type().
_DYNAMODB_ONLY_ATTR_CODES: Final[frozenset[str]] = frozenset(
    code for code in _DYNAMODB_ATTR_LOGICAL if code.lower() not in CANONICAL_TYPES
)

_ARROW_TIME_UNIT_FSP: Final[dict[str, int]] = {
    "s": 0,
    "ms": 3,
    "us": 6,
    "ns": 9,
}

# Avro logicalType bare tokens (Confluent / Iceberg / Kafka Connect).
_AVRO_LOGICAL_TOKEN_CARRIER: Final[dict[str, str]] = {
    "uuid": "UUID",
    "date": "DATE",
    "time-millis": "TIME(3)",
    "time-micros": "TIME(6)",
    "timestamp-millis": "TIMESTAMPTZ",
    "timestamp-micros": "TIMESTAMPTZ",
    "local-timestamp-millis": "TIMESTAMP_NTZ",
    "local-timestamp-micros": "TIMESTAMP_NTZ",
    "duration": "INTERVAL DAY TO SECOND",
}


def avro_logical_token_to_carrier(token: str | None) -> str | None:
    """Map a bare Avro ``logicalType`` string to a Datawrap carrier.

    Full Avro field dicts go through :func:`services.avro_schema.avro_type_to_logical`.
    This handles introspect/dtype strings like ``timestamp-millis`` alone.
    """
    key = (token or "").strip().lower()
    if not key:
        return None
    return _AVRO_LOGICAL_TOKEN_CARRIER.get(key)


def arrow_dtype_to_carrier(dtype: str | None) -> str | None:
    """Map Apache Arrow / PyArrow type strings to Datawrap carriers.

    Research: Arrow ``timestamp[us, tz=UTC]`` ↔ Iceberg timestamptz; bare
    ``timestamp[us]`` ↔ timestamp NTZ; ``decimal128(p,s)`` ↔ DECIMAL; 
    ``fixed_size_binary[n]`` ↔ BINARY(n). Never invent TEXT for these.
    """
    raw = (dtype or "").strip()
    if not raw:
        return None
    lower = raw.lower().replace(" ", "")

    m_ts = re.match(
        r"^timestamp\[(s|ms|us|ns)(?:,tz=([^,\]]+))?\]$",
        lower,
    )
    if m_ts:
        fsp = _ARROW_TIME_UNIT_FSP[m_ts.group(1)]
        tz = (m_ts.group(2) or "").strip()
        if tz and tz not in {"none", '""', "''"}:
            return f"TIMESTAMPTZ({fsp})" if fsp else "TIMESTAMPTZ"
        return f"TIMESTAMP_NTZ({fsp})" if fsp else "TIMESTAMP_NTZ"

    # Require Arrow decimal width prefix — bare decimal(p,s) stays parametric path.
    m_dec = re.match(r"^decimal(32|64|128|256)\((\d+),(\d+)\)$", lower)
    if m_dec:
        return f"DECIMAL({int(m_dec.group(2))},{int(m_dec.group(3))})"

    m_fsb = re.match(r"^fixed_size_binary\[(\d+)\]$", lower)
    if m_fsb:
        return f"BINARY({int(m_fsb.group(1))})"

    if lower in {"date32", "date64"}:
        return "DATE"

    m_time = re.match(r"^time(?:32|64)\[(s|ms|us|ns)\]$", lower)
    if m_time:
        fsp = _ARROW_TIME_UNIT_FSP[m_time.group(1)]
        return f"TIME({fsp})" if fsp else "TIME"

    if re.match(r"^duration\[(s|ms|us|ns)\]$", lower):
        return "INTERVAL DAY TO SECOND"

    # Prefer distinctive Arrow tokens only — bare string/binary/bool stay CANONICAL.
    if lower in {"large_string", "large_utf8"}:
        return "TEXT"
    if lower == "large_binary":
        return "BINARY"
    if lower in {"float16", "halffloat"}:
        return "FLOAT"

    # dictionary<values=T, indices=…> — logical type is the value type.
    m_dict = re.match(
        r"^dictionary<\s*values\s*=\s*([^,>]+)",
        raw,
        re.I,
    )
    if m_dict:
        return arrow_dtype_to_carrier(m_dict.group(1).strip()) or "TEXT"

    return None


def normalize_logical_type(inferred: str | None) -> str:
    """Return a canonical logical type for parser, DB, and warehouse types."""
    raw = strip_identity_qualifier(inferred)
    if not raw:
        return LOGICAL_STRING

    # Nested / complex carriers from Arrow, BigQuery, Spark — keep category.
    # STRUCT/MAP stay distinct from opaque JSON so Validate can enforce field
    # contracts (Airbyte Destinations V2 stores objects as JSON — we still label
    # the *source* as struct/map and treat nested→document as an explicit collapse).
    upper = raw.upper()
    # ClickHouse Nullable / LowCardinality wrappers — unwrap before TEXT fallthrough.
    m_wrap = re.match(
        r"^(?:NULLABLE|LOWCARDINALITY)\s*\(\s*(.+)\s*\)$",
        raw,
        re.IGNORECASE | re.DOTALL,
    )
    if m_wrap:
        return normalize_logical_type(m_wrap.group(1).strip())
    # DynamoDB AttributeValue type codes — exact tokens only (never match bare
    # "n"/"s" inside other dialects). AWS docs: S/N/B/BOOL/NULL/M/L/SS/NS/BS.
    # Codes that are also generic SQL/Avro aliases defer to CANONICAL_TYPES, so
    # an Avro/JSON-Schema "null" branch stays a nullable scalar instead of
    # inheriting DynamoDB's typed-null envelope.
    if upper in _DYNAMODB_ONLY_ATTR_CODES:
        return _DYNAMODB_ATTR_LOGICAL[upper]
    # Apache Arrow / PyArrow dtype strings (Parquet/Iceberg catalog paths).
    arrow_carrier = arrow_dtype_to_carrier(raw)
    if arrow_carrier is not None and arrow_carrier != raw:
        return normalize_logical_type(arrow_carrier)
    # Avro logicalType bare tokens (timestamp-millis, local-timestamp-micros, …).
    avro_carrier = avro_logical_token_to_carrier(raw)
    if avro_carrier is not None and avro_carrier != raw:
        return normalize_logical_type(avro_carrier)
    if (
        upper.startswith("ARRAY<")
        or upper.startswith("LIST<")
        or upper.startswith("ARRAY(")
        or upper.startswith("LIST(")
        # Postgres / DuckDB postfix: INTEGER[], TEXT[][], etc.
        or re.match(r"^[A-Z][A-Z0-9_ ]*(\[\s*\])+$", upper)
    ):
        return LOGICAL_ARRAY
    if (
        upper.startswith("STRUCT<")
        or upper.startswith("RECORD<")
        or upper.startswith("OBJECT(")
        or upper.startswith("STRUCT(")
        or upper.startswith("ROW(")
    ):
        return LOGICAL_STRUCT
    if upper.startswith("MAP<") or upper.startswith("MAP("):
        return LOGICAL_MAP
    if upper.startswith("TUPLE(") or upper.startswith("NESTED("):
        return LOGICAL_STRUCT if upper.startswith("TUPLE(") else LOGICAL_ARRAY
    # ClickHouse DateTime / DateTime64 — datetime logical before bare-token strip.
    if upper.startswith("DATETIME64") or upper == "DATETIME" or (
        upper.startswith("DATETIME(") and not upper.startswith("DATETIME2")
    ):
        return LOGICAL_DATETIME
    # ClickHouse Decimal32/64/128/256(S) — scale-only typmod (precision implied).
    if re.match(r"^DECIMAL(?:32|64|128|256)\s*\(\s*\d+\s*\)$", upper):
        return LOGICAL_DECIMAL

    # Parametric types — preserve precision semantics before stripping ().
    m = re.match(r"^([A-Za-z_ ]+?)\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)$", raw)
    if m:
        base = m.group(1).strip().lower()
        p1 = int(m.group(2))
        scale = m.group(3)
        # Iceberg ``fixed(L)`` is a fixed-length byte array (spec). MySQL
        # ``FIXED(p,s)`` is a DECIMAL synonym — only the two-arg form is decimal.
        if base == "fixed":
            if scale is None:
                return LOGICAL_BINARY
            if int(scale) == 0 and zero_scale_fits_signed_bigint(p1):
                return LOGICAL_INTEGER
            return LOGICAL_DECIMAL
        if base in {
            "number",
            "numeric",
            "decimal",
            "bignumeric",
            "bigdecimal",
            "decimal32",
            "decimal64",
            "decimal128",
            "decimal256",
        }:
            if scale is not None and int(scale) == 0:
                # Wide zero-scale decimals (e.g. NUMBER(38,0)) must not become
                # signed BIGINT — that overflows values Airbyte quietly corrupts.
                if zero_scale_fits_signed_bigint(p1):
                    return LOGICAL_INTEGER
                return LOGICAL_DECIMAL
            return LOGICAL_DECIMAL
        # SQL Server BIT is boolean; PostgreSQL BIT(n>1) / BIT VARYING is a bitstring.
        if base == "bit":
            return LOGICAL_BOOLEAN if p1 <= 1 else LOGICAL_BINARY
        # MySQL TINYINT(1) is the conventional boolean display width.
        if base == "tinyint" and p1 == 1:
            return LOGICAL_BOOLEAN
        if base in {"vector", "halfvec", "sparsevec"}:
            return LOGICAL_VECTOR
        # ClickHouse FixedString(n) — fixed-length bytes, not text.
        if base == "fixedstring":
            return LOGICAL_BINARY

    # Snowflake-style VECTOR(FLOAT, n) — first param is element type, not digits.
    if re.match(
        r"^(?:half|sparse)?vec(?:tor)?\s*\(\s*[A-Za-z_][A-Za-z0-9_]*\s*,\s*\d+\s*\)$",
        raw,
        re.IGNORECASE,
    ):
        return LOGICAL_VECTOR

    key = re.sub(r"\([^)]*\)", "", raw).strip().lower()
    key = key.replace("_", " ")
    # Schema-qualified Oracle/SQL types (MDSYS.SDO_GEOMETRY → sdo geometry).
    if "." in key and not key.startswith(("array<", "struct<", "map<", "record<", "list<")):
        key = key.rsplit(".", 1)[-1].strip()
    # Transfer fidelity: unsigned 64-bit integers must not land as signed BIGINT.
    if "unsigned" in key and ("bigint" in key or key in {"uint64", "ubyte8"}):
        return LOGICAL_DECIMAL
    if key in {"uint64", "uint128", "uint256", "int128", "int256", "hugeint", "uhugeint"}:
        return LOGICAL_DECIMAL
    # FLOAT/DECIMAL UNSIGNED must not fall through to LOGICAL_STRING.
    if "unsigned" in key and re.search(r"\b(float|double|real)\b", key):
        return LOGICAL_FLOAT
    if "unsigned" in key and re.search(r"\b(decimal|numeric|number)\b", key):
        return LOGICAL_DECIMAL
    # Oracle short forms INTERVAL DAY / INTERVAL YEAR — not bare strings.
    if key.startswith("interval"):
        return LOGICAL_INTERVAL
    return CANONICAL_TYPES.get(key, CANONICAL_TYPES.get(key.replace(" ", "_"), LOGICAL_STRING))


_NESTED_DDL_ENGINES: Final[frozenset[str]] = frozenset({
    "databricks",
    "duckdb",
    "clickhouse",
    "iceberg",
    "bigquery",
    "trino",
    "presto",
    # Snowflake structured types: OBJECT(...), ARRAY(...), MAP(...).
    "snowflake",
})

# Engines with native T[] arrays but no STRUCT invent on create-new.
_ARRAY_NATIVE_ENGINES: Final[frozenset[str]] = frozenset({
    "postgresql",
    "postgres",
    "redshift",
    "timescaledb",
    "cockroachdb",
    "alloydb",
    "yugabytedb",
    "citus",
    "supabase",
    "greenplum",
})


def _leaf_ddl_for_nested(db: str, leaf: str) -> str:
    """Map a nested leaf logical carrier to destination-native leaf DDL."""
    leaf = (leaf or "STRING").strip()
    # Avoid recursion into nested helpers — leafs are scalars.
    logical = normalize_logical_type(leaf)
    if logical == LOGICAL_DECIMAL and db in _DECIMAL_PARAM_TEMPLATES:
        return _decimal_ddl_for_dest(db, leaf)
    if logical == LOGICAL_DATETIME:
        tz_ddl = _datetime_ddl_for_dest(db, leaf)
        if tz_ddl:
            return tz_ddl
    # Width-preserving invent for nested INT/FLOAT — never BIGINT[] from ARRAY<INT>.
    if logical == LOGICAL_INTEGER:
        int_ddl = _integer_ddl_for_dest(db, leaf)
        if int_ddl:
            return int_ddl
    if logical == LOGICAL_FLOAT:
        float_ddl = _float_ddl_for_dest(db, leaf)
        if float_ddl:
            return float_ddl
    return DDL_TYPES.get(db, {}).get(logical, DEFAULT_DDL.get(db, "TEXT"))


def _format_array_ddl(db: str, element_ddl: str) -> str:
    if db == "duckdb" or db in _ARRAY_NATIVE_ENGINES:
        return f"{element_ddl}[]"
    if db == "clickhouse":
        return f"Array({element_ddl})"
    if db == "iceberg":
        return f"list<{element_ddl}>"
    if db in {"trino", "presto"}:
        return f"array({element_ddl})"
    if db == "snowflake":
        return f"ARRAY({element_ddl})"
    # databricks / bigquery / spark
    return f"ARRAY<{element_ddl}>"


def _format_struct_ddl(db: str, fields: list[tuple[str, str]]) -> str:
    if db == "duckdb":
        inner = ", ".join(f"{n} {t}" for n, t in fields)
        return f"STRUCT({inner})"
    if db == "clickhouse":
        # Named Tuple(name Type, …) preserves the STRUCT field contract.
        # Positional Tuple(T1,T2) drops names and false-collapses create-new Map.
        inner = ", ".join(f"{n} {t}" for n, t in fields)
        return f"Tuple({inner})"
    if db == "iceberg":
        inner = ", ".join(f"{n}: {t}" for n, t in fields)
        return f"struct<{inner}>"
    if db in {"trino", "presto"}:
        inner = ", ".join(f"{n} {t}" for n, t in fields)
        return f"row({inner})"
    if db == "snowflake":
        # Snowflake structured OBJECT(name TYPE, ...) — typed fields, not VARIANT blob.
        inner = ", ".join(f"{n} {t}" for n, t in fields)
        return f"OBJECT({inner})"
    inner = ", ".join(f"{n}:{t}" for n, t in fields)
    return f"STRUCT<{inner}>"


def _split_nested_type_parts(body: str) -> list[str]:
    """Split STRUCT/MAP/OBJECT body on top-level commas (ignore nested <> / ())."""
    depth_angle = 0
    depth_paren = 0
    buf = ""
    parts: list[str] = []
    for ch in body:
        if ch == "<":
            depth_angle += 1
            buf += ch
        elif ch == ">":
            depth_angle = max(0, depth_angle - 1)
            buf += ch
        elif ch == "(":
            depth_paren += 1
            buf += ch
        elif ch == ")":
            depth_paren = max(0, depth_paren - 1)
            buf += ch
        elif ch == "," and depth_angle == 0 and depth_paren == 0:
            parts.append(buf.strip())
            buf = ""
        else:
            buf += ch
    if buf.strip():
        parts.append(buf.strip())
    return parts


def parse_struct_fields(inferred: str | None) -> list[tuple[str, str]]:
    """Parse fielded nested carriers into ``[(name, type), ...]``.

    Covers BigQuery ``STRUCT<a:T>``, DuckDB ``STRUCT(a T)``, Snowflake
    ``OBJECT(a T)``, Trino/Presto ``ROW(a T)``, ClickHouse ``Tuple(T1,T2)`` /
    ``Nested(a T, b U)``.
    """
    raw = (inferred or "").strip()
    upper = raw.upper()
    body = ""
    if (upper.startswith("STRUCT<") or upper.startswith("RECORD<")) and raw.endswith(">"):
        body = raw[raw.index("<") + 1 : -1].strip()
    elif (
        upper.startswith("OBJECT(")
        or upper.startswith("STRUCT(")
        or upper.startswith("ROW(")
        or upper.startswith("NESTED(")
    ) and raw.endswith(")"):
        body = raw[raw.index("(") + 1 : -1].strip()
    elif upper.startswith("TUPLE(") and raw.endswith(")"):
        # ClickHouse Tuple — named ``Tuple(a Int32)`` or positional ``Tuple(Int32)``.
        els = _split_nested_type_parts(raw[raw.index("(") + 1 : -1].strip())
        out: list[tuple[str, str]] = []
        for i, el in enumerate(els):
            el = (el or "").strip()
            if not el:
                continue
            if " " in el:
                name, typ = el.split(None, 1)
                name_clean = name.strip().strip('"').strip("`").strip("'")
                typ = typ.strip()
                if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name_clean) and typ:
                    out.append((name_clean, typ))
                    continue
            out.append((f"_{i}", el))
        return out
    else:
        return []
    fields: list[tuple[str, str]] = []
    for part in _split_nested_type_parts(body):
        if ":" in part:
            name, typ = part.split(":", 1)
        elif " " in part:
            name, typ = part.split(None, 1)
        else:
            continue
        name = name.strip()
        typ = typ.strip()
        if name and typ:
            fields.append((name, typ))
    return fields


def parse_map_key_value(inferred: str | None) -> tuple[str, str] | None:
    """Parse ``MAP<K, V>`` / Snowflake ``MAP(K, V)`` into ``(key_type, value_type)``."""
    raw = (inferred or "").strip()
    upper = raw.upper()
    if upper.startswith("MAP<") and raw.endswith(">"):
        body = raw[raw.index("<") + 1 : -1].strip()
    elif upper.startswith("MAP(") and raw.endswith(")"):
        body = raw[raw.index("(") + 1 : -1].strip()
    else:
        return None
    parts = _split_nested_type_parts(body)
    if len(parts) != 2:
        return None
    return parts[0], parts[1]


def parse_array_element(inferred: str | None) -> str | None:
    """Parse ``ARRAY<T>`` / ``LIST<T>`` / Snowflake ``ARRAY(T)`` / ``T[]`` element type."""
    raw = (inferred or "").strip()
    upper = raw.upper()
    if (upper.startswith("ARRAY<") or upper.startswith("LIST<")) and raw.endswith(">"):
        inner = raw[raw.index("<") + 1 : -1].strip()
        return inner or None
    if (upper.startswith("ARRAY(") or upper.startswith("LIST(")) and raw.endswith(")"):
        inner = raw[raw.index("(") + 1 : -1].strip()
        return inner or None
    # Postfix T[] / T[][] — peel one dimension (PG / DuckDB wire).
    m = re.match(r"^(.+?)\s*\[\s*\]\s*$", raw, flags=re.IGNORECASE)
    if m:
        inner = m.group(1).strip()
        return inner or None
    return None


def decimal_params_would_narrow(source_type: str, target_type: str) -> bool:
    """True when DECIMAL(p,s) → DECIMAL(p',s') shrinks scale or integer-digit capacity.

    Mirrors warehouse rules (BigQuery NUMERIC rounding / Fivetran precision caps):
    head samples can look clean while body rows truncate. Same-family logical
    ``decimal→decimal`` must not soft-pass when params narrow.
    """
    if normalize_logical_type(source_type) != LOGICAL_DECIMAL:
        return False
    if normalize_logical_type(target_type) != LOGICAL_DECIMAL:
        return False
    sp, ss = parse_numeric_precision_scale(source_type)
    tp, ts = parse_numeric_precision_scale(target_type)
    if sp is None and ss is None:
        # Bare DECIMAL → DECIMAL(p,s) invents a capacity the source never proved.
        if tp is not None or ts is not None:
            return True
        return False
    if tp is None and ts is None:
        # Proven (p,s) → bare DECIMAL invents platform default (often MySQL
        # DECIMAL(10,0) / engine-specific) — Accept risk, never silent green.
        return True
    if ss is not None and ts is not None and ts < ss:
        return True
    if sp is not None and tp is not None:
        src_int_digits = sp - (ss if ss is not None else 0)
        tgt_int_digits = tp - (ts if ts is not None else 0)
        if tgt_int_digits < src_int_digits:
            return True
        if ts is not None and ss is not None and ts == ss and tp < sp:
            return True
    return False


def is_decfloat_carrier(inferred: str | None) -> bool:
    """True for IBM ``DECFLOAT`` / ``DECFLOAT(16|34)`` IEEE decimal-float."""
    upper = strip_identity_qualifier(inferred).upper().replace(" ", "")
    return upper == "DECFLOAT" or upper.startswith("DECFLOAT(")


def decfloat_domain_would_collapse(source_type: str, target_type: str) -> bool:
    """True when DECFLOAT polarity would be lost into fixed DECIMAL/FLOAT/text."""
    if not is_decfloat_carrier(source_type):
        return False
    if is_decfloat_carrier(target_type):
        return False
    return True


def bignumeric_capacity_would_invent(source_type: str, target_type: str) -> bool:
    """True when bare NUMBER/DECIMAL invents BigQuery BIGNUMERIC (76,38) class."""
    if normalize_logical_type(source_type) != LOGICAL_DECIMAL:
        return False
    if normalize_logical_type(target_type) != LOGICAL_DECIMAL:
        return False
    src_u = strip_identity_qualifier(source_type).upper().replace(" ", "")
    tgt_u = strip_identity_qualifier(target_type).upper().replace(" ", "")
    src_big = src_u.startswith("BIGNUMERIC") or src_u.startswith("BIGDECIMAL")
    tgt_big = tgt_u.startswith("BIGNUMERIC") or tgt_u.startswith("BIGDECIMAL")
    if tgt_big and not src_big:
        # Same (p,s) BIGNUMERIC is BigQuery create-new physical wire — not invent.
        sp, ss = parse_numeric_precision_scale(source_type)
        tp, ts = parse_numeric_precision_scale(target_type)
        if sp is not None and tp is not None and sp == tp and (
            (ss is None and ts is None) or ss == ts
        ):
            return False
        return True
    return False


def decimal_fixed_point_would_collapse_to_text(
    source_type: str, target_type: str
) -> bool:
    """True when fixed-point DECIMAL collapses to open TEXT/STRING.

    Create-new may stamp TEXT when (p,s) exceeds the destination DECIMAL cap
    (e.g. ClickHouse Decimal256→MySQL). That preserves digits as strings but
    drops fixed-point polarity — Accept risk, never silent green.
    """
    if normalize_logical_type(source_type) != LOGICAL_DECIMAL:
        return False
    if is_decfloat_carrier(source_type):
        return False
    tgt = normalize_logical_type(target_type)
    return tgt in {LOGICAL_STRING, LOGICAL_TEXT}


def smalldatetime_domain_would_invent(source_type: str, target_type: str) -> bool:
    """True when SMALLDATETIME (minute accuracy) invents second-level TIMESTAMP.

    Microsoft SMALLDATETIME is minute-rounded; inventing TIMESTAMP(0)/DATETIME
    claims second fidelity the source never had — Accept risk required.
    """
    src = strip_identity_qualifier(source_type).upper().replace(" ", "")
    if src != "SMALLDATETIME":
        return False
    tgt = strip_identity_qualifier(target_type).upper().replace(" ", "")
    if tgt == "SMALLDATETIME":
        return False
    return normalize_logical_type(target_type) in {
        LOGICAL_DATETIME,
        LOGICAL_DATE,
        LOGICAL_STRING,
        LOGICAL_TEXT,
    }


def is_opaque_document_logical(inferred: str | None) -> bool:
    """True for VARIANT/JSONB/SUPER-style document sinks (not fielded STRUCT/MAP)."""
    return normalize_logical_type(inferred) == LOGICAL_JSON


# Document polarity class — JSON / JSONB / VARIANT / SUPER / OBJECT / BSON.
# Specialty invent is "leave this class"; dialect physical wires may be text LOBs
# (SQL Server NVARCHAR(MAX), Oracle CLOB) without losing create-new document intent.
_DOCUMENT_POLARITY_BASES: Final[frozenset[str]] = frozenset(
    {
        "JSON",
        "JSONB",
        "VARIANT",
        "SUPER",
        "OBJECT",
        "BSON",
        "M",  # Mongo document envelope alias
    }
)

# Unbounded text LOBs that engines use as *the* create-new document wire when they
# lack a native JSON type. Bounded VARCHAR(n) is never a document wire.
_DOCUMENT_TEXT_WIRE_TOKENS: Final[frozenset[str]] = frozenset(
    {
        "NVARCHAR(MAX)",
        "VARCHAR(MAX)",
        "CLOB",
        "NCLOB",
        "LONGTEXT",
        "LONG VARCHAR",
        "LONG",
    }
)


def is_document_polarity_carrier(inferred: str | None) -> bool:
    """True when the carrier is in the opaque document polarity class."""
    if not inferred:
        return False
    if normalize_logical_type(inferred) != LOGICAL_JSON:
        return False
    base = strip_identity_qualifier(inferred).upper().strip()
    bare = re.sub(r"\s*\(\s*\d+\s*\)", "", base).strip()
    if bare in _DOCUMENT_POLARITY_BASES or bare.startswith("JSON"):
        return True
    # Logical JSON aliases (object / variant / super / bson) with no JSON token.
    return True


def is_dialect_native_document_wire(
    target_type: str,
    *,
    dest_db: str = "",
) -> bool:
    """True when target is the destination's create-new document sink.

    Covers native JSON/JSONB/VARIANT/SUPER and dialect text LOB wires
    (NVARCHAR(MAX) / CLOB) that are the intentional document projection —
    not a fidelity collapse.
    """
    raw = strip_identity_qualifier(target_type).strip()
    if not raw:
        return False
    upper = raw.upper()
    bare = re.sub(r"\s*\(\s*\d+\s*\)", "", upper).strip()
    if bare in _DOCUMENT_POLARITY_BASES or bare.startswith("JSON"):
        return True
    if upper in _DOCUMENT_TEXT_WIRE_TOKENS or bare in _DOCUMENT_TEXT_WIRE_TOKENS:
        return True
    db = (dest_db or "").strip().lower()
    if not db:
        return False
    try:
        native = ddl_type(db, "JSON")
    except Exception:
        return False
    native_u = strip_identity_qualifier(native).upper().strip()
    return upper == native_u or bare == re.sub(r"\s*\(\s*\d+\s*\)", "", native_u).strip()


def document_domain_would_collapse(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
) -> bool:
    """True when JSON/VARIANT/SUPER/JSONB loses document validation into open text.

    Serializing a document to bounded VARCHAR/STRING/TEXT drops JSON parse
    polarity. Dialect-native document wires (JSONB, VARIANT, NVARCHAR(MAX),
    CLOB, …) are create-new projections — not collapse.
    """
    if normalize_logical_type(source_type) != LOGICAL_JSON:
        return False
    if is_dialect_native_document_wire(target_type, dest_db=dest_db):
        return False
    tgt = normalize_logical_type(target_type)
    return tgt in {LOGICAL_STRING, LOGICAL_TEXT}


def document_domain_would_invent(source_type: str, target_type: str) -> bool:
    """True when open string/text invents a JSON/VARIANT document domain.

    Writers may wrap scalars as JSON, but Map must Accept risk — never imply the
    source already carried document validation polarity.
    """
    if normalize_logical_type(target_type) != LOGICAL_JSON:
        return False
    src = normalize_logical_type(source_type)
    return src in {LOGICAL_STRING, LOGICAL_TEXT}


def is_nested_document_collapse(source_type: str, target_type: str) -> bool:
    """True when STRUCT/MAP/ARRAY collapses into opaque JSON/VARIANT or text.

    Airbyte Destinations V2 often stores objects as JSON — that is valid but is
    **not** field/element DDL fidelity. Operators must see it (G3 warn/block).
    Nested→VARCHAR/TEXT is the same field-DDL collapse (serialized document).
    """
    src = normalize_logical_type(source_type)
    tgt = normalize_logical_type(target_type)
    if src not in {LOGICAL_STRUCT, LOGICAL_MAP, LOGICAL_ARRAY}:
        return False
    return tgt in {LOGICAL_JSON, LOGICAL_STRING, LOGICAL_TEXT}


def nested_struct_fields_incompatible(source_type: str, target_type: str, *, dest_db: str = "") -> bool:
    """True when STRUCT field contracts are lost or inventively declared.

    Opaque ``RECORD``/bare STRUCT → fielded ``STRUCT<a:INT>`` invents a schema
    the source never proved — fail closed (Accept risk).
    """
    src_fields = parse_struct_fields(source_type)
    tgt_fields = parse_struct_fields(target_type)
    src_l = normalize_logical_type(source_type)
    tgt_l = normalize_logical_type(target_type)
    if src_l == LOGICAL_STRUCT and tgt_l == LOGICAL_STRUCT:
        # Unfielded ↔ fielded invents or drops a field DDL contract.
        if bool(src_fields) != bool(tgt_fields):
            return True
    if not src_fields or not tgt_fields:
        return False
    tgt_by = {n.lower(): t for n, t in tgt_fields}
    # Positional ClickHouse Tuple(_0,_1,…) ↔ named STRUCT — match by order when
    # every target name is a synthetic positional index (legacy positional stamps).
    positional_tgt = all(
        n.startswith("_") and n[1:].isdigit() for n, _ in tgt_fields
    )
    positional_src = all(
        n.startswith("_") and n[1:].isdigit() for n, _ in src_fields
    )
    if positional_tgt != positional_src and len(src_fields) == len(tgt_fields):
        if positional_tgt or positional_src:
            pairs = list(zip(src_fields, tgt_fields))
            src_fields = [(sn, st) for (sn, st), _ in pairs]
            tgt_fields = [(sn, tt) for (sn, _), (_, tt) in pairs]
            tgt_by = {n.lower(): t for n, t in tgt_fields}
    # Safe leaf widenings inside a nested shape — numeric widen + text↔text only.
    # INT→STRING under STRUCT rewrites the nested DDL contract (Accept risk).
    safe_leaf: set[tuple[str, str]] = {
        (LOGICAL_INTEGER, LOGICAL_DECIMAL),
        (LOGICAL_DATE, LOGICAL_DATETIME),
        (LOGICAL_STRING, LOGICAL_TEXT),
        (LOGICAL_TEXT, LOGICAL_STRING),
        (LOGICAL_STRUCT, LOGICAL_JSON),  # nested document path — flagged separately
        (LOGICAL_MAP, LOGICAL_JSON),
        # ARRAY→JSON is document collapse (is_nested_document_collapse), not a safe leaf.
    }
    for name, src_t in src_fields:
        tgt_t = tgt_by.get(name.lower())
        if tgt_t is None:
            return True
        src_l = normalize_logical_type(src_t)
        tgt_l = normalize_logical_type(tgt_t)
        if src_l == LOGICAL_STRUCT and tgt_l == LOGICAL_STRUCT:
            if nested_struct_fields_incompatible(src_t, tgt_t, dest_db=dest_db):
                return True
            continue
        if is_nested_document_collapse(src_t, tgt_t):
            return True
        if src_l == tgt_l:
            # Same family — still catch IEEE/time/TZ collapse when helpers exist.
            if is_precision_collapse_coercion(src_t, tgt_t, dest_db=dest_db):
                return True
            continue
        if (src_l, tgt_l) not in safe_leaf:
            return True
        # Safe leaf (STRING↔TEXT) must still catch specialty→text / width / IEEE.
        if is_precision_collapse_coercion(src_t, tgt_t, dest_db=dest_db):
            return True
    return False


def nested_array_elements_incompatible(source_type: str, target_type: str, *, dest_db: str = "") -> bool:
    """True when ARRAY/LIST element types collapse fidelity (e.g. FLOAT→INTEGER)."""
    src_el = parse_array_element(source_type)
    tgt_el = parse_array_element(target_type)
    # Bare ARRAY/LIST ↔ typed ARRAY<T> invents or drops the element contract.
    if bool(src_el) != bool(tgt_el):
        return True
    if not src_el or not tgt_el:
        return False
    if is_precision_collapse_coercion(src_el, tgt_el, dest_db=dest_db):
        return True
    if is_nested_shape_collapse(src_el, tgt_el, dest_db=dest_db):
        return True
    if decimal_params_would_narrow(src_el, tgt_el):
        return True
    s_l, t_l = normalize_logical_type(src_el), normalize_logical_type(tgt_el)
    if s_l == t_l:
        # Same logical family — only narrow collapses (match scalar SSOT).
        # ARRAY<INTEGER>→ARRAY<INT64> on BigQuery is the engine's single int wire,
        # not a silent invent; ARRAY<BIGINT>→ARRAY<INT32> still fails closed.
        if s_l == LOGICAL_INTEGER and integer_width_would_narrow(src_el, tgt_el):
            return True
        if s_l == LOGICAL_FLOAT and float_mantissa_would_narrow(
            src_el, tgt_el, dest_db=dest_db
        ):
            return True
        return False
    # Safe element widenings — numeric widen + text↔text only (nested SSOT).
    # Integer→decimal still allowed; integer→wider integer is width-safe above.
    safe_el = {
        (LOGICAL_INTEGER, LOGICAL_DECIMAL),
        (LOGICAL_DATE, LOGICAL_DATETIME),
        (LOGICAL_STRING, LOGICAL_TEXT),
        (LOGICAL_TEXT, LOGICAL_STRING),
        (LOGICAL_STRUCT, LOGICAL_JSON),
        (LOGICAL_MAP, LOGICAL_JSON),
        # ARRAY→JSON element collapse is not a safe widening.
    }
    return (s_l, t_l) not in safe_el


def is_nested_shape_collapse(source_type: str, target_type: str, *, dest_db: str = "") -> bool:
    """Fielded nested shape lost: document / STRUCT / MAP / ARRAY element collapse."""
    dest_db = _normalize_dest_db(dest_db) if dest_db else ""
    if is_nested_document_collapse(source_type, target_type):
        return True
    src = normalize_logical_type(source_type)
    tgt = normalize_logical_type(target_type)
    if src == LOGICAL_STRUCT and tgt == LOGICAL_STRUCT:
        return nested_struct_fields_incompatible(source_type, target_type, dest_db=dest_db)
    if src == LOGICAL_MAP and tgt == LOGICAL_MAP:
        skv = parse_map_key_value(source_type)
        tkv = parse_map_key_value(target_type)
        # Bare MAP ↔ typed MAP<K,V> invents or drops the key/value contract.
        if bool(skv) != bool(tkv):
            return True
        if skv and tkv:
            if normalize_logical_type(skv[0]) != normalize_logical_type(tkv[0]):
                return True
            if is_nested_shape_collapse(skv[1], tkv[1], dest_db=dest_db):
                return True
            if is_precision_collapse_coercion(skv[1], tkv[1], dest_db=dest_db):
                return True
            if decimal_params_would_narrow(skv[1], tkv[1]):
                return True
            s_l, t_l = normalize_logical_type(skv[1]), normalize_logical_type(tkv[1])
            # Same nested SSOT as STRUCT/ARRAY — INT→STRING rewrites value DDL.
            if s_l != t_l and (s_l, t_l) not in {
                (LOGICAL_INTEGER, LOGICAL_DECIMAL),
                (LOGICAL_DATE, LOGICAL_DATETIME),
                (LOGICAL_STRING, LOGICAL_TEXT),
                (LOGICAL_TEXT, LOGICAL_STRING),
                (LOGICAL_STRUCT, LOGICAL_JSON),
                (LOGICAL_MAP, LOGICAL_JSON),
            }:
                return True
    if src == LOGICAL_ARRAY and tgt == LOGICAL_ARRAY:
        return nested_array_elements_incompatible(source_type, target_type, dest_db=dest_db)
    return False


def _nested_ddl_for_dest(db: str, inferred: str | None) -> str | None:
    """Emit native ARRAY/STRUCT/MAP DDL when source declares nested carriers.

    Engines without nested support return None so callers fall back to base maps
    (JSON/SUPER/TEXT) — never invent a STRUCT on PostgreSQL. Typed arrays on
    PG-family still emit native ``T[]`` (never silent JSONB invent).
    """
    raw = (inferred or "").strip()
    if not raw:
        return None
    upper = raw.upper()

    array_el = parse_array_element(raw)
    if array_el is not None and (
        db in _NESTED_DDL_ENGINES or db in _ARRAY_NATIVE_ENGINES
    ):
        # Recurse only on full nested engines; PG-family keeps scalar leaves.
        nested_inner = (
            _nested_ddl_for_dest(db, array_el) if db in _NESTED_DDL_ENGINES else None
        )
        element = nested_inner if nested_inner else _leaf_ddl_for_nested(db, array_el)
        return _format_array_ddl(db, element)

    if db not in _NESTED_DDL_ENGINES:
        return None

    if upper in {"ARRAY", "LIST"}:
        return DDL_TYPES.get(db, {}).get(LOGICAL_ARRAY, DEFAULT_DDL.get(db, "TEXT"))

    if (
        ((upper.startswith("STRUCT<") or upper.startswith("RECORD<")) and raw.endswith(">"))
        or (
            (
                upper.startswith("OBJECT(")
                or upper.startswith("STRUCT(")
                or upper.startswith("ROW(")
                or upper.startswith("TUPLE(")
            )
            and raw.endswith(")")
        )
    ):
        fields_raw = parse_struct_fields(raw)
        fields: list[tuple[str, str]] = []
        for name, typ in fields_raw:
            nested_t = _nested_ddl_for_dest(db, typ)
            fields.append((name, nested_t if nested_t else _leaf_ddl_for_nested(db, typ)))
        if not fields:
            return DDL_TYPES.get(db, {}).get(LOGICAL_JSON, DEFAULT_DDL.get(db, "TEXT"))
        return _format_struct_ddl(db, fields)

    # ClickHouse Nested — parallel arrays of a struct (CH docs). Create-new keeps
    # Nested on CH; lakehouse engines get ARRAY<STRUCT<…>>; non-nested → None
    # so callers fall through to JSONB/VARIANT (never invent Nested on PG).
    if upper.startswith("NESTED(") and raw.endswith(")"):
        fields_raw = parse_struct_fields(raw)
        if not fields_raw:
            return DDL_TYPES.get(db, {}).get(LOGICAL_JSON, DEFAULT_DDL.get(db, "TEXT"))
        if db == "clickhouse":
            inner = ", ".join(
                f"{n} {_nested_ddl_for_dest(db, t) or _leaf_ddl_for_nested(db, t)}"
                for n, t in fields_raw
            )
            return f"Nested({inner})"
        if db not in _NESTED_DDL_ENGINES:
            return None
        # Cross-engine: array-of-struct (Spark/Trino class), not invent flat columns.
        struct_carrier = "STRUCT<" + ", ".join(f"{n}:{t}" for n, t in fields_raw) + ">"
        return _nested_ddl_for_dest(db, f"ARRAY<{struct_carrier}>")

    if (upper.startswith("MAP<") and raw.endswith(">")) or (
        upper.startswith("MAP(") and raw.endswith(")")
    ):
        kv = parse_map_key_value(raw)
        if not kv:
            return DDL_TYPES.get(db, {}).get(LOGICAL_JSON, DEFAULT_DDL.get(db, "TEXT"))
        key_t, val_t = kv
        key_ddl = _nested_ddl_for_dest(db, key_t) or _leaf_ddl_for_nested(db, key_t)
        val_ddl = _nested_ddl_for_dest(db, val_t) or _leaf_ddl_for_nested(db, val_t)
        if db == "duckdb":
            return f"MAP({key_ddl}, {val_ddl})"
        if db == "clickhouse":
            return f"Map({key_ddl}, {val_ddl})"
        if db in {"trino", "presto"}:
            return f"map({key_ddl}, {val_ddl})"
        if db == "iceberg":
            return f"map<{key_ddl}, {val_ddl}>"
        if db == "snowflake":
            return f"MAP({key_ddl}, {val_ddl})"
        return f"MAP<{key_ddl},{val_ddl}>"

    return None


def _normalize_range_carrier(inferred: str | None) -> str | None:
    """Map BigQuery ``RANGE<T>`` / PG range twins to a canonical uppercase carrier.

    Google SQL RANGE has no Snowflake twin — create-new on non-range engines
    falls through to opaque JSON/VARIANT (surfaced as specialty collapse).
    """
    raw = strip_identity_qualifier(inferred)
    if not raw:
        return None
    upper = raw.upper().strip()
    if upper.startswith("RANGE<") and raw.endswith(">"):
        inner = raw[raw.index("<") + 1 : -1].strip().upper()
        return {
            "DATE": "DATERANGE",
            "DATETIME": "TSRANGE",
            "TIMESTAMP": "TSTZRANGE",
            "TIMESTAMPTZ": "TSTZRANGE",
        }.get(inner, f"RANGE<{inner}>" if inner else "RANGE")
    if upper == "RANGE":
        return "RANGE"
    if "MULTIRANGE" in upper or (upper.endswith("RANGE") and "<" not in upper):
        return upper
    return None


def _range_ddl_for_dest(db: str, inferred: str | None) -> str | None:
    """Emit native RANGE DDL (BigQuery / PostgreSQL) or None to fall through."""
    carrier = _normalize_range_carrier(inferred)
    if carrier is None:
        return None
    if db == "bigquery":
        bq = {
            "DATERANGE": "RANGE<DATE>",
            "TSRANGE": "RANGE<DATETIME>",
            "TSTZRANGE": "RANGE<TIMESTAMP>",
            "RANGE": "RANGE",
        }.get(carrier)
        if bq:
            return bq
        if carrier.startswith("RANGE<"):
            return carrier
        return None
    if db in {
        "postgresql",
        "postgres",
        "cockroachdb",
        "timescaledb",
        "alloydb",
        "yugabytedb",
        "citus",
        "supabase",
        "greenplum",
    }:
        # Bare RANGE without element — refuse invent of a concrete twin.
        if carrier == "RANGE":
            return None
        if carrier.startswith("RANGE<"):
            return None
        return carrier
    return None


def _is_unsigned_integer_decimal_carrier(inferred: str | None) -> bool:
    """True when a DECIMAL-logical carrier is really an unsigned integer widen.

    MySQL ``BIGINT UNSIGNED`` / ``UINT64`` map to LOGICAL_DECIMAL so they do not
    overflow signed BIGINT — but create-new must stay zero-scale, never invent
    ``NUMBER(38,10)`` fractional digits.
    """
    upper = strip_identity_qualifier(inferred).upper()
    if not upper:
        return False
    if re.match(r"^U?INT\d*$", upper.replace(" ", "")):
        return upper.startswith("UINT") or upper.startswith("INT") and "UNSIGNED" in upper
    if "UNSIGNED" in upper and re.search(
        r"\b(?:BIGINT|INTEGER|INT|MEDIUMINT|SMALLINT|TINYINT)\b", upper
    ):
        return True
    return False


def _decimal_ddl_for_dest(db: str, inferred: str | None) -> str:
    """Emit destination DECIMAL preserving source scale when possible.

    If source scale exceeds the destination platform cap, return the destination
    lossless text type instead of truncating fractional digits.
    """
    template = _DECIMAL_PARAM_TEMPLATES.get(db)
    default_ddl = DDL_TYPES.get(db, {}).get(LOGICAL_DECIMAL, DEFAULT_DDL.get(db, "TEXT"))
    # Bare BIGNUMERIC on BigQuery round-trip — keep the (76,38) token.
    src_u = strip_identity_qualifier(inferred).upper()
    if db == "bigquery" and (
        src_u == "BIGNUMERIC"
        or src_u.startswith("BIGNUMERIC(")
        or src_u == "BIGDECIMAL"
        or src_u.startswith("BIGDECIMAL(")
    ):
        precision, scale = parse_numeric_precision_scale(inferred)
        if precision is not None and scale is not None:
            return f"BIGNUMERIC({precision},{scale})"
        if precision is not None:
            return f"BIGNUMERIC({precision})"
        return "BIGNUMERIC"
    if not template:
        return default_ddl

    precision, scale = parse_numeric_precision_scale(inferred)
    cap_p, cap_s = _DECIMAL_CAPS.get(db, (38, 37))

    # No source params → platform default (PG stays bare NUMERIC; others use
    # a generous floor so values never truncate at write time).
    if precision is None and scale is None:
        # BIGINT UNSIGNED / UINT64 travel as LOGICAL_DECIMAL for overflow safety
        # but must not invent fractional scale (NUMBER(38,10) invent).
        if _is_unsigned_integer_decimal_carrier(inferred):
            if db == "postgresql":
                return "NUMERIC(20,0)"
            if db == "bigquery":
                return "BIGNUMERIC(20,0)"
            return template.format(p=min(cap_p, 38), s=0)
        if db == "postgresql":
            return default_ddl
        if db == "bigquery":
            return default_ddl  # bare BIGNUMERIC when scale unknown
        default_s = min(_DECIMAL_DEFAULT_SCALE.get(db, 10), cap_s)
        return template.format(p=cap_p, s=default_s)

    src_p = precision if precision is not None else cap_p
    src_s = scale if scale is not None else 0

    if src_s > cap_s:
        # Preserve digits as text rather than silently truncating scale.
        return DEFAULT_DDL.get(db, "TEXT")

    # Precision clamp is also silent data loss — refuse and use lossless text.
    if src_p > cap_p:
        return DEFAULT_DDL.get(db, "TEXT")

    out_s = min(src_s, cap_s)
    out_p = min(max(src_p, out_s), cap_p)
    if out_p < out_s:
        out_p = out_s
    return template.format(p=out_p, s=out_s)


def ddl_carrier_type(inferred: str | None) -> str:
    """Logical DDL carrier that keeps DECIMAL(p,s) / VECTOR(n) params.

    Use this for CREATE / column_types — never ``normalize_logical_type().upper()``,
    which strips precision and invents bare DECIMAL / VECTOR.
    """
    raw = (inferred or "").strip()
    if not raw:
        return "VARCHAR"
    # Arrow dtype strings → canonical carriers before nested/DECIMAL lookup.
    arrow = arrow_dtype_to_carrier(raw)
    if arrow is not None:
        raw = arrow
    upper = raw.upper()
    # Preserve nested carriers for Map / preflight (do not collapse to JSON/ARRAY).
    if upper.startswith(
        (
            "ARRAY<",
            "LIST<",
            "STRUCT<",
            "RECORD<",
            "MAP<",
            "RANGE<",
            "ARRAY(",
            "LIST(",
            "OBJECT(",
            "MAP(",
            "TUPLE(",
            "STRUCT(",
            "ROW(",
            "NESTED(",
        )
    ):
        return raw
    if upper.startswith("DATETIME64") or upper in {"DATETIME", "IPV4", "IPV6"}:
        return raw
    if upper.startswith("DATETIME(") and not upper.startswith("DATETIME2"):
        return raw
    # BigQuery / PG range twins — keep specialty tokens for create-new bind.
    if upper in {
        "RANGE",
        "DATERANGE",
        "TSRANGE",
        "TSTZRANGE",
        "INT4RANGE",
        "INT8RANGE",
        "NUMRANGE",
    } or (
        ("MULTIRANGE" in upper or (upper.endswith("RANGE") and upper != "RANGE"))
        and "<" not in upper
    ):
        return upper
    # Preserve native specialty carriers (INET, OBJECTID, …) — never collapse to
    # VARCHAR/TEXT before create-new DDL / risk stamping.
    specialty = specialty_carrier_base(raw)
    if specialty is not None:
        return specialty
    # Preserve UNSIGNED integer polarity — bare INTEGER invents signed 32-bit CREATE.
    if "UNSIGNED" in upper or re.search(r"\bUINT\d*\b", upper) or re.match(
        r"^UINT(8|16|32|64)\b", upper.replace(" ", "")
    ):
        return strip_identity_qualifier(raw).strip() or raw
    # ClickHouse UInt* (case-sensitive leading U).
    if re.match(r"^UInt(8|16|32|64)\b", strip_identity_qualifier(raw) or ""):
        return strip_identity_qualifier(raw).strip() or raw
    logical = normalize_logical_type(raw)
    if logical == LOGICAL_DECIMAL:
        # Preserve BIGNUMERIC polarity (76,38) vs bare DECIMAL/NUMERIC (38,9).
        if upper.startswith("BIGNUMERIC") or upper.startswith("BIGDECIMAL"):
            precision, scale = parse_numeric_precision_scale(raw)
            if precision is not None and scale is not None:
                return f"BIGNUMERIC({precision},{scale})"
            if precision is not None:
                return f"BIGNUMERIC({precision})"
            return "BIGNUMERIC"
        # BIGINT UNSIGNED / UINT64 travel as DECIMAL logical for overflow safety
        # but create-new must keep the unsigned token (never invent NUMBER(38,10)).
        if _is_unsigned_integer_decimal_carrier(raw):
            return strip_identity_qualifier(raw).strip() or raw
        precision, scale = parse_numeric_precision_scale(raw)
        if precision is not None and scale is not None:
            return f"DECIMAL({precision},{scale})"
        if precision is not None:
            return f"DECIMAL({precision})"
        return "DECIMAL"
    if logical == LOGICAL_VECTOR:
        # Preserve HALFVEC/SPARSEVEC encoding polarity — never invent dense VECTOR.
        enc_raw = strip_identity_qualifier(raw).upper().replace(" ", "")
        dim = parse_vector_dimension(raw)
        if enc_raw.startswith("HALFVEC"):
            return f"HALFVEC({dim})" if dim is not None else "HALFVEC"
        if enc_raw.startswith("SPARSEVEC"):
            return f"SPARSEVEC({dim})" if dim is not None else "SPARSEVEC"
        if dim is not None:
            return f"VECTOR({dim})"
        return "VECTOR"
    if logical == LOGICAL_FLOAT:
        return "FLOAT"
    if logical == LOGICAL_INTEGER:
        return "INTEGER"
    if logical == LOGICAL_BOOLEAN:
        return "BOOLEAN"
    if logical == LOGICAL_DATE:
        return "DATE"
    if logical == LOGICAL_DATETIME:
        # Preserve TZ polarity for CREATE — never collapse TIMESTAMPTZ → TIMESTAMP.
        raw_u = raw.upper().replace("_", " ")
        tz_tokens = (
            "TIMESTAMPTZ",
            "TIMESTAMP TZ",
            "TIMESTAMP LTZ",
            "TIMESTAMP WITH TIME ZONE",
            "TIMESTAMP WITH LOCAL TIME ZONE",
            "DATETIMEOFFSET",
        )
        ntz_tokens = (
            "TIMESTAMP NTZ",
            "TIMESTAMP WITHOUT TIME ZONE",
            "DATETIME2",
            "SMALLDATETIME",
        )
        if any(t in raw_u for t in tz_tokens) or raw_u.endswith(" WITH TIME ZONE"):
            return "TIMESTAMPTZ"
        if (
            any(t in raw_u for t in ntz_tokens)
            or raw_u.startswith("DATETIME(")
            or raw_u == "TIMESTAMP NTZ"
        ):
            return "TIMESTAMP_NTZ"
        return "TIMESTAMP"
    if logical == LOGICAL_TIME:
        return "TIME"
    if logical == LOGICAL_UUID:
        return "UUID"
    if logical == LOGICAL_OBJECTID:
        return "OBJECTID"
    if logical == LOGICAL_JSON:
        return "JSON"
    if logical == LOGICAL_ARRAY:
        return "ARRAY"
    if logical == LOGICAL_BINARY:
        return "BINARY"
    if logical == LOGICAL_INTERVAL:
        return "INTERVAL"
    if logical == LOGICAL_GEOGRAPHY:
        return "GEOGRAPHY"
    if logical == LOGICAL_TEXT:
        return "TEXT"
    if logical == LOGICAL_STRING:
        return "VARCHAR"
    return logical.upper() if logical else "VARCHAR"


def _normalize_dest_db(db_type: str | None) -> str:
    """Canonical destination engine id for DDL / cap lookups."""
    db = (db_type or "").strip().lower()
    # PostgreSQL family — DDL_TYPES / caps keyed only on ``postgresql``.
    # Without this, create-new invents TEXT for NUMBER/DATE/BOOLEAN with soft-pass.
    if db in {
        "postgres",
        "pg",
        "postgresql",
        "cockroachdb",
        "cockroach",
        "timescaledb",
        "timescale",
        "alloydb",
        "yugabytedb",
        "yugabyte",
        "citus",
        "supabase",
        "supabase_db",
        "greenplum",
        "greenplum_cloud",
        "neon",
        "neon_serverless",
        "azure_postgres",
        "aws_rds_postgres",
        "rds_postgres",
        "aurora",
        "aurora_postgres",
        "aurora-postgresql",
        "pgbouncer",
        "cloudsql_postgres",
        "gcp_cloud_sql_postgres",
        "cloud_sql_postgres",
        "opengauss",
        "open_gauss",
        "kingbase",
        "vastbase",
        "hologres",
        "tdsql",
        "materialize",
        "risingwave",
    }:
        return "postgresql"
    if db in {
        "mariadb",
        "tidb",
        "tidb_cloud",
        "mysql2",
        "aurora_mysql",
        "aurora-mysql",
        "singlestore",
        "memsql",
        "cloudsql_mysql",
        "gcp_cloud_sql_mysql",
        "rds_mysql",
        "maria",
        "percona",
        "doris",
        "starrocks",
        "oceanbase",
        "selectdb",
        # Product catalog wires these through mysql+pymysql (not PG wire).
        "polardb",
        "gaussdb",
        "goldendb",
        "vitess",
        "planetscale",
        "mysql_planetscale",
    }:
        return "mysql"
    if db in {
        "mongo",
        "mongodb",
        "documentdb",
        "document_db",
        "cosmos",
        "cosmos-mongodb",
        "cosmos_mongodb",
        "cosmosdb",
        "firestore",
    }:
        return "mongodb"
    if db in {
        "spark",
        "delta",
        "delta_lake",
        "databricks_sql",
        "unity_catalog",
        "databricks_azure",
        "databricks_aws",
        "databricks_gcp",
        "hive",
        "impala",
        "emr",
        "glue",
        "synapse_spark",
        "flink",
        "maxcompute",
        "odps",
        "databend",
    }:
        return "databricks"
    if db in {"apache_iceberg", "iceberg_rest", "nessie"}:
        return "iceberg"
    if db in {"opensearch", "amazon_elasticsearch", "elastic_cloud"}:
        return "elasticsearch"
    if db in {"amazon_dynamodb"}:
        return "dynamodb"
    if db in {"redis-kv", "redis_kv"}:
        return "redis"
    if db in {"ch", "clickhouse_cloud", "bytehouse"}:
        return "clickhouse"
    if db in {"redshift", "redshift_serverless", "amazon_redshift"}:
        return "redshift"
    if db in {"snowflake", "snowflake_aws", "snowflake_azure", "snowflake_gcp"}:
        return "snowflake"
    if db in {"athena", "amazon_athena", "aws_athena", "dremio"}:
        return "trino"
    # Spanner is NOT BigQuery — inventing DATETIME/BIGNUMERIC/TIME is illegal DDL.
    if db in {"spanner", "google_spanner", "cloud_spanner"}:
        return "spanner"
    if db in {"duckdb", "motherduck"}:
        return "duckdb"
    if db in {"sqlite", "libsql", "turso"}:
        return "sqlite"
    # Microsoft T-SQL family — one DDL SSOT (NVARCHAR / BIT / DATETIME2).
    if db in {
        "mssql",
        "azure_sql",
        "azure_sql_db",
        "azure_sql_mi",
        "azuresql",
        "azure-sql",
        "sqlazure",
        "synapse",
        "azure_synapse",
        "sql_server",
        "fabric",
        "fabric_sql",
        "fabric_warehouse",
    }:
        return "sqlserver"
    # No dedicated DDL map yet — generic SQL rather than soft-pass TEXT.
    if db in {
        "db2",
        "ibm_db2",
        "ibm-db2",
        "db2luw",
        "cassandra",
        "bigtable",
        "google_bigtable",
        "teradata",
        "vertica",
        "netezza",
        "exasol",
        "crate",
        "cratedb",
        "questdb",
        "pinot",
        "druid",
        "kylin",
        "beam",
        "datafusion",
    }:
        return "generic_sql"
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

    For datetime carriers, TZ polarity is preserved when the source declared it
    (TIMESTAMPTZ vs TIMESTAMP / TIMESTAMP_NTZ) so create-new does not invent
    the wrong clock semantics.

    Nested ARRAY/STRUCT/MAP carriers are preserved on lakehouse engines
    (Databricks, DuckDB, ClickHouse, Iceberg, BigQuery, Trino, Snowflake).
    """
    db = _normalize_dest_db(db_type)
    nested = _nested_ddl_for_dest(db, inferred)
    if nested:
        return nested
    # BigQuery RANGE / PG range twins before TEXT fall-through.
    range_ddl = _range_ddl_for_dest(db, inferred)
    if range_ddl:
        return range_ddl
    # MONEY/YEAR carriers before DECIMAL/INTEGER collapse so create-new keeps
    # engine-native tokens (SQL Server MONEY, MySQL YEAR).
    base_early = strip_identity_qualifier(inferred).upper()
    # ClickHouse DateTime64 / DateTime — before STRING fall-through.
    if base_early.startswith("DATETIME64") or base_early == "DATETIME" or (
        base_early.startswith("DATETIME(") and not base_early.startswith("DATETIME2")
    ):
        if db == "clickhouse":
            ch_native = _clickhouse_native_datetime_ddl(inferred)
            if ch_native:
                return ch_native
        tz_ddl = _datetime_ddl_for_dest(db, inferred)
        if tz_ddl:
            return tz_ddl
    if base_early in {"IPV4", "IPV6"}:
        if db == "clickhouse":
            return "IPv4" if base_early == "IPV4" else "IPv6"
        if db in {
            "postgresql",
            "postgres",
            "cockroachdb",
            "timescaledb",
            "alloydb",
            "yugabytedb",
            "citus",
            "supabase",
            "greenplum",
        }:
            return "INET"
        return "VARCHAR(45)"
    # Spark/Databricks VOID — typed null column; never invent STRING silently.
    if base_early == "VOID":
        if db == "databricks":
            return "VOID"
        return DDL_TYPES.get(db, {}).get(LOGICAL_TEXT, DEFAULT_DDL.get(db, "TEXT"))
    # Logical UUID → dialect-native UUID DDL (MySQL CHAR(36), PG UUID, …).
    # Exact CHAR(36)/VARCHAR(36) wires are preserved as-is — never promote a
    # plain VARCHAR(36) text column to PostgreSQL UUID (invalid for non-UUID
    # values). Also never collapse CHAR(36) through STRING→TEXT.
    if normalize_logical_type(inferred) == LOGICAL_UUID:
        types_early = DDL_TYPES.get(db) or {}
        native_uuid = types_early.get(LOGICAL_UUID)
        if native_uuid:
            return native_uuid
        return "VARCHAR(36)"
    if uuid_exact_wire_carrier(inferred):
        return strip_identity_qualifier(inferred).strip()
    # MongoDB ObjectId — dialect-native wire (never invent BigQuery VARCHAR).
    if base_early in {"OBJECTID", "OBJECT_ID"} or normalize_logical_type(inferred) == LOGICAL_OBJECTID:
        types_oid = DDL_TYPES.get(db) or {}
        native_oid = types_oid.get(LOGICAL_OBJECTID)
        if native_oid:
            return native_oid
        return _OBJECTID_DDL_DEFAULTS.get(db, "VARCHAR(24)")
    # Oracle LONG is a deprecated text LOB on Oracle dest (CLOB invent).
    # Off-Oracle, ``long`` is the Spark/Hive INT64 synonym — never invent TEXT
    # with soft-pass (INTEGER→TEXT allow-list greenwash). LONG→BIGINT is gated
    # by oracle_long_numeric_invent (Accept risk).
    if base_early == "LONG":
        if db == "oracle":
            return "CLOB"
        int_ddl = _integer_ddl_for_dest(db, "BIGINT")
        if int_ddl:
            return int_ddl
        return DDL_TYPES.get(db, {}).get(LOGICAL_INTEGER, "BIGINT")
    # SQL Server SYSNAME ≡ NVARCHAR(128) — never invent NVARCHAR(MAX)/TEXT.
    if base_early == "SYSNAME":
        if db == "sqlserver":
            return "NVARCHAR(128)"
        return "VARCHAR(128)"
    # SQL Server IDENTITY — preserve identity polarity on create-new (never TEXT).
    if base_early == "IDENTITY" or base_early.startswith("IDENTITY("):
        if db == "sqlserver":
            return "INT IDENTITY(1,1)"
        if db in {
            "postgresql",
            "postgres",
            "cockroachdb",
            "timescaledb",
            "alloydb",
            "yugabytedb",
            "citus",
            "supabase",
            "greenplum",
            "redshift",
        }:
            return "INTEGER GENERATED BY DEFAULT AS IDENTITY"
        if db in {"mysql", "mariadb", "tidb"}:
            return "INT AUTO_INCREMENT"
        int_ddl = _integer_ddl_for_dest(db, "INTEGER")
        return int_ddl or DDL_TYPES.get(db, {}).get(LOGICAL_INTEGER, "INTEGER")
    # Oracle LONG RAW — unbounded binary LOB (not fixed RAW).
    if base_early == "LONG RAW" or base_early.replace(" ", "") == "LONGRAW":
        if db == "oracle":
            return "BLOB"
        if db in {"postgresql", "postgres", "cockroachdb", "timescaledb", "alloydb", "yugabytedb", "citus", "supabase", "greenplum", "redshift"}:
            return "BYTEA"
        if db in {"mysql", "mariadb", "tidb"}:
            return "LONGBLOB"
        if db in {"sqlserver", "mssql"}:
            return "VARBINARY(MAX)"
        if db == "snowflake":
            return "BINARY"
        if db == "bigquery":
            return "BYTES"
        return DDL_TYPES.get(db, {}).get(LOGICAL_BINARY, "BYTEA")
    # IEEE half / float16 — stamp REAL/FLOAT32, never invent DOUBLE or TEXT.
    if base_early in {"HALF", "HALFFLOAT", "FLOAT16"}:
        types_h = DDL_TYPES.get(db) or {}
        if db in {"postgresql", "postgres", "cockroachdb", "timescaledb", "alloydb", "yugabytedb", "citus", "supabase", "greenplum", "redshift"}:
            return "REAL"
        if db in {"sqlserver", "mssql"}:
            return "REAL"
        if db in {"mysql", "mariadb", "tidb"}:
            return "FLOAT"
        if db == "oracle":
            return "BINARY_FLOAT"
        if db in {"databricks", "spark", "delta", "delta_lake"}:
            return "FLOAT"
        if db in {"snowflake", "bigquery"}:
            return types_h.get(LOGICAL_FLOAT, "FLOAT")
        if db == "iceberg":
            return "float"
        return types_h.get(LOGICAL_FLOAT, "FLOAT")
    # Opaque PG USER-DEFINED / UDT — stamp open text; specialty collapse forces Accept risk.
    if base_early in {"USER-DEFINED", "USER_DEFINED"}:
        return DDL_TYPES.get(db, {}).get(LOGICAL_TEXT, DEFAULT_DDL.get(db, "TEXT"))
    # ClickHouse Enum8/Enum16 / Nothing / Dynamic — keep native or TEXT + specialty collapse.
    if base_early.startswith("ENUM8") or base_early.startswith("ENUM16"):
        if db == "clickhouse":
            return "Enum8" if base_early.startswith("ENUM8") else "Enum16"
        return DDL_TYPES.get(db, {}).get(LOGICAL_TEXT, DEFAULT_DDL.get(db, "TEXT"))
    if base_early in {"NOTHING", "DYNAMIC"}:
        if db == "clickhouse":
            return base_early.title() if base_early == "NOTHING" else "Dynamic"
        return DDL_TYPES.get(db, {}).get(LOGICAL_TEXT, DEFAULT_DDL.get(db, "TEXT"))
    # ClickHouse AggregateFunction / SimpleAggregateFunction — opaque state; TEXT + collapse.
    if base_early.startswith("AGGREGATEFUNCTION") or base_early.startswith(
        "SIMPLEAGGREGATEFUNCTION"
    ):
        if db == "clickhouse":
            return strip_identity_qualifier(inferred).strip() or base_early
        return DDL_TYPES.get(db, {}).get(LOGICAL_TEXT, DEFAULT_DDL.get(db, "TEXT"))
    # IBM DECFLOAT — IEEE decimal float; never invent NUMBER(p,0) from digit count.
    if base_early == "DECFLOAT" or base_early.startswith("DECFLOAT("):
        if db in {
            "postgresql",
            "postgres",
            "cockroachdb",
            "timescaledb",
            "alloydb",
            "yugabytedb",
            "citus",
            "supabase",
            "greenplum",
        }:
            return "NUMERIC"
        if db == "oracle":
            return "BINARY_DOUBLE"
        if db == "sqlserver":
            return "FLOAT"
        if db == "bigquery":
            return "BIGNUMERIC"
        if db == "snowflake":
            return "FLOAT"
        return DDL_TYPES.get(db, {}).get(LOGICAL_FLOAT, "DOUBLE PRECISION")
    # Redshift HLLSKETCH — keep native or fall to VARCHAR with specialty collapse.
    if base_early == "HLLSKETCH":
        if db == "redshift":
            return "HLLSKETCH"
        return DDL_TYPES.get(db, {}).get(LOGICAL_TEXT, DEFAULT_DDL.get(db, "TEXT"))
    # Oracle ANYDATA — polymorphic envelope; JSON/CLOB wire off-engine.
    if base_early == "ANYDATA":
        if db == "oracle":
            return "ANYDATA"
        return DDL_TYPES.get(db, {}).get(LOGICAL_JSON, DEFAULT_DDL.get(db, "TEXT"))
    # PostgreSQL jsonpath — specialty path expression type (never invent TEXT).
    if base_early == "JSONPATH":
        if db in {
            "postgresql",
            "postgres",
            "cockroachdb",
            "timescaledb",
            "alloydb",
            "yugabytedb",
            "citus",
            "supabase",
            "greenplum",
        }:
            return "JSONPATH"
        return DDL_TYPES.get(db, {}).get(LOGICAL_TEXT, DEFAULT_DDL.get(db, "TEXT"))
    # DynamoDB AttributeValue round-trip — keep wire codes on DynamoDB dest.
    if db == "dynamodb" and base_early in _DYNAMODB_ATTR_LOGICAL:
        return base_early
    # Apache Arrow dtype paste — normalize to carrier then continue mapping.
    arrow_early = arrow_dtype_to_carrier(inferred)
    if arrow_early is not None and arrow_early.upper() != base_early:
        return ddl_type(db, arrow_early)
    # Elasticsearch specialty field types — keep native ES tokens on ES dest.
    if base_early in {
        "KEYWORD",
        "SCALED_FLOAT",
        "GEO_POINT",
        "GEO_SHAPE",
        "DENSE_VECTOR",
        "SPARSE_VECTOR",
        "FLATTENED",
        "IP",
        "VERSION",
        "COMPLETION",
        "SEARCH_AS_YOU_TYPE",
        "TOKEN_COUNT",
        "RANK_FEATURE",
        "RANK_FEATURES",
    }:
        if db in {"elasticsearch", "opensearch", "amazon_elasticsearch", "elastic_cloud"}:
            return {
                "KEYWORD": "keyword",
                "SCALED_FLOAT": "scaled_float",
                "GEO_POINT": "geo_point",
                "GEO_SHAPE": "geo_shape",
                "DENSE_VECTOR": "dense_vector",
                "SPARSE_VECTOR": "sparse_vector",
                "FLATTENED": "flattened",
                "IP": "ip",
                "VERSION": "version",
                "COMPLETION": "completion",
                "SEARCH_AS_YOU_TYPE": "search_as_you_type",
                "TOKEN_COUNT": "token_count",
                "RANK_FEATURE": "rank_feature",
                "RANK_FEATURES": "rank_features",
            }[base_early]
        if base_early in {"GEO_POINT", "GEO_SHAPE"}:
            geo = _geography_ddl_for_dest(db, "GEOGRAPHY")
            if geo:
                return geo
            return DDL_TYPES.get(db, {}).get(LOGICAL_GEOGRAPHY, "TEXT")
        if base_early in {"DENSE_VECTOR", "SPARSE_VECTOR"}:
            return DDL_TYPES.get(db, {}).get(LOGICAL_VECTOR, "TEXT")
        if base_early == "IP":
            if db in {
                "postgresql",
                "postgres",
                "cockroachdb",
                "timescaledb",
                "alloydb",
                "yugabytedb",
                "citus",
                "supabase",
                "greenplum",
            }:
                return "INET"
            if db == "clickhouse":
                return "IPv4"
            return "VARCHAR(45)"
        if base_early == "FLATTENED":
            return DDL_TYPES.get(db, {}).get(LOGICAL_JSON, DEFAULT_DDL.get(db, "TEXT"))
        if base_early == "SCALED_FLOAT":
            return DDL_TYPES.get(db, {}).get(LOGICAL_FLOAT, "DOUBLE PRECISION")
        if base_early == "KEYWORD":
            string_ddl = _string_ddl_for_dest(db, "VARCHAR")
            return string_ddl or DDL_TYPES.get(db, {}).get(LOGICAL_STRING, "TEXT")
    # BIGNUMERIC before DECIMAL path so (76,38) polarity is not lost on BQ
    # round-trip; non-BQ engines use decimal caps (Snowflake NUMBER max 38).
    if base_early == "BIGNUMERIC" or base_early.startswith("BIGNUMERIC(") or (
        base_early == "BIGDECIMAL" or base_early.startswith("BIGDECIMAL(")
    ):
        return _decimal_ddl_for_dest(db, inferred)
    if base_early in {"MONEY", "CURRENCY"}:
        if db == "sqlserver":
            return "MONEY"
        if db == "postgresql":
            return "DECIMAL(19,4)"
        # SQLite has no fixed-point — TEXT avoids NUMERIC affinity IEEE loss.
        if db == "sqlite":
            return "TEXT"
        return (
            _decimal_ddl_for_dest(db, "DECIMAL(19,4)")
            if db in _DECIMAL_PARAM_TEMPLATES
            else "DECIMAL(19,4)"
        )
    if base_early == "SMALLMONEY":
        if db == "sqlserver":
            return "SMALLMONEY"
        if db == "sqlite":
            return "TEXT"
        return (
            _decimal_ddl_for_dest(db, "DECIMAL(10,4)")
            if db in _DECIMAL_PARAM_TEMPLATES
            else "DECIMAL(10,4)"
        )
    if base_early == "YEAR" or base_early.startswith("YEAR("):
        if db == "mysql":
            return "YEAR"
        return "SMALLINT"
    # SQL Server ROWVERSION / TIMESTAMP synonym — binary concurrency token.
    # Never map to temporal TIMESTAMP (classic MSSQL→PG migration footgun).
    if base_early == "ROWVERSION":
        if db == "sqlserver":
            return "ROWVERSION"
        if db in {
            "postgresql",
            "postgres",
            "cockroachdb",
            "timescaledb",
            "alloydb",
            "yugabytedb",
            "citus",
            "supabase",
            "greenplum",
            "redshift",
        }:
            return "BYTEA"
        if db in {"mysql", "mariadb", "tidb"}:
            return "BINARY(8)"
        if db == "oracle":
            return "RAW(8)"
        return "BINARY(8)"
    # SQL Server hierarchyid — AWS DMS/string collapse; we prefer LTREE on PG
    # (slash→dot polarity) so tree semantics survive create-new.
    if base_early == "HIERARCHYID":
        if db == "sqlserver":
            return "HIERARCHYID"
        if db in {
            "postgresql",
            "postgres",
            "cockroachdb",
            "timescaledb",
            "alloydb",
            "yugabytedb",
            "citus",
            "supabase",
            "greenplum",
        }:
            return "LTREE"
        if db == "oracle":
            return "VARCHAR2(892)"
        return "VARCHAR(892)"
    if base_early in {"XML", "XMLTYPE"}:
        if db == "sqlserver":
            return "XML"
        if db in {
            "postgresql",
            "postgres",
            "cockroachdb",
            "timescaledb",
            "alloydb",
            "yugabytedb",
            "citus",
            "supabase",
            "greenplum",
        }:
            return "XML"
        if db == "oracle":
            return "XMLTYPE"
        if db in {"mysql", "mariadb", "tidb"}:
            return "LONGTEXT"
        return "TEXT"
    # SQL Server sql_variant — no PG twin (AWS SCT → VARCHAR(8000)).
    # JSONB preserves a typed envelope better than opaque TEXT for create-new.
    if base_early == "SQL_VARIANT":
        if db == "sqlserver":
            return "SQL_VARIANT"
        if db in {
            "postgresql",
            "postgres",
            "cockroachdb",
            "timescaledb",
            "alloydb",
            "yugabytedb",
            "citus",
            "supabase",
            "greenplum",
        }:
            return "JSONB"
        if db in {"snowflake", "databricks"}:
            return "VARIANT"
        if db == "oracle":
            return "CLOB"
        return "VARCHAR(8000)"
    if base_early in {"ROWID", "UROWID"}:
        if db == "oracle":
            return base_early
        # Physical row addresses are not portable — surface as bounded string.
        if db == "sqlserver":
            return "VARCHAR(18)"
        return "VARCHAR(18)"
    # SQL Server SMALLDATETIME — one-minute accuracy (Microsoft docs).
    # SQLines/AWS SCT map to TIMESTAMP(0); we keep the native token on MSSQL.
    if base_early == "SMALLDATETIME":
        if db == "sqlserver":
            return "SMALLDATETIME"
        if db in {
            "postgresql",
            "postgres",
            "cockroachdb",
            "timescaledb",
            "alloydb",
            "yugabytedb",
            "citus",
            "supabase",
            "greenplum",
        }:
            return "TIMESTAMP(0)"
        if db in {"mysql", "mariadb", "tidb"}:
            return "DATETIME"
        if db == "oracle":
            return "TIMESTAMP(0)"
        if db == "snowflake":
            return "TIMESTAMP_NTZ(0)"
        # Engines that take no temporal typmod (BigQuery DATETIME, Databricks
        # TIMESTAMP, ClickHouse DateTime64) reject a literal TIMESTAMP(0) — let
        # the shared NTZ mapper pick a valid, non-narrowing column.
        sdt_ddl = _datetime_ddl_for_dest(db, "SMALLDATETIME")
        if sdt_ddl:
            return _apply_temporal_fsp(db, sdt_ddl, 0)
        return "TIMESTAMP(0)"
    enum_ddl = _enum_set_ddl_for_dest(db, inferred)
    if enum_ddl:
        return enum_ddl
    logical = normalize_logical_type(inferred)
    if logical == LOGICAL_DECIMAL and db in _DECIMAL_PARAM_TEMPLATES:
        return _decimal_ddl_for_dest(db, inferred)
    if logical == LOGICAL_VECTOR:
        return _vector_ddl_for_dest(db, inferred)
    if logical == LOGICAL_DATETIME:
        tz_ddl = _datetime_ddl_for_dest(db, inferred)
        if tz_ddl:
            return tz_ddl
    if logical == LOGICAL_TIME:
        time_ddl = _time_ddl_for_dest(db, inferred)
        if time_ddl:
            return time_ddl
    if logical == LOGICAL_INTERVAL:
        interval_ddl = _interval_ddl_for_dest(db, inferred)
        if interval_ddl:
            return interval_ddl
    if logical == LOGICAL_GEOGRAPHY:
        geo_ddl = _geography_ddl_for_dest(db, inferred)
        if geo_ddl:
            return geo_ddl
    if logical == LOGICAL_FLOAT:
        float_ddl = _float_ddl_for_dest(db, inferred)
        if float_ddl:
            return float_ddl
    if logical == LOGICAL_INTEGER:
        int_ddl = _integer_ddl_for_dest(db, inferred)
        if int_ddl:
            return int_ddl
    if logical == LOGICAL_BINARY and is_bitstring_carrier(inferred):
        bit_ddl = _bitstring_ddl_for_dest(db, inferred)
        if bit_ddl:
            return bit_ddl
    # Bounded BINARY/VARBINARY/BYTES(n) — create-new must not invent unbounded
    # BLOB and silently accept oversize payloads (Fivetran/BQ max_length class).
    if logical == LOGICAL_BINARY:
        binary_ddl = _binary_ddl_for_dest(db, inferred)
        if binary_ddl:
            return binary_ddl
    # Fielded nested carriers without engine-native DDL → opaque JSON/VARIANT
    # (Airbyte Destinations V2 document path). Never invent STRUCT on PG/MySQL.
    if logical in {LOGICAL_STRUCT, LOGICAL_MAP}:
        return DDL_TYPES.get(db, {}).get(LOGICAL_JSON, DEFAULT_DDL.get(db, "TEXT"))
    # SERIAL / BIGSERIAL create-new — preserve identity semantics on PG-family.
    base_carrier = strip_identity_qualifier(inferred).upper()
    if base_carrier in {"SERIAL", "BIGSERIAL", "SMALLSERIAL"} and db in {
        "postgresql",
        "redshift",
    }:
        return base_carrier
    if base_carrier == "CITEXT" and db == "postgresql":
        return "CITEXT"
    # PostgreSQL specialty carriers — create-new must not invent TEXT/INTEGER
    # and lose bind/quarantine algorithms (Airbyte/HVR Compare class).
    if db in {"postgresql", "postgres", "cockroachdb", "timescaledb", "alloydb", "yugabytedb", "citus", "supabase", "greenplum"}:
        _pg_native = {
            "INET",
            "CIDR",
            "MACADDR",
            "MACADDR8",
            "POINT",
            "LINE",
            "LSEG",
            "BOX",
            "PATH",
            "POLYGON",
            "CIRCLE",
            "PG_LSN",
            "OID",
            "TID",
            "XID",
            "XID8",
            "CID",
            "HSTORE",
            "XML",
            "LTREE",
            "TSVECTOR",
            "TSQUERY",
            "JSONB",
            "JSONPATH",
            "TXID_SNAPSHOT",
            "PG_SNAPSHOT",
        }
        if base_carrier in _pg_native:
            return base_carrier
        if "MULTIRANGE" in base_carrier or (
            base_carrier.endswith("RANGE") and base_carrier != "RANGE"
        ):
            return base_carrier
    # Bounded VARCHAR/CHAR(n) + COLLATE — create-new must not invent TEXT and
    # drop CI/AI semantics (Airbyte/Informatica class schema loss).
    # Unlimited VARCHAR(MAX)/TEXT on Oracle — CLOB, never invent VARCHAR2(4000).
    if logical in {LOGICAL_STRING, LOGICAL_TEXT}:
        if is_unlimited_string_carrier(inferred) and db == "oracle":
            national = is_national_string_carrier(inferred)
            return "NCLOB" if national else "CLOB"
        string_ddl = _string_ddl_for_dest(db, inferred)
        if string_ddl:
            return _with_collation_clause(db, inferred, string_ddl, logical)
    result = DDL_TYPES.get(db, {}).get(logical, DEFAULT_DDL.get(db, "TEXT"))
    return _with_collation_clause(db, inferred, result, logical)


_STRING_DDL_CAPS: Final[dict[str, int]] = {
    "mysql": 16383,
    "mariadb": 16383,
    "postgresql": 10485760,
    "sqlserver": 4000,
    "oracle": 4000,
    "redshift": 65535,
    "bigquery": 16483,
    # Snowflake VARCHAR max is 16 MB (UTF-8); preserve source (n) on create-new.
    "snowflake": 16777216,
    # Databricks SQL VARCHAR(n) max 65535; bare STRING stays unbounded.
    "databricks": 65535,
}

_BINARY_DDL_CAPS: Final[dict[str, int]] = {
    "mysql": 65535,
    "mariadb": 65535,
    "sqlserver": 8000,
    "oracle": 2000,
    "snowflake": 8388608,
    "redshift": 1024000,
    "bigquery": 10485760,
    # Iceberg fixed(L) — binary remains unbounded; bounded create-new uses fixed.
    "iceberg": 1048576,
    # ClickHouse FixedString(n) max practical bound for create-new.
    "clickhouse": 1048576,
}


def _float_ddl_for_dest(db: str, inferred: str | None) -> str | None:
    """Width-preserving IEEE invent — never stamp DOUBLE from REAL/BINARY_FLOAT.

    Bare ``FLOAT`` stays dialect-default (PG ≡ DOUBLE PRECISION). Explicit
    single-precision tokens keep a single-precision sink so create-new does not
    soft-pass invent-widen as identity.
    """
    if normalize_logical_type(inferred) != LOGICAL_FLOAT:
        return None
    upper = re.sub(
        r"\bUNSIGNED\b",
        "",
        strip_identity_qualifier(inferred).upper(),
    ).strip().replace(" ", "")
    bits = float_mantissa_bits(inferred)
    if bits is None:
        return None
    # IEEE half handled in ddl_type early path.
    if bits <= 11:
        return None
    explicit_single = upper in {
        "REAL",
        "FLOAT4",
        "FLOAT32",
        "BINARY_FLOAT",
    } or upper.startswith("REAL(")
    if not explicit_single:
        m = re.match(r"^FLOAT\((\d+)\)$", upper)
        if m and int(m.group(1)) <= 24:
            explicit_single = True
    if bits <= 24 and explicit_single:
        if db in {
            "postgresql",
            "postgres",
            "cockroachdb",
            "timescaledb",
            "alloydb",
            "yugabytedb",
            "citus",
            "supabase",
            "greenplum",
            "redshift",
            "duckdb",
        }:
            return "REAL"
        if db in {"mysql", "mariadb", "tidb"}:
            return "FLOAT"
        if db in {"sqlserver", "mssql"}:
            return "REAL"
        if db == "oracle":
            return "BINARY_FLOAT"
        if db == "snowflake":
            # Snowflake FLOAT is IEEE-64 — stamp FLOAT and rely on Map dest token.
            return "FLOAT"
        if db == "bigquery":
            return "FLOAT64"
        if db in {"databricks", "spark", "delta"}:
            return "FLOAT"
        if db == "iceberg":
            return "float"
        if db == "clickhouse":
            return "Float32"
        if db in {"trino", "presto"}:
            return "real"
        return "REAL"
    # Bare FLOAT with single-precision fail-closed bits: keep IEEE-32 on engines
    # whose FLOAT/float token is single (never invent list<double> from ARRAY<FLOAT>).
    # PostgreSQL-family FLOAT ≡ DOUBLE PRECISION — fall through to dialect default.
    if bits <= 24 and upper == "FLOAT":
        if db in {"mysql", "mariadb", "tidb"}:
            return "FLOAT"
        if db in {"databricks", "spark", "delta", "delta_lake", "databricks_sql"}:
            return "FLOAT"
        if db == "iceberg":
            return "float"
        if db == "clickhouse":
            return "Float32"
        if db in {"trino", "presto"}:
            return "real"
    # Double / bare FLOAT — dialect default.
    return _FLOAT_DDL.get(db)


def _integer_ddl_for_dest(db: str, inferred: str | None) -> str | None:
    """Width-preserving integer invent — never stamp BIGINT from TINYINT/INT.

    Engines with a single integer wire (BigQuery INT64) still invent that wire;
    operators see INT64 on Map rather than a false TINYINT identity stamp.
    """
    if normalize_logical_type(inferred) != LOGICAL_INTEGER:
        return None
    upper = strip_identity_qualifier(inferred).upper()
    # YEAR / SERIAL handled in ddl_type early paths.
    if upper == "YEAR" or upper.startswith("YEAR("):
        return None
    if upper in {"SERIAL", "BIGSERIAL", "SMALLSERIAL", "TINYSERIAL"}:
        return None
    width = integer_bit_width(inferred)
    if width is None:
        return None
    unsigned = "UNSIGNED" in upper or bool(re.search(r"\bUINT\d*\b", upper))

    if db in {"mysql", "mariadb", "tidb"}:
        if width <= 8:
            return "TINYINT"
        if width == 9:
            return "TINYINT UNSIGNED"
        if width <= 16:
            return "SMALLINT"
        if width == 17:
            return "SMALLINT UNSIGNED"
        if width <= 24:
            return "MEDIUMINT"
        if width == 25:
            return "MEDIUMINT UNSIGNED"
        if width <= 32:
            return "INT"
        if width == 33:
            return "INT UNSIGNED"
        if width <= 64:
            return "BIGINT"
        return "BIGINT UNSIGNED"

    if db in {
        "postgresql",
        "postgres",
        "cockroachdb",
        "timescaledb",
        "alloydb",
        "yugabytedb",
        "citus",
        "supabase",
        "greenplum",
        "redshift",
        "duckdb",
    }:
        if width <= 16:
            return "SMALLINT"
        if width <= 32:
            return "INTEGER"
        return "BIGINT"

    if db in {"sqlserver", "mssql"}:
        # T-SQL TINYINT is 0–255 (unsigned 8).
        if width <= 8 or (unsigned and width == 9):
            return "TINYINT"
        if width <= 16:
            return "SMALLINT"
        if width <= 32:
            return "INT"
        return "BIGINT"

    if db == "oracle":
        if width <= 16:
            return "NUMBER(5,0)"
        if width <= 32:
            return "NUMBER(10,0)"
        return "NUMBER(38,0)"

    if db == "snowflake":
        if width <= 16:
            return "SMALLINT"
        if width <= 32:
            return "INTEGER"
        return "BIGINT"

    if db == "bigquery":
        return "INT64"

    if db in {"databricks", "spark", "delta"}:
        if width <= 32:
            return "INT"
        return "BIGINT"

    if db == "iceberg":
        if width <= 32:
            return "int"
        return "long"

    if db == "clickhouse":
        if width <= 8:
            return "Int8" if not unsigned else "UInt8"
        if width <= 16:
            return "Int16" if not unsigned else "UInt16"
        if width <= 32:
            return "Int32" if not unsigned else "UInt32"
        return "Int64" if not unsigned else "UInt64"

    if db in {"trino", "presto"}:
        if width <= 16:
            return "smallint"
        if width <= 32:
            return "integer"
        return "bigint"

    if db == "sqlite":
        return "INTEGER"

    return None


def _string_ddl_for_dest(db: str, inferred: str | None) -> str | None:
    """Emit bounded CHAR/VARCHAR(n) / STRING(n) when source width is known."""
    width = parse_string_carrier_width(inferred)
    if width is None:
        return None
    cap = _STRING_DDL_CAPS.get(db)
    if cap is None:
        return None
    fixed = is_fixed_width_char_carrier(inferred)
    upper = strip_identity_qualifier(inferred).upper()
    national = bool(
        re.search(r"\bN(?:VAR)?CHAR\b", upper)
        or re.search(r"\bNATIONAL\s+CHARACTER\b", upper)
        or re.search(r"\bNATIONAL\s+CHAR\b", upper)
    )
    if db == "bigquery":
        return f"STRING({min(width, cap)})"
    if db == "snowflake":
        return f"VARCHAR({min(width, cap)})"
    if db == "databricks":
        # Unity Catalog / Databricks SQL VARCHAR(n) — never invent bare STRING
        # and drop declared width (Delta schema-enforcement class).
        return f"VARCHAR({min(width, cap)})"
    if db == "sqlserver":
        # Preserve source national polarity — never invent NCHAR from CHAR.
        if not fixed and width > 4000:
            return "NVARCHAR(MAX)" if national else "VARCHAR(MAX)"
        w = min(width, 4000)
        if national:
            return f"{'NCHAR' if fixed else 'NVARCHAR'}({w})"
        return f"{'CHAR' if fixed else 'VARCHAR'}({w})"
    if db in {"mysql", "mariadb"}:
        # Preserve NATIONAL CHAR/VARCHAR — never invent non-national from NCHAR.
        if national:
            return f"{'NCHAR' if fixed else 'NVARCHAR'}({min(width, cap)})"
        return f"{'CHAR' if fixed else 'VARCHAR'}({min(width, cap)})"
    if db in {"postgresql", "redshift"}:
        # No national types — CHAR/VARCHAR; remap polarity via national_charset_would_collapse.
        return f"{'CHAR' if fixed else 'VARCHAR'}({min(width, cap)})"
    if db == "oracle":
        w = min(width, cap)
        if national:
            return f"{'NCHAR' if fixed else 'NVARCHAR2'}({w})"
        unit = "CHAR" if re.search(r"\(\s*\d+\s*CHAR\s*\)", upper) else "BYTE"
        if re.search(r"\(\s*\d+\s*BYTE\s*\)", upper):
            unit = "BYTE"
        return f"{'CHAR' if fixed else 'VARCHAR2'}({w} {unit})"
    return None


def is_fixed_width_binary_carrier(inferred: str | None, *, dest_db: str = "") -> bool:
    """True for BINARY(n) / RAW(n) / FIXED(n) (not VARBINARY / VARBYTE / BYTES / LONG RAW).

    Snowflake ``BINARY(n)`` is max-length variable (not MySQL-style pad-fixed) —
    when ``dest_db`` is snowflake, do not treat it as fixed-pad.
    """
    upper = strip_identity_qualifier(inferred).upper()
    if not upper:
        return False
    # Oracle LONG RAW is an unbounded LOB — never treat as fixed RAW(n).
    if re.search(r"\bLONG\s+RAW\b", upper) or upper.replace(" ", "") == "LONGRAW":
        return False
    if re.search(r"\b(?:VARBINARY|VARBYTE|BYTES|BYTEA|BLOB|IMAGE)\b", upper):
        return False
    if not re.search(r"\b(?:BINARY|RAW|FIXED|FIXEDSTRING)\b", upper):
        return False
    db = (dest_db or "").strip().lower()
    if db in {"snowflake", "sf"} and re.search(r"\bBINARY\b", upper):
        return False
    return True


def _binary_ddl_for_dest(db: str, inferred: str | None) -> str | None:
    """Emit bounded BINARY/VARBINARY/BYTES(n) when source byte width is known."""
    width = parse_binary_carrier_width(inferred)
    if width is None:
        return None
    cap = _BINARY_DDL_CAPS.get(db)
    if cap is None:
        return None
    w = min(width, cap)
    fixed = is_fixed_width_binary_carrier(inferred, dest_db=db)
    if db == "bigquery":
        return f"BYTES({w})"
    if db == "snowflake":
        return f"BINARY({w})"
    if db == "redshift":
        return f"VARBYTE({w})"
    if db == "sqlserver":
        if not fixed and width > 8000:
            return "VARBINARY(MAX)"
        return f"{'BINARY' if fixed else 'VARBINARY'}({min(w, 8000)})"
    if db in {"mysql", "mariadb"}:
        return f"{'BINARY' if fixed else 'VARBINARY'}({w})"
    if db == "oracle":
        # RAW max 2000; wider binary → BLOB (unbounded).
        if width > 2000:
            return "BLOB"
        return f"RAW({w})"
    if db == "iceberg":
        # Spec: fixed(L) is fixed-length bytes; bare binary is unbounded.
        return f"fixed({w})"
    if db == "clickhouse":
        # FixedString(n) when source declares fixed width; else String.
        if fixed or strip_identity_qualifier(inferred).upper().startswith("FIXEDSTRING"):
            return f"FixedString({w})"
        return None
    return None


def _collation_compatible_with_dest(db: str, collation: str) -> bool:
    """Refuse cross-engine invent (MySQL utf8mb4_* on PG, etc.)."""
    coll = (collation or "").strip()
    if not coll or len(coll) > 128:
        return False
    upper = coll.upper()
    # MySQL-only tokens — do not treat SQL Server Latin1_General_CI_* as MySQL.
    mysqlish = bool(
        "UTF8MB4" in upper
        or "UTF8MB3" in upper
        or "_0900_" in upper
        or "_AI_CI" in upper
        or "_AS_CI" in upper
        or (
            upper.endswith(("_UNICODE_CI", "_GENERAL_CI"))
            and "LATIN1_GENERAL" not in upper
            and not upper.startswith("SQL_")
        )
    )
    windowish = bool(
        re.search(r"LATIN1_GENERAL|SQL_LATIN|_C[IS]_A[IS]", upper)
        or upper.startswith("SQL_")
    )
    if db in {"mysql", "mariadb"}:
        if not re.match(r"^[A-Za-z0-9_]+$", coll):
            return False
        if windowish:
            return False
        return bool(
            re.search(r"UTF8|LATIN1|ASCII|UCA|BINARY|UNICODE|GENERAL", upper)
            or upper.endswith(("_CI", "_CS", "_BIN"))
        )
    if db == "sqlserver":
        if not re.match(r"^[A-Za-z0-9_]+$", coll):
            return False
        if mysqlish:
            return False
        return bool(
            re.search(r"LATIN|SQL_|JAPANESE|CHINESE|KOREAN|CYRILLIC|_C[IS]_A[IS]", upper)
        )
    if db in {"postgresql", "redshift"}:
        if coll.lower() in {"default", "c", "posix"}:
            return False
        # ICU / libc names only — never invent MySQL/SS collations on PG.
        if mysqlish or windowish:
            return False
        return bool(re.match(r"^[A-Za-z0-9_.\-]+$", coll))
    return False


def _format_collate_clause(db: str, collation: str) -> str:
    coll = collation.strip()
    if db in {"postgresql", "redshift"}:
        safe = coll.replace('"', "")
        return f' COLLATE "{safe}"'
    return f" COLLATE {coll}"


def _with_collation_clause(
    db: str,
    inferred: str | None,
    ddl: str,
    logical: str,
) -> str:
    """Re-attach source COLLATE onto create-new string DDL when engine-compatible."""
    if not ddl or logical not in {LOGICAL_STRING, LOGICAL_TEXT}:
        return ddl
    if db not in {"mysql", "mariadb", "postgresql", "sqlserver", "redshift"}:
        return ddl
    if re.search(r"\bCOLLATE\b", ddl, re.I):
        return ddl
    coll = parse_collation(inferred)
    if not coll or not _collation_compatible_with_dest(db, coll):
        return ddl
    return f"{ddl}{_format_collate_clause(db, coll)}"


def _interval_ddl_for_dest(db: str, inferred: str | None) -> str | None:
    """Emit family-correct INTERVAL DDL when the destination supports qualifiers.

    Oracle ``INTERVAL DAY(d) TO SECOND(s)`` / ``YEAR(y) TO MONTH`` precision is
    preserved on Oracle create-new (ANSI leading-field precision contract).
    """
    fam = interval_family(inferred)
    raw_u = strip_identity_qualifier(inferred).upper()
    if fam == "ym":
        if db == "oracle":
            m = re.search(
                r"YEAR(?:\s*\(\s*(\d+)\s*\))?\s+TO\s+MONTH(?:\s*\(\s*(\d+)\s*\))?",
                raw_u,
            )
            if m and (m.group(1) or m.group(2)):
                y = m.group(1) or "2"
                return f"INTERVAL YEAR({y}) TO MONTH"
            return "INTERVAL YEAR TO MONTH"
        ym = {
            "trino": "interval year to month",
            "presto": "interval year to month",
            "postgresql": "INTERVAL",
            "duckdb": "INTERVAL",
            "bigquery": "INTERVAL",
        }.get(db)
        if ym:
            return ym
        # Engines without YM native type — lossless text, never invent DAY TO SECOND.
        if db in {"snowflake", "mysql", "sqlserver", "redshift", "databricks", "iceberg", "clickhouse"}:
            return DDL_TYPES.get(db, {}).get(LOGICAL_INTERVAL) or "TEXT"
    if fam == "ds":
        if db == "oracle":
            m = re.search(
                r"DAY(?:\s*\(\s*(\d+)\s*\))?\s+TO\s+SECOND(?:\s*\(\s*(\d+)\s*\))?",
                raw_u,
            )
            if m and (m.group(1) or m.group(2)):
                d = m.group(1) or "2"
                s = m.group(2) or "6"
                return f"INTERVAL DAY({d}) TO SECOND({s})"
            return "INTERVAL DAY TO SECOND"
        ds = {
            "trino": "interval day to second",
            "presto": "interval day to second",
            "postgresql": "INTERVAL",
            "duckdb": "INTERVAL",
            "bigquery": "INTERVAL",
        }.get(db)
        if ds:
            return ds
    return None


def _geography_ddl_for_dest(db: str, inferred: str | None) -> str | None:
    """Preserve GEOMETRY vs GEOGRAPHY polarity (+ SRID typmod when PG-like).

    Bare logical ``geography`` (exact lowercase alias) falls through to
    ``DDL_TYPES`` defaults (PG→GEOMETRY). Explicit ``GEOGRAPHY`` /
    ``GEOMETRY`` / typmod carriers keep polarity — never treat uppercase
    ``GEOGRAPHY`` as the logical alias (SQL Server→PostGIS footgun).
    """
    raw = (inferred or "").strip()
    # Exact logical alias only — ``GEOGRAPHY`` / ``GEOMETRY`` are dual carriers.
    if raw == LOGICAL_GEOGRAPHY:
        return None
    pol = spatial_polarity(inferred)
    srid = parse_geography_srid(inferred)
    if db == "postgresql":
        if pol == "geography":
            kind = geometry_kind(inferred) or "Geometry"
            return f"GEOGRAPHY({kind},{srid})" if srid else "GEOGRAPHY"
        if pol == "geometry":
            kind = geometry_kind(inferred) or "Geometry"
            return f"GEOMETRY({kind},{srid})" if srid else "GEOMETRY"
        return None
    if db == "sqlserver":
        if pol == "geometry":
            return "GEOMETRY"
        if pol == "geography":
            return "GEOGRAPHY"
        return None
    if db == "mysql" and pol in {"geometry", "geography"}:
        return "GEOMETRY"
    if db == "oracle" and (
        pol is not None or "SDO_GEOMETRY" in raw.upper()
    ):
        return "SDO_GEOMETRY"
    if db in {"snowflake", "bigquery"} and pol == "geography":
        return "GEOGRAPHY"
    return None


# Destination DDL when source carrier is timezone-aware vs wall-clock NTZ.
_TZ_AWARE_DDL: Final[dict[str, str]] = {
    "postgresql": "TIMESTAMPTZ",
    "redshift": "TIMESTAMPTZ",
    "snowflake": "TIMESTAMP_TZ",
    # MySQL TIMESTAMP is session-TZ — not offset-preserving. Prefer DATETIME(6)
    # and document UTC-normalize at write rather than invent TIMESTAMPTZ fidelity.
    "mysql": "DATETIME(6)",
    "sqlserver": "DATETIMEOFFSET",
    "oracle": "TIMESTAMP WITH TIME ZONE",
    "bigquery": "TIMESTAMP",
    "spanner": "TIMESTAMP",
    "duckdb": "TIMESTAMPTZ",
    "timescaledb": "timestamptz",
    "databricks": "TIMESTAMP",
    "clickhouse": "DateTime64(6, 'UTC')",
    "trino": "timestamp(6) with time zone",
    "presto": "timestamp with time zone",
    "iceberg": "timestamptz",
}
_TZ_NAIVE_DDL: Final[dict[str, str]] = {
    "postgresql": "TIMESTAMP",
    "redshift": "TIMESTAMP",
    "snowflake": "TIMESTAMP_NTZ",
    "mysql": "DATETIME(6)",
    "sqlserver": "DATETIME2(7)",
    "oracle": "TIMESTAMP",
    "bigquery": "DATETIME",
    # Spanner has no DATETIME — wall-clock NTZ must not invent UTC TIMESTAMP.
    "spanner": "STRING(30)",
    "duckdb": "TIMESTAMP",
    "timescaledb": "timestamp",
    # Keep lakehouse NTZ spellings aligned with DDL_TYPES[LOGICAL_DATETIME].
    # Databricks TIMESTAMP is session-TZ aware; NTZ sources must stamp TIMESTAMP_NTZ.
    "databricks": "TIMESTAMP_NTZ",
    "clickhouse": "DateTime64(3)",
    "trino": "timestamp(3)",
    "presto": "timestamp",
    "iceberg": "timestamp",
}


def _bitstring_ddl_for_dest(db: str, inferred: str | None) -> str | None:
    """Emit native BIT/VARBIT DDL — never invent BYTEA from a bitstring carrier.

    PostgreSQL stores bit masks as bit strings (``B'1010'``), not opaque bytes.
    Mapping BIT(n)→BYTEA invents a byte packing the operator did not declare.
    """
    if not is_bitstring_carrier(inferred):
        return None
    width = parse_bitstring_width(inferred)
    varying = is_varying_bitstring_carrier(inferred)
    if db in {"postgresql", "redshift", "duckdb"}:
        if varying:
            return f"BIT VARYING({width})" if width else "BIT VARYING"
        return f"BIT({width})" if width else "BIT"
    if db in {"mysql", "mariadb"}:
        # MySQL BIT(m) max 64; varying not supported — clamp honestly.
        if width is None:
            return "BIT(64)"
        return f"BIT({min(width, 64)})"
    # Engines without bitstring types — lossless text of 0/1 digits (not BYTEA).
    if width is not None:
        return f"VARCHAR({width})"
    return DDL_TYPES.get(db, {}).get(LOGICAL_TEXT) or DDL_TYPES.get(db, {}).get(
        LOGICAL_STRING, "TEXT"
    )


# Native TZ-aware TIME types. Engines without one land plain TIME and the
# polarity loss is surfaced by ``time_timezone_polarity_loss`` in preflight
# (Snowflake/BigQuery have no TIMETZ).
_TIME_TZ_DDL: Final[dict[str, str]] = {
    "postgresql": "TIME WITH TIME ZONE",
    "postgres": "TIME WITH TIME ZONE",
    "cockroachdb": "TIME WITH TIME ZONE",
    "timescaledb": "TIME WITH TIME ZONE",
    "alloydb": "TIME WITH TIME ZONE",
    "yugabytedb": "TIME WITH TIME ZONE",
    "citus": "TIME WITH TIME ZONE",
    "supabase": "TIME WITH TIME ZONE",
    "greenplum": "TIME WITH TIME ZONE",
    "redshift": "TIME WITH TIME ZONE",
    "duckdb": "TIMETZ",
    "trino": "time with time zone",
    "presto": "time with time zone",
}

# Max fractional digits accepted on a TIME typmod. Narrower than the timestamp
# map: DuckDB rejects "TIME(6)" ("Type TIME does not support any modifiers!"),
# Redshift takes no parameter, and Oracle has no TIME type at all.
_TIME_FSP_CAPS: Final[dict[str, int]] = {
    "postgresql": 6,
    "postgres": 6,
    "cockroachdb": 6,
    "timescaledb": 6,
    "alloydb": 6,
    "yugabytedb": 6,
    "citus": 6,
    "supabase": 6,
    "greenplum": 6,
    "mysql": 6,
    "mariadb": 6,
    "tidb": 6,
    "sqlserver": 7,
    "snowflake": 9,
    "trino": 12,
    "presto": 12,
}


def time_timezone_polarity(inferred: str | None) -> str | None:
    """Return ``tz`` / ``ntz`` for TIME carriers, or None when ambiguous."""
    raw = (inferred or "").strip().upper().replace("_", " ")
    if not raw:
        return None
    # Collapse the typmod so ``TIME(6) WITH TIME ZONE`` — the SQL-standard and
    # information_schema spelling — is not read as ambiguous and silently
    # stripped of its offset.
    collapsed = re.sub(r"\s*\(\s*\d+\s*\)", "", raw).strip()
    if (
        collapsed.startswith("TIMETZ")
        or "TIME WITH TIME ZONE" in collapsed
        or collapsed == "TIME TZ"
    ):
        return "tz"
    if collapsed.startswith("TIME WITHOUT TIME ZONE") or (
        collapsed.startswith("TIME")
        and "ZONE" not in collapsed
        and not collapsed.startswith("TIMESTAMP")
    ):
        # Bare TIME is wall-clock NTZ on PG/MySQL.
        return "ntz"
    return None


def time_timezone_polarity_loss(source_type: str, target_type: str) -> bool:
    """True when TIME offset polarity would be dropped or invented.

    Covers TIMETZ→TIME (offset drop) and TIME→TIMETZ (offset invent on naive).
    """
    src = time_timezone_polarity(source_type)
    tgt = time_timezone_polarity(target_type)
    if src == "tz" and tgt == "ntz":
        return True
    if src == "ntz" and tgt == "tz":
        return True
    return False


def _time_ddl_for_dest(db: str, inferred: str | None) -> str | None:
    """Propagate TIME(p) / TIMETZ polarity into create-new DDL when known.

    MySQL default FSP is 0 when omitted — inventing bare TIME from TIME(6)
    silently rounds/truncates fractional seconds on write. Snowflake/Openflow
    has no TIMETZ — map to TIME and surface polarity loss in preflight.
    """
    fsp = parse_temporal_fractional_precision(inferred)
    pol = time_timezone_polarity(inferred)
    base = _TIME_TZ_DDL.get(db) if pol == "tz" else None
    if base is None:
        base = DDL_TYPES.get(db, {}).get(LOGICAL_TIME)
    if not base:
        return None
    if fsp is None:
        return base
    return _apply_temporal_fsp(db, base, fsp, caps=_TIME_FSP_CAPS)


# Snowflake LTZ = session-relative instant (≈ PG TIMESTAMPTZ). TZ = offset-pinned.
_TZ_LTZ_DDL: Final[dict[str, str]] = {
    "snowflake": "TIMESTAMP_LTZ",
    "oracle": "TIMESTAMP WITH LOCAL TIME ZONE",
    "postgresql": "TIMESTAMPTZ",
    "redshift": "TIMESTAMPTZ",
    "duckdb": "TIMESTAMPTZ",
    "timescaledb": "timestamptz",
    "sqlserver": "DATETIMEOFFSET",
    "mysql": "DATETIME(6)",
    "bigquery": "TIMESTAMP",
    "spanner": "TIMESTAMP",
    "databricks": "TIMESTAMP",
    "clickhouse": "DateTime64(6, 'UTC')",
    "trino": "timestamp(6) with time zone",
    "iceberg": "timestamptz",
}
_TZ_OFFSET_DDL: Final[dict[str, str]] = {
    "snowflake": "TIMESTAMP_TZ",
    "oracle": "TIMESTAMP WITH TIME ZONE",
    "postgresql": "TIMESTAMPTZ",
    "redshift": "TIMESTAMPTZ",
    "duckdb": "TIMESTAMPTZ",
    "timescaledb": "timestamptz",
    "sqlserver": "DATETIMEOFFSET",
    "mysql": "DATETIME(6)",
    "bigquery": "TIMESTAMP",
    "spanner": "TIMESTAMP",
    "databricks": "TIMESTAMP",
    "clickhouse": "DateTime64(6, 'UTC')",
    "trino": "timestamp(6) with time zone",
    "iceberg": "timestamptz",
}


# Max fractional-second digits each engine accepts as a TIMESTAMP/DATETIME
# typmod. Engines absent from this map take no precision argument at all, so
# appending one is a DDL syntax error rather than a narrower column:
# BigQuery DATETIME/TIMESTAMP, Databricks TIMESTAMP, and Redshift (always
# microseconds — "Amazon Redshift does not support precision parameters in the
# data type definition"). Caps are the documented per-engine maxima.
_TEMPORAL_FSP_CAPS: Final[dict[str, int]] = {
    "postgresql": 6,
    "postgres": 6,
    "cockroachdb": 6,
    "timescaledb": 6,
    "alloydb": 6,
    "yugabytedb": 6,
    "citus": 6,
    "supabase": 6,
    "greenplum": 6,
    "mysql": 6,
    "mariadb": 6,
    "tidb": 6,
    "sqlserver": 7,
    "oracle": 9,
    "snowflake": 9,
    "duckdb": 6,
    "clickhouse": 9,
    "trino": 12,
    "presto": 12,
}

# Engines that accept a typmod on the naive timestamp but reject it on the
# TZ-aware variant. Verified against DuckDB 1.3.2, which raises
# "Type TIMESTAMP WITH TIME ZONE does not support any modifiers!".
_NO_TZ_TYPMOD_ENGINES: Final[frozenset[str]] = frozenset({"duckdb"})

# Engines that reject TIMESTAMP/TIME typmod entirely (always microsecond
# or fixed internal precision). Promoting TIMESTAMP(6) here is illegal DDL.
_NO_TEMPORAL_TYPMOD_ENGINES: Final[frozenset[str]] = frozenset(
    {"redshift", "bigquery", "databricks", "iceberg", "spark", "delta"}
)


def _is_tz_aware_ddl(base: str) -> bool:
    """True when destination DDL already carries timezone awareness."""
    upper = base.upper()
    return (
        "WITH TIME ZONE" in upper
        or "_TZ" in upper
        or "_LTZ" in upper
        or upper.endswith("TZ")
    )


def _apply_temporal_fsp(
    db: str,
    base: str,
    fsp: int,
    *,
    caps: dict[str, int] | None = None,
) -> str:
    """Re-apply a declared fractional-second precision onto destination DDL.

    Single owner for temporal typmod so no caller invents its own cap. Engines
    that take no precision argument keep ``base`` verbatim; the rest are clamped
    to their documented maximum (MySQL 6, SQL Server 7, ClickHouse 9, Trino 12)
    so we never emit DDL the destination will reject.

    Only genuine temporal carriers are parameterised. Destinations that land
    time as text (Oracle ``VARCHAR2(32)``, ClickHouse ``String``) would
    otherwise read the *seconds* precision as a *character width* and truncate
    the value — ``TIME(6)`` → ``VARCHAR2(6)`` cannot hold ``12:34:56.123456``.
    """
    cap = (_TEMPORAL_FSP_CAPS if caps is None else caps).get(db)
    if cap is None:
        return base
    if not base.upper().lstrip().startswith(("TIME", "DATETIME", "SMALLDATETIME")):
        return base
    if db in _NO_TZ_TYPMOD_ENGINES and _is_tz_aware_ddl(base):
        return base
    capped = min(max(fsp, 0), cap)
    if db == "clickhouse":
        # ClickHouse puts precision first: DateTime64(p[, 'tz']). Plain DateTime
        # accepts only a timezone, so precision always implies DateTime64.
        tz = re.search(r",\s*(['\"][^'\"]+['\"])\s*\)\s*$", base)
        return f"DateTime64({capped}, {tz.group(1)})" if tz else f"DateTime64({capped})"
    bare = re.sub(r"\s*\(\s*\d+\s*\)\s*", " ", base).strip()
    # Suffix typmod attaches to the type name, ahead of any WITH TIME ZONE tail.
    m = re.match(r"^(.*?)(\s+WITH(?:OUT)?\s+(?:LOCAL\s+)?TIME\s+ZONE)$", bare, re.I)
    if m:
        return f"{m.group(1)}({capped}){m.group(2)}"
    return f"{bare}({capped})"


def _clickhouse_native_datetime_ddl(inferred: str | None) -> str | None:
    """Preserve ClickHouse-native spellings; None hands off to the shared mapper.

    ``DateTime64(p[, 'tz'])`` round-trips verbatim, as does ``DateTime('tz')``.
    Plain ``DateTime`` accepts *only* a timezone argument, so a numeric typmod
    (MySQL ``DATETIME(6)``) must fall through and become ``DateTime64(6)`` —
    ``DateTime(6)`` is a syntax error. Bare ``DATETIME`` is an ambiguous
    cross-dialect alias and also falls through: ``DateTime`` starts at
    1970-01-01, so pre-1970 values silently overflow, while ``DateTime64``
    reaches back to 1900.
    """
    raw = strip_identity_qualifier(inferred).strip()
    upper = raw.upper()
    idx = raw.find("(")
    if upper.startswith("DATETIME64"):
        # The ClickHouse grammar requires the precision argument.
        return ("DateTime64" + raw[idx:]) if idx >= 0 else "DateTime64(3)"
    if idx >= 0 and re.match(r"^DATETIME\s*\(\s*['\"]", upper):
        return "DateTime" + raw[idx:]
    return None


def _datetime_ddl_for_dest(db: str, inferred: str | None) -> str | None:
    """Return TZ-aware or NTZ DDL when source polarity is knowable; else None.

    When source declares fractional-second precision ``(p)``, propagate it into
    create-new DDL for engines that accept typmod (PG/MySQL/SQL Server/Oracle).
    Silent default-to-0 would truncate — Fivetran/Airbyte-class fidelity gap.

    A declared ``(p)`` is honoured even when polarity is *ambiguous* (bare
    ``TIMESTAMP(6)`` from Oracle/PG): polarity still defers to the destination
    platform default, but dropping the precision would silently narrow
    microseconds to the table default (ClickHouse/Trino default to millis).

    Snowflake ``TIMESTAMP_LTZ`` vs ``TIMESTAMP_TZ`` polarity is preserved
    (Openflow maps PG timestamptz→LTZ; Airbyte #80914).
    """
    # Bare TIMESTAMP / DATETIME are ambiguous — fall through to the destination
    # platform default, which is wall-clock NTZ (TIMESTAMP / TIMESTAMP_NTZ /
    # DATETIME). Explicit TIMESTAMPTZ / WITH TIME ZONE keep aware polarity.
    # Inventing TIMESTAMPTZ from bare datetime silently relocates civil times.
    polarity = datetime_timezone_polarity(inferred)
    fsp = parse_temporal_fractional_precision(inferred)
    base: str | None = None
    if polarity == "ltz":
        base = (
            _TZ_LTZ_DDL.get(db)
            or _TZ_AWARE_DDL.get(db)
            or DDL_TYPES.get(db, {}).get(LOGICAL_DATETIME)
        )
    elif polarity == "tz":
        base = (
            _TZ_OFFSET_DDL.get(db)
            or _TZ_AWARE_DDL.get(db)
            or DDL_TYPES.get(db, {}).get(LOGICAL_DATETIME)
        )
    elif polarity == "ntz":
        base = _TZ_NAIVE_DDL.get(db) or DDL_TYPES.get(db, {}).get(LOGICAL_DATETIME)
    elif fsp is not None:
        base = DDL_TYPES.get(db, {}).get(LOGICAL_DATETIME)
    else:
        return None
    if not base or fsp is None:
        return base
    return _apply_temporal_fsp(db, base, fsp)


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


def decimal_precision_would_truncate(source_type: str | None, dest_db_type: str | None) -> bool:
    """True when mapping source DECIMAL(p,s) onto dest would clamp precision.

    Mirror of ``decimal_scale_would_truncate`` — silent ``min(src_p, cap_p)`` is
    data loss for values that need the full digit width.
    """
    db = _normalize_dest_db(dest_db_type)
    if normalize_logical_type(source_type) != LOGICAL_DECIMAL:
        return False
    if db not in _DECIMAL_CAPS:
        return False
    precision, _scale = parse_numeric_precision_scale(source_type)
    if precision is None:
        return False
    return precision > _DECIMAL_CAPS[db][0]


def vector_encoding_polarity(inferred: str | None) -> str | None:
    """Return ``dense`` / ``half`` / ``sparse`` for vector carriers, else None."""
    if normalize_logical_type(inferred) != LOGICAL_VECTOR:
        return None
    upper = strip_identity_qualifier(inferred).upper().replace(" ", "")
    if upper.startswith("SPARSEVEC"):
        return "sparse"
    if upper.startswith("HALFVEC"):
        return "half"
    if upper.startswith("VECTOR") or "VECTOR(" in upper:
        return "dense"
    return "dense"



def vector_to_array_wire_preserved(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
) -> bool:
    """True for VECTOR(n) → ARRAY<FLOAT>/FLOAT[] create-new on lakehouse sinks.

    Databricks/Spark/Iceberg have no native VECTOR — ARRAY<FLOAT>/list<float>
    is the intentional embedding wire. PostgreSQL/Snowflake native VECTOR must
    not silent-green an ARRAY sink (embedding domain drop).
    """
    if normalize_logical_type(source_type) != LOGICAL_VECTOR:
        return False
    if normalize_logical_type(target_type) != LOGICAL_ARRAY:
        return False
    el = parse_array_element(target_type)
    if not el:
        # Bare ARRAY / LIST drops element contract.
        return False
    if normalize_logical_type(el) != LOGICAL_FLOAT:
        return False
    db = (dest_db or "").strip().lower()
    if not db:
        # Fail closed without destination — ARRAY wire is not universal.
        return False
    if db in {
        "postgresql",
        "postgres",
        "pg",
        "cockroachdb",
        "timescaledb",
        "alloydb",
        "snowflake",
        "redshift",
    }:
        return False
    return db in {
        "databricks",
        "spark",
        "delta",
        "delta_lake",
        "databricks_sql",
        "unity_catalog",
        "iceberg",
        "bigquery",
        "duckdb",
        "clickhouse",
    }


def vector_encoding_would_collapse(source_type: str, target_type: str) -> bool:
    """True when HALFVEC/SPARSEVEC ↔ dense VECTOR invents a different encoding."""
    src = vector_encoding_polarity(source_type)
    tgt = vector_encoding_polarity(target_type)
    if src is None or tgt is None:
        return False
    return src != tgt


def vector_dim_mismatch(source_type: str | None, target_type: str | None) -> bool:
    """True when VECTOR dims differ, or known↔unknown invents/drops a width.

    Used by G3 / DDL compatibility to fail closed on embedding-width drift
    (e.g. ``VECTOR(768)`` → ``VECTOR(FLOAT, 1536)``) and bare ``VECTOR`` →
    ``VECTOR(1536)`` invent.
    """
    if normalize_logical_type(source_type) != LOGICAL_VECTOR:
        return False
    if normalize_logical_type(target_type) != LOGICAL_VECTOR:
        return False
    src_dim = parse_vector_dimension(source_type)
    tgt_dim = parse_vector_dimension(target_type)
    if src_dim is None and tgt_dim is None:
        return False
    if src_dim is None or tgt_dim is None:
        return True
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
    return normalize_logical_type(inferred) in {
        LOGICAL_JSON,
        LOGICAL_ARRAY,
        LOGICAL_STRUCT,
        LOGICAL_MAP,
    }


def is_binary_type(inferred: str | None) -> bool:
    return normalize_logical_type(inferred) == LOGICAL_BINARY


# Declared conversions that must never soft-pass on head samples — body rows can
# still lose fidelity (IEEE→fixed, scale collapse, time-of-day truncation).
PRECISION_COLLAPSE_PAIRS: Final[frozenset[tuple[str, str]]] = frozenset({
    (LOGICAL_FLOAT, LOGICAL_DECIMAL),
    (LOGICAL_FLOAT, LOGICAL_INTEGER),
    (LOGICAL_DECIMAL, LOGICAL_INTEGER),
    # Fixed→IEEE loses magnitude/scale even when head samples look clean
    # (Airbyte sample-green / body-wrong class).
    (LOGICAL_DECIMAL, LOGICAL_FLOAT),
    (LOGICAL_DATETIME, LOGICAL_DATE),
})


def datetime_timezone_polarity(inferred: str | None, *, dest_db: str = "") -> str | None:
    """Return ``ltz`` / ``tz`` / ``ntz`` when DDL tokens make polarity knowable.

    - ``ltz``: session-relative instant (Snowflake TIMESTAMP_LTZ, Oracle LOCAL TZ,
      PG TIMESTAMPTZ — Openflow / Airbyte #80914)
    - ``tz``: offset-pinned (Snowflake TIMESTAMP_TZ, DATETIMEOFFSET)
    - ``ntz``: wall-clock naive (including bare DATETIME/TIMESTAMP)
    - ``None``: non-temporal or unrecognized carrier

    When ``dest_db`` is BigQuery/Databricks, bare ``TIMESTAMP`` is an instant
    carrier (not NTZ) — matches native DDL and clears false TZ-collapse on
    create-new TIMESTAMPTZ→TIMESTAMP wires.
    """
    dest_db = _normalize_dest_db(dest_db) if dest_db else ""
    # Arrow timestamp[unit, tz=…] → carrier before polarity token scan.
    arrow = arrow_dtype_to_carrier(inferred)
    if arrow is not None:
        inferred = arrow
    else:
        avro = avro_logical_token_to_carrier(inferred)
        if avro is not None:
            inferred = avro
    raw = (inferred or "").strip().upper().replace("_", " ")
    if not raw:
        return None
    # ClickHouse DateTime64(p, 'tz') / DateTime('tz') — column TZ metadata (LTZ).
    # Must run before the generic DATETIME( MySQL ntz token (quote after paren).
    if re.search(r"DATETIME64\s*\(\s*\d+\s*,", raw) or re.match(
        r"^DATETIME\s*\(\s*['\"]", raw
    ):
        return "ltz"
    if re.match(r"^DATETIME64\b", raw):
        return "ntz"
    # A numeric typmod must not break the SQL-standard token match:
    # ``TIMESTAMP(6) WITHOUT TIME ZONE`` is PostgreSQL's information_schema
    # spelling, and missing it would fall through and invent TZ polarity.
    # Keep ``raw`` for the dialect checks that need the parenthesis.
    collapsed = re.sub(r"\s*\(\s*\d+\s*\)", "", raw).strip()
    ntz_tokens = (
        "TIMESTAMP NTZ",
        "TIMESTAMP WITHOUT TIME ZONE",
        "DATETIME2",
        "SMALLDATETIME",
    )
    if (
        any(t in collapsed for t in ntz_tokens)
        or raw.startswith("DATETIME(")
        or collapsed == "TIMESTAMP NTZ"
    ):
        return "ntz"
    # LOCAL / LTZ / TIMESTAMPTZ before generic WITH TIME ZONE (substring overlap).
    if (
        "TIMESTAMP LTZ" in collapsed
        or "WITH LOCAL TIME ZONE" in collapsed
        or collapsed.startswith("TIMESTAMPTZ")
    ):
        return "ltz"
    if (
        "TIMESTAMP TZ" in collapsed
        or collapsed.startswith("DATETIMEOFFSET")
        or ("WITH TIME ZONE" in collapsed and "LOCAL" not in collapsed)
    ):
        return "tz"
    # Bare DATETIME = wall-clock NTZ. Bare TIMESTAMP defaults to NTZ unless the
    # destination engine's TIMESTAMP token is an instant (BQ / Databricks).
    if collapsed in {"DATETIME", "TIMESTAMP"} or collapsed.startswith("DATETIME "):
        if collapsed == "TIMESTAMP":
            db = (dest_db or "").strip().lower()
            if db in {
                "bigquery",
                "bq",
                "spanner",
                "google_spanner",
                "cloud_spanner",
                "databricks",
                "spark",
                "delta",
                "delta_lake",
                "databricks_sql",
                "unity_catalog",
            }:
                return "ltz"
        return "ntz"
    return None


# Engines whose single temporal carrier stores a full instant, so the logical
# "date" DDL token there still holds a time of day. BSON date is milliseconds
# since epoch; there is no date-only BSON type to narrow into.
_INSTANT_ONLY_TEMPORAL_ENGINES = frozenset({"mongodb", "cosmosdb", "documentdb", "firestore"})


def temporal_carrier_holds_time(db_type: str) -> bool:
    """True when the destination's date carrier also stores the time of day.

    Document stores map both LOGICAL_DATE and LOGICAL_DATETIME onto one BSON
    ``date``. Reading that token as a calendar day truncated every timestamp to
    midnight on write — silent loss that no gate could see, because the carrier
    was in fact wide enough to hold the value.
    """
    return (db_type or "").strip().lower() in _INSTANT_ONLY_TEMPORAL_ENGINES


def temporal_value_has_timezone(value: Any) -> bool:
    """True when a temporal cell carries an explicit UTC/offset (Z or ±HH:MM).

    Used by write quarantine so TIMESTAMPTZ wire is not silently stripped into
    TIMESTAMP_NTZ / DATETIME (Airbyte-class UTC invent).
    """
    if value is None:
        return False
    try:
        from datetime import datetime

        if isinstance(value, datetime) and value.tzinfo is not None:
            return True
    except Exception:
        pass
    text = str(value).strip()
    if not text:
        return False
    if text[-1:] in {"Z", "z"}:
        return True
    # ISO / SQL offsets: 2024-01-01T12:00:00+00:00 / …-0500 / …+00
    return bool(re.search(r"[+-]\d{2}:?\d{2}$", text))


def parse_vector_length(value: Any) -> int | None:
    """Return embedding length for list/tuple/JSON-array wire, else None."""
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return len(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        try:
            text = bytes(value).decode("utf-8")
        except Exception:
            return None
    else:
        text = str(value).strip()
    if not text:
        return None
    if text.startswith("[") and text.endswith("]"):
        try:
            import json

            parsed = json.loads(text)
            if isinstance(parsed, list):
                return len(parsed)
        except Exception:
            pass
    # pgvector textual: [1,2,3] already covered; space-separated rare.
    return None


def is_timezone_polarity_loss(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
) -> bool:
    """True when timezone polarity would be invented or dropped silently.

    Covers aware→NTZ, NTZ→aware (UTC invent on naive wall-clock), and LTZ↔TZ.
    ``dest_db`` makes **destination** TIMESTAMP polarity dialect-aware (BQ/Databricks
    instant). Source polarity never borrows dest_db — otherwise create-new
    ``TIMESTAMP→TIMESTAMP_NTZ`` / ``TIMESTAMP→DATETIME`` false-collapses when the
    sink engine's bare TIMESTAMP token is an instant.
    """
    dest_db = _normalize_dest_db(dest_db) if dest_db else ""
    src = datetime_timezone_polarity(source_type)
    tgt = datetime_timezone_polarity(target_type, dest_db=dest_db)
    if src in {"tz", "ltz"} and tgt == "ntz":
        return True
    # Naive / NTZ → TZ-aware invents an instant (UTC stamp) — fail-closed.
    if src == "ntz" and tgt in {"tz", "ltz"}:
        return True
    # Session-relative ↔ offset-pinned is a silent semantic rewrite.
    if {src, tgt} == {"tz", "ltz"}:
        return True
    return False


def timezone_aware_would_collapse_to_string(
    source_type: str, target_type: str
) -> bool:
    """True when offset-aware datetime/time collapses to open TEXT/STRING.

    ``TIMESTAMPTZ→TEXT`` / ``DATETIMEOFFSET→STRING`` / ``TIMETZ→STRING`` look
    like free serialization but drop the offset contract — Accept risk required.
    """
    dt = datetime_timezone_polarity(source_type)
    tm = time_timezone_polarity(source_type)
    if dt not in {"tz", "ltz"} and tm != "tz":
        return False
    tgt = normalize_logical_type(target_type)
    return tgt in {LOGICAL_STRING, LOGICAL_TEXT}


def is_long_raw_carrier(inferred: str | None) -> bool:
    """True for Oracle ``LONG RAW`` unbounded binary LOB (not ``RAW(n)``)."""
    upper = strip_identity_qualifier(inferred).upper()
    return upper == "LONG RAW" or upper.replace(" ", "") == "LONGRAW"


def long_raw_locator_would_collapse(source_type: str, target_type: str) -> bool:
    """True when LONG RAW locator polarity would be lost into BYTEA/BLOB/BINARY."""
    if not is_long_raw_carrier(source_type):
        return False
    if is_long_raw_carrier(target_type):
        return False
    return True


def bitstring_opaque_bytes_collapse(source_type: str, target_type: str) -> bool:
    """True when BIT(n)/VARBIT ↔ BYTEA/BINARY invents a packing the operator never declared."""
    src_bit = is_bitstring_carrier(source_type)
    tgt_bit = is_bitstring_carrier(target_type)
    if src_bit == tgt_bit:
        return False
    src_bin = normalize_logical_type(source_type) == LOGICAL_BINARY
    tgt_bin = normalize_logical_type(target_type) == LOGICAL_BINARY
    if not (src_bin and tgt_bin):
        return False
    # One side bitstring, the other opaque bytes.
    return True


def year_domain_would_collapse(source_type: str, target_type: str) -> bool:
    """True when MySQL YEAR polarity is dropped or invented.

    YEAR→SMALLINT invents a plain integer; INTEGER/STRING→YEAR invents the
    1901–2155 YEAR domain (out-of-range only fails at bind — Map must Accept risk).
    """
    src_year = is_year_carrier(source_type)
    tgt_year = is_year_carrier(target_type)
    if src_year == tgt_year:
        return False
    if src_year and not tgt_year:
        tgt = normalize_logical_type(target_type)
        return tgt in {
            LOGICAL_INTEGER,
            LOGICAL_DECIMAL,
            LOGICAL_FLOAT,
            LOGICAL_STRING,
            LOGICAL_TEXT,
            LOGICAL_JSON,
            LOGICAL_DATE,
            LOGICAL_DATETIME,
        }
    # Invent YEAR from open integer/string/float.
    src = normalize_logical_type(source_type)
    return src in {
        LOGICAL_INTEGER,
        LOGICAL_DECIMAL,
        LOGICAL_FLOAT,
        LOGICAL_STRING,
        LOGICAL_TEXT,
        LOGICAL_JSON,
        LOGICAL_DATE,
        LOGICAL_DATETIME,
    }


def money_domain_would_collapse(source_type: str, target_type: str) -> bool:
    """True when MONEY polarity or SMALLMONEY capacity would be lost/invented.

    ``DECIMAL(19,4)`` is a money-*scale* carrier but not locale MONEY — switching
    onto ``MONEY``/``SMALLMONEY`` still needs Accept risk. ``MONEY→SMALLMONEY``
    narrows range (~±214k) — fail closed.
    """
    src_base = strip_identity_qualifier(source_type).upper().replace(" ", "")
    tgt_base = strip_identity_qualifier(target_type).upper().replace(" ", "")
    named = {"MONEY", "SMALLMONEY", "CURRENCY"}
    src_named = src_base in named
    tgt_named = tgt_base in named
    if src_named and not tgt_named:
        # Create-new PG/warehouse money-scale wire — not a polarity invent.
        # MONEY → DECIMAL(19,4); SMALLMONEY → DECIMAL(10,4).
        p, s = parse_numeric_precision_scale(target_type)
        if p == 19 and s == 4 and src_base in {"MONEY", "CURRENCY"}:
            return False
        if p == 10 and s == 4 and src_base == "SMALLMONEY":
            return False
        # MONEY stamped as (10,4) would narrow — keep fail-closed.
        if p == 19 and s == 4 and src_base == "SMALLMONEY":
            return False
        return True
    if src_named != tgt_named:
        return True
    if src_base == "MONEY" and tgt_base == "SMALLMONEY":
        return True
    return False


def set_to_array_polarity_preserved(source_type: str, target_type: str) -> bool:
    """True for MySQL SET → TEXT[] / ARRAY (intentional multi-value sink)."""
    src = parse_enum_or_set_ordered_members(source_type)
    if src is None or src[0] != "SET":
        return False
    tgt_u = (target_type or "").strip().upper().replace(" ", "")
    if tgt_u in {"TEXT[]", "VARCHAR[]", "STRING[]"} or tgt_u.startswith("ARRAY<"):
        return True
    return normalize_logical_type(target_type) == LOGICAL_ARRAY


_STRING_WIDTH_RE = re.compile(
    r"(?:n?varchar2?|n?char|character\s+varying|character|string)"
    r"\s*\(\s*(\d+)\s*(?:BYTE|CHAR)?\s*\)",
    re.I,
)
_UNBOUNDED_STRING_RE = re.compile(
    r"(?:n?varchar2?|n?char|character\s+varying)\s*\(\s*max\s*\)",
    re.I,
)
_UNBOUNDED_TEXT_RE = re.compile(
    r"^(?:n?text|clob|nclob|longtext|mediumtext|tinytext|long\s+varchar|"
    r"string|bytes)\b(?!\s*\()",
    re.I,
)

# MySQL LOB tier rank — higher is wider. Bare TEXT/BLOB stay "unlimited" for
# DDL width propagation (logical ``text`` must not become VARCHAR(65535)).
_MYSQL_TEXT_TIER_RANK: Final[dict[str, int]] = {
    "TINYTEXT": 1,
    "TEXT": 2,
    "MEDIUMTEXT": 3,
    "LONGTEXT": 4,
}
_MYSQL_BLOB_TIER_RANK: Final[dict[str, int]] = {
    "TINYBLOB": 1,
    "BLOB": 2,
    "MEDIUMBLOB": 3,
    "LONGBLOB": 4,
}


def mysql_text_tier_rank(inferred: str | None) -> int | None:
    upper = strip_identity_qualifier(inferred).upper().split()[0] if inferred else ""
    return _MYSQL_TEXT_TIER_RANK.get(upper)


def mysql_blob_tier_rank(inferred: str | None) -> int | None:
    upper = strip_identity_qualifier(inferred).upper().split()[0] if inferred else ""
    return _MYSQL_BLOB_TIER_RANK.get(upper)


def parse_string_carrier_width(inferred: str | None) -> int | None:
    """Return bounded VARCHAR/CHAR width, or None if unlimited/unknown.

    Mirrors Airbyte MySQL CHAR truncation class — declared width must be proven,
    not inferred from short head samples. MySQL LOB tiers use
    :func:`mysql_text_tier_rank` (not a fake VARCHAR width).
    """
    text = (inferred or "").strip()
    if not text:
        return None
    upper = strip_identity_qualifier(text).upper().split()[0] if text else ""
    # PG ``name`` is a 63-byte identifier type.
    if upper == "NAME":
        return 63
    # TINYTEXT is the only MySQL LOB with a tight practical bound for width math.
    if upper == "TINYTEXT":
        return 255
    if _UNBOUNDED_STRING_RE.search(text) or _UNBOUNDED_TEXT_RE.match(text):
        return None
    m = _STRING_WIDTH_RE.search(text)
    if not m:
        return None
    width = int(m.group(1))
    # Redshift / warehouse LOB ceiling — treat as unbounded for width math.
    if width >= 65535:
        return None
    return width if width > 0 else None


def is_unlimited_string_carrier(inferred: str | None) -> bool:
    """True for TEXT/CLOB/VARCHAR(MAX)/MEDIUMTEXT/LONGTEXT — not TINYTEXT/NAME.

    Redshift has no TEXT type — ``VARCHAR(65535)`` is the practical LOB ceiling
    and must not false-narrow unlimited TEXT/CLOB create-new wires.
    """
    text = (inferred or "").strip()
    if not text:
        return False
    upper = strip_identity_qualifier(text).upper().split()[0] if text else ""
    if upper in {"TINYTEXT", "NAME"}:
        return False
    if _UNBOUNDED_STRING_RE.search(text) or _UNBOUNDED_TEXT_RE.match(text):
        return True
    # Redshift max VARCHAR / warehouse LOB ceiling — not a tight sink.
    m = _STRING_WIDTH_RE.search(text)
    if m and int(m.group(1)) >= 65535:
        return True
    return normalize_logical_type(text) == LOGICAL_TEXT


def is_national_string_carrier(inferred: str | None) -> bool:
    """True for NVARCHAR/NCHAR/NCLOB / NATIONAL CHARACTER (Unicode) carriers."""
    upper = strip_identity_qualifier(inferred).upper()
    if not upper:
        return False
    compact = upper.replace(" ", "")
    return (
        compact.startswith("NVARCHAR")
        or compact.startswith("NCHAR")
        or compact.startswith("NCLOB")
        or compact.startswith("NVARCHAR2")
        or compact.startswith("NTEXT")
        or bool(re.search(r"\bNATIONAL\s+CHARACTER\b", upper))
        or bool(re.search(r"\bNATIONAL\s+CHAR\b", upper))
    )


def national_charset_would_collapse(source_type: str, target_type: str) -> bool:
    """True when Unicode national string lands on non-national CHAR/VARCHAR/CLOB."""
    if not is_national_string_carrier(source_type):
        return False
    if is_national_string_carrier(target_type):
        return False
    tgt_l = normalize_logical_type(target_type)
    return tgt_l in {LOGICAL_STRING, LOGICAL_TEXT}


def national_charset_would_invent(source_type: str, target_type: str) -> bool:
    """True when non-national CHAR/VARCHAR invents national NCHAR/NVARCHAR polarity.

    Exception: SQL Server's only LOB text wire is ``NVARCHAR(MAX)`` — create-new
    TEXT/CLOB/STRING→NVARCHAR(MAX) is platform LOB twin, not Unicode invent.
    """
    if is_national_string_carrier(source_type):
        return False
    if not is_national_string_carrier(target_type):
        return False
    src_l = normalize_logical_type(source_type)
    if src_l not in {LOGICAL_STRING, LOGICAL_TEXT}:
        return False
    # SQL Server / Azure create-new LOB text wire.
    tgt_u = strip_identity_qualifier(target_type).upper().replace(" ", "")
    if tgt_u in {"NVARCHAR(MAX)", "NTEXT"} and (
        is_unlimited_string_carrier(source_type)
        or strip_identity_qualifier(source_type).upper() in {"STRING", "TEXT", "CLOB", "NCLOB"}
    ):
        return False
    return True


def bounded_string_sink_would_truncate(source_type: str, target_type: str) -> bool:
    """True when a scalar/document lands on tight CHAR/VARCHAR(n)/TINYTEXT.

    Safe-list ``integer→string`` must not greenwash ``INTEGER→VARCHAR(1)``.
    MySQL ``TEXT``/``MEDIUMTEXT``/``LONGTEXT`` remain practical scalar sinks
    (≥64KB); only typmod-bounded and TINYTEXT fail closed.
    """
    tgt_l = normalize_logical_type(target_type)
    if tgt_l not in {LOGICAL_STRING, LOGICAL_TEXT}:
        return False
    if is_unlimited_string_carrier(target_type):
        return False
    upper = strip_identity_qualifier(target_type).upper().split()[0] if target_type else ""
    tight = upper == "TINYTEXT" or bool(_STRING_WIDTH_RE.search(target_type or ""))
    if not tight:
        return False
    tgt_w = parse_string_carrier_width(target_type)
    if tgt_w is None:
        return False
    src_l = normalize_logical_type(source_type)
    if src_l in {LOGICAL_STRING, LOGICAL_TEXT}:
        return string_width_would_narrow(source_type, target_type)
    # Exact UUID 36-char wire is the industry create-new sink — not truncate.
    if normalize_logical_type(source_type) == LOGICAL_UUID and uuid_exact_wire_carrier(
        target_type
    ):
        return False
    # ObjectId CHAR/VARCHAR(24) / BINARY(12) wire — not truncate.
    if specialty_wire_preserves_value("OBJECTID", target_type) and (
        normalize_logical_type(source_type) == LOGICAL_OBJECTID
        or specialty_carrier_base(source_type) == "OBJECTID"
    ):
        return False
    # BIT(n) → VARCHAR(n) create-new is 0/1 digit text of known length — not truncate.
    if is_bitstring_carrier(source_type):
        bits = parse_bitstring_width(source_type)
        if bits is not None and tgt_w >= bits:
            return False
    # Non-string → tight sink — fail closed (Accept risk).
    return True


def string_width_would_narrow(source_type: str, target_type: str) -> bool:
    """True when source string capacity exceeds destination VARCHAR(n)/TEXT tier.

    Cases: ``VARCHAR(255)→VARCHAR(50)``, ``TEXT→VARCHAR(10)``,
    ``LONGTEXT→TINYTEXT``. Bare ``VARCHAR`` without a width stays unknown.
    """
    src_l = normalize_logical_type(source_type)
    tgt_l = normalize_logical_type(target_type)
    if src_l not in {LOGICAL_STRING, LOGICAL_TEXT}:
        return False
    if tgt_l not in {LOGICAL_STRING, LOGICAL_TEXT}:
        return False
    # MySQL LOB tier narrow (LONGTEXT→MEDIUMTEXT) before unlimited early-out.
    src_rank = mysql_text_tier_rank(source_type)
    tgt_rank = mysql_text_tier_rank(target_type)
    if src_rank is not None and tgt_rank is not None and src_rank > tgt_rank:
        return True
    # Unlimited / LOB-ceiling sinks (TEXT, NVARCHAR(MAX), VARCHAR(65535)) never narrow.
    if is_unlimited_string_carrier(target_type):
        return False
    tgt_w = parse_string_carrier_width(target_type)
    if tgt_w is None:
        return False
    src_w = parse_string_carrier_width(source_type)
    if src_w is None:
        # Unlimited generic TEXT/CLOB → bounded; bare VARCHAR unknown → no invent.
        return is_unlimited_string_carrier(source_type)
    return src_w > tgt_w


_BINARY_WIDTH_RE = re.compile(
    r"(?:varbinary|binary|varbyte|raw|bytes|fixedstring|fixed)\s*\(\s*(\d+)\s*\)",
    re.I,
)
_UNBOUNDED_BINARY_RE = re.compile(
    r"(?:varbinary|binary)\s*\(\s*max\s*\)|"
    r"\blong\s+raw\b|"
    r"^(?:bytea|blob|longblob|mediumblob|tinyblob|image|bytes|varbyte|binary)\b(?!\s*\()",
    re.I,
)


def parse_binary_carrier_width(inferred: str | None) -> int | None:
    """Return bounded BINARY/VARBINARY width, or None if unlimited/unknown.

    BIT/VARBIT widths are bit-counted — use ``parse_bitstring_width`` instead.
    MySQL BLOB tiers use :func:`mysql_blob_tier_rank` (not a fake VARBINARY width).
    """
    text = (inferred or "").strip()
    if not text:
        return None
    if is_bitstring_carrier(text):
        return None
    upper = strip_identity_qualifier(text).upper().split()[0] if text else ""
    if upper == "TINYBLOB":
        return 255
    if _UNBOUNDED_BINARY_RE.search(text):
        return None
    m = _BINARY_WIDTH_RE.search(text)
    if not m:
        return None
    width = int(m.group(1))
    return width if width > 0 else None


def is_unlimited_binary_carrier(inferred: str | None) -> bool:
    """True for BYTEA / BLOB / VARBINARY(MAX) — not TINYBLOB."""
    text = (inferred or "").strip()
    if not text:
        return False
    if is_bitstring_carrier(text):
        return False
    upper = strip_identity_qualifier(text).upper().split()[0] if text else ""
    if upper == "TINYBLOB":
        return False
    return bool(_UNBOUNDED_BINARY_RE.search(text))


def is_bitstring_carrier(inferred: str | None) -> bool:
    """True for PostgreSQL/MySQL BIT(n>1) / BIT VARYING / VARBIT bitstrings.

    Bare ``BIT`` / ``BIT(1)`` normalize to boolean and are excluded.
    """
    if normalize_logical_type(inferred) != LOGICAL_BINARY:
        return False
    text = strip_identity_qualifier(inferred).upper()
    if not text:
        return False
    return bool(re.search(r"\b(?:VARBIT|BIT\s+VARYING|BIT)\b", text))


def is_varying_bitstring_carrier(inferred: str | None) -> bool:
    """True for BIT VARYING / VARBIT (variable length up to n)."""
    text = strip_identity_qualifier(inferred).upper()
    return "VARYING" in text or "VARBIT" in text


def parse_bitstring_width(inferred: str | None) -> int | None:
    """Return BIT/VARBIT bit-width, or None if unbounded/unknown."""
    text = strip_identity_qualifier(inferred)
    if not text:
        return None
    m = re.search(
        r"(?:BIT\s+VARYING|VARBIT|BIT)\s*\(\s*(\d+)\s*\)",
        text,
        re.I,
    )
    if not m:
        return None
    width = int(m.group(1))
    return width if width > 0 else None


def bitstring_width_would_narrow(source_type: str, target_type: str) -> bool:
    """True when source bit capacity exceeds destination BIT(n)/VARBIT(n).

    PG ``BIT(n)`` requires exact length; ``BIT VARYING(n)`` rejects longer.
    BYTEA→BIT(n) is fail-closed (different unit / silent invent of bits).
    """
    if not is_bitstring_carrier(target_type):
        return False
    tgt_w = parse_bitstring_width(target_type)
    if tgt_w is None:
        return False
    if is_bitstring_carrier(source_type):
        src_w = parse_bitstring_width(source_type)
        if src_w is None:
            return True
        return src_w > tgt_w
    if normalize_logical_type(source_type) == LOGICAL_BINARY:
        bw = parse_binary_carrier_width(source_type)
        if bw is not None:
            return bw * 8 > tgt_w
        return is_unlimited_binary_carrier(source_type)
    return False


def bitstring_pad_polarity_loss(source_type: str, target_type: str) -> bool:
    """True when BIT VARYING ↔ fixed BIT(n) changes exact-length polarity."""
    if not is_bitstring_carrier(source_type) or not is_bitstring_carrier(target_type):
        return False
    return is_varying_bitstring_carrier(source_type) != is_varying_bitstring_carrier(
        target_type
    )


def oracle_char_byte_unit(inferred: str | None) -> str | None:
    """Return ``CHAR`` / ``BYTE`` length semantics for Oracle VARCHAR2/CHAR."""
    upper = strip_identity_qualifier(inferred).upper()
    if not upper:
        return None
    if re.search(r"\(\s*\d+\s*CHAR\s*\)", upper):
        return "CHAR"
    if re.search(r"\(\s*\d+\s*BYTE\s*\)", upper):
        return "BYTE"
    return None


def oracle_char_byte_polarity_loss(source_type: str, target_type: str) -> bool:
    """True when VARCHAR2(n CHAR) ↔ VARCHAR2(n BYTE) changes multibyte budget."""
    src_u = oracle_char_byte_unit(source_type)
    tgt_u = oracle_char_byte_unit(target_type)
    if not src_u or not tgt_u:
        return False
    return src_u != tgt_u


def is_oracle_long_text_carrier(inferred: str | None) -> bool:
    """True for Oracle deprecated LONG text LOB (exact token, not LONGTEXT/BIGINT)."""
    upper = strip_identity_qualifier(inferred).upper().replace(" ", "")
    return upper == "LONG"


def oracle_long_numeric_invent(source_type: str, target_type: str) -> bool:
    """True when Oracle LONG text would be stamped/mapped as NUMBER/integer.

    Exact ``LONG`` is Oracle's deprecated text LOB. Mapping to BIGINT/NUMBER
    invents numeric polarity — Accept risk. Lakehouse INT64 should use INT64 /
    BIGINT tokens, not bare LONG, when the source is not Oracle.
    """
    if not is_oracle_long_text_carrier(source_type):
        return False
    if is_oracle_long_text_carrier(target_type):
        return False
    tgt_u = strip_identity_qualifier(target_type).upper().replace(" ", "")
    if tgt_u.startswith(
        ("NUMBER", "DECIMAL", "NUMERIC", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE", "DOUBLE")
    ):
        return True
    tgt = normalize_logical_type(target_type)
    return tgt in {LOGICAL_INTEGER, LOGICAL_DECIMAL, LOGICAL_FLOAT}


def parse_interval_precision(
    inferred: str | None,
) -> tuple[int | None, int | None] | None:
    """Return (leading_precision, fractional_seconds) when declared on INTERVAL."""
    upper = strip_identity_qualifier(inferred).upper()
    if not upper or "INTERVAL" not in upper:
        return None
    leading: int | None = None
    frac: int | None = None
    m_lead = re.search(
        r"\b(?:YEAR|DAY|HOUR|MONTH|MINUTE|SECOND)\s*\(\s*(\d+)\s*\)",
        upper,
    )
    if m_lead:
        leading = int(m_lead.group(1))
    m_frac = re.search(r"SECOND\s*\(\s*(\d+)\s*\)", upper)
    if m_frac:
        # DAY(d) TO SECOND(s) — second capture is fractional when TO SECOND(s).
        if re.search(r"TO\s+SECOND\s*\(", upper):
            frac = int(m_frac.group(1))
            # Prefer leading from DAY/YEAR when present.
            m_day = re.search(r"\bDAY\s*\(\s*(\d+)\s*\)", upper)
            m_year = re.search(r"\bYEAR\s*\(\s*(\d+)\s*\)", upper)
            if m_day:
                leading = int(m_day.group(1))
            elif m_year:
                leading = int(m_year.group(1))
        elif leading is None:
            leading = int(m_frac.group(1))
    if leading is None and frac is None:
        return None
    return leading, frac


def interval_precision_would_narrow(source_type: str, target_type: str) -> bool:
    """True when INTERVAL leading/fractional precision shrinks or invents typmod."""
    if normalize_logical_type(source_type) != LOGICAL_INTERVAL:
        return False
    if normalize_logical_type(target_type) != LOGICAL_INTERVAL:
        return False
    sp = parse_interval_precision(source_type)
    tp = parse_interval_precision(target_type)
    if sp is None and tp is None:
        return False
    # Bare ↔ proven typmod invents/drops precision contract.
    if (sp is None) != (tp is None):
        return True
    assert sp is not None and tp is not None
    sl, sf = sp
    tl, tf = tp
    if sl is not None and tl is not None and tl < sl:
        return True
    if sf is not None and tf is not None and tf < sf:
        return True
    if sf is not None and tf is None:
        return True
    if sl is not None and tl is None and sf is None:
        return True
    return False


def binary_width_would_narrow(source_type: str, target_type: str) -> bool:
    """True when source binary capacity exceeds destination BINARY(n)/VARBINARY(n).

    Mirrors VARCHAR narrowing — ``VARBINARY(64)→VARBINARY(16)`` is silent truncate
    unless G3 blocks and write quarantine holds out oversized payloads.
    Bitstring destinations use ``bitstring_width_would_narrow`` instead.
    """
    if normalize_logical_type(source_type) != LOGICAL_BINARY:
        return False
    if normalize_logical_type(target_type) != LOGICAL_BINARY:
        return False
    if is_bitstring_carrier(target_type):
        return bitstring_width_would_narrow(source_type, target_type)
    src_rank = mysql_blob_tier_rank(source_type)
    tgt_rank = mysql_blob_tier_rank(target_type)
    if src_rank is not None and tgt_rank is not None and src_rank > tgt_rank:
        return True
    tgt_w = parse_binary_carrier_width(target_type)
    if tgt_w is None:
        return False
    if is_bitstring_carrier(source_type):
        # BIT(n) into BYTEA/VARBINARY(k): need ceil(n/8) bytes.
        src_bits = parse_bitstring_width(source_type)
        if src_bits is None:
            return True
        return (src_bits + 7) // 8 > tgt_w
    src_w = parse_binary_carrier_width(source_type)
    if src_w is None:
        # BYTEA/BLOB/VARBINARY(MAX) into bounded BINARY(n) — fail closed.
        return is_unlimited_binary_carrier(source_type)
    return src_w > tgt_w


def is_fixed_width_char_carrier(inferred: str | None) -> bool:
    """True for CHAR/NCHAR/BPCHAR (blank-padded), not VARCHAR/NVARCHAR."""
    upper = (inferred or "").upper()
    if not upper:
        return False
    if "VARCHAR" in upper or "VARYING" in upper:
        return False
    return bool(
        re.search(r"\b(?:N?CHAR|BPCHAR|CHARACTER)\b", upper)
        or re.match(r"^N?CHAR\s*\(", upper)
    )


def fixed_width_pad_polarity_loss(
    source_type: str, target_type: str, *, dest_db: str = ""
) -> bool:
    """True when CHAR↔VARCHAR or BINARY↔VARBINARY changes pad/trim equality polarity."""
    src_l = normalize_logical_type(source_type)
    tgt_l = normalize_logical_type(target_type)
    if src_l in {LOGICAL_STRING, LOGICAL_TEXT} and tgt_l in {LOGICAL_STRING, LOGICAL_TEXT}:
        src_fixed = is_fixed_width_char_carrier(source_type)
        tgt_fixed = is_fixed_width_char_carrier(target_type)
        # Only when at least one side is clearly fixed-width (CHAR/BPCHAR).
        if src_fixed or tgt_fixed:
            return src_fixed != tgt_fixed
        return False
    if src_l == LOGICAL_BINARY and tgt_l == LOGICAL_BINARY:
        src_fixed = is_fixed_width_binary_carrier(source_type, dest_db=dest_db)
        tgt_fixed = is_fixed_width_binary_carrier(target_type, dest_db=dest_db)
        if src_fixed or tgt_fixed:
            # Unbounded binary LOB sinks (BYTEA/BLOB) store exact bytes — not
            # CHAR-style pad invent on create-new into engines without FIXED BINARY.
            if is_unlimited_binary_carrier(target_type) or is_unlimited_binary_carrier(
                source_type
            ):
                return False
            # BQ BYTES(n) / Snowflake BINARY(n) / Redshift VARBYTE(n) are max-length
            # varying carriers — equal-or-wider capacity is exact-byte wire, not pad invent.
            db = (dest_db or "").strip().lower()
            if db in {"bigquery", "bq", "snowflake", "sf", "redshift"}:
                sw = parse_binary_carrier_width(source_type)
                tw = parse_binary_carrier_width(target_type)
                if sw is not None and tw is not None and tw >= sw:
                    return False
            return src_fixed != tgt_fixed
        return False
    return False


def parse_enum_or_set_members(inferred: str | None) -> tuple[str, frozenset[str]] | None:
    """Return ('ENUM'|'SET', members) when carrier declares a closed domain."""
    ordered = parse_enum_or_set_ordered_members(inferred)
    if ordered is None:
        return None
    kind, members = ordered
    return kind, frozenset(members)


def parse_enum_or_set_ordered_members(
    inferred: str | None,
) -> tuple[str, tuple[str, ...]] | None:
    """Return ('ENUM'|'SET', members-in-definition-order) for ordinal/bitmask bind.

    MySQL ENUM indexes are 1-based in declaration order; SET uses bit 0 = first
    member (MySQL 8.4 / Debezium integer wire). Order must be preserved.
    """
    text = (inferred or "").strip()
    m = re.match(r"^(ENUM|SET)\s*\((.*)\)\s*$", text, re.I | re.DOTALL)
    if not m:
        return None
    kind = m.group(1).upper()
    body = m.group(2).strip()
    if not body:
        return kind, ()
    members: list[str] = []
    for part in re.findall(r"'((?:\\'|[^'])*)'|\"((?:\\\"|[^\"])*)\"", body):
        token = part[0] if part[0] else part[1]
        members.append(token.replace("\\'", "'").replace('\\"', '"'))
    return kind, tuple(members)


def format_enum_domain_carrier(members: list[str] | tuple[str, ...]) -> str:
    """Build ``ENUM('a','b')`` carrier from ordered labels (MySQL / PG pg_enum)."""
    parts: list[str] = []
    for lab in members:
        text = str(lab)
        text = text.replace("\\", "\\\\").replace("'", "\\'")
        parts.append(f"'{text}'")
    return "ENUM(" + ",".join(parts) + ")"


def format_set_domain_carrier(members: list[str] | tuple[str, ...]) -> str:
    """Build ``SET('a','b')`` carrier from ordered labels."""
    parts: list[str] = []
    for lab in members:
        text = str(lab)
        text = text.replace("\\", "\\\\").replace("'", "\\'")
        parts.append(f"'{text}'")
    return "SET(" + ",".join(parts) + ")"


def pg_enum_type_name(members: list[str] | tuple[str, ...]) -> str:
    """Stable PostgreSQL type name for an ENUM domain (CREATE TYPE … AS ENUM)."""
    import hashlib

    key = "\0".join(str(m) for m in members)
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
    return f"df_enum_{digest}"


def pg_enum_create_type_sql(type_name: str, members: list[str] | tuple[str, ...]) -> str:
    """Idempotent ``CREATE TYPE … AS ENUM`` (duplicate_object → no-op)."""
    name = re.sub(r"[^a-zA-Z0-9_]", "_", (type_name or "").strip())
    if not name or not re.match(r"^[A-Za-z_]", name):
        raise ValueError(f"invalid PG enum type name: {type_name!r}")
    labels = format_enum_domain_carrier(members)[len("ENUM") :]  # ('a','b')
    return (
        "DO $df_enum$ BEGIN "
        f"CREATE TYPE {name} AS ENUM {labels}; "
        "EXCEPTION WHEN duplicate_object THEN NULL; "
        "END $df_enum$;"
    )


def collect_pg_enum_prerequisites(inferred_types: list[str] | tuple[str, ...]) -> list[str]:
    """Return CREATE TYPE statements for ENUM carriers mapped to PostgreSQL."""
    stmts: list[str] = []
    seen: set[str] = set()
    for inferred in inferred_types:
        parsed = parse_enum_or_set_ordered_members(inferred)
        if parsed is None or parsed[0] != "ENUM" or not parsed[1]:
            continue
        name = pg_enum_type_name(parsed[1])
        if name in seen:
            continue
        seen.add(name)
        stmts.append(pg_enum_create_type_sql(name, parsed[1]))
    return stmts


def _enum_set_ddl_for_dest(db: str, inferred: str | None) -> str | None:
    """Map ENUM/SET carriers to destination-native create-new DDL.

    MySQL keeps inline ENUM/SET. PostgreSQL uses ``CREATE TYPE`` + type name
    (prerequisites via ``collect_pg_enum_prerequisites``). Other engines get a
    bounded VARCHAR — domain still enforced at write quarantine (fail-closed).
    """
    parsed = parse_enum_or_set_ordered_members(inferred)
    if parsed is None:
        return None
    kind, members = parsed
    if not members:
        if db in {"mysql", "mariadb", "tidb"}:
            return (
                format_enum_domain_carrier(())
                if kind == "ENUM"
                else format_set_domain_carrier(())
            )
        return "VARCHAR(1)"
    if db in {"mysql", "mariadb", "tidb"}:
        return (
            format_enum_domain_carrier(members)
            if kind == "ENUM"
            else format_set_domain_carrier(members)
        )
    if kind == "ENUM" and db in {
        "postgresql",
        "postgres",
        "cockroachdb",
        "timescaledb",
        "alloydb",
        "yugabytedb",
        "citus",
        "supabase",
        "greenplum",
    }:
        return pg_enum_type_name(members)
    # MySQL SET → PostgreSQL TEXT[] (multi-value polarity). Airbyte/Fivetran
    # often collapse SET to string/JSON; we keep an array sink + write quarantine.
    if kind == "SET" and db in {
        "postgresql",
        "postgres",
        "cockroachdb",
        "timescaledb",
        "alloydb",
        "yugabytedb",
        "citus",
        "supabase",
        "greenplum",
    }:
        return "TEXT[]"
    width = max(len(m) for m in members)
    if kind == "SET":
        width = sum(len(m) for m in members) + max(0, len(members) - 1)
    width = max(1, min(width, 4000))
    if db == "sqlserver":
        return f"NVARCHAR({min(width, 4000)})"
    if db == "oracle":
        return f"VARCHAR2({min(width, 4000)})"
    return f"VARCHAR({width})"


def enum_domain_would_collapse(source_type: str, target_type: str) -> bool:
    """True when a closed ENUM/SET domain would become an open string sink.

    PostgreSQL ``df_enum_*`` type names preserve the domain via CREATE TYPE.
    MySQL ``SET`` → ``TEXT[]`` preserves multi-value polarity (not a string sink).
    """
    src = parse_enum_or_set_ordered_members(source_type)
    if src is None:
        return False
    if parse_enum_or_set_ordered_members(target_type) is not None:
        return False
    tgt = (target_type or "").strip()
    if re.match(r"^df_enum_[0-9a-f]{8,}$", tgt, re.I):
        return False
    tgt_u = tgt.upper().replace(" ", "")
    if src[0] == "SET" and (
        tgt_u in {"TEXT[]", "VARCHAR[]", "STRING[]"}
        or tgt_u.startswith("ARRAY<")
        or normalize_logical_type(target_type) == LOGICAL_ARRAY
    ):
        return False
    tgt_l = normalize_logical_type(target_type)
    return tgt_l in {LOGICAL_STRING, LOGICAL_TEXT, LOGICAL_JSON}


def enum_set_domain_would_reject(source_type: str, target_type: str) -> bool:
    """True when destination ENUM/SET cannot hold every source ENUM/SET member.

    MySQL non-strict ENUM stores invalid values as '' — silent wipe. Fail closed
    when dest domain is a proper subset of source domain.
    """
    src = parse_enum_or_set_members(source_type)
    tgt = parse_enum_or_set_members(target_type)
    if tgt is None:
        return False
    if src is None:
        # Open/unknown source → closed ENUM/SET: unfit values only appear at
        # write (quarantine). Fail-closed so Map/G3 require Accept risk.
        return True
    src_kind, src_members = src
    tgt_kind, tgt_members = tgt
    # SET↔ENUM rewrites multi-value vs single-label polarity — never preserve.
    if src_kind != tgt_kind:
        return True
    if not tgt_members:
        return True
    return not src_members.issubset(tgt_members)


def interval_family(inferred: str | None) -> str | None:
    """Return ``ym`` / ``ds`` interval family, or None when unqualified/unknown.

    ANSI / Oracle / Snowflake: YEAR-MONTH and DAY-SECOND are distinct families
    that must not silently cast into each other (Snowflake AIM, Google Oracle→PG).
    """
    upper = (inferred or "").upper()
    if not upper:
        return None
    # Explicit YEAR-MONTH family (including YEAR / MONTH alone).
    if re.search(
        r"YEAR\s+TO\s+MONTH|\bINTERVAL\s+YEAR\b|\bINTERVAL\s+MONTH\b|YEAR_MONTH",
        upper,
    ):
        return "ym"
    # DAY-SECOND family (day/hour/minute/second qualifiers).
    if re.search(
        r"DAY\s+TO\s+SECOND|\bINTERVAL\s+DAY\b|HOUR\s+TO|MINUTE\s+TO|"
        r"\bINTERVAL\s+HOUR\b|\bINTERVAL\s+MINUTE\b|\bINTERVAL\s+SECOND\b",
        upper,
    ):
        return "ds"
    if normalize_logical_type(inferred) == LOGICAL_INTERVAL:
        return None
    return None


def interval_family_would_collapse(
    source_type: str, target_type: str, *, dest_db: str = ""
) -> bool:
    """True when YEAR-MONTH ↔ DAY-SECOND polarity would be lost or invented.

    Bare ``INTERVAL`` ↔ explicit YM/DS invents a family the operator never
    declared — fail closed (Snowflake AIM / Oracle INTERVAL class).

    Engines whose single native INTERVAL type holds both families
    (PostgreSQL, DuckDB, BigQuery) treat YM/DS → bare INTERVAL as create-new
    wire, not invent/drop — when ``dest_db`` names that engine.
    """
    if normalize_logical_type(source_type) != LOGICAL_INTERVAL:
        return False
    if normalize_logical_type(target_type) != LOGICAL_INTERVAL:
        # Interval → text/json is serialization (lossy via nested/doc rules elsewhere).
        return False
    src_f = interval_family(source_type)
    tgt_f = interval_family(target_type)
    if src_f and tgt_f and src_f != tgt_f:
        return True
    # Unqualified ↔ qualified family invent/drop.
    if (src_f is None) != (tgt_f is None):
        db = _normalize_dest_db(dest_db) if dest_db else ""
        # Unified INTERVAL engines — qualified source → bare INTERVAL is native.
        if (
            db in {"postgresql", "duckdb", "bigquery"}
            and src_f is not None
            and tgt_f is None
        ):
            return False
        return True
    return False

def spatial_polarity(inferred: str | None) -> str | None:
    """Return ``geography`` / ``geometry`` / ``sdo``, or None.

    Oracle ``SDO_GEOMETRY`` / Esri ``ST_GEOMETRY`` are opaque spatial carriers —
    mapping them onto dual GEOGRAPHY/GEOMETRY invents planar vs geodetic polarity.
    """
    upper = (inferred or "").upper().strip()
    if not upper:
        return None
    if "SDO_GEOMETRY" in upper or "ST_GEOMETRY" in upper:
        return "sdo"
    # Prefer typmod / exact dual-type carriers over the bare logical name
    # ``geography`` (which is polarity-unknown for create-new DDL defaults).
    if upper.startswith("GEOGRAPHY(") or upper == "GEOGRAPHY":
        return "geography"
    if upper.startswith("GEOMETRY(") or upper == "GEOMETRY":
        return "geometry"
    if re.search(r"\bGEOGRAPHY\b", upper) and "(" in upper:
        return "geography"
    if re.search(r"\bGEOMETRY\b", upper):
        return "geometry"
    return None


def parse_geography_srid(inferred: str | None) -> int | None:
    """Extract SRID from typmod / EWKT-style carriers (e.g. geography(Point,4326))."""
    text = (inferred or "").strip()
    if not text:
        return None
    m = re.search(r",\s*(\d+)\s*\)\s*$", text)
    if m:
        return int(m.group(1))
    m = re.search(r"\bSRID\s*=\s*(\d+)\b", text, re.I)
    if m:
        return int(m.group(1))
    return None


_GEOMETRY_KIND_TOKENS: Final[tuple[str, ...]] = (
    "GEOMETRYCOLLECTION",
    "MULTIPOLYGON",
    "MULTILINESTRING",
    "MULTIPOINT",
    "LINESTRING",
    "POLYGON",
    "POINT",
    "RING",  # ClickHouse Ring
    "CIRCLE",
    "BOX",
    "PATH",
    "LINE",
    "LSEG",
)


def geometry_kind(inferred: str | None) -> str | None:
    """Return POINT/LINESTRING/POLYGON/… when declared, else None for bare GEOMETRY."""
    upper = strip_identity_qualifier(inferred).upper().replace(" ", "")
    if not upper:
        return None
    # GEOMETRY(POINT,4326) / GEOGRAPHY(POLYGON,4326)
    m = re.match(r"^(?:GEOMETRY|GEOGRAPHY)\((\w+)", upper)
    if m:
        return m.group(1)
    for kind in _GEOMETRY_KIND_TOKENS:
        if upper == kind or upper.startswith(kind + "("):
            return kind
    return None


def geography_contract_would_collapse(source_type: str, target_type: str) -> bool:
    """True when GEOMETRY↔GEOGRAPHY polarity, SRID, or subtype would be lost.

    Planar→geodetic, SRID rewrite, and Polygon→Point are silent fidelity losses
    unless the operator chooses an explicit store-as-WKT / reproject policy.

    Oracle ``SDO_GEOMETRY`` is the sole spatial carrier — bare GEOMETRY/GEOGRAPHY
    → SDO is create-new native wire, not planar↔geodetic invent inside Oracle.
    """
    if normalize_logical_type(source_type) != LOGICAL_GEOGRAPHY:
        return False
    if normalize_logical_type(target_type) != LOGICAL_GEOGRAPHY:
        return False
    sp = spatial_polarity(source_type)
    tp = spatial_polarity(target_type)
    # Oracle opaque SDO ↔ bare planar GEOMETRY (no subtype/SRID) — create-new wire.
    # GEOGRAPHY→SDO still drops geodetic polarity — keep fail-closed.
    if tp == "sdo" and sp in {"geometry", "sdo"}:
        ss = parse_geography_srid(source_type)
        ts = parse_geography_srid(target_type)
        if ss is not None and ts is not None and ss != ts:
            return True
        if ss is not None and ts is None:
            return True
        sk = geometry_kind(source_type)
        tk = geometry_kind(target_type)
        if sk and tk and sk != tk:
            return True
        if bool(sk) != bool(tk):
            return True
        return False
    if sp and tp and sp != tp:
        return True
    ss = parse_geography_srid(source_type)
    ts = parse_geography_srid(target_type)
    if ss is not None and ts is not None and ss != ts:
        return True
    # Declared SRID → bare geometry drops the spatial contract.
    if ss is not None and ts is None:
        return True
    sk = geometry_kind(source_type)
    tk = geometry_kind(target_type)
    if sk and tk and sk != tk:
        return True
    # Bare ↔ typed subtype invents or drops a geometry kind contract.
    if bool(sk) != bool(tk):
        return True
    return False


# Elasticsearch IP / PG INET / IPv4 / IPv6 — same host-address polarity.
# CIDR is network-mask polarity and is NOT in this twin set.
_IP_HOST_ADDRESS_TWINS: Final[frozenset[str]] = frozenset(
    {"IP", "INET", "IPV4", "IPV6"}
)

# Oracle XMLTYPE ↔ ANSI/SQL Server XML — same document-XML polarity.
_XML_DOCUMENT_TWINS: Final[frozenset[str]] = frozenset({"XML", "XMLTYPE"})


_SPECIALTY_NATIVE_CARRIERS: Final[frozenset[str]] = frozenset(
    {
        "INET",
        "CIDR",
        "MACADDR",
        "MACADDR8",
        "REGCLASS",
        "NAME",
        "POINT",
        "LINE",
        "LSEG",
        "BOX",
        "PATH",
        "POLYGON",
        "CIRCLE",
        "RING",  # ClickHouse / geo ring
        "LINESTRING",
        "MULTILINESTRING",
        "MULTIPOINT",
        "MULTIPOLYGON",
        "GEOMETRYCOLLECTION",
        "PG_LSN",
        "OID",
        "TID",
        "CTID",
        "XID",
        "XID8",
        "CID",
        "HSTORE",
        "LTREE",
        "TSVECTOR",
        "TSQUERY",
        "TXID_SNAPSHOT",
        "PG_SNAPSHOT",
        "CITEXT",
        "XML",
        "XMLTYPE",
        "JSONB",
        "ROWVERSION",
        "HIERARCHYID",
        "SQL_VARIANT",
        "ROWID",
        "UROWID",
        "IPV4",
        "IPV6",
        "IP",  # Elasticsearch ip field → INET
        "VOID",
        "JSONPATH",
        "OBJECTID",
        "ANYDATA",  # Oracle polymorphic envelope
        "HLLSKETCH",  # Redshift HyperLogLog sketch
        "USER-DEFINED",  # PG opaque UDT — never silent-green → TEXT
        "USER_DEFINED",
        "ENUM8",  # ClickHouse closed enum
        "ENUM16",
        "NOTHING",  # ClickHouse nothing type
        "DYNAMIC",  # ClickHouse dynamic type
        "AGGREGATEFUNCTION",  # ClickHouse aggregate state
        "SIMPLEAGGREGATEFUNCTION",
    }
)


def is_sql_variant_carrier(inferred: str | None) -> bool:
    return strip_identity_qualifier(inferred).upper().strip() == "SQL_VARIANT"


def sql_variant_would_collapse(source_type: str, target_type: str) -> bool:
    """True when SQL_VARIANT loses its typed-union polarity into open text.

    JSONB/VARIANT sinks keep a typed envelope; VARCHAR/TEXT invent document
    polarity without base-type tags (AWS SCT VARCHAR(8000) honesty gap).
    """
    if not is_sql_variant_carrier(source_type):
        return False
    if is_sql_variant_carrier(target_type):
        return False
    tgt_u = strip_identity_qualifier(target_type).upper().strip()
    if tgt_u in {"JSONB", "JSON", "VARIANT", "OBJECT"}:
        return False
    tgt = normalize_logical_type(target_type)
    return tgt in {LOGICAL_STRING, LOGICAL_TEXT}


def is_hierarchyid_carrier(inferred: str | None) -> bool:
    """True for SQL Server ``hierarchyid`` path carrier."""
    return strip_identity_qualifier(inferred).upper().strip() == "HIERARCHYID"


def hierarchyid_to_ltree_path(path: str) -> str:
    """Convert hierarchyid ``/1/2/3/`` polarity to PostgreSQL ``ltree`` ``1.2.3``.

    AWS DMS leaves slash strings in VARCHAR; Datawrap create-new uses LTREE when
    the destination is PostgreSQL-family (Microsoft / AWS migration guidance).
    """
    text = (path or "").strip()
    if not text or text == "/":
        return ""
    if text.startswith("/") or text.endswith("/"):
        parts = [p for p in text.strip("/").split("/") if p]
        if not parts:
            return ""
        for lab in parts:
            if not re.fullmatch(r"[A-Za-z0-9_]+", lab):
                raise ValueError(
                    f"hierarchyid label {lab!r} invalid for LTREE — refuse invent"
                )
        return ".".join(parts)
    # Already ltree-shaped (dot labels).
    if re.fullmatch(r"[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*", text):
        return text
    raise ValueError(
        f"hierarchyid path is not /n/n/ or ltree — refuse invent: {text[:64]!r}"
    )


def is_rowversion_carrier(inferred: str | None) -> bool:
    """True for SQL Server ``ROWVERSION`` (TIMESTAMP synonym) concurrency token."""
    return strip_identity_qualifier(inferred).upper().strip() == "ROWVERSION"


def rowversion_would_collapse_to_temporal(
    source_type: str, target_type: str
) -> bool:
    """True when ROWVERSION would be misread as a clock datetime type.

    SQL Server ``TIMESTAMP`` ≠ ANSI timestamp. Mapping to TIMESTAMP/DATETIME
    invents wall-clock polarity (Estuary / HVR BYTEA mapping — fail closed).
    """
    if not is_rowversion_carrier(source_type):
        return False
    if is_rowversion_carrier(target_type):
        return False
    tgt_u = strip_identity_qualifier(target_type).upper().strip()
    if tgt_u in {"BYTEA", "BINARY", "VARBINARY", "BLOB", "RAW", "IMAGE"}:
        return False
    if tgt_u.startswith(("BINARY(", "VARBINARY(", "RAW(", "BYTES(")):
        return False
    tgt = normalize_logical_type(target_type)
    # String sinks are covered by specialty_carrier_would_collapse(ROWVERSION).
    return tgt in {LOGICAL_DATETIME, LOGICAL_DATE, LOGICAL_TIME}


def specialty_carrier_base(inferred: str | None) -> str | None:
    """Return uppercase specialty base (INET, PG_LSN, …) or None."""
    upper = re.sub(r"\s+", " ", (inferred or "").upper().strip())
    if not upper:
        return None
    # ClickHouse Enum8/Enum16 before generic ENUM( domain parse.
    if upper.startswith("ENUM8"):
        return "ENUM8"
    if upper.startswith("ENUM16"):
        return "ENUM16"
    if upper.startswith("SIMPLEAGGREGATEFUNCTION"):
        return "SIMPLEAGGREGATEFUNCTION"
    if upper.startswith("AGGREGATEFUNCTION"):
        return "AGGREGATEFUNCTION"
    # Parametric ENUM/SET are closed domains — not specialty native carriers.
    if upper.startswith(("ENUM(", "SET(")):
        return None
    if upper.startswith("ARRAY<") or upper.endswith("[]"):
        return None
    # Strip schema/quotes — SYS.XMLTYPE / PG_CATALOG.INET must not greenwash.
    upper = upper.replace('"', "").replace("`", "").replace("[", "").replace("]", "")
    if "." in upper and not upper.startswith(
        ("ARRAY<", "STRUCT<", "MAP<", "RECORD<", "LIST<", "RANGE<")
    ):
        upper = upper.rsplit(".", 1)[-1].strip()
    # PG DATERANGE ↔ BigQuery RANGE<DATE> are dialect twins — one specialty base.
    range_canon = _normalize_range_carrier(inferred)
    if range_canon is not None:
        return range_canon
    # Re-normalize after schema strip for RANGE carriers.
    range_canon2 = _normalize_range_carrier(upper)
    if range_canon2 is not None:
        return range_canon2
    if "(" in upper and "RANGE" not in upper:
        upper = upper.split("(", 1)[0].strip()
    if upper in _SPECIALTY_NATIVE_CARRIERS:
        return upper
    return None


def specialty_wire_preserves_value(source_specialty: str, target_type: str) -> bool:
    """True when target is the industry-standard wire for a specialty carrier.

    Mongo ObjectId → ``CHAR/VARCHAR(24)`` hex or ``BINARY(12)`` (common RDBMS
    practice). IP families → ``VARCHAR(45)`` (IPv6). Bare TEXT/STRING still
    collapses polarity and must surface in preflight.
    """
    raw = strip_identity_qualifier(target_type).strip()
    if not raw:
        return False
    upper = raw.upper()
    spec = (source_specialty or "").upper()
    if spec == "OBJECTID":
        if upper in {"BINARY(12)", "VARBINARY(12)"}:
            return True
        m = re.match(
            r"^(?:N?VAR)?CHAR(?:ACTER)?(?:\s+VARYING)?\s*\(\s*(\d+)\s*\)$",
            upper,
        )
        if m and int(m.group(1)) >= 24:
            return True
        m = re.match(r"^VARCHAR2\s*\(\s*(\d+)\s*(?:BYTE|CHAR)?\s*\)$", upper)
        if m and int(m.group(1)) >= 24:
            return True
        # BigQuery STRING(n) create-new hex wire.
        m = re.match(r"^STRING\s*\(\s*(\d+)\s*\)$", upper)
        return bool(m and int(m.group(1)) >= 24)
    if spec in {"INET", "CIDR", "IPV4", "IPV6", "IP", "MACADDR", "MACADDR8"}:
        m = re.match(r"^(?:N?VAR)?CHAR(?:ACTER)?(?:\s+VARYING)?\s*\(\s*(\d+)\s*\)$", upper)
        if m and int(m.group(1)) >= 45:
            return True
        m = re.match(r"^VARCHAR2\s*\(\s*(\d+)\s*(?:BYTE|CHAR)?\s*\)$", upper)
        if m and int(m.group(1)) >= 45:
            return True
        m = re.match(r"^STRING\s*\(\s*(\d+)\s*\)$", upper)
        return bool(m and int(m.group(1)) >= 45)
    return False


def specialty_domain_would_invent(source_type: str, target_type: str) -> bool:
    """True when open string/json/scalar invents a native specialty domain.

    ``TEXT→INET`` / ``TEXT→TSVECTOR`` / ``JSON→HSTORE`` look like free casts but
    invent validation algorithms the source never had — Accept risk required.
    CITEXT invent is handled by :func:`case_fold_polarity_invent`.

    ``JSON→JSONB`` is a create-new dialect twin (same ``LOGICAL_JSON``) — not invent.
    """
    tgt = specialty_carrier_base(target_type)
    if tgt is None or tgt == "CITEXT":
        return False
    if specialty_carrier_base(source_type) is not None:
        return False
    src_l = normalize_logical_type(source_type)
    # Document polarity class → document specialty (JSONB) is dialect twin, not invent.
    # JSON→HSTORE still invents (HSTORE is not in the document class).
    if tgt in _DOCUMENT_POLARITY_BASES and src_l == LOGICAL_JSON:
        return False
    return src_l in {
        LOGICAL_STRING,
        LOGICAL_TEXT,
        LOGICAL_JSON,
        LOGICAL_INTEGER,
        LOGICAL_DECIMAL,
        LOGICAL_FLOAT,
        LOGICAL_BOOLEAN,
        LOGICAL_BINARY,
    }

def specialty_carrier_would_collapse(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
) -> bool:
    """True when a native specialty carrier would collapse to opaque/scalar sink.

    Airbyte maps inet/hstore/pg_lsn/geometric → string. Datawrap preserves natives
    on PG create-new; mapping to bare VARCHAR/TEXT/STRING invents document polarity
    and must surface in preflight (never silent green). Width-safe create-new
    wires (ObjectId→VARCHAR(24)) are not a collapse. OID→INTEGER invents a
    numeric polarity the catalog OID domain did not declare.
    """
    src = specialty_carrier_base(source_type)
    if src is None:
        return False
    tgt = specialty_carrier_base(target_type)
    if tgt is not None:
        # Host-address dialect twins (Elasticsearch IP / PG INET / IPv4/IPv6).
        # CIDR stays a distinct network polarity (INET→CIDR still collapses).
        if src in _IP_HOST_ADDRESS_TWINS and tgt in _IP_HOST_ADDRESS_TWINS:
            return False
        # Oracle XMLTYPE ↔ ANSI/SQL Server XML — same XML document polarity.
        if src in _XML_DOCUMENT_TWINS and tgt in _XML_DOCUMENT_TWINS:
            return False
        # Distinct specialty bases rewrite domain (INET→CIDR, MACADDR→MACADDR8).
        return src != tgt
    if specialty_wire_preserves_value(src, target_type):
        return False
    tgt_l = normalize_logical_type(target_type)
    # JSONB → dialect document LOB wire (NVARCHAR(MAX)/CLOB/STRING) is create-new twin.
    if src in _DOCUMENT_POLARITY_BASES and is_dialect_native_document_wire(
        target_type, dest_db=dest_db
    ):
        return False
    # JSONB→JSON / VARIANT / SUPER keep document polarity (dialect twin, not TEXT).
    if src == "JSONB" and tgt_l == LOGICAL_JSON:
        tgt_u = strip_identity_qualifier(target_type).upper().strip()
        bare = re.sub(r"\s*\(\s*\d+\s*\)", "", tgt_u).strip()
        if bare in {"JSON", "VARIANT", "OBJECT", "SUPER", "BSON", "JSONB"}:
            return False
    # Opaque / scalar sinks — specialty validation algorithms would not run.
    return tgt_l in {
        LOGICAL_STRING,
        LOGICAL_TEXT,
        LOGICAL_JSON,
        LOGICAL_INTEGER,
        LOGICAL_DECIMAL,
        LOGICAL_FLOAT,
        LOGICAL_BOOLEAN,
        LOGICAL_UUID,
        LOGICAL_DATE,
        LOGICAL_DATETIME,
        LOGICAL_TIME,
        LOGICAL_BINARY,
    }

def resolve_mapping_target_type(
    mapping: dict,
    *,
    target_types: dict[str, str] | None = None,
    source_type: str = "",
    dest_db_type: str = "",
) -> str:
    """Resolve the DDL type a mapping row should validate/write against.

    Create-new: stamped ``mapping["target_type"]`` is authoritative (writer
    intent) — never invent UUID→UUID green when the map stamped STRING.
    Existing columns: live destination type wins, then stamped, then source.
    """
    types = target_types or {}
    tgt = str(mapping.get("target") or "").strip()
    stamped = str(mapping.get("target_type") or "").strip()
    live = ""
    if tgt and tgt in types:
        live = str(types.get(tgt) or "").strip()
    elif tgt:
        lower_map = {str(k).lower(): v for k, v in types.items()}
        live = str(lower_map.get(tgt.lower(), "") or "").strip()
    src = (
        (source_type or "").strip()
        or str(mapping.get("source_type") or "").strip()
        or "VARCHAR"
    )
    if mapping.get("create_new"):
        db = (
            (dest_db_type or "").strip()
            or str(mapping.get("dest_db_type") or mapping.get("destination_db") or "").strip()
        )
        if stamped:
            # Upgrade legacy bare TIMESTAMP stamps when source declares FSP.
            return promote_create_new_temporal_stamp(src, stamped, db)
        if live:
            return live
        # Empty stamp must not fall back to source identity (BQ UUID→UUID lie).
        if db:
            return create_new_mapping_target_type(src, db)
        return src
    return live or stamped or src



def promote_create_new_temporal_stamp(src_type: str, stamped: str, dest_db_type: str = "") -> str:
    """Upgrade or strip temporal stamps for destination-legal DDL.

    PG/MySQL/SQL Server: bare TIMESTAMP/TIME/DATETIME2 are FSP-0 (or ambiguous)
    and must promote when source declares (p). Redshift/BigQuery/Databricks/
    Iceberg reject typmod — never invent TIMESTAMP(6) (illegal CREATE).
    """
    out = (stamped or "").strip()
    if not out:
        return out
    bare = re.sub(r"\s*\(\s*\d+\s*\)", "", out.upper()).strip()
    db = (dest_db_type or "").strip().lower()
    # Destinations that cannot take typmod: strip illegal (p) if present.
    # Keep TIMESTAMP_NTZ / TIMESTAMPTZ polarity tokens — never collapse NTZ→TIMESTAMP
    # on Databricks (TIMESTAMP is session-TZ aware).
    if db in _NO_TEMPORAL_TYPMOD_ENGINES and bare in {
        "TIMESTAMP",
        "TIME",
        "DATETIME",
        "TIMESTAMP_NTZ",
        "TIMESTAMPTZ",
        "TIMESTAMP_LTZ",
        "TIMESTAMP_TZ",
    }:
        if "(" in out.upper():
            # Strip typmod only; preserve polarity token.
            return bare
        return out
    src_p = parse_temporal_fractional_precision(src_type)
    if src_p is None:
        # Already-parameterized stamps must not be upgraded (DATETIME2(6)→(7)
        # via materialize_dest_ddl empty-src promote).
        if parse_temporal_fractional_precision(out) is not None:
            return out
        # SQL Server bare DATETIME2 defaults to precision 7 — stamp it so G3
        # does not treat create-new as FSP-0 collapse vs TIMESTAMP_NTZ(6+).
        if bare == "DATETIME2" and db in {"sqlserver", "mssql", ""}:
            return "DATETIME2(7)"
        if bare == "TIME" and db in {"sqlserver", "mssql"} and "(" not in out.upper():
            return "TIME(7)"
        return out
    stamp_p = parse_temporal_fractional_precision(out)
    # Already-parameterized stamps (including Accept-risk narrow) stay as-is.
    if stamp_p is not None:
        return out
    if bare == "TIMESTAMP":
        if db in _NO_TEMPORAL_TYPMOD_ENGINES:
            return out
        if db in {
            "",
            "postgresql",
            "postgres",
            "pg",
            "cockroach",
            "cockroachdb",
            "alloydb",
            "timescaledb",
            "yugabytedb",
            "citus",
            "supabase",
            "greenplum",
        }:
            return f"TIMESTAMP({src_p})"
        return out
    if bare == "TIME":
        if db in _NO_TEMPORAL_TYPMOD_ENGINES:
            return out
        if db in {
            "",
            "mysql",
            "mariadb",
            "tidb",
            "postgresql",
            "postgres",
            "pg",
            "cockroachdb",
            "alloydb",
            "sqlserver",
            "mssql",
        }:
            return f"TIME({src_p})"
        return out
    if bare == "DATETIME" and db in {"mysql", "mariadb", "tidb", ""}:
        return f"DATETIME({src_p})"
    if bare == "DATETIME2" and db in {"sqlserver", "mssql", ""}:
        return f"DATETIME2({src_p})"
    return out



def create_new_mapping_target_type(src_type: str, dest_db_type: str = "") -> str:
    """Target type stamped on create-new mappings for Validate + writers.

    Stamp **physical** DDL whenever the destination has no native UUID type —
    even for exact ``CHAR(36)`` / ``VARCHAR(36)`` wires. Map must match CREATE
    (never silent-green UUID→UUID while writers emit VARCHAR). Native UUID /
    UNIQUEIDENTIFIER destinations keep the engine token.
    """
    if normalize_logical_type(src_type) == LOGICAL_UUID:
        db_uuid = (dest_db_type or "").strip()
        if not db_uuid:
            return "UUID"
        physical_uuid = ddl_type(db_uuid, src_type)
        phys_base = strip_identity_qualifier(physical_uuid).upper()
        src_u = strip_identity_qualifier(src_type).upper()
        # SQL Server native token — stamp UNIQUEIDENTIFIER (matches CREATE).
        if phys_base in {"UNIQUEIDENTIFIER", "GUID"}:
            return physical_uuid
        # Native UUID engines (PG, …) — keep logical UUID.
        if normalize_logical_type(physical_uuid) == LOGICAL_UUID and not uuid_exact_wire_carrier(
            physical_uuid
        ):
            return "UUID"
        if phys_base in {"UUID"}:
            return "UUID"
        # Exact-wire / STRING / TEXT sinks — stamp physical so Map ≡ CREATE.
        # UNIQUEIDENTIFIER/GUID off-SQL-Server land here too (never bare STRING).
        phys_u = strip_identity_qualifier(physical_uuid).upper().strip()
        if phys_u in {"STRING", "TEXT", "VARCHAR", "NVARCHAR"} and not uuid_exact_wire_carrier(
            physical_uuid
        ):
            db_l = db_uuid.strip().lower()
            if db_l in {"bigquery", "bq"}:
                return "STRING(36)"
            if db_l in {"databricks", "spark", "delta", "delta_lake"}:
                return "VARCHAR(36)"
            if db_l == "iceberg":
                return "string"
        # UNIQUEIDENTIFIER off-SQL-Server with non-bare physical (CHAR(36), …).
        if src_u in {"UNIQUEIDENTIFIER", "GUID"} and phys_base not in {
            "UUID",
            "UNIQUEIDENTIFIER",
            "GUID",
        }:
            return physical_uuid
        return physical_uuid
    specialty = specialty_carrier_base(src_type)
    db = (dest_db_type or "").strip()
    if specialty:
        if db:
            physical = ddl_type(db, src_type)
            # Dest keeps a native specialty token (PG→PG INET) — stamp that.
            if specialty_carrier_base(physical) is not None:
                return physical
            # Off-engine IP/INET host-address wire — VARCHAR(45) hex/text, not bare TEXT.
            if specialty in {"INET", "CIDR", "IPV4", "IPV6", "IP", "MACADDR", "MACADDR8"}:
                if specialty_wire_preserves_value(specialty, physical):
                    return physical
                db_l = db.lower()
                if db_l in {"bigquery", "bq"}:
                    return "STRING(45)"
                if db_l in {"sqlserver", "mssql"}:
                    return "VARCHAR(45)"
                if db_l == "oracle":
                    return "VARCHAR2(45)"
                return "VARCHAR(45)"
            # Off-engine wire (ObjectId→VARCHAR(24), …) — stamp physical sink.
            return physical
        return specialty
    if db:
        return promote_create_new_temporal_stamp(src_type, ddl_type(db, src_type), dest_db_type)
    return (src_type or "VARCHAR").strip() or "VARCHAR"



# Tokens that writers must not re-interpret via ddl_type (Map stamp authority).
_PHYSICAL_STAMP_PASS_THROUGH: Final[frozenset[str]] = frozenset({
    "REAL", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION",
    "BINARY_FLOAT", "BINARY_DOUBLE",
    "HALF", "FLOAT16", "FLOAT32", "FLOAT64",
    "JSONB", "JSON", "VARIANT", "SUPER", "HSTORE", "AVRO",
    "INET", "CIDR", "UUID", "BYTEA", "CITEXT",
    "TIMESTAMPTZ", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ",
    "DATETIME2", "DATETIMEOFFSET", "SMALLDATETIME", "TIMETZ",
    "MONEY", "SMALLMONEY", "YEAR",
    "NVARCHAR2", "VARCHAR2", "NCHAR", "NVARCHAR", "NCLOB", "CLOB", "BLOB",
    "NUMBER", "NUMERIC", "DECIMAL", "BIGNUMERIC",
    "GEOMETRY", "GEOGRAPHY", "VECTOR", "HLLSKETCH",
    "TINYINT", "MEDIUMINT", "SMALLINT", "BIGINT", "INTEGER", "INT", "BIGINT UNSIGNED",
    "BOOLEAN", "BOOL", "DATE", "TIME", "TIMESTAMP", "DATETIME", "DATETIME64",
    "TEXT", "STRING", "VARCHAR", "CHAR", "BPCHAR", "CHARACTER VARYING",
    "OBJECTID", "UNIQUEIDENTIFIER", "GUID", "ROWVERSION", "BIT",
    "ENUM", "SET", "ARRAY", "STRUCT", "MAP", "RECORD",
})

# Pass-through tokens that are illegal or not the create-new wire on a dest.
# Explicit — never infer. Bare JSON on PG rematerializes to JSONB (create-new).
# UUID/JSON on Redshift/Snowflake/BQ must not invent non-existent DDL.
_PASS_THROUGH_REJECT_ON_DEST: Final[dict[str, frozenset[str]]] = {
    "redshift": frozenset({
        "JSON", "JSONB", "UUID", "BYTEA", "INET", "CIDR", "CITEXT", "HSTORE",
        "NVARCHAR2", "VARCHAR2", "NUMBER", "BIGNUMERIC", "UNIQUEIDENTIFIER",
        # Foreign binary typmod → VARBYTE(n) create-new wire.
        "BINARY", "VARBINARY", "BYTES", "FIXED",
    }),
    "snowflake": frozenset({
        "JSON", "JSONB", "UUID", "BYTEA", "INET", "CIDR", "CITEXT", "HSTORE",
        "SUPER", "NVARCHAR2", "BIGNUMERIC", "UNIQUEIDENTIFIER",
        # Native wire is BINARY(n); rematerialize foreign aliases.
        "VARBINARY", "BYTES", "VARBYTE", "FIXED",
    }),
    "bigquery": frozenset({
        "UUID", "JSONB", "BYTEA", "INET", "CIDR", "CITEXT", "HSTORE", "SUPER",
        "VARIANT", "NVARCHAR2", "VARCHAR2", "NUMBER", "UNIQUEIDENTIFIER",
        # Native wire is BYTES(n); rematerialize BINARY/VARBINARY/fixed typmods.
        "BINARY", "VARBINARY", "VARBYTE", "FIXED",
    }),
    "spanner": frozenset({
        "UUID", "JSONB", "BYTEA", "INET", "CIDR", "CITEXT", "HSTORE", "SUPER",
        "VARIANT", "NVARCHAR2", "VARCHAR2", "BIGNUMERIC", "UNIQUEIDENTIFIER",
        "DATETIME", "TIME",  # Spanner has no DATETIME/TIME — use STRING wire
        "BINARY", "VARBINARY", "VARBYTE", "FIXED",
    }),
    "postgresql": frozenset({
        # Bare JSON is a logical alias; create-new document wire is JSONB.
        "JSON",
        "SUPER", "VARIANT", "BIGNUMERIC", "UNIQUEIDENTIFIER", "NVARCHAR2",
        # Native wire is BYTEA; rematerialize BINARY(n)/BYTES(n)/fixed(n).
        "BINARY", "VARBINARY", "BYTES", "VARBYTE", "FIXED",
    }),
    "mysql": frozenset({
        "JSONB", "UUID", "BYTEA", "SUPER", "VARIANT", "BIGNUMERIC",
        "UNIQUEIDENTIFIER", "NVARCHAR2", "HSTORE",
        # Native BINARY(n)/VARBINARY(n); rematerialize foreign aliases only.
        "BYTES", "VARBYTE", "FIXED",
    }),
    "sqlserver": frozenset({
        "JSONB", "UUID", "BYTEA", "SUPER", "VARIANT", "BIGNUMERIC", "JSON",
        "NVARCHAR2", "HSTORE", "INET", "CIDR",
        "BYTES", "VARBYTE", "FIXED",
    }),
    "oracle": frozenset({
        "JSONB", "UUID", "BYTEA", "SUPER", "VARIANT", "BIGNUMERIC", "JSON",
        "UNIQUEIDENTIFIER", "HSTORE", "INET", "CIDR",
        # Typmod BINARY(n) → RAW(n); bare BINARY already rematerializes to BLOB.
        "BINARY", "VARBINARY", "BYTES", "VARBYTE", "FIXED",
    }),
    "databricks": frozenset({
        "JSONB", "UUID", "BYTEA", "SUPER", "VARIANT", "BIGNUMERIC",
        "UNIQUEIDENTIFIER", "NVARCHAR2", "HSTORE", "INET", "CIDR",
        "BINARY", "VARBINARY", "BYTES", "VARBYTE", "FIXED",
    }),
    "iceberg": frozenset({
        "JSONB", "UUID", "BYTEA", "SUPER", "VARIANT", "BIGNUMERIC",
        "UNIQUEIDENTIFIER", "NVARCHAR2", "HSTORE", "INET", "CIDR", "JSON",
        # Widthed binary → fixed(n); bare BINARY already → binary. Keep FIXED native.
        "BINARY", "VARBINARY", "BYTES", "VARBYTE",
    }),
    # SQLite has no true fixed-point type. DECIMAL/NUMERIC/NUMBER stamps get
    # NUMERIC affinity and silently store high-precision values as IEEE real.
    # Rematerialize via ddl_type → TEXT (Map≡CREATE honesty).
    # Foreign binary typmods → BLOB (SQLite ignores length; no affinity invent).
    "sqlite": frozenset({
        "DECIMAL", "NUMERIC", "NUMBER", "BIGNUMERIC", "BIGDECIMAL",
        "MONEY", "SMALLMONEY", "CURRENCY",
        "BINARY", "VARBINARY", "BYTES", "VARBYTE", "BYTEA", "FIXED",
    }),
}


def _is_explicit_physical_stamp(carrier: str, dest_db: str = "") -> bool:
    """True when carrier is already dest DDL (Map stamp) — do not re-ddl invent."""
    raw = strip_identity_qualifier(carrier).strip()
    if not raw:
        return False
    upper = raw.upper()
    db = _normalize_dest_db(dest_db) if dest_db else ""
    reject = _PASS_THROUGH_REJECT_ON_DEST.get(db, frozenset()) if db else frozenset()
    # Typmod / nested / array brackets are physical unless the bare token is
    # illegal on this dest (e.g. DECIMAL(38,18) on SQLite → TEXT rematerialize).
    if "(" in upper or "[" in upper or "<" in upper:
        if "<" in upper or "[" in upper:
            return True
        bare_typmod = upper.split("(", 1)[0].strip()
        if bare_typmod in reject:
            return False
        return True
    bare = upper.split("(", 1)[0].strip()
    if bare in _PHYSICAL_STAMP_PASS_THROUGH or upper in _PHYSICAL_STAMP_PASS_THROUGH:
        # Refuse pass-through of tokens illegal / non-create-wire on this dest.
        if bare in reject:
            return False
        return True
    # MySQL/Maria FLOAT is a real physical stamp (HALF create-new). On PG/etc.
    # bare FLOAT is the logical alias that must still map via ddl_type → DOUBLE.
    if bare == "FLOAT":
        return db in {"mysql", "mariadb", "tidb", "sqlserver", "mssql"}
    if specialty_carrier_base(raw) is not None:
        return True
    # Dialect multi-word tokens
    if upper.startswith("TIMESTAMP ") or upper.startswith("TIME WITH"):
        return True
    if upper.startswith("DOUBLE ") or upper.startswith("CHARACTER "):
        return True
    return False


def materialize_dest_ddl(db_type: str, carrier: str | None) -> str:
    """Writer CREATE DDL: honor Map physical stamps; map logicals via ddl_type.

    SSOT so Execute CREATE cannot invent REAL→DOUBLE, TIMESTAMP→DATETIME (BQ),
    NVARCHAR2→VARCHAR2 BYTE, or ARRAY<FLOAT>→ARRAY<DOUBLE> after Map stamped.
    Illegal typmod on no-typmod engines is still legalized via promote.

    Pass-through is dest-gated via ``_PASS_THROUGH_REJECT_ON_DEST``: tokens that
    are illegal on the destination (Redshift ``JSON``/``UUID``, BQ ``UUID``, …)
    or are logical aliases of the create-new wire (PG ``JSON``→``JSONB``) always
    rematerialize. Native stamps (``SUPER``, ``JSONB``, ``VARIANT``, PG ``UUID``)
    still pass through unchanged.

    Iceberg nested spelling is an exception: ``ARRAY<T>`` / ``T[]`` / ``VECTOR``
    must become ``list<…>`` (float leaves stay float) — not pass-through Spark
    ARRAY tokens the Iceberg writer cannot CREATE.

    Known limitation: typmod-bearing stamps (``VARCHAR(n)``) usually pass
    through even when ``n`` exceeds a destination cap — width collapse is
    Validate's job, not silent CREATE rewrite. Exception: tokens listed in
    ``_PASS_THROUGH_REJECT_ON_DEST`` (e.g. ``DECIMAL(p,s)`` on SQLite) always
    rematerialize via ``ddl_type`` so CREATE cannot invent NUMERIC affinity.
    """
    raw = strip_identity_qualifier(carrier).strip()
    if not raw:
        return ddl_type(db_type, "VARCHAR")
    db = _normalize_dest_db(db_type)
    upper = raw.upper()
    if db == "iceberg":
        if (
            upper.startswith("ARRAY<")
            or upper.startswith("ARRAY(")
            or upper.startswith("LIST<")
            or upper.startswith("LIST(")
            or upper.endswith("[]")
            or normalize_logical_type(raw) == LOGICAL_VECTOR
        ):
            return ddl_type(db, raw)
    if _is_explicit_physical_stamp(raw, db):
        legalized = promote_create_new_temporal_stamp("", raw, db)
        return legalized or raw
    # Rematerialized UUID aliases must use create-new width-safe wire
    # (BQ STRING(36), not bare STRING) so writers match Map stamps.
    if normalize_logical_type(raw) == LOGICAL_UUID:
        return create_new_mapping_target_type(raw, db_type)
    return ddl_type(db, raw)


def uuid_exact_wire_carrier(target_type: str | None) -> bool:
    """True only for exact 36-char UUID string wires (not VARCHAR(50)+)."""
    raw = strip_identity_qualifier(target_type).strip()
    if not raw:
        return False
    upper = raw.upper()
    if upper in {"UUID", "UNIQUEIDENTIFIER", "GUID"}:
        return True
    m = re.match(
        r"^(?:N?VAR)?CHAR(?:ACTER)?\s*\(\s*(\d+)\s*\)$",
        upper,
    )
    if m and int(m.group(1)) == 36:
        return True
    m = re.match(r"^VARCHAR2\s*\(\s*(\d+)\s*\)$", upper)
    if m and int(m.group(1)) == 36:
        return True
    m = re.match(r"^STRING\s*\(\s*(\d+)\s*\)$", upper)
    return bool(m and int(m.group(1)) == 36)


def uuid_capacity_string_carrier(target_type: str | None) -> bool:
    """True when the physical type can hold a canonical UUID without truncation.

    MySQL/Oracle/Redshift create-new emit ``CHAR(36)`` / ``VARCHAR(36)`` /
    ``VARCHAR2(36)`` for UUID. Those are the dialect-native wire — not an
    opaque domain collapse. Bare ``VARCHAR`` / ``TEXT`` / ``STRING`` still
    collapse (lost UUID polarity) and must surface in preflight.

    Widths ``> 36`` also preserve the value (no truncation) for collapse
    checks, but :func:`ddl_type` must not rewrite them to UUID DDL — see
    :func:`uuid_exact_wire_carrier`.
    """
    if uuid_exact_wire_carrier(target_type):
        return True
    raw = strip_identity_qualifier(target_type).strip()
    if not raw:
        return False
    upper = raw.upper()
    m = re.match(
        r"^(?:N?VAR)?CHAR(?:ACTER)?\s*\(\s*(\d+)\s*\)$",
        upper,
    )
    if m and int(m.group(1)) >= 36:
        return True
    m = re.match(r"^VARCHAR2\s*\(\s*(\d+)\s*\)$", upper)
    if m and int(m.group(1)) >= 36:
        return True
    m = re.match(r"^STRING\s*\(\s*(\d+)\s*\)$", upper)
    return bool(m and int(m.group(1)) >= 36)



def objectid_would_collapse(source_type: str, target_type: str) -> bool:
    """True when ObjectId polarity collapses to opaque string/text.

    Width-safe create-new wires (``VARCHAR(24)`` / ``BINARY(12)``) preserve the
    hex contract and are not a collapse. Bare TEXT/VARCHAR/STRING drop domain
    polarity and must surface in preflight — never silent green.
    """
    if normalize_logical_type(source_type) != LOGICAL_OBJECTID:
        return False
    if normalize_logical_type(target_type) == LOGICAL_OBJECTID:
        return False
    if specialty_wire_preserves_value("OBJECTID", target_type):
        return False
    tgt_l = normalize_logical_type(target_type)
    return tgt_l in {LOGICAL_STRING, LOGICAL_TEXT, LOGICAL_JSON, LOGICAL_BINARY}


def uuid_would_collapse(source_type: str, target_type: str) -> bool:
    """True when UUID polarity collapses to opaque string/text.

    Top-level UUID→bare VARCHAR/TEXT/STRING is common on Snowflake/Databricks
    and must surface in preflight — never silent green. Dialect-native
    ``CHAR(36)`` / ``VARCHAR(36)`` carriers (MySQL create-new) preserve the
    value *and* the 36-char contract, so they are not a collapse.
    """
    if normalize_logical_type(source_type) != LOGICAL_UUID:
        return False
    if normalize_logical_type(target_type) == LOGICAL_UUID:
        return False
    if uuid_capacity_string_carrier(target_type):
        return False
    tgt_l = normalize_logical_type(target_type)
    return tgt_l in {LOGICAL_STRING, LOGICAL_TEXT, LOGICAL_JSON}


def strip_collation_qualifier(inferred: str | None) -> str:
    """Remove ``COLLATE name`` / ``NONDETERMINISTIC`` suffixes for logical lookup."""
    text = (inferred or "").strip()
    if not text:
        return ""
    text = re.sub(r"\s+NONDETERMINISTIC\b", "", text, flags=re.I)
    text = re.sub(r"\s+COLLATE\s+\S+", "", text, flags=re.I)
    return text.strip()


def parse_collation(inferred: str | None) -> str | None:
    """Return collation name from ``… COLLATE name`` carrier, else None."""
    m = re.search(r"\bCOLLATE\s+(\S+)", (inferred or ""), re.I)
    return m.group(1) if m else None


def is_case_insensitive_collation(inferred: str | None) -> bool:
    """True when destination collation equates ``A`` and ``a``.

    Covers MySQL ``*_ci``, SQL Server ``CI_AS`` / ``CI_AI``, PG ``citext``, and ICU
    nondeterministic collations (``collisdeterministic = false``).
    """
    raw = (inferred or "").upper()
    if not raw:
        return False
    if "CITEXT" in raw or "NONDETERMINISTIC" in raw:
        return True
    name = (parse_collation(inferred) or "").upper() or raw
    if "CITEXT" in name:
        return True
    if re.search(r"CASE.?INSENSITIVE", name):
        return True
    # Explicit case-sensitive wins (SQL Server _CS_, MySQL _cs).
    if re.search(r"(?:^|_)CS(?:_|$)|_CS\b|_CS_", name):
        return False
    # MySQL utf8mb4_general_ci / unicode_ci; SQL Server Latin1_General_CI_AS/AI.
    if re.search(r"(?:^|_)CI(?:_|$)|_CI\b|CI_AS|CI_AI", name):
        return True
    return False


def is_accent_insensitive_collation(inferred: str | None) -> bool:
    """True when destination collation equates ``cafe`` and ``café``.

    Mirrors SQL Server ``_AI`` / MySQL ``*_ai_ci`` / legacy ``general_ci`` and
    ``unicode_ci``. Explicit ``_AS`` / ``*_as_ci`` stays accent-sensitive so
    Validate does not false-block distinct accented keys on CI_AS columns.
    """
    name = (parse_collation(inferred) or "").upper()
    if not name:
        return False
    # Explicit accent-insensitive (Latin1_General_CI_AI, utf8mb4_0900_ai_ci).
    if re.search(r"(?:^|_)AI(?:_|$)|_AI\b|_AI_", name):
        return True
    # Explicit accent-sensitive (Latin1_General_CI_AS, utf8mb4_0900_as_ci).
    if re.search(r"(?:^|_)AS(?:_|$)|_AS\b|_AS_", name):
        return False
    # MySQL legacy CI collations are accent-insensitive by default.
    if re.search(r"(?:GENERAL|UNICODE)_CI$", name):
        return True
    if name.endswith("_GENERAL_CI") or name.endswith("_UNICODE_CI"):
        return True
    return False


def _is_windows_style_collation(name: str) -> bool:
    """True for SQL Server Windows/SQL collations with CI/CS + AI/AS suffixes."""
    upper = (name or "").upper()
    if not upper or "_BIN" in upper:
        return False
    return bool(re.search(r"_C[IS]_A[IS]", upper))


def is_width_insensitive_collation(inferred: str | None) -> bool:
    """True when collation equates fullwidth/halfwidth forms.

    SQL Server: omitting ``_WS`` means width-insensitive (MS collation docs).
    Explicit ``_WS`` stays width-sensitive. MySQL has no WS token — leave alone.
    """
    name = (parse_collation(inferred) or "").upper()
    if not name:
        return False
    if re.search(r"(?:^|_)WS(?:_|$)|_WS\b|_WS_", name):
        return False
    if re.search(r"(?:^|_)WI(?:_|$)|_WI\b|_WI_", name):
        return True
    # Latin1_General_CI_AS (no _WS) ≡ width-insensitive per SQL Server.
    return _is_windows_style_collation(name)


def is_kana_insensitive_collation(inferred: str | None) -> bool:
    """True when collation equates Hiragana and Katakana (SQL Server default).

    Omitting ``_KS`` is kana-insensitive; explicit ``_KS`` keeps them distinct.
    """
    name = (parse_collation(inferred) or "").upper()
    if not name:
        return False
    if re.search(r"(?:^|_)KS(?:_|$)|_KS\b|_KS_", name):
        return False
    return _is_windows_style_collation(name)


def is_variation_insensitive_collation(inferred: str | None) -> bool:
    """True when collation ignores ideographic variation selectors (SQL Server).

    Omitting ``_VSS`` / ``_VS`` is variation-selector-insensitive (MS docs).
    Explicit ``_VSS`` / ``_VS`` keeps selectors distinct.
    """
    name = (parse_collation(inferred) or "").upper()
    if not name:
        return False
    if re.search(r"(?:^|_)VSS?(?:_|$)|_VSS?\b|_VSS?_", name):
        return False
    return _is_windows_style_collation(name)


def fold_diacritics(text: str) -> str:
    """NFKD + strip combining marks — enterprise AI collation approximation.

    Matches the common ETL/search algorithm (café→cafe) used when the engine
    collation is accent-insensitive. Not a full Windows sort-weight table, but
    catches the Latin diacritic collisions that otherwise false-green Validate.
    """
    import unicodedata

    if not text:
        return text
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(c for c in decomposed if unicodedata.category(c) != "Mn")


def fold_width_forms(text: str) -> str:
    """NFKC compatibility fold for width-insensitive collations (Ａ→A)."""
    import unicodedata

    if not text:
        return text
    return unicodedata.normalize("NFKC", text)


def fold_kana(text: str) -> str:
    """Map Hiragana→Katakana for kana-insensitive uniqueness (SQL Server KI)."""
    if not text:
        return text
    out: list[str] = []
    for ch in text:
        code = ord(ch)
        # Hiragana letter block → corresponding Katakana (+0x60).
        if 0x3041 <= code <= 0x3096:
            out.append(chr(code + 0x60))
        else:
            out.append(ch)
    return "".join(out)


def fold_variation_selectors(text: str) -> str:
    """Strip Unicode variation selectors for VSS-insensitive uniqueness.

    Covers VS1–VS16 (U+FE00–U+FE0F) and IVS (U+E0100–U+E01EF).
    """
    if not text:
        return text
    out: list[str] = []
    for ch in text:
        code = ord(ch)
        if 0xFE00 <= code <= 0xFE0F:
            continue
        if 0xE0100 <= code <= 0xE01EF:
            continue
        out.append(ch)
    return "".join(out)


def _unique_key_column_names(uk: dict[str, Any]) -> set[str]:
    return {
        str(c).lower()
        for c in (
            list(uk.get("columns") or []) + list(uk.get("expression_columns") or [])
        )
    }


def unique_keys_covering_column(
    column: str | None,
    unique_keys: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Return catalog unique keys whose columns/expressions cover ``column``."""
    col = (column or "").strip().lower()
    if not col:
        return []
    return [uk for uk in (unique_keys or []) if col in _unique_key_column_names(uk)]


def unique_key_forces_casefold(
    column: str | None,
    *,
    ddl_type: str | None = None,
    unique_keys: list[dict[str, Any]] | None = None,
) -> bool:
    """True when dest column equality is case-insensitive for uniqueness.

    Covers column CI/CITEXT collations and expression unique indexes such as
    ``UNIQUE (lower(email))`` / ``UNIQUE (upper(code))`` discovered from catalog.
    """
    if is_case_insensitive_collation(ddl_type):
        return True
    return any(bool(uk.get("case_insensitive")) for uk in unique_keys_covering_column(column, unique_keys))


def unique_key_nulls_collide(
    column: str | None,
    unique_keys: list[dict[str, Any]] | None = None,
) -> bool:
    """True when PG ``NULLS NOT DISTINCT`` (or equivalent) treats NULL as equal.

    Default UNIQUE allows many NULLs; NULLS NOT DISTINCT rejects a second NULL —
    Validate must count empty/null sample keys or write fails after green Validate.
    """
    return any(
        bool(uk.get("nulls_not_distinct"))
        for uk in unique_keys_covering_column(column, unique_keys)
    )


_FILTER_IS_NOT_NULL_RE = re.compile(
    r"^\(?\s*\[?\"?([A-Za-z_][\w$]*)\"?\]?\s+IS\s+NOT\s+NULL\s*\)?$",
    re.I,
)
_FILTER_EQ_STR_RE = re.compile(
    r"^\(?\s*\[?\"?([A-Za-z_][\w$]*)\"?\]?\s*=\s*'((?:[^']|'')*)'\s*(?:::\s*\w+)?\s*\)?$",
    re.I,
)
_FILTER_EQ_NUM_RE = re.compile(
    r"^\(?\s*\[?\"?([A-Za-z_][\w$]*)\"?\]?\s*=\s*\(?(-?\d+(?:\.\d+)?)\)?\s*\)?$",
    re.I,
)
_FILTER_EQ_BOOL_RE = re.compile(
    r"^\(?\s*\[?\"?([A-Za-z_][\w$]*)\"?\]?\s*=\s*\(?(true|false|0|1)\)?\s*\)?$",
    re.I,
)


def _split_top_level_and(predicate: str) -> list[str]:
    """Split ``a AND b AND c`` at top level (respecting quotes/parens)."""
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    in_str = False
    i = 0
    s = predicate
    while i < len(s):
        ch = s[i]
        if ch == "'" and not in_str:
            in_str = True
            buf.append(ch)
            i += 1
            continue
        if ch == "'" and in_str:
            # SQL escaped quote ''
            if i + 1 < len(s) and s[i + 1] == "'":
                buf.append("''")
                i += 2
                continue
            in_str = False
            buf.append(ch)
            i += 1
            continue
        if not in_str:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth = max(0, depth - 1)
            elif depth == 0 and s[i : i + 5].upper() == " AND ":
                part = "".join(buf).strip()
                if part:
                    parts.append(part)
                buf = []
                i += 5
                continue
        buf.append(ch)
        i += 1
    part = "".join(buf).strip()
    if part:
        parts.append(part)
    return parts or [predicate.strip()]


def row_matches_unique_filter(
    row: dict[str, Any],
    filter_predicate: str | None,
) -> bool:
    """Evaluate simple partial-unique predicates against a sample row.

    Covers ``col IS NOT NULL``, ``col = literal``, and top-level ``AND``
    conjunctions (PG ``WHERE``, SQL Server filtered indexes). Unknown atoms
    conservatively include the row — false-fail over silent write failure.
    """
    pred = (filter_predicate or "").strip()
    if not pred:
        return True
    # Strip one layer of wrapping parens around a conjunction.
    if pred.startswith("(") and pred.endswith(")"):
        inner = pred[1:-1].strip()
        if _split_top_level_and(inner) != [inner]:
            pred = inner

    and_parts = _split_top_level_and(pred)
    if len(and_parts) > 1:
        return all(row_matches_unique_filter(row, part) for part in and_parts)

    def _cell(col: str) -> Any:
        if col in row:
            return row.get(col)
        lower = {str(k).lower(): v for k, v in row.items()}
        return lower.get(col.lower())

    m = _FILTER_IS_NOT_NULL_RE.match(pred)
    if m:
        val = _cell(m.group(1))
        if val is None:
            return False
        text = str(val).strip()
        return bool(text) and text.lower() not in {"none", "null", "<na>"}

    m = _FILTER_EQ_STR_RE.match(pred)
    if m:
        expected = m.group(2).replace("''", "'")
        val = _cell(m.group(1))
        return str(val if val is not None else "").strip() == expected

    m = _FILTER_EQ_NUM_RE.match(pred)
    if m:
        val = _cell(m.group(1))
        try:
            return float(val) == float(m.group(2))
        except Exception:
            return False

    m = _FILTER_EQ_BOOL_RE.match(pred)
    if m:
        raw_exp = m.group(2).lower()
        expected = raw_exp in {"true", "1"}
        val = _cell(m.group(1))
        text = str(val if val is not None else "").strip().lower()
        actual = text in {"true", "1", "t", "yes"}
        return actual is expected

    # Unevaluable filter — include row (prefer Validate block over silent 23505).
    return True


def unique_key_row_in_scope(
    row: dict[str, Any],
    column: str | None,
    unique_keys: list[dict[str, Any]] | None = None,
) -> bool:
    """True when sample row participates in at least one covering unique key.

    Rows outside every covering filter are skipped for that column's duplicate
    probe (partial unique index honesty).
    """
    covering = unique_keys_covering_column(column, unique_keys)
    if not covering:
        return True
    filtered = [uk for uk in covering if str(uk.get("filter_predicate") or "").strip()]
    if not filtered:
        return True
    # Row is in scope if it matches any covering partial unique (OR of filters).
    return any(
        row_matches_unique_filter(row, str(uk.get("filter_predicate") or ""))
        for uk in filtered
    )


def trailing_spaces_insignificant_for_unique(
    ddl_type: str | None,
    *,
    dest_kind: str | None = None,
) -> bool:
    """True when UNIQUE equality ignores trailing spaces (SQL PAD SPACE).

    - CHAR/NCHAR/BPCHAR: always PAD SPACE (PG/Oracle/MySQL/SQL Server)
    - VARCHAR: PAD SPACE on MySQL/SQL Server; NO PAD on PostgreSQL/Oracle
    """
    if is_fixed_width_char_carrier(ddl_type):
        return True
    logical = normalize_logical_type(ddl_type)
    if logical not in {LOGICAL_STRING, LOGICAL_TEXT}:
        return False
    upper = strip_identity_qualifier(ddl_type).upper()
    if "VARCHAR2" in upper or "NVARCHAR2" in upper:
        return False
    kind = (dest_kind or "").strip().lower().replace("-", "_")
    if kind in {
        "postgresql",
        "postgres",
        "redshift",
        "greenplum",
        "oracle",
        "oracle_db",
        "amazon_rds_oracle",
    }:
        return False
    if kind in {
        "mysql",
        "mariadb",
        "sqlserver",
        "mssql",
        "azure_sql",
        "amazon_rds_sqlserver",
    }:
        return True
    # Infer from DDL tokens when dest_kind omitted.
    if re.search(r"\bN(?:VAR)?CHAR\b", upper) and "VARCHAR2" not in upper:
        return True
    coll = (parse_collation(ddl_type) or "").upper()
    if re.search(r"UTF8|UCA|LATIN1_GENERAL|SQL_LATIN", coll):
        # MySQL / SQL Server collations → PAD SPACE on VARCHAR.
        if "UTF8" in coll or "UCA" in coll:
            return True
        if _is_windows_style_collation(coll):
            return True
    return False


def unique_equality_key(
    value: Any,
    ddl_type: str | None = None,
    *,
    force_casefold: bool = False,
    null_sentinel: str | None = None,
    dest_kind: str | None = None,
) -> str:
    """Normalize a cell for uniqueness equality matching the destination engine.

    CI collations / CITEXT / ``UNIQUE (lower(col))`` equate ``Abc`` and ``abc``;
    AI collations also equate ``cafe``/``café``; WI folds fullwidth forms.
    Trailing spaces follow PAD SPACE (CHAR; MySQL/SS VARCHAR) vs NO PAD
    (PostgreSQL/Oracle VARCHAR) — never invent collisions across engines.

    When ``null_sentinel`` is set (``NULLS NOT DISTINCT``), empty/null cells
    collide on that sentinel instead of being skipped.
    """
    if value is None:
        return null_sentinel or ""
    raw = str(value)
    pad_space = trailing_spaces_insignificant_for_unique(
        ddl_type, dest_kind=dest_kind
    )
    if pad_space:
        text = raw.rstrip(" ")
        if not text:
            return null_sentinel or ""
    else:
        # NO PAD: trailing spaces are significant; only pure-empty is null-like.
        if raw == "":
            return null_sentinel or ""
        text = raw
    if is_width_insensitive_collation(ddl_type):
        text = fold_width_forms(text)
    if is_kana_insensitive_collation(ddl_type):
        text = fold_kana(text)
    if is_variation_insensitive_collation(ddl_type):
        text = fold_variation_selectors(text)
    if is_accent_insensitive_collation(ddl_type):
        text = fold_diacritics(text)
    if force_casefold or is_case_insensitive_collation(ddl_type):
        text = text.casefold()
    return text


_CI_INDEX_EXPR_RE = re.compile(
    r"\b(?:lower|upper|casefold)\s*\(|::\s*citext\b|\bcitext\s*\("
    r"|\bnlssort\s*\(",
    re.I,
)
_CI_INDEX_COL_RE = re.compile(
    r"\b(?:lower|upper|casefold)\s*\(\s*\(?\s*\"?([A-Za-z_][\w$]*)\"?",
    re.I,
)
_NLSSORT_CI_COL_RE = re.compile(
    r"\bnlssort\s*\(\s*\"?([A-Za-z_][\w$]*)\"?\s*,\s*'[^']*BINARY_CI[^']*'",
    re.I,
)


def parse_case_insensitive_index_expression(expression: str | None) -> list[str]:
    """Return column names folded by ``lower``/``upper``/citext/NLSSORT BINARY_CI.

    Oracle function-based uniques often use
    ``NLSSORT(email, 'NLS_SORT=BINARY_CI')`` — must casefold like the engine.
    """
    text = (expression or "").strip()
    if not text or not _CI_INDEX_EXPR_RE.search(text):
        return []
    cols = [m.group(1) for m in _CI_INDEX_COL_RE.finditer(text)]
    for m in _NLSSORT_CI_COL_RE.finditer(text):
        col = m.group(1)
        if col not in cols:
            cols.append(col)
    return cols


def parse_temporal_fractional_precision(inferred: str | None) -> int | None:
    """Return fractional-second precision from TIME/TIMESTAMP/DATETIME2 carriers.

    Arrow / Avro spellings resolve to their carrier first (as
    :func:`datetime_timezone_polarity` does), so ``timestamp[ns]`` reports 9.
    Without this the nanosecond→microsecond clamp onto PostgreSQL looked like a
    no-op and the truncation was never surfaced to the operator.
    """
    text = strip_identity_qualifier(inferred)
    if not text:
        return None
    carrier = arrow_dtype_to_carrier(text) or avro_logical_token_to_carrier(text)
    if carrier is not None and carrier != text:
        text = carrier
    # DateTime64(3, 'UTC') — precision is the first arg (timezone may follow).
    m64 = re.search(r"DATETIME64\s*\(\s*(\d+)\s*(?:,|\))", text, re.I)
    if m64:
        return int(m64.group(1))
    m = re.search(
        r"(?:TIMESTAMP(?:_NTZ|_TZ|_LTZ)?|TIMESTAMPTZ|TIMETZ|DATETIME2?|DATETIMEOFFSET|TIME)"
        r"\s*\(\s*(\d+)\s*\)",
        text,
        re.I,
    )
    if not m:
        return None
    return int(m.group(1))


def temporal_precision_would_narrow(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
) -> bool:
    """True when source fractional seconds exceed destination TIME/TIMESTAMP(p).

    ``TIME(6)→TIME(0)`` / ``DATETIME2(7)→DATETIME2(0)`` silently truncates
    unless G3 blocks and write paths refuse inventing lower precision.

    ``SMALLDATETIME`` is one-minute accuracy — any second/fraction datetime
    source into SMALLDATETIME is a silent round (Microsoft / UGO class).

    Bare ``TIMESTAMP`` defaults are dialect-aware when ``dest_db`` is set:
    MySQL/Maria → FSP 0; PostgreSQL-family / Redshift → 6; SQL Server
    DATETIME2 bare → 7. Without ``dest_db``, bare TIMESTAMP stays fail-closed
    at 0 (MySQL) so truncation cannot silent-green.
    """
    src_l = normalize_logical_type(source_type)
    tgt_l = normalize_logical_type(target_type)
    if src_l not in {LOGICAL_TIME, LOGICAL_DATETIME} or tgt_l not in {
        LOGICAL_TIME,
        LOGICAL_DATETIME,
    }:
        return False
    tgt_u = strip_identity_qualifier(target_type).upper().strip()
    src_u = strip_identity_qualifier(source_type).upper().strip()
    if tgt_u == "SMALLDATETIME" and src_u != "SMALLDATETIME" and src_l == LOGICAL_DATETIME:
        return True
    tgt_p = parse_temporal_fractional_precision(target_type)
    src_p = parse_temporal_fractional_precision(source_type)
    if src_p is None:
        # SQL Server bare DATETIME2 defaults to precision 7 — never treat as
        # unknown and soft-pass DATETIME2→DATETIME (≈3.33ms round).
        bare_src = re.sub(r"\s*\(\s*\d+\s*\)", "", src_u).strip()
        if bare_src == "DATETIME2":
            src_p = 7
        elif bare_src == "DATETIMEOFFSET":
            src_p = 7
        else:
            return False
    if tgt_p is None:
        # MySQL bare TIME/DATETIME/TIMESTAMP default FSP 0 — TIME(6)→TIME must
        # not silent-green. PostgreSQL bare TIMESTAMP / TIMESTAMP WITHOUT TIME
        # ZONE defaults to precision 6 — TIMESTAMP_NTZ(6)→TIMESTAMP is preserve
        # on create-new PG, not a fractional collapse.
        bare = re.sub(r"\s*\(\s*\d+\s*\)", "", tgt_u).strip()
        if bare in {
            "TIME WITHOUT TIME ZONE",
            "TIMESTAMP WITHOUT TIME ZONE",
            "TIMESTAMP NTZ",
            "TIMESTAMPTZ",
            "TIMESTAMP WITH TIME ZONE",
            "TIMETZ",
            "TIME WITH TIME ZONE",
        }:
            # PostgreSQL / ANSI spellings default to precision 6.
            tgt_p = 6
        elif bare == "TIMESTAMP":
            db = (dest_db or "").strip().lower()
            if db in {
                "postgresql", "postgres", "pg", "redshift", "cockroach",
                "cockroachdb", "alloydb", "timescaledb", "yugabytedb",
                "citus", "supabase", "greenplum",
                # Lakehouse bare TIMESTAMP is microsecond (no typmod engines).
                "databricks", "spark", "delta", "delta_lake", "iceberg",
                "duckdb", "bigquery", "bq",
            }:
                tgt_p = 6
            elif db in {"sqlserver", "mssql"}:
                tgt_p = 7
            else:
                # MySQL/Maria/unknown: bare TIMESTAMP is FSP 0 — fail closed.
                tgt_p = 0
        elif bare == "DATETIME" or (
            bare.startswith("DATETIME") and bare not in {"DATETIME2", "DATETIMEOFFSET"}
        ):
            db = (dest_db or "").strip().lower()
            # BigQuery DATETIME is microsecond; MySQL/Maria bare DATETIME is FSP 0.
            if db in {"bigquery", "bq"}:
                tgt_p = 6
            else:
                tgt_p = 0
        elif bare in {"DATETIME2", "DATETIMEOFFSET"}:
            # SQL Server bare DATETIME2 / DATETIMEOFFSET default precision 7.
            tgt_p = 7
        elif bare == "TIME":
            db = (dest_db or "").strip().lower()
            if db in {
                "bigquery", "bq", "postgresql", "postgres", "pg", "redshift",
                "duckdb", "databricks", "spark", "delta", "iceberg",
                "cockroachdb", "alloydb", "timescaledb",
            }:
                tgt_p = 6
            elif db in {"sqlserver", "mssql"}:
                tgt_p = 7
            else:
                tgt_p = 0
        elif bare.startswith("DATETIME"):
            tgt_p = 0
        else:
            return False
    return src_p > tgt_p

def strip_identity_qualifier(inferred: str | None) -> str:
    """Remove GENERATED / AUTO_INCREMENT / COLLATE qualifiers for logical lookup."""
    text = strip_collation_qualifier(inferred)
    if not text:
        return ""
    text = re.sub(
        r"\s+GENERATED\s+ALWAYS(?:\s+AS\s+IDENTITY)?",
        "",
        text,
        flags=re.I,
    )
    text = re.sub(
        r"\s+GENERATED\s+BY\s+DEFAULT(?:\s+AS\s+IDENTITY)?",
        "",
        text,
        flags=re.I,
    )
    text = re.sub(r"\s+AUTO_INCREMENT\b", "", text, flags=re.I)
    return text.strip()


def is_year_carrier(inferred: str | None) -> bool:
    """True for MySQL YEAR / YEAR(4) carriers."""
    base = strip_identity_qualifier(inferred).upper()
    return base == "YEAR" or base.startswith("YEAR(")


def expand_mysql_year(value: Any) -> int | None:
    """Expand MySQL YEAR wire to a 4-digit / zero year (manual 8.4).

    Critical polarity: numeric ``0`` → ``0000``; string ``'0'``/``'00'`` → ``2000``.
    Two-digit ``1–69`` → ``2001–2069``; ``70–99`` → ``1970–1999``.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("YEAR cannot bind bool — refuse invent")
    if isinstance(value, int):
        n = value
        if n == 0:
            return 0
        if 1 <= n <= 69:
            return 2000 + n
        if 70 <= n <= 99:
            return 1900 + n
        return n
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError("YEAR float must be integral — refuse invent")
        return expand_mysql_year(int(value))
    text = str(value).strip()
    if not text:
        return None
    # String digits — including '0'/'00' → 2000 (not numeric-0 → 0000).
    if text.isdigit() or (text[0] in "+-" and text[1:].isdigit()):
        n = int(text)
        if text[0] == "-":
            raise ValueError("YEAR cannot be negative — refuse invent")
        if 0 <= n <= 99:
            return 2000 + n if n <= 69 else 1900 + n
        return n
    raise ValueError(f"YEAR wire is not numeric — refuse invent: {text[:32]!r}")


def year_value_fits(value: Any) -> bool:
    """MySQL YEAR valid set: 0 (→0000) or 1901–2155 (manual 8.4).

    Non-strict MySQL stores out-of-range as 0000 — silent wipe. Fail closed.
    Two-digit inputs are expanded with MySQL rules before the range check.
    """
    if value is None:
        return True
    try:
        if isinstance(value, str) and not value.strip():
            return True
        n = expand_mysql_year(value)
    except (ValueError, TypeError, OverflowError):
        return False
    if n is None:
        return True
    return n == 0 or 1901 <= n <= 2155


def is_money_carrier(inferred: str | None) -> bool:
    """True for MONEY / SMALLMONEY / currency-scale DECIMAL(19,4)/(10,4) tokens."""
    base = strip_identity_qualifier(inferred).upper().replace(" ", "")
    if base in {"MONEY", "SMALLMONEY", "CURRENCY"}:
        return True
    if base in {"DECIMAL(19,4)", "NUMERIC(19,4)", "NUMBER(19,4)"}:
        return True
    if base in {"DECIMAL(10,4)", "NUMERIC(10,4)", "NUMBER(10,4)"}:
        return True
    return False


def has_currency_marker(value: Any) -> bool:
    """True when a cell still carries a currency symbol/code (not pure numeric)."""
    if value is None or isinstance(value, (int, float, bool)):
        return False
    try:
        from decimal import Decimal

        if isinstance(value, Decimal):
            return False
    except Exception:
        pass
    text = str(value).strip()
    if not text:
        return False
    try:
        from services.transform_engine import _CURRENCY_RE

        return bool(_CURRENCY_RE.search(text))
    except Exception:
        return bool(re.search(r"[$€£¥₹]", text))


# Canonical boolean wire tokens — refuse inventing TRUE from "yes"/"Y"/2.
_BOOLEAN_TRUE_TOKENS: Final[frozenset[str]] = frozenset({"1", "true", "t"})
_BOOLEAN_FALSE_TOKENS: Final[frozenset[str]] = frozenset({"0", "false", "f"})


def boolean_value_fits(value: Any) -> bool:
    """True when value is a canonical boolean wire form (bool / 0|1 / true|false|t|f).

    MySQL TINYINT(1) and PG BOOLEAN accept a wider informal set; Airbyte-style
    silent ``'yes'→true`` invents truth. Fail closed — operator must transform.
    """
    if value is None:
        return True
    if isinstance(value, bool):
        return True
    if isinstance(value, int) and not isinstance(value, bool):
        return value in (0, 1)
    if isinstance(value, float):
        return value in (0.0, 1.0)
    try:
        from decimal import Decimal

        if isinstance(value, Decimal):
            return value in (Decimal(0), Decimal(1))
    except Exception:
        pass
    if isinstance(value, (bytes, bytearray)):
        try:
            text = bytes(value).decode("utf-8").strip().lower()
        except UnicodeDecodeError:
            return False
    else:
        text = str(value).strip().lower()
    if not text:
        return True
    return text in _BOOLEAN_TRUE_TOKENS or text in _BOOLEAN_FALSE_TOKENS


def value_fractional_second_digits(value: Any) -> int:
    """Count fractional-second digits present on a temporal cell (0 if none)."""
    if value is None:
        return 0
    # datetime/time objects
    try:
        from datetime import date, datetime, time

        if isinstance(value, datetime):
            us = value.microsecond
            if us == 0:
                return 0
            return len(f"{us:06d}".rstrip("0"))
        if isinstance(value, time):
            us = value.microsecond
            if us == 0:
                return 0
            return len(f"{us:06d}".rstrip("0"))
        if isinstance(value, date):
            return 0
    except Exception:
        pass
    text = str(value).strip()
    if not text:
        return 0
    # ISO / SQL: …HH:MM:SS.ffffff[+/-offset]
    m = re.search(r"\.(\d+)", text)
    if not m:
        return 0
    return len(m.group(1).rstrip("0")) or 0


def temporal_value_exceeds_precision(value: Any, target_type: str) -> bool:
    """True when cell fractional seconds exceed destination TIME/TIMESTAMP(p)."""
    tgt_p = parse_temporal_fractional_precision(target_type)
    if tgt_p is None:
        return False
    digits = value_fractional_second_digits(value)
    return digits > tgt_p


def is_generated_always_column(inferred: str | None) -> bool:
    """True when destination forbids client-supplied values (GENERATED ALWAYS)."""
    return bool(re.search(r"\bGENERATED\s+ALWAYS\b", (inferred or ""), re.I))


def is_identity_column(inferred: str | None) -> bool:
    """True for SERIAL / IDENTITY / AUTO_INCREMENT carriers."""
    upper = (inferred or "").upper()
    if not upper:
        return False
    if is_generated_always_column(inferred):
        return True
    if "GENERATED BY DEFAULT" in upper or "AUTO_INCREMENT" in upper:
        return True
    # SQL Server IDENTITY(1,1) — must match before strip_identity_qualifier removes it.
    if re.search(r"\bIDENTITY\b", upper):
        return True
    base = strip_identity_qualifier(inferred).upper()
    return base in {"SERIAL", "BIGSERIAL", "SMALLSERIAL"} or base.startswith("SERIAL")


def generated_always_overwrite_risk(target_type: str) -> bool:
    """Mapping into GENERATED ALWAYS invents overwrite / mid-batch insert failure."""
    return is_generated_always_column(target_type)


def identity_polarity_would_collapse(source_type: str, target_type: str) -> bool:
    """True when SERIAL/IDENTITY/AUTO_INCREMENT polarity is dropped on the sink.

    Create-new that stamps plain BIGINT while the source was GENERATED ALWAYS
    invents a writable numeric column — Accept risk (or stamp identity DDL).
    """
    if not is_identity_column(source_type):
        return False
    if is_identity_column(target_type):
        return False
    tgt = normalize_logical_type(target_type)
    return tgt in {
        LOGICAL_INTEGER,
        LOGICAL_DECIMAL,
        LOGICAL_FLOAT,
        LOGICAL_STRING,
        LOGICAL_TEXT,
        LOGICAL_JSON,
    }


def identity_domain_would_invent(source_type: str, target_type: str) -> bool:
    """True when plain scalar invents SERIAL/IDENTITY/AUTO_INCREMENT polarity."""
    if is_identity_column(source_type):
        return False
    if not is_identity_column(target_type):
        return False
    src = normalize_logical_type(source_type)
    return src in {
        LOGICAL_INTEGER,
        LOGICAL_DECIMAL,
        LOGICAL_FLOAT,
        LOGICAL_STRING,
        LOGICAL_TEXT,
        LOGICAL_JSON,
    }


def is_bfile_locator(inferred: str | None) -> bool:
    """True for Oracle BFILE external locator (not inlined LOB bytes)."""
    upper = strip_identity_qualifier(inferred).upper().strip()
    return upper == "BFILE" or upper.startswith("BFILE(")


def bfile_locator_would_collapse(source_type: str, target_type: str) -> bool:
    """True when BFILE locator polarity would be lost into BLOB/BYTES/text."""
    if not is_bfile_locator(source_type):
        return False
    if is_bfile_locator(target_type):
        return False
    return True


def integer_bit_width(inferred: str | None) -> int | None:
    """Signed bit width; UNSIGNED adds +1 so INT UNSIGNED is wider than INT."""
    raw = strip_identity_qualifier(inferred)
    if not raw:
        return None
    # ClickHouse Int8/UInt8/… — case-sensitive; must not collide with PG INT8≡BIGINT.
    m_ch = re.match(r"^(U?Int)(8|16|32|64)\b", raw)
    if m_ch:
        bits = int(m_ch.group(2))
        return bits + 1 if m_ch.group(1).startswith("U") else bits
    upper = raw.upper()
    unsigned = "UNSIGNED" in upper or bool(
        re.search(r"\bUINT\d*\b", upper)
    )
    base: int | None = None
    # Explicit widths before bare INT — INT64/LONG must not miss \\bINT\\b.
    if (
        "BIGSERIAL" in upper
        or "BIGINT" in upper
        or re.search(r"\bINT64\b", upper)
        or re.search(r"\bUINT64\b", upper)
        or re.search(r"\bLONG\b", upper)
        or re.search(r"\bINT8\b", upper)  # PostgreSQL INT8 ≡ BIGINT
    ):
        base = 64
    elif "MEDIUMINT" in upper:
        base = 24
    elif (
        "SMALLSERIAL" in upper
        or "SMALLINT" in upper
        or re.search(r"\bINT16\b", upper)
        or re.search(r"\bINT2\b", upper)
        or re.search(r"\bUINT16\b", upper)
        or re.search(r"\bSHORT\b", upper)
    ):
        base = 16
    elif (
        "TINYSERIAL" in upper
        or "TINYINT" in upper
        or re.search(r"\bINT1\b", upper)
        or re.search(r"\bUINT8\b", upper)
    ):
        base = 8
    elif (
        "SERIAL" in upper
        or "INTEGER" in upper
        or re.search(r"\bINT32\b", upper)
        or re.search(r"\bINT4\b", upper)
        or re.search(r"\bUINT32\b", upper)
    ):
        base = 32
    elif re.search(r"\bINT\b", upper):
        base = 32
    if base is None:
        return None
    if unsigned:
        # Unsigned same nominal width holds larger max → needs signed width+1
        # (e.g. INT UNSIGNED → 33 so BIGINT is a valid widen).
        return base + 1
    return base


def integer_storage_bounds(inferred: str | None) -> tuple[int, int] | None:
    """Inclusive (lo, hi) for a signed/unsigned integer carrier, else None.

    Uses ``integer_bit_width`` (UNSIGNED bumped +1) so INT UNSIGNED →
    ``(0, 2**32-1)`` and signed INT → ``(-(2**31), 2**31-1)``.
    """
    if normalize_logical_type(inferred) != LOGICAL_INTEGER:
        return None
    upper = (inferred or "").upper()
    unsigned = "UNSIGNED" in upper or bool(re.search(r"\bUINT\d*\b", upper))
    width = integer_bit_width(inferred)
    if width is None:
        width = 32
        unsigned = False
    if unsigned:
        nominal = max(1, width - 1)
        return (0, (1 << nominal) - 1)
    return (-(1 << (width - 1)), (1 << (width - 1)) - 1)


def integer_width_would_narrow(source_type: str, target_type: str) -> bool:
    """True when signed/unsigned integer bit width shrinks (BIGINT→INT invent)."""
    if normalize_logical_type(source_type) != LOGICAL_INTEGER:
        return False
    if normalize_logical_type(target_type) != LOGICAL_INTEGER:
        return False
    src_w = integer_bit_width(source_type)
    tgt_w = integer_bit_width(target_type)
    if src_w is None or tgt_w is None:
        return False
    return src_w > tgt_w


def float_mantissa_bits(inferred: str | None, *, dest_db: str = "") -> int | None:
    """IEEE significand bits for float carriers (53=double, 24=single, 11=half).

    Bare ``FLOAT`` is dialect-dependent: SQL Server / Snowflake FLOAT is IEEE-64;
    MySQL FLOAT is IEEE-32. When ``dest_db`` is known, use the dialect default so
    DOUBLE→FLOAT create-new on those engines is not a false mantissa collapse.
    """
    if normalize_logical_type(inferred) != LOGICAL_FLOAT:
        return None
    # Strip UNSIGNED so REAL UNSIGNED / FLOAT UNSIGNED keep single-width tokens.
    upper = re.sub(
        r"\bUNSIGNED\b",
        "",
        strip_identity_qualifier(inferred).upper(),
    ).strip().replace(" ", "")
    # IEEE half / float16 (~10 explicit + 1 implicit significand bits).
    if upper in {"HALF", "HALFFLOAT", "FLOAT16"} or upper.startswith("HALFFLOAT"):
        return 11
    # Single-precision tokens.
    if upper in {
        "REAL",
        "FLOAT4",
        "FLOAT32",
        "BINARY_FLOAT",
    } or upper.startswith("REAL("):
        return 24
    # SQL FLOAT(p): p≤24 → single; p>24 → double (SQL Server / ANSI).
    m = re.match(r"^FLOAT\((\d+)\)$", upper)
    if m:
        return 24 if int(m.group(1)) <= 24 else 53
    if upper in {
        "DOUBLE",
        "DOUBLEPRECISION",
        "FLOAT8",
        "FLOAT64",
        "BINARY_DOUBLE",
    } or upper.startswith("DOUBLE"):
        return 53
    # Bare FLOAT is dialect-dependent.
    if upper == "FLOAT" or upper.startswith("FLOAT"):
        db = (dest_db or "").strip().lower()
        if db in {"sqlserver", "mssql", "snowflake"}:
            return 53
        # Fail-closed single so DOUBLE→FLOAT never silent-greens without dest.
        return 24
    return 53


def float_mantissa_would_narrow(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
) -> bool:
    """True when DOUBLE/FLOAT64 lands on REAL/FLOAT32/HALF (silent IEEE drop)."""
    src_b = float_mantissa_bits(source_type, dest_db=dest_db)
    tgt_b = float_mantissa_bits(target_type, dest_db=dest_db)
    if src_b is None or tgt_b is None:
        return False
    return src_b > tgt_b


def specialty_polarity_mismatch(source_type: str, target_type: str) -> bool:
    """True when two distinct specialty carriers would rewrite domain polarity.

    INET→CIDR invents network masking; MACADDR→MACADDR8 changes wire width;
    HSTORE→LTREE is not identity. IP/INET/IPv4/IPv6 are host-address twins.
    """
    src = specialty_carrier_base(source_type)
    tgt = specialty_carrier_base(target_type)
    if src is None or tgt is None:
        return False
    if src in _IP_HOST_ADDRESS_TWINS and tgt in _IP_HOST_ADDRESS_TWINS:
        return False
    if src in _XML_DOCUMENT_TWINS and tgt in _XML_DOCUMENT_TWINS:
        return False
    return src != tgt


def case_fold_polarity_invent(source_type: str, target_type: str) -> bool:
    """True when mapping invents or drops case-fold equality polarity.

    Covers TEXT→CITEXT / CS→CI invent and CITEXT→TEXT / explicit CS drop.
    MySQL/SQL Server CI collation metadata → bare create-new TEXT/VARCHAR is
    dialect normalization (values round-trip); uniqueness follows the destination
    platform default and must not block every string column on Validate.
    """
    src_l = normalize_logical_type(source_type)
    tgt_l = normalize_logical_type(target_type)
    if src_l not in {LOGICAL_STRING, LOGICAL_TEXT} or tgt_l not in {
        LOGICAL_STRING,
        LOGICAL_TEXT,
    }:
        return False
    tgt_ci = is_case_insensitive_collation(target_type) or (
        specialty_carrier_base(target_type) == "CITEXT"
    )
    src_ci = is_case_insensitive_collation(source_type) or (
        specialty_carrier_base(source_type) == "CITEXT"
    )
    # Only when at least one side declares CI/CS polarity (or CITEXT).
    src_declares = bool(parse_collation(source_type)) or (
        specialty_carrier_base(source_type) == "CITEXT"
    )
    tgt_declares = bool(parse_collation(target_type)) or (
        specialty_carrier_base(target_type) == "CITEXT"
    )
    if not src_declares and not tgt_declares:
        return False
    # Inventing CI / CITEXT on the target from a CS source.
    if tgt_ci and not src_ci:
        return True
    # Dropping CITEXT specialty into bare CS text — real polarity loss.
    if specialty_carrier_base(source_type) == "CITEXT" and not tgt_ci:
        return True
    # Explicit CS (or non-CI) collation on target vs CI source.
    if src_ci and tgt_declares and not tgt_ci:
        return True
    # Source CI collation metadata + bare destination TEXT/VARCHAR: normalize.
    return False

def accent_polarity_invent(source_type: str, target_type: str) -> bool:
    """True when mapping invents or explicitly drops accent-insensitive equality.

    Source AI collation → bare create-new TEXT is dialect strip, not invent.
    """
    src_l = normalize_logical_type(source_type)
    tgt_l = normalize_logical_type(target_type)
    if src_l not in {LOGICAL_STRING, LOGICAL_TEXT} or tgt_l not in {
        LOGICAL_STRING,
        LOGICAL_TEXT,
    }:
        return False
    if not parse_collation(source_type) and not parse_collation(target_type):
        return False
    src_ai = is_accent_insensitive_collation(source_type)
    tgt_ai = is_accent_insensitive_collation(target_type)
    # Invent AI on an explicitly collated target.
    if tgt_ai and not src_ai:
        return True
    # Drop AI only when the target declares an accent-sensitive collation.
    if src_ai and not tgt_ai and parse_collation(target_type):
        return True
    return False

def width_fold_polarity_invent(source_type: str, target_type: str) -> bool:
    """True when mapping invents/drops width-insensitive equality (WS omit).

    SQL Server: ``_WS`` is width-sensitive; omitting ``_WS`` folds fullwidth/
    halfwidth — unique keys can collide without Accept risk.
    """
    src_l = normalize_logical_type(source_type)
    tgt_l = normalize_logical_type(target_type)
    if src_l not in {LOGICAL_STRING, LOGICAL_TEXT} or tgt_l not in {
        LOGICAL_STRING,
        LOGICAL_TEXT,
    }:
        return False
    if not parse_collation(source_type) or not parse_collation(target_type):
        return False
    src_name = (parse_collation(source_type) or "").upper()
    tgt_name = (parse_collation(target_type) or "").upper()
    if not _is_windows_style_collation(src_name) and not _is_windows_style_collation(
        tgt_name
    ):
        return False
    return is_width_insensitive_collation(source_type) != is_width_insensitive_collation(
        target_type
    )


def kana_fold_polarity_invent(source_type: str, target_type: str) -> bool:
    """True when mapping invents/drops kana-insensitive equality (KS omit)."""
    src_l = normalize_logical_type(source_type)
    tgt_l = normalize_logical_type(target_type)
    if src_l not in {LOGICAL_STRING, LOGICAL_TEXT} or tgt_l not in {
        LOGICAL_STRING,
        LOGICAL_TEXT,
    }:
        return False
    if not parse_collation(source_type) or not parse_collation(target_type):
        return False
    src_name = (parse_collation(source_type) or "").upper()
    tgt_name = (parse_collation(target_type) or "").upper()
    if not _is_windows_style_collation(src_name) and not _is_windows_style_collation(
        tgt_name
    ):
        return False
    return is_kana_insensitive_collation(source_type) != is_kana_insensitive_collation(
        target_type
    )


def date_to_tz_aware_invent(source_type: str, target_type: str) -> bool:
    """True when DATE widens into TZ-aware datetime (midnight instant invent)."""
    if normalize_logical_type(source_type) != LOGICAL_DATE:
        return False
    if normalize_logical_type(target_type) != LOGICAL_DATETIME:
        return False
    return datetime_timezone_polarity(target_type) in {"tz", "ltz"}


def unsigned_integer_would_overflow(source_type: str, target_type: str) -> bool:
    """True when UNSIGNED source max can exceed a signed (or narrower) integer dest.

    Fivetran/Airbyte class: BIGINT UNSIGNED → signed BIGINT is silent overflow
    unless widened to DECIMAL/NUMBER. INT UNSIGNED → INT is the same class.
    """
    src_raw = (source_type or "").lower()
    if "unsigned" not in src_raw and not re.search(r"\buint\d*\b", src_raw):
        # ClickHouse UInt8/UInt32 — case-sensitive leading U.
        raw = strip_identity_qualifier(source_type) or ""
        if not re.match(r"^UInt(8|16|32|64)\b", raw):
            return False
    tgt_l = normalize_logical_type(target_type)
    # Lossless sinks for unsigned range.
    if tgt_l in {LOGICAL_DECIMAL, LOGICAL_STRING, LOGICAL_TEXT, LOGICAL_JSON}:
        return False
    if tgt_l == LOGICAL_FLOAT:
        # IEEE cannot represent full UINT64 / large UINT32 exactly.
        src_w = integer_bit_width(source_type)
        return src_w is not None and src_w > 24
    if tgt_l != LOGICAL_INTEGER:
        return False
    src_w = integer_bit_width(source_type)
    tgt_w = integer_bit_width(target_type)
    if src_w is None:
        # Unknown unsigned width into signed integer — fail closed.
        return "unsigned" not in (target_type or "").lower() and not re.match(
            r"^UInt", strip_identity_qualifier(target_type) or ""
        )
    if tgt_w is None:
        tgt_w = 32  # bare INTEGER / INT
    return src_w > tgt_w


def _is_unsigned_integer_carrier(inferred: str | None) -> bool:
    """True for MySQL UNSIGNED / ClickHouse UInt* / UINT* integer carriers."""
    raw = strip_identity_qualifier(inferred) or ""
    if re.match(r"^UInt(8|16|32|64)\b", raw):
        return True
    lower = raw.lower()
    return "unsigned" in lower or bool(re.search(r"\buint\d*\b", lower))


def unsigned_signed_polarity_invent(source_type: str, target_type: str) -> bool:
    """True when UNSIGNED/UInt invents a signed integer sink (or reverse).

    Value-safe *clear* widens (``INT UNSIGNED → BIGINT``) do not invent
    Accept-risk theater — the signed 64-bit sink holds the full unsigned 32-bit
    range. Tighter signed sinks (``UInt8 → SMALLINT``) still drop unsigned
    polarity and require Accept risk even when samples fit. Overflow stays
    blocked by :func:`unsigned_integer_would_overflow`.
    """
    if normalize_logical_type(source_type) != LOGICAL_INTEGER:
        return False
    if normalize_logical_type(target_type) != LOGICAL_INTEGER:
        return False
    src_u = _is_unsigned_integer_carrier(source_type)
    tgt_u = _is_unsigned_integer_carrier(target_type)
    if src_u == tgt_u:
        return False
    # INT / INT UNSIGNED → BIGINT class: full range fits in signed 64-bit.
    if src_u and not tgt_u and not unsigned_integer_would_overflow(source_type, target_type):
        src_w = integer_bit_width(source_type)
        tgt_w = integer_bit_width(target_type)
        if src_w is not None and tgt_w is not None and tgt_w >= 64 and src_w <= 33:
            return False
    return True


def is_precision_collapse_coercion(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
) -> bool:
    """True when source→target collapses precision even if samples appear clean.

    Used by DDL compatibility, schema-drift, G3, and integrity sample soft-pass so
    Airbyte-style "head sample coerced → green" never hides float→decimal /
    datetime→date / timestamptz→timestamp_ntz / DECIMAL(p,s) / VARCHAR width /
    BINARY width / UNSIGNED→signed overflow / ENUM·SET domain shrink /
    INTERVAL YM↔DS / GEOGRAPHY SRID·polarity / GENERATED ALWAYS overwrite /
    TIME·TIMESTAMP fractional-second (p) narrow.

    ``dest_db`` threads dialect defaults (document wires, bare TIMESTAMP FSP).
    """
    dest_db = _normalize_dest_db(dest_db) if dest_db else ""
    src = normalize_logical_type(source_type)
    tgt = normalize_logical_type(target_type)
    if (src, tgt) in PRECISION_COLLAPSE_PAIRS:
        return True
    if is_timezone_polarity_loss(source_type, target_type, dest_db=dest_db):
        return True
    if time_timezone_polarity_loss(source_type, target_type):
        return True
    if timezone_aware_would_collapse_to_string(source_type, target_type):
        return True
    if long_raw_locator_would_collapse(source_type, target_type):
        return True
    if decfloat_domain_would_collapse(source_type, target_type):
        return True
    if bignumeric_capacity_would_invent(source_type, target_type):
        return True
    if decimal_fixed_point_would_collapse_to_text(source_type, target_type):
        return True
    if smalldatetime_domain_would_invent(source_type, target_type):
        return True
    if decimal_params_would_narrow(source_type, target_type):
        return True
    if string_width_would_narrow(source_type, target_type):
        return True
    if bounded_string_sink_would_truncate(source_type, target_type):
        return True
    if national_charset_would_collapse(source_type, target_type):
        return True
    if national_charset_would_invent(source_type, target_type):
        return True
    if fixed_width_pad_polarity_loss(source_type, target_type, dest_db=dest_db):
        return True
    if binary_width_would_narrow(source_type, target_type):
        return True
    if bitstring_width_would_narrow(source_type, target_type):
        return True
    if unsigned_integer_would_overflow(source_type, target_type):
        return True
    if unsigned_signed_polarity_invent(source_type, target_type):
        return True
    if integer_width_would_narrow(source_type, target_type):
        return True
    if float_mantissa_would_narrow(source_type, target_type, dest_db=dest_db):
        return True
    if year_domain_would_collapse(source_type, target_type):
        return True
    if money_domain_would_collapse(source_type, target_type):
        return True
    if bitstring_opaque_bytes_collapse(source_type, target_type):
        return True
    if enum_set_domain_would_reject(source_type, target_type):
        return True
    if enum_domain_would_collapse(source_type, target_type):
        return True
    if interval_family_would_collapse(source_type, target_type, dest_db=dest_db):
        return True
    if interval_precision_would_narrow(source_type, target_type):
        return True
    if bitstring_pad_polarity_loss(source_type, target_type):
        return True
    if oracle_char_byte_polarity_loss(source_type, target_type):
        return True
    if oracle_long_numeric_invent(source_type, target_type):
        return True
    if geography_contract_would_collapse(source_type, target_type):
        return True
    if specialty_carrier_would_collapse(source_type, target_type, dest_db=dest_db):
        return True
    if specialty_domain_would_invent(source_type, target_type):
        return True
    if specialty_polarity_mismatch(source_type, target_type):
        return True
    if case_fold_polarity_invent(source_type, target_type):
        return True
    if accent_polarity_invent(source_type, target_type):
        return True
    if width_fold_polarity_invent(source_type, target_type):
        return True
    if kana_fold_polarity_invent(source_type, target_type):
        return True
    if date_to_tz_aware_invent(source_type, target_type):
        return True
    if vector_dim_mismatch(source_type, target_type):
        return True
    if vector_encoding_would_collapse(source_type, target_type):
        return True
    if rowversion_would_collapse_to_temporal(source_type, target_type):
        return True
    if sql_variant_would_collapse(source_type, target_type):
        return True
    # ObjectId → VARCHAR(24)/BINARY(12) is the industry create-new wire — not lossy.
    if (
        normalize_logical_type(source_type) == LOGICAL_OBJECTID
        and specialty_wire_preserves_value("OBJECTID", target_type)
    ):
        return False
    if uuid_would_collapse(source_type, target_type):
        return True
    if objectid_would_collapse(source_type, target_type):
        return True
    if generated_always_overwrite_risk(target_type):
        return True
    if identity_polarity_would_collapse(source_type, target_type):
        return True
    if identity_domain_would_invent(source_type, target_type):
        return True
    if bfile_locator_would_collapse(source_type, target_type):
        return True
    if document_domain_would_collapse(source_type, target_type, dest_db=dest_db):
        return True
    if document_domain_would_invent(source_type, target_type):
        return True
    if temporal_precision_would_narrow(source_type, target_type, dest_db=dest_db):
        return True
    return False




def suggest_remap_target(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
) -> str:
    """One-click Remap target that preserves fidelity — never invent bare VARCHAR.

    SSOT for Validate / agentic repair CTAs. Prefer create-new physical DDL for
    the destination; specialty / UUID keep native carriers; same-logical twins
    keep the destination stamp.
    """
    db = (dest_db or "").strip() or "postgresql"
    src_l = normalize_logical_type(source_type)
    tgt_l = normalize_logical_type(target_type)
    specialty = specialty_carrier_base(source_type)
    if src_l == LOGICAL_UUID and tgt_l in {LOGICAL_STRING, LOGICAL_TEXT, LOGICAL_JSON}:
        return create_new_mapping_target_type(source_type, db)
    if specialty and tgt_l in {LOGICAL_STRING, LOGICAL_TEXT, LOGICAL_JSON}:
        # Prefer create-new wire (ObjectId→VARCHAR(24)) over raw specialty token
        # when the destination has no native carrier.
        stamped = create_new_mapping_target_type(source_type, db)
        return stamped or specialty
    if src_l == tgt_l and src_l in {
        LOGICAL_STRING,
        LOGICAL_TEXT,
        LOGICAL_JSON,
        LOGICAL_DATETIME,
        LOGICAL_DATE,
        LOGICAL_TIME,
    }:
        stamped = (target_type or "").strip()
        if stamped and "COLLATE" not in stamped.upper():
            return promote_create_new_temporal_stamp(source_type, stamped, db)
        return create_new_mapping_target_type(source_type, db) or stamped or "TEXT"
    if src_l == LOGICAL_FLOAT and tgt_l in {LOGICAL_DECIMAL, LOGICAL_INTEGER}:
        return create_new_mapping_target_type("DOUBLE", db) or "DOUBLE"
    if src_l == LOGICAL_DECIMAL and tgt_l in {LOGICAL_INTEGER, LOGICAL_FLOAT}:
        return source_type.strip() or create_new_mapping_target_type(source_type, db)
    if src_l in {LOGICAL_DATETIME, LOGICAL_DATE, LOGICAL_TIME} and tgt_l in {
        LOGICAL_DATETIME,
        LOGICAL_DATE,
        LOGICAL_TIME,
        LOGICAL_STRING,
        LOGICAL_TEXT,
    }:
        return create_new_mapping_target_type(source_type, db) or source_type.strip()
    if src_l in {LOGICAL_STRING, LOGICAL_TEXT} and tgt_l in {
        LOGICAL_INTEGER,
        LOGICAL_DECIMAL,
        LOGICAL_FLOAT,
    }:
        return create_new_mapping_target_type("TEXT", db) or "TEXT"
    # Last resort: destination text sink — still dialect-aware, never bare VARCHAR invent.
    return create_new_mapping_target_type("TEXT", db) or "TEXT"


def assess_bson_affinity(
    source_type: str,
    target_type: str,
    *,
    destination_db_type: str = "",
) -> list[dict]:
    """Schemaless G3 semantic contract — type affinity when DDL is absent.

    MongoDB / DynamoDB / Redis do not enforce column DDL, but declared (or
    inferred) carriers still have BSON/AttributeValue affinity. ObjectId↛NUMBER
    and DECIMAL↛INTEGER must surface before write — never silent SKIP-as-green.
    """
    risks: list[dict] = []
    src = normalize_logical_type(source_type)
    tgt = normalize_logical_type(target_type) if (target_type or "").strip() else src
    db = (destination_db_type or "").strip().lower()

    hard = {
        (LOGICAL_OBJECTID, LOGICAL_INTEGER),
        (LOGICAL_OBJECTID, LOGICAL_DECIMAL),
        (LOGICAL_OBJECTID, LOGICAL_FLOAT),
        (LOGICAL_OBJECTID, LOGICAL_BOOLEAN),
        (LOGICAL_UUID, LOGICAL_INTEGER),
        (LOGICAL_UUID, LOGICAL_DECIMAL),
        (LOGICAL_BINARY, LOGICAL_INTEGER),
        (LOGICAL_BINARY, LOGICAL_DECIMAL),
        (LOGICAL_JSON, LOGICAL_INTEGER),
        (LOGICAL_JSON, LOGICAL_DECIMAL),
        (LOGICAL_STRUCT, LOGICAL_INTEGER),
        (LOGICAL_ARRAY, LOGICAL_INTEGER),
    }
    if (src, tgt) in hard:
        risks.append({
            "kind": "bson_affinity_block",
            "severity": "block",
            "message": (
                f"Schemaless affinity: {source_type or src} → {target_type or tgt}"
                + (f" on {db}" if db else "")
                + " invents a numeric/scalar domain the source never had."
            ),
        })
        return risks

    soft = {
        (LOGICAL_DECIMAL, LOGICAL_INTEGER),
        (LOGICAL_FLOAT, LOGICAL_INTEGER),
        (LOGICAL_FLOAT, LOGICAL_DECIMAL),
        (LOGICAL_DECIMAL, LOGICAL_FLOAT),
        (LOGICAL_DATETIME, LOGICAL_DATE),
        (LOGICAL_STRING, LOGICAL_INTEGER),
        (LOGICAL_STRING, LOGICAL_DECIMAL),
        (LOGICAL_STRING, LOGICAL_BOOLEAN),
        (LOGICAL_TEXT, LOGICAL_INTEGER),
        (LOGICAL_TEXT, LOGICAL_DECIMAL),
        # Specialty / domain → open string invents no validation at schemaless sinks.
        (LOGICAL_UUID, LOGICAL_STRING),
        (LOGICAL_UUID, LOGICAL_TEXT),
        (LOGICAL_OBJECTID, LOGICAL_STRING),
        (LOGICAL_OBJECTID, LOGICAL_TEXT),
        (LOGICAL_BINARY, LOGICAL_STRING),
        (LOGICAL_BINARY, LOGICAL_TEXT),
        (LOGICAL_JSON, LOGICAL_STRING),
        (LOGICAL_JSON, LOGICAL_TEXT),
        (LOGICAL_VECTOR, LOGICAL_ARRAY),
        (LOGICAL_VECTOR, LOGICAL_STRING),
        (LOGICAL_VECTOR, LOGICAL_TEXT),
    }
    specialty_to_open = (
        specialty_carrier_base(source_type) is not None
        and normalize_logical_type(target_type or "") in {
            LOGICAL_STRING, LOGICAL_TEXT, LOGICAL_JSON, LOGICAL_ARRAY,
        }
        and specialty_carrier_base(target_type) is None
        and not specialty_wire_preserves_value(
            specialty_carrier_base(source_type) or "", target_type or ""
        )
    )
    if (
        (src, tgt) in soft
        or objectid_would_collapse(source_type, target_type or source_type)
        or specialty_to_open
    ):
        risks.append({
            "kind": "bson_affinity_warn",
            "severity": "warn",
            "message": (
                f"Schemaless affinity risk: {source_type or src} → {target_type or tgt}"
                + (f" on {db}" if db else "")
                + ". No DDL contract — confirm samples before write."
            ),
        })
    return risks


def assess_create_new_type_risk(
    source_type: str,
    target_type: str,
    *,
    destination_db_type: str = "",
) -> list[dict]:
    """Risk chips for create-new columns before Validate/write.

    Returns a list of ``{kind, severity, message}`` — empty when lossless.
    """
    risks: list[dict] = []
    src = (source_type or "").strip() or "VARCHAR"
    tgt = (target_type or "").strip() or src
    db = (destination_db_type or "").strip().lower()

    if is_precision_collapse_coercion(src, tgt, dest_db=db):
        risks.append({
            "kind": "precision_collapse",
            "severity": "warn",
            "message": (
                f"Create-new {src} → {tgt} collapses precision"
                + (f" on {db}" if db else "")
                + ". Review before execute."
            ),
        })
    elif is_lossy_coercion(src, tgt, dest_db=db):
        risks.append({
            "kind": "lossy_coercion",
            "severity": "warn",
            "message": (
                f"Create-new {src} → {tgt} may lose information"
                + (f" on {db}" if db else "")
                + "."
            ),
        })

    # Dialect VARCHAR capacity ceilings that create-new often hit.
    src_w = parse_string_carrier_width(src)
    tgt_w = parse_string_carrier_width(tgt)
    dialect_cap = {
        "oracle": 4000,
        "sqlserver": 8000,
        "mssql": 8000,
        "redshift": 65535,
        "mysql": 16383,  # utf8mb4 InnoDB row practical ceiling for VARCHAR
    }.get(db)
    if dialect_cap and (src_w or 0) > dialect_cap and (not tgt_w or tgt_w >= dialect_cap):
        risks.append({
            "kind": "varchar_width_cap",
            "severity": "warn",
            "message": (
                f"Source width {src_w} exceeds {db} VARCHAR capacity (~{dialect_cap}). "
                "Create-new may truncate unless you use CLOB/TEXT."
            ),
        })
    if src_w and tgt_w and tgt_w < src_w:
        risks.append({
            "kind": "varchar_narrow",
            "severity": "warn",
            "message": f"Create-new narrows VARCHAR({src_w}) → VARCHAR({tgt_w}).",
        })

    if is_timezone_polarity_loss(src, tgt, dest_db=db) or time_timezone_polarity_loss(src, tgt):
        risks.append({
            "kind": "timezone_polarity",
            "severity": "warn",
            "message": f"Create-new drops timezone polarity: {src} → {tgt}.",
        })
    if uuid_would_collapse(src, tgt):
        risks.append({
            "kind": "uuid_domain",
            "severity": "warn",
            "message": (
                f"Create-new stores UUID as {tgt}"
                + (f" on {db}" if db else "")
                + " — UUID domain is not enforced at destination."
            ),
        })
    elif (
        normalize_logical_type(src) == LOGICAL_UUID
        and normalize_logical_type(tgt) != LOGICAL_UUID
        and uuid_exact_wire_carrier(tgt)
    ):
        # Exact CHAR/VARCHAR(36) wire preserves values but invents no UUID type —
        # Accept risk so Map never looks UUID→UUID while CREATE emits string DDL.
        risks.append({
            "kind": "uuid_domain",
            "severity": "warn",
            "message": (
                f"Create-new stores UUID as {tgt}"
                + (f" on {db}" if db else "")
                + " — exact 36-char wire; UUID domain is not enforced at destination."
            ),
        })
    if objectid_would_collapse(src, tgt):
        risks.append({
            "kind": "objectid_domain",
            "severity": "warn",
            "message": (
                f"Create-new stores ObjectId as {tgt}"
                + (f" on {db}" if db else "")
                + " — ObjectId domain is not enforced at destination."
            ),
        })
    elif (
        normalize_logical_type(src) == LOGICAL_OBJECTID
        and normalize_logical_type(tgt) != LOGICAL_OBJECTID
        and specialty_wire_preserves_value("OBJECTID", tgt)
    ):
        risks.append({
            "kind": "objectid_domain",
            "severity": "warn",
            "message": (
                f"Create-new stores ObjectId as {tgt}"
                + (f" on {db}" if db else "")
                + " — hex/binary wire; ObjectId domain is not enforced at destination."
            ),
        })
    return risks


def is_lossy_coercion(source_type: str, target_type: str, *, dest_db: str = "") -> bool:
    """True when converting source→target may lose precision, fail silently, or
    change the semantic meaning of a value.

    The allow-list below captures the widening / reversible conversions that the
    transform engine can perform without losing the original value:

      * any value → string/text/json/array (structural serialization)
      * integer → decimal/string/text/json (integer→float is lossy — IEEE mantissa)
      * decimal → string/text/json (decimal→float is lossy)
      * float → string/text/json (float→decimal and float→integer are lossy)
      * boolean → string/text/json/integer/decimal/float
      * date → datetime/string/text/json
      * datetime/time → string/text/json
      * json/array → string/text/json/array
      * string/text/uuid/json/array → binary (base64 reversible)
      * binary → string/text/json (base64 reversible)

    Everything else is considered lossy and should be surfaced in preflight.

    Note ``uuid`` → string/text/json is value-preserving but is still reported
    lossy: :func:`uuid_would_collapse` treats the lost UUID *domain constraint*
    as an operator-visible collapse rather than silent green.
    """
    dest_db = _normalize_dest_db(dest_db) if dest_db else ""
    src = normalize_logical_type(source_type)
    tgt = normalize_logical_type(target_type)
    if src == tgt:
        # Same logical family can still drop TZ polarity, DECIMAL params,
        # VARCHAR width, UNSIGNED range, STRUCT/MAP fields, or ARRAY elements.
        if is_timezone_polarity_loss(source_type, target_type, dest_db=dest_db):
            return True
        if time_timezone_polarity_loss(source_type, target_type):
            return True
        if timezone_aware_would_collapse_to_string(source_type, target_type):
            return True
        if long_raw_locator_would_collapse(source_type, target_type):
            return True
        if decfloat_domain_would_collapse(source_type, target_type):
            return True
        if bignumeric_capacity_would_invent(source_type, target_type):
            return True
        if decimal_fixed_point_would_collapse_to_text(source_type, target_type):
            return True
        if smalldatetime_domain_would_invent(source_type, target_type):
            return True
        if decimal_params_would_narrow(source_type, target_type):
            return True
        if string_width_would_narrow(source_type, target_type):
            return True
        if bounded_string_sink_would_truncate(source_type, target_type):
            return True
        if national_charset_would_collapse(source_type, target_type):
            return True
        if national_charset_would_invent(source_type, target_type):
            return True
        if fixed_width_pad_polarity_loss(source_type, target_type, dest_db=dest_db):
            return True
        if bitstring_opaque_bytes_collapse(source_type, target_type):
            return True
        if binary_width_would_narrow(source_type, target_type):
            return True
        if bitstring_width_would_narrow(source_type, target_type):
            return True
        if year_domain_would_collapse(source_type, target_type):
            return True
        if money_domain_would_collapse(source_type, target_type):
            return True
        if unsigned_integer_would_overflow(source_type, target_type):
            return True
        if unsigned_signed_polarity_invent(source_type, target_type):
            return True
        if integer_width_would_narrow(source_type, target_type):
            return True
        if float_mantissa_would_narrow(source_type, target_type, dest_db=dest_db):
            return True
        if enum_set_domain_would_reject(source_type, target_type):
            return True
        if enum_domain_would_collapse(source_type, target_type):
            return True
        if interval_family_would_collapse(source_type, target_type, dest_db=dest_db):
            return True
        if interval_precision_would_narrow(source_type, target_type):
            return True
        if bitstring_pad_polarity_loss(source_type, target_type):
            return True
        if oracle_char_byte_polarity_loss(source_type, target_type):
            return True
        if oracle_long_numeric_invent(source_type, target_type):
            return True
        if geography_contract_would_collapse(source_type, target_type):
            return True
        if specialty_carrier_would_collapse(source_type, target_type, dest_db=dest_db):
            return True
        if specialty_domain_would_invent(source_type, target_type):
            return True
        if specialty_polarity_mismatch(source_type, target_type):
            return True
        if case_fold_polarity_invent(source_type, target_type):
            return True
        if accent_polarity_invent(source_type, target_type):
            return True
        if width_fold_polarity_invent(source_type, target_type):
            return True
        if kana_fold_polarity_invent(source_type, target_type):
            return True
        if vector_dim_mismatch(source_type, target_type):
            return True
        if vector_encoding_would_collapse(source_type, target_type):
            return True
        if rowversion_would_collapse_to_temporal(source_type, target_type):
            return True
        if sql_variant_would_collapse(source_type, target_type):
            return True
        if uuid_would_collapse(source_type, target_type):
            return True
        if objectid_would_collapse(source_type, target_type):
            return True
        if generated_always_overwrite_risk(target_type):
            return True
        if identity_polarity_would_collapse(source_type, target_type):
            return True
        if identity_domain_would_invent(source_type, target_type):
            return True
        if bfile_locator_would_collapse(source_type, target_type):
            return True
        if temporal_precision_would_narrow(source_type, target_type, dest_db=dest_db):
            return True
        if src == LOGICAL_STRUCT and nested_struct_fields_incompatible(
            source_type, target_type, dest_db=dest_db
        ):
            return True
        if src in {LOGICAL_MAP, LOGICAL_ARRAY} and is_nested_shape_collapse(
            source_type, target_type, dest_db=dest_db
        ):
            return True
        return False

    # MySQL SET → TEXT[] is the intentional multi-value sink (not string→array invent).
    if set_to_array_polarity_preserved(source_type, target_type):
        return False
    # BIT(n) → VARCHAR(n) create-new is 0/1 digit text — not opaque BYTEA packing.
    if is_bitstring_carrier(source_type) and tgt in {LOGICAL_STRING, LOGICAL_TEXT}:
        return False
    # ObjectId → VARCHAR(24)/BINARY(12)/STRING(24) is the industry create-new wire.
    if (
        normalize_logical_type(source_type) == LOGICAL_OBJECTID
        and specialty_wire_preserves_value("OBJECTID", target_type)
    ):
        return False
    # VECTOR(n) → ARRAY<FLOAT> lakehouse create-new wire — not embedding invent.
    if vector_to_array_wire_preserved(source_type, target_type, dest_db=dest_db):
        return False
    # JSON → dialect-native document wire (CLOB/NVARCHAR(MAX)/JSONB/…) — not lossy.
    if src == LOGICAL_JSON and is_dialect_native_document_wire(
        target_type, dest_db=dest_db
    ):
        return False
    # Dialect-aware collapse SSOT — same rules as G3/probe (never MySQL-default FSP
    # when dest_db is postgresql/redshift, never false-collapse JSON→JSONB).
    if is_precision_collapse_coercion(source_type, target_type, dest_db=dest_db):
        return True
    # Fielded STRUCT/MAP → opaque JSON/VARIANT is intentional on many warehouses
    # (Airbyte V2) but is still a field-DDL collapse — treat as lossy so G3 surfaces it.
    if is_nested_document_collapse(source_type, target_type):
        return True
    if timezone_aware_would_collapse_to_string(source_type, target_type):
        return True
    if long_raw_locator_would_collapse(source_type, target_type):
        return True
    if decfloat_domain_would_collapse(source_type, target_type):
        return True
    if bignumeric_capacity_would_invent(source_type, target_type):
        return True
    if decimal_fixed_point_would_collapse_to_text(source_type, target_type):
        return True
    if smalldatetime_domain_would_invent(source_type, target_type):
        return True
    if year_domain_would_collapse(source_type, target_type):
        return True
    if money_domain_would_collapse(source_type, target_type):
        return True
    if bitstring_opaque_bytes_collapse(source_type, target_type):
        return True
    if unsigned_integer_would_overflow(source_type, target_type):
        return True
    if unsigned_signed_polarity_invent(source_type, target_type):
        return True
    if integer_width_would_narrow(source_type, target_type):
        return True
    if float_mantissa_would_narrow(source_type, target_type, dest_db=dest_db):
        return True
    if string_width_would_narrow(source_type, target_type):
        return True
    if bounded_string_sink_would_truncate(source_type, target_type):
        return True
    if national_charset_would_collapse(source_type, target_type):
        return True
    if national_charset_would_invent(source_type, target_type):
        return True
    if fixed_width_pad_polarity_loss(source_type, target_type, dest_db=dest_db):
        return True
    if binary_width_would_narrow(source_type, target_type):
        return True
    if bitstring_width_would_narrow(source_type, target_type):
        return True
    if enum_set_domain_would_reject(source_type, target_type):
        return True
    if enum_domain_would_collapse(source_type, target_type):
        return True
    if interval_family_would_collapse(source_type, target_type, dest_db=dest_db):
        return True
    if interval_precision_would_narrow(source_type, target_type):
        return True
    if bitstring_pad_polarity_loss(source_type, target_type):
        return True
    if oracle_char_byte_polarity_loss(source_type, target_type):
        return True
    if oracle_long_numeric_invent(source_type, target_type):
        return True
    if geography_contract_would_collapse(source_type, target_type):
        return True
    if specialty_carrier_would_collapse(source_type, target_type, dest_db=dest_db):
        return True
    if specialty_domain_would_invent(source_type, target_type):
        return True
    if specialty_polarity_mismatch(source_type, target_type):
        return True
    if case_fold_polarity_invent(source_type, target_type):
        return True
    if accent_polarity_invent(source_type, target_type):
        return True
    if width_fold_polarity_invent(source_type, target_type):
        return True
    if kana_fold_polarity_invent(source_type, target_type):
        return True
    if date_to_tz_aware_invent(source_type, target_type):
        return True
    if vector_dim_mismatch(source_type, target_type):
        return True
    if vector_encoding_would_collapse(source_type, target_type):
        return True
    if rowversion_would_collapse_to_temporal(source_type, target_type):
        return True
    if sql_variant_would_collapse(source_type, target_type):
        return True
    if uuid_would_collapse(source_type, target_type):
        return True
    if objectid_would_collapse(source_type, target_type):
        return True
    if generated_always_overwrite_risk(target_type):
        return True
    if identity_polarity_would_collapse(source_type, target_type):
        return True
    if identity_domain_would_invent(source_type, target_type):
        return True
    if bfile_locator_would_collapse(source_type, target_type):
        return True
    if temporal_precision_would_narrow(source_type, target_type, dest_db=dest_db):
        return True
    if document_domain_would_collapse(source_type, target_type, dest_db=dest_db):
        return True
    if document_domain_would_invent(source_type, target_type):
        return True
    # ARRAY→ARRAY is in the safe allow-list below only when element types widen.
    if src == LOGICAL_ARRAY and tgt == LOGICAL_ARRAY and is_nested_shape_collapse(
        source_type, target_type, dest_db=dest_db
    ):
        return True
    if src == LOGICAL_MAP and tgt == LOGICAL_MAP and is_nested_shape_collapse(
        source_type, target_type, dest_db=dest_db
    ):
        return True

    safe: set[tuple[str, str]] = {
        # text / structural containers are universal sinks
        (LOGICAL_STRING, LOGICAL_TEXT),
        (LOGICAL_TEXT, LOGICAL_STRING),
        # Open string → JSON invents document domain — not allow-listed.
        # JSON/VARIANT/SUPER → open string drops document domain — not allow-listed.
        (LOGICAL_JSON, LOGICAL_JSON),
        # ARRAY→JSON/text is document collapse (lossy) — not allow-listed.
        # JSON→ARRAY invents array domain from a document — not allow-listed.
        (LOGICAL_ARRAY, LOGICAL_ARRAY),
        # numeric widening and text renderings
        (LOGICAL_INTEGER, LOGICAL_DECIMAL),
        # integer→float is LOSSY for large ints (IEEE mantissa) — not allow-listed
        (LOGICAL_INTEGER, LOGICAL_STRING),
        (LOGICAL_INTEGER, LOGICAL_TEXT),
        (LOGICAL_INTEGER, LOGICAL_JSON),
        (LOGICAL_DECIMAL, LOGICAL_STRING),
        (LOGICAL_DECIMAL, LOGICAL_TEXT),
        (LOGICAL_DECIMAL, LOGICAL_JSON),
        (LOGICAL_FLOAT, LOGICAL_STRING),
        (LOGICAL_FLOAT, LOGICAL_TEXT),
        (LOGICAL_FLOAT, LOGICAL_JSON),
        # float→decimal is LOSSY (IEEE → fixed-point) — not in this allow-list
        # boolean renderings and scalar widenings
        (LOGICAL_BOOLEAN, LOGICAL_STRING),
        (LOGICAL_BOOLEAN, LOGICAL_TEXT),
        (LOGICAL_BOOLEAN, LOGICAL_JSON),
        (LOGICAL_BOOLEAN, LOGICAL_INTEGER),
        (LOGICAL_BOOLEAN, LOGICAL_DECIMAL),
        (LOGICAL_BOOLEAN, LOGICAL_FLOAT),
        # date→datetime NTZ widening only; DATE→TIMESTAMPTZ invents midnight TZ.
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
        # uuid renderings (domain still collapses via uuid_would_collapse above)
        (LOGICAL_UUID, LOGICAL_STRING),
        (LOGICAL_UUID, LOGICAL_TEXT),
        (LOGICAL_UUID, LOGICAL_JSON),
        # VECTOR→ARRAY drops embedding domain — not allow-listed.
        # STRUCT/MAP → text/JSON is nested document collapse (handled above).
        (LOGICAL_STRUCT, LOGICAL_STRUCT),
        (LOGICAL_MAP, LOGICAL_MAP),
    }

    if (src, tgt) in safe:
        # DATE→TIMESTAMPTZ / DATETIMEOFFSET invents an instant — not a free widen.
        if date_to_tz_aware_invent(source_type, target_type):
            return True
        if case_fold_polarity_invent(source_type, target_type):
            return True
        if accent_polarity_invent(source_type, target_type):
            return True
        if width_fold_polarity_invent(source_type, target_type):
            return True
        if kana_fold_polarity_invent(source_type, target_type):
            return True
        if national_charset_would_collapse(source_type, target_type):
            return True
        if national_charset_would_invent(source_type, target_type):
            return True
        if fixed_width_pad_polarity_loss(source_type, target_type, dest_db=dest_db):
            return True
        # INTEGER→VARCHAR(1) / JSON→CHAR(10) — bounded sink truncates.
        if bounded_string_sink_would_truncate(source_type, target_type):
            return True
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
