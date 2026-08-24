"""Kernel Type Invent Engine — destination DDL invent bodies (Phase C2).

Implementation home for invent/normalize/materialize/width carriers.
``type_system`` keeps thin backward-compat shims; Map/Validate/Execute import
via ``services.decision_kernel`` (or this module). Specialty detectors and
dialect helper tables remain in ``type_system`` until later C2 splits.
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Final

from services.decision_kernel.logical_type import LogicalType, NativeType
from services.source_engine_scope import active_source_engine

# Shared carriers/tables owned by ``type_system``. Imported by name so a rename
# there fails at import here, not as a NameError mid-load: ``type_system``
# reaches this module only through function-local shims, so the cycle stays open.
from services.type_system import (
    CANONICAL_TYPES,
    DDL_TYPES,
    DEFAULT_DDL,
    LOGICAL_ARRAY,
    LOGICAL_BINARY,
    LOGICAL_BOOLEAN,
    LOGICAL_DATETIME,
    LOGICAL_DECIMAL,
    LOGICAL_FLOAT,
    LOGICAL_GEOGRAPHY,
    LOGICAL_INTEGER,
    LOGICAL_INTERVAL,
    LOGICAL_JSON,
    LOGICAL_MAP,
    LOGICAL_OBJECTID,
    LOGICAL_STRING,
    LOGICAL_STRUCT,
    LOGICAL_TEXT,
    LOGICAL_TIME,
    LOGICAL_UUID,
    LOGICAL_VECTOR,
    _DECIMAL_PARAM_TEMPLATES,
    _DYNAMODB_ATTR_LOGICAL,
    _DYNAMODB_ONLY_ATTR_CODES,
    _NO_TEMPORAL_TYPMOD_ENGINES,
    _OBJECTID_DDL_DEFAULTS,
    _apply_temporal_fsp,
    _binary_ddl_for_dest,
    _bitstring_ddl_for_dest,
    _clickhouse_native_datetime_ddl,
    _datetime_ddl_for_dest,
    _decimal_ddl_for_dest,
    _enum_set_ddl_for_dest,
    _float_ddl_for_dest,
    _geography_ddl_for_dest,
    _integer_ddl_for_dest,
    _interval_ddl_for_dest,
    _is_unsigned_integer_decimal_carrier,
    _nested_ddl_for_dest,
    _normalize_dest_db,
    _range_ddl_for_dest,
    _string_ddl_for_dest,
    _time_ddl_for_dest,
    _vector_ddl_for_dest,
    _with_collation_clause,
    arrow_dtype_to_carrier,
    avro_logical_token_to_carrier,
    ddl_carrier_type,
    destination_is_file_export,
    float_mantissa_bits,
    integer_bit_width,
    is_bitstring_carrier,
    is_fixed_width_char_carrier,
    is_national_string_carrier,
    is_unlimited_string_carrier,
    parse_array_element,
    parse_numeric_precision_scale,
    parse_string_carrier_width,
    parse_temporal_fractional_precision,
    specialty_carrier_base,
    specialty_wire_preserves_value,
    strip_identity_qualifier,
    uuid_exact_wire_carrier,
    zero_scale_fits_signed_bigint,
)



# Type strings are a tiny fixed vocabulary per job while this runs once per
# *cell* on the bind and fingerprint paths — a 10M-row load called it ~230M
# times, and the regex work dominated the profile. Memoized on the raw string;
# the function is pure (str in, str out) so the cache cannot change a verdict.
@lru_cache(maxsize=8192)
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




_FILE_EXPORT_DECIMAL_PRECISION: Final[int] = 38


def _file_export_ddl(inferred: str | None) -> str:
    """Type carried by a file/object export — class kept, declared width dropped.

    An object export has no DDL, so the type comes from the format itself: JSON
    numbers, Parquet/Avro typed columns. Collapsing to the TEXT default quotes
    every integer, decimal and date, and downstream Athena/Spark then read a
    typed source back as strings.

    Widths are a different matter: a file has no column width to overflow, and
    a source width inferred from a sample would quarantine every later row that
    exceeds it. Keep the scale (it is the value's own precision) and widen the
    precision to the decimal128 maximum the export formats carry.
    """
    carrier = ddl_carrier_type(inferred)
    logical = normalize_logical_type(carrier)
    if logical == LOGICAL_DECIMAL:
        _, scale = parse_numeric_precision_scale(carrier)
        if scale is None:
            return carrier
        return f"DECIMAL({_FILE_EXPORT_DECIMAL_PRECISION},{min(int(scale), _FILE_EXPORT_DECIMAL_PRECISION)})"
    width = parse_string_carrier_width(carrier)
    if width is not None and logical in {LOGICAL_STRING, LOGICAL_TEXT}:
        return "VARCHAR"
    return carrier


def ddl_type(db_type: str, inferred: str | LogicalType | NativeType | None) -> str:
    """Map a logical source type to a destination-native DDL type.

    Property 1: prefer ``LogicalType`` / ``NativeType`` over bare strings.
    String inputs remain accepted for compatibility, but ambiguous keywords
    (``INTEGER`` / ``INT`` / ``FLOAT`` any case) invent 64-bit — never
    case-select Int32/Float32. True INT32/IEEE-32 sources must use unambiguous
    carriers (``INT4`` / ``FLOAT32`` / ``REAL``).

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
    if isinstance(inferred, NativeType):
        # Same-family physical passthrough; cross-family rematerialize.
        if (inferred.db or "").strip().lower() == (db_type or "").strip().lower():
            return inferred.text
        inferred = inferred.text
    elif isinstance(inferred, LogicalType):
        # Width-bearing logical → unambiguous carrier, then invent.
        inferred = inferred.to_carrier()
    raw_db = (db_type or "").strip().lower()
    # QuestDB has no DECIMAL/TIME/UUID natives — stamp honest DOUBLE/VARCHAR
    # before generic_sql normalize invents DECIMAL(38,15) Map stamps.
    if raw_db == "questdb":
        t = normalize_logical_type(inferred)
        if t == LOGICAL_DECIMAL:
            return "DOUBLE"
        if t == LOGICAL_TIME:
            return "VARCHAR"
        if t == LOGICAL_UUID:
            return "VARCHAR"
        # Fall through for other logicals via generic_sql mapping.
    if destination_is_file_export(db_type):
        return _file_export_ddl(inferred)
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
        # Keep the 36-char contract, but spell it the way this destination
        # spells a string: ``STRING(36)`` is BigQuery/Databricks wire and a
        # syntax error on PostgreSQL / MySQL / Oracle CREATE.
        exact_wire = strip_identity_qualifier(inferred).strip()
        bounded = _string_ddl_for_dest(db, exact_wire)
        if bounded:
            return bounded
        # No bounded string wire on this engine (SQLite / DuckDB / ClickHouse /
        # Iceberg): keep the foreign token only when its base is what this
        # engine already spells, else fall back to the native unbounded wire
        # (wider, never narrower — width is not the UUID contract here).
        native_string = DDL_TYPES.get(db, {}).get(LOGICAL_STRING) or DEFAULT_DDL.get(db, "")
        exact_base = exact_wire.upper().split("(", 1)[0].strip()
        native_base = native_string.upper().split("(", 1)[0].strip()
        widthless_native = "(" in exact_wire and "(" not in native_string
        if native_string and (exact_base != native_base or widthless_native):
            return native_string
        return exact_wire
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
        if db == "spanner":
            # No HALF — nearest Spanner wire is FLOAT32 (never invent FLOAT64 widen).
            return "FLOAT32"
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
    # Oracle ANSI FLOAT(p) is NUMBER-backed binary precision (bare = FLOAT(126),
    # ~38 decimal digits), so BINARY_DOUBLE would cut it to a 53-bit mantissa.
    #
    # Only a *declared* Oracle carrier keeps that storage class. Two things it
    # is not: the logical family alias ``float`` (a family, not a stamp), and a
    # bare ``FLOAT`` read off some other engine's catalog — PostgreSQL and SQL
    # Server both spell IEEE-64 that way, and holding those in NUMBER-backed
    # FLOAT(126) changes the destination's storage class on nothing but the
    # spelling's letter case. A precision is unambiguous; a bare token needs an
    # Oracle source to mean the Oracle type.
    if db == "oracle" and strip_identity_qualifier(inferred).strip() != LOGICAL_FLOAT:
        declared = re.match(r"^FLOAT(\((\d+)\))?$", base_early)
        if declared and (declared.group(2) or active_source_engine() == "oracle"):
            return base_early
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





# Families whose create-new stamp is a pure *width* projection, so a wider
# source declaration can only mean the earlier sample was too small. Temporal
# carriers are excluded on purpose: MySQL ``TIMESTAMP`` → ``DATETIME(6)`` also
# moves the 1970–2038 range, which is a semantic change an operator must see
# (``promote_create_new_temporal_stamp`` owns the temporal question).
_CAPACITY_PROMOTABLE_LOGICALS: Final[frozenset[str]] = frozenset(
    {LOGICAL_STRING, LOGICAL_TEXT, LOGICAL_INTEGER, LOGICAL_DECIMAL, LOGICAL_BINARY}
)


def promote_create_new_capacity_stamp(
    src_type: str,
    stamped: str,
    dest_db_type: str = "",
) -> str:
    """Widen a projected create-new stamp that can no longer hold the source.

    A create-new stamp is a *projection* of the source type, not an operator
    decision: on a sampled source (CSV/Excel/document store) Map projects it
    from the first rows, and the full read then declares a wider type —
    ``DECIMAL(6,4)`` from eight rows becomes ``DECIMAL(8,4)`` over the file.
    Enforcing the earlier projection makes Datawrap block its own CREATE TABLE
    for a fidelity collapse it invented, which is the worst kind of refusal:
    there is no destination DDL to protect yet, and no remap the operator can
    make that is more correct than the one we would write ourselves.

    So the stamp is re-projected from the current source type, and only ever
    widened *inside the carrier family the stamp already chose*: a
    ``NUMBER(6,4)`` that can no longer hold ``DECIMAL(9,4)`` grows its
    precision, but a ``VARCHAR(64)`` stamped on a decimal column is a
    deliberate representation change — nobody projects a string carrier for a
    numeric source — and stands as written. Landing a value the stamp cannot
    hold is then a real cast failure the writer must quarantine, not a width
    Datawrap may silently re-choose. An operator-chosen narrowing
    (``user_override`` / ``risk_acknowledged``) never reaches here.
    """
    from services.type_system import is_lossy_coercion, normalize_logical_type

    stamp = (stamped or "").strip()
    src = (src_type or "").strip()
    db = (dest_db_type or "").strip()
    if not stamp or not src:
        return stamp
    logical = normalize_logical_type(src)
    if logical != normalize_logical_type(stamp):
        return stamp
    if logical not in _CAPACITY_PROMOTABLE_LOGICALS:
        return stamp
    if not is_lossy_coercion(src, stamp, dest_db=db):
        return stamp
    reprojected = create_new_mapping_target_type(src, db)
    if not reprojected or reprojected.strip().upper() == stamp.upper():
        return stamp
    if is_lossy_coercion(src, reprojected, dest_db=db):
        return stamp
    return reprojected


def create_new_mapping_target_type(
    src_type: str,
    dest_db_type: str = "",
    *,
    samples: list | None = None,
    source_db: str = "",
) -> str:
    """Target type stamped on create-new mappings for Validate + writers.

    ``source_db`` is the source engine id. It only widens the stamp: a source
    that can emit any code point must not land on a SQL Server code-page
    ``VARCHAR`` that silently rewrites it to ``?``."""
    from services.type_system import unicode_safe_target_carrier

    stamp = _create_new_mapping_target_type(
        src_type, dest_db_type, samples=samples, source_db=source_db
    )
    stamp = unicode_safe_target_carrier(
        stamp, dest_db=dest_db_type, source_db=source_db
    )
    stamp = refuse_create_new_numeric_collapse(src_type, stamp, dest_db_type)
    inherited = inherit_measured_string_width(
        stamp, src_type, dest_db=dest_db_type
    )
    return inherited or stamp


def refuse_create_new_numeric_collapse(
    src_type: str, stamp: str, dest_db_type: str
) -> str:
    """Create-new must not invent a narrower dest than the declared source.

    Sample-sized BIGINT / NUMERIC(9,4) from NUMBER(38,0) / DECIMAL(12,2) is the
    Airbyte-class cliff: Validate looks green on 25 rows, Execute can overflow
    or round the rest of the population.
    """
    from services.type_system import is_precision_collapse_coercion

    src = (src_type or "").strip()
    dest = (stamp or "").strip()
    db = (dest_db_type or "").strip()
    if not src or not dest:
        return stamp
    # Only rewrite numeric/integer/float invent. DECIMAL→TEXT is an explicit
    # Map stamp (quarantine unfit cells), not the BIGINT / NUMERIC(9,4) cliff.
    dest_logical = normalize_logical_type(dest)
    if dest_logical not in {LOGICAL_DECIMAL, LOGICAL_INTEGER, LOGICAL_FLOAT}:
        return stamp
    # Bare DECIMAL/NUMBER may still be sample-sized. Only rewrite when the
    # source declared a precision the stamp collapses (NUMBER(38,0)→BIGINT).
    src_p, _src_s = parse_numeric_precision_scale(src)
    if src_p is None and dest_logical != LOGICAL_INTEGER:
        return stamp
    if not is_precision_collapse_coercion(
        src, dest, dest_db=db, dest_table_exists=False
    ):
        return stamp
    if _unsigned_polarity_only_collapse(src, dest, db):
        return stamp
    recovered = ddl_type(db, src) if db else src
    if recovered and not is_precision_collapse_coercion(
        src, recovered, dest_db=db, dest_table_exists=False
    ):
        return recovered
    # Falling back to the source token is only honest when the destination
    # engine is unknown. With a destination in hand it hands CREATE a token
    # from the *source* dialect (ClickHouse ``UInt8`` into PostgreSQL), which
    # the engine rejects at DDL time — keep the widest legal stamp instead.
    if db:
        return recovered or stamp
    return src


def _unsigned_polarity_only_collapse(src: str, dest: str, db: str) -> bool:
    """True when the sole collapse is losing UNSIGNED polarity, not capacity.

    ``UInt8 → SMALLINT`` keeps every source value; what it drops is the
    declaration that negatives are impossible, which is an Accept-risk finding
    for the operator — not a reason to re-invent a narrower-or-foreign stamp.
    """
    from services.type_system import (
        decimal_params_would_narrow,
        float_mantissa_would_narrow,
        integer_width_would_narrow,
        string_width_would_narrow,
        unsigned_integer_would_overflow,
        unsigned_signed_polarity_invent,
    )

    if not unsigned_signed_polarity_invent(src, dest):
        return False
    return not (
        unsigned_integer_would_overflow(src, dest)
        or integer_width_would_narrow(src, dest, dest_db=db)
        or decimal_params_would_narrow(src, dest, dest_db=db)
        or float_mantissa_would_narrow(src, dest, dest_db=db)
        or string_width_would_narrow(src, dest)
    )


def _create_new_mapping_target_type(
    src_type: str,
    dest_db_type: str = "",
    *,
    samples: list | None = None,
    source_db: str = "",
) -> str:
    """Target type stamped on create-new mappings for Validate + writers.

    Stamp **physical** DDL whenever the destination has no native UUID type —
    even for exact ``CHAR(36)`` / ``VARCHAR(36)`` wires. Map must match CREATE
    (never silent-green UUID→UUID while writers emit VARCHAR). Native UUID /
    UNIQUEIDENTIFIER destinations keep the engine token.

    When ``src_type`` is bare DECIMAL/NUMERIC and ``samples`` are provided,
    invent observed ``DECIMAL(p,s)`` or ``FLOAT`` (IEEE residue) — never silent
    platform floor ``DECIMAL(38,15)`` from an empty typmod.
    """
    # Bare DECIMAL/NUMERIC invent from samples before specialty / UUID paths.
    # Declared DECIMAL(p,s) wins; empty samples fall through (no fake (38,15)).
    # Declared FLOAT/DOUBLE/REAL must never invent DECIMAL from samples — that
    # collapses IEEE polarity (audit §2.1 twin). Sample→DECIMAL only for bare
    # fixed-point or open text sources (observe path elsewhere).
    if samples and normalize_logical_type(src_type) == LOGICAL_DECIMAL:
        p, _s = parse_numeric_precision_scale(src_type)
        from services.decimal_observe import source_declares_numeric_domain

        # A typed source (relational NUMBER/DECIMAL, BSON Decimal128) holds a
        # domain the Validate sample never bounds — sizing create-new from that
        # sample invents a carrier narrower than the source, which the product
        # then blocks as its own fidelity collapse and which would quarantine
        # unsampled rows. Fall through to the platform carrier instead.
        if p is None and not source_declares_numeric_domain(source_db):
            from services.decimal_observe import (
                create_new_decimal_carrier,
                observe_numeric_samples,
            )

            obs = observe_numeric_samples(samples)
            if obs.get("kind") not in {None, "empty"}:
                carrier: str = create_new_decimal_carrier(
                    samples, dest_db=dest_db_type, source_type=src_type
                )
                db = (dest_db_type or "").strip()
                if db:
                    return promote_create_new_temporal_stamp(
                        carrier, ddl_type(db, carrier), dest_db_type
                    )
                return carrier

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
        # Bare DECIMAL/NUMERIC → DECIMAL(38,15); quarantine needs (p,s).
        "DECIMAL", "NUMERIC",
        # Foreign temporals → TIMESTAMP / TIMESTAMPTZ SSOT.
        "DATETIME", "DATETIME2", "DATETIME64", "DATETIMEOFFSET", "SMALLDATETIME",
        "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ", "TIMETZ", "YEAR",
        # Foreign IEEE aliases → REAL / DOUBLE PRECISION. Keep REAL/DOUBLE PRECISION.
        "FLOAT4", "FLOAT8", "HALF", "HALFFLOAT", "FLOAT16", "FLOAT32", "FLOAT64",
        "BINARY_FLOAT", "BINARY_DOUBLE", "DOUBLE", "FLOAT",
        # Foreign VECTOR/BIT/ENUM/MONEY/YEAR/MEDIUMINT → ddl_type SSOT.
        "VECTOR", "HALFVEC", "SPARSEVEC",
        "BIT", "BOOL", "TINYINT",
        "ENUM", "SET",
        "MONEY", "SMALLMONEY", "CURRENCY",
        "YEAR", "MEDIUMINT",
        "BOOLEAN",
    }),
    "snowflake": frozenset({
        "JSON", "JSONB", "UUID", "BYTEA", "INET", "CIDR", "CITEXT", "HSTORE",
        "SUPER", "NVARCHAR2", "BIGNUMERIC", "UNIQUEIDENTIFIER",
        # Native wire is BINARY(n); rematerialize foreign aliases.
        "VARBINARY", "BYTES", "VARBYTE", "FIXED",
        # Bare DECIMAL/NUMBER → NUMBER(38,10) SSOT (never batch invent).
        "DECIMAL", "NUMERIC", "NUMBER",
        # TIMESTAMP/DATETIME → TIMESTAMP_NTZ; TIMESTAMPTZ → TIMESTAMP_LTZ.
        # Keep TIMESTAMP_NTZ / TIMESTAMP_LTZ / TIMESTAMP_TZ native.
        "TIMESTAMP", "DATETIME", "DATETIME2", "DATETIME64", "TIMESTAMPTZ",
        "DATETIMEOFFSET", "SMALLDATETIME", "TIMETZ", "YEAR",
        # Foreign IEEE → FLOAT wire. Keep FLOAT.
        "FLOAT4", "FLOAT8", "REAL", "HALF", "HALFFLOAT", "FLOAT16", "FLOAT32",
        "FLOAT64", "BINARY_FLOAT", "BINARY_DOUBLE", "DOUBLE",
        # Foreign VECTOR/BIT/ENUM/MONEY/YEAR/MEDIUMINT → ddl_type SSOT.
        "VECTOR", "HALFVEC", "SPARSEVEC",
        "BIT", "BOOL", "TINYINT",
        "ENUM", "SET",
        "MONEY", "SMALLMONEY", "CURRENCY",
        "YEAR", "MEDIUMINT",
    }),
    "bigquery": frozenset({
        "UUID", "JSONB", "BYTEA", "INET", "CIDR", "CITEXT", "HSTORE", "SUPER",
        "VARIANT", "NVARCHAR2", "VARCHAR2", "NUMBER", "UNIQUEIDENTIFIER",
        # Native wire is BYTES(n); rematerialize BINARY/VARBINARY/fixed typmods.
        "BINARY", "VARBINARY", "VARBYTE", "FIXED",
        # Bare DECIMAL/NUMERIC → BIGNUMERIC create-new wire.
        "DECIMAL", "NUMERIC",
        # Foreign temporals → DATETIME/TIMESTAMP SSOT. Keep DATETIME/TIMESTAMP/DATE/TIME.
        "DATETIME2", "DATETIME64", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ",
        "TIMESTAMPTZ", "DATETIMEOFFSET", "SMALLDATETIME", "TIMETZ", "YEAR",
        # Foreign IEEE → FLOAT64 only. Keep FLOAT64.
        "FLOAT4", "FLOAT8", "REAL", "HALF", "HALFFLOAT", "FLOAT16", "FLOAT32",
        "BINARY_FLOAT", "BINARY_DOUBLE", "DOUBLE", "FLOAT",
        # Foreign VECTOR/BIT/ENUM/MONEY/YEAR/MEDIUMINT → ddl_type SSOT.
        "VECTOR", "HALFVEC", "SPARSEVEC",
        "BIT", "BOOL", "TINYINT",
        "ENUM", "SET",
        "MONEY", "SMALLMONEY", "CURRENCY",
        "YEAR", "MEDIUMINT",
        "BOOLEAN",
        # Foreign string/integer aliases → STRING / INT64. Keep STRING/INT64/BYTES.
        "VARCHAR", "CHAR", "NVARCHAR", "TEXT", "CLOB", "NCLOB", "BPCHAR",
        "CHARACTER VARYING",
        "INTEGER", "INT", "BIGINT", "SMALLINT",
    }),
    "spanner": frozenset({
        "UUID", "JSONB", "BYTEA", "INET", "CIDR", "CITEXT", "HSTORE", "SUPER",
        "VARIANT", "NVARCHAR2", "VARCHAR2", "BIGNUMERIC", "UNIQUEIDENTIFIER",
        "DATETIME", "TIME",  # Spanner has no DATETIME/TIME — use STRING wire
        "BINARY", "VARBINARY", "VARBYTE", "FIXED",
        "DECIMAL", "NUMERIC", "NUMBER",
        "DATETIME2", "DATETIME64", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ",
        "TIMESTAMPTZ", "DATETIMEOFFSET", "SMALLDATETIME", "TIMETZ", "YEAR",
        "TIMESTAMP",  # NTZ invent — SSOT STRING(30) / TIMESTAMP for aware via ddl
        # Foreign IEEE → FLOAT32/FLOAT64. Keep FLOAT32/FLOAT64. Never invent REAL.
        "FLOAT4", "FLOAT8", "REAL", "HALF", "HALFFLOAT", "FLOAT16",
        "BINARY_FLOAT", "BINARY_DOUBLE", "DOUBLE", "FLOAT",
        # Foreign VECTOR/BIT/ENUM/MONEY/YEAR/MEDIUMINT → ddl_type SSOT.
        "VECTOR", "HALFVEC", "SPARSEVEC",
        "BIT", "BOOL", "TINYINT",
        "ENUM", "SET",
        "MONEY", "SMALLMONEY", "CURRENCY",
        "YEAR", "MEDIUMINT",
        "BOOLEAN",
    }),
    "postgresql": frozenset({
        # Bare JSON is a logical alias; create-new document wire is JSONB.
        "JSON",
        "SUPER", "VARIANT", "BIGNUMERIC", "UNIQUEIDENTIFIER", "NVARCHAR2",
        # Native wire is BYTEA; rematerialize BINARY(n)/BYTES(n)/fixed(n).
        "BINARY", "VARBINARY", "BYTES", "VARBYTE", "FIXED",
        # Bare DECIMAL/NUMBER → NUMERIC (unbounded). Keep bare NUMERIC native.
        "DECIMAL", "NUMBER",
        # Foreign temporals → TIMESTAMP/TIMESTAMPTZ. Keep TIMESTAMP/TIMESTAMPTZ/DATE/TIME.
        "DATETIME", "DATETIME2", "DATETIME64", "DATETIMEOFFSET", "SMALLDATETIME",
        "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ", "TIMETZ", "YEAR",
        # Foreign IEEE aliases → REAL / DOUBLE PRECISION. Keep REAL/DOUBLE PRECISION.
        "FLOAT4", "FLOAT8", "HALF", "HALFFLOAT", "FLOAT16", "FLOAT32", "FLOAT64",
        "BINARY_FLOAT", "BINARY_DOUBLE", "DOUBLE", "FLOAT",
        # Foreign VECTOR/BIT/ENUM/MONEY/YEAR/MEDIUMINT → ddl_type SSOT.
        "VECTOR", "HALFVEC", "SPARSEVEC",
        "BIT", "BOOL", "TINYINT",
        "ENUM", "SET",
        "MONEY", "SMALLMONEY", "CURRENCY",
        "YEAR", "MEDIUMINT",
    }),
    "mysql": frozenset({
        "JSONB", "UUID", "BYTEA", "SUPER", "VARIANT", "BIGNUMERIC",
        "UNIQUEIDENTIFIER", "NVARCHAR2", "HSTORE",
        # Native BINARY(n)/VARBINARY(n); rematerialize foreign aliases only.
        "BYTES", "VARBYTE", "FIXED",
        # Bare DECIMAL invents MySQL DECIMAL(10,0); SSOT is DECIMAL(38,15).
        "DECIMAL", "NUMERIC", "NUMBER",
        # TIMESTAMP invents session-TZ; DATETIME/TIME need FSP(6). Keep DATE/YEAR.
        "TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ",
        "DATETIME", "DATETIME2", "DATETIME64", "DATETIMEOFFSET", "SMALLDATETIME",
        "TIME", "TIMETZ",
        # Foreign IEEE → FLOAT / DOUBLE. Keep FLOAT/DOUBLE.
        "FLOAT4", "FLOAT8", "REAL", "HALF", "HALFFLOAT", "FLOAT16", "FLOAT32",
        "FLOAT64", "BINARY_FLOAT", "BINARY_DOUBLE",
        # Foreign VECTOR/BIT/ENUM/MONEY/YEAR/MEDIUMINT → ddl_type SSOT.
        "VECTOR", "HALFVEC", "SPARSEVEC",
        "BIT", "BOOL", "TINYINT",
        "ENUM", "SET",
        "MONEY", "SMALLMONEY", "CURRENCY",
            }),
    "sqlserver": frozenset({
        "JSONB", "UUID", "BYTEA", "SUPER", "VARIANT", "BIGNUMERIC", "JSON",
        "NVARCHAR2", "HSTORE", "INET", "CIDR",
        "BYTES", "VARBYTE", "FIXED",
        "DECIMAL", "NUMERIC", "NUMBER",
        # TIMESTAMP is T-SQL ROWVERSION — never CREATE as datetime. Rematerialize
        # to DATETIME2(7). Keep DATETIME2 / DATETIMEOFFSET / SMALLDATETIME / DATE.
        "TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ",
        "DATETIME", "DATETIME64", "TIMETZ", "YEAR",
        # Foreign IEEE → REAL / FLOAT. Keep REAL/FLOAT (incl. FLOAT(n) typmod).
        "FLOAT4", "FLOAT8", "HALF", "HALFFLOAT", "FLOAT16", "FLOAT32", "FLOAT64",
        "BINARY_FLOAT", "BINARY_DOUBLE", "DOUBLE",
        # Foreign VECTOR/BIT/ENUM/MONEY/YEAR/MEDIUMINT → ddl_type SSOT.
        "VECTOR", "HALFVEC", "SPARSEVEC",
        "BIT", "BOOL", "TINYINT",
        "ENUM", "SET",
                "YEAR", "MEDIUMINT",
        "BOOLEAN",
    }),
    "oracle": frozenset({
        "JSONB", "UUID", "BYTEA", "SUPER", "VARIANT", "BIGNUMERIC", "JSON",
        "UNIQUEIDENTIFIER", "HSTORE", "INET", "CIDR",
        # Typmod BINARY(n) → RAW(n); bare BINARY already rematerializes to BLOB.
        "BINARY", "VARBINARY", "BYTES", "VARBYTE", "FIXED",
        # Bare NUMBER/DECIMAL → NUMBER(38,10) SSOT.
        "DECIMAL", "NUMERIC", "NUMBER",
        # Foreign temporals → TIMESTAMP / WITH TIME ZONE. Keep TIMESTAMP/DATE.
        "DATETIME", "DATETIME2", "DATETIME64", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ",
        "TIMESTAMP_TZ", "TIMESTAMPTZ", "DATETIMEOFFSET", "SMALLDATETIME",
        "TIME", "TIMETZ", "YEAR",
        # Foreign IEEE → BINARY_FLOAT / BINARY_DOUBLE. Keep BINARY_*.
        # FLOAT / FLOAT(p) is *not* foreign IEEE here: Oracle's ANSI FLOAT is
        # NUMBER-backed binary precision (bare = 126 binary digits), so
        # rematerializing it to BINARY_DOUBLE cuts an Oracle→Oracle column down
        # to a 53-bit mantissa. Keep it native; it is never narrower.
        "FLOAT4", "FLOAT8", "REAL", "HALF", "HALFFLOAT", "FLOAT16", "FLOAT32",
        "FLOAT64", "DOUBLE",
        # Foreign VECTOR/BIT/ENUM/MONEY/YEAR/MEDIUMINT → ddl_type SSOT.
        "VECTOR", "HALFVEC", "SPARSEVEC",
        "BIT", "BOOL", "TINYINT",
        "ENUM", "SET",
        "MONEY", "SMALLMONEY", "CURRENCY",
        "YEAR", "MEDIUMINT",
        "BOOLEAN",
    }),
    "databricks": frozenset({
        "JSONB", "UUID", "BYTEA", "SUPER", "VARIANT", "BIGNUMERIC",
        "UNIQUEIDENTIFIER", "NVARCHAR2", "HSTORE", "INET", "CIDR",
        "BINARY", "VARBINARY", "BYTES", "VARBYTE", "FIXED",
        "DECIMAL", "NUMERIC", "NUMBER",
        # Databricks TIMESTAMP is native session-TZ wire — keep Map stamps.
        # Foreign DATETIME*/TIMESTAMPTZ → TIMESTAMP_NTZ / TIMESTAMP via ddl.
        "DATETIME", "DATETIME2", "DATETIME64", "TIMESTAMPTZ",
        "DATETIMEOFFSET", "SMALLDATETIME", "TIMETZ", "YEAR",
        "TIMESTAMP_TZ", "TIMESTAMP_LTZ",
        # Foreign IEEE → FLOAT / DOUBLE. Keep FLOAT/DOUBLE (HALF rematerializes).
        "FLOAT4", "FLOAT8", "REAL", "HALF", "HALFFLOAT", "FLOAT16", "FLOAT32",
        "FLOAT64", "BINARY_FLOAT", "BINARY_DOUBLE",
        # Foreign VECTOR/BIT/ENUM/MONEY/YEAR/MEDIUMINT → ddl_type SSOT.
        "VECTOR", "HALFVEC", "SPARSEVEC",
        "BIT", "BOOL", "TINYINT",
        "ENUM", "SET",
        "MONEY", "SMALLMONEY", "CURRENCY",
        "YEAR", "MEDIUMINT",
    }),
    "iceberg": frozenset({
        "JSONB", "UUID", "BYTEA", "SUPER", "VARIANT", "BIGNUMERIC",
        "UNIQUEIDENTIFIER", "NVARCHAR2", "HSTORE", "INET", "CIDR", "JSON",
        # Widthed binary → fixed(n); bare BINARY already → binary. Keep FIXED native.
        "BINARY", "VARBINARY", "BYTES", "VARBYTE",
        "DECIMAL", "NUMERIC", "NUMBER",
        "DATETIME", "DATETIME2", "DATETIME64", "DATETIMEOFFSET", "SMALLDATETIME",
        "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ", "TIMETZ", "YEAR",
        # Foreign IEEE → float / double. Keep lowercase float/double.
        "FLOAT4", "FLOAT8", "REAL", "HALF", "HALFFLOAT", "FLOAT16", "FLOAT32",
        "FLOAT64", "BINARY_FLOAT", "BINARY_DOUBLE", "DOUBLE", "FLOAT",
        # Foreign VECTOR/BIT/ENUM/MONEY/YEAR/MEDIUMINT → ddl_type SSOT.
        "VECTOR", "HALFVEC", "SPARSEVEC",
        "BIT", "BOOL", "TINYINT",
        "ENUM", "SET",
        "MONEY", "SMALLMONEY", "CURRENCY",
        "YEAR", "MEDIUMINT",
        "BOOLEAN",
    }),
    "duckdb": frozenset({
        "DECIMAL", "NUMERIC", "NUMBER", "BIGNUMERIC", "BIGDECIMAL",
        "BINARY", "VARBINARY", "BYTES", "VARBYTE", "BYTEA", "FIXED",
        "JSONB", "UUID", "SUPER", "VARIANT", "UNIQUEIDENTIFIER", "NVARCHAR2",
        "DATETIME", "DATETIME2", "DATETIME64", "DATETIMEOFFSET", "SMALLDATETIME",
        "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ", "TIMETZ", "YEAR",
        # Foreign IEEE → REAL / DOUBLE. Keep REAL/DOUBLE.
        "FLOAT4", "FLOAT8", "HALF", "HALFFLOAT", "FLOAT16", "FLOAT32", "FLOAT64",
        "BINARY_FLOAT", "BINARY_DOUBLE", "FLOAT",
        # Foreign VECTOR/BIT/ENUM/MONEY/YEAR/MEDIUMINT → ddl_type SSOT.
        "VECTOR", "HALFVEC", "SPARSEVEC",
        "BIT", "BOOL", "TINYINT",
        "ENUM", "SET",
        "MONEY", "SMALLMONEY", "CURRENCY",
        "YEAR", "MEDIUMINT",
    }),
    "clickhouse": frozenset({
        "DECIMAL", "NUMERIC", "NUMBER", "BIGNUMERIC", "BIGDECIMAL",
        "BINARY", "VARBINARY", "BYTES", "VARBYTE", "BYTEA", "FIXED",
        "JSONB", "UUID", "SUPER", "VARIANT", "UNIQUEIDENTIFIER", "NVARCHAR2",
        "DATETIME", "DATETIME2", "TIMESTAMP", "TIMESTAMPTZ", "DATETIMEOFFSET",
        "SMALLDATETIME", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ",
        "TIMETZ", "YEAR",
        # Foreign IEEE → Float32 / Float64. Keep Float32/Float64.
        "FLOAT4", "FLOAT8", "REAL", "HALF", "HALFFLOAT", "FLOAT16", "FLOAT32",
        "FLOAT64", "BINARY_FLOAT", "BINARY_DOUBLE", "DOUBLE", "FLOAT",
        # Foreign VECTOR/BIT/ENUM/MONEY/YEAR/MEDIUMINT → ddl_type SSOT.
        "VECTOR", "HALFVEC", "SPARSEVEC",
        "BIT", "BOOL", "TINYINT",
        "ENUM", "SET",
        "MONEY", "SMALLMONEY", "CURRENCY",
        "YEAR", "MEDIUMINT",
        "BOOLEAN",
    }),
    "trino": frozenset({
        "DECIMAL", "NUMERIC", "NUMBER", "BIGNUMERIC", "BIGDECIMAL",
        "BINARY", "VARBINARY", "BYTES", "VARBYTE", "BYTEA", "FIXED",
        "JSONB", "UUID", "SUPER", "VARIANT", "UNIQUEIDENTIFIER", "NVARCHAR2",
        "DATETIME", "DATETIME2", "DATETIME64", "DATETIMEOFFSET", "SMALLDATETIME",
        "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ", "TIMETZ", "YEAR",
        "FLOAT4", "FLOAT8", "HALF", "HALFFLOAT", "FLOAT16", "FLOAT32", "FLOAT64",
        "BINARY_FLOAT", "BINARY_DOUBLE", "DOUBLE", "FLOAT",
        # Foreign VECTOR/BIT/ENUM/MONEY/YEAR/MEDIUMINT → ddl_type SSOT.
        "VECTOR", "HALFVEC", "SPARSEVEC",
        "BIT", "BOOL", "TINYINT",
        "ENUM", "SET",
        "MONEY", "SMALLMONEY", "CURRENCY",
        "YEAR", "MEDIUMINT",
        "BOOLEAN",
    }),
    "presto": frozenset({
        "DECIMAL", "NUMERIC", "NUMBER", "BIGNUMERIC", "BIGDECIMAL",
        "BINARY", "VARBINARY", "BYTES", "VARBYTE", "BYTEA", "FIXED",
        "JSONB", "UUID", "SUPER", "VARIANT", "UNIQUEIDENTIFIER", "NVARCHAR2",
        "DATETIME", "DATETIME2", "DATETIME64", "DATETIMEOFFSET", "SMALLDATETIME",
        "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ", "TIMETZ", "YEAR",
        "FLOAT4", "FLOAT8", "HALF", "HALFFLOAT", "FLOAT16", "FLOAT32", "FLOAT64",
        "BINARY_FLOAT", "BINARY_DOUBLE", "DOUBLE", "FLOAT",
        # Foreign VECTOR/BIT/ENUM/MONEY/YEAR/MEDIUMINT → ddl_type SSOT.
        "VECTOR", "HALFVEC", "SPARSEVEC",
        "BIT", "BOOL", "TINYINT",
        "ENUM", "SET",
        "MONEY", "SMALLMONEY", "CURRENCY",
        "YEAR", "MEDIUMINT",
        "BOOLEAN",
    }),
    "generic_sql": frozenset({
        "DECIMAL", "NUMERIC", "NUMBER", "BIGNUMERIC", "BIGDECIMAL",
        "FLOAT4", "FLOAT8", "HALF", "HALFFLOAT", "FLOAT16", "FLOAT32", "FLOAT64",
        "BINARY_FLOAT", "BINARY_DOUBLE", "REAL", "DOUBLE", "FLOAT",
        # Foreign VECTOR/BIT/ENUM/MONEY/YEAR/MEDIUMINT → ddl_type SSOT.
        "VECTOR", "HALFVEC", "SPARSEVEC",
        "BIT", "BOOL", "TINYINT",
        "ENUM", "SET",
        "MONEY", "SMALLMONEY", "CURRENCY",
        "YEAR", "MEDIUMINT",
        "BOOLEAN",
    }),
    # SQLite has no true fixed-point type. DECIMAL/NUMERIC/NUMBER stamps get
    # NUMERIC affinity and silently store high-precision values as IEEE real.
    # Rematerialize via ddl_type → TEXT (Map≡CREATE honesty).
    # Foreign binary typmods → BLOB (SQLite ignores length; no affinity invent).
    # UUID/JSON/TIMESTAMP/GUID/… also get NUMERIC affinity (not INT/CHAR/CLOB/
    # BLOB/REAL) — digit-looking payloads become integer/real/inf. Rematerialize
    # to TEXT/INTEGER SSOT.
    "sqlite": frozenset({
        "DECIMAL", "NUMERIC", "NUMBER", "BIGNUMERIC", "BIGDECIMAL",
        "MONEY", "SMALLMONEY", "CURRENCY",
        "BINARY", "VARBINARY", "BYTES", "VARBYTE", "BYTEA", "FIXED",
        "UUID", "UNIQUEIDENTIFIER", "GUID", "OBJECTID",
        "JSON", "JSONB", "VARIANT", "SUPER", "HSTORE", "CITEXT", "AVRO",
        "TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ",
        "TIMETZ", "DATETIME", "DATETIME2", "DATETIME64", "DATETIMEOFFSET",
        "SMALLDATETIME", "DATE", "TIME", "YEAR",
        "BOOLEAN", "BOOL", "BIT",
        "ENUM", "SET", "INET", "CIDR", "VECTOR", "STRING",
        "STRUCT", "MAP", "RECORD", "ARRAY",
        # Foreign IEEE aliases → REAL (SQLite affinity SSOT). Keep REAL.
        "FLOAT4", "FLOAT8", "HALF", "HALFFLOAT", "FLOAT16", "FLOAT32", "FLOAT64",
        "BINARY_FLOAT", "BINARY_DOUBLE", "DOUBLE", "FLOAT",
        # Foreign VECTOR/BIT/ENUM/MONEY/YEAR/MEDIUMINT → ddl_type SSOT.
        "VECTOR", "HALFVEC", "SPARSEVEC",
        "BIT", "BOOL", "TINYINT",
        "ENUM", "SET",
        "MONEY", "SMALLMONEY", "CURRENCY",
        "YEAR", "MEDIUMINT",
        "BOOLEAN",
    }),
}




def _dest_spells_string_as_string(db: str) -> bool:
    """True when ``STRING`` is the destination's own create-new string wire.

    ``STRING`` is a physical type on BigQuery / Databricks / Spanner / Hive-class
    engines and a logical alias everywhere else. Passing the alias through as
    CREATE DDL emitted ``"_df_lsn" string`` on PostgreSQL, which aborted the
    transaction and failed the whole CDC run. Asked of the dialect table rather
    than a hand-kept engine list so a new destination cannot regress it.
    """
    if not db:
        return True
    wire = ddl_type(db, LOGICAL_STRING) or ""
    return wire.strip().upper().split("(", 1)[0].strip() == "STRING"


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
        # Oracle character-length semantics (``VARCHAR2(64 BYTE)``) are physical
        # only on Oracle. Everywhere else the unit is a syntax error, so the
        # carrier must be rematerialized rather than pasted into the CREATE.
        if re.search(r"\(\s*\d+\s+(BYTE|CHAR)\s*\)$", upper) and db not in {
            "oracle",
            "oracledb",
        }:
            return False
        # Valued MySQL ENUM/SET is native CREATE wire — keep Map stamp.
        if bare_typmod in {"ENUM", "SET"} and db in {"mysql", "mariadb", "tidb"}:
            return True
        # TINYINT(1) is the MySQL boolean synonym — rematerialize via ddl_type.
        if bare_typmod == "TINYINT":
            m_ti = re.match(r"^TINYINT\((\d+)\)$", upper)
            if m_ti and int(m_ti.group(1)) == 1:
                return False
        # MySQL YEAR(4) → YEAR create-new wire (display-width alias).
        if bare_typmod == "YEAR":
            return False
        # BIT(n): n<=1 → boolean polarity (rematerialize); n>1 → bitstring
        # native only on PG/MySQL/DuckDB — elsewhere VARCHAR(n) SSOT.
        if bare_typmod == "BIT":
            m_bit = re.match(r"^BIT\((\d+)\)$", upper)
            if m_bit:
                width = int(m_bit.group(1))
                if width <= 1:
                    return False
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
                    "mysql",
                    "mariadb",
                    "tidb",
                    "duckdb",
                }:
                    return True
                return False
        # VECTOR / HALFVEC / SPARSEVEC typmods always rematerialize to dest
        # ddl_type (Snowflake VECTOR(FLOAT,n), PG vector(n), else text/array).
        if bare_typmod in {"VECTOR", "HALFVEC", "SPARSEVEC"}:
            return False
        # BigQuery: parameterized NUMERIC/BIGNUMERIC Map stamps are native CREATE
        # wire — never rewrite NUMERIC(10,2) → BIGNUMERIC(10,2). Bare NUMERIC
        # still rejects below → BIGNUMERIC. DECIMAL(p,s) rematerializes to
        # BIGNUMERIC(p,s) (BQ has no DECIMAL type name).
        if db == "bigquery" and bare_typmod in {"NUMERIC", "BIGNUMERIC"}:
            return True
        # MySQL ``TIMESTAMP(p)`` is the native instant carrier and the exact
        # spelling INFORMATION_SCHEMA reports for such a column, so it is a
        # physical stamp — not a foreign token to rewrite. Only two producers
        # reach here with it: Map's create-new stamp for a TZ-aware source
        # (_TZ_AWARE_DDL) and the live catalog. Rewriting it to DATETIME(6)
        # retyped an instant column as wall-clock, and the write-time NTZ guard
        # then quarantined every offset-bearing row the column could hold.
        # Bare ``TIMESTAMP`` stays foreign/ambiguous (PostgreSQL and Oracle
        # spell wall-clock that way) and still rematerializes to DATETIME(6).
        if bare_typmod == "TIMESTAMP" and db in {"mysql", "mariadb", "tidb"}:
            return True
        if bare_typmod == "STRING" and not _dest_spells_string_as_string(db):
            return False
        if bare_typmod in reject:
            return False
        return True
    bare = upper.split("(", 1)[0].strip()
    if bare in _PHYSICAL_STAMP_PASS_THROUGH or upper in _PHYSICAL_STAMP_PASS_THROUGH:
        # Refuse pass-through of tokens illegal / non-create-wire on this dest.
        if bare in reject or upper in reject:
            return False
        # DOUBLE PRECISION is a multi-word pass-through token. Keep only on
        # engines whose create-new wire is DOUBLE PRECISION (PG-family /
        # Redshift). Elsewhere rematerialize via ddl_type (MySQL DOUBLE,
        # Snowflake FLOAT, SQLite REAL, …).
        if upper == "DOUBLE PRECISION" and db not in {
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
            return False
        # Ambiguous INTEGER/INT (width unknown) must NOT pass through — those
        # SQL keywords are INT32 on PG/MySQL while ddl_type invents BIGINT.
        # Unambiguous INT4/INT32 keep integer_bit_width==32 and stay physical.
        if bare in {"INTEGER", "INT", "SIGNED"} and integer_bit_width(raw) is None:
            return False
        if bare == "STRING" and not _dest_spells_string_as_string(db):
            return False
        return True
    # Bare FLOAT (mantissa unknown) rematerializes via ddl_type → IEEE-64.
    # Unambiguous FLOAT32 / FLOAT4 / REAL stay physical single-precision.
    if bare == "FLOAT":
        if float_mantissa_bits(raw) is None:
            return False
        return db in {"mysql", "mariadb", "tidb", "sqlserver", "mssql"}
    if specialty_carrier_base(raw) is not None:
        return True
    # Dialect multi-word tokens
    if upper.startswith("TIMESTAMP ") or upper.startswith("TIME WITH"):
        # Rematerialize to dest ddl_type SSOT (MySQL → DATETIME(6), SF →
        # TIMESTAMP_TZ, PG → TIMESTAMPTZ, SQLite → TEXT). Never invent
        # foreign multi-word temporals as CREATE DDL.
        return False
    if upper.startswith("DOUBLE ") or upper.startswith("CHARACTER "):
        # DOUBLE PRECISION handled above when in pass-through set; remaining
        # DOUBLE … forms rematerialize. CHARACTER VARYING → ddl on SQLite /
        # BigQuery (STRING) / other non-native engines.
        if upper.startswith("DOUBLE "):
            return False
        if db in {"sqlite", "bigquery", "spanner", "databricks", "iceberg"}:
            return False
        return True
    return False




# Bare Map stamps that name a string family but declare no width. These are
# Studio/Map defaults, not an operator request for a LOB. TEXT/CLOB/MAX stay
# unbounded (Map≡CREATE). Widthless CHAR is CHAR(1) on MySQL — treat it as a
# string-family alias unless the source itself is pad-fixed CHAR(n).
_WIDTHLESS_VARCHAR_STAMPS = frozenset({
    "VARCHAR",
    "NVARCHAR",
    "VARCHAR2",
    "NVARCHAR2",
    "CHAR",
    "NCHAR",
    "CHARACTER",
    "CHARACTER VARYING",
    "NATIONAL CHAR",
    "NATIONAL CHARACTER",
    "NATIONAL CHARACTER VARYING",
    "STRING",
})
_NATIONAL_STRING_FAMILIES = frozenset({
    "NVARCHAR",
    "NVARCHAR2",
    "NCHAR",
    "NATIONAL CHAR",
    "NATIONAL CHARACTER",
    "NATIONAL CHARACTER VARYING",
})
_FIXED_STRING_FAMILIES = frozenset({
    "CHAR",
    "NCHAR",
    "CHARACTER",
    "NATIONAL CHAR",
    "NATIONAL CHARACTER",
})
_ORACLE_LENGTH_FAMILIES = frozenset({
    "VARCHAR",
    "NVARCHAR",
    "VARCHAR2",
    "NVARCHAR2",
    "CHAR",
    "NCHAR",
})
_MYSQL_CHARSET_DESTS = frozenset({"mysql", "mariadb", "tidb"})
_STRING_QUALIFIER_RE = re.compile(
    r"(?i)^(.+?)((?:\s+(?:CHARACTER\s+SET|CHARSET|COLLATE)\s+\S+)+)$"
)
_LENGTH_SEMANTICS_RE = re.compile(r"\(\s*\d+\s*(CHAR|BYTE)\s*\)", re.I)


def _split_string_qualifiers(stamp: str) -> tuple[str, str]:
    """Split ``VARCHAR COLLATE utf8mb4_bin`` into (base, trailing charset/collate)."""
    text = (stamp or "").strip()
    matched = _STRING_QUALIFIER_RE.match(text)
    if not matched:
        return text, ""
    return matched.group(1).strip(), matched.group(2)


def _fold_widthless_string_stamp(stamp: str) -> str:
    folded = re.sub(r"\s+COLLATE\s+\S+", "", stamp, flags=re.I)
    folded = re.sub(r"\s+CHARACTER\s+SET\s+\S+", "", folded, flags=re.I)
    folded = re.sub(r"\s+CHARSET\s+\S+", "", folded, flags=re.I)
    return re.sub(r"\s+", " ", folded).strip().upper()


def _length_semantics_unit(inferred: str) -> str:
    """Oracle ``(n CHAR)`` / ``(n BYTE)`` unit, else empty."""
    matched = _LENGTH_SEMANTICS_RE.search(strip_identity_qualifier(inferred))
    return matched.group(1).upper() if matched else ""


def _family_for_inherited_width(folded: str, source_type: str) -> str:
    """Map-stamp family + source pad polarity → the token that receives ``(n)``.

    Map owns national/varying polarity (``NVARCHAR`` vs ``VARCHAR``). Source
    owns whether a widthless ``CHAR`` was a pad-fixed request (source is
    CHAR(n)) or a Studio string alias (source is VARCHAR(n) → VARCHAR(n)).
    """
    national = folded in _NATIONAL_STRING_FAMILIES or is_national_string_carrier(folded)
    if folded in _FIXED_STRING_FAMILIES:
        if is_fixed_width_char_carrier(source_type):
            return "NCHAR" if national else "CHAR"
        return "NVARCHAR" if national else "VARCHAR"
    if folded in {"CHARACTER VARYING", "NATIONAL CHARACTER VARYING"}:
        return "NVARCHAR" if national else "VARCHAR"
    if folded == "STRING":
        return "STRING"
    if folded == "VARCHAR2":
        return "NVARCHAR2" if national else "VARCHAR2"
    if folded == "NVARCHAR2":
        return "NVARCHAR2"
    if folded == "NVARCHAR":
        return "NVARCHAR"
    return "NVARCHAR" if national else "VARCHAR"


def inherit_measured_string_width(
    map_stamp: str | None,
    source_type: str | None,
    *,
    dest_db: str = "",
) -> str:
    """CREATE-new dest DDL: a widthless VARCHAR stamp inherits source ``(n)``.

    Competitor pain: Fivetran auto-promotes types; Airbyte lands TEXT and the
    unique key disappears. A measured ``VARCHAR(255)`` source must not become
    MySQL ``TEXT`` (unindexable without an invented prefix) just because Map
    stamped bare ``VARCHAR``.

    Rules (Map≡CREATE, never silent LOB invent):
    - Bounded Map stamps (``VARCHAR(10)``) stay as stamped.
    - Explicit LOB stamps (``TEXT``, ``CLOB``, ``LONGTEXT``, ``VARCHAR(MAX)``)
      stay unbounded.
    - Widthless family stamps inherit source width onto the **Map family**
      (national / STRING / VARCHAR2 polarity is the operator's), then project
      through ``_string_ddl_for_dest`` when ``dest_db`` is known so over-cap
      widths become LONGTEXT/CLOB/MAX instead of illegal ``VARCHAR(n)``.
    """
    raw_stamp = (map_stamp or "").strip()
    if not raw_stamp:
        return ""
    # Split charset/collate before strip_identity_qualifier — that helper
    # drops COLLATE, which would silently un-pin a case-sensitive unique key.
    base_raw, qualifiers = _split_string_qualifiers(raw_stamp)
    stamp = strip_identity_qualifier(base_raw).strip()
    src = strip_identity_qualifier(source_type).strip() if source_type else ""
    # ``stamp`` is the identity/charset-stripped form used only to decide
    # whether a width can be inherited. Every path that inherits nothing hands
    # back the stamp as given: returning the stripped form dropped
    # ``GENERATED BY DEFAULT AS IDENTITY`` off a create-new integer column, so
    # the destination was created without the key generator.
    if not stamp:
        return raw_stamp
    if parse_string_carrier_width(stamp) is not None:
        return raw_stamp
    folded = _fold_widthless_string_stamp(stamp)
    if folded not in _WIDTHLESS_VARCHAR_STAMPS:
        return raw_stamp
    width = parse_string_carrier_width(src)
    if width is None:
        return raw_stamp
    family = _family_for_inherited_width(folded, src)
    unit = _length_semantics_unit(src)
    if unit and family in _ORACLE_LENGTH_FAMILIES:
        synthetic = f"{family}({width} {unit})"
    else:
        synthetic = f"{family}({width})"
    db = _normalize_dest_db(dest_db) if dest_db else ""
    inherited = synthetic
    if db:
        projected = _string_ddl_for_dest(db, synthetic)
        if projected:
            inherited = projected
    if qualifiers and (not db or db in _MYSQL_CHARSET_DESTS):
        return f"{inherited}{qualifiers}"
    return inherited


def materialize_dest_ddl(
    db_type: str,
    carrier: str | None,
    source_type: str | None = None,
) -> str:
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

    ``source_type``: when Map stamped a widthless VARCHAR family token, inherit
    the source's measured ``(n)`` so MySQL UNIQUE/PK stay indexable.
    """
    raw = strip_identity_qualifier(carrier).strip()
    db = _normalize_dest_db(db_type)
    if raw and source_type:
        inherited = inherit_measured_string_width(raw, source_type, dest_db=db)
        if inherited:
            raw = inherited
    if not raw:
        return ddl_type(db_type, "VARCHAR")
    upper = raw.upper()
    if db == "iceberg":
        # Rematerialize SQL/Spark ARRAY / VECTOR spellings to list<…>.
        # Native Iceberg ``list<float>`` / ``list<int>`` are physical stamps —
        # pass through (Property 1: do not rewrite dest-native IEEE-32 leaves).
        if (
            upper.startswith("ARRAY<")
            or upper.startswith("ARRAY(")
            or upper.endswith("[]")
            or normalize_logical_type(raw) == LOGICAL_VECTOR
        ):
            return ddl_type(db, raw)
    # Property 1: nested ARRAY/LIST whose leaf is ambiguous INTEGER/INT/FLOAT
    # must rematerialize — never pass through INT32/IEEE-32 element wire.
    # Iceberg-native ``list<float>`` / ``list<int>`` are physical stamps (keep).
    array_el = parse_array_element(raw)
    if array_el is not None:
        el = strip_identity_qualifier(array_el).strip()
        el_u = el.upper()
        iceberg_native_list = db == "iceberg" and upper.startswith("LIST")
        if not iceberg_native_list and (
            el_u in {"INTEGER", "INT", "FLOAT", "SIGNED"}
            or el in {LOGICAL_INTEGER, LOGICAL_FLOAT}
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




def integer_width_carrier(native: str | None) -> str | None:
    """Width-preserving integer carrier for introspect / Map / DDL invent.

    SSOT for native→carrier integer spelling. Returns ``None`` when ``native``
    is not an integer family type. Bare / ambiguous ``integer`` / ``INTEGER`` /
    ``INT`` → ``BIGINT`` (never-narrower invent default). Unambiguous INT32
    spellings (``INT4`` / ``INT32`` / ``SERIAL``) stay 32-bit carriers.
    """
    raw = strip_identity_qualifier(native)
    if not raw:
        return None
    if normalize_logical_type(raw) != LOGICAL_INTEGER:
        # YEAR / SERIAL handled as integer logical — normalize covers them.
        # Unsigned BIGINT travels as DECIMAL logical — keep token.
        if _is_unsigned_integer_decimal_carrier(raw):
            return strip_identity_qualifier(raw).strip().upper().replace("  ", " ")
        return None
    upper = raw.upper().strip()
    unsigned = "UNSIGNED" in upper or bool(re.search(r"\bUINT\d*\b", upper))
    # ClickHouse case-sensitive wires.
    m_ch = re.match(r"^(U?Int)(8|16|32|64)\b", raw.strip())
    if m_ch:
        return m_ch.group(0)
    width = integer_bit_width(raw)
    if width is None:
        # Bare / ambiguous family → safe 64-bit carrier.
        return "BIGINT"
    # Map width → canonical SQL carrier (UNSIGNED polarity preserved).
    if width <= 8 or (unsigned and width == 9):
        return "TINYINT UNSIGNED" if unsigned else "TINYINT"
    if width == 9 and not unsigned:
        return "TINYINT"
    if width <= 16 or (unsigned and width == 17):
        return "SMALLINT UNSIGNED" if unsigned else "SMALLINT"
    if "MEDIUMINT" in upper or width in {24, 25}:
        return "MEDIUMINT UNSIGNED" if unsigned else "MEDIUMINT"
    if width <= 32 or (unsigned and width == 33):
        # Prefer unambiguous INT4 tokens (Property 1).
        if unsigned:
            return "INT4 UNSIGNED"
        if re.search(r"\bINT32\b", upper):
            return "INT32"
        return "INT4"
    # 64-bit (and unsigned 64 → DECIMAL path usually; if still integer logical)
    if unsigned:
        return "BIGINT UNSIGNED"
    if "BIGSERIAL" in upper:
        return "BIGSERIAL"
    if "SERIAL" in upper and "BIG" not in upper and "SMALL" not in upper:
        return "SERIAL"
    return "BIGINT"




def float_width_carrier(native: str | None) -> str | None:
    """Width-preserving float carrier — REAL/FLOAT32 vs DOUBLE/FLOAT64.

    Bare logical ``float`` → ``DOUBLE`` (never-narrower invent default).
    Explicit single-precision tokens stay ``REAL`` / ``FLOAT`` / ``FLOAT32``.
    """
    raw = strip_identity_qualifier(native)
    if not raw:
        return None
    if normalize_logical_type(raw) != LOGICAL_FLOAT:
        return None
    if raw.strip().lower() == LOGICAL_FLOAT:
        return "DOUBLE"
    upper = re.sub(r"\bUNSIGNED\b", "", raw.upper()).strip()
    compact = upper.replace(" ", "")
    if compact in {"HALF", "HALFFLOAT", "FLOAT16"} or compact.startswith("HALFFLOAT"):
        return "FLOAT16"
    if compact in {"REAL", "FLOAT4", "FLOAT32", "BINARY_FLOAT"} or compact.startswith("REAL("):
        if compact == "BINARY_FLOAT":
            return "BINARY_FLOAT"
        if compact in {"FLOAT32"}:
            return "FLOAT32"
        return "REAL"
    m = re.match(r"^FLOAT\((\d+)\)$", compact)
    if m:
        return f"FLOAT({m.group(1)})"
    if compact in {
        "DOUBLE",
        "DOUBLEPRECISION",
        "FLOAT8",
        "FLOAT64",
        "BINARY_DOUBLE",
    } or compact.startswith("DOUBLE"):
        if compact == "BINARY_DOUBLE":
            return "BINARY_DOUBLE"
        if compact == "FLOAT64":
            return "FLOAT64"
        if "PRECISION" in upper:
            return "DOUBLE PRECISION"
        return "DOUBLE"
    # Bare FLOAT (any case) — ambiguous; invent IEEE-64.
    if compact == "FLOAT" or compact.startswith("FLOAT"):
        return "DOUBLE"
    return "DOUBLE"




def ddl_invent_never_narrower_than_table(
    dest_db: str,
    logical: str,
) -> bool:
    """True when ``ddl_type(dest, logical)`` is at least as wide as ``DDL_TYPES``.

    Audit P0 / harness gate: bare logical invent must not undercut the
    destination table default (e.g. logical integer → INT32 while DDL_TYPES
    says BIGINT/Int64/long).
    """
    table = (DDL_TYPES.get(dest_db) or {}).get(logical)
    if not table:
        return True
    invented = ddl_type(dest_db, logical)
    if logical == LOGICAL_INTEGER:
        tw = integer_bit_width(table)
        iw = integer_bit_width(invented)
        # Table BIGINT / Int64 → treat missing width on dialect tokens via parse.
        def _wide_token(s: str) -> bool:
            u = (s or "").upper()
            if u.strip() in {"N", "LONG", "INT64", "BIGINT", "I64"}:
                return True
            return any(
                tok in u for tok in ("BIGINT", "INT64", "NUMBER(38", "INT8")
            ) or bool(re.search(r"\bLONG\b", u))

        if tw is None:
            tw = 64 if _wide_token(str(table)) else 32
        if iw is None:
            iw = 64 if _wide_token(str(invented)) else 32
        return iw >= tw
    if logical == LOGICAL_FLOAT:
        tb = float_mantissa_bits(table, dest_db=dest_db)
        ib = float_mantissa_bits(invented, dest_db=dest_db)
        if tb is None:
            tb = 53
        if ib is None:
            ib = 53
        return ib >= tb
    return True


__all__ = [
    'normalize_logical_type',
    'ddl_type',
    'create_new_mapping_target_type',
    'refuse_create_new_numeric_collapse',
    'materialize_dest_ddl',
    'inherit_measured_string_width',
    'integer_width_carrier',
    'float_width_carrier',
    'ddl_invent_never_narrower_than_table',
    'promote_create_new_temporal_stamp',
]


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
    from services.type_system import (
        DOCUMENT_INSTANT_FRACTIONAL_DIGITS,
        SNOWFLAKE_DEFAULT_TIMESTAMP_FRACTIONAL_DIGITS,
        SNOWFLAKE_UNAVOIDABLE_FSP_FLOOR,
        _SNOWFLAKE_BARE_TIMESTAMP_SPELLINGS,
        destination_temporal_fractional_digits,
        is_document_instant_token,
    )

    src_l = normalize_logical_type(source_type)
    tgt_l = normalize_logical_type(target_type)
    if is_document_instant_token(dest_db, target_type):
        # Millisecond carrier spelled ``date``. Restate it as a datetime of that
        # precision so the comparison below reports the truncation that actually
        # happens instead of stopping at the date-family mismatch.
        target_type = f"DATETIME({DOCUMENT_INSTANT_FRACTIONAL_DIGITS})"
        tgt_l = LOGICAL_DATETIME
    if src_l not in {LOGICAL_TIME, LOGICAL_DATETIME} or tgt_l not in {
        LOGICAL_TIME,
        LOGICAL_DATETIME,
    }:
        return False
    tgt_u = strip_identity_qualifier(target_type).upper().strip()
    src_u = strip_identity_qualifier(source_type).upper().strip()
    if tgt_u == "SMALLDATETIME" and src_u != "SMALLDATETIME" and src_l == LOGICAL_DATETIME:
        return True
    tgt_p = destination_temporal_fractional_digits(target_type, dest_db=dest_db)
    src_p = parse_temporal_fractional_precision(source_type)
    if tgt_p is None:
        return False
    if src_p is None:
        # SQL Server bare DATETIME2 defaults to precision 7 — never treat as
        # unknown and soft-pass DATETIME2→DATETIME (≈3.33ms round).
        bare_src = re.sub(r"\s*\(\s*\d+\s*\)", "", src_u).strip()
        if bare_src in {"DATETIME2", "DATETIMEOFFSET"}:
            src_p = 7
        elif bare_src in _SNOWFLAKE_BARE_TIMESTAMP_SPELLINGS:
            if bare_src == re.sub(r"\s*\(\s*\d+\s*\)", "", tgt_u).strip():
                # Both sides carry the same unparameterized declaration, so the
                # column keeps whatever that carrier keeps — it cannot truncate
                # itself. The underscore spellings are also how introspection
                # reports a zoneless carrier on MySQL/PostgreSQL, so reading the
                # source as Snowflake's nanosecond ceiling while the destination
                # resolves through ``dest_db`` invented a fidelity collapse
                # (``TIMESTAMP_NTZ → TIMESTAMP_NTZ``) on routes that never
                # touched Snowflake. One declaration, one precision rule.
                return False
            # Snowflake declares TIMESTAMP_NTZ/LTZ/TZ with no typmod in its
            # catalog but stores nanoseconds (default scale 9). Reading the
            # absent typmod as "unknown" green-lit Snowflake→MySQL DATETIME
            # (FSP 0), which drops every fractional second on write. These
            # spellings exist in no other dialect, so the default is safe to
            # apply without knowing the source engine.
            #
            # It is a declared ceiling rather than observed nanoseconds, so it
            # accuses only loss an operator can act on. Every mainstream
            # destination clamps at microseconds or better, so sub-microsecond
            # narrowing is unavoidable and reporting it would put a Risk
            # Contract on every Snowflake timestamp column and teach operators
            # to sign unread. Landing at millisecond or whole-second FSP is the
            # fixable case: widen the destination column.
            if tgt_p >= SNOWFLAKE_UNAVOIDABLE_FSP_FLOOR:
                return False
            src_p = SNOWFLAKE_DEFAULT_TIMESTAMP_FRACTIONAL_DIGITS
        else:
            return False
    return src_p > tgt_p
