"""Compiled per-column bind route — which wire coercion a DDL type demands.

Bind normalization is decided by two facts only: the destination column's DDL
type and the destination engine. Both are constant for every cell of a column,
yet the decision used to be re-derived per cell through a ~40-branch chain of
regex parses (``ENUM(...)`` member parse, bitstring width, specialty carrier,
logical-type normalize). On a 1M-row × 10-column load that is 10M replays of a
decision that has at most a handful of distinct answers per table, and it was
the dominant cost of the write path.

The route is resolved once per ``(ddl_type, engine)`` and cached. Cells then pay
one cache lookup plus the coercion they actually need. Value-dependent rules
(temporal coercion, Oracle ``''`` → NULL) stay in the caller: only the *type*
facts that steer them are precomputed here.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Final

from connectors.sql_temporal import sql_base_type, sql_type_is_temporal
from services.type_system import (
    LOGICAL_GEOGRAPHY,
    LOGICAL_INTERVAL,
    LOGICAL_MAP,
    LOGICAL_STRING,
    LOGICAL_STRUCT,
    LOGICAL_TEXT,
    is_bitstring_carrier,
    is_varying_bitstring_carrier,
    is_year_carrier,
    normalize_logical_type,
    parse_bitstring_width,
    parse_enum_or_set_ordered_members,
    specialty_carrier_base,
)

# Engine families that share a wire decision. ``startswith("postgres")`` keeps
# vendor forks (postgresql_rds, postgres_flex, …) on the Postgres wire.
_PG_ENGINES: Final[frozenset[str]] = frozenset({
    "postgresql",
    "postgres",
    "pg",
    "cockroachdb",
    "timescaledb",
    "alloydb",
    "yugabytedb",
    "citus",
    "supabase",
    "greenplum",
})
_MSSQL_ENGINES: Final[frozenset[str]] = frozenset({
    "sqlserver",
    "mssql",
    "azure_sql",
    "synapse",
    "azure_synapse",
})
_MYSQL_ENGINES: Final[frozenset[str]] = frozenset({"mysql", "mariadb"})
_MYSQL_BIT_ENGINES: Final[frozenset[str]] = frozenset({"mysql", "mariadb", "tidb"})
_ORACLE_ENGINES: Final[frozenset[str]] = frozenset({
    "oracle",
    "oracledb",
    "oracle_autonomous",
})
_VARIANT_JSON_ENGINES: Final[frozenset[str]] = _PG_ENGINES | {"snowflake", "databricks"}

_INTEGER_TYPES: Final[frozenset[str]] = frozenset({
    "TINYINT",
    "SMALLINT",
    "MEDIUMINT",
    "INT",
    "INTEGER",
    "BIGINT",
    "INT2",
    "INT4",
    "INT8",
    "SERIAL",
    "BIGSERIAL",
    "SMALLSERIAL",
})
_FLOAT_TYPES: Final[frozenset[str]] = frozenset({
    "FLOAT",
    "FLOAT4",
    "FLOAT8",
    "FLOAT16",
    "FLOAT32",
    "FLOAT64",
    "HALF",
    "HALFFLOAT",
    "REAL",
    "DOUBLE",
    "DOUBLE PRECISION",
    "BINARY_FLOAT",
    "BINARY_DOUBLE",
})
_DECIMAL_TYPES: Final[frozenset[str]] = frozenset({
    "DECIMAL",
    "NUMERIC",
    "NUMBER",
    "MONEY",
    "SMALLMONEY",
    "BIGNUMERIC",
    "BIGDECIMAL",
    "CURRENCY",
})
_GEOGRAPHY_TYPES: Final[frozenset[str]] = frozenset({
    "GEOGRAPHY",
    "GEOMETRY",
    "SDO_GEOMETRY",
    "GEOGRAPHY(POINT)",
    "GEOMETRY(POINT)",
})

# One tag per wire coercion. ``PASSTHROUGH`` means the value binds as-is.
PASSTHROUGH: Final[str] = "passthrough"


@dataclass(frozen=True, slots=True)
class BindRoute:
    """Type/engine facts that decide one column's bind wire.

    ``kind`` selects the coercion; the remaining fields carry the arguments that
    coercion needs, so the dispatch does no parsing.
    """

    kind: str
    upper: str
    ddl_arg: str
    eng: str
    # Type-only facts for value-dependent rules the caller still applies.
    oracle_empty_is_null: bool = False
    is_temporal_candidate: bool = True
    # Coercion arguments.
    pg_list: bool = False
    bit_width: int | None = None
    bit_varying: bool = False
    as_int: bool = False
    as_uuid: bool = False
    eui64: bool = False
    json_envelope: bool = False
    multi_range: bool = False


def _is_pg(eng: str) -> bool:
    return eng in _PG_ENGINES or eng.startswith("postgres")


def _oracle_empty_is_null(ddl_type: str, upper: str, eng: str) -> bool:
    """Oracle stores a zero-length VARCHAR2/CHAR as NULL (Oracle/HVR semantics).

    Specialty DDL (INET/CITEXT/TSVECTOR/…) normalizes to a string logical type
    but must keep ``''`` or raise, so it is excluded.
    """
    if not (eng in _ORACLE_ENGINES or eng.startswith("oracle")):
        return False
    if specialty_carrier_base(ddl_type or upper):
        return False
    return not upper or normalize_logical_type(ddl_type or upper) in {
        LOGICAL_STRING,
        LOGICAL_TEXT,
    }


def _kind_for(upper: str, ddl_type: str, eng: str) -> tuple[str, dict]:
    """Resolve the coercion tag and its arguments from type facts alone."""
    # Bitstrings before BINARY so BIT(32) is not bound as bytes, and before the
    # BIT(1) boolean collapse so width polarity survives.
    if (
        is_bitstring_carrier(ddl_type)
        or upper in {"BIT VARYING", "VARBIT"}
        or (upper == "BIT" and parse_bitstring_width(ddl_type) not in {None, 1})
    ):
        return "bitstring", {
            "bit_width": parse_bitstring_width(ddl_type),
            "bit_varying": (
                is_varying_bitstring_carrier(ddl_type)
                or upper in {"BIT VARYING", "VARBIT"}
            ),
        }
    if upper == "BIT":
        # MySQL / SQL Server BIT(1) — boolean polarity ('0' stays 0, not True).
        return "boolean", {"as_int": eng in _MYSQL_BIT_ENGINES}
    if upper == "ROWVERSION":
        return "rowversion", {}
    if upper == "SQL_VARIANT":
        return "sql_variant", {"json_envelope": _is_pg(eng) or eng in _VARIANT_JSON_ENGINES}
    if upper in {"ROWID", "UROWID"}:
        return "rowid", {}
    if upper == "HIERARCHYID":
        return "hierarchyid", {"pg_list": _is_pg(eng)}
    if upper in {"BINARY", "BLOB", "LONGBLOB", "VARBINARY", "BYTEA"}:
        return "binary", {}
    if upper in {"UUID", "UNIQUEIDENTIFIER", "GUID"}:
        # pyodbc UNIQUEIDENTIFIER prefers native uuid.UUID (ODBC 8169 on strings).
        return "uuid", {"as_uuid": eng in _MSSQL_ENGINES}
    if upper in {"PG_LSN", "LSN"}:
        return "pg_lsn", {}
    if upper == "OID":
        return "oid", {}
    if upper in {"TID", "CTID"}:
        return "tid", {}
    if upper == "XID8":
        return "xid", {"as_int": True}
    if upper == "XID":
        return "xid", {"as_int": False}
    if upper == "CID":
        return "cid", {}
    if upper in {"TXID_SNAPSHOT", "PG_SNAPSHOT"}:
        return "txid_snapshot", {}
    if upper in {"INET", "IPV4", "IPV6", "IP"}:
        return "inet", {}
    if upper == "CIDR":
        return "cidr", {}
    if upper in {"MACADDR", "MACADDR8"}:
        return "macaddr", {"eui64": upper == "MACADDR8"}
    if upper in {"XML", "XMLTYPE"}:
        return "xml", {}
    if upper == "JSONPATH":
        return "jsonpath", {}
    if upper == "CITEXT":
        return "citext", {}
    if upper == "LTREE":
        return "ltree", {}
    if upper in {"TSVECTOR", "TSQUERY"}:
        return "tsvector", {}
    if upper == "POINT":
        return "point", {}
    if upper == "BOX":
        return "box", {}
    if upper == "CIRCLE":
        return "circle", {}
    if upper == "LSEG":
        return "lseg", {}
    if upper == "LINE":
        return "line", {}
    if upper == "PATH":
        return "path", {}
    if upper == "POLYGON":
        return "polygon", {}
    if upper == "HSTORE":
        return "hstore", {}
    if "MULTIRANGE" in upper:
        return "range", {"multi_range": True}
    if upper.endswith("RANGE"):  # int4range, daterange, … and bare RANGE
        return "range", {"multi_range": False}
    if (
        upper == "ARRAY"
        or upper.endswith("[]")
        or ((upper.startswith(("ARRAY<", "LIST<"))) and upper.endswith(">"))
        or (
            upper.startswith(("ARRAY(", "LIST(", "NESTED("))
            and upper.endswith(")")
        )
    ):
        return "array", {}
    if upper == "STRUCT" or upper.startswith(
        ("STRUCT<", "RECORD<", "STRUCT(", "ROW(", "OBJECT(", "TUPLE(")
    ):
        return "struct", {}
    if upper == "MAP" or upper.startswith(("MAP<", "MAP(")):
        return "map", {}
    if upper in {"JSON", "JSONB", "VARIANT", "OBJECT", "SUPER"}:
        return "json", {}
    if upper in {"BOOLEAN", "BOOL"}:
        return "boolean", {"as_int": eng in _MYSQL_ENGINES}
    if upper == "TINYINT" and eng in _MYSQL_BIT_ENGINES:
        # MySQL TINYINT(1) convention — same 0/1 int wire as BOOLEAN.
        return "boolean", {"as_int": True}
    if upper in _INTEGER_TYPES:
        # SQL Server TINYINT stays numeric 0–255 (pyodbc/Microsoft) — never bool.
        return "integer", {}
    if upper in _FLOAT_TYPES or upper.startswith("FLOAT("):
        return "float", {}
    if upper in _DECIMAL_TYPES or upper.startswith(
        ("DECIMAL(", "NUMERIC(", "NUMBER(", "BIGNUMERIC(")
    ):
        return "decimal", {}
    logical = normalize_logical_type(ddl_type or upper)
    if logical == LOGICAL_STRUCT:
        return "struct", {}
    if logical == LOGICAL_MAP:
        return "map", {}
    if logical == LOGICAL_INTERVAL or upper.startswith("INTERVAL"):
        return "interval", {}
    if logical == LOGICAL_GEOGRAPHY or upper in _GEOGRAPHY_TYPES:
        return "geography", {}
    return PASSTHROUGH, {}


@lru_cache(maxsize=8192)
def bind_route(ddl_type: str, engine: str) -> BindRoute:
    """Compile the bind route for one destination column type."""
    eng = (engine or "").strip().lower()
    # ENUM/SET first — the paren strip in sql_base_type would drop the member
    # domain that ordinal/bitmask bind depends on.
    enum_set = parse_enum_or_set_ordered_members(ddl_type)
    if enum_set is not None:
        kind, _members = enum_set
        if kind == "ENUM":
            return BindRoute(
                kind="enum",
                upper="ENUM",
                ddl_arg=ddl_type,
                eng=eng,
                is_temporal_candidate=False,
            )
        # Postgres create-new maps SET → TEXT[]; bind as list (psycopg array).
        return BindRoute(
            kind="set",
            upper="SET",
            ddl_arg=ddl_type,
            eng=eng,
            pg_list=_is_pg(eng),
            is_temporal_candidate=False,
        )

    upper = sql_base_type(ddl_type)
    if is_year_carrier(ddl_type) or upper == "YEAR":
        # MySQL YEAR before the INTEGER collapse — string '0' → 2000 polarity.
        return BindRoute(
            kind="year",
            upper=upper,
            ddl_arg=ddl_type or upper,
            eng=eng,
            is_temporal_candidate=False,
        )

    kind, args = _kind_for(upper, ddl_type, eng)
    return BindRoute(
        kind=kind,
        upper=upper,
        ddl_arg=ddl_type or upper,
        eng=eng,
        oracle_empty_is_null=_oracle_empty_is_null(ddl_type, upper, eng),
        is_temporal_candidate=sql_type_is_temporal(ddl_type),
        **args,
    )
