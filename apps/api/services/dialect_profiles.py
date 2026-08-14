"""Dialect profiles — single source of truth for cross-system SQL conventions.

Industry alignment (Informatica / Airbyte / Fivetran class)
----------------------------------------------------------
Informatica maps **native → transformation (logical) → native** so Oracle→SQL Server
does not inherit Oracle naming. Airbyte/Fivetran keep destination-specific writers
but a shared catalog/type layer.

Datawrap mirrors that:

1. ``type_system.py`` — logical types (STRING/INTEGER/…) + per-dialect DDL.
2. ``dialect_profiles.py`` — default schema, case fold, quote style (this module).
3. ``sql_identifiers.quote_table_ref`` — physical ``schema.table`` quoting.
4. Mapping / preflight — coerce fail-fast; quarantine bad rows (never silent drop).

Never apply Postgres defaults (``public``, lowercase fold) to Snowflake,
SQL Server, Oracle, BigQuery, or MySQL.

All resolve/probe/quote/preview paths must call these helpers instead of
hardcoding ``\"public\"``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

FoldMode = Literal["lower", "upper", "none"]
QuoteStyle = Literal["double", "backtick", "bracket", "none"]


@dataclass(frozen=True)
class DialectProfile:
    """Physical naming rules for one SQL/warehouse dialect."""

    driver: str
    # None = schema/namespace not used (MySQL database-as-catalog, SQLite, …)
    default_schema: str | None
    uses_schema: bool
    fold: FoldMode
    quote: QuoteStyle
    # Human label for UI (schema vs dataset vs namespace)
    namespace_label: str = "schema"


# Canonical profiles — extend here, not in adapters/UI/routers.
DIALECT_PROFILES: dict[str, DialectProfile] = {
    "postgresql": DialectProfile("postgresql", "public", True, "lower", "double"),
    "postgres": DialectProfile("postgres", "public", True, "lower", "double"),
    "redshift": DialectProfile("redshift", "public", True, "lower", "double"),
    "pgvector": DialectProfile("pgvector", "public", True, "lower", "double"),
    "snowflake": DialectProfile("snowflake", "PUBLIC", True, "upper", "double"),
    "mysql": DialectProfile("mysql", None, False, "none", "backtick"),
    "mariadb": DialectProfile("mariadb", None, False, "none", "backtick"),
    "sqlserver": DialectProfile("sqlserver", "dbo", True, "none", "bracket"),
    "mssql": DialectProfile("mssql", "dbo", True, "none", "bracket"),
    "oracle": DialectProfile("oracle", None, True, "upper", "double"),  # often username
    "bigquery": DialectProfile("bigquery", "dataflow", True, "none", "backtick", "dataset"),
    "sqlite": DialectProfile("sqlite", None, False, "none", "double"),
    "duckdb": DialectProfile("duckdb", "main", True, "none", "double"),
    "databricks": DialectProfile("databricks", "default", True, "none", "backtick"),
    "presto": DialectProfile("presto", "public", True, "none", "double"),
    "trino": DialectProfile("trino", "default", True, "none", "double"),
    "generic_sql": DialectProfile("generic_sql", None, True, "none", "double"),
}

_ALIASES: dict[str, str] = {
    "mssql+pyodbc": "sqlserver",
    "postgresql+psycopg2": "postgresql",
    "mysql+pymysql": "mysql",
    "oracle+oracledb": "oracle",
    "bq": "bigquery",
}


def normalize_driver(driver: str | None) -> str:
    raw = (driver or "").strip().lower()
    if not raw:
        return ""
    return _ALIASES.get(raw, raw)


def dialect_profile(driver: str | None) -> DialectProfile:
    key = normalize_driver(driver)
    if key in DIALECT_PROFILES:
        return DIALECT_PROFILES[key]
    # Unknown SQL-ish engines: no Postgres leak — require explicit schema.
    return DialectProfile(key or "unknown", None, True, "none", "double")


def default_schema_for(driver: str | None) -> str | None:
    """Default namespace for empty schema fields (None = omit / not applicable)."""
    return dialect_profile(driver).default_schema


def uses_schema(driver: str | None) -> bool:
    return dialect_profile(driver).uses_schema


def fold_identifier(driver: str | None, name: str | None) -> str:
    """Apply dialect case-fold rules (Snowflake UPPER, PG lower, etc.).

    Mixed-case names are preserved (intentional quoted identifiers).
    """
    raw = (name or "").strip()
    if not raw:
        return raw
    profile = dialect_profile(driver)
    if profile.fold == "none":
        return raw
    if raw != raw.upper() and raw != raw.lower():
        return raw  # mixed case — preserve
    if profile.fold == "upper":
        return raw.upper()
    if profile.fold == "lower":
        return raw.lower()
    return raw


def normalize_schema(
    driver: str | None,
    schema: str | None,
    *,
    username: str | None = None,
) -> str | None:
    """Resolve schema/namespace for a dialect.

    - Empty → dialect default (or Oracle username when available)
    - Leaked Postgres ``public`` on non-PG dialects → treat as unset (use dialect default)
    - MySQL / SQLite → None (no schema layer)
    """
    profile = dialect_profile(driver)
    if not profile.uses_schema:
        return None
    raw = (schema or "").strip()
    # Old Transfer Studio / API defaults sent Postgres ``public`` for every dest.
    # That must not stick on Snowflake / SQL Server / BigQuery / Oracle / …
    _pg_family = {
        "postgresql",
        "postgres",
        "redshift",
        "pgvector",
        "presto",
    }
    if raw.lower() == "public" and profile.driver not in _pg_family:
        raw = ""
    if not raw:
        if profile.driver == "oracle" and (username or "").strip():
            return fold_identifier(driver, username)
        return profile.default_schema
    return fold_identifier(driver, raw)


def schema_from_cfg(
    driver: str | None,
    cfg: dict | None = None,
    *,
    schema: str | None = None,
    username: str | None = None,
) -> str:
    """Convenience for writers/readers: dialect schema as a string (never Postgres leak)."""
    raw = schema
    user = username
    if cfg is not None:
        if raw is None:
            raw = cfg.get("schema")
        if user is None:
            user = cfg.get("username")
    return normalize_schema(driver, raw, username=user) or ""


def catalog_namespace(
    driver: str | None,
    cfg: dict | None = None,
    *,
    schema: str | None = None,
) -> str:
    """The name the engine catalog uses to find this object.

    Distinct from :func:`schema_from_cfg`, which is the SQL qualifier in front
    of a table. MySQL has no schema layer, so that helper correctly returns
    empty — but ``information_schema.columns.table_schema`` is the *database
    name*. Looking up ``public`` on MySQL matches nothing and silently falls
    back to mapping hints, which is how ``timestamp`` (an instant on MySQL, a
    wall clock on PostgreSQL) gets misclassified.

    This is the single lookup rule every catalog reader should use. Extend the
    per-dialect branch when a new engine's catalog is keyed differently; do
    not copy a ``or "public"`` fallback into the caller.
    """
    cfg = cfg or {}
    profile = dialect_profile(driver)
    hinted = schema if schema is not None else cfg.get("schema")
    if profile.uses_schema:
        return schema_from_cfg(driver, cfg, schema=hinted if isinstance(hinted, str) or hinted is None else str(hinted))
    key = normalize_driver(driver)
    if key in {"mysql", "mariadb"}:
        database = str(cfg.get("database") or "").strip()
        leaked = str(hinted or "").strip()
        # A leaked Postgres default is not a MySQL database.
        if leaked.lower() in {"", "public"}:
            return database
        return leaked or database
    return str(cfg.get("database") or hinted or "").strip()


# Dialects that reject ``LIMIT``/``OFFSET`` and use the SQL:2008 form instead.
# Oracle (12c+), SQL Server (2012+), DB2 and Derby all parse
# ``OFFSET n ROWS FETCH NEXT m ROWS ONLY``; emitting ``LIMIT`` there raises
# ORA-03047 / incorrect-syntax and makes the source unreadable.
_FETCH_FIRST_DIALECTS: frozenset[str] = frozenset(
    {
        "oracle",
        "oracle+oracledb",
        "oracle+cx_oracle",
        "autonomous_database",
        "amazon_rds_oracle",
        "mssql",
        "sqlserver",
        "microsoft_sql_server",
        "azure_sql_database",
        "azure_sql_managed_instance",
        "synapse_analytics",
        "azure_synapse_dedicated",
        "azure_synapse_serverless",
        "google_cloud_sql_sql_server",
        "amazon_rds_sql_server",
        "db2",
        "ibm_db2",
        "db2_luw",
        "db2_iseries",
        "derby",
    }
)

_ORACLE_LIKE: frozenset[str] = frozenset(
    {"oracle", "oracle+oracledb", "oracle+cx_oracle", "autonomous_database", "amazon_rds_oracle"}
)


def uses_fetch_first_pagination(driver: str | None) -> bool:
    """True when the dialect needs ``OFFSET … FETCH NEXT`` instead of ``LIMIT``."""
    return normalize_driver(driver) in _FETCH_FIRST_DIALECTS or (driver or "").strip().lower() in _FETCH_FIRST_DIALECTS


def is_oracle_like(driver: str | None) -> bool:
    raw = (driver or "").strip().lower()
    return normalize_driver(driver) in _ORACLE_LIKE or raw in _ORACLE_LIKE


_UPPER_FOLDING_DIALECTS: frozenset[str] = frozenset(
    {
        "oracle",
        "oracle+oracledb",
        "oracle+cx_oracle",
        "autonomous_database",
        "amazon_rds_oracle",
        "snowflake",
        "db2",
        "ibm_db2",
        "db2_luw",
        "db2_iseries",
        "derby",
        "h2",
        "hsqldb",
        "firebird",
        "teradata",
    }
)


def folds_identifiers_upper(driver: str | None) -> bool:
    """True when unquoted identifiers are stored folded to UPPER CASE.

    The canonical profile table only carries the engines it lists; the
    Oracle/DB2 SKU aliases and other SQL-standard folders resolve here so every
    caller agrees on identifier case.
    """
    raw = (driver or "").strip().lower()
    if raw in _UPPER_FOLDING_DIALECTS or normalize_driver(driver) in _UPPER_FOLDING_DIALECTS:
        return True
    return dialect_profile(driver).fold == "upper"


def denormalize_result_key(driver: str | None, name: str) -> str:
    """Physical spelling of a DBAPI result key, for use inside quoted SQL.

    Drivers for upper-folding engines (Oracle, DB2, Snowflake) hand back
    case-insensitive column names lowercased, so quoting the key verbatim
    references a column that does not exist (ORA-00904). A name that is already
    mixed case was created quoted and is returned exactly as stored.
    """
    if not name:
        return name
    if not folds_identifiers_upper(driver):
        return name
    return name.upper() if name.islower() else name


def page_clause(driver: str | None, offset: int, limit: int) -> str:
    """Dialect-correct row-window clause (caller supplies its own ORDER BY)."""
    if uses_fetch_first_pagination(driver):
        return f"OFFSET {int(offset)} ROWS FETCH NEXT {int(limit)} ROWS ONLY"
    return f"LIMIT {int(limit)} OFFSET {int(offset)}"


def zero_row_probe_sql(driver: str | None, qualified: str) -> str:
    """A ``SELECT`` that returns column metadata but no rows, on any dialect."""
    raw = (driver or "").strip().lower()
    if normalize_driver(driver) in {"mssql", "sqlserver"} or raw in {
        "mssql",
        "sqlserver",
        "microsoft_sql_server",
        "azure_sql_database",
        "azure_sql_managed_instance",
        "synapse_analytics",
        "azure_synapse_dedicated",
        "azure_synapse_serverless",
        "google_cloud_sql_sql_server",
        "amazon_rds_sql_server",
    }:
        return f"SELECT TOP 0 * FROM {qualified}"  # nosec B608
    if uses_fetch_first_pagination(driver):
        # ``LIMIT 0`` is a syntax error on Oracle/DB2; ``WHERE 1=0`` is universal.
        return f"SELECT * FROM {qualified} WHERE 1=0"  # nosec B608
    return f"SELECT * FROM {qualified} LIMIT 0"  # nosec B608


def quote_char_for(driver: str | None) -> str:
    style = dialect_profile(driver).quote
    if style == "backtick":
        return "`"
    if style == "bracket":
        return "["
    if style == "none":
        return ""
    return '"'
