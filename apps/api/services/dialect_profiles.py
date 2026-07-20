"""Dialect profiles — single source of truth for cross-system SQL conventions.

Industry alignment (Informatica / Airbyte / Fivetran class)
----------------------------------------------------------
Informatica maps **native → transformation (logical) → native** so Oracle→SQL Server
does not inherit Oracle naming. Airbyte/Fivetran keep destination-specific writers
but a shared catalog/type layer.

DataFlow mirrors that:

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


def quote_char_for(driver: str | None) -> str:
    style = dialect_profile(driver).quote
    if style == "backtick":
        return "`"
    if style == "bracket":
        return "["
    if style == "none":
        return ""
    return '"'
