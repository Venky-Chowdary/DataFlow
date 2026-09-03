"""Shared Snowflake identity-COPY helpers.

Dest COUNT is ``destination_row_count`` → ``SELECT COUNT(*)`` through the
native Snowflake driver (``_snowflake_row_count``). Never writer ack,
never ``COPY INTO`` stage ack, never leftover MERGE, never ``CLONE``
(CLONE would copy unmapped columns). Same account+database+schema+table
declines. Cross-account / cross-database declines. Schema may differ on
the same account+database. fakesnow (``account=localhost``) is an
emulator, not a customer-tenant PRODUCTION_SKU.
"""

from __future__ import annotations

from typing import Any

from services.copy_fast_path import FastPathResult, FastPathUnavailable

_SNOWFLAKE_FAMILY = frozenset({
    "snowflake",
    "snowflake_aws",
    "snowflake_azure",
    "snowflake_gcp",
    "snowflake_standard",
    "snowflake_enterprise",
})

_UNSAFE_BASES = frozenset({
    "GEOGRAPHY",
    "GEOMETRY",
    "VECTOR",
    "JAVASCRIPT",
    "REGEX",
})

_SAFE_BASES = frozenset({
    "NUMBER",
    "INT",
    "INTEGER",
    "BIGINT",
    "SMALLINT",
    "TINYINT",
    "BYTEINT",
    "FLOAT",
    "FLOAT4",
    "FLOAT8",
    "DOUBLE",
    "DOUBLEPRECISION",
    "REAL",
    "DECIMAL",
    "NUMERIC",
    "VARCHAR",
    "STRING",
    "TEXT",
    "CHAR",
    "CHARACTER",
    "NCHAR",
    "NVARCHAR",
    "BOOLEAN",
    "BOOL",
    "DATE",
    "TIME",
    "DATETIME",
    "TIMESTAMP",
    "TIMESTAMP_NTZ",
    "TIMESTAMP_LTZ",
    "TIMESTAMP_TZ",
    "TIMESTAMPNTZ",
    "TIMESTAMPLTZ",
    "TIMESTAMPTZ",
    "VARIANT",
    "ARRAY",
    "OBJECT",
    "BINARY",
    "VARBINARY",
    "LONG",
})

_LOCAL_ACCOUNTS = frozenset({
    "localhost",
    "127.0.0.1",
    "::1",
    "0.0.0.0",
    "local",
    "fakesnow",
})


def snowflake_family_name(name: str) -> str:
    n = (name or "").strip().lower()
    if n in _SNOWFLAKE_FAMILY:
        return "snowflake"
    return n


def snowflake_type_is_copy_safe(declared: str) -> bool:
    raw = (declared or "").strip().upper().replace(" ", "")
    if not raw:
        return True
    base = raw.split("(", 1)[0]
    if base in _UNSAFE_BASES:
        return False
    return base in _SAFE_BASES


def snowflake_schema_of(cfg: dict[str, Any]) -> str:
    return str(cfg.get("schema") or "PUBLIC").strip() or "PUBLIC"


def snowflake_database_of(cfg: dict[str, Any]) -> str:
    return str(cfg.get("database") or "").strip()


def snowflake_ident(name: str) -> str:
    from connectors.sql_identifiers import quote_sql_identifier, require_safe_identifier

    return quote_sql_identifier(
        require_safe_identifier(name, preserve_case=True, max_len=255),
        '"',
    )


def snowflake_table_ref(schema: str, table: str) -> str:
    from connectors.snowflake_conn import snowflake_qualified_table

    return snowflake_qualified_table(
        schema or "PUBLIC",
        require_safe_table(table),
    )


def require_safe_table(table: str) -> str:
    from connectors.sql_identifiers import require_safe_identifier

    return require_safe_identifier(table, preserve_case=True, max_len=255)


def _norm_sf_account(cfg: dict[str, Any]) -> str:
    from connectors.snowflake_conn import normalize_account

    acct = normalize_account(str(cfg.get("host") or "")).strip().lower()
    if acct in _LOCAL_ACCOUNTS:
        return "localhost"
    return acct


def snowflake_same_account(src_cfg: dict[str, Any], dest_cfg: dict[str, Any]) -> bool:
    """True only when account locator and database are present and equal."""
    src_acct = _norm_sf_account(src_cfg)
    dest_acct = _norm_sf_account(dest_cfg)
    if not src_acct or not dest_acct:
        return False
    src_db = snowflake_database_of(src_cfg).lower()
    dest_db = snowflake_database_of(dest_cfg).lower()
    if not src_db or not dest_db:
        return False
    return src_acct == dest_acct and src_db == dest_db


def snowflake_same_table(
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    source_table: str,
    dest_table: str,
) -> bool:
    if not snowflake_same_account(src_cfg, dest_cfg):
        return False
    src_sch = snowflake_schema_of(src_cfg).lower()
    dest_sch = snowflake_schema_of(dest_cfg).lower()
    return (
        src_sch == dest_sch
        and source_table.strip().lower() == dest_table.strip().lower()
    )


def snowflake_proxy_fail_closed(cfg: dict[str, Any]) -> bool:
    from connectors.write_resilience import is_public_proxy_host

    return any(
        is_public_proxy_host(str(cfg.get(key) or ""))
        for key in ("host", "connection_string", "dsn")
    )


def snowflake_connect(cfg: dict[str, Any]) -> Any:
    from connectors.snowflake_conn import get_connection, normalize_account

    try:
        return get_connection(
            account=normalize_account(str(cfg.get("host") or "")),
            username=str(cfg.get("username") or cfg.get("user") or ""),
            password=str(cfg.get("password") or ""),
            database=snowflake_database_of(cfg),
            schema=snowflake_schema_of(cfg),
            warehouse=str(cfg.get("warehouse") or ""),
            connection_string=str(cfg.get("connection_string") or ""),
            role=str(cfg.get("role") or ""),
            private_key=str(cfg.get("private_key") or ""),
            private_key_passphrase=str(cfg.get("private_key_passphrase") or ""),
        )
    except Exception as exc:
        raise FastPathUnavailable(f"Snowflake connect failed: {exc}") from exc


def snowflake_execute(cur: Any, sql: str, params: Any = None) -> None:
    """Identity path must never emit COPY INTO / MERGE / CLONE."""
    compact = f" {sql.upper()} "
    if "COPY INTO" in compact or " CLONE " in compact or compact.lstrip().startswith("MERGE "):
        raise FastPathUnavailable(
            "Snowflake identity COPY refuses COPY INTO / MERGE / CLONE"
        )
    if params is None:
        cur.execute(sql)
    else:
        cur.execute(sql, params)


def snowflake_dest_count(cfg: dict[str, Any], table: str) -> int:
    """Dest-engine ``COUNT(*)``. Missing table is 0. Unknowable fails closed."""
    from services.dest_precount import destination_row_count

    n = destination_row_count(
        "snowflake",
        cfg,
        schema=snowflake_schema_of(cfg),
        table_name=table,
    )
    if n is None:
        raise FastPathUnavailable("Snowflake dest COUNT(*) unknowable")
    return int(n)


def skip_complete_snowflake(
    *,
    source_count: int,
    dest_count: int,
    extra_snapshot: dict[str, Any] | None = None,
) -> FastPathResult:
    proof = f"dest_count:{dest_count}"
    snapshot = {
        "copy_workers": 1,
        "copy_split": "skip",
        "copy_partitions": 1,
        "partitions_skipped": 1,
        "partitions_loaded": 0,
        "shard_mode": "table",
        **(extra_snapshot or {}),
    }
    return FastPathResult(
        rows_copied=source_count,
        source_rows=source_count,
        source_checksum=proof,
        target_rows=dest_count,
        target_checksum=proof,
        source_snapshot=snapshot,
        proof_scope="dest_count_equals_source_snapshot_count",
    )
