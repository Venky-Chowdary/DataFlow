"""Shared pgvector identity-COPY helpers.

pgvector is PostgreSQL underneath. Dest COUNT is ``COUNT(*)`` — never
``scan_source_ids`` DISTINCT source_id, never upsert ack, never writer
``rows_written``. Same host+port+database+schema+table declines.
Cross-endpoint binary COPY declines. Occupancy is counted **before**
delete. Desktop-lab pgvector is not a customer-tenant PRODUCTION_SKU.
"""

from __future__ import annotations

from typing import Any

from services.copy_fast_path import FastPathUnavailable, skip_complete_identity_copy

_PGVECTOR_FAMILY = frozenset({
    "pgvector",
})

_PGVECTOR_COPY_SAFE_TYPES = frozenset({
    "smallint",
    "int2",
    "integer",
    "int",
    "int4",
    "bigint",
    "int8",
    "real",
    "float4",
    "double",
    "float8",
    "float",
    "numeric",
    "decimal",
    "varchar",
    "character",
    "varying",
    "char",
    "bpchar",
    "text",
    "citext",
    "boolean",
    "bool",
    "date",
    "timestamp",
    "datetime",
    "json",
    "jsonb",
    "uuid",
    "vector",
    "halfvec",
})


def pgvector_family_name(name: str) -> str:
    n = (name or "").strip().lower()
    if n in _PGVECTOR_FAMILY:
        return "pgvector"
    return n


def pgvector_type_is_copy_safe(declared: str) -> bool:
    raw = (declared or "").strip().lower()
    if not raw:
        return True
    if raw.endswith("[]"):
        return False
    base = raw.split("(", 1)[0].split("<", 1)[0].strip()
    if base in {"bytea", "timestamptz", "timestamp with time zone"}:
        return False
    if "time zone" in raw and "without" not in raw:
        return False
    return base in _PGVECTOR_COPY_SAFE_TYPES


def pgvector_schema(cfg: dict[str, Any]) -> str:
    return str(cfg.get("schema") or "public").strip() or "public"


def pgvector_table(table: str, cfg: dict[str, Any] | None = None) -> str:
    name = (table or "").strip()
    if not name and cfg:
        name = str(cfg.get("table") or cfg.get("database") or "").strip()
    if not name:
        raise FastPathUnavailable("pgvector table required")
    if any(ch in name for ch in "*?\\/ "):
        raise FastPathUnavailable("pgvector COPY refuses glob characters in the table")
    return name


def pgvector_endpoint_key(cfg: dict[str, Any]) -> str:
    host = str(cfg.get("host") or "").strip().lower().replace("localhost", "127.0.0.1")
    port = int(cfg.get("port") or 5432)
    cs = str(cfg.get("connection_string") or "").strip()
    if cs:
        from connectors.url_authority import parse_url_authority

        parsed = parse_url_authority(cs)
        if parsed.host:
            host = str(parsed.host).strip().lower().replace("localhost", "127.0.0.1")
        if parsed.port:
            port = int(parsed.port)
    db = str(cfg.get("database") or cfg.get("dbname") or "").strip().lower()
    schema = pgvector_schema(cfg).lower()
    host = host or "127.0.0.1"
    return f"{host}:{port}:{db}:{schema}"


def pgvector_object_id(cfg: dict[str, Any], table: str) -> tuple[str, str]:
    return (pgvector_endpoint_key(cfg), pgvector_table(table, cfg).lower())


def pgvector_proxy_fail_closed(cfg: dict[str, Any]) -> bool:
    from connectors.write_resilience import is_public_proxy_host

    return any(
        is_public_proxy_host(str(cfg.get(key) or ""))
        for key in ("host", "connection_string", "dsn")
    )


def _pgvector_connection(cfg: dict[str, Any]) -> Any:
    from connectors.postgresql_conn import get_connection

    return get_connection(
        host=str(cfg.get("host") or ""),
        port=int(cfg.get("port") or 5432),
        database=str(cfg.get("database") or cfg.get("dbname") or ""),
        username=str(cfg.get("username") or ""),
        password=str(cfg.get("password") or ""),
        connection_string=str(cfg.get("connection_string") or ""),
        ssl=bool(cfg.get("ssl", False)),
    )


def pgvector_table_exists(cfg: dict[str, Any], table: str) -> bool:
    schema = pgvector_schema(cfg)
    name = pgvector_table(table, cfg)
    conn = _pgvector_connection(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f'"{schema}"."{name}"',))
            row = cur.fetchone()
            return bool(row and row[0] is not None)
    finally:
        conn.close()


def pgvector_row_count(cfg: dict[str, Any], table: str) -> int:
    """Physical ``COUNT(*)`` — never DISTINCT source_id."""
    from connectors.sql_identifiers import quote_table_ref

    schema = pgvector_schema(cfg)
    name = pgvector_table(table, cfg)
    ref = quote_table_ref(name, schema, dialect="postgresql")
    conn = _pgvector_connection(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {ref}")  # nosec B608
            row = cur.fetchone()
            if not row:
                raise ValueError(f"pgvector dest COUNT(*) unmeasured for {table}")
            return int(row[0])
    finally:
        conn.close()


def pgvector_column_names(cfg: dict[str, Any], table: str) -> list[str]:
    """Source catalog column names in ordinal order — identity COPY must cover all."""
    schema = pgvector_schema(cfg)
    name = pgvector_table(table, cfg)
    conn = _pgvector_connection(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema, name),
            )
            return [str(row[0]) for row in cur.fetchall() if row and row[0]]
    finally:
        conn.close()


def pgvector_require_full_column_mapping(
    cfg: dict[str, Any],
    table: str,
    pairs: list[tuple[str, str]],
) -> None:
    """Identity COPY cannot omit source columns (vector payloads would silently drop)."""
    catalog = pgvector_column_names(cfg, table)
    if not catalog:
        raise FastPathUnavailable("pgvector source catalog has no columns")
    mapped = {src for src, _tgt in pairs}
    missing = [col for col in catalog if col not in mapped]
    if missing:
        raise FastPathUnavailable(
            "pgvector identity COPY requires all source columns; "
            f"unmapped: {', '.join(missing)}"
        )


def skip_complete_pgvector(
    *,
    source_count: int,
    dest_count: int,
    extra_snapshot: dict[str, Any] | None = None,
):
    return skip_complete_identity_copy(
        source_count=source_count,
        dest_count=dest_count,
        shard_mode="table",
        extra_snapshot=extra_snapshot,
    )
