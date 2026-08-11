"""Validate's sync contract gate: read scope, cursor meaning, cursor/key presence.

Kept beside the gate that uses it rather than inside it, so the transfer's read
side and Validate's checks resolve a route's watermark through one call. A gate
that rebuilt the key itself found no watermark and silently fell back to
reasoning about the whole table.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from services.cursor_semantics import evaluate_cursor_semantics
from services.sync_cursor import IncrementalReadScope, resolve_incremental_read_scope

logger = logging.getLogger(__name__)

#: Destinations that honor SCD2 / mirror streaming paths (must match Studio gating).
SQL_HISTORY_SYNC_DESTS = frozenset({
    "postgresql",
    "mysql",
    "sqlite",
    "snowflake",
    "bigquery",
    "redshift",
    "generic_sql",
    "sqlserver",
    "mssql",
    "oracle",
    "duckdb",
})

#: Sources that can drive CDC (log / change-stream) in production.
CDC_CAPABLE_SOURCES = frozenset({
    "postgresql",
    "mysql",
    "sqlserver",
    "mssql",
    "oracle",
    "mongodb",
    "azure_sql_database",
    "microsoft_sql_server",
    "amazon_rds_sql_server",
    "amazon_rds_postgresql",
    "amazon_rds_mysql",
    "amazon_aurora_postgresql",
    "amazon_aurora_mysql",
})

MODES_REQUIRING_CURSOR = frozenset(
    {"incremental_append", "incremental_deduped", "cdc"}
)
MODES_REQUIRING_PRIMARY_KEY = frozenset(
    {"upsert", "incremental_deduped", "cdc", "scd2", "mirror"}
)


def resolve_read_scope(
    *,
    sync_mode: str,
    stream_contracts: list[dict[str, Any]] | None,
    source_format: str,
    source_config: Mapping[str, Any] | None,
    source_table: str,
    destination_db_type: str,
    destination_config: Mapping[str, Any] | None,
    destination_table: str,
) -> IncrementalReadScope:
    """The cursor state of this route, as the transfer's read side sees it.

    Built from the same identifiers the transfer uses for its watermark, so the
    delta a gate reasons about is the delta the reader will deliver. A route
    whose watermark cannot be resolved reports no watermark, which keeps checks
    on the whole sample rather than silently narrowing them.
    """
    try:
        from src.transfer.connector_capabilities import resolve_driver_type

        source_type = resolve_driver_type(source_format or "")
    except Exception:
        source_type = (source_format or "").strip().lower()
    src_cfg = source_config or {}
    dst_cfg = destination_config or {}
    try:
        return resolve_incremental_read_scope(
            sync_mode=sync_mode,
            stream_contracts=stream_contracts,
            source_type=source_type,
            source_database=str(src_cfg.get("database") or ""),
            source_object=source_table,
            dest_type=(destination_db_type or "").strip().lower(),
            dest_database=str(dst_cfg.get("database") or ""),
            dest_object=destination_table,
        )
    except Exception as exc:
        logger.warning("incremental read scope unresolved: %s", exc, exc_info=exc)
        return IncrementalReadScope()


def evaluate_stream_cursor_semantics(
    contracts: list[dict[str, Any]],
    *,
    sync_mode: str,
    validation_mode: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Judge each stream's cursor declaration; return verdicts and blocking issues.

    Never inferred from the column's name: a name cannot establish whether the
    source moves the value when a row changes.
    """
    verdicts: list[dict[str, Any]] = []
    issues: list[str] = []
    for c in contracts:
        cursor = str(c.get("cursor_field") or c.get("cursor") or "").strip()
        if not cursor:
            continue
        verdict = evaluate_cursor_semantics(
            sync_mode=sync_mode,
            cursor_field=cursor,
            declared=str(c.get("cursor_semantics") or ""),
            validation_mode=validation_mode,
        )
        if verdict.status == "not_applicable":
            continue
        stream = c.get("name") or c.get("stream") or "stream"
        entry = verdict.to_dict()
        entry["stream"] = stream
        verdicts.append(entry)
        if verdict.blocks:
            issues.append(f"{stream}.{cursor}: {verdict.reason}")
    return verdicts, issues


def build_sync_contract_gate(
    contracts: list[dict[str, Any]],
    *,
    sync: str,
    validation: str,
    dest: str,
    src: str,
    kind: str,
    source_columns: list[str] | None,
    pass_status: str,
    block_status: str,
) -> dict[str, Any]:
    """The g9 gate: is this route's sync contract complete and semantically sound?

    Presence of a cursor and a key is necessary but not sufficient — what the
    cursor *means* decides whether the read can lose rows, so both are judged
    here and reported as one gate with one primary action per cause.
    """
    multi_stream = len(contracts) > 1
    requires_cursor = sync in MODES_REQUIRING_CURSOR
    requires_primary_key = sync in MODES_REQUIRING_PRIMARY_KEY

    missing_cursor = [
        c.get("name") or c.get("stream") or "stream"
        for c in contracts
        if requires_cursor and not (c.get("cursor_field") or c.get("cursor"))
    ]
    missing_primary_key = [
        c.get("name") or c.get("stream") or "stream"
        for c in contracts
        if requires_primary_key and not (c.get("primary_key") or c.get("primary_keys"))
    ]

    # Live column check — typo'd cursor/PK names must fail at Validate, not mid-run.
    source_col_set = {
        str(c).strip().lower() for c in (source_columns or []) if str(c).strip()
    }
    unknown_cursor: list[str] = []
    unknown_pk: list[str] = []
    if source_col_set:
        for c in contracts:
            stream = c.get("name") or c.get("stream") or "stream"
            if requires_cursor:
                cursor = str(c.get("cursor_field") or c.get("cursor") or "").strip()
                if cursor and cursor.lower() not in source_col_set:
                    unknown_cursor.append(f"{stream}.{cursor}")
            if requires_primary_key:
                raw_pk = c.get("primary_key") or c.get("primary_keys") or []
                pk_fields = [raw_pk] if isinstance(raw_pk, str) else list(raw_pk or [])
                for pk in pk_fields:
                    name = str(pk).strip()
                    if name and name.lower() not in source_col_set:
                        unknown_pk.append(f"{stream}.{name}")

    issues: list[str] = []
    if sync in {"scd2", "mirror"}:
        if multi_stream:
            issues.append(
                f"{sync.upper()} is not supported for multi-stream transfers"
            )
        elif not dest:
            issues.append(f"{sync.upper()} requires a SQL table destination")
        elif dest not in SQL_HISTORY_SYNC_DESTS:
            issues.append(
                f"{sync.upper()} requires a SQL table destination (not '{dest}')"
            )
    if sync == "cdc":
        if kind in {"file", "cloud"}:
            issues.append("CDC requires a database source (not file/cloud)")
        elif src and src not in CDC_CAPABLE_SOURCES:
            issues.append(f"CDC is not supported for source type '{src}'")
    if missing_cursor:
        issues.append(f"Missing cursor field for {', '.join(missing_cursor[:5])}")
    if missing_primary_key:
        issues.append(f"Missing primary key for {', '.join(missing_primary_key[:5])}")
    if unknown_cursor:
        issues.append(
            f"Cursor field not in source schema: {', '.join(unknown_cursor[:5])}"
        )
    if unknown_pk:
        issues.append(f"Primary key not in source schema: {', '.join(unknown_pk[:5])}")

    verdicts, semantics_issues = evaluate_stream_cursor_semantics(
        contracts, sync_mode=sync, validation_mode=validation
    )
    issues.extend(semantics_issues)

    if issues:
        return {
            "id": "g9_sync_contract",
            "status": block_status,
            "message": "Sync mode contract incomplete",
            "duration_ms": 0,
            "details": {
                "issues": issues,
                "sync_mode": sync,
                "cursor_semantics": verdicts or None,
            },
        }
    return {
        "id": "g9_sync_contract",
        "status": pass_status,
        "message": f"Sync contract valid for {sync.replace('_', ' ')}",
        "duration_ms": 0,
        "details": {
            "sync_mode": sync,
            "streams": len(contracts),
            "requires_cursor": requires_cursor,
            "requires_primary_key": requires_primary_key,
            "cursor_semantics": verdicts or None,
        },
    }
