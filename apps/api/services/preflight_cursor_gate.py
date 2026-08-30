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
            source=src_cfg,
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


def cursor_identity_issue(scope: IncrementalReadScope | None) -> str:
    """Refusal text when the stored watermark belongs to a different column.

    The cursor column of a route is part of the meaning of its watermark. When
    an operator repoints the cursor (``id`` → ``updated_at``), the stored value
    can neither bound the new read nor be translated into it: applying it either
    aborts the read or, when both types compare, silently skips the rows between
    the two columns' orderings. The only sound answers are reset-and-resnapshot
    or restore the previous cursor, and both are the operator's call.
    """
    if scope is None or not scope.cursor_column_changed:
        return ""
    return (
        f"Cursor changed for this route: the stored watermark "
        f"'{scope.watermark}' was measured on '{scope.watermark_cursor_column}', "
        f"not on '{scope.cursor_column}' — it cannot bound a read on a different "
        f"column. Reset the cursor (POST /api/v1/ops/cdc-cursors/clear with "
        f"cursor_key '{scope.cursor_key}') to re-snapshot, or restore "
        f"'{scope.watermark_cursor_column}' as the cursor field."
    )


def cursor_destination_reset_issue(
    scope: IncrementalReadScope | None,
    dest_rows: int | None,
) -> str:
    """Refusal text when the watermark outlived the destination it delivered to.

    A watermark asserts "everything at or below this value is already at rest in
    that destination". Dropping, recreating or truncating the destination voids
    that assertion, but the cursor survives it, so the next incremental run reads
    only what changed since and reports a green, near-empty load — the whole
    history is silently missing. Fivetran and Airbyte answer this with an
    operator-triggered state reset; so do we, and we refuse until it happens.

    ``dest_rows`` is ``None`` when the destination cannot be counted (an engine
    with no cheap count, or an unreadable probe). Unknown is not evidence of a
    reset, so it never refuses — only a measured empty destination does.
    """
    if scope is None or not scope.bounded or dest_rows is None or dest_rows > 0:
        return ""
    return (
        f"Destination reset since the last incremental run: the stored watermark "
        f"'{scope.watermark}' on '{scope.cursor_column}' claims those rows are "
        f"already at rest, but the destination is empty — resuming would skip "
        f"the whole history and report success. Clear the cursor (POST "
        f"/api/v1/ops/cdc-cursors/clear with cursor_key '{scope.cursor_key}') to "
        f"re-snapshot, or point the run at the destination that holds the history."
    )


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
    source_read_mode: str = "",
    read_scope: IncrementalReadScope | None = None,
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
    from services.procedure_source import callable_sync_refusal

    refused = callable_sync_refusal(sync, source_read_mode=source_read_mode)
    if refused:
        issues.append(refused)
    if sync in {"scd2", "mirror"} and not refused:
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
    if sync == "cdc" and not refused:
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

    cursor_conflict = cursor_identity_issue(read_scope)
    if cursor_conflict:
        issues.append(cursor_conflict)

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


def read_scope_for_transfer_request(request: Any) -> IncrementalReadScope:
    """:func:`resolve_read_scope` addressed by a ``TransferRequest``.

    Execute and the read side must ask the same question about the same cursor
    key, so the endpoint-shaped translation lives beside the resolver rather
    than being restated by each caller.
    """
    src = getattr(request, "source", None)
    dest = getattr(request, "destination", None)
    src_extra = getattr(src, "extra", None) or {}
    dest_extra = getattr(dest, "extra", None) or {}
    return resolve_read_scope(
        sync_mode=str(getattr(request, "sync_mode", "") or ""),
        stream_contracts=list(getattr(request, "stream_contracts", None) or []),
        source_format=str(getattr(src, "format", None) or ""),
        source_config={"database": str(getattr(src, "database", "") or ""), **src_extra},
        source_table=str(getattr(src, "table", "") or getattr(src, "collection", "") or ""),
        destination_db_type=str(getattr(dest, "format", None) or ""),
        destination_config={
            "database": str(getattr(dest, "database", "") or ""),
            **dest_extra,
        },
        destination_table=str(
            getattr(dest, "table", "") or getattr(dest, "collection", "") or ""
        ),
    )


def countable_dest_rows(dest_type: str, dest_cfg: dict[str, Any], dest_table: str) -> int | None:
    """Rows at rest in the destination, or ``None`` when it cannot be counted.

    Unknown must stay distinguishable from empty: a probe that failed is not
    evidence the destination was reset, and treating it as one would block a
    healthy incremental run.
    """
    from services.dest_precount import precount_table

    try:
        return precount_table(dest_type, dest_cfg, dest_table)
    except Exception as exc:
        logger.debug("destination pre-count for cursor check failed: %s", exc, exc_info=exc)
        return None


def refuse_unusable_cursor_state(
    scope: IncrementalReadScope,
    dest_type: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
) -> None:
    """Raise before a bounded read that its own watermark cannot justify.

    Two states make a bounded read unsafe: the watermark was measured on a
    different column (either aborts mid-run or skips the rows between the two
    orderings), and a destination measured empty while the watermark claims the
    history already landed (the bounded read would skip it for good).
    """
    if scope.cursor_column_changed:
        raise ValueError(cursor_identity_issue(scope))
    if not scope.bounded:
        return
    issue = cursor_destination_reset_issue(
        scope, countable_dest_rows(dest_type, dest_cfg, dest_table)
    )
    if issue:
        raise ValueError(issue)
