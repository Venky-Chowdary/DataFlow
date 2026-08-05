"""Pre-ingestion quarantine staging for SQL destinations.

Honesty
-------
When ``write_via_staging`` is enabled:

1. Every source row lands in ``{table}_df_staging`` (inspectable), with cells
   that failed conversion stored as NULL. The stage is a complete landing zone:
   a row the operator must diagnose is never absent from it.
2. Rows with any cell failure are **not** written to the primary table, so the
   primary is never altered by a bad row.
3. Findings still go to ``{table}_df_quarantine`` via the existing DLQ path,
   carrying the original cell value for replay.
4. Promote is at-least-once upsert/insert of clean rows — not exactly-once.

Strict / maximum validation fail-closed: staging is retained, primary is
untouched, and the transfer surfaces ``promote_blocked``.

The engine forces the buffered execute path when staging is on so row
classification sees the full batch (file/DB streaming shortcuts are skipped).
"""

from __future__ import annotations

import logging
import re
from dataclasses import replace
from typing import Any, Callable

logger = logging.getLogger(__name__)


def staging_table_name(dest_table: str | None) -> str:
    """Stable per-target staging table (pairs with ``{table}_df_quarantine``)."""
    base = re.sub(r"[^a-zA-Z0-9_]", "_", (dest_table or "import").strip()) or "import"
    base = re.sub(r"_+", "_", base).strip("_")[:48] or "import"
    return f"{base}_df_staging"[:63]


# SQL table writers only — Mongo/document DLQ ≠ pre-ingestion staging tables.
_STAGING_SUPPORTED = frozenset({
    "sqlite",
    "postgresql",
    "postgres",
    "mysql",
    "sqlserver",
    "mssql",
    "oracle",
    "snowflake",
    "redshift",
    "generic_sql",
    "duckdb",
    "bigquery",
})


def dest_supports_staging(dest_type: str) -> bool:
    from src.transfer.connector_capabilities import resolve_driver_type

    driver = resolve_driver_type(dest_type or "")
    raw = (dest_type or "").strip().lower()
    return driver in _STAGING_SUPPORTED or raw in _STAGING_SUPPORTED


def staging_endpoint(destination: Any, *, dest_table: str | None = None) -> Any:
    table = dest_table or getattr(destination, "table", None) or getattr(destination, "collection", None) or "import"
    name = staging_table_name(str(table))
    return replace(destination, table=name, collection=name)


def _drop_table(destination: Any) -> bool:
    try:
        from connectors.table_manager import drop_table
        from src.transfer.adapters import resolve_connector_config, resolve_dest_table
        from src.transfer.connector_capabilities import resolve_driver_type

        db_type = resolve_driver_type(getattr(destination, "format", "") or "")
        cfg = resolve_connector_config(destination)
        table_name = resolve_dest_table(db_type, destination)
        return drop_table(db_type, cfg, table_name, cfg.get("schema"))
    except Exception as exc:
        logger.debug("staging drop skipped: %s", exc)
        return False


def write_via_pre_ingestion_staging(
    destination: Any,
    records: list[dict],
    columns: list[str],
    schema: dict[str, str],
    mappings: list[dict],
    *,
    validation_mode: str = "balanced",
    backfill_new_fields: bool = False,
    write_mode: str = "insert",
    conflict_columns: list[str] | None = None,
    job_id: str | None = None,
    on_checkpoint: Callable[..., None] | None = None,
    drop_primary: bool = False,
) -> tuple[int, list[str], dict[str, Any]]:
    """Stage all rows, then promote only clean rows to the primary table.

    Returns ``(rows_written_to_primary, ddl_log, destination_summary)``.
    When promote is blocked (strict/maximum with rejects), ``rows_written`` is 0
    and ``destination_summary["promote_blocked"]`` is True — callers must fail
    the job after persisting quarantine.
    """
    from src.transfer.adapters import write_destination_database
    from src.transfer.connector_capabilities import resolve_driver_type

    dest_type = resolve_driver_type(getattr(destination, "format", "") or "")
    if not dest_supports_staging(dest_type):
        raise ValueError(
            f"write_via_staging is not supported for destination '{dest_type or 'unknown'}' "
            "(SQL table destinations only)"
        )
    if getattr(destination, "kind", "") == "file_export":
        raise ValueError("write_via_staging requires a database destination")

    stage_ep = staging_endpoint(destination)
    ddl_log: list[str] = []

    # 1) Refresh staging and load *every* source row so the stage is a complete,
    #    inspectable landing zone. ``coerce_null`` is explicit here rather than
    #    inherited from validation_mode: balanced/strict both hold bad rows out of
    #    the table they are writing, which would leave the stage missing exactly
    #    the rows the operator needs to diagnose. The original cell value is still
    #    preserved in rejected_details for DLQ and replay.
    _drop_table(stage_ep)
    staged_n, stage_ddl, stage_summary = write_destination_database(
        stage_ep,
        records,
        columns,
        schema,
        mappings,
        on_checkpoint=on_checkpoint,
        validation_mode="balanced",
        backfill_new_fields=backfill_new_fields,
        write_mode="insert",
        conflict_columns=None,
        job_id=f"{job_id}_stg" if job_id else None,
        error_policy="coerce_null",
    )
    ddl_log.extend(list(stage_ddl or [])[:50])
    ddl_log.append(f"PRE-INGESTION STAGE: {staged_n} row(s) → {stage_ep.table}")

    rejected_details = list(stage_summary.get("rejected_details") or [])
    rejected_row_nums = {
        int(d["row"])
        for d in rejected_details
        if isinstance(d, dict) and d.get("row") is not None
    }
    mode = (validation_mode or "balanced").strip().lower()

    staging_meta = {
        "enabled": True,
        "table": stage_ep.table,
        "staged_rows": int(staged_n or 0),
        "rejected_row_count": len(rejected_row_nums),
        "validation_mode": mode,
        # Rows present in the stage with at least one cell nulled by coercion.
        # The primary never receives these rows, so this is stage-only context.
        "staged_coerced_null_rows": len(rejected_row_nums),
    }

    # 2) Strict / maximum: fail-closed — leave primary untouched.
    if rejected_row_nums and mode in ("strict", "maximum"):
        summary: dict[str, Any] = {
            "table": getattr(destination, "table", None) or getattr(destination, "collection", None),
            "type": dest_type,
            "ok": False,
            "promote_blocked": True,
            "pre_ingestion_staging": {
                **staging_meta,
                "promoted_rows": 0,
                "blocked_reason": (
                    f"{len(rejected_row_nums)} row(s) failed validation; "
                    f"primary table not written (use balanced to promote clean rows only)"
                ),
            },
            "staging_table": stage_ep.table,
            "staged_rows": int(staged_n or 0),
            "promoted_rows": 0,
            "rejected_rows": len(rejected_row_nums),
            "coerced_null_rows": 0,
            "rejected_details": rejected_details[:2000],
            "warnings": list(stage_summary.get("warnings") or [])[:10],
        }
        ddl_log.append(
            f"PRE-INGESTION BLOCKED: {len(rejected_row_nums)} bad row(s); "
            f"primary not written; inspect {stage_ep.table}"
        )
        return 0, ddl_log, summary

    # 3) Promote clean rows only (entire bad rows stay off primary).
    clean_records = [
        r for i, r in enumerate(records, start=1) if i not in rejected_row_nums
    ]
    if drop_primary:
        _drop_table(destination)

    if not clean_records:
        summary = {
            "table": getattr(destination, "table", None) or getattr(destination, "collection", None),
            "type": dest_type,
            "ok": True,
            "pre_ingestion_staging": {**staging_meta, "promoted_rows": 0},
            "staging_table": stage_ep.table,
            "staged_rows": int(staged_n or 0),
            "promoted_rows": 0,
            "rejected_rows": len(rejected_row_nums),
            "coerced_null_rows": 0,
            "rejected_details": rejected_details[:2000],
            "warnings": list(stage_summary.get("warnings") or [])[:10],
        }
        ddl_log.append("PRE-INGESTION PROMOTE: 0 clean rows (all quarantined)")
        return 0, ddl_log, summary

    promoted_n, promo_ddl, promo_summary = write_destination_database(
        destination,
        clean_records,
        columns,
        schema,
        mappings,
        on_checkpoint=on_checkpoint,
        validation_mode=validation_mode,
        backfill_new_fields=backfill_new_fields,
        write_mode=write_mode,
        conflict_columns=conflict_columns,
        job_id=job_id,
    )
    ddl_log.extend(list(promo_ddl or [])[:50])
    ddl_log.append(
        f"PRE-INGESTION PROMOTE: {promoted_n} clean row(s) → "
        f"{getattr(destination, 'table', None) or getattr(destination, 'collection', None)}; "
        f"{len(rejected_row_nums)} held in staging/DLQ"
    )

    # Prefer stage rejects (original row numbers) over promote diagnostics.
    summary = dict(promo_summary or {})
    summary["ok"] = True
    summary["pre_ingestion_staging"] = {**staging_meta, "promoted_rows": int(promoted_n or 0)}
    summary["staging_table"] = stage_ep.table
    summary["staged_rows"] = int(staged_n or 0)
    summary["promoted_rows"] = int(promoted_n or 0)
    summary["rejected_rows"] = len(rejected_row_nums)
    summary["coerced_null_rows"] = 0
    summary["rejected_details"] = rejected_details[:2000]
    if stage_summary.get("warnings"):
        summary["warnings"] = list(stage_summary.get("warnings") or [])[:10]
    return int(promoted_n or 0), ddl_log, summary
