"""SCD2 mirror streaming: stage the source, then apply history in the destination.

Extracted from ``src.transfer.stream`` (Phase F8 size freeze) with no behaviour
change. The mirror runs in two provable halves — a full staging load, then an
SCD2/mirror apply against the destination's own engine — so a partial stage can
never be reported as applied history. ``stream`` re-exports these names.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from typing import Any

from connectors.writer_common import resolve_target_columns
from services.engine_pool import release_engine

from .adapters import resolve_connector_config
from .connector_capabilities import resolve_driver_type
from .models import EndpointConfig
from .type_mapper import ddl_carrier_type

logger = logging.getLogger(__name__)


def _qualified(table: str, schema: str | None, dialect: str = "") -> str:
    """Return a SQL quoted qualified table name for this destination engine.

    Quoting is per dialect: MySQL backticks, SQL Server brackets, ANSI double
    quotes elsewhere. Emitting ANSI quotes everywhere made every SCD2 / mirror
    statement a syntax error on MySQL.
    """
    from connectors.writer_common import quote_sql_identifier
    from services.dialect_profiles import quote_char_for

    q = quote_char_for(dialect) or '"'
    table_q = quote_sql_identifier(table, q)
    if schema:
        return f"{quote_sql_identifier(schema, q)}.{table_q}"
    return table_q


#: Prefix of the SCD2/mirror source spool inside the destination schema.
#: Operator listings hide it, so a spool left behind by a run killed mid-flight
#: (deploy, OOM, API restart) was invisible and never cleaned. Naming, age
#: stamps and the sweep are owned once in ``services.staging_reaper``.
SCD2_STAGING_PREFIX = "_dataflow_stg_"


def _staging_endpoint(destination: EndpointConfig, job_id: str) -> EndpointConfig:
    """Clone a destination endpoint for use as a per-transfer staging table.

    The name carries the job it belongs to *and* an age stamp: a job's retry can
    start while the first attempt is still draining, and a name derived from the
    job alone let the second run drop the first one's spool mid-read.
    """
    from dataclasses import replace

    from services.staging_reaper import staging_table_name

    suffix = re.sub(r"[^a-zA-Z0-9]", "", job_id)[:16] or "stg"
    name = staging_table_name(SCD2_STAGING_PREFIX, suffix)
    return replace(destination, table=name, collection=name)


def _reap_orphan_spools(
    dest_cfg: dict[str, Any], schema_name: str, dest_type: str, *, keep: str
) -> list[str]:
    """Drop source spools no live run can own. Never fatal to the transfer."""
    from connectors.generic_sql import get_sqlalchemy_engine
    from services.staging_reaper import reap_orphan_staging

    engine = get_sqlalchemy_engine(dest_cfg)
    dialect = str(getattr(getattr(engine, "dialect", None), "name", "") or dest_type)
    try:
        with engine.connect() as conn:
            return reap_orphan_staging(
                conn, SCD2_STAGING_PREFIX, schema_name or "", dialect, keep=keep
            )
    except Exception as exc:
        logger.debug("staging spool sweep skipped: %s", exc)
        return []
    finally:
        release_engine(engine)


def stream_scd2_mirror_transfer(
    source: EndpointConfig,
    destination: EndpointConfig,
    mappings: list[dict],
    schema: dict[str, str],
    on_checkpoint: Callable[..., None] | None = None,
    *,
    sync_mode: str = "full_refresh_mirror",
    stream_contracts: list[dict] | None = None,
    job_id: str | None = None,
    checkpoint: Any = None,  # noqa: ARG001 - reserved for future resume support
    checkpoint_service: Any = None,  # noqa: ARG001
    backfill_new_fields: bool = False,
    validation_mode: str = "strict",
    limit: int = 0,
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    """Stream a mirror or SCD2 database-to-database transfer through a staging table.

    Instead of loading the entire source table into memory, this helper:
      1. Streams the source into a temporary staging table.
      2. For SCD2, applies the slowly-changing-dimension merge in batches.
      3. For mirror, upserts the staging table into the target and then runs a
         single SQL pass to reactivate present keys and soft-delete missing keys.
    """
    import math

    from connectors.generic_sql import drop_table, get_sql_schema, get_sqlalchemy_engine

    # Imported here, not at module scope: ``stream`` re-exports this module.
    from .stream import _NoOpCheckpointService, stream_database_transfer
    from services.sync_cursor import (
        map_source_to_target,
        resolve_effective_sync_mode,
        resolve_sync_contract,
    )

    contract = resolve_sync_contract(stream_contracts)
    effective_sync = resolve_effective_sync_mode(
        sync_mode,
        contract.sync_mode if contract else None,
    ).lower()

    src_type = resolve_driver_type(source.format)
    dest_type = resolve_driver_type(destination.format)

    # Supported SQL-backed destinations that can be driven through SQLAlchemy.
    _SQL_STREAMING_DESTS = {
        "generic_sql", "postgresql", "mysql", "sqlite", "snowflake", "bigquery", "redshift",
    }
    if dest_type not in _SQL_STREAMING_DESTS:
        raise NotImplementedError(
            f"{effective_sync} streaming transfer is currently implemented for SQL destinations; "
            f"'{destination.format}' is not yet supported."
        )

    dest_cfg = resolve_connector_config(destination)
    # get_sql_schema() already respects dialect defaults; ignore the default
    # "public" placeholder that resolve_connector_config sets for SQLite/MySQL.
    schema_name = get_sql_schema(dest_cfg) or ""

    if not mappings:
        mappings = [{"source": c, "target": c, "confidence": 0.95} for c in schema]
    target_cols, _ = resolve_target_columns(mappings, schema, preserve_case=True)
    column_types = {c: ddl_carrier_type(schema.get(c, "string")) for c in schema}

    staging = _staging_endpoint(destination, job_id or "")
    staging_qualified = _qualified(staging.table, schema_name, dest_type)
    target_qualified = _qualified(destination.table or staging.table, schema_name,
                                  dest_type)

    # 1. Sweep spools no live run can own, then stream source into staging.
    _reap_orphan_spools(dest_cfg, schema_name, dest_type, keep=staging.table)
    drop_table(dest_cfg, staging.table, schema_name or None)

    stage_cb: Callable[..., None] | None = None
    if on_checkpoint:
        def stage_cb(chunk: int, chunks: int, rows: int, checkpoint: dict | None = None) -> None:  # type: ignore[misc]
            pct = int(25 + (chunk / max(chunks, 1)) * 35)
            on_checkpoint(chunk, chunks, rows, checkpoint=checkpoint or {"phase": "staging", "progress_pct": pct})

    rows_written = 0
    ddl_log: list[str] = []
    dest_summary: dict[str, Any] = {}

    # The staging load is inside the same try as the apply: a source that dies
    # mid-stage is the most likely failure of the two, and outside the try it
    # left its spool behind in the customer's schema forever.
    try:
        rows_staged, stage_ddl, stage_summary, stage_columns = stream_database_transfer(
            source,
            staging,
            mappings,
            schema,
            on_checkpoint=stage_cb,
            sync_mode="full_refresh_overwrite",
            stream_contracts=[{"selected": True, "sync_mode": "full_refresh_overwrite"}],
            job_id=f"{job_id or ''}_stage",
            checkpoint_service=_NoOpCheckpointService(),
            backfill_new_fields=backfill_new_fields,
            validation_mode=validation_mode,
            limit=limit,
        )

        ddl_log = [
            f"STAGING {src_type}.{source.table or source.collection} → {staging_qualified} "
            f"({rows_staged:,} rows)",
        ]
        dest_summary = {
            "source_rows": rows_staged,
            "source_row_count": rows_staged,
            "staging_table": staging_qualified,
            "sync_mode": effective_sync,
        }

        if effective_sync == "scd2":
            from services.scd2_engine import apply_scd2, prepare_scd2_mapped_rows

            batch_size = 1_000
            if contract and contract.primary_key:
                conflict_columns = [
                    map_source_to_target(col, mappings) for col in contract.primary_key_columns()
                ]
            else:
                raise ValueError(
                    "SCD2 requires an explicit primary_key on the stream contract; "
                    "refuse inventing a conflict key from the first mapped column"
                )
            written_total = 0
            updated_total = 0
            active_rows = 0
            active_checksum = ""
            batch_idx = 0
            # Rough batch count for progress reporting; exact number does not matter.
            approx_batches = max(1, math.ceil(rows_staged / batch_size))

            stage_rejects = list(stage_summary.get("rejected_details") or [])
            rejected_all: list[dict] = []
            scd2_block_error: str | None = None
            partial_committed = False

            # Pass 1 — map/PK validate every staging batch BEFORE any history
            # merge. Per-batch SCD2 commits must not leave partial history when
            # a later batch hits FAIL_JOB / strict abort.
            for records in _read_staging_batches(
                staging, dest_cfg, schema_name, target_cols, batch_size
            ):
                if not records:
                    break
                prepared = prepare_scd2_mapped_rows(
                    destination,
                    records,
                    target_cols,
                    column_types,
                    mappings,
                    conflict_columns,
                    validation_mode=validation_mode,
                )
                rejected_all.extend(list(prepared.get("rejected_details") or []))
                if prepared.get("ok") is False:
                    scd2_block_error = str(
                        prepared.get("error")
                        or "SCD2 map/Risk Contract blocked history merge"
                    )
                    break

            if scd2_block_error:
                dest_summary["ok"] = False
                dest_summary["error"] = scd2_block_error
                dest_summary["partial_scd2_committed"] = False
                dest_summary["active_rows"] = 0
                dest_summary["active_checksum"] = ""
                dest_summary["updated_rows"] = 0
                dest_summary["rejected_details"] = stage_rejects + rejected_all
                dest_summary["rejected_rows"] = len(stage_rejects) + len(rejected_all)
                dest_summary["primary_key_columns"] = list(conflict_columns)
                rows_written = 0
            else:
                # Pass 2 — history merge (map already proven on staging snapshot).
                rejected_all = []
                for records in _read_staging_batches(
                    staging, dest_cfg, schema_name, target_cols, batch_size
                ):
                    if not records:
                        break
                    summary = apply_scd2(
                        destination,
                        records,
                        target_cols,
                        column_types,
                        mappings=mappings,
                        conflict_columns=conflict_columns,
                        batch_size=batch_size,
                        validation_mode=validation_mode,
                    )
                    rejected_all.extend(list(summary.get("rejected_details") or []))
                    if summary.get("ok") is False:
                        # Unexpected after preflight — still report honest partial.
                        scd2_block_error = str(
                            summary.get("error")
                            or "SCD2 map/Risk Contract blocked history merge"
                        )
                        partial_committed = written_total > 0
                        active_rows = int(summary.get("active_rows", 0))
                        active_checksum = str(summary.get("active_checksum", ""))
                        break
                    written_total += int(summary.get("rows_written", 0))
                    updated_total += int(summary.get("updated_rows", 0))
                    active_rows = int(summary.get("active_rows", 0))
                    active_checksum = str(summary.get("active_checksum", ""))
                    batch_idx += 1
                    if on_checkpoint:
                        on_checkpoint(
                            batch_idx,
                            approx_batches,
                            written_total,
                            checkpoint={"phase": "scd2"},
                        )

                dest_summary["active_rows"] = active_rows
                dest_summary["active_checksum"] = active_checksum
                dest_summary["updated_rows"] = updated_total
                dest_summary["rejected_details"] = stage_rejects + rejected_all
                dest_summary["rejected_rows"] = len(stage_rejects) + len(rejected_all)
                dest_summary["primary_key_columns"] = list(conflict_columns)
                if scd2_block_error:
                    dest_summary["ok"] = False
                    dest_summary["error"] = scd2_block_error
                    dest_summary["partial_scd2_committed"] = partial_committed
                    rows_written = written_total
                else:
                    rows_written = written_total

        elif effective_sync in ("full_refresh_mirror", "mirror"):
            from services.mirror_engine import (
                _compute_active_checksum,
                apply_inferred_deletes_via_staging,
            )

            # Stream upsert staging → target.
            upsert_contract = [{"selected": True, "sync_mode": "upsert", "primary_key": contract.primary_key}] if contract else None
            rows_upserted, _, upsert_summary, _ = stream_database_transfer(
                staging,
                destination,
                mappings,
                schema,
                on_checkpoint=on_checkpoint,
                sync_mode="upsert",
                stream_contracts=upsert_contract,
                job_id=f"{job_id or ''}_mirror",
                checkpoint_service=_NoOpCheckpointService(),
                backfill_new_fields=backfill_new_fields,
                validation_mode=validation_mode,
            )

            rows_written = rows_upserted
            dest_summary["upserted"] = rows_upserted
            dest_summary["checksum"] = upsert_summary.get("checksum", "")
            # Merge upsert quarantine into dest_summary — never drop stage/upsert DLQ.
            upsert_rejects = list(upsert_summary.get("rejected_details") or [])
            stage_rejects = list(stage_summary.get("rejected_details") or [])
            merged_rejects = stage_rejects + upsert_rejects
            if merged_rejects:
                dest_summary["rejected_details"] = merged_rejects
                dest_summary["rejected_rows"] = len(merged_rejects)
            else:
                dest_summary.setdefault(
                    "rejected_rows", int(upsert_summary.get("rejected_rows") or 0)
                )

            # Single SQL pass to reactivate present keys and soft-delete missing keys.
            engine = get_sqlalchemy_engine(dest_cfg)
            if contract and contract.primary_key:
                pk_cols = [
                    map_source_to_target(col, mappings) for col in contract.primary_key_columns()
                ]
            else:
                raise ValueError(
                    "Mirror requires an explicit primary_key on the stream contract; "
                    "refuse inventing a conflict key from the first mapped column"
                )
            with engine.connect() as conn:
                dest_summary.update(
                    apply_inferred_deletes_via_staging(
                        conn,
                        target_qualified,
                        staging_qualified,
                        pk_cols,
                        dialect=dest_type,
                    )
                )
                conn.commit()
                active_count, active_checksum = _compute_active_checksum(
                    conn, target_qualified, target_cols, "_deleted", batch_size=1_000
                )
                conn.commit()
            release_engine(engine)
            dest_summary["active_rows"] = active_count
            dest_summary["active_checksum"] = active_checksum

        else:
            raise ValueError(f"Unsupported sync mode for SCD2/mirror streaming: {effective_sync}")
    finally:
        # This run's spool goes on both the success and the failure path, and a
        # drop that fails must not mask the transfer's own error.
        try:
            drop_table(dest_cfg, staging.table, schema_name or None)
        except Exception as exc:
            logger.warning(
                "staging table %s could not be dropped: %s", staging_qualified, exc
            )

    ddl_log.append(f"{effective_sync.upper()} {staging_qualified} → {target_qualified}")
    # Rejected/coerced counts come from the staging write and the SCD2/mirror
    # merge itself; they do NOT mean "unchanged rows" for idempotent modes.
    dest_summary.setdefault("rejected_rows", stage_summary.get("rejected_rows", 0))
    dest_summary.setdefault("coerced_null_rows", stage_summary.get("coerced_null_rows", 0))
    return rows_written, ddl_log, dest_summary, target_cols


def _read_staging_batches(
    endpoint: EndpointConfig,
    cfg: dict[str, Any],
    schema_name: str,
    columns: list[str],
    batch_size: int,
):
    """Yield batches from staging via one streamed SELECT. Never OFFSET."""
    import sqlalchemy as sa
    from connectors.generic_sql import get_sqlalchemy_engine
    from connectors.writer_common import quote_sql_identifier
    from services.reconciliation_api import iter_select_row_dicts

    engine = get_sqlalchemy_engine(cfg)
    from services.dialect_profiles import quote_char_for

    dialect = str(getattr(getattr(engine, "dialect", None), "name", "") or "")
    qchar = quote_char_for(dialect) or '"'
    qualified = _qualified(endpoint.table, schema_name, dialect)
    try:
        with engine.connect() as conn:
            cols = ",".join(quote_sql_identifier(c, qchar) for c in columns)
            sql = f"SELECT {cols} FROM {qualified}"  # nosec B608
            yield from iter_select_row_dicts(
                conn, sa.text(sql), columns, itersize=batch_size
            )
    finally:
        release_engine(engine)
