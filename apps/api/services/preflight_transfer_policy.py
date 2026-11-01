"""Transfer-level policy gates evaluated before any data is read (re-exported by services.preflight_service)."""

from __future__ import annotations

import logging
from preflight.models import GateStatus
from services.preflight_cursor_gate import build_sync_contract_gate
from typing import Any

logger = logging.getLogger("services.preflight_service")


def run_transfer_policy_gates(
    *,
    sync_mode: str = "full_refresh_overwrite",
    schema_policy: str = "manual_review",
    validation_mode: str = "strict",
    stream_contracts: list[dict[str, Any]] | None = None,
    backfill_new_fields: bool = False,
    source_columns: list[str] | None = None,
    dest_type: str | None = None,
    source_type: str | None = None,
    source_kind: str = "file",
    write_via_staging: bool = False,
    source_read_mode: str = "",
    delivery_guarantee: str = "auto",
    allow_append_only: bool = False,
    read_scope: Any = None,
    priority_column: str = "",
    priority_direction: str = "desc",
    row_limit: int = 0,
    source_endpoint: Any = None,
    destination_endpoint: Any = None,
    catalog_primary_key_columns: list[str] | None = None,
    mappings: list[dict[str, Any]] | None = None,
    source_table: str = "",
    source_config: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Validate enterprise run policy that sits above source/destination probes."""

    from services.preflight_service import (
        _policy_source_config,
        _ps,
        confidence_threshold_for_mode,
    )

    from services.schema_drift import schema_policy_honesty_line

    contracts = [c for c in stream_contracts or [] if c.get("selected", True)]
    # Aliases such as ``cdc_incremental`` must hit the same contract rules as
    # ``cdc``. Leaving the raw token here let Validate approve a route Execute
    # then refused for a missing stream primary key.
    raw_sync = (sync_mode or "full_refresh_overwrite").strip().lower()
    try:
        from services.sync_cursor import normalize_sync_mode

        sync = normalize_sync_mode(raw_sync, default=raw_sync) if raw_sync else raw_sync
    except Exception:
        sync = raw_sync
    schema = (schema_policy or "manual_review").lower()
    validation = (validation_mode or "strict").lower()
    dest = (dest_type or "").strip().lower()
    src = (source_type or "").strip().lower()
    source_config = _policy_source_config(source_config, source_endpoint)
    product = str((source_config or {}).get("type") or "").strip().lower()
    # A TimescaleDB connector must not pass g9 because a driver alias rewrote
    # the format to postgresql. The product id is the one the operator saved.
    if product in {
        "timescaledb",
        "timescale",
        "cockroachdb",
        "citus",
        "greenplum",
        "yugabytedb",
        "redshift",
        "duckdb",
    }:
        src = product
    kind = (source_kind or "file").strip().lower()
    gates: list[dict[str, Any]] = []
    gates.append(
        build_sync_contract_gate(
            contracts,
            sync=sync,
            validation=validation,
            dest=dest,
            src=src,
            kind=kind,
            source_columns=source_columns,
            pass_status=GateStatus.PASS.value,
            block_status=GateStatus.BLOCK.value,
            source_read_mode=source_read_mode,
            read_scope=read_scope,
            catalog_primary_key_columns=catalog_primary_key_columns,
            mappings=mappings,
            source_table=source_table,
        )
    )

    schema_issues: list[str] = []
    from services.schema_drift import SCHEMA_POLICIES as allowed_schema
    if schema not in allowed_schema:
        schema_issues.append(
            f"Unknown schema policy '{schema}' — expected one of: "
            + ", ".join(sorted(allowed_schema))
        )

    # Stuck Studio toggle: backfill=true while policy is still manual_review (operator
    # switched policy back but the checkbox state was never cleared). That must not
    # fail Execute after Validate already passed — coerce to additive propagation.
    # type_locked / pause_on_change still forbid silent ADD COLUMN.
    from services.schema_drift import effective_schema_policy

    coerced_schema = effective_schema_policy(
        schema, backfill_new_fields=backfill_new_fields
    )
    policy_coerced = coerced_schema != schema
    schema = coerced_schema
    if backfill_new_fields and schema in {"type_locked", "pause_on_change"}:
        schema_issues.append(
            "Backfill new fields conflicts with schema policy "
            f"'{schema}' — switch to Propagate columns, or turn backfill off"
        )

    breaking = {
        "manual_review": "pause_for_manual_review",
        "pause_on_change": "halt_pipeline",
        "type_locked": "reject_type_changes",
        "propagate_columns": "auto_add_columns",
        "propagate_all": "auto_propagate_schema",
        "block_all": "halt_pipeline",
    }.get(schema, "pause_for_manual_review")

    if schema_issues:
        gates.append(
            {
                "id": "g10_schema_policy",
                "status": GateStatus.BLOCK.value,
                "message": "Schema change policy incomplete",
                "duration_ms": 0,
                "details": {"issues": schema_issues, "schema_policy": schema},
            }
        )
    else:
        gates.append(
            {
                "id": "g10_schema_policy",
                "status": GateStatus.PASS.value,
                "message": (
                    f"Schema policy set to {schema.replace('_', ' ')}"
                    + (
                        " (aligned backfill with propagate columns)"
                        if policy_coerced
                        else ""
                    )
                ),
                "duration_ms": 0,
                "details": {
                    "schema_policy": schema,
                    "backfill_new_fields": backfill_new_fields,
                    "breaking_changes": breaking,
                    "policy_coerced_from_manual_review": policy_coerced,
                    "honesty_line": schema_policy_honesty_line(schema),
                },
            }
        )

    from services.validation_mode_contract import (
        VALIDATION_MODE_ALIASES,
        normalize_validation_mode,
    )

    effective_mode = normalize_validation_mode(validation)
    threshold = confidence_threshold_for_mode(effective_mode)
    posture = f"Validation posture {effective_mode} uses confidence threshold {threshold:.2f}"
    if validation != effective_mode:
        if VALIDATION_MODE_ALIASES.get(validation) == effective_mode:
            posture += f" (requested {validation!r} runs as {effective_mode})"
        else:
            posture += f" (requested {validation!r} is not a validation mode — applied strict)"
            logger.warning(
                "Validate: unknown validation_mode %r, applying strict (floor %.2f)",
                validation,
                threshold,
            )
    gates.append(
        {
            "id": "g11_validation_posture",
            "status": GateStatus.PASS.value,
            "message": posture,
            "duration_ms": 0,
            "details": {
                "validation_mode": effective_mode,
                "requested_validation_mode": validation,
                "confidence_threshold": threshold,
            },
        }
    )

    staging_issues: list[str] = []
    if write_via_staging:
        try:
            from services.pre_ingestion_staging import dest_supports_staging

            if not dest:
                staging_issues.append(
                    "write_via_staging requires a SQL destination type"
                )
            elif not dest_supports_staging(dest):
                staging_issues.append(
                    f"write_via_staging is not supported for destination '{dest}' "
                    "(SQL table destinations only)"
                )
        except Exception:
            staging_issues.append(
                "write_via_staging could not be verified for this destination"
            )
    if staging_issues:
        gates.append(
            {
                "id": "g12_staging_policy",
                "status": GateStatus.BLOCK.value,
                "message": "Write-via-staging not supported for this route",
                "duration_ms": 0,
                "details": {
                    "issues": staging_issues,
                    "write_via_staging": True,
                    "dest_type": dest or None,
                },
            }
        )
    else:
        gates.append(
            {
                "id": "g12_staging_policy",
                "status": GateStatus.PASS.value,
                "message": (
                    "Write-via-staging enabled for SQL destination"
                    if write_via_staging
                    else "Direct write (staging off)"
                ),
                "duration_ms": 0,
                "details": {
                    "write_via_staging": bool(write_via_staging),
                    "dest_type": dest or None,
                },
            }
        )

    # An append-only destination (registry: append yes; upsert, merge and
    # overwrite no) cannot honour a sync mode that promises destination-side
    # semantics - overwrite replace, deduped/upsert merge, mirror delete, scd2
    # versioning, reverse_etl. Kafka topics are the case in point: those modes
    # would silently run as a plain produce, publishing duplicates that cannot be
    # rolled back. Refuse at Validate instead of failing mid-run (C26/D90).
    from services.connector_capability_registry import is_append_only_destination
    from services.copy_kafka_common import kafka_family_name
    from services.sync_cursor import is_append_sync

    # ``kafka_family_name`` folds the Kafka-protocol spellings the registry has
    # no record for (redpanda, msk, confluent_kafka ...) onto its ``kafka`` entry.
    if dest and is_append_only_destination(kafka_family_name(dest)):
        if is_append_sync(sync):
            gates.append(
                {
                    "id": "g_dest_append_only",
                    "status": GateStatus.PASS.value,
                    "message": (
                        f"Destination '{dest}': append only - written rows "
                        "are final and cannot be rolled back"
                    ),
                    "duration_ms": 0,
                    "details": {"dest_type": dest, "sync_mode": sync},
                }
            )
        else:
            gates.append(
                {
                    "id": "g_dest_append_only",
                    "status": GateStatus.BLOCK.value,
                    "message": (
                        f"Destination '{dest}' supports append only - sync_mode "
                        f"'{sync}' cannot replace, dedupe or merge into it. "
                        "Choose Append or Incremental append; rows written "
                        "cannot be rolled back."
                    ),
                    "duration_ms": 0,
                    "details": {
                        "dest_type": dest,
                        "sync_mode": sync,
                        "allowed_modes": ["full_refresh_append", "incremental_append"],
                    },
                }
            )

    from services.cdc_exactly_once import (
        preflight_delivery_gate, route_declares_log_position, route_has_cdc_pk)

    eos_gate = preflight_delivery_gate(
        sync_mode=sync,
        dest_type=dest,
        source_type=src,
        delivery_guarantee=delivery_guarantee,
        has_primary_key=route_has_cdc_pk(contracts),
        allow_append_only=allow_append_only,
        callable_source=(source_read_mode or "").strip().lower() in {"procedure", "query"},
        has_lsn_column=route_declares_log_position(contracts),
    )
    if eos_gate:
        gates.append(eos_gate)

    from services.cdc_snapshot_mode import build_snapshot_mode_preflight_gate

    snap_wm = getattr(read_scope, "watermark", None) if read_scope is not None else None
    snap_key = str(getattr(read_scope, "cursor_key", "") or "") if read_scope is not None else ""
    slot_probe = None
    if sync == "cdc":
        from services.cdc_slot_resume import probe_postgres_slot_for_preflight

        slot_probe = probe_postgres_slot_for_preflight(
            source_config,
            source_type=src,
            watermark=snap_wm,
            table=source_table,
            cursor_key=snap_key,
        )
    snap_gate = build_snapshot_mode_preflight_gate(
        sync_mode=sync,
        stream_contracts=contracts,
        watermark=snap_wm,
        retention=slot_probe,
        cursor_key=snap_key,
    )
    if snap_gate:
        gates.append(snap_gate)
    src_key = str(product or src or "").strip().lower().replace("-", "_")
    from services.cdc_log_capture_probe import is_postgres_log_source

    postgresish = is_postgres_log_source(src_key)
    if sync == "cdc" and (kind not in {"file", "cloud"} or postgresish):
        gates.extend(
            _ps._cdc_log_capture_gates(
                contracts,
                source_type=product or src,
                source_config=source_config,
                source_table=source_table,
                catalog_primary_key_columns=catalog_primary_key_columns,
                mappings=mappings,
            )
        )

    cap = max(0, int(row_limit or 0))
    priority = str(priority_column or "").strip()
    direction = "asc" if str(priority_direction or "").strip().lower() == "asc" else "desc"
    if cap > 0 or priority:
        gates.append(
            {
                "id": "g17_row_cap",
                "status": GateStatus.PASS.value,
                "severity": "warn",
                "message": (
                    "Execute writes"
                    + (f" at most {cap} rows" if cap > 0 else " the full mapped population")
                    + (f" after sorting by {priority} {direction}" if priority else "")
                    + ". Validate type-fit still walks the uncapped source (stricter). "
                    "Counts and uniqueness are not a proof of the capped write."
                ),
                "duration_ms": 0,
                "details": {
                    "priority_column": priority or None,
                    "priority_direction": direction if priority else None,
                    "row_limit": cap,
                    "honesty": "execute_applies_cap",
                },
            }
        )

    from services.multi_stream_plan import stream_procedure_policy_gate
    from services.policy_gate_notes import redis_ttl_policy_gate

    redis_gate = redis_ttl_policy_gate(dest, src)
    if redis_gate:
        gates.append(redis_gate)
    procedure_gate = stream_procedure_policy_gate(
        contracts,
        sync_mode=sync,
        source_endpoint=source_endpoint,
        destination_endpoint=destination_endpoint,
    )
    if procedure_gate:
        gates.append(procedure_gate)

    return gates
