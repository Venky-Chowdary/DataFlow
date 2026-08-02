"""Preflight validation for DataTransfer transfers."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Add preflight package to path
# apps/api/services → repo root is parents[3]
_PREFLIGHT_ROOT = Path(__file__).resolve().parents[3] / "packages" / "preflight" / "src"
if str(_PREFLIGHT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PREFLIGHT_ROOT))

from preflight import PreflightEngine
from preflight.models import (
    ColumnMapping,
    ColumnSchema,
    DestinationConfig,
    GateStatus,
    PreflightContext,
    SourceConfig,
    TransferPlan,
)

from services.connector_capability_registry import (
    classify_payload,
    recommended_batch_size,
)
from services.db_type_utils import SCHEMALESS_DESTS, normalize_dest_kind
from services.primary_key import resolve_primary_key_source
from services.source_duplicate_probe import probe_source_duplicate_keys
from services.transform_engine import (
    infer_date_locale,
    reset_active_date_locale,
    set_active_date_locale,
)
from services.validation_plan import build_validation_plan
from services.value_serializer import cell_to_string


def _with_date_locale(fn):
    """Set the active date_locale context for the duration of the call."""

    def wrapper(*args, **kwargs):
        locale = kwargs.get("date_locale", "")
        token = set_active_date_locale(locale)
        try:
            return fn(*args, **kwargs)
        finally:
            reset_active_date_locale(token)

    return wrapper


class FilePreflightContext(PreflightContext):
    """Preflight context for file → database transfers."""

    def __init__(
        self,
        plan: TransferPlan,
        sample_rows: list[dict] | None = None,
        source_duplicate_findings: list[dict[str, Any]] | None = None,
        source_duplicate_probe_ran: bool = False,
        source_duplicate_probe_pk: str = "",
    ):
        super().__init__(plan=plan, sample_rows=sample_rows or [])
        self.source_duplicate_findings = source_duplicate_findings or []
        self.source_duplicate_probe_ran = bool(source_duplicate_probe_ran)
        self.source_duplicate_probe_pk = str(source_duplicate_probe_pk or "")

    def run_dry_run(self, sample_size: int = 1000) -> tuple[bool, list[str]]:
        if not self.sample_rows:
            return False, [
                (
                    "No sample rows available for dry-run validation. "
                    "Re-run Source introspect so DataFlow can load a preview sample "
                    "from the source table (column metadata alone is not enough)."
                )
            ]

        headers = list(self.sample_rows[0].keys()) if self.sample_rows else []
        # Use cell_to_string so nested lists/dicts from schemaless sources become
        # valid JSON strings instead of Python repr() artifacts.
        scanned = self.sample_rows[:sample_size]
        rows = [[cell_to_string(row.get(h, "")) for h in headers] for row in scanned]
        self._last_dry_run_meta = {
            "sample_rows_scanned": len(scanned),
            "sample_rows_available": len(self.sample_rows),
            "sample_cap": sample_size,
        }
        column_types = {c.name: c.inferred_type for c in self.plan.source.columns}
        dest_types_by_name = {
            c.name: c.inferred_type for c in self.plan.destination.target_columns
        }
        mapping_dicts = [
            {
                "source": m.source,
                "target": m.target,
                "transform": getattr(m, "transform", ""),
                # Prefer live dest DDL; keep stamped create-new type when absent.
                "target_type": dest_types_by_name.get(m.target)
                or getattr(m, "target_type", None),
                "create_new": bool(getattr(m, "create_new", False)),
            }
            for m in self.plan.mappings
        ]

        try:
            from services.transform_engine import dry_run_sample

            return dry_run_sample(
                headers=headers,
                sample_rows=rows,
                mappings=mapping_dicts,
                column_types=column_types,
            )
        except Exception as exc:
            logger.debug("dry-run sample failed: %s", exc, exc_info=exc)
            errors: list[str] = []
            for i, row in enumerate(self.sample_rows[:sample_size]):
                for m in self.plan.mappings:
                    if m.source not in row and m.source in {
                        c.name for c in self.plan.source.columns
                    }:
                        errors.append(f"Row {i}: missing source column '{m.source}'")
                        if len(errors) >= 10:
                            return False, errors
            return len(errors) == 0, errors

    def coercion_report(self) -> dict[str, Any]:
        """Predict per-value write coercion for the plan against sample rows.

        Reuses the exact transform-resolution and coercion the write path uses so
        the preflight verdict matches the real write outcome. Cached so G3 and the
        API response layer share one computation.
        """
        cached = getattr(self, "_coercion_report_cache", None)
        if cached is not None:
            return cached
        try:
            from services.coercion_probe import analyze_coercion

            source_types = {c.name: c.inferred_type for c in self.plan.source.columns}
            dest_types = {
                c.name: c.inferred_type for c in self.plan.destination.target_columns
            }
            mapping_dicts = [
                {
                    "source": m.source,
                    "target": m.target,
                    "transform": getattr(m, "transform", None),
                    "target_type": dest_types.get(m.target)
                    or getattr(m, "target_type", None),
                    "create_new": bool(getattr(m, "create_new", False)),
                }
                for m in self.plan.mappings
            ]
            report = analyze_coercion(
                sample_rows=self.sample_rows,
                mappings=mapping_dicts,
                source_types=source_types,
                dest_types=dest_types,
                dest_db_type=self.plan.destination.db_type,
                table_exists=getattr(self.plan.destination, "table_exists", None),
                validation_mode=getattr(self.plan, "validation_mode", None) or "strict",
            )
        except Exception as exc:
            logger.warning(
                "coercion probe failed during preflight: %s", exc, exc_info=exc
            )
            report = {}
        self._coercion_report_cache = report
        return report

    def probe_unique_constraint(self, columns: list[str]) -> list[dict[str, Any]]:
        if not columns or not self.sample_rows:
            return []
        col = columns[0]
        source_col = col
        for m in self.plan.mappings:
            if m.target == col:
                source_col = m.source
                break
        # Case-insensitive dest collations equate A/a — uniqueness must too
        # (MySQL utf8mb4_*_ci / SQL Server *_CI_AS / CITEXT), else Validate false-greens.
        dest_type = ""
        for c in getattr(self.plan.destination, "target_columns", None) or []:
            if getattr(c, "name", None) == col:
                dest_type = str(getattr(c, "inferred_type", "") or "")
                break
        from services.type_system import (
            unique_equality_key,
            unique_key_forces_casefold,
            unique_key_nulls_collide,
            unique_key_row_in_scope,
        )

        unique_keys = getattr(self.plan, "destination_unique_keys", None) or []
        casefold = unique_key_forces_casefold(
            col,
            ddl_type=dest_type,
            unique_keys=unique_keys,
        )
        nulls_collide = unique_key_nulls_collide(col, unique_keys=unique_keys)
        null_sentinel = "\x00NULL\x00" if nulls_collide else None
        seen: dict[str, int] = {}
        examples: dict[str, str] = {}
        dupes: list[dict[str, Any]] = []
        for row in self.sample_rows:
            scope = dict(row)
            if col not in scope and source_col in scope:
                scope[col] = scope.get(source_col)
            if not unique_key_row_in_scope(scope, col, unique_keys=unique_keys):
                continue
            raw_cell = row.get(source_col, "")
            raw = cell_to_string(raw_cell) if raw_cell is not None else ""
            dest_kind = ""
            try:
                dest_kind = str(getattr(self.plan.destination, "db_type", "") or "")
            except Exception:
                dest_kind = ""
            key = unique_equality_key(
                None if raw_cell is None else raw,
                dest_type,
                force_casefold=casefold,
                null_sentinel=null_sentinel,
                dest_kind=dest_kind,
            )
            if not key and not nulls_collide:
                continue
            if not key:
                continue
            seen[key] = seen.get(key, 0) + 1
            examples.setdefault(key, raw if raw else "<NULL>")
        for key, count in seen.items():
            if count > 1 and key:
                dupes.append(
                    {
                        "column": col,
                        "value": examples.get(key, key),
                        "count": count,
                        "collation_casefold": casefold,
                        "nulls_not_distinct": nulls_collide,
                    }
                )
        return dupes[:5]

    def run_integrity_audit(self) -> dict[str, Any]:
        from services.data_integrity import run_integrity_audit as audit

        source_columns = [c.name for c in self.plan.source.columns]
        mapping_dicts = [
            {
                "source": m.source,
                "target": m.target,
                "confidence": m.confidence,
                "transform": m.transform,
                "requires_review": m.requires_review,
                "target_type": next(
                    (
                        c.inferred_type
                        for c in self.plan.destination.target_columns
                        if c.name == m.target
                    ),
                    None,
                )
                or getattr(m, "target_type", None),
                "create_new": bool(getattr(m, "create_new", False)),
            }
            for m in self.plan.mappings
        ]
        source_schemas = [
            {"name": c.name, "inferred_type": c.inferred_type, "samples": c.samples}
            for c in self.plan.source.columns
        ]
        target_schemas = [
            {"name": c.name, "inferred_type": c.inferred_type}
            for c in self.plan.destination.target_columns
        ]
        mode = getattr(self.plan, "validation_mode", "strict") or "strict"
        sync_mode = getattr(self.plan, "sync_mode", "") or ""
        return audit(
            source_columns=source_columns,
            mappings=mapping_dicts,
            source_schemas=source_schemas,
            target_schemas=target_schemas,
            sample_rows=self.sample_rows,
            validation_mode=mode,
            destination_db_type=self.plan.destination.db_type,
            sync_mode=sync_mode,
            contract_primary_key=getattr(self.plan, "contract_primary_key", None)
            or None,
            destination_pk_columns=getattr(self.plan, "destination_pk_columns", None)
            or None,
            destination_unique_keys=getattr(self.plan, "destination_unique_keys", None)
            or None,
            source_duplicate_findings=self.source_duplicate_findings,
            source_duplicate_probe_ran=self.source_duplicate_probe_ran,
            source_duplicate_probe_pk=self.source_duplicate_probe_pk,
        )


VALIDATION_CONFIDENCE_THRESHOLDS = {
    "balanced": 0.75,
    "strict": 0.85,
    "maximum": 0.95,
}


def confidence_threshold_for_mode(validation_mode: str | None) -> float:
    return VALIDATION_CONFIDENCE_THRESHOLDS.get(
        (validation_mode or "strict").lower(), 0.85
    )


# Destinations that honor SCD2 / mirror streaming paths (must match Studio gating).
_SQL_HISTORY_SYNC_DESTS = frozenset({
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

# Sources that can drive CDC (log / change-stream) in production.
_CDC_CAPABLE_SOURCES = frozenset({
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
) -> list[dict[str, Any]]:
    """Validate enterprise run policy that sits above source/destination probes."""
    contracts = [c for c in stream_contracts or [] if c.get("selected", True)]
    sync = (sync_mode or "full_refresh_overwrite").lower()
    schema = (schema_policy or "manual_review").lower()
    validation = (validation_mode or "strict").lower()
    dest = (dest_type or "").strip().lower()
    src = (source_type or "").strip().lower()
    kind = (source_kind or "file").strip().lower()
    multi_stream = len(contracts) > 1
    requires_cursor = sync in {"incremental_append", "incremental_deduped", "cdc"}
    requires_primary_key = sync in {
        "upsert",
        "incremental_deduped",
        "cdc",
        "scd2",
        "mirror",
    }

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

    gates: list[dict[str, Any]] = []
    sync_issues: list[str] = []
    if sync in {"scd2", "mirror"}:
        if multi_stream:
            sync_issues.append(
                f"{sync.upper()} is not supported for multi-stream transfers"
            )
        elif not dest:
            sync_issues.append(
                f"{sync.upper()} requires a SQL table destination"
            )
        elif dest not in _SQL_HISTORY_SYNC_DESTS:
            sync_issues.append(
                f"{sync.upper()} requires a SQL table destination (not '{dest}')"
            )
    if sync == "cdc":
        if kind in {"file", "cloud"}:
            sync_issues.append("CDC requires a database source (not file/cloud)")
        elif src and src not in _CDC_CAPABLE_SOURCES:
            sync_issues.append(
                f"CDC is not supported for source type '{src}'"
            )
    if missing_cursor:
        sync_issues.append(f"Missing cursor field for {', '.join(missing_cursor[:5])}")
    if missing_primary_key:
        sync_issues.append(
            f"Missing primary key for {', '.join(missing_primary_key[:5])}"
        )
    if unknown_cursor:
        sync_issues.append(
            f"Cursor field not in source schema: {', '.join(unknown_cursor[:5])}"
        )
    if unknown_pk:
        sync_issues.append(
            f"Primary key not in source schema: {', '.join(unknown_pk[:5])}"
        )

    if sync_issues:
        gates.append(
            {
                "id": "g9_sync_contract",
                "status": GateStatus.BLOCK.value,
                "message": "Sync mode contract incomplete",
                "duration_ms": 0,
                "details": {"issues": sync_issues, "sync_mode": sync},
            }
        )
    else:
        gates.append(
            {
                "id": "g9_sync_contract",
                "status": GateStatus.PASS.value,
                "message": f"Sync contract valid for {sync.replace('_', ' ')}",
                "duration_ms": 0,
                "details": {
                    "sync_mode": sync,
                    "streams": len(contracts),
                    "requires_cursor": requires_cursor,
                    "requires_primary_key": requires_primary_key,
                },
            }
        )

    schema_issues: list[str] = []
    allowed_schema = {
        "manual_review",
        "propagate_columns",
        "propagate_all",
        "pause_on_change",
        "type_locked",
    }
    if schema not in allowed_schema:
        schema_issues.append(f"Unknown schema policy '{schema}'")

    # Stuck Studio toggle: backfill=true while policy is still manual_review (operator
    # switched policy back but the checkbox state was never cleared). That must not
    # fail Execute after Validate already passed — coerce to additive propagation.
    # type_locked / pause_on_change still forbid silent ADD COLUMN.
    policy_coerced = False
    if backfill_new_fields and schema == "manual_review":
        schema = "propagate_columns"
        policy_coerced = True
    elif backfill_new_fields and schema in {"type_locked", "pause_on_change"}:
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
                },
            }
        )

    gates.append(
        {
            "id": "g11_validation_posture",
            "status": GateStatus.PASS.value,
            "message": f"Validation posture {validation} uses confidence threshold {confidence_threshold_for_mode(validation):.2f}",
            "duration_ms": 0,
            "details": {
                "validation_mode": validation,
                "confidence_threshold": confidence_threshold_for_mode(validation),
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

    return gates


def is_compliance_only_block(proof_blockers: list[str]) -> bool:
    """Return True when every proof blocker is purely a PII/compliance review."""
    if not proof_blockers:
        return False
    return all(
        "PII/compliance" in b or "compliance review" in b.lower()
        for b in proof_blockers
    )


def apply_policy_gates(
    result: dict[str, Any],
    policy_gates: list[dict[str, Any]],
    validation_mode: str = "strict",
    destination_db_type: str = "postgresql",
) -> dict[str, Any]:
    proof_bundle = result.get("proof_bundle") or {}
    transfer_decision = (proof_bundle.get("transfer_decision") or {}).get("decision")
    proof_blockers = (proof_bundle.get("transfer_decision") or {}).get("blockers") or []
    compliance_only = bool(
        (proof_bundle.get("transfer_decision") or {}).get("compliance_only")
    ) or is_compliance_only_block(proof_blockers)

    is_strict = (validation_mode or "strict").lower() in {"strict", "maximum"}

    # In non-strict modes, PII/compliance review is a warning, not a hard blocker.
    # In strict mode, compliance-only stays a dedicated ack gate (Approve PII CTA)
    # rather than masquerading as a failed schema/data check.
    if is_strict:
        active_proof_blockers = list(proof_blockers)
    else:
        active_proof_blockers = [
            b
            for b in proof_blockers
            if "PII/compliance" not in b and "compliance review" not in b.lower()
        ]

    blockers = [
        {"id": b["id"], "message": b["message"], "details": b.get("details", {})}
        for b in result.get("blockers", [])
    ]
    for idx, message in enumerate(active_proof_blockers):
        is_compliance = (
            "PII/compliance" in str(message) or "compliance review" in str(message).lower()
        )
        blockers.append({
            "id": f"proof_{idx}",
            "message": str(message),
            "details": {
                "compliance_ack_required": bool(is_compliance and compliance_only),
                "remediation_kind": "acknowledge_compliance" if is_compliance else "fix_proof",
            },
        })

    if policy_gates:
        gates = [*result.get("gates", []), *policy_gates]
        blockers.extend(
            {"id": g["id"], "message": g["message"], "details": g.get("details", {})}
            for g in policy_gates
            if g.get("status") == GateStatus.BLOCK.value
        )
    else:
        gates = list(result.get("gates", []))

    passed_count = sum(1 for g in gates if g.get("status") == GateStatus.PASS.value)
    total_gates = len(gates)
    has_blocks = any(g.get("status") == GateStatus.BLOCK.value for g in gates)

    proof_blocks = (
        transfer_decision in {"block", "review"} or proof_bundle.get("passed") is False
    )
    if proof_blocks and not is_strict:
        if active_proof_blockers:
            proof_blocks = True
        else:
            proof_blocks = False

    if proof_blocks:
        has_blocks = True

    if proof_bundle:
        proof_bundle = {**proof_bundle}
        base_decision = proof_bundle.get("transfer_decision") or {}
        if has_blocks:
            gate_blocker_messages = [b["message"] for b in blockers]
            decision_blockers = list(base_decision.get("blockers") or [])
            for msg in gate_blocker_messages:
                if msg not in decision_blockers:
                    decision_blockers.append(msg)
            # Compliance-only: keep decision=review so the UI shows Approve PII,
            # not a generic "schema failed" block with contradictory 12/12 passed.
            decision_label = "review" if compliance_only and not any(
                g.get("status") == GateStatus.BLOCK.value for g in gates
            ) else "block"
            proof_bundle["passed"] = False
            proof_bundle["transfer_decision"] = {
                "decision": decision_label,
                "blockers": decision_blockers,
                "compliance_only": compliance_only,
                "reason": "; ".join(decision_blockers)
                if decision_blockers
                else "Preflight gates blocked the transfer",
                "warnings": [],
            }
        else:
            # No hard gate blocks; downgrade proof decision to review/approve and surface
            # compliance warnings so the UI shows the risk without disabling the transfer.
            warnings = [b for b in proof_blockers if b not in active_proof_blockers]
            decision = (
                "review"
                if (transfer_decision in {"block", "review"} or compliance_only)
                else "approve"
            )
            proof_bundle["passed"] = True
            proof_bundle["transfer_decision"] = {
                "decision": decision,
                "blockers": [],
                "compliance_only": False,
                "reason": (
                    "No blocking issues detected"
                    if not warnings
                    else "; ".join(warnings)
                ),
                "warnings": warnings,
            }

    from services.preflight_rules import enrich_blockers

    dest_kind = normalize_dest_kind(destination_db_type)
    enriched_blockers = enrich_blockers(
        blockers,
        dest_kind=dest_kind,
        validation_mode=validation_mode,
    )

    return {
        **result,
        "passed": not has_blocks,
        "passed_count": passed_count,
        "total_gates": total_gates,
        "readiness_score": round(passed_count / max(total_gates, 1) * 100, 1),
        "gates": gates,
        "blockers": enriched_blockers,
        "proof_bundle": proof_bundle,
    }


@_with_date_locale
def run_file_preflight(
    *,
    columns: list[str],
    column_types: dict[str, str],
    row_count: int,
    mappings: list[dict[str, Any]],
    column_nullability: dict[str, bool] | None = None,
    destination_connected: bool = False,
    destination_error: str | None = None,
    source_connected: bool = True,
    source_error: str | None = None,
    source_kind: str = "file",
    source_format: str = "",
    sync_mode: str = "append",
    sample_rows: list[dict] | None = None,
    estimated_bytes: int = 0,
    confidence_threshold: float = 0.85,
    validation_mode: str = "strict",
    destination_column_types: dict[str, str] | None = None,
    destination_column_nullability: dict[str, bool] | None = None,
    destination_table_exists: bool | None = None,
    destination_can_create: bool | None = None,
    destination_can_write: bool | None = None,
    privilege_probe: dict[str, Any] | None = None,
    available_staging_bytes: int | None = None,
    destination_db_type: str = "postgresql",
    source_connector_id: str = "",
    source_config: dict[str, Any] | None = None,
    source_table: str = "",
    destination_table: str = "",
    source_filename: str = "",
    schema_policy: str = "manual_review",
    backfill_new_fields: bool = False,
    stored_source_fp: str = "",
    stored_target_fp: str = "",
    previous_source_columns: list[str] | None = None,
    previous_source_schema: dict[str, str] | None = None,
    contract_primary_key: str | None = None,
    destination_pk_columns: list[str] | None = None,
    destination_unique_keys: list[dict[str, Any]] | None = None,
    date_locale: str = "",
    cursor_fields: list[str] | None = None,
    compliance_acknowledged: bool = False,
    schema_drift_acknowledged: bool = False,
    acknowledgment_actor: str = "",
    acknowledgment_reason: str = "",
) -> dict[str, Any]:
    """Run preflight gates for file/DB Studio transfers (G1–G8 + integrity)."""

    if row_count <= 0 and sample_rows:
        row_count = len(sample_rows)

    # Preflight is a sample-based safety check, not a full table scan.  Cap the
    # sample size so very large file previews or database samples cannot make the
    # validate step hang. Keep in sync with coercion_probe.DEFAULT_SAMPLE_LIMIT.
    from services.coercion_probe import PREFLIGHT_SAMPLE_LIMIT

    if sample_rows and len(sample_rows) > PREFLIGHT_SAMPLE_LIMIT:
        sample_rows = sample_rows[:PREFLIGHT_SAMPLE_LIMIT]

    # If the operator did not specify a locale for ambiguous day/month dates,
    # scan the sample for an unambiguous majority before any date coercion.
    if sample_rows and columns:
        inferred_locale = infer_date_locale(
            sample_rows, columns, existing_locale=date_locale
        )
        if inferred_locale and not date_locale:
            date_locale = inferred_locale
            set_active_date_locale(date_locale)

    # If the caller did not supply rich source types, infer them from the sample
    # rows. This keeps schemaless sources (MongoDB, DynamoDB, Redis, S3 JSON) from
    # being treated as all-VARCHAR against a typed warehouse target.
    if sample_rows and columns:
        generic_types = {"", "varchar", "text", "string"}
        if not column_types or all(
            (column_types.get(c) or "").lower() in generic_types for c in columns
        ):
            try:
                from services.file_parser import FileParser

                inferred = FileParser.infer_schema(sample_rows)
                if inferred:
                    column_types = {
                        **column_types,
                        **{
                            c: inferred.get(c, column_types.get(c, "VARCHAR"))
                            for c in columns
                        },
                    }
            except Exception as exc:
                logger.debug("preflight schema inference failed: %s", exc, exc_info=exc)

    # Source nullability defaults to True (unknown), which is the safe reading
    # for files. For an introspected database source it is knowable, and
    # assuming otherwise made G3's NOT NULL contract fire on a NOT NULL source
    # column — so copying a table onto an identical table with a PRIMARY KEY
    # blocked with a coercion issue that did not exist.
    src_nulls = column_nullability or {}

    def _src_nullable(name: str) -> bool:
        if name in src_nulls:
            return bool(src_nulls[name])
        return next(
            (bool(v) for k, v in src_nulls.items() if k.lower() == name.lower()),
            True,
        )

    source_cols = [
        ColumnSchema(
            name=c,
            inferred_type=column_types.get(c, "VARCHAR").upper(),
            nullable=_src_nullable(c),
        )
        for c in columns
    ]
    from services.mapping_constraints import is_intentional_omit, write_mappings

    dest_types = destination_column_types or {}
    dest_nulls = destination_column_nullability or {}
    write_maps = write_mappings(mappings)
    dest_cols = []
    for m in write_maps:
        tgt = m.get("target") or ""
        if not tgt:
            continue
        inferred = dest_types.get(
            tgt,
            m.get("target_type") or column_types.get(m["source"], "VARCHAR"),
        ).upper()
        # Prefer explicit map; else case-insensitive lookup; default nullable=True
        # (create-new / unknown) so we never invent NOT NULL.
        if tgt in dest_nulls:
            nullable = bool(dest_nulls[tgt])
        else:
            nullable = next(
                (bool(dest_nulls[k]) for k in dest_nulls if k.lower() == str(tgt).lower()),
                True,
            )
        if "target_nullable" in m:
            nullable = bool(m.get("target_nullable"))
        dest_cols.append(
            ColumnSchema(name=tgt, inferred_type=inferred, nullable=nullable)
        )
    plan_mappings = [
        ColumnMapping(
            source=m["source"],
            target=m.get("target") or "",
            confidence=float(m.get("confidence", 0.0)),
            transform=m.get("transform"),
            user_override=bool(m.get("user_override", False)),
            reasoning=m.get("reasoning") or m.get("reason", ""),
            requires_review=bool(m.get("requires_review", False)),
            score_gap=float(m.get("score_gap", 1.0)),
            target_type=m.get("target_type") or m.get("targetType"),
            create_new=bool(m.get("create_new") or m.get("createNew", False)),
            struct_policy=m.get("struct_policy") or m.get("structPolicy"),
            struct_derived=bool(
                m.get("struct_derived") or m.get("structDerived", False)
            ),
            struct_parent=m.get("struct_parent") or m.get("structParent"),
            fidelity=(m.get("fidelity") or None),
            type_narrowing=bool(m.get("type_narrowing") or m.get("typeNarrowing", False)),
            risk_acknowledged=bool(
                m.get("risk_acknowledged") or m.get("riskAcknowledged", False)
            ),
            intentional_omit=is_intentional_omit(m),
        )
        for m in mappings
    ]

    has_samples = bool(sample_rows)
    est_bytes = estimated_bytes if estimated_bytes > 0 else max(row_count * 128, 0)
    is_file_source = source_kind == "file"

    if available_staging_bytes is None:
        available_staging_bytes = _available_staging_bytes(est_bytes)

    dest_can_create = (
        destination_can_create
        if destination_can_create is not None
        else destination_connected
    )
    dest_can_write = (
        destination_can_write
        if destination_can_write is not None
        else destination_connected
    )
    # Keep tri-state: None = probe unknown. Never coerce unknown → create-new.
    dest_table_exists = destination_table_exists

    from services.ddl_compatibility import evaluate_ddl_compatibility
    from services.schema_drift import detect_schema_drift

    dest_kind = normalize_dest_kind(destination_db_type, default="postgresql")
    schemaless = dest_kind in SCHEMALESS_DESTS

    # Split drift vs DDL contracts:
    # - create-new / schemaless: no live dest types (stale Studio maps are noise)
    # - unknown existence: keep type hints for G6 lossy/width checks, but drift
    #   still treats the dest as non-live (no fingerprint / orphan locks)
    # - existing table: full live contract
    hinted_dest_types = dict(destination_column_types or {})
    if schemaless or dest_table_exists is False:
        drift_dest_types: dict[str, str] = {}
        ddl_dest_types: dict[str, str] = {}
    elif dest_table_exists is True:
        drift_dest_types = hinted_dest_types
        ddl_dest_types = hinted_dest_types
    else:
        drift_dest_types = {}
        ddl_dest_types = hinted_dest_types

    target_cols = list(drift_dest_types.keys())
    ddl_compatible, ddl_issues = evaluate_ddl_compatibility(
        mappings=write_maps,
        source_schema=column_types,
        target_schema=ddl_dest_types,
        sample_rows=sample_rows,
        table_exists=dest_table_exists,
        dest_connected=destination_connected,
        dest_db_type=destination_db_type,
        allow_create=dest_can_create,
        backfill_new_fields=backfill_new_fields,
        schema_policy=schema_policy,
        sync_mode=sync_mode,
        destination_table=destination_table,
        destination_pk_columns=destination_pk_columns,
        contract_primary_key=contract_primary_key,
    )

    drift = detect_schema_drift(
        source_columns=columns,
        source_schema=column_types,
        target_columns=target_cols or [m["target"] for m in write_maps if m.get("target")],
        target_schema=drift_dest_types,
        mappings=mappings,
        destination_db_type=destination_db_type,
        sample_rows=sample_rows,
        stored_source_fp=stored_source_fp or "",
        stored_target_fp=stored_target_fp or "",
        previous_source_columns=previous_source_columns,
        previous_source_schema=previous_source_schema,
        previous_primary_key=None,
        # Source contract PK only — destination DDL PK must not invent PK change.
        live_primary_key=None,
        cursor_fields=cursor_fields,
        schema_policy=schema_policy,
        table_exists=dest_table_exists,
    )
    # Do NOT fold drift into ddl_issues / ddl_compatible. G6 must mean real DDL
    # (missing columns, width, types). Drift is a separate contract gate below.

    sample_quality: dict[str, Any] = {}
    if sample_rows and columns:
        from services.sample_quality import analyze_dataset_quality

        sample_quality = analyze_dataset_quality(
            columns, sample_rows, schema=column_types, dest_kind=dest_kind
        )
        # Sample-quality findings (high null rates, outliers, etc.) describe the data,
        # not the target schema.  They are surfaced by the data-integrity gate (G9);
        # conflating them with DDL compatibility causes false "Target DDL incompatible"
        # blockers for real-world sparse collections.
        if schemaless:
            sample_quality["blocks_transfer"] = False

    plan = TransferPlan(
        source=SourceConfig(
            kind=source_kind,
            connected=source_connected and bool(columns),
            parseable=(is_file_source and has_samples and bool(columns))
            or (not is_file_source and bool(columns)),
            columns=source_cols,
            row_count_estimate=row_count,
            error=source_error,
        ),
        destination=DestinationConfig(
            kind="database",
            db_type=dest_kind,
            connected=destination_connected,
            can_create_table=dest_can_create,
            can_write=dest_can_write,
            target_columns=dest_cols,
            table_exists=dest_table_exists,
            error=destination_error,
            privilege_probe=privilege_probe,
        ),
        mappings=plan_mappings,
        dry_run_passed=False,
        ddl_compatible=ddl_compatible,
        ddl_issues=ddl_issues,
        estimated_bytes=est_bytes,
        available_staging_bytes=available_staging_bytes,
        confidence_threshold=confidence_threshold,
        validation_mode=validation_mode,
        sync_mode=sync_mode,
        contract_primary_key=str(contract_primary_key or "").strip(),
        destination_pk_columns=list(destination_pk_columns or []),
        destination_unique_keys=list(destination_unique_keys or []),
    )

    # Source-side duplicate-key probe: a small sample can miss duplicates in large
    # tables, so query the source directly when we have a resolved identity key.
    source_duplicate_findings: list[dict[str, Any]] = []
    source_duplicate_probe_ran = False
    source_duplicate_probe_pk = ""
    if (
        (source_connector_id or source_config)
        and source_table
        and source_kind in ("database", "cloud")
    ):
        try:
            source_pk = resolve_primary_key_source(
                mappings=mappings,
                source_columns=columns,
                dest_kind=dest_kind,
                validation_mode=validation_mode,
                purpose="uniqueness",
                destination_pk_columns=destination_pk_columns,
                contract_primary_key=contract_primary_key,
            )
            if source_pk:
                source_duplicate_probe_pk = source_pk
                source_duplicate_findings = probe_source_duplicate_keys(
                    source_connector_id=source_connector_id,
                    source_config=source_config,
                    source_table=source_table,
                    primary_key=source_pk,
                )
                source_duplicate_probe_ran = True
        except Exception as exc:
            logger.warning("Source duplicate-key probe skipped: %s", exc, exc_info=exc)

    ctx = FilePreflightContext(
        plan,
        sample_rows,
        source_duplicate_findings=source_duplicate_findings,
        source_duplicate_probe_ran=source_duplicate_probe_ran,
        source_duplicate_probe_pk=source_duplicate_probe_pk,
    )
    # Always collect every reachable gate on Validate. fail_fast=True hid G6 DDL
    # behind G5 integrity blocks and forced a multi-run fix loop. Transfer still
    # refuses to move rows when any blocker remains (passed=False).
    engine = PreflightEngine(fail_fast=False)
    result = engine.run(ctx)

    from services.preflight_proof_bundle import build_preflight_proof_bundle

    proof_bundle = build_preflight_proof_bundle(
        columns=columns,
        sample_rows=sample_rows or [],
        mappings=mappings,
        source_schemas=[
            {
                "name": c,
                "inferred_type": column_types.get(c, "VARCHAR").upper(),
                "samples": [
                    cell_to_string(row.get(c, ""))
                    for row in (sample_rows or [])[:20]
                    if row.get(c) is not None
                ],
            }
            for c in columns
        ],
        source_records=sample_rows or [],
        target_records=[],
        validation_mode=validation_mode,
        confidence_threshold=confidence_threshold,
        compliance_acknowledged=compliance_acknowledged,
        acknowledgment_actor=acknowledgment_actor,
        acknowledgment_reason=acknowledgment_reason,
    )

    from services.preflight_rules import enrich_blockers

    blockers = [
        {"id": b.gate_id.value, "message": b.message, "details": b.details}
        for b in result.blockers
    ]
    enriched_blockers = enrich_blockers(
        blockers,
        dest_kind=dest_kind,
        validation_mode=validation_mode,
    )

    from services.type_system import is_binary_type, is_structural_type

    has_binary = any(is_binary_type(t) for t in column_types.values())
    has_unstructured = any(is_structural_type(t) for t in column_types.values())
    _src_fmt = (source_format or source_kind).lower()
    _tgt_fmt = (destination_db_type or "").lower()
    payload_shape = classify_payload(
        source_format=_src_fmt,
        target_format=_tgt_fmt,
        has_binary=has_binary,
        has_unstructured=has_unstructured,
    )
    validation_plan = build_validation_plan(
        source_format=_src_fmt,
        target_format=_tgt_fmt,
        validation_mode=validation_mode,
        write_semantics=sync_mode,
        confidence_threshold=confidence_threshold,
    )

    out = {
        "passed": result.passed,
        "passed_count": result.passed_count,
        "total_gates": result.total_gates,
        "readiness_score": round(
            result.passed_count / max(result.total_gates, 1) * 100, 1
        ),
        "gates": [
            {
                "id": g.gate_id.value,
                "status": g.status.value,
                "message": g.message,
                "duration_ms": round(g.duration_ms, 2),
                "details": g.details,
            }
            for g in result.gates
        ],
        "blockers": enriched_blockers,
        "schema_drift": drift,
        "ddl_issues": ddl_issues,
        "sample_quality": sample_quality,
        "proof_bundle": proof_bundle,
        "payload_shape": payload_shape,
        "validation_plan": validation_plan.to_dict(),
        "coercion_report": ctx.coercion_report(),
        "date_locale": date_locale,
        "privilege_probe": privilege_probe or {},
        "recommended_batch_size": min(
            recommended_batch_size(_src_fmt),
            recommended_batch_size(_tgt_fmt) or recommended_batch_size(_src_fmt),
        ),
    }

    # Multi-load intelligence: compare sample to last N loads of this route.
    try:
        from services.data_quality_history import compare_route_to_history

        src_table = (source_table or source_filename or "").strip()
        dst_table = (destination_table or "").strip()
        src_ep = {
            "kind": source_kind,
            "format": source_format or "",
            "table": src_table,
            "collection": src_table,
        }
        dst_ep = {
            "kind": "database",
            "format": destination_db_type or "",
            "table": dst_table,
            "collection": dst_table,
        }
        out["load_history_report"] = compare_route_to_history(
            sample_rows or [],
            src_ep,
            dst_ep,
            schema=column_types,
        )
    except Exception as hist_exc:
        logger.warning("load history compare failed during preflight", exc_info=True)
        out["load_history_report"] = {
            "passed": True,
            "anomalies": [],
            "prior_load_count": 0,
            "warning": f"Load-history compare unavailable: {hist_exc!s}"[:240],
        }

    # Schema drift is its own rule — never masquerade as Target DDL.
    # Airbyte rule: hard-breaking ALWAYS pauses (even under propagate_*).
    # Propagate auto-applies additive; manual_review keeps existing mappings.
    evolution = drift.get("schema_evolution") or {}
    action = evolution.get("action")
    # Evolution is the sole authority for pause|propagate|review|continue.
    # Fingerprint flags alone must not bypass resolve_schema_evolution.
    requires_drift_decision = bool(
        evolution.get("should_pause")
        or evolution.get("should_propagate")
        or action not in (None, "continue")
    )
    if requires_drift_decision:
        policy = (schema_policy or "manual_review").strip().lower()
        if schemaless and not evolution.get("hard_breaking"):
            out.setdefault("warnings", []).append(
                {
                    "id": "schema_drift",
                    "message": (
                        "Mapping/schema fingerprint changed on a schemaless destination — "
                        "informational only (no DDL to invalidate)."
                    ),
                    "details": {
                        "issues": list(drift.get("issues") or []),
                        "severity": "warning",
                        "schema_evolution": evolution,
                    },
                }
            )
        elif evolution.get("should_pause") or (
            drift.get("severity") == "breaking"
            and policy == "pause_on_change"
        ):
            hard = evolution.get("hard_breaking") or []
            if hard and isinstance(hard[0], dict) and hard[0].get("kind"):
                drift_msg = (
                    f"Breaking schema change: {hard[0].get('kind')} "
                    "— sync paused for review (Airbyte-class fail-closed)"
                )
            elif drift.get("issues"):
                drift_msg = str(drift["issues"][0])
            else:
                drift_msg = "Schema drift requires review"
            drift_gate = {
                "id": "schema_drift",
                "status": "block",
                "message": drift_msg,
                "duration_ms": 0,
                "details": {
                    "issues": list(drift.get("issues") or []),
                    "severity": "breaking",
                    "source_changed": drift.get("source_changed"),
                    "target_changed": drift.get("target_changed"),
                    "schema_evolution": evolution,
                    "rule_id": "schema_drift.breaking",
                    "remediation_kind": "approve_schema_drift",
                },
            }
            out["gates"] = [*out["gates"], drift_gate]
            drift_blocker = enrich_blockers(
                [
                    {
                        "id": "schema_drift",
                        "message": drift_msg,
                        "details": drift_gate["details"],
                    }
                ],
                dest_kind=dest_kind,
                validation_mode=validation_mode,
            )
            out["blockers"] = [*out["blockers"], *drift_blocker]
            out["passed"] = False
            out["passed_count"] = sum(
                1 for g in out["gates"] if g.get("status") == "pass"
            )
            out["total_gates"] = len(out["gates"])
            out["readiness_score"] = round(
                out["passed_count"] / max(out["total_gates"], 1) * 100, 1
            )
        elif evolution.get("should_propagate"):
            from services.schema_drift import apply_propagate_mappings

            propagated_maps, applied = apply_propagate_mappings(
                list(mappings or []),
                source_columns=list(columns or []),
                source_schema=column_types or {},
                evolution=evolution,
                schema_policy=policy,
            )
            if applied:
                out["effective_mappings"] = propagated_maps
                out["propagated_mappings"] = applied
            out["gates"] = [
                *out["gates"],
                {
                    "id": "schema_drift",
                    "status": "pass",
                    "message": (
                        f"Schema auto-propagate ({policy}): "
                        f"{len(applied)} column mapping(s) added; "
                        "destination ADD COLUMN on Execute"
                    ),
                    "duration_ms": 0,
                    "details": {
                        "issues": list(drift.get("issues") or []),
                        "severity": "additive",
                        "schema_policy": policy,
                        "schema_evolution": evolution,
                        "propagated": [a.get("source") for a in applied],
                        "backfill_recommended": evolution.get("backfill_recommended"),
                    },
                },
            ]
            out["schema_drift"] = drift
        else:
            # manual_review / type_locked review: human must acknowledge before green.
            # Silently passing hides new columns that will not transfer.
            if schema_drift_acknowledged:
                from datetime import datetime, timezone

                ack = {
                    "actor": (acknowledgment_actor or "operator").strip() or "operator",
                    "at": datetime.now(timezone.utc).isoformat(),
                    "reason": (
                        acknowledgment_reason
                        or "Operator acknowledged schema drift; existing mappings kept for this run"
                    ).strip(),
                }
                out["gates"] = [
                    *out["gates"],
                    {
                        "id": "schema_drift",
                        "status": "pass",
                        "message": (
                            f"Schema change acknowledged — policy '{policy}' keeps "
                            "existing mappings for this run (exception recorded)"
                        ),
                        "duration_ms": 0,
                        "details": {
                            "issues": list(drift.get("issues") or []),
                            "severity": evolution.get("severity") or "warning",
                            "schema_policy": policy,
                            "schema_evolution": evolution,
                            "acknowledged": True,
                            "acknowledgment": ack,
                            "rule_id": "schema_drift.acknowledged",
                        },
                    },
                ]
                out["schema_drift"] = {**drift, "acknowledged": True, "acknowledgment": ack}
            else:
                drift_msg = (
                    f"Schema change detected — policy '{policy}' requires review "
                    "before Execute (include new columns, ignore for this run, or "
                    "update the contract)"
                )
                drift_gate = {
                    "id": "schema_drift",
                    "status": "block",
                    "message": drift_msg,
                    "duration_ms": 0,
                    "details": {
                        "issues": list(drift.get("issues") or []),
                        "severity": evolution.get("severity") or "warning",
                        "schema_policy": policy,
                        "schema_evolution": evolution,
                        "rule_id": "schema_drift.manual_review",
                        "remediation_kind": "acknowledge_schema_drift",
                        "ack_required": True,
                    },
                }
                out["gates"] = [*out["gates"], drift_gate]
                drift_blocker = enrich_blockers(
                    [
                        {
                            "id": "schema_drift",
                            "message": drift_msg,
                            "details": drift_gate["details"],
                        }
                    ],
                    dest_kind=dest_kind,
                    validation_mode=validation_mode,
                )
                out["blockers"] = [*out["blockers"], *drift_blocker]
                out["passed"] = False
                out["passed_count"] = sum(
                    1 for g in out["gates"] if g.get("status") == "pass"
                )
                out["total_gates"] = len(out["gates"])
                out["readiness_score"] = round(
                    out["passed_count"] / max(out["total_gates"], 1) * 100, 1
                )
                out["schema_drift"] = drift
    elif drift.get("issues"):
        # Soft notes (extra destination columns, etc.) — pass with evidence, no Execute lock.
        note = str(drift["issues"][0])
        out["gates"] = [
            *out["gates"],
            {
                "id": "schema_drift",
                "status": "pass",
                "message": f"Schema notes (non-blocking): {note}",
                "duration_ms": 0,
                "details": {
                    "issues": list(drift.get("issues") or []),
                    "severity": drift.get("severity") or "none",
                    "schema_evolution": evolution,
                    "rule_id": "schema_drift.informational",
                },
            },
        ]
        out["schema_drift"] = drift
        out["passed_count"] = sum(1 for g in out["gates"] if g.get("status") == "pass")
        out["total_gates"] = len(out["gates"])
        out["readiness_score"] = round(
            out["passed_count"] / max(out["total_gates"], 1) * 100, 1
        )

    return out


def probe_destination(endpoint) -> tuple[bool, str]:
    """Live connectivity probe for database destinations (Gate G2).

    When a saved ``connector_id`` is set, use the exact same probe as
    Connectors → Test so Validate never invents different credentials.
    """
    if endpoint.kind != "database":
        return True, "Non-database destination"

    if getattr(endpoint, "connector_id", None):
        from services.connector_probe import probe_saved_connector

        ok, msg, _cfg = probe_saved_connector(endpoint.connector_id)
        return ok, msg

    from src.transfer.adapters import resolve_connector_config, resolve_dest_table
    from src.transfer.connector_registry import run_probe

    cfg = resolve_connector_config(endpoint)
    db_type = (cfg.get("type") or endpoint.format or "").lower()
    if db_type == "dynamodb":
        cfg["table"] = resolve_dest_table(db_type, endpoint)
    return run_probe(db_type, cfg)


def _available_staging_bytes(estimated_bytes: int) -> int:
    """Estimate writable staging capacity from local exports volume."""
    import shutil
    from pathlib import Path

    export_dir = Path(__file__).resolve().parents[2] / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    try:
        usage = shutil.disk_usage(export_dir)
        # Reserve 15% headroom; require at least 3× estimated transfer size
        usable = int(usage.free * 0.85)
        required = max(estimated_bytes * 3, 1_048_576)
        return max(usable, required) if usable >= required else usable
    except OSError:
        return max(estimated_bytes * 3, 8_388_608)


def inspect_destination_for_preflight(
    *,
    connector_id: str | None = None,
    dest_type: str | None = None,
    dest_host: str | None = None,
    dest_port: int | None = None,
    dest_database: str | None = None,
    dest_table: str | None = None,
    dest_collection: str | None = None,
    dest_schema: str | None = None,
    dest_username: str | None = None,
    dest_password: str | None = None,
    dest_connection_string: str | None = None,
    dest_warehouse: str | None = None,
    dest_auth_source: str | None = None,
    dest_auth_mode: str | None = None,
    dest_auth_role: str | None = None,
    dest_api_key: str | None = None,
    dest_service_account: str | None = None,
    dest_kind: str = "database",
) -> dict[str, Any]:
    """Introspect destination for table existence and column schema."""
    out: dict[str, Any] = {
        "connected": False,
        "table_exists": None,
        "can_create_table": False,
        "column_types": {},
        "column_nullability": {},
        "columns": [],
        "db_type": (dest_type or "").lower(),
        "message": "",
    }
    if dest_kind == "file_export":
        out["connected"] = True
        out["can_create_table"] = True
        out["message"] = "File export destination"
        return out

    from src.transfer.models import EndpointConfig

    if connector_id:
        # CRITICAL: Validate G2 must use the same decrypted secrets as Connectors Test.
        # Never rebuild an EndpointConfig from empty Studio form fields (password /
        # connection_string omitted when connector_id is set) — that path defaulted
        # host→localhost and produced "auth failed" while Test still passed.
        from services.connector_probe import (
            endpoint_from_saved_connector,
            probe_saved_connector,
        )

        ok, msg, cfg = probe_saved_connector(connector_id)
        db_type = (cfg.get("type") or dest_type or "").lower()
        out["db_type"] = db_type
        out["_saved_cfg"] = cfg
        if not ok:
            out["connected"] = False
            out["message"] = msg or "Destination unreachable"
            return out

        endpoint = endpoint_from_saved_connector(
            connector_id,
            table=dest_table or "",
            collection=dest_collection or dest_table or "",
            schema=dest_schema or "",
            database=dest_database or "",
        )
        if not endpoint:
            out["message"] = f"Connector '{connector_id}' not found"
            return out
        # Prefer operator-chosen auth_source override from Studio when present.
        if dest_auth_source:
            endpoint.auth_source = dest_auth_source
    elif dest_host or dest_connection_string:
        db_type = (dest_type or "mongodb").lower()
        out["db_type"] = db_type
        from services.dialect_profiles import normalize_schema

        endpoint = EndpointConfig(
            kind="database",
            format=db_type,
            host=dest_host or "localhost",
            port=int(dest_port or 0),
            database=dest_database or "",
            schema=normalize_schema(db_type, dest_schema, username=dest_username) or "",
            table=dest_table or "",
            collection=dest_collection or dest_table or "",
            username=dest_username or "",
            password=dest_password or "",
            connection_string=dest_connection_string or "",
            warehouse=dest_warehouse or "",
            auth_source=dest_auth_source or "",
            auth_mode=dest_auth_mode or "",
            auth_role=dest_auth_role or "",
            api_key=dest_api_key or "",
            service_account=dest_service_account or "",
        )
    else:
        out["message"] = "Destination not configured"
        return out

    # Same honesty contract as /transfer/introspect: never steal columns from
    # another DB/schema when Validate re-probes the destination.
    endpoint.extra = {**(endpoint.extra or {}), "introspect_purpose": "destination"}

    from src.transfer.endpoint_intelligence import introspect_endpoint

    info = introspect_endpoint(endpoint)
    # Connectivity already proven via probe_saved_connector when connector_id set;
    # trust that over a second introspect failure (schema-only hiccups).
    if connector_id and out.get("db_type"):
        out["connected"] = True
        if not info.get("connected"):
            # Schema introspect failed but ping passed — keep connected, surface note.
            out["message"] = info.get("message") or msg or "Connected"
        else:
            out["message"] = info.get("message") or msg or "Connected"
    else:
        out["connected"] = bool(info.get("connected"))
        out["message"] = info.get("message", "")
    schema = info.get("schema") or {}
    cols = info.get("columns") or list(schema.keys())
    out["columns"] = cols
    out["column_types"] = schema
    out["column_nullability"] = {
        str(k): bool(v)
        for k, v in dict(info.get("schema_nullability") or {}).items()
    }
    # Live UNIQUE / PK catalog — feeds identity uniqueness + append PK enforce.
    out["primary_key_columns"] = list(info.get("primary_key_columns") or [])
    out["unique_keys"] = list(info.get("unique_keys") or [])
    out["pk_columns"] = list(out["primary_key_columns"])
    # Advisory-key / introspect honesty notes (BQ NOT ENFORCED, Redshift
    # informational, Snowflake NOT ENFORCED) — warn-only, never invent blockers.
    dest_warnings = [str(w) for w in (info.get("warnings") or []) if w]
    if dest_warnings:
        out["warnings"] = dest_warnings
        out["schema_warnings"] = dest_warnings
    stream = dest_collection or dest_table or endpoint.collection or endpoint.table
    # Prefer introspect's explicit existence (True / False / None). Recomputing
    # with exact string match broke public.jobs vs jobs and wiped create-new.
    if "table_exists" in info:
        out["table_exists"] = info.get("table_exists")
    elif stream and cols:
        out["table_exists"] = True
    elif stream and info.get("objects"):
        from src.transfer.endpoint_intelligence import _object_name_match

        names = [
            str(o.get("name") or "")
            for o in (info.get("objects") or [])
            if isinstance(o, dict)
        ]
        matched = _object_name_match(names, str(stream))
        out["table_exists"] = bool(matched)
    out["can_create_table"] = out["connected"]
    out["can_write"] = out["connected"]

    # Enterprise G2: measure write/create via privilege metadata (never CREATE/INSERT probe).
    if out["connected"]:
        try:
            from services.destination_privilege_probe import (
                probe_destination_privileges,
                resolve_write_flags,
            )

            cfg: dict[str, Any] = {}
            if out.get("_saved_cfg"):
                cfg = dict(out.pop("_saved_cfg") or {})
            elif connector_id:
                from services.connector_probe import probe_saved_connector

                _ok, _msg, cfg = probe_saved_connector(connector_id)
            else:
                cfg = {
                    "host": getattr(endpoint, "host", "") or "",
                    "port": int(getattr(endpoint, "port", 0) or 0),
                    "database": getattr(endpoint, "database", "") or "",
                    "username": getattr(endpoint, "username", "") or "",
                    "password": getattr(endpoint, "password", "") or "",
                    "connection_string": getattr(endpoint, "connection_string", "")
                    or "",
                    "schema": getattr(endpoint, "schema", "") or "",
                    "type": out.get("db_type") or "",
                    "warehouse": getattr(endpoint, "warehouse", "")
                    or dest_warehouse
                    or "",
                    "role": getattr(endpoint, "auth_role", "") or dest_auth_role or "",
                    "service_account": getattr(endpoint, "service_account", "")
                    or dest_service_account
                    or "",
                    "ssl": bool(getattr(endpoint, "ssl", False)),
                }

            probe_schema = str(
                dest_schema
                or cfg.get("schema")
                or cfg.get("dataset")
                or getattr(endpoint, "schema", "")
                or ""
            )
            probe = probe_destination_privileges(
                out.get("db_type") or cfg.get("type") or "",
                host=str(cfg.get("host") or ""),
                port=int(cfg.get("port") or 0),
                database=str(cfg.get("database") or cfg.get("project_id") or ""),
                schema=probe_schema,
                table=str(
                    dest_table
                    or dest_collection
                    or getattr(endpoint, "table", "")
                    or getattr(endpoint, "collection", "")
                    or ""
                ),
                username=str(cfg.get("username") or ""),
                password=str(cfg.get("password") or ""),
                connection_string=str(cfg.get("connection_string") or ""),
                table_exists=(
                    out.get("table_exists")
                    if isinstance(out.get("table_exists"), bool)
                    else None
                ),
                ssl=bool(cfg.get("ssl") or False),
                warehouse=str(
                    cfg.get("warehouse")
                    or dest_warehouse
                    or getattr(endpoint, "warehouse", "")
                    or ""
                ),
                role=str(
                    cfg.get("role")
                    or cfg.get("auth_role")
                    or dest_auth_role
                    or getattr(endpoint, "auth_role", "")
                    or ""
                ),
                account=str(cfg.get("account") or cfg.get("host") or ""),
                project_id=str(cfg.get("project_id") or cfg.get("database") or ""),
                dataset=str(cfg.get("dataset") or probe_schema),
                service_account=str(
                    cfg.get("service_account")
                    or dest_service_account
                    or getattr(endpoint, "service_account", "")
                    or ""
                ),
                location=str(cfg.get("location") or ""),
                auth_source=str(
                    cfg.get("auth_source")
                    or dest_auth_source
                    or getattr(endpoint, "auth_source", "")
                    or ""
                ),
                api_key=str(
                    cfg.get("api_key") or getattr(endpoint, "api_key", "") or ""
                ),
            )
            can_write, can_create, priv_meta = resolve_write_flags(True, probe)
            out["can_write"] = can_write
            out["can_create_table"] = can_create
            out["privilege_probe"] = priv_meta
            if probe.status == "denied" and probe.detail:
                # Surface explicit deny in message without wiping connectivity success.
                out["message"] = probe.detail
            elif probe.status == "unavailable" and probe.detail:
                out["privilege_probe_warning"] = probe.detail
        except Exception as exc:  # noqa: BLE001
            out["privilege_probe"] = {
                "status": "unavailable",
                "detail": str(exc),
                "can_write": None,
                "can_create_table": None,
            }
    # Persist auto-resolved Mongo authSource so Validate/Execute match Connectors Test.
    resolved_auth = (getattr(endpoint, "auth_source", "") or "").strip()
    if (
        out["connected"]
        and resolved_auth
        and (out.get("db_type") or "").lower() == "mongodb"
    ):
        out["auth_source"] = resolved_auth
        if connector_id:
            try:
                from services.connector_store import get_connector, update_connector

                conn = get_connector(connector_id)
                if conn and (conn.auth_source or "") != resolved_auth:
                    update_connector(connector_id, {"auth_source": resolved_auth})
            except Exception as exc:
                logger.debug(
                    "mongodb auth_source persistence failed: %s", exc, exc_info=exc
                )
    return out
