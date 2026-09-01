"""Preflight validation for DataTransfer transfers."""

from __future__ import annotations

import logging
import sys
from collections.abc import Callable, Iterable, Mapping
from contextlib import nullcontext
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
from services.data_profiler import source_types_are_authoritative
from services.db_type_utils import dest_declares_column_ddl, normalize_dest_kind
from services.destination_key_collision_probe import probe_append_key_collisions
from services.preflight_fk_gate import build_fk_block
from services.coercion_gate_reconcile import reconcile_coercion_report
from services.preflight_source_catalog import load_source_foreign_keys
from services.destination_requirements_gate import build_mapping_contract_gates
from services.preflight_cursor_gate import (
    build_sync_contract_gate,
    resolve_read_scope as _resolve_read_scope,
)
from services.source_duplicate_probe import probe_source_duplicate_keys_result
from services.transform_engine import (
    ambiguous_date_columns,
    ambiguous_number_columns,
    assumed_number_locale,
    infer_date_locale,
    infer_number_locale,
    reset_active_date_locale,
    reset_active_number_locale,
    set_active_date_locale,
    set_active_number_locale,
)
from services.validation_plan import build_validation_plan
from services.value_serializer import cell_to_string, project_row_cells


def _hydrate_risk_contract(
    m: dict[str, Any],
    *,
    table: str = "",
    migration_id: str = "",
) -> dict[str, Any] | None:
    """Verify or sign a Migration Risk Contract draft — SSOT in migration_risk_contract."""
    from services.migration_risk_contract import hydrate_risk_contract_dict

    return hydrate_risk_contract_dict(m, table=table, migration_id=migration_id)


def _with_date_locale(fn):
    """Set date_locale and number_locale for the duration of the call."""

    def wrapper(*args, **kwargs):
        date_token = set_active_date_locale(kwargs.get("date_locale", ""))
        number_token = set_active_number_locale(kwargs.get("number_locale", ""))
        try:
            return fn(*args, **kwargs)
        finally:
            reset_active_number_locale(number_token)
            reset_active_date_locale(date_token)

    return wrapper


class FilePreflightContext(PreflightContext):
    """Preflight context for file → database transfers."""

    def __init__(
        self,
        plan: TransferPlan,
        sample_rows: list[dict] | None = None,
        destination_collision: Any = None,
        source_duplicate_findings: list[dict[str, Any]] | None = None,
        source_duplicate_probe_ran: bool = False,
        source_duplicate_probe_pk: str = "",
        source_duplicate_probe_status: str = "",
        source_duplicate_probe_message: str = "",
        source_duplicate_probe_expected: bool = False,
    ):
        super().__init__(plan=plan, sample_rows=sample_rows or [])
        self.destination_collision = destination_collision
        self.source_duplicate_findings = source_duplicate_findings or []
        self.source_duplicate_probe_ran = bool(source_duplicate_probe_ran)
        self.source_duplicate_probe_pk = str(source_duplicate_probe_pk or "")
        self.source_duplicate_probe_status = str(source_duplicate_probe_status or "")
        self.source_duplicate_probe_message = str(source_duplicate_probe_message or "")
        self.source_duplicate_probe_expected = bool(source_duplicate_probe_expected)

    def _mapping_dict_for_probe(self, m: Any, dest_types: dict[str, str]) -> dict[str, Any]:
        """Serialize plan mappings for coercion / integrity — keep Map Accept risk.

        Stripping risk_acknowledged / fidelity left Validate blocked after Map
        Accept risk (G3 probe severity + G9 coercion_safety).
        """
        live = dest_types.get(m.target)
        stamped = live or getattr(m, "target_type", None)
        src_type = next(
            (
                c.inferred_type
                for c in self.plan.source.columns
                if c.name == m.source
            ),
            None,
        )
        if (
            not live
            and stamped
            and src_type
            and not bool(getattr(m, "user_override", False))
        ):
            try:
                from services.decision_kernel import refuse_create_new_numeric_collapse

                dest_db = str(getattr(self.plan.destination, "db_type", "") or "")
                stamped = refuse_create_new_numeric_collapse(
                    str(src_type), str(stamped), dest_db
                )
            except Exception:
                pass
        return {
            "source": m.source,
            "target": m.target,
            "confidence": getattr(m, "confidence", 0.0),
            "transform": getattr(m, "transform", None),
            "requires_review": bool(getattr(m, "requires_review", False)),
            "user_override": bool(getattr(m, "user_override", False)),
            # Prefer live dest DDL; refuse sample-invented create-new collapse.
            "target_type": stamped,
            "source_type": src_type,
            "create_new": bool(getattr(m, "create_new", False)),
            "struct_policy": getattr(m, "struct_policy", None) or None,
            "struct_derived": bool(getattr(m, "struct_derived", False)),
            "struct_parent": getattr(m, "struct_parent", None) or None,
            "structural_class": getattr(m, "structural_class", None) or None,
            "child_table_spec": getattr(m, "child_table_spec", None) or None,
            "fidelity": getattr(m, "fidelity", None) or None,
            "type_narrowing": bool(getattr(m, "type_narrowing", False)),
            "risk_acknowledged": bool(getattr(m, "risk_acknowledged", False)),
            "intentional_omit": bool(getattr(m, "intentional_omit", False)),
            "risk_contract": getattr(m, "risk_contract", None),
            "code_crosswalk": getattr(m, "code_crosswalk", None),
            "code_crosswalk_system": getattr(m, "code_crosswalk_system", None),
        }

    def run_dry_run(self, sample_size: int = 1000) -> tuple[bool, list[str]]:
        if not self.sample_rows:
            return False, [
                (
                    "No sample rows available for dry-run validation. "
                    "Re-run Source introspect so Datawrap can load a preview sample "
                    "from the source table (column metadata alone is not enough)."
                )
            ]

        headers = list(self.sample_rows[0].keys()) if self.sample_rows else []
        # Nested lists/dicts from schemaless sources become valid JSON strings
        # instead of Python repr() artifacts; a key the document does not carry
        # stays the missing sentinel rather than an empty string the typed
        # transforms would reject as a cast failure.
        scanned = self.sample_rows[:sample_size]
        rows = [project_row_cells(row, headers) for row in scanned]
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
            self._mapping_dict_for_probe(m, dest_types_by_name)
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
                self._mapping_dict_for_probe(m, dest_types)
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
            if isinstance(report, dict):
                from services.validation_coverage import stamp_validation_coverage

                report = {
                    **report,
                    "validation_coverage": stamp_validation_coverage(
                        layer="datatype",
                        rows_examined=len(self.sample_rows or []),
                        note=(
                            "Datatype / coercion classification only — "
                            "not sample population proof."
                        ),
                    ),
                }
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
        dest_types = {
            c.name: c.inferred_type for c in self.plan.destination.target_columns
        }
        mapping_dicts = [
            self._mapping_dict_for_probe(m, dest_types)
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
        report = audit(
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
            source_duplicate_probe_status=self.source_duplicate_probe_status,
            source_duplicate_probe_message=self.source_duplicate_probe_message,
            source_duplicate_probe_expected=self.source_duplicate_probe_expected,
            dest_table_exists=getattr(self.plan.destination, "table_exists", None),
        )
        # Normalize/hybrid without a valid child_table_spec — fail closed in G9.
        try:
            from services.structural_array import array_strategy_gate_issues

            array_issues = array_strategy_gate_issues(
                mapping_dicts,
                known_source_columns=set(source_columns),
            )
        except Exception:
            array_issues = []
        if array_issues:
            issues = list(report.get("issues") or [])
            issues.extend(array_issues)
            report = {
                **report,
                "issues": issues,
                "blocks_transfer": True,
            }
        return report


VALIDATION_CONFIDENCE_THRESHOLDS = {
    "balanced": 0.75,
    "migration": 0.75,
    "strict": 0.85,
    "audit": 0.85,
    "maximum": 0.95,
    "discovery": 0.0,
}


def apply_readiness_honesty_caps(out: dict[str, Any]) -> dict[str, Any]:
    """Cap Validate readiness when G9 uniqueness is sample-only.

    Gate pass-ratio can be 100% while population uniqueness is unproven.
    Fivetran/Airbyte do not call that certified-ready. Execute may still
    proceed; the score must not look like a migration certificate.
    """
    payload = out if isinstance(out, dict) else {}
    reasons: list[str] = []
    for gate in payload.get("gates") or []:
        if not isinstance(gate, dict):
            continue
        gid = str(gate.get("id") or "").lower()
        details = gate.get("details") if isinstance(gate.get("details"), dict) else {}
        coverage = str(details.get("coverage") or "").lower()
        msg = str(gate.get("message") or "").lower()
        if "g9" in gid or "data_integrity" in gid:
            if (
                coverage == "sample"
                or "population uniqueness not proven" in msg
                or "validate sample only" in msg
            ):
                reasons.append("g9_sample_uniqueness")
        if "g5" in gid or "dry_run" in gid:
            if (
                coverage == "sample"
                or details.get("sample_cap") is not None
                or "sample" in msg
                or gid == "g5_dry_run"
            ):
                reasons.append("g5_sample_dry_run")
    if reasons:
        try:
            score = float(payload.get("readiness_score") or 0)
        except (TypeError, ValueError):
            score = 0.0
        payload["readiness_score"] = min(score, 92.0)
        if "g9_sample_uniqueness" in reasons:
            payload["population_uniqueness_proven"] = False
        payload["readiness_cap_reason"] = "+".join(dict.fromkeys(reasons))
    return payload


def confidence_threshold_for_mode(validation_mode: str | None) -> float:
    try:
        from services.validation_mode_contract import confidence_floor_for_mode

        return confidence_floor_for_mode(validation_mode)
    except Exception:
        return VALIDATION_CONFIDENCE_THRESHOLDS.get(
            (validation_mode or "strict").lower(), 0.85
        )


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
    delivery_guarantee: str = "at_least_once",
    allow_append_only: bool = False,
    read_scope: Any = None,
    priority_column: str = "",
    priority_direction: str = "desc",
    row_limit: int = 0,
) -> list[dict[str, Any]]:
    """Validate enterprise run policy that sits above source/destination probes."""
    from services.schema_drift import schema_policy_honesty_line

    contracts = [c for c in stream_contracts or [] if c.get("selected", True)]
    sync = (sync_mode or "full_refresh_overwrite").lower()
    schema = (schema_policy or "manual_review").lower()
    validation = (validation_mode or "strict").lower()
    dest = (dest_type or "").strip().lower()
    src = (source_type or "").strip().lower()
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
        )
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
                    "honesty_line": schema_policy_honesty_line(schema),
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


    from services.cdc_exactly_once import preflight_delivery_gate, route_has_cdc_pk

    eos_gate = preflight_delivery_gate(
        sync_mode=sync,
        dest_type=dest,
        source_type=src,
        delivery_guarantee=delivery_guarantee,
        has_primary_key=route_has_cdc_pk(contracts),
        allow_append_only=allow_append_only,
        callable_source=(source_read_mode or "").strip().lower() in {"procedure", "query"},
    )
    if eos_gate:
        gates.append(eos_gate)

    from services.cdc_snapshot_mode import build_snapshot_mode_preflight_gate

    snap_wm = getattr(read_scope, "watermark", None) if read_scope is not None else None
    snap_gate = build_snapshot_mode_preflight_gate(
        sync_mode=sync,
        stream_contracts=contracts,
        watermark=snap_wm,
    )
    if snap_gate:
        gates.append(snap_gate)

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
                    f"Execute writes"
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

    # Redis KV TTL/EXPIRE is not a first-class transfer guarantee (soft warning).
    if dest in {"redis", "redis_enterprise", "amazon_elasticache_redis", "azure_cache_redis", "google_memorystore_redis"} or src in {
        "redis", "redis_enterprise", "amazon_elasticache_redis", "azure_cache_redis", "google_memorystore_redis",
    }:
        gates.append({
            "id": "redis_ttl_semantics",
            "name": "Redis TTL / EXPIRE",
            "status": GateStatus.PASS.value,
            "severity": "warn",
            "message": (
                "Redis TTL/EXPIRE is not preserved as a migration guarantee — "
                "values transfer; set EXPIRE in a post-load job if needed. "
                "See docs/REDIS_TTL_SEMANTICS.md."
            ),
            "blocks_transfer": False,
            "details": {"honesty": "ttl_not_productized"},
        })

    return gates


def _iter_table_population_for_preflight(
    *,
    source_kind: str,
    source_format: str,
    source_connector_id: str,
    source_config: dict[str, Any] | None,
    source_table: str,
    mappings: list[dict[str, Any]],
    column_types: dict[str, str],
    dest_types: dict[str, str],
    dest_db: str,
    sync_mode: str = "",
    read_scope: Any = None,
    source_filter: dict[str, Any] | None = None,
):
    """Same projected table walk Execute uses — or None when it cannot run.

    Studio/plan/Pilot used to scan only the preview. A late DECIMAL overflow
    then passed Validate and blocked Execute. Callable extracts, row filters,
    and file sources stay out: those populations are not this walk.
    """
    kind = (source_kind or "").strip().lower()
    if kind not in {"database", "cloud"}:
        return None
    table = str(source_table or "").strip()
    if not table:
        return None
    if not (source_connector_id or source_config):
        return None
    from services.procedure_source import is_callable_source

    if is_callable_source(source_config):
        return None
    cfg = dict(source_config or {})
    extra = cfg.get("extra") if isinstance(cfg.get("extra"), dict) else {}
    from services.sync_cursor import normalize_sync_mode

    if normalize_sync_mode(sync_mode, default="") == "cdc":
        # CDC writes a changelog, not a table rescan. Walking the table
        # judges the wrong population — preview stays, never exact.
        return None
    try:
        from src.transfer.models import EndpointConfig
        from src.transfer.source_peek import iter_bounded_table_population_rows
    except ImportError:  # pragma: no cover - api root on PYTHONPATH
        from transfer.models import EndpointConfig
        from transfer.source_peek import iter_bounded_table_population_rows

    data = dict(cfg)
    if source_connector_id:
        data["connector_id"] = source_connector_id
    data.setdefault("table", table)
    data.setdefault("collection", table)
    if source_format:
        data.setdefault("format", source_format)
        data.setdefault("type", source_format)
    endpoint = EndpointConfig.from_dict(
        kind if kind in {"database", "cloud"} else "database",
        data,
    )
    return iter_bounded_table_population_rows(
        endpoint,
        mappings,
        column_types=column_types,
        dest_types=dest_types,
        dest_db=dest_db,
        source_kind=source_kind,
        source_format=source_format or str(data.get("format") or ""),
        read_scope=read_scope,
        source_filter=source_filter
        or (cfg.get("source_filter") if isinstance(cfg.get("source_filter"), dict) else None)
        or (extra.get("source_filter") if isinstance(extra.get("source_filter"), dict) else None),
    )


def _fit_scan_deadline(
    absolute: float | None,
    seconds: float | None,
) -> float | None:
    """Studio budget is scan wall-clock, not time spent on sample gates first."""
    if seconds is not None and float(seconds) > 0:
        import time

        return time.monotonic() + float(seconds)
    return absolute


# F8: policy-gate merge lives in preflight_policy_gates (single authority).
from services.preflight_policy_gates import (  # noqa: E402
    apply_policy_gates,
    is_compliance_only_block,
)


@_with_date_locale
def run_file_preflight(
    *,
    columns: list[str],
    column_types: dict[str, str],
    row_count: int | None,
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
    #: Live destination DDL on a run that recreates the table. Callers that
    #: already clear ``destination_column_types`` for an overwrite (Execute
    #: does, so no typing decision reads a doomed carrier) pass the declared
    #: types here for G19, which is the one gate that must see them.
    destination_live_column_types: dict[str, str] | None = None,
    destination_column_nullability: dict[str, bool] | None = None,
    destination_column_defaults: dict[str, str] | None = None,
    destination_identity_columns: list[str] | None = None,
    destination_generated_columns: list[str] | None = None,
    destination_table_exists: bool | None = None,
    destination_can_create: bool | None = None,
    destination_can_write: bool | None = None,
    privilege_probe: dict[str, Any] | None = None,
    redshift_staging_probe: dict[str, Any] | None = None,
    available_staging_bytes: int | None = None,
    destination_db_type: str = "postgresql",
    source_connector_id: str = "",
    source_config: dict[str, Any] | None = None,
    source_table: str = "",
    destination_table: str = "",
    source_filename: str = "",
    source_file_id: str = "",
    schema_policy: str = "manual_review",
    backfill_new_fields: bool = False,
    stored_source_fp: str = "",
    stored_target_fp: str = "",
    declared_source_columns: list[str] | None = None,
    declared_source_schema: dict[str, str] | None = None,
    previous_source_columns: list[str] | None = None,
    previous_source_schema: dict[str, str] | None = None,
    contract_primary_key: str | None = None,
    destination_pk_columns: list[str] | None = None,
    destination_unique_keys: list[dict[str, Any]] | None = None,
    destination_foreign_keys: list[dict[str, Any]] | None = None,
    destination_config: Mapping[str, Any] | None = None,
    stream_contracts: list[dict[str, Any]] | None = None,
    date_locale: str = "",
    number_locale: str = "",
    cursor_fields: list[str] | None = None,
    compliance_acknowledged: bool = False,
    schema_drift_acknowledged: bool = False,
    fk_risk_acknowledged: bool = False,
    acknowledgment_actor: str = "",
    acknowledgment_reason: str = "",
    run_population_orphan_scan: bool = False,
    resume: bool = False,
    population_rows: Iterable[Mapping[str, Any]] | None = None,
    rows_are_population: bool = False,
    shape_recipe: Mapping[str, Any] | None = None,
    source_filter: Mapping[str, Any] | None = None,
    fit_scan_deadline: float | None = None,
    fit_scan_seconds: float | None = None,
    on_fit_progress: Any = None,
    skip_population_fit: bool = False,
) -> dict[str, Any]:
    """Run preflight gates for file/DB Studio transfers (G1–G9 + host policy)."""
    from services.timezone_policy import declared_source_column_types

    # A zone declared on Map is a fact about the source, so every gate below
    # reads the declared type rather than the zoneless one it was introspected
    # with. Done once here because this is the single entry every caller
    # (Studio, Pilot, scheduled runs) goes through.
    column_types = declared_source_column_types(column_types, mappings)

    # Canonical sync vocabulary — G9/DDL/policy must match writers (insert→append).
    try:
        from services.sync_cursor import normalize_sync_mode

        sync_mode = normalize_sync_mode(sync_mode)
    except Exception:
        sync_mode = (sync_mode or "").strip().lower() or "full_refresh_append"

    # An overwrite drops the destination and recreates it from the source shape,
    # so the table standing there now is not the carrier the rows land in. Every
    # type verdict below reads the DDL this run will create, or a route whose own
    # CREATE declares ``LONGTEXT`` is refused for a ``TEXT → VARCHAR(64)``
    # collapse against a table it is about to drop. Append/upsert/CDC/mirror keep
    # the live contract — there the existing column really is authoritative.
    from services.sync_cursor import is_overwrite_sync

    dest_recreated = is_overwrite_sync(sync_mode)

    # Sources with no cheap cardinality — a DynamoDB Scan, a Kafka topic, a
    # search index — report ``None`` rather than inventing a total, which is the
    # honest answer and used to crash this comparison before a single gate ran.
    # Unknown and zero are handled the same way here (size from the sample);
    # the distinction stays visible downstream, where an unmeasured source count
    # is already reported as such rather than as a measured zero.
    row_count_known = isinstance(row_count, int) and row_count > 0
    if not row_count_known:
        row_count = len(sample_rows or [])

    # Preflight is a sample-based safety check, not a full table scan.  Cap the
    # sample size so very large file previews or database samples cannot make the
    # validate step hang. Keep in sync with coercion_probe.DEFAULT_SAMPLE_LIMIT.
    from services.coercion_probe import PREFLIGHT_SAMPLE_LIMIT

    if sample_rows and len(sample_rows) > PREFLIGHT_SAMPLE_LIMIT:
        sample_rows = sample_rows[:PREFLIGHT_SAMPLE_LIMIT]

    # Sign Migration Risk Contract drafts before gates / proof_bundle see them.
    # Stamp destination table onto unsigned drafts (audit field — never invent DDL).
    hydrated_mappings: list[dict[str, Any]] = []
    for m in mappings or []:
        row = dict(m) if isinstance(m, dict) else dict(m or {})
        signed = _hydrate_risk_contract(
            row,
            table=str(destination_table or ""),
            migration_id="",
        )
        if signed is not None:
            row["risk_contract"] = signed
            # Continue-policy contract implies operator ack for G3/G4.
            from services.migration_risk_contract import contract_clears_validate_block

            if contract_clears_validate_block(signed):
                row["risk_acknowledged"] = True
        hydrated_mappings.append(row)
    mappings = hydrated_mappings
    _unstamped_additive: list[str] = []

    # If the operator did not specify a locale for ambiguous day/month dates,
    # scan the sample for an unambiguous majority before any date coercion.
    if sample_rows and columns:
        inferred_locale = infer_date_locale(
            sample_rows, columns, existing_locale=date_locale
        )
        if inferred_locale and not date_locale:
            date_locale = inferred_locale
            set_active_date_locale(date_locale)
        inferred_numbers = infer_number_locale(
            sample_rows, columns, existing_locale=number_locale
        )
        if inferred_numbers and not number_locale:
            number_locale = inferred_numbers
            set_active_number_locale(number_locale)

    if not number_locale:
        # Same contract Execute runs under: values a typed source rendered are
        # canonical, so Validate must not refuse NUMERIC(12,3) 10.129 as
        # US/EU-ambiguous and then let (or block) Run under a different rule.
        from src.transfer.connector_capabilities import typed_wire_number_locale

        wire = typed_wire_number_locale(source_kind, source_format)
        if wire:
            number_locale = wire
            set_active_number_locale(number_locale)

    number_locale_assumed: list[dict[str, Any]] = []
    if not number_locale and sample_rows and columns:
        # Nothing declared it, no typed carrier rendered it, and the sample
        # carries no grouping evidence. Assume US rather than refuse the load,
        # and hand the operator the columns it covers so switching to EU is a
        # decision they can see, not a corruption they find later.
        assumed = assumed_number_locale(sample_rows, columns, number_locale=number_locale)
        if assumed:
            number_locale_assumed = ambiguous_number_columns(
                sample_rows, columns, number_locale=number_locale
            )
            number_locale = assumed
            set_active_number_locale(number_locale)

    # If the caller did not supply rich source types, infer them from the sample
    # rows. This keeps schemaless sources (MongoDB, DynamoDB, Redis, S3 JSON) from
    # being treated as all-VARCHAR against a typed warehouse target.
    # A relational source that genuinely declares TEXT for every column is not
    # a schemaless source: re-inferring it turned a SQLite ``id TEXT`` holding
    # "0".."49" into INTEGER here while Execute kept TEXT, so Validate approved a
    # DDL Execute refused to materialize — and had they agreed, leading zeros
    # would have been dropped on the destination.
    # Profiling is per column through ``merge_profiler_schema`` — the same owner
    # and the same precedence the Map step uses. An all-or-nothing gate here
    # ("only infer when *every* declared type is generic") made the two steps
    # read one source differently: DynamoDB declares ``amount`` as ``N`` but
    # ``id`` as ``S``, so Map profiled ``id`` to INTEGER and created BIGINT while
    # preflight kept VARCHAR and then blocked the run as an ``id VARCHAR →
    # BIGINT`` collapse of a carrier the product itself had just invented.
    if (
        sample_rows
        and columns
        and not source_types_are_authoritative(source_kind, source_format)
    ):
        try:
            from services.data_profiler import merge_profiler_schema, profile_dataset
            from services.type_system import destination_carriers_are_inferred

            profiled = profile_dataset(columns, sample_rows).get("schema") or {}
            if profiled:
                column_types = merge_profiler_schema(
                    dict(column_types or {}),
                    {c: profiled[c] for c in columns if c in profiled},
                    authoritative_existing=False,
                    # A keyspace / document store declares no numeric width, so
                    # this page's DECIMAL(p,s) is a description of the sample and
                    # not a bound Execute may enforce on the population.
                    source_carriers_sampled=destination_carriers_are_inferred(
                        source_format or source_kind
                    ),
                )
        except Exception as exc:
            logger.debug("preflight schema inference failed: %s", exc, exc_info=exc)

    # Additive / create-new under backfill: Decision Kernel stamps target_type
    # after source types are known — same invent path writers honor (never bare
    # VARCHAR invent at Execute after Validate green). Use the same effective
    # backfill authority as Execute (schema policy + create_new maps).
    effective_backfill = bool(backfill_new_fields)
    try:
        from services.batch_progress import effective_backfill_new_fields

        effective_backfill = bool(
            effective_backfill_new_fields(
                backfill_new_fields=bool(backfill_new_fields),
                schema_policy=schema_policy,
                mappings=mappings,
            )
        )
    except Exception:
        effective_backfill = bool(backfill_new_fields)
    try:
        from services.decision_kernel import stamp_additive_mapping_types

        samples_by_src: dict[str, list] = {}
        for row in sample_rows or []:
            if not isinstance(row, dict):
                continue
            for k, v in row.items():
                if v is None:
                    continue
                samples_by_src.setdefault(str(k), []).append(v)
        for k in list(samples_by_src.keys()):
            samples_by_src[k] = samples_by_src[k][:32]
        # Validate must invent under the same source-engine identity Execute
        # binds: the engine id decides code-page-safe carriers and whether a
        # declared numeric domain may be sample-narrowed. Unbound here, Map
        # stamped a carrier Execute would not, and the two DDL identities
        # diverged before a single row moved.
        from services.source_engine_scope import (
            active_source_engine,
            bind_source_engine,
        )

        src_engine = (source_format or "").strip().lower() or (
            (source_kind or "").strip().lower()
            if (source_kind or "").strip().lower() not in {"database", "cloud", ""}
            else ""
        )
        stamp_scope = (
            bind_source_engine(src_engine)
            if src_engine and not active_source_engine()
            else nullcontext()
        )
        with stamp_scope:
            mappings, _unstamped_additive = stamp_additive_mapping_types(
                mappings,
                dest_db=destination_db_type or "",
                live_dest_types=destination_column_types or {},
                source_types=column_types or {},
                samples_by_source=samples_by_src,
                backfill_new_fields=bool(effective_backfill),
                dest_table_exists=destination_table_exists,
            )
    except Exception as stamp_exc:
        # Fail-closed: stamp failure must surface as unstamped additives, not
        # clear the list and let Validate green-wash Map VARCHAR invent.
        logger.warning(
            "additive Map stamp failed (fail-closed): %s", stamp_exc, exc_info=stamp_exc
        )
        _unstamped_additive = [
            str(m.get("target") or m.get("source") or "?")
            for m in (mappings or [])
            if isinstance(m, dict)
            and (
                bool(m.get("create_new"))
                or str(m.get("assignment_strategy") or "")
                in {"create_compatible_new", "identity_passthrough"}
                or effective_backfill
            )
            and not str(m.get("target_type") or m.get("dest_type") or "").strip()
        ]

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

    # A sink with no column DDL (document/KV store, object-store prefix) reports
    # types profiled from sampled values. Judging the source against them is
    # judging it against Dataflow's own sample, so the Map stamp is the contract.
    dest_declares_ddl = dest_declares_column_ddl(destination_db_type)
    dest_types = (destination_column_types or {}) if dest_declares_ddl else {}
    dest_nulls = destination_column_nullability or {}
    write_maps = write_mappings(mappings)
    dest_cols = []
    for m in write_maps:
        tgt = m.get("target") or ""
        if not tgt:
            continue
        live = dest_types.get(tgt)
        if live is None:
            live = next(
                (
                    dest_types[k]
                    for k in dest_types
                    if str(k).lower() == str(tgt).lower()
                ),
                None,
            )
        if dest_recreated:
            live = None
        # Existing table: only live introspect counts as dest_types for Validate.
        # Map target_type fallback greens empties as VARCHAR while write binds
        # physical DATE/INT — refuse that false-green invent.
        if destination_table_exists is True and dest_declares_ddl and not dest_recreated:
            if not live:
                continue
            inferred = str(live).upper()
        else:
            inferred = str(
                live
                or m.get("target_type")
                or column_types.get(m["source"], "VARCHAR")
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
            structural_class=m.get("structural_class") or m.get("structuralClass"),
            child_table_spec=(
                m.get("child_table_spec")
                if isinstance(m.get("child_table_spec"), dict)
                else m.get("childTableSpec")
                if isinstance(m.get("childTableSpec"), dict)
                else None
            ),
            fidelity=(m.get("fidelity") or None),
            type_narrowing=bool(m.get("type_narrowing") or m.get("typeNarrowing", False)),
            risk_acknowledged=bool(
                m.get("risk_acknowledged") or m.get("riskAcknowledged", False)
            ),
            intentional_omit=is_intentional_omit(m),
            risk_contract=_hydrate_risk_contract(
                m if isinstance(m, dict) else dict(m or {}),
                table=str(destination_table or ""),
            ),
            assignment_strategy=str(m.get("assignment_strategy") or "") or None,
            review_kind=str(m.get("review_kind") or "") or None,
        )
        for m in mappings
    ]

    has_samples = bool(sample_rows)
    est_bytes = estimated_bytes if estimated_bytes > 0 else max(row_count * 128, 0)
    is_file_source = source_kind == "file"

    if available_staging_bytes is None:
        available_staging_bytes = _available_staging_bytes(est_bytes)

    # Unknown privilege must not invent create-new (Pilot often omits the flag).
    dest_can_create = (
        bool(destination_can_create) if destination_can_create is not None else False
    )
    dest_can_write = (
        bool(destination_can_write)
        if destination_can_write is not None
        else bool(destination_connected)
    )
    # Keep tri-state: None = probe unknown. Never coerce unknown → create-new.
    dest_table_exists = destination_table_exists

    from services.ddl_compatibility import evaluate_ddl_compatibility
    from services.schema_drift import detect_schema_drift

    dest_kind = normalize_dest_kind(destination_db_type, default="postgresql")
    # Document/KV stores and object-store prefixes alike declare no column
    # types: what a probe returns for them was profiled from sampled values, so
    # binding it as the live DDL contract makes the gates judge the source
    # against Dataflow's own sample (``DECIMAL(12,2)`` → ``DECIMAL(2,2)``).
    schemaless = not dest_declares_column_ddl(dest_kind)

    # Split drift vs DDL contracts:
    # - create-new / no dest DDL: no live dest types (stale Studio maps are noise)
    # - unknown existence: keep type hints for G6 lossy/width checks, but drift
    #   still treats the dest as non-live (no fingerprint / orphan locks)
    # - existing table: full live contract
    # - overwrite: the table is dropped and recreated from the source shape, so
    #   the carrier standing there now is not the one the rows land in. Judging
    #   against it refused a run for loss that cannot happen (``TEXT →
    #   VARCHAR(64)`` on a route whose own DDL creates ``LONGTEXT``).
    hinted_dest_types = dict(destination_column_types or {})
    if schemaless or dest_table_exists is False or dest_recreated:
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

    # Two questions, two truths, and an approved pre-load transform separates
    # them. "Did the source change under the stored revision?" is answered by the
    # declared source the revision fingerprinted — otherwise the operator's own
    # recipe reads as source drift. "Do these values fit the destination
    # carrier?" is answered by the transformed image the writer will bind, or a
    # column rounded to whole numbers is graded a DECIMAL→INT4 precision
    # collapse for values that are now integers.
    drift = detect_schema_drift(
        source_columns=list(columns),
        source_schema=dict(column_types),
        declared_source_columns=list(declared_source_columns or columns),
        declared_source_schema=dict(declared_source_schema or column_types),
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
        dest_recreated=dest_recreated,
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
            source_read_mode=str(
                (source_config or {}).get("source_read_mode")
                or ((source_config or {}).get("extra") or {}).get("source_read_mode")
                or ""
            ),
            db_type=str(
                (source_config or {}).get("type")
                or (source_config or {}).get("db_type")
                or source_format
                or ""
            ),
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
            redshift_staging_probe=redshift_staging_probe,
            column_nullability=dict(destination_column_nullability or {}),
            column_defaults=dict(destination_column_defaults or {}),
            identity_columns=list(destination_identity_columns or []),
            generated_columns=list(destination_generated_columns or []),
            column_types=dict(destination_column_types or {}),
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
        destination_foreign_keys=list(destination_foreign_keys or []),
        fk_risk_acknowledged=bool(fk_risk_acknowledged),
    )

    # Source-side duplicate-key probe: a small sample can miss duplicates in large
    # tables, so query the source directly when we have a resolved identity key.
    # Never stamp full_selected coverage unless status == "ran".
    source_duplicate_findings: list[dict[str, Any]] = []
    source_duplicate_probe_ran = False
    source_duplicate_probe_pk = ""
    source_duplicate_probe_status = ""
    source_duplicate_probe_message = ""
    from services.procedure_source import is_callable_source, source_read_mode_of

    callable_src = is_callable_source(source_config)
    source_duplicate_probe_expected = bool(
        (source_connector_id or source_config)
        and source_table
        and source_kind in ("database", "cloud")
        and not callable_src
    )
    if callable_src:
        source_duplicate_probe_status = "skipped_callable"
        source_duplicate_probe_message = (
            "Stored-procedure / SQL extract is a result-set snapshot — "
            "uniqueness GROUP BY is not run against a procedure name."
        )
    if source_duplicate_probe_expected:
        try:
            from services.primary_key import resolve_primary_key_source_columns

            source_pk_cols = resolve_primary_key_source_columns(
                mappings=mappings,
                source_columns=columns,
                dest_kind=dest_kind,
                validation_mode=validation_mode,
                purpose="uniqueness",
                destination_pk_columns=destination_pk_columns,
                contract_primary_key=contract_primary_key,
                stream_contracts=stream_contracts,
                stream_name=str(destination_table or source_table or ""),
            )
            if source_pk_cols:
                source_duplicate_probe_pk = ",".join(source_pk_cols)
                probe_result = probe_source_duplicate_keys_result(
                    source_connector_id=source_connector_id,
                    source_config=source_config,
                    source_table=source_table,
                    primary_key=source_pk_cols[0],
                    primary_key_columns=source_pk_cols,
                )
                source_duplicate_findings = list(probe_result.findings or [])
                source_duplicate_probe_status = probe_result.status
                source_duplicate_probe_message = probe_result.message
                source_duplicate_probe_ran = bool(probe_result.ran)
            else:
                source_duplicate_probe_status = "skipped_no_pk"
                source_duplicate_probe_message = (
                    "No uniqueness primary key resolved — probe not run"
                )
                # Without a PK the probe was not expected to prove uniqueness.
                source_duplicate_probe_expected = False
        except Exception as exc:
            logger.warning("Source duplicate-key probe skipped: %s", exc, exc_info=exc)
            source_duplicate_probe_status = "error"
            source_duplicate_probe_message = f"Source uniqueness probe skipped: {exc}"[:400]

    # Destination-side collision probe: an append into a table that already
    # enforces this key is a deterministic write abort, so it must be a Validate
    # blocker rather than a duplicate-key error after Execute starts.
    # The rows this run will read. An incremental append past a watermark only
    # writes the delta, so the collision probe must judge the delta — probing
    # the whole table refused every run after the first, on every engine.
    read_scope = _resolve_read_scope(
        sync_mode=sync_mode,
        stream_contracts=stream_contracts,
        source_format=source_format,
        source_config=source_config,
        source_table=source_table,
        destination_db_type=destination_db_type,
        destination_config=destination_config,
        destination_table=destination_table,
    )
    destination_collision = probe_append_key_collisions(
        mappings=mappings,
        source_columns=columns,
        sample_rows=sample_rows or [],
        sync_mode=sync_mode,
        dest_kind=dest_kind,
        validation_mode=validation_mode,
        destination_config=destination_config,
        destination_db_type=destination_db_type,
        destination_table=destination_table,
        destination_table_exists=dest_table_exists,
        destination_pk_columns=destination_pk_columns,
        destination_unique_keys=destination_unique_keys,
        contract_primary_key=contract_primary_key,
        stream_contracts=stream_contracts,
        source_table=source_table,
        resume=resume,
        incremental_cursor_column=read_scope.cursor_column,
        incremental_watermark=read_scope.watermark,
        incremental_tiebreak_column=read_scope.primary_key,
    )

    ctx = FilePreflightContext(
        plan,
        sample_rows,
        destination_collision=destination_collision,
        source_duplicate_findings=source_duplicate_findings,
        source_duplicate_probe_ran=source_duplicate_probe_ran,
        source_duplicate_probe_pk=source_duplicate_probe_pk,
        source_duplicate_probe_status=source_duplicate_probe_status,
        source_duplicate_probe_message=source_duplicate_probe_message,
        source_duplicate_probe_expected=source_duplicate_probe_expected,
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

    # Module 12 + Phase C11 — Conversion Contract, DDL identity, Decision Artifact.
    try:
        from services.decision_kernel import (
            assess_mapping_risk,
            build_artifact_from_mappings,
            ddl_identity_report,
            orchestrate_validation_summary,
        )

        dest_for_ddl = (destination_db_type or "").strip().lower()
        src_for_cap = (source_connector_id or "").strip().lower()
        from services.schema_fingerprint import (
            live_dest_schema_fingerprint,
            live_source_schema_fingerprint,
        )

        dest_fp = live_dest_schema_fingerprint(
            destination_column_types,
            destination_table_exists=destination_table_exists,
            sync_mode=str(sync_mode or ""),
        )

        src_fp = live_source_schema_fingerprint(
            column_types,
            authoritative=source_types_are_authoritative(source_kind, source_format),
        )
        decision_art = build_artifact_from_mappings(
            list(mappings or []),
            dest_db=dest_for_ddl,
            source_db=src_for_cap,
            route_id=f"validate:{dest_for_ddl or 'unknown'}",
            source_fingerprint=src_fp,
            dest_fingerprint=dest_fp,
            sync_mode=str(sync_mode or "full_refresh_overwrite"),
        )
        conv_cols = [
            {
                "source": m.get("source"),
                "target": m.get("target"),
                **assess_mapping_risk(m, destination_db_type=dest_for_ddl),
            }
            for m in (mappings or [])[:80]
            if isinstance(m, dict)
        ]
        art_dict = decision_art.to_dict()
        # Gates already ran — classify into Validation Orchestrator buckets.
        def _gate_row(g) -> dict:
            if isinstance(g, dict):
                return {
                    "id": str(g.get("id") or g.get("gate_id") or ""),
                    "status": str(g.get("status") or ""),
                    "message": str(g.get("message") or ""),
                }
            gid = getattr(g, "gate_id", "")
            status = getattr(g, "status", "")
            return {
                "id": str(getattr(gid, "value", gid) or ""),
                "status": str(getattr(status, "value", status) or ""),
                "message": str(getattr(g, "message", "") or ""),
            }

        validation_orch = orchestrate_validation_summary(
            decision_artifact=art_dict,
            gates=[_gate_row(g) for g in list(getattr(result, "gates", None) or [])],
            blockers=[
                {**_gate_row(b), "status": "block"}
                for b in list(getattr(result, "blockers", None) or [])
            ],
        )
        proof_bundle = {
            **proof_bundle,
            "ddl_identity": ddl_identity_report(
                list(mappings or []),
                dest_db=dest_for_ddl,
            ),
            "decision_artifact": art_dict,
            "decision_artifact_hash": decision_art.content_hash,
            "validation_orchestrator": validation_orch,
            "conversion_contract": {
                "version": "conversion_contract.v1",
                "columns": conv_cols,
            },
        }
    except Exception as conv_exc:
        # GA: never soft-skip conversion / DDL identity stamp — surface fail-closed.
        logger.warning(
            "conversion contract stamp failed closed: %s", conv_exc, exc_info=conv_exc
        )
        proof_bundle = {
            **proof_bundle,
            "ddl_identity": {
                "ddl_identity_hash": "",
                "error": str(conv_exc)[:400],
                "stamp_failed": True,
            },
            "conversion_contract": {
                "version": "conversion_contract.v1",
                "stamp_failed": True,
                "error": str(conv_exc)[:400],
            },
        }

    from services.preflight_rules import enrich_blockers

    blockers = [
        {"id": b.gate_id.value, "message": b.message, "details": b.details}
        for b in result.blockers
    ]
    # Missing DDL identity after stamp failure → operator-facing blocker.
    ddl_id = (proof_bundle.get("ddl_identity") or {}) if isinstance(proof_bundle, dict) else {}
    if ddl_id.get("stamp_failed") or (
        isinstance(proof_bundle, dict)
        and proof_bundle.get("ddl_identity") is not None
        and not str(ddl_id.get("ddl_identity_hash") or "").strip()
        and ddl_id.get("error")
    ):
        blockers.append(
            {
                "id": "ddl_identity",
                "message": (
                    "Map→DDL identity stamp failed — re-run Validate after fixing "
                    f"conversion contract: {ddl_id.get('error') or 'unknown'}"
                ),
                "details": {"ddl_identity": ddl_id},
            }
        )
    # Additive ADD without Kernel stamp must not leave Execute as the first refuse.
    create_unstamped = [
        str(m.get("target") or "")
        for m in (mappings or [])
        if isinstance(m, dict)
        and str(m.get("target") or "").strip()
        and str(m.get("assignment_strategy") or "") != "pending_dest_schema"
        and (
            m.get("create_new")
            or str(m.get("assignment_strategy") or "")
            in {"create_compatible_new", "identity_passthrough"}
            or backfill_new_fields
        )
        and not str(m.get("target_type") or m.get("dest_type") or "").strip()
        and str(m.get("target") or "") not in (destination_column_types or {})
        and str(m.get("target") or "").lower()
        not in {str(k).lower() for k in (destination_column_types or {})}
    ]
    create_unstamped = list(dict.fromkeys([c for c in create_unstamped if c]))
    additive_stamp_blocked = False
    if create_unstamped or _unstamped_additive:
        cols = list(dict.fromkeys([*_unstamped_additive, *create_unstamped]))
        sample = ", ".join(repr(c) for c in cols[:5])
        more = f" (+{len(cols) - 5} more)" if len(cols) > 5 else ""
        blockers.append(
            {
                "id": "g6_additive_stamp",
                "message": (
                    f"Additive column(s) {sample}{more} lack Map target_type under "
                    "partial Studio — Decision Kernel refuse VARCHAR ADD invent. "
                    "Re-run Map (create-new stamp) or disable backfill_new_fields."
                ),
                "details": {
                    "columns": cols[:20],
                    "backfill_new_fields": bool(backfill_new_fields),
                    "kind": "additive_map_stamp_required",
                },
            }
        )
        additive_stamp_blocked = True

    # Bounded destination carriers (DECIMAL(p,s) / VARCHAR(n) / sized INTEGER)
    # are decidable before the write. Screening a preview and letting the writer
    # discover row 431 is how a 1M-row load ends with zero rows committed and a
    # Run-phase error for a defect Validate could name. Scan whatever rows this
    # caller actually holds — the whole batch at Execute preflight, the preview in
    # Studio — and report the evidence for what it is.
    fit_gate: dict[str, Any] | None = None
    fit_report_payload: dict[str, Any] = {}
    fit_blocked = False
    try:
        from connectors.writer_common import transform_error_policy_for_validation_mode
        from services.population_fit_scan import (
            GATE_ID as _FIT_GATE_ID,
            build_population_fit_gate,
            scan_population_fit,
        )
        from services.row_filter import iter_filtered_rows
        from services.sync_cursor import incremental_read_narrows, iter_rows_after_watermark

        cfg_extra = (source_config or {}).get("extra")
        extra_filter = (
            cfg_extra.get("source_filter")
            if isinstance(cfg_extra, dict) and isinstance(cfg_extra.get("source_filter"), dict)
            else None
        )
        cfg_filter = (
            (source_config or {}).get("source_filter")
            if isinstance((source_config or {}).get("source_filter"), dict)
            else None
        )
        resolved_filter = dict(source_filter or cfg_filter or extra_filter or {})

        if skip_population_fit:
            # Execute after a stamped Studio Validate: dest probe + sample
            # gates still run. The 1M walk does not. Write-time fit binds.
            from services.population_fit_scan import (
                EVIDENCE_PARTIAL,
                FitScanReport,
                bounded_targets,
            )

            targets, undecidable, safe = bounded_targets(
                mappings,
                dest_types=destination_column_types or {},
                source_types=column_types or {},
                dest_db=destination_db_type,
                job_error_policy=transform_error_policy_for_validation_mode(
                    validation_mode
                ),
                source_kind=source_kind,
                source_format=source_format,
                sync_mode=sync_mode,
                dest_table_exists=destination_table_exists,
            )
            fit_report = FitScanReport(
                evidence=EVIDENCE_PARTIAL,
                rows_scanned=0,
                rows_total=int(row_count or 0),
                targets=tuple(targets),
                undecidable=tuple(undecidable),
                safe_by_declaration=tuple(safe),
                truncated_reason="reused_validate",
                note=(
                    "Population fit reused from approved Validate. Execute does "
                    "not re-walk the source; write-time fit still binds every row."
                ),
            )
            fit_report_payload = fit_report.to_dict()
            fit_report_payload["reused_from_validate"] = True
            fit_gate = build_population_fit_gate(fit_report)
        if not skip_population_fit:
          # A caller-supplied population is a one-shot iterable; a stored upload
          # or a table walk can be produced again. Only the replayable ones may
          # re-prove a create-new widen, because an exhausted iterator would
          # "prove" a clean scan over zero rows.
          replay_source: Callable[[], Iterable[Mapping[str, Any]] | None] | None = None
          widen = None
          if population_rows is None and str(source_file_id or "").strip():
            from services.file_parser import iter_stored_upload_rows

            stored_rows = iter_stored_upload_rows(source_file_id)
            if stored_rows is not None:
                population_rows = stored_rows
                rows_are_population = True

                def replay_source() -> Iterable[Mapping[str, Any]] | None:
                    return iter_stored_upload_rows(source_file_id)
          if population_rows is None:
            try:
                table_rows = _iter_table_population_for_preflight(
                    source_kind=source_kind,
                    source_format=source_format,
                    source_connector_id=source_connector_id,
                    source_config=source_config,
                    source_table=source_table,
                    mappings=mappings,
                    column_types=column_types,
                    dest_types=destination_column_types or {},
                    dest_db=destination_db_type,
                    sync_mode=sync_mode,
                    read_scope=read_scope if incremental_read_narrows(sync_mode) else None,
                    source_filter=resolved_filter or None,
                )
            except Exception as walk_exc:
                logger.warning(
                    "table population walk failed; Validate will use the preview: %s",
                    walk_exc,
                )
                table_rows = None
            if table_rows is not None:
                try:
                    first = next(table_rows)
                except StopIteration:
                    # Honest empty table — not a failed walk.
                    population_rows = iter(())
                    rows_are_population = True
                except Exception as walk_exc:
                    # Cursor-unreadable / down source must keep the preview,
                    # never claim an empty population as exact.
                    logger.warning(
                        "table population walk failed; Validate will use the preview: %s",
                        walk_exc,
                    )
                else:

                    def _chained():
                        yield first
                        yield from table_rows

                    walked: Iterable[Mapping[str, Any]] = _chained()
                    if shape_recipe:
                        from services.shape_preflight import shaped_population_rows

                        shaped = shaped_population_rows(
                            shape_recipe,
                            walked,
                            source_columns=columns,
                        )
                        if shaped is not None:
                            walked = shaped
                    population_rows = walked
                    rows_are_population = True

                    def replay_source() -> Iterable[Mapping[str, Any]] | None:
                        again = _iter_table_population_for_preflight(
                            source_kind=source_kind,
                            source_format=source_format,
                            source_connector_id=source_connector_id,
                            source_config=source_config,
                            source_table=source_table,
                            mappings=mappings,
                            column_types=column_types,
                            dest_types=destination_column_types or {},
                            dest_db=destination_db_type,
                            sync_mode=sync_mode,
                            read_scope=(
                                read_scope
                                if incremental_read_narrows(sync_mode)
                                else None
                            ),
                            source_filter=resolved_filter or None,
                        )
                        if again is None or not shape_recipe:
                            return again
                        from services.shape_preflight import shaped_population_rows

                        return (
                            shaped_population_rows(
                                shape_recipe, again, source_columns=columns
                            )
                            or again
                        )

          def _narrowed(rows_in: Iterable[Mapping[str, Any]]) -> Iterable[Mapping[str, Any]]:
              """Same scope the first scan measured — filter then watermark."""
              out_rows = rows_in
              if resolved_filter:
                  filtered_again = iter_filtered_rows(out_rows, resolved_filter)
                  if filtered_again is not None:
                      out_rows = filtered_again
              if incremental_read_narrows(sync_mode) and read_scope.bounded:
                  narrowed_again = iter_rows_after_watermark(
                      out_rows, read_scope, keep_unreadable=True
                  )
                  if narrowed_again is not None:
                      out_rows = narrowed_again
              return out_rows

          scan_input = population_rows if population_rows is not None else sample_rows
          fit_rows_total = int(row_count or 0)
          if resolved_filter:
            filtered = iter_filtered_rows(scan_input, resolved_filter)
            if filtered is not None:
                scan_input = filtered
            fit_rows_total = 0
          if incremental_read_narrows(sync_mode) and read_scope.bounded:
            narrowed = iter_rows_after_watermark(
                scan_input, read_scope, keep_unreadable=True
            )
            if narrowed is not None:
                scan_input = narrowed
            # Table COUNT is not this run's population — the scan's own count is.
            fit_rows_total = 0
          fit_report = scan_population_fit(
            scan_input,
            mappings,
            dest_types=destination_column_types or {},
            source_types=column_types or {},
            dest_db=destination_db_type,
            dialect_label=destination_db_type,
            # Same policy the writer resolves from the same validation mode —
            # Validate must not forecast a quarantine the writer will abort on.
            job_error_policy=transform_error_policy_for_validation_mode(validation_mode),
            rows_total=fit_rows_total,
            rows_are_population=bool(rows_are_population and population_rows is not None),
            source_kind=source_kind,
            source_format=source_format,
            sync_mode=sync_mode,
            dest_table_exists=destination_table_exists,
            deadline_monotonic=_fit_scan_deadline(
                fit_scan_deadline, fit_scan_seconds
            ),
            on_progress=on_fit_progress,
          )
          if destination_table_exists is False and replay_source is not None:
            # We own the CREATE here: no live DDL binds, so a carrier the peek
            # sized too narrow is our defect, not the operator's. Widen it from
            # the measured population and re-prove on the same rows — an
            # unproven widen is dropped and the block stands.
            from services.population_fit_scan import create_new_population_widen

            replay_scoped = replay_source

            def _replay() -> Iterable[Mapping[str, Any]] | None:
                again = replay_scoped()
                return None if again is None else _narrowed(again)

            widen = create_new_population_widen(
                fit_report,
                mappings,
                _replay,
                scan_kwargs=dict(
                    dest_types=destination_column_types or {},
                    source_types=column_types or {},
                    dest_db=destination_db_type,
                    dialect_label=destination_db_type,
                    job_error_policy=transform_error_policy_for_validation_mode(
                        validation_mode
                    ),
                    rows_total=fit_rows_total,
                    rows_are_population=bool(rows_are_population),
                    source_kind=source_kind,
                    source_format=source_format,
                    sync_mode=sync_mode,
                    dest_table_exists=destination_table_exists,
                ),
            )
            if widen is not None:
                fit_report = widen.report
          fit_report_payload = fit_report.to_dict()
          if widen is not None:
            fit_report_payload["create_new_widen"] = list(widen.applied)
          if incremental_read_narrows(sync_mode) and read_scope.bounded:
            fit_report_payload["delta_scope"] = {
                "cursor_column": read_scope.cursor_column,
                "watermark": str(read_scope.watermark),
            }
          if resolved_filter:
            from services.row_filter import filter_columns

            fit_report_payload["filter_scope"] = {
                "columns": filter_columns(resolved_filter),
            }
          # The gate is always stated, including when nothing needed scanning:
          # "no bounded carrier can be exceeded" is evidence, and a silently
          # absent gate reads as an unasked question.
          fit_gate = build_population_fit_gate(fit_report)
        if fit_gate.get("status") == "block":
            fit_blocked = True
            blockers.append(
                {
                    "id": _FIT_GATE_ID,
                    "message": str(fit_gate.get("message") or ""),
                    "details": dict(fit_gate.get("details") or {}),
                    "suggested_actions": list(
                        (fit_gate.get("details") or {}).get("suggested_actions") or []
                    ),
                }
            )
    except Exception as exc:  # noqa: BLE001 - a failed scan is unmeasured, never "fits"
        logger.warning(
            "population fit scan failed: %s", exc, exc_info=exc
        )
        fit_report_payload = {"evidence": "unmeasured", "error": str(exc)[:400]}
        fit_gate = {
            "id": "g3f_population_fit",
            "status": "warn",
            "message": (
                "Population fit could not be measured — bounded destination "
                "carriers stay unproven until the write-time checks run"
            ),
            "duration_ms": 0,
            "details": dict(fit_report_payload),
        }

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

    extra_gates: list[dict[str, Any]] = [fit_gate] if fit_gate else []
    extra_passed = sum(1 for g in extra_gates if g.get("status") != "block")
    total_gates = result.total_gates + len(extra_gates)
    passed_count = result.passed_count + extra_passed

    out = {
        # Carrier semantics are engine-specific, so the engine the gates ran
        # against travels with the verdict: a remediation resolved without it
        # would describe a different database's carriers.
        "destination_db_type": destination_db_type or "",
        "passed": bool(result.passed) and not additive_stamp_blocked and not fit_blocked,
        "passed_count": passed_count,
        "total_gates": total_gates,
        "readiness_score": round(passed_count / max(total_gates, 1) * 100, 1),
        "gates": [
            {
                "id": g.gate_id.value,
                "status": g.status.value,
                "message": g.message,
                "duration_ms": round(g.duration_ms, 2),
                "details": g.details,
            }
            for g in result.gates
        ]
        + (
            [
                {
                    "id": "g6_additive_stamp",
                    "status": "block",
                    "message": enriched_blockers[-1]["message"]
                    if additive_stamp_blocked and enriched_blockers
                    else "Additive Map stamp required",
                    "duration_ms": 0,
                    "details": {"kind": "additive_map_stamp_required"},
                }
            ]
            if additive_stamp_blocked
            else []
        )
        + extra_gates,
        "blockers": enriched_blockers,
        "population_fit": fit_report_payload,
        # Echo Kernel-stamped additive types so Map/Execute share Validate authority.
        "stamped_mappings": [
            {
                "source": str(m.get("source") or ""),
                "target": str(m.get("target") or ""),
                "target_type": str(m.get("target_type") or ""),
                "create_new": bool(m.get("create_new")),
                "assignment_strategy": str(m.get("assignment_strategy") or ""),
            }
            for m in (mappings or [])
            if isinstance(m, dict) and str(m.get("target_type") or "").strip()
        ],
        "schema_drift": drift,
        "ddl_issues": ddl_issues,
        "sample_quality": sample_quality,
        "proof_bundle": proof_bundle,
        "payload_shape": payload_shape,
        "validation_plan": validation_plan.to_dict(),
        "coercion_report": ctx.coercion_report(),
        # Canonical Kernel findings — Map / Proof / root-cause must not re-classify.
        "validation_findings": [],
        "date_locale": date_locale,
        "number_locale": number_locale,
        "privilege_probe": privilege_probe or {},
        "redshift_staging_probe": redshift_staging_probe or {},
        "recommended_batch_size": min(
            recommended_batch_size(_src_fmt),
            recommended_batch_size(_tgt_fmt) or recommended_batch_size(_src_fmt),
        ),
        # Echo signed Risk Contracts so Map/Execute share the same stamped rows
        # Validate greened — never leave FE with unsigned drafts after hydrate.
        "signed_mappings": [
            {
                "source": str(m.get("source") or ""),
                "target": str(m.get("target") or ""),
                "risk_contract": m.get("risk_contract"),
                "risk_acknowledged": bool(m.get("risk_acknowledged")),
            }
            for m in (mappings or [])
            if isinstance(m, dict) and isinstance(m.get("risk_contract"), dict)
        ],
    }
    if callable_src:
        out["callable_extract"] = {
            "mode": source_read_mode_of(source_config),
            "catalog_probes": "skipped",
            "note": (
                "Result-set snapshot — FK catalog, uniqueness GROUP BY, and "
                "population orphan scans are not run against a procedure name."
            ),
            "cdc": "refused",
            "history_sync": "refused",
            "incremental": "spool_cursor_filter_at_least_once",
            "honesty": (
                "CDC / SCD2 / mirror are refused on CALL/SELECT. Incremental "
                "filters the spool with cursor > watermark (at-least-once). "
                "Not exactly-once. Not a catalog-breadth claim."
            ),
        }

    # A gate that blocks a declared conversion must show up in the report every
    # other surface reads — otherwise Validate blocks while the panel under it
    # says there are no blocking failures.
    out["coercion_report"] = reconcile_coercion_report(
        out.get("coercion_report"), out.get("gates")
    )

    # Stamp Decision Kernel ValidationFindings onto Validate SSOT.
    # Coercion preview is 25 rows; population-fit overflows (row 293+)
    # must share the same Remap surface or the dest widen stays hidden.
    try:
        from services.decision_kernel import (
            findings_from_coercion_report,
            findings_from_population_fit,
            merge_validation_findings,
        )

        _dest = str(destination_db_type or "")
        _vf = merge_validation_findings(
            findings_from_coercion_report(
                out.get("coercion_report"),
                dest_db=_dest,
            ),
            findings_from_population_fit(
                out.get("population_fit"),
                dest_db=_dest,
            ),
        )
        out["validation_findings"] = _vf
        if isinstance(out.get("proof_bundle"), dict) and _vf:
            out["proof_bundle"] = {
                **out["proof_bundle"],
                "validation_findings": _vf,
                "validation_finding_schema": "validation_finding_v1",
            }
    except Exception as vf_exc:
        logger.warning("validation_findings stamp failed: %s", vf_exc, exc_info=vf_exc)

    # Mapping contract — every source column accounted for (G13) and every
    # required destination column filled (G14).
    src_coverage, contract_gates, contract_blockers = build_mapping_contract_gates(
        source_columns=list(columns or []),
        mappings=list(mappings or []),
        destination_table_exists=destination_table_exists,
        column_nullability=destination_column_nullability,
        column_defaults=destination_column_defaults,
        identity_columns=destination_identity_columns,
        generated_columns=destination_generated_columns,
        # Types keys are the live dest column list. Nullability keys are a
        # tautology for the partial-catalog check — do not substitute them.
        dest_columns=list((destination_column_types or {}).keys()),
        dest_recreated=dest_recreated,
    )
    out["source_coverage"] = src_coverage

    # Field Reduction Ledger (G16) — a typed, evidenced disposition per source
    # field. G13 proves the drop was declared; this proves it was decided.
    from services.field_reduction_ledger import build_field_reduction_evidence

    field_reduction, reduction_gate = build_field_reduction_evidence(
        source_columns=list(columns or []),
        mappings=list(mappings or []),
        target_columns=list((destination_column_types or {}).keys()),
        source_column_types=dict(column_types or {}),
        target_column_types=dict(destination_column_types or {}),
        sample_rows=list(sample_rows or []),
    )
    out["field_reduction_ledger"] = field_reduction
    contract_gates = [*contract_gates, reduction_gate]
    if reduction_gate.get("status") == "block":
        contract_blockers = [
            *contract_blockers,
            {
                "id": reduction_gate["id"],
                "message": reduction_gate["message"],
                "details": reduction_gate["details"],
            },
        ]

    # G19 — an overwrite recreates the destination, so the type standing there
    # now is discarded for every verdict above. Where that discarded type is
    # narrower than the source, the operator declared a carrier this run
    # replaces; say so instead of writing through the replacement.
    from services.dest_schema_replacement import build_dest_schema_replacement_gate

    replacement_gate = build_dest_schema_replacement_gate(
        mappings=list(mappings or []),
        source_column_types=dict(column_types or {}),
        destination_column_types=dict(
            destination_live_column_types or destination_column_types or {}
        ),
        destination_table_exists=destination_table_exists,
        dest_recreated=dest_recreated,
        destination_db_type=destination_db_type,
        source_db_type=_src_fmt,
    )
    contract_gates = [*contract_gates, replacement_gate]
    if replacement_gate.get("status") == "block":
        contract_blockers = [
            *contract_blockers,
            {
                "id": replacement_gate["id"],
                "message": replacement_gate["message"],
                "details": replacement_gate["details"],
            },
        ]

    # G20 — a declared code crosswalk must cover every distinct source value
    # in the population, not the Validate sample. Unmapped codes fail closed.
    # Do not consume G3F's population generator: GROUP BY / a replayable file
    # scan is the population proof. A covered sample is not.
    from services.code_crosswalk import (
        build_code_crosswalk_evidence,
        coded_mappings,
        collect_observed_codes,
        probe_population_codes,
    )

    observed_codes: dict[str, dict[str, int]] | None = None
    scan_method = ""
    coded = coded_mappings(list(mappings or []))
    if coded:
        coded_cols = [str(c["source"]) for c in coded]
        probed, _truncated, scan_method = probe_population_codes(
            columns=coded_cols,
            source_connector_id=source_connector_id,
            source_config=source_config,
            source_table=source_table,
            source_file_id=source_file_id,
            shape_recipe=shape_recipe if isinstance(shape_recipe, dict) else None,
            source_columns=list(columns or []),
        )
        if probed is not None:
            observed_codes = probed
        elif isinstance(population_rows, (list, tuple)) and rows_are_population:
            observed_codes, _truncated = collect_observed_codes(
                population_rows, coded_cols
            )
            scan_method = "population_rows"

    population_seq = (
        list(population_rows)
        if isinstance(population_rows, (list, tuple))
        else None
    )
    code_crosswalk, crosswalk_gate = build_code_crosswalk_evidence(
        mappings=list(mappings or []),
        sample_rows=list(sample_rows or []),
        population_rows=population_seq,
        rows_are_population=bool(rows_are_population)
        and observed_codes is None
        and (population_seq is not None or population_rows is None),
        observed_codes=observed_codes,
    )
    if scan_method:
        code_crosswalk["scan_method"] = scan_method
    out["code_crosswalk"] = code_crosswalk
    contract_gates = [*contract_gates, crosswalk_gate]
    if crosswalk_gate.get("status") == "block":
        contract_blockers = [
            *contract_blockers,
            {
                "id": crosswalk_gate["id"],
                "message": crosswalk_gate["message"],
                "details": crosswalk_gate["details"],
            },
        ]

    from services.control_totals import (
        build_control_totals_gate,
        build_control_totals_report,
        proof_pack_control_totals,
    )

    control_totals_report = build_control_totals_report(
        mappings=list(mappings or []),
        phase="validate",
    )
    control_totals_gate = build_control_totals_gate(
        control_totals_report, phase="validate"
    )
    out["control_totals"] = control_totals_report
    contract_gates = [*contract_gates, control_totals_gate]

    if isinstance(out.get("proof_bundle"), dict):
        out["proof_bundle"] = {
            **out["proof_bundle"],
            "source_coverage": src_coverage,
            # The reduction ledger travels with the pack: an auditor reading the
            # export must see why each field was not carried, not only that it
            # was declared omitted.
            "field_reduction_ledger": field_reduction,
            # Evidence travels with the proof pack, not only with the HTTP
            # response: an exported pack must show whether the bounded carriers
            # were decided on every row or on a preview.
            "population_fit": fit_report_payload,
            "code_crosswalk": code_crosswalk,
            "control_totals": proof_pack_control_totals(control_totals_report),
        }
    # Hosted G13/G14/G15 replace package stubs (full dest nullability / identity).
    contract_ids = {str(g.get("id") or "") for g in contract_gates}
    out["gates"] = [
        g for g in out["gates"] if str(g.get("id") or "") not in contract_ids
    ] + list(contract_gates)
    if contract_blockers:
        out["blockers"] = [
            *out["blockers"],
            *enrich_blockers(
                contract_blockers,
                dest_kind=dest_kind,
                validation_mode=validation_mode,
            ),
        ]
        out["passed"] = False
    out["passed_count"] = sum(1 for g in out["gates"] if g.get("status") == "pass")
    out["total_gates"] = len(out["gates"])
    out["readiness_score"] = round(
        out["passed_count"] / max(out["total_gates"], 1) * 100, 1
    )

    # FK / relational constraint findings + sample orphan probe.
    # Schema unmapped-FK + sample orphans fail closed in strict/maximum unless
    # acknowledged. Sample orphan never invents population RI proof.
    try:
        from preflight.constraint_hints import (
            assess_constraint_compatibility,
            constraint_findings_block_transfer,
            referential_integrity_posture,
        )
        from services.sample_orphan_probe import probe_sample_fk_orphans

        findings = list(
            assess_constraint_compatibility(
                ctx,
                validation_mode=validation_mode,
                fk_risk_acknowledged=bool(fk_risk_acknowledged),
            )
            or []
        )

        # Prefer source-introspected FKs for orphan probe (sample is source rows).
        probe_fks = list(destination_foreign_keys or [])
        try:
            from services.procedure_source import is_callable_source

            if is_callable_source(source_config):
                src_fks = []
                out["source_foreign_keys"] = []
                out["source_fk_catalog"] = {
                    "ran": False,
                    "skipped": True,
                    "reason": "callable_source",
                    "note": (
                        "Stored-procedure / SQL extract is a result-set snapshot — "
                        "no catalog relation to probe for foreign keys."
                    ),
                }
            else:
                src_fks = load_source_foreign_keys(
                    source_connector_id=source_connector_id or "",
                    source_config=source_config,
                    source_table=source_table or "",
                )
                if src_fks:
                    probe_fks = src_fks
                    out["source_foreign_keys"] = src_fks
        except Exception as src_fk_exc:
            logger.debug(
                "source FK introspect skipped: %s", src_fk_exc, exc_info=src_fk_exc
            )

        orphan_report = probe_sample_fk_orphans(
            sample_rows=sample_rows,
            mappings=list(mappings or []),
            foreign_keys=probe_fks,
            source_connector_id=source_connector_id or "",
            source_config=source_config,
            validation_mode=validation_mode,
            fk_risk_acknowledged=bool(fk_risk_acknowledged),
        )
        out["sample_orphan_probe"] = {
            k: orphan_report.get(k)
            for k in (
                "ran",
                "coverage",
                "population_proof",
                "orphan_count",
                "checked_values",
                "checks",
                "note",
                "error",
            )
            if k in orphan_report
        }
        for of in orphan_report.get("findings") or []:
            if isinstance(of, dict):
                findings.append(of)

        # Module 11 — opt-in full-table population orphan scan (only path to RI proven).
        pop_report: dict[str, Any] = {
            "ran": False,
            "coverage": "none",
            "population_proof": False,
            "complete": False,
            "orphan_count": 0,
            "note": "Population orphan scan not requested for this Validate.",
        }
        if run_population_orphan_scan:
            from services.population_orphan_probe import probe_population_fk_orphans

            pop_report = probe_population_fk_orphans(
                child_table=source_table or "",
                mappings=list(mappings or []),
                foreign_keys=probe_fks,
                source_connector_id=source_connector_id or "",
                source_config=source_config,
                validation_mode=validation_mode,
                fk_risk_acknowledged=bool(fk_risk_acknowledged),
            )
            for of in pop_report.get("findings") or []:
                if isinstance(of, dict):
                    findings.append(of)
        out["population_orphan_probe"] = {
            k: pop_report.get(k)
            for k in (
                "ran",
                "coverage",
                "population_proof",
                "complete",
                "orphan_count",
                "checks",
                "child_table",
                "note",
                "error",
            )
            if k in pop_report
        }

        out["constraint_findings"] = findings
        out["constraint_hints"] = findings
        pop_ran = bool(pop_report.get("ran"))
        # Incomplete population scan must not invent proven via orphan_count=0.
        pop_count: int | None
        if pop_ran and pop_report.get("complete"):
            pop_count = int(pop_report.get("orphan_count") or 0)
        else:
            pop_count = None
        ri_posture = referential_integrity_posture(
            findings,
            population_orphan_probe_ran=pop_ran,
            population_orphan_count=pop_count,
            sample_orphan_probe_ran=bool(orphan_report.get("ran")),
            sample_orphan_count=int(orphan_report.get("orphan_count") or 0),
        )
        if fk_risk_acknowledged:
            ri_posture = {
                **ri_posture,
                "fk_risk_acknowledged": True,
                "acknowledgment": {
                    "actor": (acknowledgment_actor or "operator").strip() or "operator",
                    "reason": (
                        (acknowledgment_reason or "").strip()
                        or "Operator acknowledged destination FK mapping risk for this run"
                    ),
                },
            }
        out["referential_integrity"] = ri_posture

        if constraint_findings_block_transfer(
            findings,
            validation_mode=validation_mode,
            fk_risk_acknowledged=bool(fk_risk_acknowledged),
        ):
            fk_msg, fk_details = build_fk_block(
                findings,
                ri_posture=ri_posture,
                population_orphan_probe_ran=pop_ran,
                sample_orphan_probe_ran=bool(orphan_report.get("ran")),
            )
            fk_gate = {
                "id": "constraint_fk",
                "status": "block",
                "message": fk_msg,
                "duration_ms": 0,
                "details": fk_details,
            }
            out["gates"] = [*out["gates"], fk_gate]
            fk_blocker = enrich_blockers(
                [
                    {
                        "id": "constraint_fk",
                        "message": fk_msg,
                        "details": fk_details,
                    }
                ],
                dest_kind=dest_kind,
                validation_mode=validation_mode,
            )
            out["blockers"] = [*out["blockers"], *fk_blocker]
            out["passed"] = False
            out["passed_count"] = sum(
                1 for g in out["gates"] if g.get("status") == "pass"
            )
            out["total_gates"] = len(out["gates"])
            out["readiness_score"] = round(
                out["passed_count"] / max(out["total_gates"], 1) * 100, 1
            )
    except Exception as hint_exc:
        logger.debug("constraint findings skipped: %s", hint_exc, exc_info=hint_exc)
        out["constraint_hints"] = []
        out["constraint_findings"] = []
        out["referential_integrity"] = {
            "proven": False,
            "coverage": "none",
            "population_orphan_probe_ran": False,
            "sample_orphan_probe_ran": False,
            "finding_count": 0,
            "note": "Constraint assessment unavailable for this run.",
        }

    # G22 — dest population RI is a post-write Gate-8 proof. State it every
    # Validate so the question is never unasked; never green-pass a sample.
    from services.destination_ri_probe import build_dest_ri_validate_gate

    has_relationships = bool(
        out.get("source_foreign_keys")
        or destination_foreign_keys
        or (out.get("constraint_findings") or [])
    )
    dest_ri_gate = build_dest_ri_validate_gate(has_relationships=has_relationships)
    out["gates"] = [
        g
        for g in out["gates"]
        if str(g.get("id") or "") != dest_ri_gate["id"]
    ] + [dest_ri_gate]
    out["passed_count"] = sum(1 for g in out["gates"] if g.get("status") == "pass")
    out["total_gates"] = len(out["gates"])
    out["readiness_score"] = round(
        out["passed_count"] / max(out["total_gates"], 1) * 100, 1
    )

    # Soft Snowflake warehouse sizing from G7 volume — never a GateId.
    try:
        dest_fmt = str(destination_db_type or "").strip().lower()
        if "snowflake" in dest_fmt:
            from services.snowflake_warehouse_advice import advise_snowflake_warehouse

            advice = advise_snowflake_warehouse(
                estimated_bytes=int(est_bytes or 0),
                row_count=int(row_count or 0),
            )
            if advice:
                out["snowflake_warehouse_advice"] = advice
    except Exception as sf_exc:
        logger.debug("snowflake warehouse advice skipped: %s", sf_exc, exc_info=sf_exc)

    # Multi-load intelligence: compare sample to last N loads of this route.
    try:
        from services.data_quality_history import compare_route_to_history

        from services.data_quality_history import resolve_history_endpoints

        src_table = (source_table or source_filename or "").strip()
        dst_table = (destination_table or "").strip()
        src_ep, dst_ep = resolve_history_endpoints(
            source_kind=source_kind,
            source_format=source_format or "",
            source_table=src_table,
            source_config=source_config,
            source_connector_id=source_connector_id or "",
            dest_kind="database",
            dest_format=destination_db_type or "",
            dest_table=dst_table,
            dest_config=destination_config,
        )
        out["load_history_report"] = compare_route_to_history(
            sample_rows or [],
            src_ep,
            dst_ep,
            schema=column_types,
        )
        # Module 17 — measured historical success (never invent a rate).
        from services.historical_success_contract import (
            measure_route_historical_success,
            stamp_mappings_historical_success,
        )

        hs = measure_route_historical_success(src_ep, dst_ep)
        out["historical_success"] = hs
        pb = dict(out.get("proof_bundle") or {})
        pb["historical_success"] = hs
        out["proof_bundle"] = pb
        # Stamp route-scoped evidence onto mapping rows carried in conversion contract.
        conv = dict(pb.get("conversion_contract") or {})
        if isinstance(conv.get("columns"), list):
            conv["columns"] = stamp_mappings_historical_success(conv["columns"], hs)
            pb["conversion_contract"] = conv
            out["proof_bundle"] = pb
    except Exception as hist_exc:
        logger.warning("load history compare failed during preflight", exc_info=True)
        out["load_history_report"] = {
            "passed": True,
            "anomalies": [],
            "prior_load_count": 0,
            "warning": f"Load-history compare unavailable: {hist_exc!s}"[:240],
        }
        try:
            from services.historical_success_contract import unmeasured_historical_success

            hs = unmeasured_historical_success(
                reason=f"Load history unavailable: {hist_exc!s}"[:200],
            )
            out["historical_success"] = hs
            pb = dict(out.get("proof_bundle") or {})
            pb["historical_success"] = hs
            out["proof_bundle"] = pb
        except Exception:
            pass

    # Schema drift is its own rule — never masquerade as Target DDL.
    # Datawrap rule: hard-breaking ALWAYS pauses (even under propagate_*).
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
                    "— sync paused for review (Datawrap fail-closed)"
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
                    "remediation_kind": "review_mappings",
                    "ack_required": False,
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

    from services.root_cause_engine import apply_root_causes_to_preflight
    from services.validation_mode_contract import stamp_validation_mode

    number_findings = ambiguous_number_columns(
        sample_rows, columns, number_locale=number_locale
    )
    date_findings = ambiguous_date_columns(
        sample_rows, columns, date_locale=date_locale
    )
    # WIRE is a property of the source, not an operator choice — the locale
    # control stays on Auto so it never reads back a value it cannot offer.
    from services.transform_engine import NUMBER_LOCALE_WIRE

    operator_locale = "" if number_locale == NUMBER_LOCALE_WIRE else number_locale
    out["number_locale"] = operator_locale
    out["number_locale_report"] = {
        "number_locale": operator_locale or "",
        "reads_typed_wire_values": number_locale == NUMBER_LOCALE_WIRE,
        "ambiguous_columns": number_findings or number_locale_assumed,
        "assumed": bool(number_locale_assumed),
        "assumed_columns": number_locale_assumed,
        "decision": (
            "assumed_us"
            if number_locale_assumed
            else ("set_locale" if number_findings else "ok")
        ),
    }
    if number_locale_assumed:
        cols = ", ".join(f["column"] for f in number_locale_assumed[:6])
        out.setdefault("warnings", []).append(
            {
                "id": "number_locale_assumed",
                "message": (
                    f"{cols}: no locale declared and the sample carries no "
                    "grouping evidence — reading as US (1,234.56). If this "
                    "export is European, set number locale EU in "
                    "Destination → Advanced before you run."
                ),
                "details": {"columns": number_locale_assumed},
            }
        )
    out["date_locale"] = date_locale
    out["date_locale_report"] = {
        "date_locale": date_locale or "",
        "ambiguous_columns": date_findings,
        "decision": "set_locale" if date_findings else "ok",
    }
    if date_findings:
        cols = ", ".join(f["column"] for f in date_findings[:6])
        out.setdefault("warnings", []).append(
            {
                "id": "date_locale",
                "message": (
                    f"{cols}: day/month order is ambiguous (01/02/2024 is "
                    "Jan 2 or Feb 1). Set date locale DMY or MDY in "
                    "Destination → Advanced — Auto will not guess."
                ),
                "details": {"columns": date_findings},
            }
        )
    if number_findings:
        cols = ", ".join(f["column"] for f in number_findings[:6])
        out.setdefault("warnings", []).append(
            {
                "id": "number_locale",
                "message": (
                    f"{cols}: grouping is ambiguous (1,234 vs 1.234). "
                    "Set number locale US or EU in Destination → Advanced — "
                    "Auto will not guess."
                ),
                "details": {"columns": number_findings},
            }
        )

    stamp_validation_mode(out, validation_mode)
    # Discovery / Audit: never invent an Execute unlock from Validate readiness.
    try:
        from services.validation_mode_contract import mode_allows_write, mode_contract

        if not mode_allows_write(validation_mode):
            c = mode_contract(validation_mode)
            out["execute_unlocked"] = False
            out["write_refused"] = True
            out.setdefault("warnings", [])
            if isinstance(out["warnings"], list):
                out["warnings"].append(
                    f"Mode `{c['id']}` refuses destination writes — "
                    f"{c['non_guarantees'][0]}"
                )
    except Exception as mode_exc:
        logger.debug("validation mode stamp side-effects skipped: %s", mode_exc)

    return apply_root_causes_to_preflight(apply_readiness_honesty_caps(out))


# --------------------------------------------------------------------------- #
# Destination probe / shape inspection lives in its own module (Phase F8 size
# freeze) and is re-exported here so every existing import keeps working.
# --------------------------------------------------------------------------- #
from services.preflight_destination import (  # noqa: E402,F401
    _available_staging_bytes,
    inspect_destination_for_preflight,
    probe_destination,
)
