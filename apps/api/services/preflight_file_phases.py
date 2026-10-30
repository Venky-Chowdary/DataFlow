"""Cohesive phases of ``run_file_preflight`` (Phase F8 size freeze).

Each function owns one contiguous block of the original function body and takes
its inputs explicitly. They mutate the preflight ``out`` payload in place,
exactly as the inline code did.
"""

from __future__ import annotations

import logging
from typing import Any

from services.preflight_file_plan import _drift_column_phrase
from services.preflight_fk_gate import build_fk_block
from services.preflight_source_catalog import load_source_foreign_keys

logger = logging.getLogger(__name__)


def _dest_probe_config(destination_config: Any) -> dict[str, Any]:
    """The destination config with its TLS / driver keywords on top level."""
    from connectors.generic_sql import with_connection_options

    return with_connection_options(dict(destination_config or {}))


def apply_constraint_findings(
    out: dict[str, Any],
    ctx: Any,
    *,
    validation_mode: str,
    fk_risk_acknowledged: bool,
    destination_foreign_keys: list[dict[str, Any]] | None,
    source_config: dict[str, Any] | None,
    source_connector_id: str,
    source_table: str,
    sample_rows: list[dict] | None,
    mappings: list[dict[str, Any]],
    run_population_orphan_scan: bool,
    destination_check_constraints: list[Any] | None,
    source_kind: str,
    source_format: str,
    column_types: dict[str, str],
    destination_column_types: dict[str, str] | None,
    destination_db_type: str,
    sync_mode: str,
    acknowledgment_actor: str,
    acknowledgment_reason: str,
    dest_kind: str,
    destination_config: Any = None,
    destination_connector_id: str = "",
) -> None:
    """FK / CHECK constraint findings, sample + population orphan probes (D112)."""
    from services.preflight_rules import enrich_blockers

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
        src_fks: list[dict[str, Any]] = []
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

        # A destination FK names a parent table that lives in the
        # destination, so its parent keys are looked up there. Probing the
        # source for it failed closed on every source that lacks the parent
        # ("no such table"), even when every key is present (D112-FK).
        probe_on_dest = probe_fks is not src_fks and bool(
            destination_config or destination_connector_id
        )
        orphan_report = probe_sample_fk_orphans(
            sample_rows=sample_rows,
            mappings=list(mappings or []),
            foreign_keys=probe_fks,
            source_connector_id=(
                (destination_connector_id or "") if probe_on_dest
                else (source_connector_id or "")
            ),
            source_config=(
                _dest_probe_config(destination_config) if probe_on_dest else source_config
            ),
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

        # D112 — destination CHECK constraints vs the source population.
        # inspect_destination_for_preflight surfaces the live catalog; the
        # probe evaluates it over the same bounded population walk g3f uses
        # (posted sample when the source cannot re-read) and names offenders
        # before Confirm instead of a raw driver error after it.
        check_report: dict[str, Any] = {
            "ran": False,
            "note": "No destination CHECK constraints reported.",
        }
        if destination_check_constraints:
            from services.dest_check_probe import (
                check_constraint_source_fields,
                probe_dest_check_constraints,
            )
            from services.preflight_population import (
                _iter_table_population_for_preflight,
            )

            check_rows = _iter_table_population_for_preflight(
                source_columns=check_constraint_source_fields(
                    list(destination_check_constraints), list(mappings or [])
                ),
                source_kind=source_kind,
                source_format=source_format,
                source_connector_id=source_connector_id or "",
                source_config=source_config,
                source_table=source_table or "",
                mappings=list(mappings or []),
                column_types=dict(column_types or {}),
                dest_types=dict(destination_column_types or {}),
                dest_db=destination_db_type,
                sync_mode=sync_mode,
            )
            check_report = probe_dest_check_constraints(
                rows=check_rows if check_rows is not None else (sample_rows or []),
                mappings=list(mappings or []),
                check_constraints=list(destination_check_constraints),
                coverage=(
                    "population_check_probe"
                    if check_rows is not None
                    else "sample_check_probe"
                ),
            )
            viol = int(check_report.get("violating_count") or 0)
            if viol:
                # The destination refuses these rows in every validation mode:
                # a warning under balanced let the run start and fail after
                # Confirm on the raw CheckViolation (or, while overwrite still
                # rebuilt the table, land the row with the CHECK gone — D195).
                sev = "error"
                offenders = "; ".join(
                    f"row {v.get('row_index')}: {v.get('constraint')} "
                    f"{v.get('values')}"
                    for v in (check_report.get("violations") or [])[:3]
                )
                check_msg = (
                    f"{viol} source row(s) violate destination CHECK "
                    f"constraint(s) — {offenders}. "
                    "The destination rejects them, so the run would fail "
                    "after Confirm; fix the source rows or map them out."
                )
                findings.append(
                    {
                        "code": "dest_check_violation",
                        "severity": sev,
                        "coverage": check_report.get("coverage"),
                        "population_proof": bool(
                            check_report.get("population_proof")
                        ),
                        "violating_rows": viol,
                        "message": check_msg,
                    }
                )
                if sev == "error":
                    # A finding alone refused nothing: strict runs wrote the
                    # rows and failed on the driver's raw CheckViolation
                    # after Confirm (D112-CHECK).
                    check_details = {
                        "coverage": check_report.get("coverage"),
                        "violating_count": viol,
                        "violations": list(check_report.get("violations") or [])[:3],
                    }
                    out["gates"] = [
                        *out["gates"],
                        {
                            "id": "constraint_check",
                            "status": "block",
                            "message": check_msg,
                            "duration_ms": 0,
                            "details": check_details,
                        },
                    ]
                    out["blockers"] = [
                        *out["blockers"],
                        *enrich_blockers(
                            [
                                {
                                    "id": "constraint_check",
                                    "message": check_msg,
                                    "details": check_details,
                                }
                            ],
                            dest_kind=dest_kind,
                            validation_mode=validation_mode,
                        ),
                    ]
                    out["passed"] = False
        out["dest_check_probe"] = {
            k: check_report.get(k)
            for k in (
                "ran", "coverage", "population_proof", "evaluated",
                "not_evaluated", "violating_count", "violations",
                "checks", "note",
            )
            if k in check_report
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


def apply_schema_drift_gate(
    out: dict[str, Any],
    drift: dict[str, Any],
    *,
    schema_policy: str,
    schemaless: bool,
    schema_drift_acknowledged: bool,
    acknowledgment_actor: str,
    acknowledgment_reason: str,
    mappings: list[dict[str, Any]],
    columns: list[str],
    column_types: dict[str, str],
    dest_kind: str,
    validation_mode: str,
) -> None:
    """Schema-drift gate: pause, propagate, acknowledge, or note (D20)."""
    from services.preflight_rules import enrich_blockers

    # Schema drift is its own rule — never masquerade as Target DDL.
    # Datawrap rule: hard-breaking ALWAYS pauses (even under propagate_*).
    # Propagate auto-applies additive; manual_review keeps existing mappings.
    evolution = drift.get("schema_evolution") or {}
    column_phrase = _drift_column_phrase(evolution)
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
        elif evolution.get("should_pause") and evolution.get("hard_breaking"):
            hard = evolution.get("hard_breaking") or []
            if hard and isinstance(hard[0], dict) and hard[0].get("kind"):
                drift_msg = (
                    f"Breaking schema change: {hard[0].get('kind')} "
                    "— sync paused for review (Datawrap fail-closed)"
                    f"{column_phrase}"
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
            if schema_drift_acknowledged and drift.get("saved_contract", True):
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
                from services.preflight_policy_gates import finalize_acknowledged_schema_drift

                finalize_acknowledged_schema_drift(
                    out,
                    evolution=evolution if isinstance(evolution, dict) else {},
                    acknowledgment=ack,
                )
            else:
                from services.schema_drift import schema_review_copy

                review = schema_review_copy(
                    drift, policy=policy, column_phrase=column_phrase
                )
                drift_msg = str(review["message"])
                drift_gate = {
                    "id": str(review["gate_id"]),
                    "status": "block",
                    "message": drift_msg,
                    "duration_ms": 0,
                    "details": {
                        "issues": list(drift.get("issues") or []),
                        "severity": evolution.get("severity") or "warning",
                        "schema_policy": policy,
                        "schema_evolution": evolution,
                        "rule_id": review["rule_id"],
                        "remediation_kind": review["remediation_kind"],
                        "ack_required": bool(review["ack_required"]),
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
