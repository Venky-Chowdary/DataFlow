"""Create-new type risk stamping — one owner for the projected → physical step.

Extracted from ``semantic_mapper`` (Phase F8 god-module decomposition). The
stamp replaces a mapping's projected carrier with the destination's own physical
DDL and records what that costs, so Map, Validate and Pilot all read the same
risk from the same place.

It deliberately does not import ``mapping_pipeline``: the pipeline calls this,
not the other way round.
"""

from __future__ import annotations

# Warn-only chips that name a destination *domain* the write already
# quarantines. They must stay visible on Map, but they are not a mapping-
# identity question and must not drop G4 under the confidence floor.
# A sampled value already outside the window is severity=block and locks.
# ``uuid_carrier_equivalent`` / ``fixed_width_not_enforced`` name a domain the
# destination engine cannot declare in *any* carrier it can spell (SQLite's one
# untyped TEXT affinity). Every value round-trips exactly, and no remap the
# operator could choose enforces more — so they are stated, not held.
_INFORMATIONAL_WARN_KINDS = frozenset({
    "instant_range_cap",
    "uuid_carrier_equivalent",
    "fixed_width_not_enforced",
})


def create_new_risk_locks_review(risk: dict) -> bool:
    """True when a create-new chip must hold Map/Validate until the operator acts.

    ``instant_range_cap`` at ``warn`` is the MySQL TIMESTAMP 1970..2038 ceiling
    with no out-of-window sample — the instant is kept, out-of-range rows
    quarantine, and auto-map Execute must not die as ``g4_mapping_confidence``.
    The same kind at ``block`` (a sampled year past 2038) still locks.
    """
    kind = str((risk or {}).get("kind") or "")
    severity = str((risk or {}).get("severity") or "warn").lower()
    if severity == "block":
        return True
    if kind in _INFORMATIONAL_WARN_KINDS:
        return False
    return True


def apply_create_new_risk_stamps(
    mappings: list[dict],
    destination_db_type: str = "",
    *,
    source_samples: dict[str, list] | None = None,
    dest_table_exists: bool | None = None,
    source_db_type: str = "",
) -> list[dict]:
    """Stamp create-new type risks without importing mapping_pipeline (cycle-safe)."""
    # Deferred: semantic_mapper imports this module, so binding its calibration
    # helper at import time would close the cycle.
    from services.semantic_mapper import (
        IDENTITY_PASSTHROUGH_CONFIDENCE,
        _calibrated_confidence,
    )
    from services.mapping_proof import mapping_fidelity
    from services.decimal_observe import (
        ieee_float_create_new_risk,
        observe_numeric_samples,
    )
    from services.decision_kernel import (
        create_new_mapping_target_type,
        is_lossy_coercion,
        is_precision_collapse_coercion,
        normalize_logical_type as _nlt,
    )
    from services.type_system import (
        assess_create_new_type_risk,
        reinvent_would_drop_dest_instant_carrier,
    )

    samples_by_src = source_samples or {}
    out: list[dict] = []
    for m in mappings:
        row = dict(m)
        # Only confirmed create-new strategies — never stamp pending schema as
        # create-new (UI keeps createNew=false; inventing risks/DDL contradicts that).
        strategy = str(row.get("assignment_strategy") or "")
        is_create = bool(
            row.get("create_new")
            or strategy in {
                "create_compatible_new",
                "identity_passthrough",
            }
        )
        if not is_create or strategy == "pending_dest_schema":
            out.append(row)
            continue
        src = str(row.get("source_type") or "VARCHAR")
        db = destination_db_type or str(row.get("dest_db_type") or "")
        stamped = str(row.get("target_type") or "").strip()
        col_samples = samples_by_src.get(str(row.get("source") or "")) or None
        physical_from_src = (
            create_new_mapping_target_type(
                src, db, samples=col_samples, source_db=source_db_type
            )
            if db
            else ""
        )
        if db and stamped:
            physical_from_stamp = create_new_mapping_target_type(
                stamped, db, samples=col_samples, source_db=source_db_type
            )
            if reinvent_would_drop_dest_instant_carrier(
                stamped, physical_from_stamp, dest_db=db
            ):
                # The stamp is already the destination's own physical carrier;
                # re-invent read it as a dialect-less source token and dropped
                # the instant it declares (MySQL TIMESTAMP(6) → DATETIME(6)).
                physical_from_stamp = stamped
            stamp_l = _nlt(stamped)
            src_phys_l = _nlt(physical_from_src or src)
            if physical_from_src and stamp_l == "float" and _nlt(src) == "float":
                # Re-inventing a float stamp re-reads a physical token as a
                # logical one and drops the width it carried: MySQL FLOAT is a
                # 24-bit mantissa, but fed back through invent it returns DOUBLE
                # because a bare FLOAT declares no width. Only the source says
                # whether single precision was ever declared, so it wins here.
                # Other logical types keep the stamp, which may carry
                # sample-observed precision the source token does not.
                tgt = physical_from_src
            # Transform/pipeline may widen VARCHAR→DATETIME(6)/JSONB/UUID wire.
            # Never erase that with source-derived TEXT create-new.
            elif stamp_l not in {"string", "text"} or src_phys_l not in {"string", "text"}:
                tgt = physical_from_stamp or stamped
            else:
                tgt = physical_from_src or physical_from_stamp or stamped
        elif db:
            tgt = physical_from_src or src
        else:
            tgt = stamped or src
        if tgt and tgt != stamped:
            row["target_type"] = tgt
            # The projected carrier just became the destination's physical DDL
            # (``TIMESTAMPTZ`` → SQL Server ``DATETIMEOFFSET``). Any verdict
            # stamped against the old spelling compared a source-dialect token
            # to a foreign dialect, so it read offset-pinned → session-relative
            # as a collapse. The calibration below reads ``fidelity``, so a
            # stale verdict caps a lossless create-new under the G4 floor and
            # demands a Risk Contract. Re-derive on the type that will run.
            verdict = mapping_fidelity(
                row,
                destination_db_type=db,
                dest_table_exists=dest_table_exists,
            )
            row["fidelity"] = verdict["verdict"]
            row["fidelity_reason"] = verdict["reason"]
            row["type_narrowing"] = verdict["type_narrowing"]
            row["conversion_class"] = verdict.get("conversion_class")
            row["invents_capacity"] = verdict.get("invents_capacity")
            row["requires_risk_contract"] = verdict.get("requires_risk_contract")
        risks = assess_create_new_type_risk(
            src, tgt, destination_db_type=db, samples=col_samples
        )
        ieee = ieee_float_create_new_risk(observe_numeric_samples(col_samples))
        if ieee and not any(r.get("kind") == "ieee_float_artifact" for r in risks):
            risks = list(risks) + [ieee]
        locking_risks = [r for r in risks if create_new_risk_locks_review(r)] if risks else []
        if risks:
            row["create_new_risks"] = risks
            kinds = ", ".join(sorted({r.get("kind", "") for r in risks if r.get("kind")}))
            reason = str(row.get("reasoning") or "")
            note = f"create-new type risk: {kinds}"
            if note not in reason.lower():
                row["reasoning"] = f"{reason} · {note}".strip(" ·")
        if locking_risks:
            row["requires_review"] = True
            if (
                (not row.get("fidelity") or row.get("fidelity") == "lossless")
                and (
                    is_lossy_coercion(src, tgt, dest_db=db)
                    or is_precision_collapse_coercion(src, tgt, dest_db=db)
                )
            ):
                row["fidelity"] = "lossy_cast"
            # Vary confidence: lossy create-new must not look like identity slam-dunk.
            try:
                base = float(row.get("confidence") or IDENTITY_PASSTHROUGH_CONFIDENCE)
            except (TypeError, ValueError):
                base = IDENTITY_PASSTHROUGH_CONFIDENCE
            row["confidence"] = _calibrated_confidence(
                base,
                score_gap=float(row.get("score_gap") or 0.0),
                requires_review=True,
                hard_cap=0.88,
                fidelity=str(row.get("fidelity") or ""),
            )
        elif strategy == "create_compatible_new":
            fid = str(row.get("fidelity") or "").strip().lower()
            # Lossless ADD COLUMN (INTEGER→INTEGER) — Approve-eligible, not 70% spam.
            # Still not silent Ready; operator confirms invent onto existing table.
            if fid in {"preserve", "lossless"} and not risks:
                row["requires_review"] = False
                row["mapping_class"] = "equivalent_add_column"
                try:
                    base = float(row.get("confidence") or 0.93)
                except (TypeError, ValueError):
                    base = 0.93
                # Cap under dest-proven identity (~0.95+) — ADD COLUMN is projected.
                row["confidence"] = round(min(0.93, max(base, 0.90)), 3)
            else:
                # Existing-table ADD COLUMN invent must stay under auto-approve (~0.85).
                row["requires_review"] = True
                try:
                    base = float(row.get("confidence") or IDENTITY_PASSTHROUGH_CONFIDENCE)
                except (TypeError, ValueError):
                    base = IDENTITY_PASSTHROUGH_CONFIDENCE
                row["confidence"] = _calibrated_confidence(
                    base,
                    score_gap=float(row.get("score_gap") or 0.0),
                    requires_review=True,
                    hard_cap=0.84,
                    fidelity=fid,
                )
        elif strategy == "identity_passthrough":
            fid = str(row.get("fidelity") or "").strip().lower()
            # A warn-only domain chip (MySQL TIMESTAMP 1970..2038) is not a
            # fidelity verdict. Empty fidelity plus no locking risk is the
            # same as preserve — Map still shows the chip from create_new_risks.
            if not fid and not locking_risks:
                fid = "preserve"
            # Equivalent create-new (preserve) — high type certainty, still not
            # silent Ready: UI Approve / Approve-eligible; no Risk Contract spam.
            if fid in {"preserve", "lossless"}:
                row["requires_review"] = False
                row["mapping_class"] = "equivalent_create_new"
                try:
                    base = float(row.get("confidence") or 0.95)
                except (TypeError, ValueError):
                    base = 0.95
                row["confidence"] = round(min(0.97, max(base, 0.95)), 3)
            else:
                # Projected CREATE with cast/mutate risk — stay under G4 floor.
                row["requires_review"] = True
                try:
                    base = float(row.get("confidence") or IDENTITY_PASSTHROUGH_CONFIDENCE)
                except (TypeError, ValueError):
                    base = IDENTITY_PASSTHROUGH_CONFIDENCE
                row["confidence"] = _calibrated_confidence(
                    base,
                    score_gap=float(row.get("score_gap") or 0.0),
                    requires_review=True,
                    hard_cap=0.84,
                    fidelity=fid,
                )
        out.append(row)
    return out
