"""Composite per-job trust score — completeness · quarantine · reconcile · freshness.

Honesty
-------
- Score is an operator signal, not a certificate of exactly-once delivery.
- Missing Gate-8 / lag evidence lowers confidence and redistributes weights.
- CDC path remains at-least-once upsert unless PK + ``_df_lsn`` is proven.
"""

from __future__ import annotations

from typing import Any

_TERMINAL = frozenset({
    "completed",
    "completed_with_quarantine",
    "failed",
    "cancelled",
    "success",
    "error",
})


def is_terminal_status(status: str | None) -> bool:
    return str(status or "").strip().lower() in _TERMINAL


def compute_job_trust(job: dict[str, Any] | None) -> dict[str, Any]:
    """Compute a 0–100 trust score from persisted job fields."""
    j = job if isinstance(job, dict) else {}
    status = str(j.get("status") or "").strip().lower()
    processed = _num(j.get("records_processed"), 0)
    rejected = _num(j.get("rejected_rows"), 0)
    if rejected <= 0:
        rejected = _num((j.get("destination_summary") or {}).get("rejected_rows"), 0)
    coerced = _num(j.get("coerced_null_rows"), 0)
    if coerced <= 0:
        coerced = _num((j.get("destination_summary") or {}).get("coerced_null_rows"), 0)
    recon = j.get("reconciliation") if isinstance(j.get("reconciliation"), dict) else {}
    lag = j.get("cdc_lag_seconds")
    lease_conflict = bool(j.get("cdc_lease_conflict"))
    cursor_gap = bool(j.get("cdc_cursor_gap")) or str(j.get("error_code") or "") in {
        "cdc_cursor_gap",
        "cdc_lsn_gap",
        "cdc_scn_gap",
    }
    source_ha_role = str(j.get("source_ha_role") or "").strip().upper() or None

    factors: list[dict[str, Any]] = []

    # Outcome / completeness
    if status in {"failed", "error"}:
        outcome = 12.0 if processed > 0 else 0.0
        outcome_note = "Transfer failed — rows may be partial; fix cause before Resume."
    elif status == "cancelled":
        outcome = 35.0
        outcome_note = "Cancelled before completion."
    elif status == "completed_with_quarantine":
        outcome = 78.0
        outcome_note = "Completed with quarantine — not full fidelity."
    elif status in {"completed", "success"}:
        outcome = 100.0
        outcome_note = "Terminal success."
    else:
        outcome = 55.0
        outcome_note = "In progress — score is provisional."
    factors.append({
        "id": "completeness",
        "label": "Completeness",
        "score": outcome,
        "weight": 0.25,
        "note": outcome_note,
    })

    # Quarantine / violation rate
    denom = max(processed, rejected, 1)
    reject_rate = min(1.0, rejected / denom)
    quarantine_score = max(0.0, 100.0 - reject_rate * 400.0)
    if rejected <= 0:
        quarantine_score = 100.0
        q_note = "No quarantined rows."
    else:
        q_note = f"{int(rejected):,} quarantined ({reject_rate * 100:.1f}% of processed)."
    factors.append({
        "id": "quarantine",
        "label": "Quarantine",
        "score": quarantine_score,
        "weight": 0.25,
        "note": q_note,
    })

    # Coercion fidelity
    coerce_rate = min(1.0, coerced / max(processed, 1))
    coerce_score = max(0.0, 100.0 - coerce_rate * 200.0) if coerced > 0 else 100.0
    factors.append({
        "id": "coercion",
        "label": "Coercion",
        "score": coerce_score,
        "weight": 0.10,
        "note": (
            f"{int(coerced):,} rows with coerced nulls."
            if coerced > 0
            else "No coerced-null rows."
        ),
    })

    # Gate-8 reconcile
    if recon:
        passed = recon.get("passed")
        fidelity = recon.get("row_fidelity_score")
        if isinstance(fidelity, (int, float)) and fidelity == fidelity:
            recon_score = max(0.0, min(100.0, float(fidelity) * (100.0 if float(fidelity) <= 1.0 else 1.0)))
            if float(fidelity) <= 1.0:
                recon_score = float(fidelity) * 100.0
        elif passed is True:
            recon_score = 100.0
        elif passed is False:
            recon_score = 18.0
        else:
            recon_score = 70.0
        missing = int(recon.get("missing_key_count") or 0)
        extra = int(recon.get("extra_key_count") or 0)
        if passed is False:
            r_note = str(recon.get("message") or "Gate-8 reconcile failed.")
        elif missing or extra:
            r_note = f"Keys missing={missing} extra={extra}."
            recon_score = min(recon_score, 70.0)
        else:
            r_note = "Gate-8 reconcile passed."
        factors.append({
            "id": "reconcile",
            "label": "Reconcile",
            "score": recon_score,
            "weight": 0.30,
            "note": r_note,
            "present": True,
        })
    else:
        factors.append({
            "id": "reconcile",
            "label": "Reconcile",
            "score": None,
            "weight": 0.30,
            "note": "No Gate-8 report on this job yet.",
            "present": False,
        })

    # Freshness (CDC lag)
    if lag is not None and str(lag) != "" and _num(lag, -1) >= 0:
        lag_f = float(lag)
        if lag_f <= 60:
            fresh_score = 100.0
        elif lag_f >= 600:
            fresh_score = 0.0
        else:
            fresh_score = max(0.0, 100.0 * (1.0 - (lag_f - 60.0) / 540.0))
        factors.append({
            "id": "freshness",
            "label": "Freshness",
            "score": fresh_score,
            "weight": 0.10,
            "note": f"CDC lag {lag_f:.1f}s (warn 60s).",
            "present": True,
        })
    else:
        factors.append({
            "id": "freshness",
            "label": "Freshness",
            "score": None,
            "weight": 0.10,
            "note": "No CDC lag on this job (batch or not reported).",
            "present": False,
        })

    present = [f for f in factors if f.get("score") is not None]
    weight_sum = sum(float(f["weight"]) for f in present) or 1.0
    score = 0.0
    for f in present:
        score += float(f["score"]) * (float(f["weight"]) / weight_sum)
    score = max(0.0, min(100.0, score))

    if lease_conflict:
        score = min(score, 35.0)
        for f in factors:
            if f["id"] == "completeness":
                f["note"] = "CDC lease conflict — concurrent consumer blocked."

    if cursor_gap:
        score = min(score, 28.0)
        for f in factors:
            if f["id"] == "completeness":
                f["note"] = (
                    "CDC cursor gap (retention / AG·Data Guard failover class) — "
                    "reset watermark and re-snapshot; continuous CDC across the gap is not claimed."
                )

    if source_ha_role in {"SECONDARY", "PHYSICAL_STANDBY", "LOGICAL_STANDBY", "SNAPSHOT_STANDBY"}:
        # Reading from a standby is unusual for CDC capture — surface in confidence.
        for f in factors:
            if f["id"] == "freshness":
                f["note"] = (
                    (f.get("note") or "")
                    + f" Source HA role={source_ha_role} — prefer PRIMARY/listener for capture."
                ).strip()

    # Confidence from evidence coverage
    evidence = sum(1 for f in factors if f.get("present") is True or f["id"] in {"completeness", "quarantine", "coercion"})
    # always have completeness/quarantine/coercion; +1 reconcile +1 freshness
    covered = 3 + sum(1 for f in factors if f.get("present") is True)
    if covered >= 5:
        confidence = "high"
    elif covered >= 4:
        confidence = "medium"
    else:
        confidence = "low"

    grade = _grade(score)
    tone = "ok" if score >= 85 else "warn" if score >= 60 else "danger"
    if not is_terminal_status(status):
        tone = "muted"

    next_action = _next_action(
        factors,
        status=status,
        lease_conflict=lease_conflict,
        cursor_gap=cursor_gap,
        rejected=rejected,
    )

    return {
        "score": round(score),
        "grade": grade,
        "tone": tone,
        "confidence": confidence,
        "factors": [
            {
                "id": f["id"],
                "label": f["label"],
                "score": None if f.get("score") is None else round(float(f["score"])),
                "weight": f["weight"],
                "note": f["note"],
                "present": f.get("present", True),
            }
            for f in factors
        ],
        "next_action": next_action,
        "lease_conflict": lease_conflict,
        "cursor_gap": cursor_gap,
        "source_ha_role": source_ha_role,
    }


def attach_trust_to_updates(status: str, updates: dict[str, Any], *, previous: dict[str, Any] | None = None) -> dict[str, Any]:
    """Merge trust_score into terminal job updates (mutates and returns ``updates``)."""
    if not is_terminal_status(status):
        return updates
    merged = dict(previous or {})
    merged.update(updates)
    merged["status"] = status
    trust = compute_job_trust(merged)
    updates["trust_score"] = trust["score"]
    updates["trust"] = trust
    return updates


def _next_action(
    factors: list[dict[str, Any]],
    *,
    status: str,
    lease_conflict: bool,
    cursor_gap: bool,
    rejected: float,
) -> dict[str, str]:
    if cursor_gap:
        return {
            "code": "cursor_gap",
            "label": "Reset CDC watermark",
            "detail": "Clear the cursor, then re-run with snapshot when_needed or initial.",
        }
    if lease_conflict:
        return {
            "code": "lease",
            "label": "Resolve CDC lease",
            "detail": "Force-release or stop the holder, then Resume.",
        }
    if status in {"failed", "error"}:
        return {
            "code": "resume",
            "label": "Fix failure then Resume",
            "detail": "Use the failure hint and event log before retrying.",
        }
    present = [f for f in factors if f.get("score") is not None]
    if not present:
        return {"code": "inspect", "label": "Inspect job", "detail": "Open Job Theater for evidence."}
    weakest = min(present, key=lambda f: float(f["score"]))
    wid = weakest["id"]
    if wid == "quarantine" or rejected > 0 and float(weakest["score"]) < 90:
        return {
            "code": "quarantine",
            "label": "Review quarantine",
            "detail": "Replay or export rejected rows — nothing was silently dropped.",
        }
    if wid == "reconcile":
        return {
            "code": "reconcile",
            "label": "Investigate Gate-8",
            "detail": "Export proof JSON or re-run Validate after fixing drift.",
        }
    if wid == "freshness":
        return {
            "code": "freshness",
            "label": "Check CDC freshness",
            "detail": "Open the pipeline — lag may need capacity or lease attention.",
        }
    if wid == "coercion":
        return {
            "code": "map",
            "label": "Tighten mapping types",
            "detail": "Coerced nulls reduce fidelity — adjust Map / transforms.",
        }
    return {
        "code": "ok",
        "label": "Trust posture healthy",
        "detail": "No action required from composite factors.",
    }


def _grade(score: float) -> str:
    if score >= 90:
        return "A"
    if score >= 80:
        return "B"
    if score >= 70:
        return "C"
    if score >= 55:
        return "D"
    return "F"


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default
