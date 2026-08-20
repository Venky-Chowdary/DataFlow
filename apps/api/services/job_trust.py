"""Composite per-job trust score — completeness · quarantine · reconcile · freshness.

Honesty
-------
- Score is an operator signal, not a certificate of exactly-once delivery.
- Missing Gate-8 / lag evidence lowers confidence and redistributes weights.
- CDC path remains at-least-once upsert unless PK + ``_df_lsn`` is proven.
"""

from __future__ import annotations

from typing import Any

from services.reconcile_coverage import NO_OP_DEST_UNCHANGED

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


def is_append_delta_proof(recon: dict[str, Any] | None) -> bool:
    """True when Gate-8 closed on dest-before delta, not comparable whole-table hashes.

    Matches Gate-8 UI ``isGate8AppendDelta``: coverage/phase ``row_count``
    alone is not enough — overwrite conservation fixtures reuse that phase.
    """
    if not isinstance(recon, dict) or not recon:
        return False
    scope = str(recon.get("checksum_scope") or "").strip().lower()
    if scope == "whole_table_not_comparable":
        return True
    coverage = str(recon.get("assurance_level") or recon.get("coverage") or "").strip().lower()
    phase = str(recon.get("phase") or "").strip().lower()
    src = str(recon.get("source_checksum") or "").strip()
    tgt = str(recon.get("target_checksum") or "").strip()
    hashes_diverge = bool(src and tgt and src != tgt)
    return (
        (coverage == "row_count" or "post_write_row_count" in phase)
        and hashes_diverge
        and recon.get("checksum_match") is not True
    )


def has_full_checksum_proof(recon: dict[str, Any] | None) -> bool:
    """True only for independent source↔dest digest match (not writer-ack/sample)."""
    if not isinstance(recon, dict) or not recon:
        return False
    if is_append_delta_proof(recon):
        return False
    assurance = str(recon.get("assurance_level") or recon.get("coverage") or "").strip().lower()
    if assurance == "full_checksum":
        return True
    if assurance in {
        "row_count",
        "writer_ack",
        "sample",
        "write_pass_dest_readback",
        NO_OP_DEST_UNCHANGED,
        "none",
    }:
        return False
    provenance = str(recon.get("source_checksum_provenance") or "").strip().lower()
    if provenance in {"writer_ack", "write_pass_fingerprints"}:
        return False
    phase = str(recon.get("phase") or "").strip().lower()
    if "writer_ack" in phase or "sample" in phase or "skipped" in phase or "row_count" in phase:
        return False
    if recon.get("unproven") is True or recon.get("skipped_readback") is True:
        return False
    src = str(recon.get("source_checksum") or "").strip()
    tgt = str(recon.get("target_checksum") or "").strip()
    return bool(src and tgt and src == tgt and recon.get("passed") is True)


def _reconcile_factor(recon: dict[str, Any]) -> dict[str, Any]:
    """Gate-8 reconcile factor — never invent Verified from writer-ack / sample."""
    passed = recon.get("passed")
    phase = str(recon.get("phase") or "").lower()
    msg = str(recon.get("message") or "").lower()
    assurance = str(recon.get("assurance_level") or recon.get("coverage") or "").lower()
    src = str(recon.get("source_checksum") or "").strip()
    tgt = str(recon.get("target_checksum") or "").strip()
    unproven = (
        recon.get("unproven") is True
        or recon.get("skipped_readback") is True
        or "post_write_skipped" in phase
        or (
            assurance == "none"
            and ("file/object" in msg or "file export" in msg or "unproven" in msg)
        )
    )
    writer_ack = (
        assurance == "writer_ack"
        or "writer_ack" in phase
        or "verified by writer" in msg
        or "read-back verifier not available" in msg
        or (passed is True and bool(src) and not tgt and not unproven)
    )
    sample = (
        assurance == "sample"
        or "sample_verified" in phase
        or "sample-verified" in msg
    )
    pre_write = (
        recon.get("preview") is True
        or recon.get("post_write_pending") is True
        or "pre_write" in phase
        or "post_write_pending" in phase
    )

    fidelity = recon.get("row_fidelity_score")
    if isinstance(fidelity, (int, float)) and fidelity == fidelity:
        recon_score = max(0.0, min(100.0, float(fidelity) * (100.0 if float(fidelity) <= 1.0 else 1.0)))
        if float(fidelity) <= 1.0:
            recon_score = float(fidelity) * 100.0
    elif passed is False:
        recon_score = 18.0
    elif unproven or pre_write:
        # Operational / pending — not independent cell fidelity.
        recon_score = 45.0
    elif writer_ack:
        recon_score = 58.0
    elif assurance == "write_pass_dest_readback" or "write_pass" in phase:
        recon_score = 82.0
    elif sample:
        recon_score = 68.0
    elif is_append_delta_proof(recon):
        recon_score = 70.0
    elif has_full_checksum_proof(recon):
        recon_score = 100.0
    elif passed is True:
        # passed without stamped assurance — do not invent grade-A Verified.
        recon_score = 70.0
    else:
        recon_score = 70.0

    missing = int(recon.get("missing_key_count") or 0)
    extra = int(recon.get("extra_key_count") or 0)
    if passed is False:
        r_note = str(recon.get("message") or "Gate-8 reconcile failed.")
    elif unproven:
        r_note = (
            "Gate-8 cell fidelity unproven (file/object export or skipped read-back) "
            "— operational pass only."
        )
    elif pre_write:
        r_note = "Pre-write / pending Gate-8 — not independent post-write proof."
    elif writer_ack:
        r_note = "Writer acknowledgment only — independent read-back not captured."
    elif assurance == "write_pass_dest_readback" or "write_pass" in phase:
        r_note = (
            "Dest read-back matches the write-pass fingerprint — source warehouse "
            "was not independently re-read. Not migration_proven."
        )
    elif sample:
        r_note = "Sample-verified Gate-8 — not full independent checksum."
    elif is_append_delta_proof(recon):
        r_note = (
            "Gate-8 append delta verified — whole-table checksums are not "
            "comparable; per-cell fidelity is not proven."
        )
    elif missing or extra:
        r_note = f"Keys missing={missing} extra={extra}."
        recon_score = min(recon_score, 70.0)
    elif has_full_checksum_proof(recon):
        r_note = "Gate-8 full checksum reconcile passed."
    elif passed is True:
        r_note = "Gate-8 passed without full_checksum assurance — incomplete proof."
    else:
        r_note = "Gate-8 reconcile pending."

    return {
        "id": "reconcile",
        "label": "Reconcile",
        "score": recon_score,
        "weight": 0.30,
        "note": r_note,
        "present": True,
    }


def _quarantine_closure(job: dict[str, Any]) -> dict[str, Any] | None:
    raw = job.get("quarantine_closure")
    if isinstance(raw, dict) and raw:
        return raw
    dest = job.get("destination_summary")
    if isinstance(dest, dict):
        stored = dest.get("quarantine_closure")
        if isinstance(stored, dict) and stored:
            return stored
    return None


def open_quarantine_count(job: dict[str, Any] | None) -> float:
    """Hold-outs still in the replay set.

    Historical ``rejected_rows`` is the original transfer's census — after
    Promote/Replay the operator signal is *open* count. Using the historical
    figure forever is the Airbyte DLQ lie this product refuses.
    """
    j = job if isinstance(job, dict) else {}
    closure = _quarantine_closure(j)
    if isinstance(closure, dict) and closure.get("open_count") is not None:
        return _num(closure.get("open_count"), 0)
    rejected = _num(j.get("rejected_rows"), 0)
    if rejected <= 0:
        rejected = _num((j.get("destination_summary") or {}).get("rejected_rows"), 0)
    return rejected


def compute_job_trust(job: dict[str, Any] | None) -> dict[str, Any]:
    """Compute a 0–100 trust score from persisted job fields."""
    j = job if isinstance(job, dict) else {}
    status = str(j.get("status") or "").strip().lower()
    processed = _num(j.get("records_processed"), 0)
    historical_rejected = _num(j.get("rejected_rows"), 0)
    if historical_rejected <= 0:
        historical_rejected = _num((j.get("destination_summary") or {}).get("rejected_rows"), 0)
    rejected = open_quarantine_count(j)
    coerced = _num(j.get("coerced_null_rows"), 0)
    if coerced <= 0:
        coerced = _num((j.get("destination_summary") or {}).get("coerced_null_rows"), 0)
    recon = j.get("reconciliation") if isinstance(j.get("reconciliation"), dict) else {}
    lag = j.get("cdc_lag_seconds")
    lease_conflict = bool(j.get("cdc_lease_conflict"))
    from services.cdc_cursor_gap import job_has_cursor_gap

    cursor_gap = job_has_cursor_gap(j)
    snapshot_mode = str(j.get("snapshot_mode") or "").strip()
    if not snapshot_mode and isinstance(j.get("snapshot_plan"), dict):
        snapshot_mode = str(j["snapshot_plan"].get("snapshot_mode") or "")
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
        # Terminal success is not a certificate. Completeness tracks Gate-8 depth.
        if recon and has_full_checksum_proof(recon):
            outcome = 100.0
            outcome_note = "Terminal success — Gate-8 independent checksum."
        elif recon and str(recon.get("assurance_level") or "").lower() == "writer_ack":
            outcome = 82.0
            outcome_note = "Terminal success — Gate-8 writer acknowledgment only."
        elif recon:
            outcome = 88.0
            outcome_note = "Terminal success — Gate-8 not independent full_checksum."
        else:
            outcome = 82.0
            outcome_note = "Terminal success — Gate-8 reconcile not on this job yet."
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

    # Quarantine / violation rate — open hold-outs, not historical rejects.
    denom = max(processed, rejected, historical_rejected, 1)
    reject_rate = min(1.0, rejected / denom)
    quarantine_score = max(0.0, 100.0 - reject_rate * 400.0)
    closure = _quarantine_closure(j)
    verdict = str((closure or {}).get("verdict") or "")
    if rejected <= 0 and verdict == "closed":
        quarantine_score = 100.0
        q_note = (
            f"{int(historical_rejected):,} original hold-out(s) remediations landed "
            "(child Gate-8) — not a rewrite of the parent checksum."
        )
    elif rejected <= 0:
        quarantine_score = 100.0
        q_note = "No quarantined rows."
    else:
        q_note = f"{int(rejected):,} open quarantined ({reject_rate * 100:.1f}% of processed)."
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

    # Gate-8 reconcile — writer_ack / sample / unproven never score as Verified.
    if recon:
        factors.append(_reconcile_factor(recon))
    else:
        factors.append({
            "id": "reconcile",
            "label": "Reconcile",
            "score": None,
            "weight": 0.30,
            "note": "No Gate-8 report on this job yet.",
            "present": False,
        })

    # Freshness (CDC lag) — byte lag + proven seconds; never heartbeat invent.
    from services.cdc_lag_honesty import BYTE_CRITICAL, BYTE_WARN, observe_cdc_lag

    lag_bytes = j.get("replication_lag_bytes")
    lag_basis = str(j.get("cdc_lag_basis") or "")
    try:
        lag_bytes_i = int(lag_bytes) if lag_bytes is not None and str(lag_bytes) != "" else None
    except (TypeError, ValueError):
        lag_bytes_i = None
    lag_f = float(lag) if lag is not None and str(lag) != "" and _num(lag, -1) >= 0 else None
    obs = observe_cdc_lag(
        last_event_commit_at=None,
        replication_lag_bytes=lag_bytes_i,
    )
    # Prefer job-stamped seconds when present; fold byte severity.
    if lag_f is not None or lag_bytes_i is not None:
        sev = str(obs.get("freshness_severity") or "unknown")
        if lag_f is not None:
            if lag_f <= 60:
                fresh_score = 100.0
            elif lag_f >= 600:
                fresh_score = 0.0
            else:
                fresh_score = max(0.0, 100.0 * (1.0 - (lag_f - 60.0) / 540.0))
            note = f"CDC lag {lag_f:.1f}s (basis={lag_basis or obs.get('cdc_lag_basis') or 'seconds'})."
        else:
            # Seconds unknown — score from bytes only.
            if lag_bytes_i is not None and lag_bytes_i <= (1 * 1024 * 1024):
                fresh_score = 100.0
                note = f"CDC caught up on WAL/binlog ({lag_bytes_i:,} B)."
            elif lag_bytes_i is not None and lag_bytes_i >= BYTE_CRITICAL:
                fresh_score = 0.0
                note = f"CDC WAL/binlog lag critical ({lag_bytes_i:,} B)."
            elif lag_bytes_i is not None and lag_bytes_i >= BYTE_WARN:
                fresh_score = 35.0
                note = f"CDC WAL/binlog lag warn ({lag_bytes_i:,} B)."
            else:
                fresh_score = None
                note = "CDC lag unknown — heartbeat is not catch-up proof."
                sev = "unknown"
        if sev == "critical" and fresh_score is not None:
            fresh_score = min(fresh_score, 15.0)
        elif sev == "warn" and fresh_score is not None:
            fresh_score = min(fresh_score, 55.0)
        factors.append({
            "id": "freshness",
            "label": "Freshness",
            "score": fresh_score,
            "weight": 0.10,
            "note": note,
            "present": fresh_score is not None,
        })
    else:
        factors.append({
            "id": "freshness",
            "label": "Freshness",
            "score": None,
            "weight": 0.10,
            "note": "No proven CDC lag on this job (batch or unknown).",
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
                    "purged-window events are gone. when_needed Resume snapshots "
                    "current source keys then streams from the new tip; initial/never "
                    "stay fail-closed. Not continuous CDC, not migration_proven."
                )

    # Never grade-A without independent full checksum proof (enterprise honesty).
    if not recon:
        score = min(score, 84.0)
    elif not has_full_checksum_proof(recon):
        score = min(score, 89.0)

    if source_ha_role in {"SECONDARY", "PHYSICAL_STANDBY", "LOGICAL_STANDBY", "SNAPSHOT_STANDBY"}:
        # Reading from a standby is unusual for CDC capture — surface in confidence.
        for f in factors:
            if f["id"] == "freshness":
                f["note"] = (
                    (f.get("note") or "")
                    + f" Source HA role={source_ha_role} — prefer PRIMARY/listener for capture."
                ).strip()

    # Confidence from evidence coverage
    sum(1 for f in factors if f.get("present") is True or f["id"] in {"completeness", "quarantine", "coercion"})
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
        snapshot_mode=snapshot_mode,
        rejected=rejected,
        recon=recon if recon else None,
        quarantine_verdict=verdict,
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
    snapshot_mode: str = "",
    rejected: float,
    recon: dict[str, Any] | None = None,
    quarantine_verdict: str = "",
) -> dict[str, str]:
    if cursor_gap:
        from services.cdc_snapshot_mode import snapshot_mode_recovers_gap

        mode = str(snapshot_mode or "").strip().lower().replace("-", "_")
        if snapshot_mode_recovers_gap(mode):
            return {
                "code": "cursor_gap",
                "label": "Resume — engine will snapshot",
                "detail": (
                    "Purged-window events are gone. Resume re-upserts current source keys, "
                    "then streams from the new tip. Not continuous CDC. Not migration_proven."
                ),
            }
        if mode == "never":
            return {
                "code": "cursor_gap",
                "label": "Set snapshot when_needed",
                "detail": (
                    "snapshot_mode=never forbids a recovery snapshot. Change the mode, then Resume. "
                    "Purged-window events are gone."
                ),
            }
        return {
            "code": "cursor_gap",
            "label": "Reset CDC watermark",
            "detail": (
                "snapshot_mode=initial will not snapshot again. Reset the cursor or set when_needed, "
                "then re-run. Purged-window events are gone."
            ),
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
    if quarantine_verdict == "closed" and rejected <= 0:
        if wid in {"quarantine", "completeness"}:
            return {
                "code": "quarantine_closed",
                "label": "Quarantine ledger closed",
                "detail": (
                    "Remediations landed with child Gate-8. Parent checksum is "
                    "historical — not migration_proven."
                ),
            }
    if wid == "quarantine" or rejected > 0 and float(weakest["score"]) < 90:
        return {
            "code": "quarantine",
            "label": "Review quarantine",
            "detail": "Replay or export remaining open rows — nothing was silently dropped.",
        }
    if wid == "reconcile":
        if recon and recon.get("passed") is True and is_append_delta_proof(recon):
            return {
                "code": "append_delta",
                "label": "Append delta closed — not a dest replace",
                "detail": (
                    "Dest grew by this run. Overwrite to replace existing rows, "
                    "or add a PK and upsert."
                ),
            }
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
