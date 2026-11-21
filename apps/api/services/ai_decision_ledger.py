"""AI decisions ledger: every AI-originated proposal or explanation on a job, and what became of it.

One row per involvement, built from what the job and its evidence already
record — no model runs here, and nothing is inferred that a store does not say:

* **Mapping proposals** (deterministic mapper): the target each column ran
  with, whether a person changed it, who ran the job with it, and what the
  Mapping Witness found for that exact mapping.
* **Model mapping suggestions** (``engine_suggestion`` from the LLM refinement
  step): proposed, never applied by the engine; adopted only if the column ran
  with that target.
* **Witness flags** (deterministic): what the witness flagged and how the
  reviewer answered (kept or changed), by whom.
* **Parity explanation** (model or template wording of classifier output) and
  **parity fixes** (deterministic proposals a person approved or dismissed,
  verified by the re-check).
* **Model calls**: every AI-egress manifest the chain holds for the job
  (provider, purpose, digest of what crossed the boundary).

Fingerprints are SHA-256 over names, types and settings only: no cell value,
and no hash of a cell value (a hash of a first name is a dictionary lookup
away from the name). The ledger rides in the signed proof pack and the signed
Parity Studio pack (:mod:`services.signed_proof_pack`), so changing a row
breaks the signature.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

LEDGER_VERSION = "ai_decision_ledger.v1"

ORIGIN_DETERMINISTIC = "deterministic"
ORIGIN_MODEL = "model"

KIND_MAPPING_PROPOSAL = "mapping_proposal"
KIND_MODEL_SUGGESTION = "model_mapping_suggestion"
KIND_WITNESS_FLAG = "witness_flag"
KIND_PARITY_EXPLANATION = "parity_explanation"
KIND_PARITY_FIX = "parity_fix"
KIND_MODEL_CALL = "model_call"

_HONESTY = (
    "Built from the job's own records: the mapping it ran, the Mapping Witness report for that mapping, "
    "Parity Studio decisions and the AI-egress chain. Fingerprints cover names, types and settings, never values. "
    "A row without a check says 'not checked'; nothing is marked verified that a check did not confirm."
)


def _sha(body: Any) -> str:
    return hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def _entry(kind: str, subject: str, origin: dict[str, Any], *, inputs: Any, proposal: dict[str, Any],
           decision: dict[str, Any], verification: dict[str, Any]) -> dict[str, Any]:
    inputs_sha256 = _sha(inputs)
    return {
        "id": "aid_" + _sha([kind, subject, inputs_sha256])[:16],
        "kind": kind,
        "subject": subject,
        "origin": origin,
        "inputs_sha256": inputs_sha256,
        "proposal": proposal,
        "decision": decision,
        "verification": verification,
    }


def _lower(value: Any) -> str:
    return str(value or "").strip().lower()


def _witness_check(witness: dict[str, Any] | None, source: str) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """(verification, the witness pair) for one source column."""
    report = (witness or {}).get("report") or {}
    pair = next((p for p in report.get("pairs") or [] if p.get("source") == source), None)
    if not pair:
        return {"check": "none", "status": "not_checked",
                "detail": "No Mapping Witness report for this mapping."}, None
    flag = pair.get("flag") or {}
    detail = flag.get("reason") or pair.get("reason") or ""
    return {
        "check": "mapping_witness",
        "status": pair.get("status"),
        "mode": report.get("mode"),
        "detail": detail,
        "report_sha256": report.get("inputs_sha256"),
        "checked_at": report.get("generated_at"),
    }, pair


def _mapping_entries(job: dict[str, Any], witness: dict[str, Any] | None) -> list[dict[str, Any]]:
    from services.parity_evidence import mappings_of, request_of

    req = request_of(job)
    ran_by = str(req.get("triggered_by") or "") or None
    ran_at = job.get("started_at") or job.get("created_at")
    out: list[dict[str, Any]] = []
    for m in mappings_of(job):
        source = str(m.get("source") or "")
        target = str(m.get("target") or "")
        if m.get("intentional_omit") or m.get("intentionalOmit") or not target:
            continue
        overrides = m.get("user_overrides") if isinstance(m.get("user_overrides"), dict) else {}
        changed = bool(m.get("user_override") or overrides.get("user_override"))
        verification, pair = _witness_check(witness, source)
        if pair is None and not changed:
            verification = {**verification, "detail": "Accepted on human judgement; no data check ran."}
        inputs = {
            "source": source, "source_type": m.get("source_type") or m.get("inferred_type"),
            "target": target, "target_type": m.get("target_type") or m.get("dest_type"),
            "method": m.get("method") or m.get("assignment_strategy"), "create_new": bool(m.get("create_new")),
        }
        out.append(_entry(
            KIND_MAPPING_PROPOSAL, f"{source} → {target}",
            {"type": ORIGIN_DETERMINISTIC, "engine": "semantic_mapper",
             "method": str(m.get("method") or m.get("assignment_strategy") or "deterministic")},
            inputs=inputs,
            proposal={"target": target, "create_new": bool(m.get("create_new")),
                      "confidence": m.get("confidence")},
            decision={
                "status": "changed" if changed else "accepted",
                "by": ran_by,
                "at": ran_at,
                "how": "A person set this target in Map." if changed else "The job ran with this target.",
            },
            verification=verification,
        ))
        suggestion = m.get("engine_suggestion") if isinstance(m.get("engine_suggestion"), dict) else None
        if suggestion and (suggestion.get("target") or suggestion.get("transform")):
            suggested = str(suggestion.get("target") or target)
            same_transform = not suggestion.get("transform") or _lower(suggestion.get("transform")) == _lower(
                m.get("transform") or m.get("transformation"))
            adopted = _lower(suggested) == _lower(target) and same_transform
            held = bool(m.get("llm_invented_transform")) and not changed
            if pair and pair.get("status") == "witnessed" and _lower(suggested) != _lower(target):
                s_verify = {"check": "mapping_witness", "status": "contradicted",
                            "detail": f"The witness confirmed {target} on matching keys, not {suggested}."}
            elif pair and _lower(((pair.get("flag") or {}).get("suggested_target"))) == _lower(suggested):
                s_verify = {"check": "mapping_witness", "status": "supported",
                            "detail": f"The witness found the values in {suggested}."}
            else:
                s_verify = {"check": "none", "status": "not_checked",
                            "detail": "No data check judged this suggestion."}
            out.append(_entry(
                KIND_MODEL_SUGGESTION, f"{source} → {suggested}",
                {"type": ORIGIN_MODEL, "provider": suggestion.get("llm_provider") or "unrecorded",
                 "model": suggestion.get("llm_model") or "unrecorded"},
                inputs={**inputs, "suggested_target": suggested, "suggested_transform": suggestion.get("transform")},
                proposal={"target": suggested, "transform": suggestion.get("transform"),
                          "confidence": suggestion.get("confidence")},
                decision={
                    "status": "held" if held else ("accepted" if adopted else "rejected"),
                    "by": ran_by,
                    "at": ran_at,
                    "how": ("Held for review: a model-invented transform is never applied on its own."
                            if held else ("The column ran with the model's suggestion." if adopted
                                          else "The deterministic target ran; the suggestion was not adopted.")),
                },
                verification=s_verify,
            ))
    return out


def _witness_entries(witness: dict[str, Any] | None) -> list[dict[str, Any]]:
    from services.mapping_witness import WITNESS_VERSION, flagged_pairs

    if not witness:
        return []
    report = witness.get("report") or {}
    decisions = {d.get("source"): d for d in witness.get("decisions") or [] if isinstance(d, dict)}
    out: list[dict[str, Any]] = []
    for pair in flagged_pairs(report):
        flag = pair.get("flag") or {}
        review = decisions.get(pair["source"])
        out.append(_entry(
            KIND_WITNESS_FLAG, f"{pair['source']} → {pair['target']}",
            {"type": ORIGIN_DETERMINISTIC, "engine": report.get("version") or WITNESS_VERSION},
            inputs={"report_sha256": report.get("inputs_sha256"), "source": pair["source"],
                    "target": pair["target"], "kind": flag.get("kind")},
            proposal={"flag": flag.get("kind"), "reason": flag.get("reason"),
                      "suggested_target": flag.get("suggested_target")},
            decision=(
                {"status": "kept" if review.get("decision") == "kept" else "changed", "by": review.get("by"),
                 "at": review.get("at"),
                 "how": ("The reviewer kept the mapping." if review.get("decision") == "kept"
                         else f"The reviewer changed the target to {review.get('new_target') or 'another column'}.")}
                if review else {"status": "unreviewed", "by": None, "at": None, "how": "No one reviewed this flag."}
            ),
            verification={"check": "mapping_witness", "status": "flagged", "mode": flag.get("basis"),
                          "detail": pair.get("reason"), "report_sha256": report.get("inputs_sha256"),
                          "checked_at": report.get("generated_at")},
        ))
    return out


def _parity_entries(job: dict[str, Any], calls: list[dict[str, Any]]) -> list[dict[str, Any]]:
    state = job.get("parity") if isinstance(job.get("parity"), dict) else {}
    out: list[dict[str, Any]] = []
    explanation = state.get("explanation") if isinstance(state.get("explanation"), dict) else None
    if explanation:
        by_model = explanation.get("source") == "llm"
        call = next((c for c in reversed(calls) if c.get("purpose") == "parity_explanation"), None) if by_model else None
        out.append(_entry(
            KIND_PARITY_EXPLANATION, "Parity report explanation",
            ({"type": ORIGIN_MODEL, "provider": (call or {}).get("provider") or "unrecorded", "model": "unrecorded"}
             if by_model else {"type": ORIGIN_DETERMINISTIC, "engine": "parity_template"}),
            inputs={"payload_sha256": (call or {}).get("payload_sha256"), "source": explanation.get("source"),
                    "generated_at": explanation.get("generated_at")},
            proposal={"wording": "model" if by_model else "template"},
            decision={"status": "informational", "by": None, "at": explanation.get("generated_at"),
                      "how": "Wording only: it decides nothing and is not sealed into sign-off."},
            verification={"check": "parity_classifier", "status": "causes_decided_deterministically",
                          "detail": "Every cause in the text comes from the deterministic classifier."},
        ))
    fixes = state.get("fixes") if isinstance(state.get("fixes"), dict) else {}
    rounds = [r for r in state.get("rounds") or [] if isinstance(r, dict)]
    for fix_id in sorted(fixes):
        fix = fixes[fix_id] if isinstance(fixes[fix_id], dict) else {}
        status = str(fix.get("status") or "")
        if status in ("", "proposed"):
            continue
        decided = {"applied": "accepted", "applying": "accepted", "failed": "accepted", "dismissed": "rejected"}
        recheck = next((r for r in reversed(rounds) if r.get("fix_id") == fix_id), None)
        if recheck:
            verification = {"check": "parity_recheck", "status": "passed"
                            if int(recheck.get("unexplained_after") or 0) < int(recheck.get("unexplained_before") or 0)
                            else "no_change",
                            "detail": (f"Unexplained {recheck.get('unexplained_before')} → "
                                       f"{recheck.get('unexplained_after')} on re-check.")}
        elif fix.get("action") == "accept":
            verification = {"check": "human_judgement", "status": "accepted_by_person",
                            "detail": "Accepting a difference is a named person's judgement."}
        else:
            verification = {"check": "none", "status": "not_checked", "detail": "No re-check recorded."}
        out.append(_entry(
            KIND_PARITY_FIX, str(fix.get("title") or fix_id),
            {"type": ORIGIN_DETERMINISTIC, "engine": "parity_fixes"},
            inputs={"fix_id": fix_id, "action": fix.get("action"),
                    "difference_ids": sorted(str(d) for d in fix.get("difference_ids") or [])},
            proposal={"action": fix.get("action")},
            decision={"status": decided.get(status, status), "by": fix.get("approved_by"),
                      "at": fix.get("decided_at") or fix.get("applied_at"),
                      "how": f"Fix {status}."},
            verification=verification,
        ))
    return out


def _call_entries(calls: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for c in calls:
        out.append(_entry(
            KIND_MODEL_CALL, f"{c.get('purpose') or 'unspecified'} via {c.get('provider') or 'unknown'}",
            {"type": ORIGIN_MODEL, "provider": c.get("provider") or "unrecorded", "model": "unrecorded"},
            inputs={"payload_sha256": c.get("payload_sha256"), "event_hash": c.get("event_hash")},
            proposal={"purpose": c.get("purpose"), "channel": c.get("channel")},
            decision={"status": "informational", "by": None, "at": c.get("sealed_at"),
                      "how": "A model call; what it proposed appears in its own row."},
            verification={"check": "ai_egress_gate",
                          "status": "metadata_only" if c.get("metadata_only") else "cells_allowed",
                          "detail": ("No cell values crossed the boundary." if not c.get("cell_values_included")
                                     else "Cell values were allowed by policy for this call."),
                          "crossed_customer_boundary": bool(c.get("crossed_customer_boundary"))},
        ))
    return out


def build_ledger(job: dict[str, Any]) -> dict[str, Any]:
    """The ledger for one job, deterministic for a given job document and chain."""
    from services.ai_egress import manifests_for_job
    from services.mapping_witness_service import record_for_job

    job_id = str(job.get("_id") or job.get("id") or job.get("job_id") or "")
    witness = record_for_job(job)
    try:
        calls = manifests_for_job(job_id) if job_id else []
    except Exception as exc:  # noqa: BLE001 - an unreadable chain is reported, not hidden
        logger.info("ai ledger: egress chain unreadable (%s)", type(exc).__name__)
        calls = []
    calls = sorted(calls, key=lambda c: str(c.get("sealed_at") or ""))
    entries = (
        _mapping_entries(job, witness)
        + _witness_entries(witness)
        + _parity_entries(job, calls)
        + _call_entries(calls)
    )
    counts: dict[str, int] = {}
    for e in entries:
        counts[e["kind"]] = counts.get(e["kind"], 0) + 1
    by_origin = {
        ORIGIN_MODEL: sum(1 for e in entries if e["origin"]["type"] == ORIGIN_MODEL),
        ORIGIN_DETERMINISTIC: sum(1 for e in entries if e["origin"]["type"] == ORIGIN_DETERMINISTIC),
    }
    report = (witness or {}).get("report") or {}
    return {
        "version": LEDGER_VERSION,
        "job_id": job_id,
        "entries": entries,
        "counts": {"total": len(entries), "by_kind": counts, "by_origin": by_origin,
                   "model_calls": len(calls)},
        "witness": ({"report_sha256": report.get("inputs_sha256"), "mode": report.get("mode"),
                     "generated_at": report.get("generated_at"), "summary": report.get("summary")}
                    if witness else None),
        "ledger_sha256": _sha(entries),
        "honesty": _HONESTY,
    }


def ledger_for_pack(job_id: str, job: dict[str, Any] | None = None) -> dict[str, Any]:
    """The ledger as a signed pack carries it; a failure is stated in the pack, never dropped."""
    try:
        if job is None:
            from services.parity_state import load_job

            job = load_job(job_id)
        return build_ledger(job)
    except Exception as exc:  # noqa: BLE001
        logger.warning("ai ledger unavailable for %s: %s", job_id, type(exc).__name__)
        return {"version": LEDGER_VERSION, "job_id": job_id, "status": "unavailable",
                "reason": "The AI decisions ledger could not be built for this pack.", "entries": []}


def markdown_lines(ledger: dict[str, Any] | None) -> list[str]:
    """The ledger as a Markdown section for the exported report."""
    if not isinstance(ledger, dict):
        return []
    lines = ["", "## AI involvement", ""]
    entries = ledger.get("entries") or []
    if ledger.get("status") == "unavailable":
        return lines + [str(ledger.get("reason") or "Unavailable.")]
    if not entries:
        return lines + ["No AI proposals or model calls are recorded for this job."]
    lines += ["| What | Proposed by | Decision | Checked by |", "|---|---|---|---|"]
    for e in entries:
        origin = e["origin"]
        who = (f"{origin.get('provider')} ({origin.get('model')})" if origin["type"] == ORIGIN_MODEL
               else f"{origin.get('engine')} (deterministic)")
        decision = e["decision"]
        dec = decision["status"] + (f" by {decision['by']}" if decision.get("by") else "")
        ver = e["verification"]
        check = ver["check"].replace("_", " ") + f": {str(ver.get('status') or '').replace('_', ' ')}"
        subject = str(e["subject"]).replace("|", "\\|")
        lines.append(f"| {e['kind'].replace('_', ' ')}: {subject} | {who} | {dec} | {check} |")
    lines.append("")
    lines.append(f"Ledger SHA-256: {ledger.get('ledger_sha256')}")
    return lines
