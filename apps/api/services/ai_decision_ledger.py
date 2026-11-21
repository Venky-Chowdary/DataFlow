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


def _witness_check(snapshot: dict[str, Any] | None, source: str) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """(verification, the witness pair) for one source column, from the job's snapshot only."""
    report = (snapshot or {}).get("report") or {}
    pair = next((p for p in report.get("pairs") or [] if str(p.get("source") or "").casefold()
                 == source.casefold()), None)
    if not pair:
        return {"check": "none", "status": "not_checked",
                "detail": "No Mapping Witness report for the mapping this job ran."}, None
    flag = pair.get("flag") or {}
    return {
        "check": "mapping_witness",
        "status": pair.get("status"),
        "mode": report.get("mode"),
        "detail": flag.get("reason") or pair.get("reason") or "",
        "report_sha256": (snapshot or {}).get("report_sha256"),
        "checked_at": report.get("generated_at"),
    }, pair


def _run_by(req: dict[str, Any]) -> tuple[str | None, str]:
    """(who or what started the run, "schedule" | "person" | "unrecorded")."""
    who = str(req.get("triggered_by") or "").strip()
    if not who:
        return None, "unrecorded"
    if who.startswith("schedule:"):
        return who.split(":", 1)[1] or "schedule", "schedule"
    return who, "person"


def _mapping_entries(job: dict[str, Any], snapshot: dict[str, Any] | None) -> list[dict[str, Any]]:
    from services.mapping_witness import mapped_pairs
    from services.parity_evidence import mappings_of, request_of

    req = request_of(job)
    run_by, run_kind = _run_by(req)
    ran_at = job.get("started_at") or job.get("created_at")
    map_decisions = (snapshot or {}).get("map_decisions") or {}
    suggestions = dict((snapshot or {}).get("model_suggestions") or {})
    rows = {str(m.get("source") or "").casefold(): m for m in mappings_of(job)}
    out: list[dict[str, Any]] = []
    for p in mapped_pairs(list(rows.values())):
        source, target = p["source"], p["target"]
        m = rows.get(source.casefold()) or {}
        verification, pair = _witness_check(snapshot, source)
        approval = map_decisions.get(source) if isinstance(map_decisions.get(source), dict) else None
        if approval and approval.get("changed"):
            status, how = "changed", "A person chose this target in Map."
        elif approval and approval.get("approved"):
            status, how = "approved", "A person approved this target in Map."
        else:
            status, how = "unreviewed", "No Map approval is recorded; the job ran the proposed target."
        if pair is None:
            verification = {**verification, "detail": verification["detail"]
                            + (" Approved on human judgement." if approval else "")}
        inputs = {
            "source": source, "source_type": p["source_type"], "target": target, "target_type": p["target_type"],
            "method": m.get("method") or m.get("assignment_strategy"), "create_new": p["create_new"],
        }
        out.append(_entry(
            KIND_MAPPING_PROPOSAL, f"{source} → {target}",
            {"type": ORIGIN_DETERMINISTIC, "engine": "semantic_mapper",
             "method": str(m.get("method") or m.get("assignment_strategy") or "deterministic")},
            inputs=inputs,
            proposal={"target": target, "create_new": p["create_new"], "confidence": m.get("confidence")},
            decision={"status": status, "by": (approval or {}).get("by"), "at": (approval or {}).get("at"),
                      "how": how, "run_by": run_by, "run_kind": run_kind, "run_at": ran_at},
            verification=verification,
        ))
        # A model suggestion: the server's record from Map, else one carried on an API-run row.
        # A suggestion with no named provider is a deterministic alternative, not a model's.
        sug = suggestions.get(source)
        if not sug:
            raw = m.get("engine_suggestion") if isinstance(m.get("engine_suggestion"), dict) else {}
            if raw.get("llm_provider"):
                sug = {"suggested_target": raw.get("target") or target, "suggested_transform": raw.get("transform"),
                       "provider": raw.get("llm_provider"), "model": raw.get("llm_model") or "unrecorded"}
        if not sug:
            continue
        suggested = str(sug.get("suggested_target") or target)
        same_transform = not sug.get("suggested_transform") or _lower(sug.get("suggested_transform")) == _lower(
            m.get("transform") or m.get("transformation"))
        adopted = _lower(suggested) == _lower(target) and same_transform
        held = bool(m.get("llm_invented_transform")) and not adopted
        flag_target = _lower(((pair or {}).get("flag") or {}).get("suggested_target"))
        if pair and pair.get("status") == "witnessed" and _lower(suggested) != _lower(target):
            s_verify = {"check": "mapping_witness", "status": "contradicted",
                        "detail": f"The witness confirmed {target} on matching keys, not {suggested}."}
        elif pair and flag_target and flag_target == _lower(suggested):
            s_verify = {"check": "mapping_witness", "status": "supported",
                        "detail": f"The witness found the values in {suggested}."}
        else:
            s_verify = {"check": "none", "status": "not_checked", "detail": "No data check judged this suggestion."}
        out.append(_entry(
            KIND_MODEL_SUGGESTION, f"{source} → {suggested}",
            {"type": ORIGIN_MODEL, "provider": str(sug.get("provider")), "model": str(sug.get("model"))},
            inputs={**inputs, "suggested_target": suggested, "suggested_transform": sug.get("suggested_transform"),
                    "payload_sha256": sug.get("payload_sha256")},
            proposal={"target": suggested, "transform": sug.get("suggested_transform")},
            decision={
                "status": "held" if held else ("accepted" if adopted else "rejected"),
                "by": (approval or {}).get("by") if adopted else None,
                "at": (approval or {}).get("at") if adopted else None,
                "how": ("Held for review: a model-invented transform is never applied on its own."
                        if held else ("The column ran with the model's suggestion." if adopted
                                      else "The deterministic target ran; the suggestion was not adopted.")),
                "run_by": run_by, "run_kind": run_kind,
            },
            verification=s_verify,
        ))
    return out


def _witness_entries(job: dict[str, Any], snapshot: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Flags on what ran (kept or not reviewed), and earlier flags resolved by changing to what ran."""
    from services.mapping_witness import WITNESS_VERSION, flagged_pairs

    if not snapshot:
        return []
    report = snapshot.get("report") or {}
    reviews = [r for r in snapshot.get("flag_reviews") or [] if isinstance(r, dict)]
    approvals = snapshot.get("map_decisions") or {}
    recorded_at = str(snapshot.get("report_recorded_at") or "")
    out: list[dict[str, Any]] = []
    for pair in flagged_pairs(report):
        flag = pair.get("flag") or {}
        kept = next((r for r in reviews if r.get("decision") == "kept"
                     and str(r.get("source") or "").casefold() == pair["source"].casefold()), None)
        approval = approvals.get(pair["source"]) if isinstance(approvals.get(pair["source"]), dict) else None
        if kept:
            decision = {"status": "kept", "by": kept.get("by"), "at": kept.get("at"),
                        "how": "The reviewer looked at the flag and kept the mapping."}
        elif approval and approval.get("approved") and str(approval.get("at") or "") >= recorded_at:
            decision = {"status": "kept", "by": approval.get("by"), "at": approval.get("at"),
                        "how": "Approved in Map after the flag was shown."}
        else:
            decision = {"status": "unreviewed", "by": None, "at": None,
                        "how": "No one reviewed this flag; the job ran the flagged mapping."}
        out.append(_entry(
            KIND_WITNESS_FLAG, f"{pair['source']} → {pair['target']}",
            {"type": ORIGIN_DETERMINISTIC, "engine": report.get("version") or WITNESS_VERSION},
            inputs={"report_sha256": snapshot.get("report_sha256"), "source": pair["source"],
                    "target": pair["target"], "kind": flag.get("kind")},
            proposal={"flag": flag.get("kind"), "reason": flag.get("reason"),
                      "suggested_target": flag.get("suggested_target")},
            decision=decision,
            verification={"check": "mapping_witness", "status": "flagged", "mode": flag.get("basis"),
                          "detail": pair.get("reason"), "report_sha256": snapshot.get("report_sha256"),
                          "checked_at": report.get("generated_at")},
        ))
    for r in reviews:
        if r.get("decision") != "changed":
            continue
        out.append(_entry(
            KIND_WITNESS_FLAG, f"{r.get('source')} → {r.get('flagged_target')}",
            {"type": ORIGIN_DETERMINISTIC, "engine": WITNESS_VERSION},
            inputs={"report_sha256": r.get("report_sha256"), "source": r.get("source"),
                    "target": r.get("flagged_target"), "kind": r.get("flag_kind")},
            proposal={"flag": r.get("flag_kind"), "reason": None, "suggested_target": None},
            decision={"status": "changed", "by": r.get("by"), "at": r.get("at"),
                      "how": f"Flagged on an earlier mapping; the reviewer changed the target to {r.get('new_target')}, "
                             "which this job ran."},
            verification={"check": "mapping_witness", "status": "flagged", "mode": None,
                          "detail": "Flagged before the change.", "report_sha256": r.get("report_sha256")},
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


#: Rows that record a decision (what approvers attest); wording and model-call rows are sealed, not attested.
ATTESTED_KINDS = (KIND_MAPPING_PROPOSAL, KIND_MODEL_SUGGESTION, KIND_WITNESS_FLAG, KIND_PARITY_FIX)


def attested_sha256(entries: list[dict[str, Any]]) -> str:
    """Digest of the decision rows: approvals of a report bind to it (parity_signoff content v2).

    What was proposed, decided, by whom and how it checked out; not when (timestamps
    are presentation, as in content v1), so re-reading an unchanged job keeps approvals.
    """
    view = [
        {
            "kind": e.get("kind"), "subject": e.get("subject"), "origin": e.get("origin"),
            "inputs_sha256": e.get("inputs_sha256"), "proposal": e.get("proposal"),
            "decision": {k: (e.get("decision") or {}).get(k) for k in ("status", "by", "run_by", "run_kind")},
            "verification": {k: (e.get("verification") or {}).get(k) for k in ("check", "status")},
        }
        for e in entries if e.get("kind") in ATTESTED_KINDS
    ]
    return _sha(view)


def attested_digest_for_job(job: dict[str, Any]) -> str | None:
    """The attested digest without reading the egress chain (decision rows never need it)."""
    from services.mapping_witness_service import snapshot_of, snapshot_sha256

    try:
        snapshot = snapshot_of(job)
        if snapshot is not None and (snapshot.get("status") != "ok" or snapshot_sha256(snapshot) != snapshot.get("sha256")):
            snapshot = None
        entries = _mapping_entries(job, snapshot) + _witness_entries(job, snapshot) + _parity_entries(job, [])
        return attested_sha256(entries)
    except Exception as exc:  # noqa: BLE001 - a report read never fails on the ledger
        logger.warning("ai ledger attested digest unavailable: %s", type(exc).__name__)
        return None


def build_ledger(job: dict[str, Any]) -> dict[str, Any]:
    """The ledger for one job, from its own records; anything unreadable makes it "partial", never silent."""
    from services.ai_egress import manifests_for_job
    from services.mapping_witness_service import snapshot_of, snapshot_sha256

    job_id = str(job.get("_id") or job.get("id") or job.get("job_id") or "")
    problems: list[str] = []
    snapshot = snapshot_of(job)
    if snapshot is None:
        problems.append("The job carries no mapping evidence snapshot (submitted before it existed).")
    elif snapshot.get("status") != "ok":
        problems.append(str(snapshot.get("reason") or "The mapping evidence snapshot is unavailable."))
        snapshot = None
    elif snapshot_sha256(snapshot) != snapshot.get("sha256"):
        problems.append("The mapping evidence snapshot does not match its digest; it was not used.")
        snapshot = None
    try:
        calls = manifests_for_job(job_id) if job_id else []
    except Exception as exc:  # noqa: BLE001 - an unreadable chain is reported, not hidden
        logger.info("ai ledger: egress chain unreadable (%s)", type(exc).__name__)
        problems.append("The AI-egress chain could not be read, so model calls may be missing.")
        calls = []
    calls = sorted(calls, key=lambda c: str(c.get("sealed_at") or ""))
    entries = (
        _mapping_entries(job, snapshot)
        + _witness_entries(job, snapshot)
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
    report = (snapshot or {}).get("report") or {}
    return {
        "version": LEDGER_VERSION,
        "job_id": job_id,
        "status": "partial" if problems else "complete",
        "problems": problems,
        "entries": entries,
        "counts": {"total": len(entries), "by_kind": counts, "by_origin": by_origin,
                   "model_calls": len(calls)},
        "witness": ({"snapshot_sha256": snapshot.get("sha256"), "report_sha256": snapshot.get("report_sha256"),
                     "mode": report.get("mode"), "generated_at": report.get("generated_at"),
                     "summary": report.get("summary")} if snapshot else None),
        "ledger_sha256": _sha(entries),
        "attested_sha256": attested_sha256(entries),
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
    if ledger.get("status") == "partial":
        lines += ["**Partial:** " + " ".join(str(p) for p in ledger.get("problems") or []), ""]
    if not entries:
        return lines + ["No AI proposals or model calls are recorded for this job."]
    lines += ["| What | Proposed by | Decision | Checked by |", "|---|---|---|---|"]
    for e in entries:
        origin = e["origin"]
        who = (f"{origin.get('provider')} ({origin.get('model')})" if origin["type"] == ORIGIN_MODEL
               else f"{origin.get('engine')} (deterministic)")
        decision = e["decision"]
        dec = decision["status"] + (f" by {decision['by']}" if decision.get("by") else "")
        if decision.get("run_kind") == "schedule":
            dec += f"; run by schedule {decision.get('run_by')}"
        ver = e["verification"]
        check = ver["check"].replace("_", " ") + f": {str(ver.get('status') or '').replace('_', ' ')}"
        subject = str(e["subject"]).replace("|", "\\|")
        lines.append(f"| {e['kind'].replace('_', ' ')}: {subject} | {who} | {dec} | {check} |")
    lines.append("")
    lines.append(f"Ledger SHA-256: {ledger.get('ledger_sha256')}")
    return lines
