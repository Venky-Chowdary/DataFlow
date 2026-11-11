"""Parity Studio: sign-off by named roles, the signed report and its export.

Sign-off opens only when no unexplained difference is left. Each approval names
a role, the person and a note, and writes an audit-chain event. When every
required role has signed, the final report is serialized canonically and signed
with the proof-pack key (:func:`services.signed_proof_pack.build_signed_parity_pack`)
— the same envelope ``POST /api/v1/transfer/proof-pack/verify`` checks — and the
signed pack is stored on the job. The Markdown export renders from that pack.
"""

from __future__ import annotations

from typing import Any

from services.parity_fixes import ParityConflict, ParityInvalid, load_job
from services.parity_state import audit, job_lock, now_iso, save_state, state_of

VERIFY_ROUTE = "POST /api/v1/transfer/proof-pack/verify"


def sign_off(job_id: str, *, role: str, actor: str, note: str = "") -> dict[str, Any]:
    """Record one role's approval; sign the report when the last role signs."""
    from services.parity_report import build_parity_report, public_report
    from services.signed_proof_pack import build_signed_parity_pack

    role = str(role or "").strip().lower()
    with job_lock(job_id):
        job = load_job(job_id)
        state = state_of(job)
        roles = [str(r).lower() for r in state["required_roles"]]
        if role not in roles:
            raise ParityInvalid(f"Role must be one of: {', '.join(roles)}.")
        report = build_parity_report(job, reread=True, job_id=job_id)
        signoff = report["signoff"]
        if signoff["status"] == "signed":
            raise ParityConflict("This report is already signed.")
        if signoff["status"] == "blocked":
            raise ParityConflict(str(signoff["blocked_reason"] or "Sign-off is blocked."))
        approvals = list(state["approvals"])
        if any(a.get("role") == role for a in approvals):
            raise ParityConflict(f"The {role} role has already signed.")
        if actor and any(a.get("user") == actor for a in approvals):
            raise ParityConflict("One person signs one role; ask another approver.")
        approvals.append({"role": role, "user": actor or "anonymous", "at": now_iso(), "note": note[:500]})
        state["approvals"] = approvals
        audit("job.parity.signed_off", job=job, job_id=job_id, actor=actor,
              details={"role": role, "note": note[:500], "verdict": report["verdict"]})
        if {a["role"] for a in approvals} >= set(roles):
            signed_at = now_iso()
            final = public_report(report)
            final["signoff"] = {
                **final["signoff"], "approvals": approvals, "status": "signed",
                "blocked_reason": None, "signature": None, "signed_at": signed_at,
            }
            final["version"] = int(state.get("version") or 0) + 1
            pack = build_signed_parity_pack(job_id=job_id, report=final, actor=actor)
            state["signed"] = {
                "signature": "hmac-sha256:" + str(pack["signature"]["value"]),
                "signed_at": signed_at,
                "content_sha256": pack["content_sha256"],
                "pack": pack,
            }
            audit("job.parity.signed_off", job=job, job_id=job_id, actor=actor,
                  details={"signed": True, "content_sha256": pack["content_sha256"],
                           "roles": sorted(roles)})
        save_state(job_id, job, state)
    return build_parity_report({**job, "parity": state}, reread=True, job_id=job_id)


def signed_pack(job: dict[str, Any]) -> dict[str, Any] | None:
    state = state_of(job)
    signed = state.get("signed") if isinstance(state.get("signed"), dict) else None
    return (signed or {}).get("pack")


def _pct(value: Any) -> str:
    return "unknown" if value is None else f"{float(value):.2f}%"


def _resolution(diff: dict[str, Any], fixes: list[dict[str, Any]]) -> str:
    done = [
        f for f in fixes
        if diff["id"] in (f.get("difference_ids") or []) and f.get("status") in {"applied", "dismissed"}
    ]
    if diff["cause"]["code"] == "accepted_by_operator":
        return "Accepted"
    if done:
        f = done[-1]
        return f"{f['action'].replace('_', ' ')} {f['status']}"
    return "Expected" if diff["cause"]["explained"] else "Open"


def render_markdown(report: dict[str, Any], pack: dict[str, Any] | None) -> str:
    """The evidence pack as Markdown: verdict, confidence, tables, differences, approvals, signature."""
    body = (pack or {}).get("parity") or report
    p = body["parity"]
    lines = [
        f"# Parity report — job {body['job_id']}",
        "",
        f"**Verdict:** {body['verdict']} — {body['verdict_reason']}",
        "",
        f"**Confidence:** {body['confidence']['statement']}",
        "",
        f"Rows in source: {p['rows_source'] if p['rows_source'] is not None else 'unknown'} · "
        f"rows in destination: {p['rows_destination'] if p['rows_destination'] is not None else 'unknown'} · "
        f"rows that differ: {p['rows_differ']} ({p['explained']} explained, {p['unexplained']} unexplained) · "
        f"proven equal: {_pct(p['proven_equal_pct'])}",
        "",
        "## Tables",
        "",
        "| Table | Check | Proven equal | Differences | Unexplained | Status |",
        "|---|---|---|---|---|---|",
    ]
    for t in body["tables"]:
        lines.append(
            f"| {t['name']} | {t['check'].replace('_', ' ')} | {_pct(t['proven_equal_pct'])} | "
            f"{t['differences']} | {t['unexplained']} | {t['status']} |"
        )
    lines += ["", "## Differences", ""]
    if not body["differences"]:
        lines.append("No differences.")
    else:
        lines += ["| Table | Column | Kind | Rows | Cause | Explained | Resolution |",
                  "|---|---|---|---|---|---|---|"]
        for d in body["differences"]:
            cause = f"{d['cause']['label']}: {d['cause']['detail']}".replace("|", "\\|")
            lines.append(
                f"| {d['table']} | {d['column'] or '—'} | {d['kind'].replace('_', ' ')} | {d['rows']} | "
                f"{cause} | {'yes' if d['cause']['explained'] else 'no'} | {_resolution(d, body['fixes'])} |"
            )
    applied = [f for f in body["fixes"] if f.get("status") != "proposed"]
    if applied:
        lines += ["", "## Fixes", ""]
        for f in applied:
            lines.append(f"- {f['title']} — {f['status']} by {f.get('approved_by') or 'unknown'}"
                         f"{': ' + str(f['result']) if f.get('result') else ''}")
    if body.get("rounds"):
        lines += ["", "## Re-check rounds", ""]
        for r in body["rounds"]:
            lines.append(f"- Round {r['n']} ({r['at']}): unexplained {r['unexplained_before']} → "
                         f"{r['unexplained_after']} after fix {r['fix_id']}")
    signoff = body["signoff"]
    lines += ["", "## Approvals", ""]
    if signoff["approvals"]:
        for a in signoff["approvals"]:
            lines.append(f"- {a['role']}: {a['user']} at {a['at']}" + (f" — {a['note']}" if a.get("note") else ""))
    else:
        lines.append("No approvals yet.")
    lines += ["", "## Signature", ""]
    if pack:
        lines += [
            f"- Signature: hmac-sha256:{pack['signature']['value']}",
            f"- Content SHA-256: {pack['content_sha256']}",
            f"- Signed at: {signoff.get('signed_at')}",
            f"- Verify: export this report with `?format=json` and send it to `{VERIFY_ROUTE}`; "
            "the answer is `ok: true` while the pack is unchanged.",
        ]
    else:
        lines.append("Not signed yet: every required role must sign first.")
    return "\n".join(lines) + "\n"


def export_markdown(job: dict[str, Any], job_id: str) -> str:
    from services.parity_report import build_parity_report, public_report

    pack = signed_pack(job)
    report = public_report(build_parity_report(job, reread=False, job_id=job_id)) if not pack else {}
    return render_markdown(report, pack)
