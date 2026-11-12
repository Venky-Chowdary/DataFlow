"""Parity Studio: sign-off by named roles, the signed report and its export.

Sign-off opens only when no unexplained difference is left. A role is signed
only by someone whose real authority in the job's workspace the configured
mapping binds to it (:mod:`services.parity_authority`); the approval records
that authority (``signer_role``), the report ``version`` and the
``content_sha256`` of what was signed. An approval counts only while the report
still has that content: any change to the verdict, the differences or a fix
invalidates it (shown under ``invalidated_approvals``) and the role signs again.
When every required role has signed the same content, the final report is
serialized canonically and signed with the proof-pack key
(:func:`services.signed_proof_pack.build_signed_parity_pack`) — the same
envelope ``POST /api/v1/transfer/proof-pack/verify`` checks — and the signed
pack is stored on the job; a signed report is final (no decision changes it).
The Markdown export renders from that pack.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from services.parity_state import (
    ParityConflict,
    ParityForbidden,
    ParityInvalid,
    audit,
    check_version,
    load_job,
    now_iso,
    state_of,
    update_state,
)

VERIFY_ROUTE = "POST /api/v1/transfer/proof-pack/verify"


def content_sha256(report: dict[str, Any]) -> str:
    """The digest of what a signer attests: verdict, counts, tables, differences, fix outcomes.

    Volatile presentation (timestamps, version, wording, the explanation, sign-off
    itself) is left out, so re-reading an unchanged report keeps approvals valid;
    the explanation is therefore not sealed in the signed pack either.
    """
    body = {
        "job_id": report.get("job_id"),
        "verdict": report.get("verdict"),
        "parity": report.get("parity"),
        "tables": [
            {k: t.get(k) for k in ("name", "check", "rows_source", "rows_destination", "proven_equal_pct",
                                   "differences", "explained", "unexplained", "status")}
            for t in report.get("tables") or []
        ],
        "differences": sorted(
            ([d.get("id"), d.get("kind"), d.get("column"), d.get("rows"), (d.get("cause") or {}).get("code"),
              bool((d.get("cause") or {}).get("explained"))] for d in report.get("differences") or []),
            key=lambda row: str(row[0]),
        ),
        "fixes": sorted([str(f.get("id")), str(f.get("status"))] for f in report.get("fixes") or []),
    }
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def valid_approvals(state: dict[str, Any], digest: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """(approvals of this exact content, approvals of content that changed since)."""
    approvals = [a for a in state.get("approvals") or [] if isinstance(a, dict)]
    valid = [a for a in approvals if a.get("content_sha256") == digest]
    return valid, [a for a in approvals if a not in valid]


def sign_off(
    job_id: str, *, role: str, actor: str, signer_role: str, note: str = "",
    expected_version: int | None = None,
) -> dict[str, Any]:
    """Record one role's approval; sign the report when the last role signs.

    ``signer_role`` is the signer's gate role in the job's workspace, resolved
    by the caller from the authenticated principal — never from the request
    body. The approval is written by compare-and-swap on the report version it
    was given against, so a concurrent change is a 409, never a signature on a
    report nobody saw.
    """
    from services.parity_authority import authority_note, signing_refusal
    from services.parity_apply import summarized
    from services.parity_report import build_parity_report, public_report
    from services.signed_proof_pack import build_signed_parity_pack

    role = str(role or "").strip().lower()
    signer_role = str(signer_role or "viewer").strip().lower()
    note = note[:500]
    job = load_job(job_id)
    check_version(job, expected_version)
    roles = [str(r).lower() for r in state_of(job)["required_roles"]]
    if role not in roles:
        raise ParityInvalid(f"Role must be one of: {', '.join(roles)}.")
    refusal = signing_refusal(role, signer_role=signer_role, signer=actor)
    if refusal:
        audit("job.parity.signoff_refused", job=job, job_id=job_id, actor=actor, outcome="denied",
              details={"role": role, "signer_role": signer_role, "reason": refusal})
        raise ParityForbidden(refusal)
    report = build_parity_report(job, reread=True, job_id=job_id)
    signoff = report["signoff"]
    if signoff["status"] == "signed":
        raise ParityConflict("This report is already signed.")
    if signoff["status"] == "blocked":
        raise ParityConflict(str(signoff["blocked_reason"] or "Sign-off is blocked."))
    digest = content_sha256(report)
    authority = authority_note(role, signer_role)
    # Decided before the compare-and-swap: what the pack will say about its public-key signatures.
    from services.ledger_packs import commitment

    promised = commitment(str(job.get("workspace_id") or ""))

    def approve(job_now: dict[str, Any], state: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None]:
        pack = _approve(state)
        return summarized(job_now, state, job_id), pack

    def _approve(state: dict[str, Any]) -> dict[str, Any] | None:
        if state.get("signed"):
            raise ParityConflict("This report is already signed.")
        approvals, invalidated = valid_approvals(state, digest)
        if any(a.get("role") == role for a in approvals):
            raise ParityConflict(f"The {role} role has already signed.")
        if actor and any(a.get("user") == actor for a in approvals):
            raise ParityConflict("One person signs one role; ask another approver.")
        approvals.append({
            "role": role, "user": actor or "anonymous", "at": now_iso(), "note": note,
            "signer_role": signer_role, "authority": authority,
            "version": int(report["version"]), "content_sha256": digest,
        })
        # Approvals of earlier content stay on the record; they no longer count.
        state["approvals"] = invalidated + approvals
        if not {a["role"] for a in approvals} >= set(roles):
            return None
        signed_at = now_iso()
        final = public_report(report)
        # The seal holds what the approvals attest (content_sha256): the explanation is
        # wording a model or a template wrote, not attested, so it is not sealed.
        final["explanation"] = None
        final["signoff"] = {
            **final["signoff"], "approvals": approvals, "status": "signed",
            "blocked_reason": None, "signature": None, "signed_at": signed_at,
        }
        final["version"] = int(state.get("version") or 0) + 1
        pack = build_signed_parity_pack(job_id=job_id, report=final, actor=actor, ledger_commitment=promised)
        state["signed"] = {
            "signature": "hmac-sha256:" + str(pack["signature"]["value"]),
            "signed_at": signed_at,
            "content_sha256": pack["content_sha256"],
            "pack": pack,
        }
        return pack

    _updated, state, (after, pack) = update_state(job_id, approve, expected_version=int(report["version"]))
    audit("job.parity.signed_off", job=job, job_id=job_id, actor=actor,
          details={"role": role, "note": note, "verdict": report["verdict"], "signer_role": signer_role,
                   "version": int(report["version"]), "content_sha256": digest})
    if pack:
        audit("job.parity.signed_off", job=job, job_id=job_id, actor=actor,
              details={"signed": True, "content_sha256": pack["content_sha256"], "roles": sorted(roles)})
        # The pack is stored: add its public-key signature and ledger entry (never blocks the sign-off).
        from services.ledger_packs import issue

        issue(pack, workspace_id=str(job.get("workspace_id") or ""), actor=actor)
    after["version"] = int(state["version"])
    return after


def signed_pack(job: dict[str, Any]) -> dict[str, Any] | None:
    state = state_of(job)
    signed = state.get("signed") if isinstance(state.get("signed"), dict) else None
    return (signed or {}).get("pack")


def exported_pack(job: dict[str, Any], *, actor: str) -> dict[str, Any] | None:
    """The signed pack as exported: with its Evidence Ledger receipt under ``ledger`` when one exists."""
    from services.ledger_packs import with_ledger

    return with_ledger(signed_pack(job), workspace_id=str(job.get("workspace_id") or ""), actor=actor)


def _pct(value: Any) -> str:
    """Two decimals, rounded **down**, and never "100.00%" for a share below 100."""
    from services.parity_report import floor_pct

    if value is None:
        return "unknown"
    return f"{floor_pct(float(value), short=float(value) < 100, decimals=2):.2f}%"


def _resolution(diff: dict[str, Any], fixes: list[dict[str, Any]]) -> str:
    done = [
        f for f in fixes
        if diff["id"] in (f.get("difference_ids") or []) and f.get("status") in {"applied", "dismissed"}
    ]
    if diff["cause"]["code"] == "accepted_by_operator":
        return "Accepted"
    if not diff["cause"]["explained"]:
        # Still unexplained after the fix: it is open, whatever ran (re-review 4, 9e).
        if not done:
            return "Open"
        f = done[-1]
        tail = "; it still differs" if f["status"] == "applied" else ""
        return f"Open ({f['action'].replace('_', ' ')} {f['status']}{tail})"
    if done:
        f = done[-1]
        return f"{f['action'].replace('_', ' ')} {f['status']}"
    return "Expected"


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
    # The same basis as the Trust Report page at this version (its re-read, reused while fresh).
    report = public_report(build_parity_report(job, reread=True, job_id=job_id, cache="use")) if not pack else {}
    return render_markdown(report, pack)
