"""Exit Evidence Pack: multi-role sign-off of a rehearsal, the signed pack, its verification.

Sign-off reuses Parity Studio's: each role is bound to real authority in the
system's workspace (:mod:`services.parity_authority`), an approval attests the
exact content it saw (``content_sha256``; approvals of content that changed
since move to ``invalidated_approvals``, :func:`services.parity_signoff.valid_approvals`),
a role signs once and one person signs one role, and only a ``proven``
rehearsal can be signed. When the last role signs, the pack is signed with the
proof-pack key in its own signature domain (``exit_evidence:<system>:<rehearsal>``,
:func:`services.signed_proof_pack.pack_subject`) and its digest is anchored
(:func:`services.audit_anchor.anchor_digest`; S3 Object Lock, or "not
anchored").
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

PACK_KIND = "exit_evidence"


def content_sha256(doc: dict[str, Any]) -> str:
    """The digest of what a signer attests: the verdict, every table's measurements, the seal, the clock."""
    body = {
        "rehearsal_id": doc.get("id"), "system_id": doc.get("system_id"), "verdict": doc.get("verdict"),
        "tables": [
            {"name": t.get("name"), "status": t.get("status"), "rows_source": t.get("rows_source"),
             "rows_extracted": t.get("rows_extracted"), "key_set": t.get("key_set"), "row_set": t.get("row_set"),
             "control_totals": t.get("control_totals"), "value_method": (t.get("value_parity") or {}).get("method"),
             "part": {k: (t.get("part") or {}).get(k) for k in ("sha256", "size_bytes", "object_key")}}
            for t in doc.get("tables") or []
        ],
        "merkle_root": (doc.get("seal") or {}).get("merkle_root"),
        "measured_seconds": (doc.get("duration") or {}).get("measured_seconds"),
        "window_seconds": (doc.get("duration") or {}).get("window_seconds"),
    }
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _blocked_reason(doc: dict[str, Any]) -> str | None:
    verdict = doc.get("verdict")
    if verdict == "running":
        return "The rehearsal is still running; sign-off opens when it is proven."
    if verdict != "proven":
        return (f"Only a proven rehearsal can be signed; this one is {str(verdict).replace('_', ' ')}: "
                f"{doc.get('verdict_reason') or ''}").strip()
    return None


def signoff_block(doc: dict[str, Any]) -> dict[str, Any]:
    """The rehearsal's ``signoff`` (contract 5.1), computed from the stored approvals."""
    from services.parity_signoff import valid_approvals

    signed = doc.get("signed") if isinstance(doc.get("signed"), dict) else None
    if signed:
        approvals = list(signed.get("approvals") or [])
        invalidated = [a for a in doc.get("approvals") or [] if a not in approvals]
    else:
        approvals, invalidated = valid_approvals({"approvals": doc.get("approvals") or []}, content_sha256(doc))
    reason = None if signed else _blocked_reason(doc)
    return {
        "required_roles": list(doc.get("required_roles") or []),
        "approvals": approvals, "invalidated_approvals": invalidated,
        "status": "signed" if signed else ("blocked" if reason else "open"),
        "blocked_reason": reason,
        "signature": (signed or {}).get("signature"), "signed_at": (signed or {}).get("signed_at"),
    }


def evidence_block(doc: dict[str, Any]) -> dict[str, Any]:
    signed = doc.get("signed") if isinstance(doc.get("signed"), dict) else {}
    return {"signed": bool(signed), "content_sha256": signed.get("content_sha256"),
            "anchor": signed.get("anchor"), "export": f"/api/v1/exit/rehearsals/{doc.get('id')}/evidence"}


# ------------------------------------------------------------------ sign-off

PACK_VERSION = "1"


def _pack_body(doc: dict[str, Any], approvals: list[dict[str, Any]], *, signed_at: str, actor: str) -> dict[str, Any]:
    """The Exit Evidence Pack body (contract 5.2): what the approvals attested, nothing volatile."""
    from services import exit_store
    from services.exit_rehearsal import public

    system = exit_store.get(exit_store.SYSTEMS, str(doc.get("system_id") or "")) or {}
    view = public(doc)
    for key in ("trend", "evidence"):
        view.pop(key, None)
    view["signoff"] = {**view["signoff"], "approvals": approvals, "invalidated_approvals": [],
                       "status": "signed", "blocked_reason": None, "signature": None, "signed_at": signed_at}
    seal = doc.get("seal") or {}
    return {
        "version": PACK_VERSION, "kind": PACK_KIND, "issued_at": signed_at,
        "system_id": doc.get("system_id"), "rehearsal_id": doc.get("id"), "workspace_id": doc.get("workspace_id"),
        "actor": actor,
        "system": {"name": system.get("name") or doc.get("system_name"), "criticality": system.get("criticality"),
                   "engine": (system.get("source") or doc.get("source") or {}).get("engine"),
                   "window_basis": system.get("window_basis"),
                   "exit_window_hours": int(doc.get("window_seconds") or 0) // 3600},
        "rehearsal": view, "merkle_root": seal.get("merkle_root"), "parts": list(seal.get("parts") or []),
        "content_sha256_attested": content_sha256(doc),
    }


def sign_off(rehearsal_id: str, *, role: str, actor: str, signer_role: str, note: str = "",
             expected_version: int | None = None) -> dict[str, Any]:
    """Record one role's approval; sign and anchor the pack when the last role signs (contract 5.1)."""
    from services import exit_store
    from services.exit_inventory import ExitConflict, ExitForbidden, ExitInvalid, ExitNotFound, audit, now_iso
    from services.exit_rehearsal import settle
    from services.parity_authority import authority_note, signing_refusal
    from services.parity_signoff import valid_approvals
    from services.signed_proof_pack import build_signed_exit_pack

    role = str(role or "").strip().lower()
    signer_role = str(signer_role or "viewer").strip().lower()
    note = str(note or "")[:500]
    doc = settle(rehearsal_id, inline=False) or exit_store.get(exit_store.REHEARSALS, rehearsal_id)
    if not doc:
        raise ExitNotFound("Rehearsal not found")
    system = {"id": doc.get("system_id"), "workspace_id": doc.get("workspace_id")}
    roles = [str(r).lower() for r in doc.get("required_roles") or []]
    if role not in roles:
        raise ExitInvalid(f"Role must be one of: {', '.join(roles)}.")
    refusal = signing_refusal(role, signer_role=signer_role, signer=actor)
    if refusal:
        audit("exit.rehearsal.signoff_refused", system=system, actor=actor, outcome="denied",
              details={"rehearsal_id": rehearsal_id, "role": role, "signer_role": signer_role, "reason": refusal})
        raise ExitForbidden(refusal)
    view = signoff_block(doc)
    if view["status"] == "signed":
        raise ExitConflict("This rehearsal is already signed; a signed rehearsal is final.")
    if view["status"] == "blocked":
        raise ExitConflict(str(view["blocked_reason"]))
    digest = content_sha256(doc)
    authority = authority_note(role, signer_role)

    def approve(current: dict[str, Any]) -> dict[str, Any] | None:
        if current.get("signed"):
            raise ExitConflict("This rehearsal is already signed; a signed rehearsal is final.")
        if content_sha256(current) != digest:
            raise ExitConflict("The rehearsal changed since it was read; reload it and sign again.")
        approvals, invalidated = valid_approvals({"approvals": current.get("approvals") or []}, digest)
        if any(a.get("role") == role for a in approvals):
            raise ExitConflict(f"The {role} role has already signed.")
        if any(a.get("user") == actor for a in approvals):
            raise ExitConflict("One person signs one role; ask another approver.")
        approvals.append({"role": role, "user": actor, "at": now_iso(), "note": note, "signer_role": signer_role,
                          "authority": authority, "version": int(current.get("version") or 0),
                          "content_sha256": digest})
        current["approvals"] = invalidated + approvals
        if not {a["role"] for a in approvals} >= set(roles):
            return None
        signed_at = now_iso()
        pack = build_signed_exit_pack(_pack_body(current, approvals, signed_at=signed_at, actor=actor))
        current["signed"] = {"signature": "hmac-sha256:" + str(pack["signature"]["value"]), "signed_at": signed_at,
                             "content_sha256": pack["content_sha256"], "approvals": approvals, "pack": pack,
                             "anchor": None}
        return pack

    try:
        after, pack = exit_store.update(exit_store.REHEARSALS, rehearsal_id, approve,
                                        expected_version=expected_version if expected_version is not None
                                        else int(doc.get("version") or 0))
    except exit_store.StoreConflict as exc:
        raise ExitConflict(f"This rehearsal {exc}; reload it and sign again.") from exc
    audit("exit.rehearsal.signed_off", system=system, actor=actor,
          details={"rehearsal_id": rehearsal_id, "role": role, "signer_role": signer_role, "note": note,
                   "content_sha256": digest})
    if pack:
        after = _anchor(rehearsal_id, pack, system=system, actor=actor) or after
    return after


def _anchor(rehearsal_id: str, pack: dict[str, Any], *, system: dict[str, Any], actor: str) -> dict[str, Any] | None:
    """Anchor the signed pack's digest; the receipt (anchored / not_anchored / failed) is kept with it."""
    from services import exit_store
    from services.audit_anchor import anchor_digest
    from services.exit_inventory import audit
    from services.signed_proof_pack import pack_subject

    receipt = anchor_digest(pack["content_sha256"], kind=PACK_KIND, subject=pack_subject(pack))

    def keep(current: dict[str, Any]) -> None:
        if isinstance(current.get("signed"), dict):
            current["signed"]["anchor"] = receipt

    doc = None
    for _ in range(3):
        try:
            doc, _ = exit_store.update(exit_store.REHEARSALS, rehearsal_id, keep)
            break
        except exit_store.StoreConflict:
            continue
    audit("exit.rehearsal.signed", system=system, actor=actor,
          outcome="succeeded" if (receipt or {}).get("status") != "failed" else "failed",
          details={"rehearsal_id": rehearsal_id, "content_sha256": pack["content_sha256"],
                   "anchor_status": (receipt or {}).get("status"), "anchor_reason": (receipt or {}).get("reason")})
    return doc


# --------------------------------------------------------------- the pack


def signed_pack(doc: dict[str, Any]) -> dict[str, Any] | None:
    signed = doc.get("signed") if isinstance(doc.get("signed"), dict) else None
    return (signed or {}).get("pack")


def verify_pack(pack: Any) -> dict[str, Any]:
    """Verify an Exit Evidence Pack: signature, Merkle root, and its anchor read back now (contract 5.2)."""
    from services.audit_anchor import find_receipt, verify_receipt
    from services.exit_completeness import merkle_root
    from services.signed_proof_pack import verify_signed_proof_pack

    if not isinstance(pack, dict) or pack.get("kind") != PACK_KIND:
        return {"ok": False, "errors": ["this is not an Exit Evidence Pack (kind exit_evidence)"],
                "kind": str((pack or {}).get("kind") or "") if isinstance(pack, dict) else "",
                "content_sha256": None, "signature_ok": False, "merkle_ok": False, "merkle_root": None,
                "anchor": verify_receipt(None)}
    checked = verify_signed_proof_pack(pack)
    errors = list(checked["errors"])
    claimed = pack.get("merkle_root")
    sealed = ((pack.get("rehearsal") or {}).get("seal") or {}).get("merkle_root")
    try:
        computed = merkle_root(pack.get("parts") or [])
    except (KeyError, TypeError, ValueError):
        computed = None
    merkle_ok = bool(claimed) and computed == claimed == sealed
    if not merkle_ok:
        errors.append("the Merkle root does not match the parts listed in the pack")
    digest = str(pack.get("content_sha256") or "")
    anchor = verify_receipt(find_receipt(digest, kind=PACK_KIND), digest=digest)
    if anchor["status"] == "failed":
        errors.append(f"the anchor could not be confirmed: {anchor.get('reason')}")
    return {"ok": not errors, "errors": errors, "kind": PACK_KIND, "content_sha256": checked["content_sha256"],
            "signature_ok": not checked["errors"], "merkle_ok": merkle_ok, "merkle_root": claimed,
            "anchor": anchor}


def render_markdown(doc: dict[str, Any]) -> str:
    """A readable Exit Evidence report; it states evidence, never compliance."""
    from services.exit_rehearsal import public

    view = public(doc)
    pack = signed_pack(doc)
    duration, seal, sign = view.get("duration") or {}, view.get("seal") or {}, view["signoff"]
    lines = [
        f"# Exit Evidence: {view.get('system_name')} — rehearsal {view.get('n')}",
        "",
        f"- Verdict: **{str(view.get('verdict')).replace('_', ' ')}** — {view.get('verdict_reason')}",
        f"- Started {view.get('started_at')}, finished {view.get('finished_at')}",
        f"- Duration: {duration.get('statement') or 'not measured'}",
        f"- Seal: {seal.get('algorithm')} root `{seal.get('merkle_root')}` over {seal.get('parts_count', 0)} part(s), "
        f"{seal.get('total_bytes', 0)} byte(s)",
        f"- Sign-off: {sign['status']}" + (f", signature `{sign['signature']}`" if sign.get("signature") else ""),
        f"- Anchor: {((view.get('evidence') or {}).get('anchor') or {}).get('status') or 'not anchored'}",
        "",
        "| Table | Status | Source rows | Extracted rows | Value check | Why |",
        "| --- | --- | ---: | ---: | --- | --- |",
    ]
    for t in view.get("tables") or []:
        reason = str(t.get("reason") or "").replace("|", "\\|")
        lines.append(f"| {t.get('name')} | {t.get('status')} | {t.get('rows_source')} | {t.get('rows_extracted')} | "
                     f"{(t.get('value_parity') or {}).get('method')} | {reason} |")
    lines += ["", "## Approvals", ""]
    lines += [f"- {a['role']}: {a['user']} ({a.get('authority')}) at {a['at']}" for a in sign.get("approvals") or []]
    lines += ["", ("Signed pack content SHA-256: `" + str(pack["content_sha256"]) + "`") if pack else
              "Not signed yet.", "",
              "Datawrap produces this evidence from measured rehearsals; it is not a statement of regulatory "
              "compliance. Your auditors decide."]
    return "\n".join(lines) + "\n"
