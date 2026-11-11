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
