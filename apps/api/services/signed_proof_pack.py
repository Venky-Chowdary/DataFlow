"""Signed, hash-chained proof packs for Gate-8 + mapping evidence.

Portable diligence artifact: canonical JSON payload, content SHA-256, HMAC-SHA256
with the platform auth secret, and optional link into the audit hash chain.

This is engineering evidence — not an auditor SOC2 letter.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
from datetime import datetime, timezone
from typing import Any

from services.value_serializer import json_default

logger = logging.getLogger(__name__)

PROOF_PACK_VERSION = 1


def _platform_secret() -> bytes:
    try:
        from services.auth_service import _token_secret

        raw = _token_secret()
        if isinstance(raw, bytes):
            return raw
        return str(raw or "").encode("utf-8")
    except Exception:
        from services.brand_env import getenv_brand

        return (getenv_brand("AUTH_SECRET", "") or "dev-only-not-for-production").encode("utf-8")


def canonical_json(payload: dict[str, Any]) -> str:
    """Stable JSON for hashing (sorted keys, no insignificant whitespace)."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=json_default)


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def hmac_sha256_hex(secret: bytes, text: str) -> str:
    return hmac.new(secret, text.encode("utf-8"), hashlib.sha256).hexdigest()


def build_signed_proof_pack(
    *,
    job_id: str,
    reconciliation: dict[str, Any] | None,
    mapping_proof: dict[str, Any] | None = None,
    preflight_summary: dict[str, Any] | None = None,
    actor: str = "system",
    prev_audit_hash: str | None = None,
) -> dict[str, Any]:
    """Build a signed proof pack for a completed (or failed) job."""
    body = {
        "version": PROOF_PACK_VERSION,
        "issued_at": datetime.now(timezone.utc).isoformat(),
        "job_id": job_id,
        "actor": actor,
        "gate8": reconciliation or {},
        "mapping_proof": mapping_proof or {},
        "preflight_summary": preflight_summary or {},
        "prev_audit_hash": prev_audit_hash,
        "delivery_semantics": {
            "cdc_default": "at_least_once",
            "exactly_once": False,
            "at_least_once": True,
            "at_most_once": False,
            "note": (
                "Destinations must upsert with PK/LSN guards under at-least-once capture; "
                "exactly-once and at-most-once are not claimed."
            ),
        },
    }
    canon = canonical_json(body)
    content_sha256 = sha256_hex(canon)
    signature = hmac_sha256_hex(_platform_secret(), f"{content_sha256}:{job_id}")
    pack = {
        **body,
        "content_sha256": content_sha256,
        "signature": {
            "alg": "HMAC-SHA256",
            "key_id": "platform_auth_secret",
            "value": signature,
        },
    }
    return pack


def verify_signed_proof_pack(pack: dict[str, Any]) -> dict[str, Any]:
    """Verify content hash + HMAC. Returns {ok, errors[]}."""
    errors: list[str] = []
    if not isinstance(pack, dict):
        return {"ok": False, "errors": ["pack must be an object"]}
    sig = pack.get("signature") if isinstance(pack.get("signature"), dict) else {}
    claimed_hash = str(pack.get("content_sha256") or "")
    claimed_sig = str(sig.get("value") or "")
    job_id = str(pack.get("job_id") or "")
    body = {
        k: v
        for k, v in pack.items()
        if k not in ("content_sha256", "signature")
    }
    canon = canonical_json(body)
    actual_hash = sha256_hex(canon)
    if not claimed_hash or claimed_hash != actual_hash:
        errors.append("content_sha256 mismatch")
    expected_sig = hmac_sha256_hex(_platform_secret(), f"{actual_hash}:{job_id}")
    if not claimed_sig or not hmac.compare_digest(claimed_sig, expected_sig):
        errors.append("HMAC signature invalid")
    return {"ok": not errors, "errors": errors, "content_sha256": actual_hash}


def export_proof_pack_for_job(job: dict[str, Any], *, actor: str = "system") -> dict[str, Any]:
    """Convenience: pull Gate-8 + mapping proof off a job document."""
    from services.audit_log import latest_event_hash

    prev = None
    try:
        prev = latest_event_hash()
    except Exception:
        prev = None
    return build_signed_proof_pack(
        job_id=str(job.get("_id") or job.get("id") or ""),
        reconciliation=job.get("reconciliation") if isinstance(job.get("reconciliation"), dict) else None,
        mapping_proof=job.get("mapping_proof") if isinstance(job.get("mapping_proof"), dict) else None,
        preflight_summary=(
            {
                "passed": (job.get("preflight") or {}).get("passed"),
                "decision": (job.get("preflight") or {}).get("decision"),
                "passed_count": (job.get("preflight") or {}).get("passed_count"),
                "total_gates": (job.get("preflight") or {}).get("total_gates"),
                "readiness_score": (job.get("preflight") or {}).get("readiness_score"),
            }
            if isinstance(job.get("preflight"), dict)
            else None
        ),
        actor=actor,
        prev_audit_hash=prev,
    )
