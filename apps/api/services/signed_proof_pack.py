"""Signed, hash-chained proof packs for Gate-8 + mapping evidence.

Portable diligence artifact: canonical JSON payload, content SHA-256, HMAC-SHA256
with the platform auth secret, and optional link into the audit hash chain.

Module 8: never claim migration correctness without post-write verification.
``migration_proven`` requires full_checksum post-write assurance. Sample / writer
ack / pre-write packs remain exportable for audit but refuse proven claims.

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


class ProofClaimError(RuntimeError):
    """Raised when a pack is asked to claim migration proven without post-write proof."""


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


def classify_post_write_assurance(
    reconciliation: dict[str, Any] | None,
) -> dict[str, Any]:
    """Classify Gate-8 evidence — never invent migration_proven from samples/preview.

    claim_level:
      none | pre_write_only | writer_ack | sample | full_checksum | failed

    migration_proven:
      True only for passed post-write full_checksum coverage.
      Sample assurance is post_write_verified but NOT migration_proven
      (sample ≠ population / full-table fidelity claim).
    """
    recon = reconciliation if isinstance(reconciliation, dict) else {}
    if not recon:
        return {
            "claim_level": "none",
            "post_write_verified": False,
            "migration_proven": False,
            "population_proof": False,
            "referential_integrity_proven": False,
            "checksum_match": False,
            "note": "No Gate-8 reconciliation attached — migration not proven.",
        }

    phase = str(recon.get("phase") or "").lower()
    coverage = str(recon.get("coverage") or "").lower()
    preview = bool(recon.get("preview") or recon.get("post_write_pending"))
    passed = bool(recon.get("passed"))
    src = str(recon.get("source_checksum") or "").strip()
    tgt = str(recon.get("target_checksum") or "").strip()
    checksum_match = bool(src and tgt and src == tgt)
    if recon.get("checksum_match") is False:
        checksum_match = False
    elif recon.get("checksum_match") is True:
        checksum_match = True

    if preview or phase.startswith("pre_write") or "simulation" in phase:
        return {
            "claim_level": "pre_write_only",
            "post_write_verified": False,
            "migration_proven": False,
            "population_proof": False,
            "referential_integrity_proven": False,
            "checksum_match": False,
            "note": (
                "Pre-write simulation only — post-write Gate-8 proof is pending. "
                "Execute-ready is not migration proven."
            ),
        }

    if (not passed) or phase.endswith("failed") or (
        coverage == "none" and "failed" in phase
    ):
        return {
            "claim_level": "failed",
            "post_write_verified": False,
            "migration_proven": False,
            "population_proof": False,
            "referential_integrity_proven": False,
            "checksum_match": checksum_match,
            "note": "Gate-8 failed or incomplete — migration not proven.",
        }

    if (
        coverage == "writer_ack"
        or phase.endswith("writer_ack")
        or (passed and src and not tgt)
    ):
        return {
            "claim_level": "writer_ack",
            "post_write_verified": False,
            "migration_proven": False,
            "population_proof": False,
            "referential_integrity_proven": False,
            "checksum_match": False,
            "note": (
                "Writer acknowledgement is not independent post-write verification — "
                "migration not proven."
            ),
        }

    sample_compared = int((recon.get("sample_compare") or {}).get("compared") or 0)
    if (
        coverage == "sample"
        or phase.endswith("sample_verified")
        or (passed and not checksum_match and sample_compared > 0)
    ):
        return {
            "claim_level": "sample",
            "post_write_verified": True,
            "migration_proven": False,
            "population_proof": False,
            "referential_integrity_proven": False,
            "checksum_match": checksum_match,
            "note": (
                "Post-write sample assurance only — not population / full-checksum "
                "migration proof. Sample must never claim full population correctness."
            ),
        }

    if coverage == "full_checksum" or (passed and checksum_match and not preview):
        return {
            "claim_level": "full_checksum",
            "post_write_verified": True,
            "migration_proven": True,
            "population_proof": False,
            "referential_integrity_proven": False,
            "checksum_match": True,
            "note": (
                "Post-write row-count + checksum match for selected transfer. "
                "Does not prove referential integrity or population orphan absence."
            ),
        }

    return {
        "claim_level": "none",
        "post_write_verified": False,
        "migration_proven": False,
        "population_proof": False,
        "referential_integrity_proven": False,
        "checksum_match": checksum_match,
        "note": "Unrecognized Gate-8 posture — refuse migration proven claim.",
    }


def assert_pack_may_claim_migration_proven(pack: dict[str, Any]) -> None:
    """Fail closed if callers try to market a pack as migration proven incorrectly."""
    assurance = pack.get("assurance") if isinstance(pack.get("assurance"), dict) else {}
    if not assurance.get("migration_proven"):
        raise ProofClaimError(
            "Proof pack must not claim migration proven without post-write "
            f"full_checksum assurance (claim_level={assurance.get('claim_level')!r}). "
            "See docs/PROOF_POST_WRITE_CONTRACT.md."
        )
    if assurance.get("claim_level") != "full_checksum":
        raise ProofClaimError(
            f"migration_proven requires claim_level=full_checksum, got "
            f"{assurance.get('claim_level')!r}"
        )


def build_signed_proof_pack(
    *,
    job_id: str,
    reconciliation: dict[str, Any] | None,
    mapping_proof: dict[str, Any] | None = None,
    preflight_summary: dict[str, Any] | None = None,
    actor: str = "system",
    prev_audit_hash: str | None = None,
    validation_mode: str | None = None,
    accepted_risks: list[dict[str, Any]] | None = None,
    rejected_rows: list[dict[str, Any]] | None = None,
    connector_versions: dict[str, Any] | None = None,
    ddl_hash: str | None = None,
    mapping_hash: str | None = None,
    transformation_hash: str | None = None,
) -> dict[str, Any]:
    """Build a signed proof pack for a completed (or failed) job.

    Always stamps ``assurance`` from Gate-8. Incomplete packs are still signed
    for audit chain integrity — they must not set ``migration_proven``.
    """
    assurance = classify_post_write_assurance(reconciliation)
    body = {
        "version": PROOF_PACK_VERSION,
        "issued_at": datetime.now(timezone.utc).isoformat(),
        "job_id": job_id,
        "actor": actor,
        "gate8": reconciliation or {},
        "mapping_proof": mapping_proof or {},
        "preflight_summary": preflight_summary or {},
        "assurance": assurance,
        "validation_mode": validation_mode,
        "accepted_risks": list(accepted_risks or []),
        "rejected_rows_count": len(rejected_rows or []),
        "rejected_rows_sample": list(rejected_rows or [])[:50],
        "connector_versions": connector_versions or {},
        "hashes": {
            "ddl_hash": ddl_hash,
            "mapping_hash": mapping_hash,
            "transformation_hash": transformation_hash,
        },
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
        "documentation": "docs/PROOF_POST_WRITE_CONTRACT.md",
    }
    canon = canonical_json(body)
    content_sha256 = sha256_hex(canon)
    signature = hmac_sha256_hex(_platform_secret(), f"{content_sha256}:{job_id}")
    return {
        **body,
        "content_sha256": content_sha256,
        "signature": {
            "alg": "HMAC-SHA256",
            "key_id": "platform_auth_secret",
            "value": signature,
        },
    }


def verify_signed_proof_pack(pack: dict[str, Any]) -> dict[str, Any]:
    """Verify content hash + HMAC. Returns {ok, errors[]}."""
    errors: list[str] = []
    if not isinstance(pack, dict):
        return {"ok": False, "errors": ["pack must be an object"]}
    sig = pack.get("signature") if isinstance(pack.get("signature"), dict) else {}
    claimed_hash = str(pack.get("content_sha256") or "")
    claimed_sig = str(sig.get("value") or "")
    job_id = str(pack.get("job_id") or "")
    body = {k: v for k, v in pack.items() if k not in ("content_sha256", "signature")}
    canon = canonical_json(body)
    actual_hash = sha256_hex(canon)
    if not claimed_hash or claimed_hash != actual_hash:
        errors.append("content_sha256 mismatch")
    expected_sig = hmac_sha256_hex(_platform_secret(), f"{actual_hash}:{job_id}")
    if not claimed_sig or not hmac.compare_digest(claimed_sig, expected_sig):
        errors.append("HMAC signature invalid")
    assurance = pack.get("assurance") if isinstance(pack.get("assurance"), dict) else {}
    if assurance.get("migration_proven") and assurance.get("claim_level") != "full_checksum":
        errors.append("migration_proven claimed without full_checksum assurance")
    return {"ok": not errors, "errors": errors, "content_sha256": actual_hash}


def export_proof_pack_for_job(job: dict[str, Any], *, actor: str = "system") -> dict[str, Any]:
    """Convenience: pull Gate-8 + mapping proof off a job document."""
    from services.audit_log import latest_event_hash

    prev = None
    try:
        prev = latest_event_hash()
    except Exception:
        prev = None
    dest = job.get("destination_summary") if isinstance(job.get("destination_summary"), dict) else {}
    rejected = dest.get("rejected_details") or job.get("rejected_details") or []
    mapping_proof = job.get("mapping_proof") if isinstance(job.get("mapping_proof"), dict) else {}
    return build_signed_proof_pack(
        job_id=str(job.get("_id") or job.get("id") or ""),
        reconciliation=job.get("reconciliation") if isinstance(job.get("reconciliation"), dict) else None,
        mapping_proof=mapping_proof or None,
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
        validation_mode=str(job.get("validation_mode") or dest.get("validation_mode") or "") or None,
        rejected_rows=list(rejected) if isinstance(rejected, list) else [],
        ddl_hash=str(dest.get("ddl_hash") or "") or None,
        mapping_hash=str(mapping_proof.get("mapping_hash") or "") or None,
    )
