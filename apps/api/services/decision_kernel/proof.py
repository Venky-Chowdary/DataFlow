"""Kernel Proof Engine (Phase C10).

Buyer evidence packs bind Decision Artifact hash + DDL identity + full SHA-256
reconcile digest. Sample preflight never overrides population checksum failure.
"""

from __future__ import annotations

from typing import Any, Mapping

from services.decision_kernel.models import DECISION_ARTIFACT_SCHEMA, ProofPlan


PROOF_ENGINE_VERSION = "proof_engine.v1"


def build_proof_plan(
    *,
    checksum_algorithm: str = "sha256",
    sample_limit_preflight: int = 500,
    reconcile_mode: str = "full_population",
) -> ProofPlan:
    """Canonical ProofPlan — full digest, sample explicitly non-population."""
    return ProofPlan(
        checksum_algorithm=checksum_algorithm,
        checksum_hex_chars=64,
        sample_limit_preflight=sample_limit_preflight,
        sample_is_population_proof=False,
        reconcile_mode=reconcile_mode,
    )


def extract_population_checksum(reconciliation: Mapping[str, Any] | None) -> dict[str, Any]:
    """Pull full-population checksum fields; refuse truncated digests as proven."""
    recon = dict(reconciliation or {})
    digest = str(
        recon.get("final_checksum")
        or recon.get("content_sha256")
        or recon.get("checksum")
        or recon.get("digest")
        or ""
    ).strip().lower()
    # Legacy 16-hex truncations are not migration_proven material.
    full = len(digest) == 64 and all(c in "0123456789abcdef" for c in digest)
    matched = bool(recon.get("matched") or recon.get("checksum_matched"))
    return {
        "checksum_algorithm": "sha256",
        "checksum": digest if full else "",
        "checksum_hex_chars": len(digest) if digest else 0,
        "full_digest": full,
        "checksum_matched": matched if full else False,
        "sample_override_forbidden": True,
    }


def build_migration_proof_pack(
    *,
    decision_artifact: Mapping[str, Any] | None,
    reconciliation: Mapping[str, Any] | None = None,
    validation_summary: Mapping[str, Any] | None = None,
    connector_versions: Mapping[str, Any] | None = None,
    job_id: str = "",
    job_success: bool = False,
) -> dict[str, Any]:
    """Compose an explainable proof pack from kernel authorities.

    ``migration_proven`` is True only when: job succeeded, artifact hash present,
    full 256-bit checksum present and matched, and validation proof class is clear.
    """
    art = dict(decision_artifact or {})
    art_hash = str(art.get("content_hash") or "").strip().lower()
    ddl_hash = str((art.get("ddl") or {}).get("ddl_identity_hash") or "").strip()
    checksum = extract_population_checksum(reconciliation)
    val = dict(validation_summary or {})
    proof_blocked = _proof_class_blocked(val)

    migration_proven = bool(
        job_success
        and len(art_hash) == 64
        and checksum.get("full_digest")
        and checksum.get("checksum_matched")
        and not proof_blocked
    )

    return {
        "proof_engine_version": PROOF_ENGINE_VERSION,
        "decision_artifact_schema": art.get("schema_version") or DECISION_ARTIFACT_SCHEMA,
        "decision_artifact_hash": art_hash or None,
        "ddl_identity_hash": ddl_hash or None,
        "proof_plan": build_proof_plan().to_dict(),
        "population_checksum": checksum,
        "validation": {
            "decision_artifact_present": bool(val.get("decision_artifact_present")),
            "blocked_classes": list(val.get("blocked_classes") or []),
            "population_note": val.get("population_note"),
        },
        "connector_versions": dict(connector_versions or {}),
        "job_id": job_id,
        "job_success": job_success,
        "migration_proven": migration_proven,
        "assurance": {
            "level": "migration_proven" if migration_proven else "incomplete",
            "reasons": _incomplete_reasons(
                art_hash=art_hash,
                checksum=checksum,
                job_success=job_success,
                proof_blocked=proof_blocked,
            ),
        },
    }


def _proof_class_blocked(val: Mapping[str, Any]) -> bool:
    blocked = set(val.get("blocked_classes") or [])
    if "proof" in blocked:
        return True
    if "decision_artifact_present" in val and val.get("decision_artifact_present") is False:
        return True
    return False


def _incomplete_reasons(
    *,
    art_hash: str,
    checksum: Mapping[str, Any],
    job_success: bool,
    proof_blocked: bool,
) -> list[str]:
    reasons: list[str] = []
    if not job_success:
        reasons.append("job_not_successful")
    if len(art_hash) != 64:
        reasons.append("decision_artifact_hash_missing")
    if not checksum.get("full_digest"):
        reasons.append("population_checksum_not_full_sha256")
    elif not checksum.get("checksum_matched"):
        reasons.append("population_checksum_mismatch")
    if proof_blocked:
        reasons.append("validation_proof_class_blocked")
    return reasons


def attach_artifact_to_signed_pack(
    signed_pack: Mapping[str, Any] | None,
    *,
    decision_artifact: Mapping[str, Any] | None,
    validation_summary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Augment an existing signed_proof_pack with kernel Decision Artifact fields."""
    pack = dict(signed_pack or {})
    kernel = build_migration_proof_pack(
        decision_artifact=decision_artifact,
        reconciliation=pack.get("reconciliation")
        if isinstance(pack.get("reconciliation"), dict)
        else pack.get("gate8"),
        validation_summary=validation_summary,
        connector_versions=pack.get("connector_versions")
        if isinstance(pack.get("connector_versions"), dict)
        else None,
        job_id=str(pack.get("job_id") or ""),
        job_success=bool(pack.get("job_success") or pack.get("success")),
    )
    # Never let sample/preflight green override kernel population refusal.
    if pack.get("migration_proven") and not kernel["migration_proven"]:
        pack["migration_proven"] = False
    pack["decision_kernel_proof"] = kernel
    pack["decision_artifact_hash"] = kernel.get("decision_artifact_hash")
    return pack


__all__ = [
    "PROOF_ENGINE_VERSION",
    "attach_artifact_to_signed_pack",
    "build_migration_proof_pack",
    "build_proof_plan",
    "extract_population_checksum",
]
