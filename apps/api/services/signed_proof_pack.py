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


class FidelityVeto:
    """A write that landed but is not full-fidelity.

    One decision procedure. Gate-8 stamping, ladder attach, and the proof pack
    all consult this — they must not grow parallel if-trees for the same
    evidence (a failed column profile, a value coerced to NULL).
    """

    __slots__ = ("claim_level", "phase", "operational_passed", "assurance_level", "coverage", "note")

    def __init__(
        self,
        *,
        claim_level: str,
        phase: str,
        operational_passed: bool,
        assurance_level: str,
        coverage: str,
        note: str,
    ) -> None:
        self.claim_level = claim_level
        self.phase = phase
        self.operational_passed = operational_passed
        self.assurance_level = assurance_level
        self.coverage = coverage
        self.note = note

    def as_claim(self, checksum_match: bool) -> dict[str, Any]:
        return {
            "claim_level": self.claim_level,
            "post_write_verified": False,
            "migration_proven": False,
            "population_proof": False,
            "referential_integrity_proven": False,
            "checksum_match": checksum_match,
            "note": self.note,
        }


def _append_delta_scope(recon: dict[str, Any]) -> bool:
    """True when Gate-8 already judged incomparable whole-table hashes via dest-before."""
    from services.reconcile_coverage import WHOLE_TABLE_NOT_COMPARABLE

    scope = str(recon.get("checksum_scope") or "")
    coverage = str(recon.get("coverage") or recon.get("assurance_level") or "").lower()
    phase = str(recon.get("phase") or "").lower()
    return (
        scope == WHOLE_TABLE_NOT_COMPARABLE
        or coverage == "row_count"
        or phase.endswith("row_count")
        or "post_write_row_count" in phase
    )


def _ladder_fail_veto(ladder: dict[str, Any]) -> FidelityVeto:
    loc = str(ladder.get("localization_summary") or "").strip()
    note = "Column-profile / verification ladder failed — migration not proven."
    if loc:
        note = f"{note} {loc}"
    return FidelityVeto(
        claim_level="failed",
        phase="post_write_failed",
        operational_passed=False,
        assurance_level="none",
        coverage="none",
        note=note,
    )


def fidelity_veto(recon: dict[str, Any]) -> FidelityVeto | None:
    """Return a veto when the destination does not hold the source values.

    Operational ``passed`` (rows landed) is a different question — a coerced
    write can complete and still be forbidden from claiming ``migration_proven``.
    """
    from services.reconcile_coverage import WRITTEN_BATCH_KEYS

    ladder = recon.get("verification_ladder") if isinstance(recon.get("verification_ladder"), dict) else {}
    if ladder and not ladder.get("skipped") and ladder.get("passed") is False:
        skip_veto = False
        if bool(recon.get("passed")):
            layers = ladder.get("layers") or {}
            l1_ok = (layers.get("L1") or {}).get("passed") is not False
            l3_ok = (layers.get("L3") or {}).get("passed") is not False
            # Dest-before already closed; whole-table hashes are not a cell failure.
            if _append_delta_scope(recon) and l1_ok:
                skip_veto = True
            # Keyed-batch cells matched; extra dest rows sit outside that proof.
            elif (
                str(recon.get("checksum_scope") or "") == WRITTEN_BATCH_KEYS
                and recon.get("checksum_match") is True
                and l3_ok
            ):
                skip_veto = True
        if not skip_veto:
            return _ladder_fail_veto(ladder)
    coverage = str(recon.get("coverage") or "").lower()
    phase = str(recon.get("phase") or "").lower()
    # Only veto a write that operationally completed. A failed run with
    # coerced_null_rows is still a failure, not a "partial success".
    if bool(recon.get("passed", True)) and (
        int(recon.get("coerced_null_rows") or 0) > 0
        or coverage == "coerced"
        or phase.endswith("partial")
    ):
        return FidelityVeto(
            claim_level="coerced",
            phase="post_write_partial",
            operational_passed=True,
            assurance_level="coerced",
            coverage="coerced",
            note=(
                "Values were coerced to NULL to land the write — checksums can "
                "still match because the same coercion is applied on the source "
                "re-read. That is not full fidelity."
            ),
        )
    return None


def apply_fidelity_veto(report: dict[str, Any]) -> dict[str, Any]:
    """Stamp a veto onto a Gate-8 report, or return it unchanged."""
    out = dict(report or {})
    veto = fidelity_veto(out)
    if veto is None:
        return out
    out["phase"] = veto.phase
    out["passed"] = veto.operational_passed
    out["assurance_level"] = veto.assurance_level
    out["coverage"] = veto.coverage
    out["migration_proven"] = False
    out["post_write_pending"] = False
    out["preview"] = False
    if not veto.operational_passed:
        loc = ""
        ladder = out.get("verification_ladder") if isinstance(out.get("verification_ladder"), dict) else {}
        loc = str(ladder.get("localization_summary") or "").strip()
        if loc:
            base = str(out.get("message") or "").rstrip()
            if loc not in base:
                out["message"] = f"{base} — {loc}" if base else loc
    return out


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


def sign_body(body: dict[str, Any], *, subject: str) -> dict[str, Any]:
    """Return ``body`` plus its content hash and HMAC over ``hash:subject``.

    The subject binds a signature to the thing it describes, so a pack signed
    for one job cannot be replayed as evidence for another.
    """
    content_sha256 = sha256_hex(canonical_json(body))
    return {
        **body,
        "content_sha256": content_sha256,
        "signature": {
            "alg": "HMAC-SHA256",
            "key_id": "platform_auth_secret",
            "value": hmac_sha256_hex(_platform_secret(), f"{content_sha256}:{subject}"),
        },
    }


def verify_body(payload: dict[str, Any], *, subject: str) -> tuple[str, list[str]]:
    """Recompute hash + HMAC for a signed body. Returns (actual_hash, errors)."""
    errors: list[str] = []
    body = {k: v for k, v in payload.items() if k not in ("content_sha256", "signature")}
    actual_hash = sha256_hex(canonical_json(body))
    if str(payload.get("content_sha256") or "") != actual_hash:
        errors.append("content_sha256 mismatch")
    sig = payload.get("signature") if isinstance(payload.get("signature"), dict) else {}
    claimed = str(sig.get("value") or "")
    expected = hmac_sha256_hex(_platform_secret(), f"{actual_hash}:{subject}")
    if not claimed or not hmac.compare_digest(claimed, expected):
        errors.append("HMAC signature invalid")
    return actual_hash, errors


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

    veto = fidelity_veto(recon)
    if veto is not None:
        return veto.as_claim(checksum_match)

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

    if _append_delta_scope(recon) and passed:
        return {
            "claim_level": "row_count",
            "post_write_verified": True,
            "migration_proven": False,
            "population_proof": False,
            "referential_integrity_proven": False,
            "checksum_match": False,
            "note": (
                "Append/upsert dest-before delta verified — whole-table digests "
                "are not comparable. Per-cell / population fidelity is not proven."
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


def collect_accepted_risks_from_job(job: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Harvest Migration Risk Contracts from job mappings / preflight (deduped by risk_id)."""
    if not isinstance(job, dict):
        return []
    seen: set[str] = set()
    out: list[dict[str, Any]] = []

    def _absorb(raw: Any) -> None:
        if not isinstance(raw, dict):
            return
        rid = str(raw.get("risk_id") or "").strip()
        key = rid or canonical_json(raw)
        if key in seen:
            return
        seen.add(key)
        out.append(dict(raw))

    for m in job.get("mappings") or []:
        if isinstance(m, dict):
            _absorb(m.get("risk_contract") or m.get("riskContract"))
    pf = job.get("preflight") if isinstance(job.get("preflight"), dict) else {}
    for m in pf.get("mappings") or []:
        if isinstance(m, dict):
            _absorb(m.get("risk_contract") or m.get("riskContract"))
    pb = pf.get("proof_bundle") if isinstance(pf.get("proof_bundle"), dict) else {}
    for raw in pb.get("accepted_risks") or pb.get("risk_contracts") or []:
        _absorb(raw)
    for raw in job.get("accepted_risks") or []:
        _absorb(raw)
    return out


def execution_policies_from_risks(accepted_risks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One execution-policy stamp per accepted risk (auditable, not inferred)."""
    policies: list[dict[str, Any]] = []
    for r in accepted_risks or []:
        if not isinstance(r, dict):
            continue
        policies.append(
            {
                "risk_id": r.get("risk_id"),
                "column": r.get("column") or r.get("target"),
                "execution_policy": r.get("execution_policy"),
                "quarantine_policy": r.get("quarantine_policy"),
                "retry_policy": r.get("retry_policy"),
                "rollback_strategy": r.get("rollback_strategy"),
            }
        )
    return policies


def _execution_policy_semantics_for_proof() -> dict[str, Any]:
    """Embed runtime policy semantics so proof packs cannot outrun the writer."""
    try:
        from services.migration_risk_contract import execution_policy_semantics

        return execution_policy_semantics()
    except Exception:
        return {}


def mapping_risk_contracts_expected(job: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Risk contracts present on job mappings (expected in Proof Pack)."""
    if not isinstance(job, dict):
        return []
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for m in job.get("mappings") or []:
        if not isinstance(m, dict):
            continue
        raw = m.get("risk_contract") or m.get("riskContract")
        if not isinstance(raw, dict):
            continue
        rid = str(raw.get("risk_id") or "").strip()
        key = rid or canonical_json(raw)
        if key in seen:
            continue
        seen.add(key)
        out.append(dict(raw))
    return out


def proof_pack_risk_completeness_errors(
    *,
    accepted_risks: list[dict[str, Any]] | None,
    expected_from_mappings: list[dict[str, Any]] | None = None,
    job_success: bool = False,
) -> list[str]:
    """Fail-closed checks: mapping contracts must appear in accepted_risks.

    Incomplete packs may still be signed for audit, but must never claim
    ``migration_proven`` (caller strips that). Errors are also surfaced on verify.
    """
    errors: list[str] = []
    accepted = [r for r in (accepted_risks or []) if isinstance(r, dict)]
    expected = [r for r in (expected_from_mappings or []) if isinstance(r, dict)]
    if not expected:
        return errors
    accepted_ids = {
        str(r.get("risk_id") or "").strip() for r in accepted if str(r.get("risk_id") or "").strip()
    }
    missing = []
    for exp in expected:
        rid = str(exp.get("risk_id") or "").strip()
        if rid and rid not in accepted_ids:
            missing.append(rid)
        elif not rid and not accepted:
            missing.append(str(exp.get("column") or "?"))
    if missing:
        errors.append(
            "accepted_risks incomplete vs mapping Risk Contracts: "
            + ", ".join(missing[:8])
            + (f" (+{len(missing) - 8} more)" if len(missing) > 8 else "")
        )
    if job_success and expected and not accepted:
        errors.append(
            "successful job exported without accepted_risks while mappings "
            "carry Risk Contracts — refuse proof completeness"
        )
    return errors


def proof_pack_evidence_completeness_errors(
    *,
    job_success: bool,
    ddl_hash: str | None,
    mapping_hash: str | None,
    connector_versions: dict[str, Any] | None,
    reconciliation: dict[str, Any] | None,
    claim_migration_proven: bool,
) -> list[str]:
    """Refuse hollow proven packs — hashes / connector attribution must exist."""
    if not job_success and not claim_migration_proven:
        return []
    errors: list[str] = []
    ddl = str(ddl_hash or "").strip()
    mph = str(mapping_hash or "").strip()
    if claim_migration_proven and not ddl and not mph:
        errors.append(
            "migration_proven refused: ddl_hash and mapping_hash both absent"
        )
    if claim_migration_proven:
        recon = reconciliation if isinstance(reconciliation, dict) else {}
        src = str(recon.get("source_checksum") or "").strip()
        tgt = str(recon.get("target_checksum") or "").strip()
        if not src or not tgt or src != tgt:
            errors.append(
                "migration_proven refused: independent matching checksums required"
            )
    versions = connector_versions if isinstance(connector_versions, dict) else {}
    if claim_migration_proven and not versions:
        errors.append("migration_proven refused: connector_versions absent")
    return errors


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
    rollback_plan: dict[str, Any] | None = None,
    risk_contracts: list[dict[str, Any]] | None = None,
    expected_risks_from_mappings: list[dict[str, Any]] | None = None,
    job_success: bool = False,
    require_risk_completeness: bool | None = None,
) -> dict[str, Any]:
    """Build a signed proof pack for a completed (or failed) job.

    Always stamps ``assurance`` from Gate-8. Incomplete packs are still signed
    for audit chain integrity — they must not set ``migration_proven``.
    """
    assurance = classify_post_write_assurance(reconciliation)
    risks = list(accepted_risks or risk_contracts or [])
    policies = execution_policies_from_risks(risks)
    completeness_errors = proof_pack_risk_completeness_errors(
        accepted_risks=risks,
        expected_from_mappings=expected_risks_from_mappings,
        job_success=job_success,
    )
    assurance = dict(assurance)
    claim_proven = bool(assurance.get("migration_proven"))
    completeness_errors.extend(
        proof_pack_evidence_completeness_errors(
            job_success=job_success,
            ddl_hash=ddl_hash,
            mapping_hash=mapping_hash,
            connector_versions=connector_versions,
            reconciliation=reconciliation if isinstance(reconciliation, dict) else None,
            claim_migration_proven=claim_proven,
        )
    )
    if completeness_errors:
        # Never allow incomplete harvest / hollow evidence to keep proven claim.
        assurance["migration_proven"] = False
        if assurance.get("claim_level") == "full_checksum":
            assurance["claim_level"] = "incomplete_proof_evidence"
        assurance["proof_incomplete_reasons"] = list(completeness_errors)
    rb = rollback_plan if isinstance(rollback_plan, dict) and rollback_plan else {
        "strategy": "DOCUMENT_ONLY",
        "executable": False,
        "population_undo_claimed": False,
        "note": "No signed rollback plan — warehouse restore not productized.",
    }
    require_complete = (
        bool(require_risk_completeness)
        if require_risk_completeness is not None
        else bool(job_success and (completeness_errors or expected_risks_from_mappings))
    )
    versions = dict(connector_versions or {})
    # Honesty: format/kind fallback is not a package version string.
    versions_are_format_only = bool(versions) and all(
        isinstance(v, str) and not any(ch.isdigit() for ch in v)
        for v in versions.values()
    )
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
        "accepted_risks": risks,
        "risk_contracts": risks,
        "execution_policies": policies,
        "execution_policy_semantics": _execution_policy_semantics_for_proof(),
        "proof_incomplete_reasons": list(completeness_errors),
        "require_risk_completeness": require_complete,
        "rejected_rows_count": len(rejected_rows or []),
        "rejected_rows_sample": list(rejected_rows or [])[:50],
        "connector_versions": versions,
        "connector_versions_honesty": (
            "format_or_kind_only"
            if versions_are_format_only
            else ("absent" if not versions else "provided")
        ),
        "rollback_plan": rb,
        "hashes": {
            "ddl_hash": ddl_hash,
            "mapping_hash": mapping_hash,
            "transformation_hash": transformation_hash,
            "source_checksum": (reconciliation or {}).get("source_checksum")
            if isinstance(reconciliation, dict)
            else None,
            "target_checksum": (reconciliation or {}).get("target_checksum")
            if isinstance(reconciliation, dict)
            else None,
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
    return sign_body(body, subject=job_id)


def verify_signed_proof_pack(pack: dict[str, Any]) -> dict[str, Any]:
    """Verify content hash + HMAC. Returns {ok, errors[]}."""
    errors: list[str] = []
    if not isinstance(pack, dict):
        return {"ok": False, "errors": ["pack must be an object"]}
    actual_hash, sig_errors = verify_body(pack, subject=str(pack.get("job_id") or ""))
    errors.extend(sig_errors)
    assurance = pack.get("assurance") if isinstance(pack.get("assurance"), dict) else {}
    if assurance.get("migration_proven") and assurance.get("claim_level") != "full_checksum":
        errors.append("migration_proven claimed without full_checksum assurance")
    incomplete = pack.get("proof_incomplete_reasons") or assurance.get(
        "proof_incomplete_reasons"
    )
    if incomplete:
        if assurance.get("migration_proven"):
            errors.append("migration_proven claimed while proof_incomplete_reasons present")
        # Completeness errors are informational for failed/unsigned harvests;
        # only fail verify when a green proven claim was attempted (above) or
        # when pack explicitly marks completeness as required.
        if pack.get("require_risk_completeness") is True:
            for reason in incomplete:
                errors.append(str(reason))
    return {"ok": not errors, "errors": errors, "content_sha256": actual_hash}


def export_proof_pack_for_job(job: dict[str, Any], *, actor: str = "system") -> dict[str, Any]:
    """Convenience: pull Gate-8 + mapping proof + risk contracts off a job document."""
    from services.audit_log import latest_event_hash

    prev = None
    try:
        prev = latest_event_hash()
    except Exception:
        prev = None
    dest = job.get("destination_summary") if isinstance(job.get("destination_summary"), dict) else {}
    rejected = dest.get("rejected_details") or job.get("rejected_details") or []
    mapping_proof = job.get("mapping_proof") if isinstance(job.get("mapping_proof"), dict) else {}
    accepted = collect_accepted_risks_from_job(job)
    expected_risks = mapping_risk_contracts_expected(job)
    # If mappings were stripped but accepted_risks stamped at execute, expected
    # may be empty — still fine. If mappings carry contracts and harvest is
    # empty, completeness errors strip migration_proven.
    rollback = dest.get("rollback_plan") if isinstance(dest.get("rollback_plan"), dict) else {}
    if not rollback and isinstance(job.get("rollback_plan"), dict):
        rollback = job["rollback_plan"]
    connector_versions = {}
    if isinstance(job.get("connector_versions"), dict):
        connector_versions = dict(job["connector_versions"])
    else:
        for key in ("source_connector_version", "destination_connector_version"):
            if job.get(key):
                connector_versions[key] = job[key]
        src = job.get("source") if isinstance(job.get("source"), dict) else {}
        dst = job.get("destination") if isinstance(job.get("destination"), dict) else {}
        if src.get("format") or src.get("kind"):
            connector_versions["source"] = src.get("format") or src.get("kind")
        if dst.get("format") or dst.get("kind"):
            connector_versions["destination"] = dst.get("format") or dst.get("kind")
    pf = job.get("preflight") if isinstance(job.get("preflight"), dict) else {}
    ddl_identity = (
        (pf.get("proof_bundle") or {}).get("ddl_identity")
        if isinstance(pf.get("proof_bundle"), dict)
        else {}
    )
    ddl_hash = (
        str(dest.get("ddl_hash") or "")
        or str((ddl_identity or {}).get("ddl_identity_hash") or "")
        or None
    )
    xform_hash = str(dest.get("transformation_hash") or job.get("transformation_hash") or "") or None
    job_success = str(job.get("status") or "").lower() in {
        "completed",
        "completed_with_quarantine",
        "success",
        "succeeded",
    }
    return build_signed_proof_pack(
        job_id=str(job.get("_id") or job.get("id") or ""),
        reconciliation=job.get("reconciliation") if isinstance(job.get("reconciliation"), dict) else None,
        mapping_proof=mapping_proof or None,
        preflight_summary=(
            {
                "passed": pf.get("passed"),
                "decision": pf.get("decision")
                or ((pf.get("proof_bundle") or {}).get("transfer_decision") or {}).get("decision"),
                "passed_count": pf.get("passed_count"),
                "total_gates": pf.get("total_gates"),
                "readiness_score": pf.get("readiness_score"),
                # Which source columns the operator chose not to carry. Without
                # it the pack cannot distinguish a declared omission from a drop.
                "source_coverage": (
                    pf.get("source_coverage")
                    or (pf.get("proof_bundle") or {}).get("source_coverage")
                    or {}
                ),
            }
            if pf
            else None
        ),
        actor=actor,
        prev_audit_hash=prev,
        validation_mode=str(job.get("validation_mode") or dest.get("validation_mode") or "") or None,
        accepted_risks=accepted,
        rejected_rows=list(rejected) if isinstance(rejected, list) else [],
        connector_versions=connector_versions,
        ddl_hash=ddl_hash,
        mapping_hash=str(mapping_proof.get("mapping_hash") or "") or None,
        transformation_hash=xform_hash,
        rollback_plan=rollback or None,
        expected_risks_from_mappings=expected_risks,
        job_success=job_success,
        require_risk_completeness=bool(job_success and expected_risks)
        or bool(job_success and not accepted and expected_risks),
    )
