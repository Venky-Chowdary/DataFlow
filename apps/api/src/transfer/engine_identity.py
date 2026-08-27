"""Map→DDL identity and Decision Artifact authority for the transfer engine.

Split out of ``engine.py`` (a god module over its size budget). Every helper here
answers one question before a write: is the mapping set about to be executed the
same one a human approved? They fail closed — a missing or crashed check refuses
the write rather than passing it through unproven.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def _inline_stamp_ddl_identity(mappings: list, dest_db: str) -> str | None:
    """Stamp Map→DDL fingerprint for programmatic skip_preflight callers.

    Returns None on success, or an error message when the stamp cannot be built.
    """
    try:
        from services.decision_kernel import approved_mapping_ddl_fingerprint

        stamped = approved_mapping_ddl_fingerprint(mappings, dest_db=dest_db or "")
        if not str(stamped or "").strip():
            return (
                "DDL identity inline stamp produced an empty fingerprint — "
                "refuse write (check Map target_type stamps)."
            )
    except Exception as exc:
        # Fail closed with the exception attached — never soft-pass invent.
        logger.error("DDL identity inline stamp failed: %s", exc, exc_info=exc)
        return f"DDL identity inline stamp failed closed: {exc}"
    return None


def _enforce_ddl_identity(
    pf: dict | None,
    mappings: list,
    *,
    dest_db: str,
    approved_ddl_identity_hash: str = "",
    skip_preflight: bool = False,
    preflight_mappings: list | None = None,
) -> str | None:
    """Module 12 / GA — fail closed when Map→DDL fingerprint drifts after Validate.

    Returns an error message when identity fails.

    Programmatic callers (``skip_preflight=True``: API/CLI/scheduler/tests) may
    omit a Validate fingerprint: the engine stamps Map→DDL **inline** from the
    current mappings. That applies when preflight is absent **or** when a stub
    proof_bundle lacks ``ddl_identity_hash`` (incomplete Validate must not block
    skip_preflight callers — audit ITEM 2).

    UI Validate→Execute (``skip_preflight=False``) still requires a stamped hash
    from preflight proof or ``approved_ddl_identity_hash``. When a hash is
    present, drift vs current mappings is always refused.

    A fingerprint is only meaningful against the mapping set it was taken over.
    The operator's hash (from Validate) is checked against the operator contract
    rows; Execute's *own* preflight hash is checked against the rows that
    preflight ran on. Crossing them refused every UI job whose destination
    catalog spells a bound column differently from the Map stamp.
    """
    has_maps = bool(mappings)
    operator_approved = (approved_ddl_identity_hash or "").strip()
    approved = operator_approved
    approved_columns: list[dict] = []
    checked = mappings
    if not approved and pf:
        stamp = (pf.get("proof_bundle") or {}).get("ddl_identity") or {}
        approved = stamp.get("ddl_identity_hash") or ""
        approved_columns = [
            c for c in (stamp.get("columns") or []) if isinstance(c, dict)
        ]
        if preflight_mappings is not None:
            checked = preflight_mappings

    if not approved:
        if has_maps and skip_preflight:
            # Programmatic path — inline stamp whether or not a hollow pf exists.
            return _inline_stamp_ddl_identity(mappings, dest_db)
        if pf and has_maps:
            return (
                "DDL identity fingerprint missing after Validate — refuse Execute "
                "(Map→DDL identity not stamped; re-run Validate)."
            )
        if has_maps:
            return (
                "DDL identity requires Validate preflight before Execute — "
                "refuse write without Map→DDL fingerprint (re-run Validate)."
            )
        return None

    try:
        from services.decision_kernel import DdlIdentityError, assert_ddl_identity

        assert_ddl_identity(
            str(approved),
            checked,
            dest_db=dest_db or "",
            approved_columns=approved_columns,
        )
    except DdlIdentityError as exc:
        return str(exc)
    except Exception as exc:  # pragma: no cover — never invent soft-pass on check crash
        logger.error("DDL identity check crashed: %s", exc, exc_info=exc)
        return f"DDL identity check failed closed: {exc}"
    return None


def _request_decision_artifact_payload(request) -> dict | None:
    raw = getattr(request, "decision_artifact", None)
    if isinstance(raw, dict) and raw:
        return raw
    return None


def _operator_contract_maps(request, mappings: list) -> list:
    """Mappings an operator-stamped artifact/fingerprint was hashed over.

    Validate hashes the Map rows the operator approved (``request.mappings``).
    Execute re-derives its own set (``_auto_map`` → enrich → auto-propagate →
    additive stamps), so hashing the derived set compared a stamp against
    facts the operator never saw: an untouched Map came back as "Decision
    Artifact DDL identity diverged from current Map". Whenever the caller
    supplies a stamp, it must be checked against the contract it was taken
    over; only an unstamped run falls back to the derived set.
    """
    supplied = bool(
        str(getattr(request, "approved_ddl_identity_hash", "") or "").strip()
        or str(getattr(request, "approved_decision_artifact_hash", "") or "").strip()
        or _request_decision_artifact_payload(request)
    )
    if not supplied:
        return mappings
    return list(getattr(request, "mappings", None) or []) or mappings


def _enforce_decision_artifact(
    pf: dict | None,
    mappings: list,
    *,
    dest_db: str,
    approved_decision_artifact_hash: str = "",
    decision_artifact: dict | None = None,
    skip_preflight: bool = False,
    sync_mode: str = "full_refresh_overwrite",
    error_policy: str = "quarantine",
) -> tuple[str | None, dict | None]:
    """Phase C11 — refuse Execute without Decision Artifact authority.

    Returns ``(error, artifact_dict)``. Programmatic ``skip_preflight`` stamps
    an inline artifact (parity with DDL identity). Validate paths may carry
    ``proof_bundle.decision_artifact`` or ``approved_decision_artifact_hash``.
    """
    from services.decision_kernel import enforce_decision_artifact

    approved = (approved_decision_artifact_hash or "").strip()
    payload = decision_artifact if isinstance(decision_artifact, dict) and decision_artifact else None
    if pf and not payload:
        pb = (pf.get("proof_bundle") or {}).get("decision_artifact")
        if isinstance(pb, dict) and pb:
            payload = pb
    if pf and not approved:
        approved = str(
            ((pf.get("proof_bundle") or {}).get("decision_artifact") or {}).get(
                "content_hash"
            )
            or (pf.get("proof_bundle") or {}).get("decision_artifact_hash")
            or ""
        ).strip()
    # C11: UI Validate→Execute requires a Decision Artifact (or hash).
    # Programmatic skip_preflight may inline-stamp even when proof_bundle is a
    # hollow stub — same honesty as DDL identity (audit ITEM 2).
    if pf and not approved and not payload and not skip_preflight:
        return (
            "Decision Artifact missing from Validate proof_bundle — refuse Execute "
            "(re-run Validate to stamp decision_artifact.content_hash).",
            None,
        )
    err, art = enforce_decision_artifact(
        mappings=list(mappings or []),
        dest_db=dest_db or "",
        approved_content_hash=approved,
        artifact_payload=payload,
        skip_preflight=bool(skip_preflight),
        sync_mode=sync_mode,
        error_policy=error_policy,
    )
    return err, (art.to_dict() if art is not None else None)


def reuse_approved_validate_population_fit(request) -> bool:
    """True when Execute should confirm Validate, not re-walk the source.

    Studio Validate already ran mapping, schema, dest access, and a bounded
    population-fit scan (Studio time budget). Execute still probes dest,
    enforces the Decision Artifact / Map→DDL fingerprint, and binds
    write-time ``fits_decimal`` on every row. Re-walking 1M rows before the
    first batch is the same question asked twice — not a second proof.

    Never set ``skip_preflight``. Missing or short artifact hashes still
    walk (API / scheduler / tests without a stamped Validate).
    """
    if bool(getattr(request, "skip_preflight", False)):
        return False
    digest = str(getattr(request, "approved_decision_artifact_hash", "") or "").strip()
    if len(digest) != 64:
        return False
    maps = list(getattr(request, "mappings", None) or [])
    return bool(maps)


def execute_preflight_progress_message(request) -> str:
    """Operator-facing Run copy — do not say Validate is running again."""
    if reuse_approved_validate_population_fit(request):
        return (
            "Confirming approved Validate — mapping and schema already passed. "
            "Write-time fit still binds every row."
        )
    return "Validating mapping and schema…"
