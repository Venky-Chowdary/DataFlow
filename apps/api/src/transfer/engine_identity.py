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


def _enforce_ddl_identity_once(
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
        if preflight_mappings is not None:
            try:
                from services.decision_kernel import ddl_identity_report

                proof_stamp = ddl_identity_report(
                    preflight_mappings, dest_db=dest_db or ""
                )
                pf.setdefault("proof_bundle", {})["ddl_identity"] = proof_stamp
            except Exception as exc:
                logger.error("Execute DDL identity stamp failed: %s", exc, exc_info=exc)
                return f"DDL identity stamp failed closed: {exc}"
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
    rows = list(getattr(request, "mappings", None) or []) or mappings
    out = verified_identity_maps(rows, getattr(request, "_g11_adoption", None))
    from services.conversion_contract import bind_stream_maps

    # Every stream's own Map rows are part of the approval (security re-check F1c).
    out.stream_maps = bind_stream_maps(
        out, list(getattr(request, "stream_contracts", None) or []),
        dest_db=str(getattr(getattr(request, "destination", None), "format", "") or ""),
    ).stream_maps
    return out


def decision_artifact_source(src, fallback_format: str = "") -> dict[str, str]:
    """``source_kind`` / ``source_format`` kwargs the Decision Artifact was bound to."""
    from .endpoint_kind import artifact_source_identity

    kind, fmt = artifact_source_identity(src, fallback_format)
    return {"source_kind": kind, "source_format": fmt}


def _enforce_decision_artifact_once(
    pf: dict | None,
    mappings: list,
    *,
    dest_db: str,
    approved_decision_artifact_hash: str = "",
    decision_artifact: dict | None = None,
    skip_preflight: bool = False,
    sync_mode: str = "full_refresh_overwrite",
    error_policy: str = "quarantine",
    dest_fingerprint: str = "",
    destination_column_types: dict | None = None,
    destination_table_exists: bool | None = None,
    source_fingerprint: str = "",
    source_column_types: dict | None = None,
    source_kind: str = "",
    source_format: str = "",
    source_connector_id: str = "",
) -> tuple[str | None, dict | None]:
    """Phase C11 — refuse Execute without Decision Artifact authority.

    Returns ``(error, artifact_dict)``. Programmatic ``skip_preflight`` stamps
    an inline artifact (parity with DDL identity). Validate paths may carry
    ``proof_bundle.decision_artifact`` or ``approved_decision_artifact_hash``.
    """
    from services.decision_kernel import enforce_decision_artifact

    approved = (approved_decision_artifact_hash or "").strip()
    payload = decision_artifact if isinstance(decision_artifact, dict) and decision_artifact else None
    if pf and not payload and not approved:
        proof_bundle = pf.get("proof_bundle") or {}
        stamped = proof_bundle.get("decision_artifact")
        if isinstance(stamped, dict) and stamped:
            try:
                from services.decision_kernel import (
                    build_artifact_from_mappings,
                    decision_artifact_from_dict,
                )

                prior = decision_artifact_from_dict(stamped)
                refreshed = build_artifact_from_mappings(
                    mappings if isinstance(mappings, list) else list(mappings or []),
                    dest_db=dest_db or "",
                    source_db=source_format,
                    tenant_id=prior.tenant_id,
                    route_id=prior.route_id,
                    source_fingerprint=prior.source_fingerprint,
                    dest_fingerprint=prior.dest_fingerprint,
                    sync_mode=sync_mode or prior.sync_mode,
                    error_policy=error_policy or prior.error_policy,
                    artifact_id=prior.artifact_id,
                    created_at=prior.created_at,
                ).to_dict()
                proof_bundle["decision_artifact"] = refreshed
                proof_bundle["decision_artifact_hash"] = refreshed.get("content_hash")
                pf["proof_bundle"] = proof_bundle
            except Exception as exc:
                logger.error(
                    "Execute decision artifact stamp failed: %s",
                    exc,
                    exc_info=exc,
                )
                return f"Decision Artifact stamp failed closed: {exc}", None
    # Honor the operator Validate stamp. Execute re-preflight after dest exists
    # overlays live dest types and a non-empty dest fingerprint. Adopting that
    # artifact as "supplied" compared dest-exists DDL to the operator Map and
    # refused every later schedule beat ("DDL identity diverged from current Map").
    if pf and not payload and not approved:
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
    from services.data_profiler import source_types_are_authoritative
    from services.schema_fingerprint import (
        live_dest_schema_fingerprint,
        live_source_schema_fingerprint,
    )

    dest_fp = (dest_fingerprint or "").strip() or live_dest_schema_fingerprint(
        destination_column_types,
        destination_table_exists=destination_table_exists,
        sync_mode=sync_mode,
    )
    src_fp = (source_fingerprint or "").strip() or live_source_schema_fingerprint(
        source_column_types,
        authoritative=source_types_are_authoritative(source_kind, source_format),
    )
    dest_names: list[str] = []
    if isinstance(destination_column_types, dict):
        dest_names = [
            str(k) for k in destination_column_types.keys() if str(k).strip()
        ]
    source_names: list[str] = []
    if isinstance(source_column_types, dict):
        source_names = [
            str(k) for k in source_column_types.keys() if str(k).strip()
        ]
    err, art = enforce_decision_artifact(
        mappings=mappings if isinstance(mappings, list) else list(mappings or []),
        dest_db=dest_db or "",
        approved_content_hash=approved,
        artifact_payload=payload,
        skip_preflight=bool(skip_preflight),
        sync_mode=sync_mode,
        error_policy=error_policy,
        dest_fingerprint=dest_fp,
        source_fingerprint=src_fp,
        source_db=source_format,
        source_connector_id=source_connector_id,
        destination_table_exists=destination_table_exists,
        dest_column_names=dest_names,
        source_column_names=source_names,
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


# -- G11: approvals stamped over the operator's Map before legacy adoption -------


from services.conversion_contract import StreamBoundMaps  # noqa: E402


class IdentityMaps(StreamBoundMaps):
    """Map rows for the identity checks, with the adoption the server proved
    and each stream's own rows (``stream_maps``, bound into both hashes)."""

    verified_adoption: list[dict] | None = None


def verified_identity_maps(rows: list, adoption: dict | None) -> IdentityMaps:
    """``rows`` plus the legacy-column adoption Execute made and can prove.

    ``adoption`` is the server's record (``request._g11_adoption``: the
    destination and the live columns adopt_for_request read). The adoption
    counts only when re-running ``adopt_existing_columns`` over the
    operator's rows and those live columns reproduces ``rows`` exactly;
    a ``map_target`` the server did not produce never does (final review B1).
    """
    from services.target_name_legacy import MAP_TARGET, adopt_existing_columns, operator_rows

    out = IdentityMaps(rows or [])
    marked = [m for m in out if isinstance(m, dict) and m.get(MAP_TARGET)]
    if not marked or not isinstance(adoption, dict):
        return out
    again, _notes = adopt_existing_columns(
        adoption.get("dest"), operator_rows(out), list(adoption.get("live") or []))

    def shape(items: list) -> list[tuple[str, str]]:
        return [(str(m.get("target") or ""), str(m.get(MAP_TARGET) or ""))
                for m in items if isinstance(m, dict)]

    if shape(again) == shape(list(out)):
        out.verified_adoption = [
            {"column": str(m.get("target") or ""), "target": str(m.get(MAP_TARGET) or "")}
            for m in marked
        ]
    return out


def _operator_contract_retry(mappings: list) -> tuple[list, list[dict]] | None:
    """The operator's rows behind a proven adoption, and the adoptions; None if none."""
    from services.target_name_legacy import operator_rows

    adopted = getattr(mappings, "verified_adoption", None)
    if not adopted:
        return None
    from services.conversion_contract import carry_stream_maps

    return carry_stream_maps(operator_rows(mappings), mappings), adopted


def _note_operator_contract(pf: dict | None, adopted: list[dict], which: str) -> None:
    """Run evidence: the approval matched the operator's contract, not the adopted rows."""
    detail = ", ".join(f"{a['target']!r} -> existing column {a['column']!r}" for a in adopted)
    note = (
        f"{which} approval matched the operator's Map (stamped before legacy-column "
        f"adoption); columns adopted from the destination: {detail}."
    )
    logger.warning("G11: %s", note)
    if isinstance(pf, dict):
        pf.setdefault("warnings", []).append(note)
        pf.setdefault("proof_bundle", {}).setdefault("name_adoption", {})[which] = {
            "matched": "operator_contract", "adopted_columns": adopted,
        }


def _enforce_ddl_identity(pf: dict | None, mappings: list, **kwargs) -> str | None:
    """``_enforce_ddl_identity_once``; a pre-G11 stamp may match the operator's rows.

    Execute adopts an existing table's legacy columns before it checks the
    approval (``target_name_legacy.adopt_for_request``). A schedule / saved job
    stamped by Validate before that adoption existed hashed the operator's own
    targets; adoption is deterministic from the live columns, so that stamp is
    accepted against ``operator_rows`` and the run says so. Any other change
    is still refused.
    """
    err = _enforce_ddl_identity_once(pf, mappings, **kwargs)
    retry = _operator_contract_retry(mappings) if err else None
    if retry is None:
        return err
    rows, adopted = retry
    if _enforce_ddl_identity_once(pf, rows, **kwargs) is None:
        _note_operator_contract(pf, adopted, "Map->DDL identity")
        return None
    return err


def _enforce_decision_artifact(
    pf: dict | None, mappings: list, **kwargs
) -> tuple[str | None, dict | None]:
    """``_enforce_decision_artifact_once`` with the same operator-contract match."""
    err, art = _enforce_decision_artifact_once(pf, mappings, **kwargs)
    retry = _operator_contract_retry(mappings) if err else None
    if retry is None:
        return err, art
    rows, adopted = retry
    err2, art2 = _enforce_decision_artifact_once(pf, rows, **kwargs)
    if err2 is None:
        _note_operator_contract(pf, adopted, "Decision Artifact")
        return None, art2
    return err, art
