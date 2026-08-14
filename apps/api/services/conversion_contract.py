"""Module 12 — Type Conversion Contract + DDL Identity.

Charter:
- Every mapping gets an explicit ConversionClass (never infer silently).
- Never invent precision / scale / timezone without Needs User Approval.
- Map → DDL → Execute must share one fingerprint; divergence fails closed.

This module is the operator-facing SSOT. ``is_lossy_coercion`` remains the
mechanical type-path oracle; ConversionClass is the explainable decision class.
"""

from __future__ import annotations

import hashlib
import json
import logging
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)

CONVERSION_CONTRACT_VERSION = "conversion_contract.v1"


class ConversionClass(str, Enum):
    """Conversion taxonomy (Phase C3) — never collapse to silent green.

    Charter gate classes (``needs_*`` / ``lossy`` / ``unsupported``) remain the
    Execute blockers. Safe-path subclasses refine former ``lossless`` so Map
    cells explain *why* the path is safe (identity / widen / equivalent / …).
    """

    # --- Safe-path detail (Phase C3 full set) ---
    IDENTITY = "identity"
    EQUIVALENT = "equivalent"
    LOSSLESS = "lossless"
    REPRESENTATION = "representation"
    NORMALIZATION = "normalization"
    WIDENING = "widening"
    # --- Risk / fidelity ---
    NARROWING = "narrowing"
    SEMANTIC = "semantic"
    POTENTIALLY_LOSSY = "potentially_lossy"
    LOSSY = "lossy"
    UNSUPPORTED = "unsupported"
    MANUAL = "manual"
    # --- Gate / operator action (Module 12 charter — keep stable) ---
    NEEDS_TRANSFORM = "needs_transform"
    NEEDS_USER_APPROVAL = "needs_user_approval"
    NEEDS_QUARANTINE = "needs_quarantine"
    NEEDS_MANUAL_MAPPING = "needs_manual_mapping"


# Logical pairs that are not a fidelity-lossy cast but an unsupported domain jump.
# Keep small and explicit — never invent compatibility.
_UNSUPPORTED_LOGICAL_PAIRS: frozenset[tuple[str, str]] = frozenset(
    {
        ("binary", "integer"),
        ("binary", "decimal"),
        ("binary", "boolean"),
        ("binary", "date"),
        ("binary", "datetime"),
        ("struct", "integer"),
        ("struct", "decimal"),
        ("struct", "boolean"),
        ("struct", "date"),
        ("struct", "datetime"),
        ("array", "integer"),
        ("array", "decimal"),
        ("array", "boolean"),
        ("array", "date"),
        ("array", "datetime"),
        ("map", "integer"),
        ("map", "decimal"),
        ("map", "boolean"),
        ("map", "date"),
        ("map", "datetime"),
    }
)


class DdlIdentityError(Exception):
    """Approved Map DDL fingerprint does not match materialize/Execute DDL."""

    def __init__(self, message: str, *, expected: str = "", actual: str = ""):
        super().__init__(message)
        self.expected = expected
        self.actual = actual


def _logical(src: str, tgt: str) -> tuple[str, str]:
    from services.decision_kernel.types import normalize_logical_type

    return normalize_logical_type(src), normalize_logical_type(tgt)


def invents_unproven_capacity(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
) -> bool:
    """True when dest stamp invents precision/scale/TZ the source never proved."""
    from services.decision_kernel.types import normalize_logical_type
    from services.type_system import (
        LOGICAL_DECIMAL,
        bignumeric_capacity_would_invent,
        decimal_params_would_narrow,
        is_timezone_polarity_loss,
        parse_numeric_precision_scale,
    )

    if decimal_params_would_narrow(source_type, target_type, dest_db=dest_db):
        sp, ss = parse_numeric_precision_scale(source_type)
        tp, ts = parse_numeric_precision_scale(target_type)
        # Bare → parametric or proven → bare: invent / invent-default.
        # Postgres bare NUMERIC is unbounded — not an invent (helper already False).
        if (sp is None and ss is None and (tp is not None or ts is not None)) or (
            tp is None and ts is None and (sp is not None or ss is not None)
        ):
            return True
    if bignumeric_capacity_would_invent(source_type, target_type):
        return True
    if is_timezone_polarity_loss(source_type, target_type, dest_db=dest_db):
        return True
    # Bare temporal → dialect FSP invent (e.g. TIMESTAMP → DATETIME2(7)).
    src_l = normalize_logical_type(source_type)
    tgt_l = normalize_logical_type(target_type)
    if src_l in {"datetime", "date", "time"} and tgt_l in {"datetime", "date", "time"}:
        src_u = (source_type or "").upper().replace(" ", "")
        tgt_u = (target_type or "").upper().replace(" ", "")
        src_has_fsp = "(" in src_u
        tgt_has_fsp = "(" in tgt_u
        if not src_has_fsp and tgt_has_fsp:
            return True
    # INTEGER→DECIMAL(p,s) invents scale/precision the integer never proved.
    if src_l in {"integer", "bigint", "smallint", "tinyint"} and tgt_l == "decimal":
        tp, ts = parse_numeric_precision_scale(target_type)
        if tp is not None or ts is not None:
            return True
    # Bare/unbounded string → VARCHAR(n) invents length the source never proved.
    if src_l in {"string", "text"} and tgt_l in {"string", "text"}:
        from services.type_system import parse_string_carrier_width

        src_w = parse_string_carrier_width(source_type)
        tgt_w = parse_string_carrier_width(target_type)
        if tgt_w is not None and src_w is None:
            return True
    _ = LOGICAL_DECIMAL  # documented domain; invent handled above
    return False


def _safe_path_conversion_class(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
) -> ConversionClass:
    """Refine a non-lossy path into Identity / Widening / Equivalent / …"""
    from services.decision_kernel.types import normalize_logical_type
    from services.type_system import integer_bit_width, integer_storage_bounds

    src_u = (source_type or "").strip().upper().replace(" ", "")
    tgt_u = (target_type or "").strip().upper().replace(" ", "")
    if src_u and src_u == tgt_u:
        return ConversionClass.IDENTITY

    src_l = normalize_logical_type(source_type)
    tgt_l = normalize_logical_type(target_type)
    if src_l == tgt_l:
        sw = integer_bit_width(source_type)
        tw = integer_bit_width(target_type)
        if sw is None or tw is None:
            # Ambiguous ``INT``/``INTEGER`` keyword: invent refuses a width, but
            # the *storage* SSOT resolves it per engine so INTEGER → BIGINT
            # still reads as widening instead of a vague "equivalent".
            src_b = integer_storage_bounds(source_type, dest_db=dest_db)
            tgt_b = integer_storage_bounds(target_type, dest_db=dest_db)
            if src_b and tgt_b:
                sw = src_b[1].bit_length()
                tw = tgt_b[1].bit_length()
        if sw is not None and tw is not None:
            if tw > sw:
                return ConversionClass.WIDENING
            if tw < sw:
                return ConversionClass.NARROWING
        # Same logical family, different native spelling (e.g. INT8 vs BIGINT).
        if src_u != tgt_u:
            return ConversionClass.EQUIVALENT
        return ConversionClass.IDENTITY

    # Cross-logical but oracle said non-lossy (e.g. specialty wire preserve).
    if src_l in {"string", "text"} and tgt_l in {"string", "text"}:
        return ConversionClass.REPRESENTATION
    return ConversionClass.LOSSLESS


def classify_conversion(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
    transform: str | None = "none",
    risk_acknowledged: bool = False,
    mapped: bool = True,
) -> dict[str, Any]:
    """Classify one source→target path into the charter ConversionClass.

    Never returns a safe-path class when invent / lossy / unsupported evidence
    exists. Safe paths are refined (identity/widening/…) per Phase C3.
    """
    from services.decision_kernel.types import is_lossy_coercion
    from services.mapping_proof import transform_fidelity

    src = (source_type or "").strip()
    tgt = (target_type or "").strip()
    if not mapped or not src or not tgt:
        return {
            "conversion_class": ConversionClass.NEEDS_MANUAL_MAPPING.value,
            "reason": "Column is unmapped or missing declared types — manual mapping required.",
            "invents_capacity": False,
            "lossy": False,
            "requires_risk_contract": False,
            "contract_version": CONVERSION_CONTRACT_VERSION,
        }

    src_l, tgt_l = _logical(src, tgt)
    if (src_l, tgt_l) in _UNSUPPORTED_LOGICAL_PAIRS:
        return {
            "conversion_class": ConversionClass.UNSUPPORTED.value,
            "reason": f"{src} → {tgt} is an unsupported domain jump ({src_l}→{tgt_l}).",
            "invents_capacity": False,
            "lossy": True,
            "requires_risk_contract": False,
            "contract_version": CONVERSION_CONTRACT_VERSION,
        }

    invent = invents_unproven_capacity(src, tgt, dest_db=dest_db)
    lossy = bool(is_lossy_coercion(src, tgt, dest_db=dest_db))
    t_fid = transform_fidelity(transform)

    if invent and not risk_acknowledged:
        return {
            "conversion_class": ConversionClass.NEEDS_USER_APPROVAL.value,
            "reason": (
                f"{src} → {tgt} invents precision, scale, FSP, or timezone polarity "
                "the source never proved — mint a Migration Risk Contract before Execute."
            ),
            "invents_capacity": True,
            "lossy": True,
            "requires_risk_contract": True,
            "contract_version": CONVERSION_CONTRACT_VERSION,
        }

    if lossy and not risk_acknowledged:
        detail = _safe_path_conversion_class(src, tgt, dest_db=dest_db)
        return {
            "conversion_class": ConversionClass.NEEDS_USER_APPROVAL.value,
            "reason": (
                f"{src} → {tgt} is lossy — Accept risk / Risk Contract required "
                "(never silent continue)."
            ),
            "invents_capacity": invent,
            "lossy": True,
            "requires_risk_contract": True,
            "detail_class": detail.value,
            "contract_version": CONVERSION_CONTRACT_VERSION,
        }

    if lossy and risk_acknowledged:
        detail = _safe_path_conversion_class(src, tgt, dest_db=dest_db)
        ack_class = (
            ConversionClass.NARROWING
            if detail is ConversionClass.NARROWING
            else ConversionClass.LOSSY
        )
        return {
            "conversion_class": ack_class.value,
            "reason": f"{src} → {tgt} is lossy under an approved Risk Contract.",
            "invents_capacity": invent,
            "lossy": True,
            "requires_risk_contract": False,
            "contract_version": CONVERSION_CONTRACT_VERSION,
        }

    if t_fid == "mutate":
        return {
            "conversion_class": ConversionClass.NEEDS_TRANSFORM.value,
            "reason": f"Transform '{transform}' rewrites values — transform contract required.",
            "invents_capacity": False,
            "lossy": False,
            "requires_risk_contract": False,
            "contract_version": CONVERSION_CONTRACT_VERSION,
        }

    if t_fid == "lossy_cast":
        return {
            "conversion_class": ConversionClass.NEEDS_QUARANTINE.value,
            "reason": (
                f"Parsed via '{transform}'; type path holds, but unparseable values "
                "quarantine rather than write."
            ),
            "invents_capacity": False,
            "lossy": False,
            "requires_risk_contract": False,
            "contract_version": CONVERSION_CONTRACT_VERSION,
        }

    safe = _safe_path_conversion_class(src, tgt, dest_db=dest_db)
    return {
        "conversion_class": safe.value,
        "reason": f"{src} → {tgt} round-trips without invent or declared loss ({safe.value}).",
        "invents_capacity": False,
        "lossy": False,
        "requires_risk_contract": False,
        "contract_version": CONVERSION_CONTRACT_VERSION,
    }


def classify_mapping(
    mapping: dict[str, Any],
    *,
    declared_source_type: str = "",
    declared_target_type: str = "",
    destination_db_type: str = "",
) -> dict[str, Any]:
    """Classify one mapping row (charter ConversionClass)."""
    src = str(
        declared_source_type
        or mapping.get("source_type")
        or mapping.get("inferred_type")
        or ""
    )
    tgt = str(
        declared_target_type
        or mapping.get("target_type")
        or mapping.get("dest_type")
        or ""
    )
    intentional_omit = bool(
        mapping.get("intentional_omit") or mapping.get("intentionalOmit")
    )
    mapped = bool(mapping.get("source") and mapping.get("target")) and not intentional_omit
    # Boolean ack alone never clears invent/lossy — verified continue-policy only.
    try:
        from services.migration_risk_contract import mapping_has_clearing_risk_contract

        cleared = mapping_has_clearing_risk_contract(mapping)
    except Exception:
        cleared = False
    return classify_conversion(
        src,
        tgt,
        dest_db=destination_db_type,
        transform=str(mapping.get("transform") or "none"),
        risk_acknowledged=cleared,
        mapped=mapped,
    )


def ddl_identity_columns(
    mappings: list[dict[str, Any]] | None,
    *,
    dest_db: str = "",
) -> list[dict[str, str]]:
    """Canonical destination DDL each mapping row materializes to.

    Identity is the *physical column contract*: target name plus the DDL the
    destination will actually receive. Map stamp spelling is deliberately not
    part of it — a live catalog reporting ``VARCHAR(255) COLLATE utf8mb4_…``
    for the operator's ``VARCHAR(255)`` materializes to the same column, and
    so does ``TIMESTAMP`` vs ``TIMESTAMP_NTZ(6)`` on MySQL. Transforms are the
    conversion contract's concern (Decision Artifact), not DDL.

    A stamp that cannot be materialized keeps its raw text, marked, so an
    unmaterializable stamp never hashes equal to a materialized one.
    """
    from services.decision_kernel.types import materialize_dest_ddl

    rows: list[dict[str, str]] = []
    for m in mappings or []:
        if not isinstance(m, dict):
            continue
        if bool(m.get("intentional_omit") or m.get("intentionalOmit")):
            continue
        src = str(m.get("source") or "").strip()
        tgt = str(m.get("target") or "").strip()
        if not src or not tgt:
            continue
        stamp = str(m.get("target_type") or m.get("dest_type") or "").strip()
        src_type = str(m.get("source_type") or m.get("inferred_type") or "")
        wire = (
            str(materialize_dest_ddl(dest_db, stamp, source_type=src_type) or "")
            if stamp
            else ""
        )
        if stamp and not wire:
            wire = f"unmaterialized:{stamp}"
        rows.append({"source": src, "target": tgt, "materialized_ddl": wire})
    rows.sort(key=lambda r: (r["source"], r["target"]))
    return rows


def approved_mapping_ddl_fingerprint(
    mappings: list[dict[str, Any]] | None,
    *,
    dest_db: str = "",
) -> str:
    """Stable hash of approved Map stamps after ``materialize_dest_ddl``.

    Map → materialize must equal Execute CREATE/ALTER stamps. Any drift changes
    this fingerprint and requires re-validation.
    """
    payload = {
        "version": CONVERSION_CONTRACT_VERSION,
        "dest_db": (dest_db or "").strip().lower(),
        "columns": ddl_identity_columns(mappings, dest_db=dest_db),
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def ddl_identity_divergence(
    approved_columns: list[dict[str, Any]] | None,
    mappings: list[dict[str, Any]] | None,
    *,
    dest_db: str = "",
    limit: int = 5,
) -> list[str]:
    """Per-column ``approved → current`` lines for a fingerprint mismatch.

    Empty when the approved column set was not carried alongside the hash: a
    hash alone cannot name what diverged, and inventing a cause is worse than
    saying nothing.
    """
    if not approved_columns:
        return []
    approved = {
        (str(r.get("target") or ""), str(r.get("source") or "")): str(
            r.get("materialized_ddl") or ""
        )
        for r in approved_columns
        if isinstance(r, dict)
    }
    current = {
        (r["target"], r["source"]): r["materialized_ddl"]
        for r in ddl_identity_columns(mappings, dest_db=dest_db)
    }
    lines: list[str] = []
    for key in sorted(set(approved) | set(current)):
        was, now = approved.get(key), current.get(key)
        if was == now:
            continue
        col = key[0] or key[1]
        if was is None:
            lines.append(f"{col}: not in approved Map → {now}")
        elif now is None:
            lines.append(f"{col}: {was} → dropped from Map")
        else:
            lines.append(f"{col}: {was} → {now}")
    return lines[:limit]


def assert_ddl_identity(
    approved_fingerprint: str,
    mappings: list[dict[str, Any]] | None,
    *,
    dest_db: str = "",
    approved_columns: list[dict[str, Any]] | None = None,
) -> str:
    """Fail closed when materialize/Execute DDL diverges from approved Map."""
    expected = (approved_fingerprint or "").strip().lower()
    if not expected:
        raise DdlIdentityError(
            "No approved DDL fingerprint — re-run Validate before Execute.",
            expected="",
            actual="",
        )
    actual = approved_mapping_ddl_fingerprint(mappings, dest_db=dest_db)
    if actual.lower() != expected:
        diverged = ddl_identity_divergence(
            approved_columns, mappings, dest_db=dest_db
        )
        detail = f" Diverged: {'; '.join(diverged)}." if diverged else ""
        raise DdlIdentityError(
            "DDL identity mismatch — Map stamp / materialize diverged from last "
            f"Validate approval. Re-validate before Execute.{detail}",
            expected=expected,
            actual=actual,
        )
    return actual


def ddl_identity_report(
    mappings: list[dict[str, Any]] | None,
    *,
    dest_db: str = "",
    approved_fingerprint: str | None = None,
) -> dict[str, Any]:
    """Honesty stamp for proof packs / Validate."""
    fp = approved_mapping_ddl_fingerprint(mappings, dest_db=dest_db)
    approved = (approved_fingerprint or "").strip().lower()
    matches = (not approved) or (fp.lower() == approved)
    return {
        "contract_version": CONVERSION_CONTRACT_VERSION,
        "ddl_identity_hash": fp,
        "columns": ddl_identity_columns(mappings, dest_db=dest_db),
        "approved_ddl_identity_hash": approved or None,
        "matches_approved": matches,
        "dest_db": (dest_db or "").strip().lower(),
        "note": (
            "Map→materialize fingerprint. Execute must match or re-validate."
            if matches
            else "Fingerprint diverged from approved Validate — Execute must fail closed."
        ),
    }
