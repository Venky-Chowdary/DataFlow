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
    """Charter 7-class taxonomy — never collapse to silent green."""

    LOSSLESS = "lossless"
    LOSSY = "lossy"
    UNSUPPORTED = "unsupported"
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
    from services.type_system import normalize_logical_type

    return normalize_logical_type(src), normalize_logical_type(tgt)


def invents_unproven_capacity(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
) -> bool:
    """True when dest stamp invents precision/scale/TZ the source never proved."""
    from services.type_system import (
        LOGICAL_DECIMAL,
        bignumeric_capacity_would_invent,
        decimal_params_would_narrow,
        is_timezone_polarity_loss,
        normalize_logical_type,
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

    Never returns lossless when invent / lossy / unsupported evidence exists.
    """
    from services.mapping_proof import transform_fidelity
    from services.type_system import is_lossy_coercion

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
        return {
            "conversion_class": ConversionClass.NEEDS_USER_APPROVAL.value,
            "reason": (
                f"{src} → {tgt} is lossy — Accept risk / Risk Contract required "
                "(never silent continue)."
            ),
            "invents_capacity": invent,
            "lossy": True,
            "requires_risk_contract": True,
            "contract_version": CONVERSION_CONTRACT_VERSION,
        }

    if lossy and risk_acknowledged:
        return {
            "conversion_class": ConversionClass.LOSSY.value,
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

    return {
        "conversion_class": ConversionClass.LOSSLESS.value,
        "reason": f"{src} → {tgt} round-trips without invent or declared loss.",
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


def approved_mapping_ddl_fingerprint(
    mappings: list[dict[str, Any]] | None,
    *,
    dest_db: str = "",
) -> str:
    """Stable hash of approved Map stamps after ``materialize_dest_ddl``.

    Map → materialize must equal Execute CREATE/ALTER stamps. Any drift changes
    this fingerprint and requires re-validation.
    """
    from services.type_system import materialize_dest_ddl

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
        wire = materialize_dest_ddl(dest_db, stamp) if stamp else ""
        rows.append(
            {
                "source": src,
                "target": tgt,
                "map_stamp": stamp,
                "materialized_ddl": str(wire or ""),
                "transform": str(m.get("transform") or "none"),
            }
        )
    rows.sort(key=lambda r: (r["source"], r["target"]))
    payload = {
        "version": CONVERSION_CONTRACT_VERSION,
        "dest_db": (dest_db or "").strip().lower(),
        "columns": rows,
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def assert_ddl_identity(
    approved_fingerprint: str,
    mappings: list[dict[str, Any]] | None,
    *,
    dest_db: str = "",
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
        raise DdlIdentityError(
            "DDL identity mismatch — Map stamp / materialize diverged from last "
            "Validate approval. Re-validate before Execute.",
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
        "approved_ddl_identity_hash": approved or None,
        "matches_approved": matches,
        "dest_db": (dest_db or "").strip().lower(),
        "note": (
            "Map→materialize fingerprint. Execute must match or re-validate."
            if matches
            else "Fingerprint diverged from approved Validate — Execute must fail closed."
        ),
    }
