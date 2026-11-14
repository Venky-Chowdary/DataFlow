"""Live type-compatibility verdicts for the Map step, from Validate's own rules.

The Map step shows a safe / lossy / incompatible chip on every row while the
operator drags ``order_id`` onto a destination column. If the browser carried
its own copy of the rules, Map would call a pair safe that Validate then
blocks — the "green here, red there" defect :mod:`services.mapping_proof`
already forbids. So this module adds no rule of its own. It asks the two
oracles Validate asks:

* :func:`services.conversion_contract.classify_conversion` — the charter
  ConversionClass. ``unsupported`` is the only *incompatible* verdict.
* :func:`services.mapping_proof.mapping_fidelity` — the canonical per-column
  fidelity verdict G4 reads. ``lossy_cast`` / ``cast`` are *lossy*.

and then names *why* using the same type-system predicates those oracles are
built from (timezone polarity, precision/scale, width), so the tooltip can say
"VARCHAR(255) → VARCHAR(50) truncates text" rather than "lossy".
"""

from __future__ import annotations

from typing import Any

SAFE = "safe"
LOSSY = "lossy"
INCOMPATIBLE = "incompatible"
UNKNOWN = "unknown"

#: A request may grade at most this many pairs (a 1,000-column table + slack).
MAX_PAIRS = 2000


def _reason(kind: str, message: str) -> dict[str, str]:
    return {"kind": kind, "message": message}


def _lossy_reasons(src: str, tgt: str, dest_db: str) -> list[dict[str, str]]:
    """Name every way ``src → tgt`` can lose data, by the type-system predicates."""
    from services.conversion_contract import invents_unproven_capacity
    from services.type_system import (
        binary_width_would_narrow,
        decimal_params_would_narrow,
        float_mantissa_would_narrow,
        integer_decimal_digits_would_narrow,
        integer_width_would_narrow,
        is_timezone_polarity_loss,
        string_width_would_narrow,
        temporal_precision_would_narrow,
    )

    reasons: list[dict[str, str]] = []
    if is_timezone_polarity_loss(src, tgt, dest_db=dest_db):
        reasons.append(_reason(
            "timezone",
            f"{src} → {tgt} drops or invents a time zone: the instant changes "
            "unless the source zone is declared.",
        ))
    if temporal_precision_would_narrow(src, tgt, dest_db=dest_db):
        reasons.append(_reason(
            "precision", f"{src} → {tgt} truncates fractional seconds."
        ))
    if decimal_params_would_narrow(src, tgt, dest_db=dest_db) or integer_decimal_digits_would_narrow(src, tgt):
        reasons.append(_reason(
            "precision", f"{src} → {tgt} has fewer digits or less scale: values round or overflow."
        ))
    if float_mantissa_would_narrow(src, tgt, dest_db=dest_db):
        reasons.append(_reason(
            "precision", f"{src} → {tgt} keeps fewer significant digits."
        ))
    if string_width_would_narrow(src, tgt, dest_db=dest_db):
        reasons.append(_reason(
            "narrowing", f"{src} → {tgt} is narrower: longer text is truncated or rejected."
        ))
    if integer_width_would_narrow(src, tgt, dest_db=dest_db):
        reasons.append(_reason(
            "narrowing", f"{src} → {tgt} holds a smaller integer range: large values overflow."
        ))
    if binary_width_would_narrow(src, tgt):
        reasons.append(_reason(
            "narrowing", f"{src} → {tgt} is narrower: longer binary values are rejected."
        ))
    if not reasons and invents_unproven_capacity(src, tgt, dest_db=dest_db):
        reasons.append(_reason(
            "precision",
            f"{src} → {tgt} declares precision, scale or a time zone the source never proved.",
        ))
    return reasons


def grade_type_pair(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
    dest_table_exists: bool | None = True,
    transform: str = "none",
) -> dict[str, Any]:
    """One row's verdict: ``safe`` / ``lossy`` / ``incompatible`` / ``unknown``."""
    from services.conversion_contract import ConversionClass, classify_conversion
    from services.mapping_proof import mapping_fidelity

    src = str(source_type or "").strip()
    tgt = str(target_type or "").strip()
    db = str(dest_db or "").strip().lower()
    if not src or not tgt:
        return {
            "source_type": src,
            "target_type": tgt,
            "verdict": UNKNOWN,
            "conversion_class": ConversionClass.NEEDS_MANUAL_MAPPING.value,
            "reasons": [_reason(
                "unknown",
                "The destination type was not read — reload the destination schema to grade this pair.",
            )],
            "requires_risk_contract": False,
        }
    conv = classify_conversion(src, tgt, dest_db=db, transform=transform)
    if conv["conversion_class"] == ConversionClass.UNSUPPORTED.value:
        return {
            "source_type": src,
            "target_type": tgt,
            "verdict": INCOMPATIBLE,
            "conversion_class": conv["conversion_class"],
            "reasons": [_reason("incompatible", str(conv.get("reason") or ""))],
            "requires_risk_contract": False,
        }
    fid = mapping_fidelity(
        {"source_type": src, "target_type": tgt, "transform": transform or "none"},
        declared_source_type=src,
        declared_target_type=tgt,
        destination_db_type=db,
        dest_table_exists=dest_table_exists,
    )
    lossy = (
        str(fid.get("verdict")) in {"lossy_cast", "cast"}
        or bool(conv.get("lossy"))
        or bool(conv.get("invents_capacity"))
    )
    if not lossy:
        return {
            "source_type": src,
            "target_type": tgt,
            "verdict": SAFE,
            "conversion_class": conv["conversion_class"],
            "reasons": [_reason("safe", str(fid.get("reason") or conv.get("reason") or ""))],
            "requires_risk_contract": False,
        }
    reasons = _lossy_reasons(src, tgt, db) or [
        _reason(
            "domain",
            f"{src} → {tgt} changes the value domain: values that do not convert are quarantined.",
        )
    ]
    return {
        "source_type": src,
        "target_type": tgt,
        "verdict": LOSSY,
        "conversion_class": conv["conversion_class"],
        "reasons": reasons,
        "requires_risk_contract": bool(
            conv.get("requires_risk_contract") or fid.get("requires_risk_contract")
        ),
    }


def grade_type_pairs(
    pairs: list[dict[str, Any]],
    *,
    dest_db: str = "",
    dest_table_exists: bool | None = True,
) -> list[dict[str, Any]]:
    """Grade each ``{source_type, target_type, transform?}``; identical pairs once."""
    cache: dict[tuple[str, str, str], dict[str, Any]] = {}
    out: list[dict[str, Any]] = []
    for pair in pairs[:MAX_PAIRS]:
        key = (
            str(pair.get("source_type") or "").strip(),
            str(pair.get("target_type") or "").strip(),
            str(pair.get("transform") or "none").strip() or "none",
        )
        if key not in cache:
            cache[key] = grade_type_pair(
                key[0],
                key[1],
                dest_db=dest_db,
                dest_table_exists=dest_table_exists,
                transform=key[2],
            )
        out.append(cache[key])
    return out
