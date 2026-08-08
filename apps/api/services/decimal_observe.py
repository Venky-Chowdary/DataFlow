"""Sample-aware DECIMAL(p,s) / IEEE float observation — create-new invent SSOT.

Migration honesty (Airbyte/Fivetran-class):

* Never invent bare ``DECIMAL`` that later widens to ``DECIMAL(38,15)``.
* Prefer observed integer digits + scale from ``Decimal.normalize()``.
* Detect Excel/IEEE residue (``111.89999999999999``) — do not treat as money scale.
* Platform caps applied via ``type_system.ddl_type`` at stamp time.
* Currency / locale numeric text MUST reuse ``transform_engine`` normalize helpers —
  never a second money-strip regex that invents wrong scale (``€2.000,50`` ≠ ``2.00050``).
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation, Overflow
from typing import Any

# Significant fractional digits beyond this → likely IEEE binary residue.
_IEEE_SCALE_HARD = 12
# When median scale is low but max is high, treat high tail as float noise.
_IEEE_SCALE_TAIL = 8
_SCI_RE = re.compile(r"[eE][+-]?\d+")


def _canonical_numeric_text(value: Any) -> str | None:
    """Locale/currency-aware decimal text via transform_engine SSOT.

    Returns ``None`` when the cell is empty / unparseable (do not invent 0).
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        return format(value.normalize(), "f")
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            return None
        try:
            return format(Decimal(str(value)).normalize(), "f")
        except (InvalidOperation, Overflow, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    from services.transform_engine import (
        _normalize_locale_separators,
        _normalize_numeric_text,
    )

    cleaned = _normalize_numeric_text(text)
    if not cleaned:
        return None
    return _normalize_locale_separators(cleaned)


def cell_int_digits_and_scale(value: Any) -> tuple[int, int]:
    """Return (integer_digits, fractional_scale) for create-new invent.

    Preserves explicit money scale (``1000.00`` → 2). Collapses only IEEE /
    Excel residue pads (``52.310500000000000`` → 4) via ``normalize()`` when
    raw scale is in the float-tail band.
    """
    try:
        text = _canonical_numeric_text(value)
        if not text:
            return 0, 0
        d = Decimal(text)
        if not d.is_finite():
            return 0, 0
        _sign, digits, exponent = d.as_tuple()
        raw_scale = -exponent if exponent < 0 else 0
        int_digits = max(0, len(digits) + exponent)
        if raw_scale >= _IEEE_SCALE_TAIL:
            d = d.normalize()
            _sign, digits, exponent = d.as_tuple()
            scale = -exponent if exponent < 0 else 0
            int_digits = max(0, len(digits) + exponent)
            return int_digits, scale
        return int_digits, raw_scale
    except (InvalidOperation, Overflow, ValueError, TypeError):
        return 0, 0


def _significant_scale(scales: list[int]) -> int:
    """Scale for invent: max among common scales; ignore IEEE tail outliers."""
    if not scales:
        return 0
    ordered = sorted(scales)
    n = len(ordered)
    median = ordered[n // 2]
    # Drop extreme float residue when the body of the distribution is short.
    if ordered[-1] >= _IEEE_SCALE_TAIL and median <= 4:
        body = [s for s in ordered if s <= max(median + 2, 4)]
        if body:
            return max(body)
    return ordered[-1]


def observe_numeric_samples(
    samples: list[Any] | None,
    *,
    safety_margin: int = 2,
    max_precision: int = 38,
) -> dict[str, Any]:
    """Profile numeric samples into invent-ready precision/scale + kind.

    Returns keys: ``kind``, ``max_int_digits``, ``max_scale``, ``scale``,
    ``precision``, ``carrier``, ``parse_rate``, ``sample_count``, ``ieee_signals``,
    ``notes``.
    """
    rows = [s for s in (samples or []) if s is not None and str(s).strip() != ""]
    if not rows:
        return {
            "kind": "empty",
            "max_int_digits": 0,
            "max_scale": 0,
            "scale": 0,
            "precision": 0,
            "carrier": "DECIMAL",
            "parse_rate": 0.0,
            "sample_count": 0,
            "ieee_signals": [],
            "notes": ["no samples"],
        }

    int_digits_list: list[int] = []
    scales: list[int] = []
    ieee_signals: list[str] = []
    parsed = 0
    for raw in rows[:500]:
        text = _canonical_numeric_text(raw)
        if not text:
            continue
        if _SCI_RE.search(str(raw)):
            ieee_signals.append("scientific_notation")
        try:
            d = Decimal(text)
            if not d.is_finite():
                continue
        except (InvalidOperation, Overflow, ValueError):
            continue
        idig, scale = cell_int_digits_and_scale(text)
        parsed += 1
        int_digits_list.append(idig)
        scales.append(scale)
        if scale >= _IEEE_SCALE_HARD:
            ieee_signals.append("hard_scale_residue")

    if not parsed:
        return {
            "kind": "empty",
            "max_int_digits": 0,
            "max_scale": 0,
            "scale": 0,
            "precision": 0,
            "carrier": "DECIMAL",
            "parse_rate": 0.0,
            "sample_count": len(rows),
            "ieee_signals": sorted(set(ieee_signals)),
            "notes": ["no parseable numeric samples"],
        }

    max_int = max(int_digits_list)
    max_scale_raw = max(scales)
    scale = _significant_scale(scales)
    ordered = sorted(scales)
    median_scale = ordered[len(ordered) // 2]

    kind = "fixed_decimal"
    notes: list[str] = []
    if max_scale_raw == 0 and all(s == 0 for s in scales):
        kind = "integer"
        notes.append("all samples integral")
    elif ieee_signals or (
        max_scale_raw >= _IEEE_SCALE_TAIL and median_scale <= 4 and max_scale_raw > scale
    ):
        kind = "ieee_float"
        notes.append(
            f"IEEE/Excel residue suspected (max_scale={max_scale_raw}, "
            f"significant_scale={scale}, median={median_scale})"
        )
        ieee_signals = sorted(set(ieee_signals + ["scale_tail_outlier"]))

    margin = max(0, int(safety_margin))
    # Modest buffer on scale for fixed money; ieee uses significant scale only.
    scale_out = scale
    if kind == "fixed_decimal" and scale_out > 0:
        scale_out = min(max_precision, scale_out + min(2, margin))
    int_out = max(1, max_int + (1 if max_int > 0 else 0))
    if int_out + scale_out > max_precision:
        scale_out = max(0, max_precision - int_out)
    if int_out + scale_out > max_precision:
        int_out = max_precision - scale_out
    precision = max(scale_out, min(max_precision, int_out + scale_out))

    if kind == "integer":
        # Wide integers beyond BIGINT → DECIMAL(p,0); else INTEGER.
        if max_int > 18:
            carrier = f"DECIMAL({min(max_precision, max(max_int + 1, 19))},0)"
        else:
            carrier = "INTEGER"
    elif kind == "ieee_float":
        # Honest approximate wire — FLOAT invent, not fake money DECIMAL(38,15).
        # Still expose a cleaned DECIMAL suggestion for operators who need fixed.
        carrier = "FLOAT"
        notes.append(
            f"suggested_fixed=DECIMAL({precision},{scale})"
            if scale
            else f"suggested_fixed=DECIMAL({precision},0)"
        )
    else:
        carrier = f"DECIMAL({precision},{scale_out})"

    return {
        "kind": kind,
        "max_int_digits": max_int,
        "max_scale": max_scale_raw,
        "scale": scale_out if kind != "ieee_float" else scale,
        "precision": precision,
        "carrier": carrier,
        "parse_rate": round(parsed / max(len(rows), 1), 4),
        "sample_count": len(rows),
        "ieee_signals": sorted(set(ieee_signals)),
        "notes": notes,
        "suggested_fixed": (
            f"DECIMAL({precision},{scale})" if kind == "ieee_float" else carrier
        ),
    }


def create_new_decimal_carrier(
    samples: list[Any] | None,
    *,
    dest_db: str = "",
    source_type: str = "",
) -> str:
    """Logical carrier for create-new invent from samples (+ optional source stamp).

    Prefer declared ``DECIMAL(p,s)`` on ``source_type`` when present. Otherwise
    observe samples. Destination physical DDL is applied by the caller via
    ``ddl_type(dest_db, carrier)``.
    """
    from services.type_system import (
        LOGICAL_DECIMAL,
        normalize_logical_type,
        parse_numeric_precision_scale,
    )

    src = (source_type or "").strip()
    if src and normalize_logical_type(src) == LOGICAL_DECIMAL:
        p, s = parse_numeric_precision_scale(src)
        if p is not None:
            return src if s is not None else f"DECIMAL({p},0)"

    obs = observe_numeric_samples(samples)
    if obs.get("kind") in {None, "empty"}:
        # No evidence — keep declared source token (caller falls through to ddl).
        return src or "DECIMAL"
    carrier = str(obs.get("carrier") or "DECIMAL")
    # dest_db reserved for future dialect-specific invent (BQ BIGNUMERIC, …).
    _ = (dest_db or "").strip()
    return carrier


def ieee_float_create_new_risk(observation: dict[str, Any] | None) -> dict[str, str] | None:
    """Risk stamp when invent chose FLOAT due to Excel/IEEE residue."""
    if not observation or observation.get("kind") != "ieee_float":
        return None
    suggested = observation.get("suggested_fixed") or "DECIMAL"
    return {
        "kind": "ieee_float_artifact",
        "severity": "warn",
        "message": (
            "Samples look like IEEE/Excel binary floats (long fractional residue). "
            f"Create-new stamps FLOAT (approximate). Prefer {suggested} only when "
            "the business domain is fixed-point money/scores — accept risk or remap."
        ),
    }
