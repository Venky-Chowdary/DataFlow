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
from decimal import Decimal, InvalidOperation, Overflow, localcontext
from typing import Any

# Significant fractional digits beyond this → likely IEEE binary residue.
_IEEE_SCALE_HARD = 12
# When median scale is low but max is high, treat high tail as float noise.
_IEEE_SCALE_TAIL = 8
# A binary double carries ~15–17 significant decimal digits, so residue like
# ``111.89999999999999`` always arrives with a long *significant* mantissa.
# ``0.00000000000000000001`` and ``1.23E-10`` have a long scale but one to three
# significant digits: they are exact decimals written small, not binary noise.
# Requiring both keeps Excel residue on FLOAT without dragging exact decimals
# there, where every digit past the 17th would be destroyed.
_IEEE_MIN_SIGNIFICANT_DIGITS = 15


def _canonical_numeric(value: Any) -> Decimal | None:
    """The write-path decimal for this cell, or ``None`` (do not invent 0).

    Auto ``1,234`` / ``1.234`` / ``1.000`` refuse. Locale money
    (``$1,234`` / ``€1.234``) binds — stripping the symbol then calling
    ``_normalize_locale_separators`` without the implied currency locale
    used to miss those whole-currency cells and invent INTEGER / empty
    from the leftover ``$99``.
    """
    from services.transform_engine import decimal_wire_value

    return decimal_wire_value(value)


def write_int_digits_and_scale(value: Any) -> tuple[int, int]:
    """(integer_digits, fractional_scale) for write fit / bind.

    Trailing zeros are padding (``2000.00`` → scale 0). Snowflake
    ``NUMBER(38,0)`` stores that as 2000. Significant cents stay
    (``2000.10`` → 2). Create-new invent must keep money scale via
    ``cell_int_digits_and_scale`` and must not call this.
    """
    try:
        d = _canonical_numeric(value)
        if d is None or not d.is_finite():
            if isinstance(value, Decimal) and value.is_finite():
                d = value
            else:
                return 0, 0
        with localcontext() as ctx:
            ctx.prec = max(len(d.as_tuple().digits) + 1, 28)
            n = d.normalize()
        _sign, digits, exponent = n.as_tuple()
        if not isinstance(exponent, int):
            return 0, 0
        scale = -exponent if exponent < 0 else 0
        int_digits = max(0, len(digits) + exponent)
        return int_digits, scale
    except (InvalidOperation, Overflow, ValueError, TypeError):
        return 0, 0


def cell_int_digits_and_scale(value: Any) -> tuple[int, int]:
    """Return (integer_digits, fractional_scale) for create-new invent.

    Preserves explicit money scale (``1000.00`` → 2). Collapses only IEEE /
    Excel residue pads (``52.310500000000000`` → 4) via ``normalize()`` when
    raw scale is in the float-tail band.
    """
    try:
        d = _canonical_numeric(value)
        if d is None or not d.is_finite():
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


def significant_digit_count(value: Any) -> int:
    """Significant decimal digits in a cell, trailing zeros removed.

    ``Decimal.normalize`` strips the padding an exporter added, so ``100`` and
    ``52.310500000000000`` report 1 and 6 rather than 3 and 17.

    Normalizing under the default 28-digit context *rounds*, so a 40-digit value
    reported 28 — under-counting precisely in the range where the answer decides
    whether something fits a 34-digit carrier. The count is taken under a
    context wide enough to leave the value alone.
    """
    try:
        d = _canonical_numeric(value)
        if d is None or not d.is_finite():
            return 0
        with localcontext() as ctx:
            ctx.prec = max(len(d.as_tuple().digits) + 1, 28)
            return len(d.normalize().as_tuple().digits)
    except (InvalidOperation, Overflow, ValueError, TypeError):
        return 0


def looks_like_binary_residue(value: Any) -> bool:
    """True when a cell carries the fingerprint of IEEE→decimal conversion.

    Both halves are required. A long fractional scale alone is satisfied by any
    small exact decimal, and a long significant mantissa alone is satisfied by a
    wide exact one such as ``12345678901234567890.1234567890``, which no double
    could have produced in the first place.
    """
    _int_digits, scale = cell_int_digits_and_scale(value)
    if scale < _IEEE_SCALE_HARD:
        return False
    return significant_digit_count(value) >= _IEEE_MIN_SIGNIFICANT_DIGITS


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


#: Extra dest scale is a critical invent bug (Snowsight ``9.083333000000``).
#: Trailing zeros after the decimal do not change the value, but CREATE /
#: invent / transform must not invent them on any connector. Keep the
#: constant at 0 so leftover callers cannot re-introduce a +2 pad.
CREATE_NEW_NUMERIC_SAFETY_MARGIN = 0


def exact_create_decimal_ps(
    max_int_digits: int,
    max_scale: int,
    *,
    max_precision: int = 38,
    safety_margin: int = 0,
) -> tuple[int, int]:
    """CREATE/invent ``(precision, scale)`` from observed digits. No pad.

    Dest scale is the significant observed scale. Dest int digits are the
    observed max. Do not add +2 scale or +1 int — that is what printed
    ``9.083333000000`` on Snowflake and the same lie on every other engine.

    Values that need more digits widen later to this same exact envelope
    (population fit + write-time ``fits_decimal``). Never invent head-room
    "just in case." ``safety_margin`` stays for explicit callers only and
    defaults to 0.

    When both observed parts exceed ``max_precision``, precision may be
    larger than the cap so ``ddl_type`` can pick BIGNUMERIC / NUMERIC
    instead of silently truncating scale.
    """
    observed_scale = max(0, int(max_scale or 0))
    observed_int = max(0, int(max_int_digits or 0))
    margin = max(0, int(safety_margin or 0))
    scale = observed_scale
    int_digits = observed_int
    if margin and scale > 0:
        scale = scale + min(2, margin)
    if margin and int_digits > 0:
        int_digits = int_digits + 1
    if scale == 0 and int_digits == 0:
        return 0, 0
    if int_digits + scale > max_precision:
        # Reclaim invented head-room only — never a digit the samples used.
        scale = max(observed_scale, min(scale, max(0, max_precision - int_digits)))
    if int_digits + scale > max_precision:
        int_digits = max(observed_int, max_precision - scale)
    precision = max(scale, int_digits + scale)
    if 0 < precision <= max_precision:
        precision = min(max_precision, precision)
    return precision, scale


def observe_source_numeric_samples(
    samples: list[Any] | None,
    *,
    max_precision: int = 38,
) -> dict[str, Any]:
    """What the column *is*, from the cells — no dest invent pad.

    Trailing zeros still collapse through ``cell_int_digits_and_scale`` (same
    rule as ``fits_decimal`` / Validate). Create-new dest invent uses the
    same exact envelope — it must not add scale the cells do not have.
    """
    return observe_numeric_samples(
        samples, safety_margin=0, max_precision=max_precision
    )


def observe_numeric_samples(
    samples: list[Any] | None,
    *,
    safety_margin: int = 0,
    max_precision: int = 38,
) -> dict[str, Any]:
    """Profile numeric samples into invent-ready precision/scale + kind.

    Default invent is the exact observed envelope (margin 0). A non-zero
    ``safety_margin`` is an explicit opt-in and must not be the product
    default — extra dest scale is operator-visible data-shape corruption.

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
    # Scale of the widest cell that actually looks like binary residue. The tail
    # heuristic below consults this instead of the raw maximum, so one exact
    # small decimal cannot brand the whole column approximate.
    residue_scale = 0
    parsed = 0
    for raw in rows[:500]:
        d = _canonical_numeric(raw)
        if d is None or not d.is_finite():
            continue
        idig, scale = cell_int_digits_and_scale(d)
        parsed += 1
        int_digits_list.append(idig)
        scales.append(scale)
        # Scientific notation is a rendering, not a storage class: exporters emit
        # ``9.87E+20`` for exact decimals all the time, so it is judged on the
        # same mantissa evidence as any other spelling.
        if looks_like_binary_residue(d):
            ieee_signals.append("hard_scale_residue")
            residue_scale = max(residue_scale, scale)

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
        residue_scale >= _IEEE_SCALE_TAIL and median_scale <= 4 and residue_scale > scale
    ):
        kind = "ieee_float"
        notes.append(
            f"IEEE/Excel residue suspected (max_scale={max_scale_raw}, "
            f"significant_scale={scale}, median={median_scale})"
        )
        ieee_signals = sorted(set(ieee_signals + ["scale_tail_outlier"]))

    margin = max(0, int(safety_margin))
    # Exact observed envelope. ieee uses significant scale (no pad) and FLOAT.
    if kind == "fixed_decimal":
        precision, scale_out = exact_create_decimal_ps(
            max_int,
            scale,
            max_precision=max_precision,
            safety_margin=margin,
        )
    else:
        precision, scale_out = exact_create_decimal_ps(
            max_int,
            scale if kind != "integer" else 0,
            max_precision=max_precision,
            safety_margin=0,
        )

    if kind == "integer":
        # Wide integers beyond BIGINT → DECIMAL(p,0); else INTEGER.
        if max_int > 18:
            wide_p, _wide_s = exact_create_decimal_ps(
                max_int, 0, max_precision=max_precision
            )
            carrier = f"DECIMAL({min(max_precision, max(wide_p, 19))},0)"
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
        carrier = f"DECIMAL({precision},{scale_out})" if precision else "DECIMAL"

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


# Sources whose cells arrive as untyped text: a numeric column exists only
# because we inferred it, so the sample IS the whole declared domain.
_UNTYPED_NUMERIC_SOURCES = frozenset(
    {
        "",
        "csv",
        "tsv",
        "psv",
        "txt",
        "text",
        "excel",
        "xls",
        "xlsx",
        "google_sheets",
        "gsheets",
        "html",
        "xml",
        "yaml",
        "yml",
        "ini",
        "fixed_width",
    }
)


def source_declares_numeric_domain(source_db: str) -> bool:
    """True when the source engine's cells carry their own numeric domain.

    A relational ``NUMBER``/``DECIMAL`` column, or a BSON ``Decimal128``, holds
    values far wider than any Validate sample proves. Sizing create-new from
    those samples invents a narrow carrier the product then reports as its own
    fidelity collapse — and would quarantine unsampled rows at write time.
    Text-shaped sources (CSV/Excel/Sheets) have no declared domain, so sample
    observation stays the honest carrier there.
    """
    return (source_db or "").strip().lower() not in _UNTYPED_NUMERIC_SOURCES


def create_new_decimal_carrier(
    samples: list[Any] | None,
    *,
    dest_db: str = "",
    source_type: str = "",
) -> str:
    """Logical carrier for create-new invent from samples (+ optional source stamp).

    Prefer declared ``DECIMAL(p,s)`` on ``source_type`` when present. Otherwise
    observe samples at the exact envelope (no +2 scale / +1 int). Destination
    physical DDL is applied by the caller via ``ddl_type(dest_db, carrier)``.
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

    obs = observe_numeric_samples(samples, safety_margin=0)
    if obs.get("kind") in {None, "empty"}:
        # No evidence — keep declared source token (caller falls through to ddl).
        return src or "DECIMAL"
    carrier = str(obs.get("carrier") or "DECIMAL")
    # dest_db reserved for future dialect-specific invent (BQ BIGNUMERIC, …).
    _ = (dest_db or "").strip()
    return carrier


def decimal_widen_precision_scale(
    value: Any,
    *,
    dest_db: str = "",
    current_type: str = "",
) -> tuple[int, int] | None:
    """Smallest (precision, scale) that holds ``value`` without shrinking dest.

    Write-path digits (trailing zeros collapse). Dest int-width is kept so a
    ``NUMBER(9,6)`` clock column that overflowed at scale 7 becomes
    ``NUMBER(10,7)``, not ``NUMBER(8,7)``. Returns ``None`` when the cell
    is not a decimal the write path would bind.
    """
    from connectors.writer_common import parse_decimal_precision_scale

    d = _canonical_numeric(value)
    if d is None or not d.is_finite():
        return None
    idig, scale = write_int_digits_and_scale(d)
    parsed = parse_decimal_precision_scale(current_type, dest_db=dest_db)
    cur_p, cur_s = parsed if parsed else (0, 0)
    cur_int = max(0, cur_p - cur_s) if parsed else 0
    need_s = max(cur_s, scale)
    need_int = max(cur_int, idig, 1 if idig == 0 and scale == 0 else 0)
    need_p = need_int + need_s
    dialect = (dest_db or "").strip().lower()
    cap = 76 if dialect in {"bigquery", "bq"} and need_s > 9 else 38
    if need_p > cap:
        need_p = cap
        need_s = min(need_s, max(0, cap - need_int))
    if need_p <= 0:
        return None
    return need_p, need_s


def _decimal_carrier_token(
    *,
    dest_db: str,
    current_type: str,
    precision: int,
    scale: int,
) -> str:
    dialect = (dest_db or "").strip().lower()
    declared = re.split(r"[\s(]", (current_type or "").strip(), maxsplit=1)[0].upper()
    if dialect in {"snowflake", "oracle"} or declared == "NUMBER":
        return "NUMBER"
    if dialect in {"bigquery", "bq"}:
        return "BIGNUMERIC" if precision > 38 or scale > 9 else "NUMERIC"
    if dialect in {"postgresql", "postgres", "redshift"} or declared == "NUMERIC":
        return "NUMERIC"
    return "DECIMAL"


def decimal_widen_carrier(
    value: Any,
    *,
    dest_db: str = "",
    current_type: str = "",
) -> str:
    """Dest-spelled NUMBER/DECIMAL/NUMERIC that would hold ``value``."""
    got = decimal_widen_precision_scale(
        value, dest_db=dest_db, current_type=current_type
    )
    if got is None:
        return ""
    precision, scale = got
    token = _decimal_carrier_token(
        dest_db=dest_db, current_type=current_type, precision=precision, scale=scale
    )
    return f"{token}({precision},{scale})"


def _decimal_precision_cap(dest_db: str, scale: int) -> int:
    dialect = (dest_db or "").strip().lower()
    return 76 if dialect in {"bigquery", "bq"} and scale > 9 else 38


def decimal_widen_from_envelope(
    *,
    max_int_digits: int,
    max_scale: int,
    dest_db: str = "",
    current_type: str = "",
) -> str:
    """One CREATE/widen type that holds every observed overflow, not the first cell.

    flights-1m: ``0.23333333`` alone suggested NUMBER(11,8); ``0.016666668``
    later still overflowed. The envelope of all unfit cells is NUMBER(12,9).

    Digit math only. Callers that emit an Apply action must use
    :func:`proven_decimal_widen` so the writer predicate agrees.
    """
    from connectors.writer_common import parse_decimal_precision_scale

    parsed = parse_decimal_precision_scale(current_type, dest_db=dest_db)
    cur_p, cur_s = parsed if parsed else (0, 0)
    cur_int = max(0, cur_p - cur_s) if parsed else 0
    need_s = max(cur_s, int(max_scale or 0))
    need_int = max(cur_int, int(max_int_digits or 0))
    if need_int == 0 and need_s == 0:
        need_int = 1
    need_p = need_int + need_s
    cap = _decimal_precision_cap(dest_db, need_s)
    if need_p > cap:
        need_p = cap
        need_s = min(need_s, max(0, cap - need_int))
    if need_p <= 0:
        return ""
    token = _decimal_carrier_token(
        dest_db=dest_db, current_type=current_type, precision=need_p, scale=need_s
    )
    return f"{token}({need_p},{need_s})"


def proven_decimal_widen(
    *,
    values: list[Any] | tuple[Any, ...] = (),
    dest_db: str = "",
    current_type: str = "",
    max_int_digits: int = 0,
    max_scale: int = 0,
    safety_margin: int = 0,
) -> str:
    """CREATE/widen type the write path accepts for every supplied overflow.

    Envelope digits can disagree with ``fits_decimal`` (IEEE residue, dest
    cap shrinking scale). Never emit a ``to_type`` the writer would refuse
    after the operator clicks Apply.
    """
    from connectors.writer_common import fits_decimal, parse_decimal_precision_scale

    parsed = parse_decimal_precision_scale(current_type, dest_db=dest_db)
    cur_p, cur_s = parsed if parsed else (0, 0)
    cur_int = max(0, cur_p - cur_s) if parsed else 0
    need_s = max(cur_s, int(max_scale or 0), 0)
    need_int = max(cur_int, int(max_int_digits or 0), 0)
    margin = max(0, int(safety_margin or 0))
    if margin:
        need_s += margin
        if need_int > 0:
            need_int += 1
    cells = [v for v in values if v is not None and str(v).strip() != ""]
    for raw in cells:
        idig, scale = write_int_digits_and_scale(raw)
        need_s = max(need_s, scale)
        need_int = max(need_int, idig)
    if need_int == 0 and need_s == 0:
        need_int = 1

    cap = _decimal_precision_cap(dest_db, need_s)
    for _ in range(cap + 2):
        need_p = need_int + need_s
        if need_p > cap:
            need_s = min(need_s, max(0, cap - need_int))
            need_p = need_int + need_s
            if need_p > cap or need_p <= 0:
                return ""
        leftovers = [
            v for v in cells
            if not fits_decimal(v, need_p, need_s, dest_db=dest_db)
        ]
        if not leftovers:
            token = _decimal_carrier_token(
                dest_db=dest_db,
                current_type=current_type,
                precision=need_p,
                scale=need_s,
            )
            return f"{token}({need_p},{need_s})"
        grew = False
        for raw in leftovers:
            idig, scale = write_int_digits_and_scale(raw)
            if scale > need_s:
                need_s = scale
                grew = True
            if idig > need_int:
                need_int = idig
                grew = True
        if not grew:
            if need_int + need_s + 1 <= cap:
                need_s += 1
                grew = True
            elif need_int + 1 + need_s <= cap:
                need_int += 1
                grew = True
        if not grew:
            return ""
        cap = _decimal_precision_cap(dest_db, need_s)
    return ""


def fractional_trailing_zeros_same_value(left: Any, right: Any) -> bool:
    """True when dest only padded scale after the decimal — the number is unchanged.

    ``9.083333`` and ``9.083333000000`` are the same value. Zeros *before* the
    decimal (``9.083333`` → ``908333.3``) would change magnitude and fail this.
    """
    a = _canonical_numeric(left)
    b = _canonical_numeric(right)
    if a is None or b is None:
        return False
    return a == b


def dest_scale_padding_honesty(
    *,
    source_example: str = "9.083333",
    dest_example: str = "9.083333000000",
) -> str:
    """Operator copy for Snowflake NUMBER display padding (flights DEP_TIME)."""
    return (
        f"Zeros after the decimal are display scale, not a bigger number. "
        f"{source_example} and {dest_example} compare equal — the time did not "
        f"increase. Zeros before the decimal would change the value; these do not. "
        f"New CREATE/invent must use the observed scale only — never invent "
        f"those extra zeros on any connector."
    )


def decimal_scale_overflow_fix(
    value: Any,
    *,
    dest_db: str = "",
    current_type: str = "",
    column: str = "",
    widened: str = "",
    create_new: bool = False,
    unfit_rows: int = 0,
    example_row: int | None = None,
) -> str:
    """One operator action when dest NUMBER/DECIMAL cannot hold the cell."""
    widened = widened or decimal_widen_carrier(
        value, dest_db=dest_db, current_type=current_type
    )
    if not widened:
        return ""
    col = str(column or "").strip() or "the column"
    if create_new:
        where = f" (first {value!r} at row {example_row})" if example_row else ""
        count = f"{unfit_rows} value(s)" if unfit_rows else "Values"
        return (
            f"New table — CREATE uses the Map type, not an ALTER. "
            f"The preview peek stamped {current_type or 'a narrow numeric type'}. "
            f"{count} in the source need {widened}{where}. "
            f"Approve updates the CREATE type to {widened}. "
            f"That type is proven against the overflow values Validate scanned "
            f"(write-path fits_decimal). Re-Validate of those same values "
            "should clear this gate. Source values are not modified. "
            f"{dest_scale_padding_honesty()} "
            "Do not silently truncate."
        )
    return (
        f"Open Map → widen {col} to {widened} (or ALTER the destination) "
        "→ re-Validate. Do not silently truncate."
    )


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
