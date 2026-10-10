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

import logging
import re
from decimal import Decimal, InvalidOperation, Overflow, localcontext
from typing import Any

logger = logging.getLogger(__name__)

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


# File transports and untyped document formats. Their numeric "types" are
# inferred by ``file_parser`` / ``object_store_common`` from the first rows
# (``infer_columns_from_rows`` reads 50), so an observed ``DECIMAL(4,2)`` is
# a sample of the column, never its domain (QA MX2-10).
_FILE_TRANSPORT_SOURCES = frozenset(
    {
        "file",
        "local_file",
        "upload",
        "json",
        "jsonl",
        "ndjson",
        "s3",
        "amazon_s3",
        "gcs",
        "google_cloud_storage",
        "azure_blob",
        "azure_blob_storage",
        "adls",
        "adls_gen2",
        "azure_data_lake",
        "sftp",
        "ftp",
        "ftps",
        "http",
        "https",
        "google_drive",
        "dropbox",
        "onedrive",
        "sharepoint",
        "box",
    }
)

#: Destinations whose bare ``NUMERIC`` is exact and unbounded — no integer
#: cap, no scale pad, the value is stored as written. Redshift is *not* one:
#: its bare NUMERIC is NUMERIC(18,0).
_UNBOUNDED_EXACT_NUMERIC_DIALECTS = frozenset(
    {
        "postgresql",
        "postgres",
        "cockroachdb",
        "yugabytedb",
        "timescaledb",
        "greenplum",
        "alloydb",
        "aurora_postgresql",
        "supabase",
        "neon",
    }
)

#: Portable exact-decimal ceiling. 38 is the largest precision every
#: bounded SQL engine accepts without falling back to TEXT (MySQL's 65 is not
#: portable through ``ddl_type``).
_OPEN_DOMAIN_PRECISION = 38


def sampled_numeric_is_not_a_domain(source_db: str) -> bool:
    """True when this source's numeric types were *inferred from a sample*.

    Only an explicitly named untyped source qualifies — an unknown (empty)
    source is not presumed untyped, so relational routes that did not pass
    their engine keep mirroring the declared type.
    """
    key = (source_db or "").strip().lower()
    if not key:
        return False
    if key in _UNTYPED_NUMERIC_SOURCES or key in _FILE_TRANSPORT_SOURCES:
        return True
    from services.schema_introspect import sample_page_is_not_a_precision_contract

    return sample_page_is_not_a_precision_contract(key)


def open_domain_decimal_carrier(scale: int | None, *, dest_db: str = "") -> str | None:
    """Create-new carrier for a decimal whose domain no catalog declares.

    The sample is evidence of *scale*, never a bound on the integer part: the
    integer part takes the destination's full capacity. PostgreSQL-family
    destinations get exact unbounded ``NUMERIC`` (holds every value as
    written, no pad). Bounded engines get ``DECIMAL(38, observed_scale)``.

    Returns ``None`` when the scale is unknown and the destination is bounded
    — inventing scale 0 would quarantine every fractional value, so the caller
    keeps its platform carrier.
    """
    db = (dest_db or "").strip().lower()
    if db in _UNBOUNDED_EXACT_NUMERIC_DIALECTS:
        return "NUMERIC"
    if scale is None:
        return None
    s = max(0, min(int(scale), _OPEN_DOMAIN_PRECISION - 1))
    return f"DECIMAL({_OPEN_DOMAIN_PRECISION},{s})"


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
        need_int = max(idig, min(need_int, cap - need_s))
        need_p = need_int + need_s
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
    observed_int = max(0, int(max_int_digits or 0))
    observed_s = max(0, int(max_scale or 0))
    margin = max(0, int(safety_margin or 0))
    cells = [v for v in values if v is not None and str(v).strip() != ""]
    for raw in cells:
        idig, scale = write_int_digits_and_scale(raw)
        observed_s = max(observed_s, scale)
        observed_int = max(observed_int, idig)
    need_s = max(cur_s, observed_s)
    need_int = max(cur_int, observed_int)
    if margin:
        need_s += margin
        if need_int > 0:
            need_int += 1
    if need_int == 0 and need_s == 0:
        need_int = 1

    cap = _decimal_precision_cap(dest_db, need_s)
    for _ in range(cap + 2):
        need_p = need_int + need_s
        if need_p > cap:
            # Reclaim unused dest integer head-room (NUMBER(38,0) + scale 6)
            # before shrinking observed scale. Never invent extra scale.
            need_int = max(observed_int, min(need_int, cap - need_s))
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
            # Do not invent +1 scale/int. Extra dest zeros are data-shape
            # corruption (currency 12.50 → 12.500). Return empty so the
            # gate cannot Apply a type the cells did not prove.
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
            f"FLOAT is approximate — do not Apply it as the default CREATE type. "
            f"Prefer {suggested} for money/clocks/scores. Accept · Risk Contract "
            "only if the business domain is truly IEEE, then remap to FLOAT."
        ),
    }


_TEXT_TYPE_CODES = frozenset(
    {
        15,  # MySQL VARCHAR
        245,  # JSON
        252,  # BLOB
        253,  # VAR_STRING
        254,  # STRING
        18,  # Postgres char
        25,  # text
        1042,  # bpchar
        1043,  # varchar
        3802,  # jsonb
        114,  # json
    }
)


def _cursor_type_code_is_text(type_code: Any) -> bool:
    """True for a driver type that is character data, not a DECIMAL."""
    if isinstance(type_code, bool) or type_code is None:
        return False
    if isinstance(type_code, int):
        return type_code in _TEXT_TYPE_CODES
    name = str(getattr(type_code, "__name__", "") or type_code).lower()
    return any(
        token in name
        for token in ("varchar", "char", "text", "string", "blob", "json")
    )


def cursor_declared_numeric_types(
    headers: list[str],
    description: Any,
) -> dict[str, str]:
    """``DECIMAL(p,s)`` from a DBAPI ``cursor.description``, by result position.

    PEP 249 puts precision at index 4 and scale at index 5. Oracle
    ``NUMBER(10,2)`` reports those; a sample of ``12.34`` would otherwise
    invent ``numeric(5,2)`` and reject a later ``12345678.90``. Unconstrained
    ``NUMBER`` (precision 0, scale -127) is omitted so inference stays the
    owner of a type the catalog never sized.
    """
    if not headers or not description:
        return {}
    out: dict[str, str] = {}
    for idx, header in enumerate(headers):
        if idx >= len(description):
            break
        col = description[idx]
        if not col or len(col) < 6:
            continue
        type_code = col[1] if len(col) > 1 else None
        if _cursor_type_code_is_text(type_code):
            continue
        # DATE / DATETIME / TINYINT carry a display width in the precision
        # slot. Reading it as DECIMAL(10,0) made every MariaDB query column
        # look numeric (DEF-B-019).
        carrier = _carrier_for_cursor_column(col)
        if carrier and carrier not in {"DECIMAL", "NUMERIC"}:
            continue
        try:
            precision = int(col[4])
            scale = int(col[5])
        except (TypeError, ValueError):
            continue
        name = str(header or "").strip()
        if not name:
            continue
        # PostgreSQL typmod -1 (unconstrained numeric) arrives as 65535/65535.
        # That is not a size; DECIMAL(65535,65535) cannot be created and was
        # rewritten to TEXT, which then blocked the route (DEF-C-026).
        if precision >= 65535 or scale >= 65535:
            out[name] = "NUMERIC"
            continue
        if precision <= 0 or scale < 0 or scale > precision:
            continue
        exact_numeric = (
            isinstance(type_code, int)
            and not isinstance(type_code, bool)
            and type_code in _CURSOR_EXACT_NUMERIC_CODES
        )
        # A real PostgreSQL NUMERIC(100,2) must survive. Junk widths on a
        # type code that is not numeric (MariaDB text at 400,39) must not.
        if exact_numeric:
            if precision > 1000 or scale > 1000:
                continue
        elif precision > 76 or scale > 38:
            continue
        out[name] = f"DECIMAL({precision},{scale})"
    return out


# Driver type codes that name a carrier. MySQL FIELD_TYPE and PostgreSQL
# OIDs do not share these values. Code 16 is MySQL BIT and Postgres bool,
# so it is left to the sample.
_CURSOR_TEMPORAL_CODES: dict[int, str] = {
    7: "TIMESTAMP",
    10: "DATE",
    11: "TIME",
    12: "DATETIME",
    13: "YEAR",
    1082: "DATE",
    1083: "TIME",
    1114: "TIMESTAMP",
    1184: "TIMESTAMPTZ",
    1266: "TIME",
}
_CURSOR_INTEGER_CODES: dict[int, str] = {
    1: "INTEGER",
    2: "INTEGER",
    3: "INTEGER",
    8: "BIGINT",
    9: "INTEGER",
    20: "BIGINT",
    21: "INTEGER",
    23: "INTEGER",
}
_CURSOR_FLOAT_CODES: dict[int, str] = {
    4: "FLOAT",
    5: "DOUBLE",
    700: "FLOAT",
    701: "DOUBLE",
}
_CURSOR_JSON_CODES = frozenset({114, 245, 3802})
_CURSOR_TEXT_CODES = frozenset({15, 18, 25, 247, 248, 253, 254, 1042, 1043})
# Exact numeric type codes. Precision on any other code (DATE is 10, DATETIME
# is 26,6) is a display width, not a DECIMAL.
_CURSOR_EXACT_NUMERIC_CODES = frozenset({0, 246, 1700})
_CURSOR_BINARY_CODES = {17: "BYTEA"}
_CURSOR_UUID_CODES = {2950: "UUID"}
_CURSOR_INTERVAL_CODES = {1186: "INTERVAL"}
# PostgreSQL array OIDs. These are fixed in pg_type. A custom enum's array
# OID is not, so that one is resolved from the catalog.
_CURSOR_ARRAY_CODES: dict[int, str] = {
    1000: "BOOLEAN[]",
    1001: "BYTEA[]",
    1005: "INTEGER[]",
    1007: "INTEGER[]",
    1009: "VARCHAR[]",
    1014: "VARCHAR[]",
    1015: "VARCHAR[]",
    1016: "BIGINT[]",
    1021: "FLOAT[]",
    1022: "DOUBLE[]",
    1028: "INTEGER[]",
    1115: "TIMESTAMP[]",
    1182: "DATE[]",
    1183: "TIME[]",
    1185: "TIMESTAMPTZ[]",
    1187: "INTERVAL[]",
    1231: "NUMERIC[]",
    199: "JSON[]",
    2951: "UUID[]",
    3807: "JSON[]",
}
_PG_CATALOG_DIALECTS = frozenset({
    "postgresql",
    "postgres",
    "pgvector",
    "redshift",
    "greenplum",
    "cockroachdb",
    "timescaledb",
    "citus",
    "alloydb",
    "yugabytedb",
    "supabase",
})
_PG_ELEMENT_CARRIERS = {
    "bool": "BOOLEAN",
    "int2": "INTEGER",
    "int4": "INTEGER",
    "int8": "BIGINT",
    "oid": "INTEGER",
    "float4": "FLOAT",
    "float8": "DOUBLE",
    "numeric": "NUMERIC",
    "text": "VARCHAR",
    "varchar": "VARCHAR",
    "bpchar": "VARCHAR",
    "uuid": "UUID",
    "json": "JSON",
    "jsonb": "JSON",
    "bytea": "BYTEA",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "timestamptz": "TIMESTAMPTZ",
    "time": "TIME",
    "timetz": "TIME",
    "interval": "INTERVAL",
}


def _carrier_for_cursor_column(col: Any, dialect: str = "") -> str:
    """Logical carrier a cursor column declared, or ``""`` when it did not.

    A CAST and a catalog type show up here. Fifty sample rows must not
    re-guess them. An unknown type code stays empty so Oracle NUMBER with
    no precision remains the sample's problem.
    """
    if not col or len(col) < 2:
        return ""
    type_code = col[1]
    # A catalog lookup stamps this when the driver only had an OID (a custom
    # enum, or that enum's array). An int never carries it.
    explicit = getattr(type_code, "carrier", None)
    if (
        isinstance(explicit, str)
        and explicit.strip()
        and not isinstance(type_code, (int, float))
    ):
        return explicit.strip()
    if isinstance(type_code, int) and not isinstance(type_code, bool):
        # Code 16 is PostgreSQL's bool OID *and* MySQL FIELD_TYPE_BIT — only
        # the dialect disambiguates. An all-NULL ``NULL::boolean`` column has
        # no sample to arbitrate, so leaving it to inference rewrote the
        # declared BOOLEAN as VARCHAR (QA T18).
        if type_code == 16:
            if (dialect or "").strip().lower() in _PG_CATALOG_DIALECTS:
                return "BOOLEAN"
        elif type_code in _CURSOR_ARRAY_CODES:
            return _CURSOR_ARRAY_CODES[type_code]
        if type_code in _CURSOR_JSON_CODES:
            return "JSON"
        if type_code in _CURSOR_TEXT_CODES:
            return "VARCHAR"
        if type_code in _CURSOR_BINARY_CODES:
            return _CURSOR_BINARY_CODES[type_code]
        if type_code in _CURSOR_UUID_CODES:
            return _CURSOR_UUID_CODES[type_code]
        if type_code in _CURSOR_INTERVAL_CODES:
            return _CURSOR_INTERVAL_CODES[type_code]
        if type_code in _CURSOR_TEMPORAL_CODES:
            return _CURSOR_TEMPORAL_CODES[type_code]
        if type_code in _CURSOR_INTEGER_CODES:
            return _CURSOR_INTEGER_CODES[type_code]
        if type_code in _CURSOR_FLOAT_CODES:
            return _CURSOR_FLOAT_CODES[type_code]
        # MariaDB TEXT shares the BLOB code. A real blob has no decimal
        # scale; precision 400 scale 39 is the text-column junk.
        if type_code == 252 and len(col) >= 6:
            try:
                precision = int(col[4])
                scale = int(col[5])
            except (TypeError, ValueError):
                return ""
            if precision > 76 or scale > 38:
                return "VARCHAR"
        return ""
    # oracledb.DbType exposes ``name`` (``DB_TYPE_NUMBER``). Its class
    # ``__name__`` is ``DbType`` for every column, which hid the carrier and
    # left 0/1 samples free to invent BOOLEAN (DEF-B-011). A real class
    # name (``int``, ``INTEGER``) still wins so exact-code checks stay exact.
    class_name = str(getattr(type_code, "__name__", "") or "")
    named = getattr(type_code, "name", None)
    if class_name and class_name.lower() not in {"dbtype", "type"}:
        name = class_name.lower()
    elif named:
        name = str(named).lower()
    else:
        name = str(type_code).lower()
    if "json" in name:
        return "JSON"
    if "number" in name or "numeric" in name or name.endswith("decimal"):
        return "DECIMAL"
    if any(token in name for token in ("varchar", "char", "text", "string")):
        return "VARCHAR"
    if "timestamptz" in name or "timestamp with time zone" in name:
        return "TIMESTAMPTZ"
    if "datetime" in name:
        return "DATETIME"
    if "timestamp" in name:
        return "TIMESTAMP"
    if name == "date" or name.endswith(".date"):
        return "DATE"
    if name in {"int", "int4", "integer", "int8", "int2", "bigint"}:
        return "BIGINT" if "8" in name or "big" in name else "INTEGER"
    if "[]" in name:
        return name.upper().replace(" ", "")
    tokens = set(re.split(r"[^a-z0-9]+", name))
    if "enum" in tokens:
        return "ENUM"
    return ""


class CursorDeclaredType:
    """A carrier the catalog named when the driver only reported an OID."""

    def __init__(self, name: str, carrier: str) -> None:
        self.name = name
        self.carrier = carrier

    def __repr__(self) -> str:
        return f"CursorDeclaredType({self.carrier!r})"


def carrier_from_pg_type_row(
    *,
    typname: str,
    typtype: str,
    typelem: int,
    elem_name: str = "",
    elem_type: str = "",
) -> str:
    """Carrier for one ``pg_type`` row. Empty means the sample stays the owner.

    ``typtype = e`` is an enum. An array has a non-zero ``typelem``. A
    composite, domain, or range is not relabelled from this row.
    """
    kind = (typtype or "").strip().lower()
    if kind == "e":
        return "ENUM"
    try:
        element_oid = int(typelem or 0)
    except (TypeError, ValueError):
        element_oid = 0
    if element_oid == 0:
        return ""
    if (elem_type or "").strip().lower() == "e":
        return "VARCHAR[]"
    element = _PG_ELEMENT_CARRIERS.get((elem_name or "").lstrip("_").lower(), "")
    if element:
        return f"{element}[]"
    # The catalog says this OID is an array. The element is not a builtin
    # we name, so the carrier stays an array and does not become text.
    if (typname or "").startswith("_"):
        return "ARRAY"
    return ""


def _fetch_pg_type_rows(conn: Any, oids: list[int]) -> list[Any]:
    """``pg_type`` rows for these OIDs. A failed lookup leaves the sample in charge."""
    if not oids:
        return []
    if any((not isinstance(oid, int)) or isinstance(oid, bool) or oid < 0 for oid in oids):
        return []
    id_list = ",".join(str(int(oid)) for oid in oids)
    sql = (
        "SELECT t.oid, t.typname, t.typtype, t.typelem, "
        "COALESCE(e.typname, ''), COALESCE(e.typtype, '') "
        "FROM pg_type t LEFT JOIN pg_type e ON e.oid = t.typelem "
        f"WHERE t.oid IN ({id_list})"
    )
    try:
        import sqlalchemy as sa

        return list(conn.execute(sa.text(sql)))
    except Exception as exc:
        logger.debug("pg_type lookup skipped: %s", exc)
        return []


def annotate_unresolved_pg_types(conn: Any, description: Any, *, dialect: str) -> Any:
    """Replace unknown PostgreSQL OIDs with the catalog carrier.

    Built-in arrays already have a type code. A custom enum does not: its
    OID is assigned per database, and the sample then guessed INTERVAL or
    BOOLEAN from the label. A lookup that fails leaves the description
    unchanged.
    """
    if description is None:
        return None
    if (dialect or "").strip().lower() not in _PG_CATALOG_DIALECTS:
        return description
    unknown: list[int] = []
    for col in description:
        if not col or len(col) < 2:
            continue
        code = col[1]
        if isinstance(code, int) and not isinstance(code, bool) and _carrier_for_cursor_column(col) == "":
            unknown.append(int(code))
    if not unknown:
        return description
    rows = _fetch_pg_type_rows(conn, sorted(set(unknown)))
    if not rows:
        return description
    by_oid: dict[int, Any] = {}
    for row in rows:
        try:
            by_oid[int(row[0])] = row
        except (TypeError, ValueError, IndexError):
            continue
    copied = []
    for col in description:
        if not col or len(col) < 2:
            copied.append(col)
            continue
        code = col[1]
        found = by_oid.get(int(code)) if isinstance(code, int) and not isinstance(code, bool) else None
        if found is None:
            copied.append(tuple(col) if not isinstance(col, tuple) else col)
            continue
        try:
            carrier = carrier_from_pg_type_row(
                typname=str(found[1] or ""),
                typtype=str(found[2] or ""),
                typelem=int(found[3] or 0),
                elem_name=str(found[4] or ""),
                elem_type=str(found[5] or ""),
            )
        except (TypeError, ValueError, IndexError):
            carrier = ""
        if not carrier:
            copied.append(tuple(col) if not isinstance(col, tuple) else col)
            continue
        updated = list(col)
        updated[1] = CursorDeclaredType(str(found[1] or ""), carrier)
        copied.append(tuple(updated))
    return tuple(copied)


def cursor_declared_carriers(
    headers: list[str],
    description: Any,
    dialect: str = "",
) -> dict[str, str]:
    """Cursor-declared carriers. A sized DECIMAL wins over a family name.

    Sample inference runs first at the call site. This map replaces it.
    """
    numeric = cursor_declared_numeric_types(headers, description)
    if not headers or not description:
        return numeric
    out = dict(numeric)
    for idx, header in enumerate(headers):
        name = str(header or "").strip()
        if not name or name in out or idx >= len(description):
            continue
        carrier = _carrier_for_cursor_column(description[idx], dialect)
        if carrier:
            out[name] = carrier
    return out
