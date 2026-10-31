"""Per-destination 'unfit value' quarantine passes for mapped rows (re-exported by connectors.writer_common)."""

from __future__ import annotations

import logging
from services.value_serializer import cell_to_string
from services.value_serializer import is_reader_null_cell
from services.value_serializer import unescape_wire_text
from typing import Any

logger = logging.getLogger("connectors.writer_common")


def temporal_cell_unfit_reason(raw: Any, typ: str, *, dest_db: str = "") -> str:
    """Why the writer would hold this temporal cell out, or ``""``.

    The writer's own rule, for one cell. Validate's wire probe calls it so a
    plan cannot go green on a value Execute quarantines (an offset-bearing
    SQLite text timestamp into a PostgreSQL ``TIMESTAMP``: M6).
    """
    from connectors.writer_common import (
        _dest_is_instant_only,
        _mysql_datetime_utc_normalizes,
    )
    from services.type_system import (
        datetime_timezone_polarity,
        normalize_logical_type,
        parse_temporal_fractional_precision,
    )

    logical = normalize_logical_type(typ)
    if logical not in {"date", "time", "datetime"}:
        return ""
    check_fsp = parse_temporal_fractional_precision(typ) is not None
    check_tz = (
        logical == "datetime"
        and datetime_timezone_polarity(typ, dest_db=dest_db) == "ntz"
        and not _mysql_datetime_utc_normalizes(typ, dest_db)
        and not _dest_is_instant_only(typ, dest_db)
    )
    return _temporal_unfit_reason(
        raw, typ, dest_db=dest_db, check_fsp=check_fsp, check_tz=check_tz
    )


def _temporal_unfit_reason(
    raw: Any, typ: str, *, dest_db: str, check_fsp: bool, check_tz: bool
) -> str:
    from connectors.writer_common import (
        _mysql_timestamp_range_violation,
        _time_out_of_carrier_range,
    )
    from services.type_system import (
        temporal_value_exceeds_precision,
        temporal_value_has_timezone,
    )

    if isinstance(raw, str) and not raw.strip():
        return (
            f"empty string into temporal destination {typ} "
            "— quarantined (refuse silent NULL invent)"
        )
    from connectors.sql_temporal import invalid_calendar_reason

    calendar_reason = invalid_calendar_reason(raw)
    if not calendar_reason and isinstance(raw, str):
        from services.transform_engine import mixed_date_format_reason

        calendar_reason = mixed_date_format_reason(raw)
    if calendar_reason:
        return f"{calendar_reason} (destination {typ})"
    if check_fsp and temporal_value_exceeds_precision(raw, typ):
        return (
            f"fractional seconds exceed destination {typ} "
            "— quarantined (refuse silent truncate)"
        )
    if _mysql_timestamp_range_violation(typ, raw, dest_db=dest_db):
        return (
            f"value outside the MySQL TIMESTAMP epoch range for {typ} "
            "— quarantined; map to DATETIME(6) with a UTC-normalize "
            "contract to carry instants beyond 2038"
        )
    if _time_out_of_carrier_range(typ, raw, dest_db=dest_db):
        return (
            f"TIME value outside the 0–24h wall-clock range on {dest_db} "
            f"for {typ} — quarantined; map to an interval or text "
            "carrier to carry out-of-range TIME"
        )
    if check_tz and temporal_value_has_timezone(raw):
        return (
            f"timezone-aware value into NTZ destination {typ} "
            "— quarantined (refuse silent offset strip; map to an instant "
            "carrier such as TIMESTAMPTZ, declare source_timezone, or use an "
            "explicit UTC transform)"
        )
    return ""


def quarantine_unfit_temporals(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    dest_db: str = "",
) -> list[tuple]:
    """Hold out temporal cells that would invent NULL, lose FSP, or strip TZ.

    - Empty ``\"\"`` into DATE/TIME/DATETIME → refuse silent NULL invent (Iceberg
      Arrow / SQL bind parity)
    - ``TIME(6)`` → ``TIME(0)`` truncates fractional seconds
    - Offset-aware / ``Z`` wire into NTZ/DATETIME strips the offset (Airbyte invent)
    - MySQL ``TIMESTAMP`` outside 1970..2038 — the carrier is an instant, but an
      epoch-bounded one, so out-of-range values are held out rather than zeroed

    ``dest_db`` decides bare ``TIMESTAMP`` polarity: on MySQL it is a UTC instant
    carrier, so an offset-bearing wire is *not* a strip and must not quarantine.
    """

    from connectors.writer_common import (
        _dest_is_instant_only,
        _mysql_datetime_utc_normalizes,
        _unfit_cell_absent,
        append_write_quarantine_detail,
    )

    from services.type_system import (
        datetime_timezone_polarity,
        normalize_logical_type,
        parse_temporal_fractional_precision,
    )

    temporal_cols: list[tuple[int, str, bool, bool]] = []
    for i, typ in enumerate(target_types):
        logical = normalize_logical_type(typ)
        if logical not in {"date", "time", "datetime"}:
            continue
        check_fsp = parse_temporal_fractional_precision(typ) is not None
        check_tz = (
            logical == "datetime"
            and datetime_timezone_polarity(typ, dest_db=dest_db) == "ntz"
            and not _mysql_datetime_utc_normalizes(typ, dest_db)
            and not _dest_is_instant_only(typ, dest_db)
        )
        # Always include temporal columns so empty refuse runs even without FSP/TZ.
        temporal_cols.append((i, typ, check_fsp, check_tz))
    if not temporal_cols:
        return mapped_rows

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, typ, check_fsp, check_tz in temporal_cols:
            if _unfit_cell_absent(cells, col_idx):
                continue
            raw = cells[col_idx]
            reason = _temporal_unfit_reason(
                raw, typ, dest_db=dest_db, check_fsp=check_fsp, check_tz=check_tz
            )
            if not reason:
                continue
            sample = cell_to_string(raw)[:120]
            append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": reason,
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=row,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                from services.value_serializer import DF_MISSING_SENTINEL
                cells[col_idx] = DF_MISSING_SENTINEL
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def quarantine_unfit_floats(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    dialect_label: str = "FLOAT",
    dest_db: str = "",
) -> list[tuple]:
    """Hold out empty / non-finite / non-numeric cells into FLOAT/DOUBLE sinks.

    Matrix SSOT for Kafka/S3/GCS/Iceberg. SQL bind quarantines the same
    non-finite values in :func:`bind_rows_keeping_numbers` — MySQL DOUBLE
    would otherwise store NaN as NULL. Empty ``\"\"`` must never invent
    JSON null / 0.0.
    """

    from connectors.writer_common import (
        _infer_dest_db_from_dialect_label,
        _is_nonfinite_float,
        _unfit_cell_absent,
        append_write_quarantine_detail,
        engine_stores_nonfinite_float,
    )

    from services.type_system import normalize_logical_type

    float_cols: list[tuple[int, str]] = []
    for i, typ in enumerate(target_types):
        if normalize_logical_type(typ) != "float":
            continue
        float_cols.append((i, typ))
    if not float_cols:
        return mapped_rows

    from connectors.sql_bind import coerce_float_wire

    engine = (dest_db or "").strip() or _infer_dest_db_from_dialect_label(dialect_label)
    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, typ in float_cols:
            if _unfit_cell_absent(cells, col_idx):
                continue
            raw = cells[col_idx]
            reason = ""
            try:
                coerced = coerce_float_wire(raw, ddl_type=typ, engine=engine)
            except ValueError as exc:
                reason = str(exc)
                coerced = None
            if (
                not reason
                and _is_nonfinite_float(coerced)
                and not engine_stores_nonfinite_float(engine)
            ):
                reason = (
                    f"non-finite float into {dialect_label}({typ}) "
                    "— quarantined (refuse invent)"
                )
            if not reason:
                continue
            sample = cell_to_string(raw)[:120]
            append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": (
                        f"{reason} — quarantined ({dialect_label} refuse silent "
                        "NULL/0.0 invent)"
                        if "quarantined" not in reason
                        else reason
                    ),
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                from services.value_serializer import DF_MISSING_SENTINEL

                cells[col_idx] = DF_MISSING_SENTINEL
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def quarantine_unfit_integers(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    dialect_label: str = "INTEGER",
    dest_db: str = "",
) -> list[tuple]:
    """Hold out / NULL cells that overflow signed/unsigned integer destinations.

    Mirrors DECIMAL/VARCHAR write quarantine: preflight samples can miss production
    outliers (UINT32 into INT, BIGINT into SMALLINT). Fail-closed — never wrap.
    """

    from connectors.writer_common import (
        _unfit_cell_absent,
        append_write_quarantine_detail,
        integer_overflow_suggested_type,
    )

    from services.type_system import integer_storage_bounds, normalize_logical_type

    int_cols: list[tuple[int, str]] = []
    for i, typ in enumerate(target_types):
        if normalize_logical_type(typ) != "integer":
            continue
        if integer_storage_bounds(typ, dest_db=dest_db) is None:
            continue
        int_cols.append((i, typ))
    if not int_cols:
        return mapped_rows


    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, typ in int_cols:
            if _unfit_cell_absent(cells, col_idx):
                continue
            unfit = integer_fit_failure(cells[col_idx], typ, dest_db=dest_db)
            if unfit is None:
                continue
            sample = cell_to_string(cells[col_idx])[:120]
            suggested_type = integer_overflow_suggested_type(
                cells[col_idx], typ, dest_db=dest_db
            )
            suggested_fix = (
                (
                    f"Open Map → widen {target_cols[col_idx]} to {suggested_type} "
                    "(or ALTER the destination) → re-Validate. "
                    "Do not silently truncate."
                )
                if suggested_type
                else ""
            )
            append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": (
                        f"value does not fit {dialect_label}({typ}) "
                        f"— quarantined: {unfit}"
                    ),
                    "policy": "write_quarantine",
                    "suggested_fix": suggested_fix,
                    "suggested_target_type": suggested_type,
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                from services.value_serializer import DF_MISSING_SENTINEL
                cells[col_idx] = DF_MISSING_SENTINEL
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def quarantine_unfit_decimals(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    dialect_label: str = "DECIMAL",
    dest_db: str = "",
) -> list[tuple]:
    """Hold out / NULL cells that cannot fit DECIMAL/NUMBER(p,s).

    ``quarantine`` omits the whole row from the primary write (no NULL invent).
    ``coerce_null`` keeps the row with a NULL cell. ``fail`` stamps unfit cells and holds out rows like ``quarantine`` so
    ``reject_on_strict_policy`` can abort before bind (never rely on soft drivers).
    """

    from connectors.writer_common import (
        _unfit_cell_absent,
        append_write_quarantine_detail,
        decimal_reason_label,
        fits_decimal,
        parse_decimal_precision_scale,
    )

    number_cols: list[tuple[int, int, int, str]] = []
    for i, typ in enumerate(target_types):
        parsed = parse_decimal_precision_scale(typ, dest_db=dest_db)
        if parsed:
            number_cols.append((i, parsed[0], parsed[1], str(typ or "")))
    if not number_cols:
        return mapped_rows


    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, precision, scale, declared_type in number_cols:
            if _unfit_cell_absent(cells, col_idx):
                continue
            if fits_decimal(cells[col_idx], precision, scale, dest_db=dest_db):
                continue
            sample = cell_to_string(cells[col_idx])[:120]
            from services.decimal_observe import (
                decimal_scale_overflow_fix,
                decimal_widen_carrier,
            )

            suggested_type = decimal_widen_carrier(
                cells[col_idx], dest_db=dest_db, current_type=declared_type
            )
            suggested_fix = decimal_scale_overflow_fix(
                cells[col_idx],
                dest_db=dest_db,
                current_type=declared_type,
                column=target_cols[col_idx],
            )
            append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": (
                    "decimal does not fit "
                    f"{decimal_reason_label(dialect_label, dest_db, declared_type)}"
                    f"({precision},{scale}) "
                    "— quarantined (would truncate/overflow on write)"
                    ),
                    "policy": "write_quarantine",
                    "suggested_fix": suggested_fix,
                    "suggested_target_type": suggested_type,
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                from services.value_serializer import DF_MISSING_SENTINEL
                cells[col_idx] = DF_MISSING_SENTINEL
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def quarantine_unfit_years(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
) -> list[tuple]:
    """Hold out / NULL cells outside MySQL YEAR range (0 or 1901–2155).

    Non-strict MySQL stores invalid YEAR as 0000 — silent wipe. Fail closed.
    """

    from connectors.writer_common import (
        _unfit_cell_absent,
        append_write_quarantine_detail,
    )

    from services.type_system import is_year_carrier, year_value_fits

    year_cols = [i for i, typ in enumerate(target_types) if is_year_carrier(typ)]
    if not year_cols:
        return mapped_rows

    from connectors.sql_bind import coerce_year_wire

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx in year_cols:
            if _unfit_cell_absent(cells, col_idx):
                continue
            try:
                cells[col_idx] = coerce_year_wire(cells[col_idx])
                continue
            except ValueError:
                pass
            if year_value_fits(cells[col_idx]):
                continue
            sample = cell_to_string(cells[col_idx])[:120]
            append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": (
                        "year outside MySQL YEAR range (0 or 1901–2155) "
                        "— quarantined (non-strict MySQL would store 0000)"
                    ),
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                from services.value_serializer import DF_MISSING_SENTINEL
                cells[col_idx] = DF_MISSING_SENTINEL
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def quarantine_unfit_booleans(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
) -> list[tuple]:
    """Hold out / NULL cells that are not canonical boolean wire forms.

    Accepts bool / 0|1 / true|false|t|f only. Refuses silent ``yes``/``Y``/``2``
    invent into BOOLEAN / BIT / TINYINT(1) destinations.
    """

    from connectors.writer_common import (
        _unfit_cell_absent,
        append_write_quarantine_detail,
    )

    from services.type_system import boolean_value_fits, normalize_logical_type

    bool_cols = [
        i
        for i, typ in enumerate(target_types)
        if normalize_logical_type(typ) == "boolean"
    ]
    if not bool_cols:
        return mapped_rows

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx in bool_cols:
            if _unfit_cell_absent(cells, col_idx):
                continue
            if boolean_value_fits(cells[col_idx]):
                continue
            sample = cell_to_string(cells[col_idx])[:120]
            append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": (
                    "non-canonical boolean wire token "
                    "(accept 0|1|true|false|t|f only) — quarantined"
                    ),
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                from services.value_serializer import DF_MISSING_SENTINEL
                cells[col_idx] = DF_MISSING_SENTINEL
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def quarantine_currency_markers_into_numeric(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
) -> list[tuple]:
    """Hold out cells that still carry currency symbols into numeric/MONEY columns.

    Identity-mapped ``$1,234.56`` must not be silently stripped to 1234.56 or
    fail mid-batch. Operator must apply an explicit currency transform first.
    """

    from connectors.writer_common import (
        _unfit_cell_absent,
        append_write_quarantine_detail,
    )

    from services.type_system import (
        has_currency_marker,
        is_money_carrier,
        normalize_logical_type,
    )

    numeric_cols: list[int] = []
    for i, typ in enumerate(target_types):
        logical = normalize_logical_type(typ)
        if logical in {"decimal", "integer", "float"} or is_money_carrier(typ):
            numeric_cols.append(i)
    if not numeric_cols:
        return mapped_rows

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx in numeric_cols:
            if _unfit_cell_absent(cells, col_idx):
                continue
            if not has_currency_marker(cells[col_idx]):
                continue
            sample = cell_to_string(cells[col_idx])[:120]
            append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": (
                    "currency marker present in numeric/MONEY cell "
                    "— quarantined (refuse silent strip; use currency transform)"
                    ),
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                from services.value_serializer import DF_MISSING_SENTINEL
                cells[col_idx] = DF_MISSING_SENTINEL
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def integer_fit_failure(
    value: Any, type_str: str, *, dest_db: str = ""
) -> str | None:
    """Why ``value`` cannot land in the integer carrier, or ``None`` if it can.

    One rule for Validate and for the write. ``22.433332`` is not an integer
    value: the write path's ``_parse_integer`` refuses any non-integral decimal
    ("Invalid integer"), so a fit check that truncated to ``22`` and called it
    in range promised a write that the writer would then reject at row 1.
    Rounding a fractional value into an integer column is a declared
    transformation, never a side effect of a bounds test.

    Unparseable text is named here too, on the same parser the write binds
    through: leaving ``'ABC-1'`` for "type coercion" meant Validate reported an
    INT column as fitting and the write refused every such row afterwards.
    """
    from decimal import Decimal, InvalidOperation
    from services.transform_engine import integer_wire_value
    from services.type_system import integer_storage_bounds

    bounds = integer_storage_bounds(type_str, dest_db=dest_db)
    if bounds is None:
        return None
    if is_reader_null_cell(value):
        return None
    if isinstance(value, bool):
        # bool is a subclass of int — treat as 0/1.
        dec = Decimal(int(value))
    elif isinstance(value, int):
        dec = Decimal(value)
    else:
        text = str(unescape_wire_text(value)).strip()
        if not text:
            # Empty ≠ natural NULL on INTEGER — bind must not invent NULL;
            # quarantine_unfit_integers / bind quarantine hold the row out.
            return f"empty value is not an integer for {type_str}"
        parsed = integer_wire_value(text)
        if parsed is None:
            from services.transform_engine import decimal_wire_value

            # Write-path decimal first. Decimal(text) invented Auto 1.000 as
            # integral 1 and called it overflow. Locale money with cents is
            # fractional. Dest-canonical 1.234 stays the fractional message.
            wire = decimal_wire_value(text)
            if wire is not None and wire.is_finite():
                if wire != wire.to_integral_value():
                    return (
                        f"fractional value {wire} is not an integer for "
                        f"{type_str} — widen the destination to DECIMAL/DOUBLE, "
                        "or round it explicitly before the write"
                    )
                return f"{text} exceeds the integer wire budget for {type_str}"
            try:
                dest = Decimal(text)
            except (InvalidOperation, ValueError, TypeError, OverflowError):
                dest = None
            if dest is not None and dest.is_finite() and dest != dest.to_integral_value():
                return (
                    f"fractional value {dest} is not an integer for "
                    f"{type_str} — widen the destination to DECIMAL/DOUBLE, "
                    "or round it explicitly before the write"
                )
            return (
                f"{text!r} is not an integer for {type_str} — repair the source "
                "value, or map the column to a text or decimal carrier"
            )
        dec = Decimal(parsed)
    if dec != dec.to_integral_value():
        return (
            f"fractional value {dec} is not an integer for {type_str} "
            "— widen the destination to DECIMAL/DOUBLE, or round it explicitly "
            "before the write"
        )
    lo, hi = bounds
    if not (lo <= int(dec) <= hi):
        return f"{dec} is out of range for {type_str} ({lo}..{hi})"
    return None
