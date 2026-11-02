from __future__ import annotations

from typing import Any
from connectors.writer_common import (
    _ARRAY_NESTED_FORBIDDEN_DIALECTS,
    _ARRAY_NULL_STRICT_DIALECTS,
    _infer_dest_db_from_dialect_label,
    _unfit_cell_absent,
    append_write_quarantine_detail,
    array_element_unfit_reason,
    cell_to_string,
    fits_varchar,
    parse_array_wire_elements,
    string_storage_units,
    varchar_overflow_suggested_type,
)

def quarantine_unfit_strings(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    dialect_label: str = "VARCHAR",
    dest_db: str = "",
) -> list[tuple]:
    """Hold out / NULL cells that exceed bounded VARCHAR/NVARCHAR/CHAR width.

    Preflight only sees samples — production rows longer than samples used to
    reach SQL Server and silently truncate. Quarantine is fail-closed.
    Unlimited carriers (TEXT, NVARCHAR(MAX), STRING) skip the width check
    and still run encoding-capacity (utf8mb3 TEXT cannot store emoji).
    """

    from services.ddl_compatibility import parse_varchar_width
    from services.encoding_capacity import quarantine_unfit_encoding

    width_cols: list[tuple[int, int, str]] = []
    for i, typ in enumerate(target_types):
        width = parse_varchar_width(typ)
        if width is not None:
            width_cols.append((i, width, typ))

    kept = mapped_rows
    if width_cols:
        from services.value_serializer import cell_to_string

        out: list[tuple] = []
        for row_idx, row in enumerate(mapped_rows):
            cells = list(row)
            hold_out = False
            for col_idx, width, typ in width_cols:
                if _unfit_cell_absent(cells, col_idx):
                    continue
                cell = cells[col_idx]
                # ASCII text spends one unit per character under every length
                # rule (code points, UTF-8 bytes, UTF-16 units), so a short
                # ASCII value fits without resolving the dialect unit.
                if type(cell) is str and len(cell) <= width and cell.isascii():
                    continue
                if fits_varchar(
                    cells[col_idx], width, typ, dialect_label=dialect_label
                ):
                    continue
                sample = cell_to_string(cells[col_idx])[:120]
                units = string_storage_units(
                    cells[col_idx], typ, dialect_label=dialect_label
                )
                suggested_type = varchar_overflow_suggested_type(
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
                        f"value length {units} exceeds {dialect_label}({width}) "
                        "— quarantined (would truncate on write)"
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
        kept = out

    engine = (dest_db or "").strip() or _infer_dest_db_from_dialect_label(
        dialect_label
    )
    return quarantine_unfit_encoding(
        kept,
        target_cols,
        target_types,
        rejected_details,
        policy,
        dest_db=engine,
    )


def quarantine_unfit_arrays(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    dialect_label: str = "destination",
    dest_db: str = "",
) -> list[tuple]:
    """Hold out ARRAY cells whose payload or elements cannot reach the column.

    Scalar carriers each had a fail-closed gate while ARRAY columns had none —
    malformed payloads and unfit elements reached PG ``int[]``, BigQuery
    ``ARRAY<INT64>``, ClickHouse ``Array(T)``, and lakehouse list columns
    unchecked. Engine rules enforced here:

    - BigQuery result ARRAYs may not contain NULL elements, and arrays of
      arrays are unsupported (ARRAY of STRUCT is required).
    - ClickHouse ``Array(T)`` rejects NULL unless declared ``Array(Nullable(T))``.
    - Postgres permits both NULL arrays and NULL elements.

    Ambiguous scalars (SET joiner text, engine-native literals) are never held
    out — only unambiguous breakage is.
    """

    from services.type_system import (
        LOGICAL_ARRAY,
        normalize_logical_type,
        parse_array_element,
    )

    array_cols: list[tuple[int, str, str]] = []
    for i, typ in enumerate(target_types):
        raw = (typ or "").strip()
        if not raw:
            continue
        is_array = raw.endswith("[]") or normalize_logical_type(raw) == LOGICAL_ARRAY
        if not is_array:
            continue
        element = (
            raw[:-2].strip() if raw.endswith("[]") else (parse_array_element(raw) or "")
        )
        array_cols.append((i, element, raw))
    if not array_cols:
        return mapped_rows

    label = (dialect_label or "destination").strip() or "destination"
    low = label.lower()
    null_strict = any(d in low for d in _ARRAY_NULL_STRICT_DIALECTS)
    nested_forbidden = any(d in low for d in _ARRAY_NESTED_FORBIDDEN_DIALECTS)


    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, element_carrier, typ in array_cols:
            if _unfit_cell_absent(cells, col_idx):
                continue
            elements, parse_error = parse_array_wire_elements(cells[col_idx])
            reason = ""
            if parse_error:
                reason = f"{parse_error} — quarantined (would not load into {typ})"
            elif elements is not None:
                for element in elements:
                    if element is None:
                        if null_strict:
                            reason = (
                                f"{label} ARRAY element may not be NULL "
                                f"(declare Array(Nullable(T)) or drop the element) "
                                f"— quarantined"
                            )
                            break
                        continue
                    if nested_forbidden and isinstance(element, (list, tuple)):
                        reason = (
                            f"{label} does not support arrays of arrays "
                            "(use ARRAY of STRUCT) — quarantined"
                        )
                        break
                    unfit = array_element_unfit_reason(
                        element, element_carrier, dest_db=dest_db
                    )
                    if unfit:
                        reason = f"{unfit} — quarantined (would not load into {typ})"
                        break
            if not reason:
                continue
            append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": cell_to_string(cells[col_idx])[:120],
                    "reason": f"{label} ARRAY: {reason}",
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
