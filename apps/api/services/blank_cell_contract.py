"""One decision for a blank cell on a typed column.

A spreadsheet blank is absence. On a nullable typed destination the row is
kept and the cell is SQL NULL. That is not a cast failure, and it is not
fidelity loss of a value the source actually held.

A proven NOT NULL destination refuses the NULL. The cell stays a finding.
A text column's empty string is a stored value. A numeric, temporal, boolean,
uuid, or binary column cannot store ``""`` — that spelling is a SQL NULL the
extract flattened, and a nullable destination keeps it as NULL.

Validate dry-run, Gate-8, the coercion probe, and the file writer all call
this module. A second copy of the rule is how Execute used to reject phones
Validate had already accepted.
"""

from __future__ import annotations

from typing import Any


def is_spreadsheet_blank(value: Any) -> bool:
    """True for extract NULL, missing, and whitespace. ``0`` and ``False`` stay present."""
    from services.value_serializer import is_null_evidence

    return is_null_evidence(value)


def _target_not_null(
    mapping: dict[str, Any] | None,
    target: str,
    dest_nullability: dict[str, bool] | None,
) -> bool:
    from connectors.writer_common import _target_explicitly_not_null

    return _target_explicitly_not_null(mapping, target, dest_nullability or {})


# Carriers that have no empty-string domain. A blank cell on one of these is
# a flattened SQL NULL, not a value the source stored.
_NULL_ONLY_BLANK_LOGICALS = frozenset(
    {
        "integer",
        "float",
        "decimal",
        "number",
        "money",
        "boolean",
        "date",
        "datetime",
        "time",
        "timestamp",
        "uuid",
        "binary",
    }
)


def source_carrier_cannot_store_empty(source_type: str) -> bool:
    """True when ``""`` cannot be a real value of this source carrier."""
    from services.type_system import normalize_logical_type

    return normalize_logical_type(source_type) in _NULL_ONLY_BLANK_LOGICALS


def typed_blank_stores_sql_null(
    raw: Any,
    mapping: dict[str, Any] | None,
    target: str,
    *,
    empty_cells_as_null: bool,
    dest_nullability: dict[str, bool] | None,
) -> bool:
    """Blank file cell → SQL NULL, before a typed cast is attempted.

    ``empty_cells_as_null`` is the file-source opt-in. Unknown nullability
    stays nullable. Proven NOT NULL returns false.
    """
    if not empty_cells_as_null or not is_spreadsheet_blank(raw):
        return False
    return not _target_not_null(mapping, target, dest_nullability)


def nullable_typed_blank_is_absence(
    raw: Any,
    mapping: dict[str, Any] | None,
    target: str,
    *,
    empty_cells_as_null: bool,
    dest_nullability: dict[str, bool] | None,
    source_type: str = "",
    database_extract: bool = False,
) -> bool:
    """Blank cell → SQL NULL on a nullable typed destination.

    File blanks opt in with ``empty_cells_as_null``. A database carrier that
    cannot store ``""`` (numeric, temporal, boolean, uuid, binary) reaches
    Validate as a flattened blank; that is the source NULL, including on
    overwrite and in lenient mode. A text column's empty string stays stored.
    A file column keeps the opt-in even when inference labeled it INTEGER.
    Proven NOT NULL stays a finding.
    """
    if typed_blank_stores_sql_null(
        raw,
        mapping,
        target,
        empty_cells_as_null=empty_cells_as_null,
        dest_nullability=dest_nullability,
    ):
        return True
    if not database_extract or not is_spreadsheet_blank(raw):
        return False
    if _target_not_null(mapping, target, dest_nullability):
        return False
    declared = str(source_type or (mapping or {}).get("source_type") or "")
    return bool(declared) and source_carrier_cannot_store_empty(declared)


def blank_typed_cell_is_sql_null(
    raw: Any,
    err: str | None,
    mapping: dict[str, Any] | None,
    *,
    empty_cells_as_null: bool,
    dest_nullability: dict[str, bool] | None,
    target: str = "",
    database_extract: bool = False,
    source_type: str = "",
) -> bool:
    """Same decision after ``apply_transform`` refused an empty typed cell.

    ``target`` is the destination column the writer is binding. Nullability
    is looked up on that name, then on the mapping's target.
    """
    if not err or not str(err).lower().startswith("empty value cannot coerce"):
        return False
    named = str(
        target
        or (mapping or {}).get("target")
        or (mapping or {}).get("source")
        or ""
    )
    return nullable_typed_blank_is_absence(
        raw,
        mapping,
        named,
        empty_cells_as_null=empty_cells_as_null,
        dest_nullability=dest_nullability,
        source_type=source_type or str((mapping or {}).get("source_type") or ""),
        database_extract=database_extract,
    )
