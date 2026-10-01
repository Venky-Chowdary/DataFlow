"""One decision for a blank cell on a typed column.

A spreadsheet blank is absence. On a nullable typed destination the row is
kept and the cell is SQL NULL. That is not a cast failure, and it is not
fidelity loss of a value the source actually held.

A proven NOT NULL destination refuses the NULL. The cell stays a finding.
A database extract does not opt in: an empty string there is a stored value,
and turning it into NULL would drop it.

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


def blank_typed_cell_is_sql_null(
    raw: Any,
    err: str | None,
    mapping: dict[str, Any] | None,
    *,
    empty_cells_as_null: bool,
    dest_nullability: dict[str, bool] | None,
    target: str = "",
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
    return typed_blank_stores_sql_null(
        raw,
        mapping,
        named,
        empty_cells_as_null=empty_cells_as_null,
        dest_nullability=dest_nullability,
    )
