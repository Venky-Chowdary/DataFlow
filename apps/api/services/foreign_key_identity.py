"""One identity for a foreign key, shared by the catalog diff and the orphan scan.

A relationship is the parent relation plus the set of (child column, parent
column) pairs. DDL order is not a second relationship. Two schema-qualified
names with different schemas are not the same table. An unqualified name
matches one qualified name, because many engines omit the default schema.

The catalog diff and Gate-8 must not each invent a comparison. A diff that
ignores schema will call ``sales.parent`` carried when the destination
constraint points at ``archive.parent``, and the orphan scan will then refuse
that same pair.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def fold(name: Any) -> str:
    return str(name or "").strip().casefold()


def table_parts(name: str) -> tuple[str | None, str]:
    """``(schema, table)``. An unqualified name has no schema."""
    folded = fold(name)
    if "." not in folded:
        return None, folded
    schema, table = folded.rsplit(".", 1)
    if not schema or not table:
        return None, folded
    return schema, table


def same_parent_table(left: str, right: str) -> bool:
    """True when both names are the same destination relation."""
    if fold(left) == fold(right):
        return True
    left_schema, left_table = table_parts(left)
    right_schema, right_table = table_parts(right)
    if not left_table or left_table != right_table:
        return False
    if left_schema is None or right_schema is None:
        return True
    return left_schema == right_schema


def relationship_identity(
    child_columns: list[str] | tuple[str, ...],
    parent_table: str,
    parent_columns: list[str] | tuple[str, ...],
) -> tuple[str, tuple[tuple[str, str], ...]] | None:
    """Parent table plus the set of (child column, parent column) pairs.

    ``(a, b) REFERENCES (x, y)`` is the same relationship as
    ``(b, a) REFERENCES (y, x)``. A missing or uneven pairing is not an
    identity.
    """
    children = [str(c).strip() for c in child_columns]
    parents = [str(c).strip() for c in parent_columns]
    if not str(parent_table or "").strip() or not children or len(children) != len(parents):
        return None
    if any(not child or not parent for child, parent in zip(children, parents)):
        return None
    pairs = tuple(sorted((fold(child), fold(parent)) for child, parent in zip(children, parents)))
    return (fold(parent_table), pairs)


def qualified_parent(fk: Mapping[str, Any]) -> tuple[str, str]:
    """``(schema, table)`` from a catalog fact or a rendered parent token."""
    schema = fold(fk.get("referred_schema") or fk.get("referenced_schema") or "")
    table = fold(fk.get("referred_table") or fk.get("referenced_table") or "")
    if not schema and "." in table:
        parsed_schema, parsed_table = table_parts(table)
        if parsed_schema and parsed_table:
            return parsed_schema, parsed_table
    return schema, table


def parent_label(schema: str, table: str) -> str:
    if schema and table:
        return f"{schema}.{table}"
    return table


def fk_identity(fk: Mapping[str, Any]) -> tuple[str, tuple[tuple[str, str], ...]] | None:
    schema, table = qualified_parent(fk)
    return relationship_identity(
        list(fk.get("constrained_columns") or fk.get("columns") or ()),
        parent_label(schema, table),
        list(fk.get("referred_columns") or fk.get("referenced_columns") or ()),
    )


def same_relationship(
    left: tuple[str, tuple[tuple[str, str], ...]] | None,
    right: tuple[str, tuple[tuple[str, str], ...]] | None,
) -> bool:
    if left is None or right is None:
        return False
    return left[1] == right[1] and same_parent_table(left[0], right[0])


def foreign_key_wire(
    child_columns: tuple[str, ...] | list[str],
    schema: str,
    table: str,
    parent_columns: tuple[str, ...] | list[str],
) -> tuple[str, str, str]:
    """``(child columns, parent relation, parent columns)`` for the catalog report.

    Built from the fact, never by splitting a rendered string. A column name
    that itself contains ``->`` must not change how many parts the wire has.
    """
    parent = parent_label(fold(schema), fold(table))
    return (
        "+".join(fold(c) for c in child_columns if str(c).strip()),
        parent,
        "+".join(fold(c) for c in parent_columns if str(c).strip()),
    )


def render_foreign_key_fact(
    child_columns: tuple[str, ...] | list[str],
    schema: str,
    table: str,
    parent_columns: tuple[str, ...] | list[str],
) -> str:
    """Catalog wire. Schema is part of the parent token when the catalog named one."""
    return "->".join(foreign_key_wire(child_columns, schema, table, parent_columns))
