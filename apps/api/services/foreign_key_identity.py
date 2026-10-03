"""One identity for a foreign key, shared by catalog diff, orphan scan, and load order.

A relationship is the parent relation plus the set of (child column, parent
column) pairs. DDL order is not a second relationship. Two schema-qualified
names with different schemas are not the same table. An unqualified name
matches one qualified name, because many engines omit the default schema.

The catalog diff, Gate-8, and parents-first ordering must not each invent a
comparison. A diff that ignores schema will call ``sales.parent`` carried
when the destination constraint points at ``archive.parent``. A load order
that ignores schema will wait on the local ``customers`` table when the
foreign key points at ``archive.customers``.

The orphan scan reads the same fact. Source catalogs say ``columns`` /
``referenced_*``, Studio says ``ref_table``, and the destination inspector
says ``constrained_columns`` / ``referred_*``. Picking the first non-empty
alias scans whichever parent was listed first. When those aliases name
different relations, the scan does not run.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, NamedTuple

from connectors.sql_identifiers import split_qualified_table


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


class ParsedForeignKey(NamedTuple):
    """One scan target, or a conflict when the payload names more than one.

    ``scan_label`` is the string the anti-join passes to
    ``split_qualified_table``. A quoted identifier that contains a dot stays
    one table name. ``child_columns`` keeps the first spelling when every
    present alias fold-matches; a disagreement leaves them empty and sets
    ``conflict``.
    """

    child_columns: tuple[str, ...]
    parent_schema: str
    parent_table: str
    parent_columns: tuple[str, ...]
    scan_label: str
    conflict: str


class _Parent(NamedTuple):
    schema: str
    table: str
    label: str


_CHILD_KEYS = ("columns", "column", "fk_columns", "constrained_columns")
_PARENT_COL_KEYS = ("referenced_columns", "ref_columns", "referred_columns")
_PARENT_SHAPES = (
    ("referenced_schema", "referenced_table"),
    ("ref_schema", "ref_table"),
    ("referred_schema", "referred_table"),
)


def _blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple)):
        return not any(str(item).strip() for item in value)
    return False


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _needs_quote(name: str) -> bool:
    return any(ch in name for ch in (".", '"', " "))


def scan_label(schema: str, table: str) -> str:
    """Qualified name the anti-join can split back into schema and table.

    An ordinary ``sales.orders`` stays unquoted. A table whose own name
    contains a dot is quoted, so a later split does not invent a schema.
    """
    leaf = _quote_ident(table) if _needs_quote(table) else table
    if not schema:
        return leaf
    qualifier = _quote_ident(schema) if _needs_quote(schema) else schema
    return f"{qualifier}.{leaf}"


def _read_column_lists(
    fk: Mapping[str, Any],
    keys: Sequence[str],
    *,
    role: str,
) -> tuple[tuple[str, ...], str]:
    found: list[list[str]] = []
    for key in keys:
        if key not in fk or _blank(fk.get(key)):
            continue
        raw = fk.get(key)
        if isinstance(raw, str):
            cols = [raw.strip()]
        elif isinstance(raw, (list, tuple)):
            cols = [str(item).strip() for item in raw if str(item).strip()]
        else:
            return (), f"Foreign key {role} could not be read."
        if cols:
            found.append(cols)
    if not found:
        return (), ""
    first = found[0]
    folded = [fold(col) for col in first]
    distinct: list[list[str]] = [first]
    for other in found[1:]:
        if [fold(col) for col in other] == folded:
            continue
        other_folded = [fold(col) for col in other]
        if not any(other_folded == [fold(col) for col in seen] for seen in distinct):
            distinct.append(other)
    if len(distinct) > 1:
        rendered = " and ".join("+".join(cols) for cols in distinct)
        return (), f"Foreign key names two {role} ({rendered})."
    return tuple(first), ""


def _resolve_parent(schema_raw: Any, table_raw: Any) -> tuple[_Parent | None, str]:
    schema = "" if schema_raw is None else str(schema_raw).strip()
    table = "" if table_raw is None else str(table_raw).strip()
    if not schema and not table:
        return None, ""
    if not table:
        return None, f"Foreign key names parent schema {schema} without a table."
    own_schema, own_table = split_qualified_table(table, None)
    own_schema = own_schema or ""
    own_table = (own_table or "").strip()
    if not own_table:
        return None, "Foreign key parent table could not be read."
    if schema and own_schema and fold(schema) != fold(own_schema):
        return None, (
            f"Foreign key schema {schema} does not match table qualifier "
            f"{own_schema} on {own_schema}.{own_table}."
        )
    if own_schema:
        # The explicit schema and the qualifier are the same relation.
        # Qualify once; do not emit ``main.main.customers``.
        chosen_schema = schema or own_schema
        return _Parent(chosen_schema, own_table, parent_label(chosen_schema, own_table)), ""
    if schema:
        return _Parent(schema, own_table, parent_label(schema, own_table)), ""
    return _Parent("", own_table, own_table), ""


def _prefer_parent(current: _Parent, new: _Parent) -> _Parent:
    """Keep the schema-qualified label when an unqualified alias is the same table."""
    if new.schema and not current.schema:
        return new
    return current


def _merge_parents(parents: list[_Parent]) -> tuple[_Parent | None, str]:
    if not parents:
        return None, ""
    groups: list[_Parent] = []
    for parent in parents:
        matched = False
        for index, group in enumerate(groups):
            if same_parent_table(group.label, parent.label):
                groups[index] = _prefer_parent(group, parent)
                matched = True
                break
        if not matched:
            groups.append(parent)
    if len(groups) > 1:
        rendered = " and ".join(group.label for group in groups)
        return None, f"Foreign key names two parents ({rendered})."
    return groups[0], ""


def parse_foreign_key(fk: Mapping[str, Any] | None) -> ParsedForeignKey:
    """The one parent this payload names, or a conflict.

    Aliases that fold to the same columns, and parent labels
    :func:`same_parent_table` treats as one relation, are one target. The
    scan uses the qualified label when only one side carried a schema.
    Two present aliases that are not that relation are a conflict: the scan
    must not certify the parent that happens to contain the child key.
    """
    empty = ParsedForeignKey((), "", "", (), "", "")
    if not isinstance(fk, Mapping):
        return empty._replace(conflict="Foreign key could not be read.")
    child, child_error = _read_column_lists(fk, _CHILD_KEYS, role="child column lists")
    if child_error:
        return empty._replace(conflict=child_error)
    parent_cols, parent_col_error = _read_column_lists(
        fk, _PARENT_COL_KEYS, role="parent column lists"
    )
    if parent_col_error:
        return empty._replace(child_columns=child, conflict=parent_col_error)
    parents: list[_Parent] = []
    for schema_key, table_key in _PARENT_SHAPES:
        schema_present = schema_key in fk and not _blank(fk.get(schema_key))
        table_present = table_key in fk and not _blank(fk.get(table_key))
        if not schema_present and not table_present:
            continue
        parent, error = _resolve_parent(
            fk.get(schema_key) if schema_present else "",
            fk.get(table_key) if table_present else "",
        )
        if error:
            return ParsedForeignKey(child, "", "", parent_cols, "", error)
        if parent is not None:
            parents.append(parent)
    chosen, merge_error = _merge_parents(parents)
    if merge_error or chosen is None:
        return ParsedForeignKey(child, "", "", parent_cols, "", merge_error)
    return ParsedForeignKey(
        child,
        chosen.schema,
        chosen.table,
        parent_cols,
        scan_label(chosen.schema, chosen.table),
        "",
    )


def qualified_parent(fk: Mapping[str, Any]) -> tuple[str, str]:
    """``(schema, table)`` from a catalog fact. Empty when the fact conflicts."""
    parsed = parse_foreign_key(fk)
    if parsed.conflict or not parsed.parent_table:
        return "", ""
    return fold(parsed.parent_schema), fold(parsed.parent_table)


def parent_label(schema: str, table: str) -> str:
    if schema and table:
        return f"{schema}.{table}"
    return table


def select_job_table(
    schema: str,
    table: str,
    selected: list[str],
    *,
    job_schema: str = "",
) -> str | None:
    """The one selected stream this catalog parent refers to.

    Unqualified stream names are tables in ``job_schema``. A parent the catalog
    placed in another schema matches only a selected name that carries that
    schema. ``None`` when the parent is outside the job. Binding ``archive.customers``
    to a local ``customers`` stream would order the wrong parent first.
    """
    leaf = fold(table)
    if not leaf:
        return None
    parent_schema = fold(schema)
    job = fold(job_schema)
    effective = parent_schema or job
    qualified = parent_label(effective, leaf) if effective else leaf
    exact = [name for name in selected if name and fold(name) == qualified]
    if len(exact) == 1:
        return exact[0]
    # A different schema never borrows an unqualified stream of the same leaf.
    if parent_schema and job and parent_schema != job:
        return None
    if parent_schema and not job:
        return None
    bare = [
        name
        for name in selected
        if name and table_parts(name)[0] is None and fold(name) == leaf
    ]
    if len(bare) == 1:
        return bare[0]
    if job:
        in_job = [
            name for name in selected if name and fold(name) == parent_label(job, leaf)
        ]
        if len(in_job) == 1:
            return in_job[0]
    return None


def fk_identity(fk: Mapping[str, Any]) -> tuple[str, tuple[tuple[str, str], ...]] | None:
    parsed = parse_foreign_key(fk)
    if parsed.conflict:
        return None
    return relationship_identity(
        parsed.child_columns,
        parent_label(parsed.parent_schema, parsed.parent_table),
        parsed.parent_columns,
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
