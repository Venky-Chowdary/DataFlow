"""Selected-table catalog for rule bind.

Clio / Cupid: a correspondence names an object + attribute. The same column
name on two selected tables is not a unique winner — that is how migrations
silently remap ``id``. This module does not invent joins.
"""

from __future__ import annotations

from .normalize import fold, split_qualified


def split_selected_tables(
    source_table: str = "",
    source_tables: list[str] | None = None,
) -> list[str]:
    """Form default may be one name or a comma list. Prefer an explicit list."""
    if source_tables:
        seen: set[str] = set()
        out: list[str] = []
        for raw in source_tables:
            name = (raw or "").strip()
            if not name:
                continue
            key = fold(name)
            if key in seen:
                continue
            seen.add(key)
            out.append(name)
        return out
    text = (source_table or "").strip()
    if not text:
        return []
    if "," in text:
        return split_selected_tables("", [part.strip() for part in text.split(",")])
    return [text]


def catalog_table(name: str, catalog: dict[str, list[str]]) -> tuple[str, list[str]]:
    """Canonical catalog key + columns, or ('', []) when the table is unknown."""
    want = fold(name)
    if not want:
        return "", []
    for table, cols in catalog.items():
        if fold(table) == want:
            return table, [str(c) for c in cols if str(c).strip()]
    return "", []


def owners_of_column(column: str, catalog: dict[str, list[str]]) -> list[str]:
    """Tables that own this unqualified name. More than one is not unique."""
    spoken = split_qualified(column)[1] or column
    want = fold(spoken)
    if not want or not catalog:
        return []
    owners: list[str] = []
    for table, cols in catalog.items():
        if any(fold(c) == want or fold(split_qualified(c)[1]) == want for c in cols):
            owners.append(table)
    return owners


def resolve_catalog_table(
    *,
    cell_table: str,
    qualified_table: str,
    spoken_column: str,
    selected: list[str],
    catalog: dict[str, list[str]],
    form_default: str,
    noun: str = "source",
) -> tuple[str, str]:
    """Effective table + reason when bind must stay in review.

    Unique owner of an unqualified name is allowed (Cupid unique winner).
    Several owners, an unnamed table among many selected, or a table that
    is not selected, stay unbound.
    """
    label = "Source table" if noun == "source" else "Destination table"
    named = (cell_table or qualified_table or "").strip()
    if named:
        if selected and not any(fold(named) == fold(item) for item in selected):
            return named, (
                f"{label} “{named}” is not among the selected tables "
                f"({', '.join(selected)}). It was not applied."
            )
        if catalog:
            canon, _cols = catalog_table(named, catalog)
            if not canon:
                return named, (
                    f"{label} “{named}” is not in the introspected catalog. "
                    "It was not applied."
                )
            return canon, ""
        return named, ""

    if len(selected) == 1:
        return selected[0], ""
    if form_default and "," not in form_default and len(selected) <= 1:
        return form_default, ""

    if catalog and spoken_column:
        owners = owners_of_column(spoken_column, catalog)
        if len(owners) == 1:
            return owners[0], ""
        if len(owners) > 1:
            return "", (
                f"“{spoken_column}” is on {', '.join(owners)}. "
                "Name Table.column — a guessed table is silent remap."
            )

    if len(selected) > 1:
        return "", (
            f"Several {noun} tables are selected. Name the {noun} table "
            f"(or Table.column) — a primary-stream guess is silent remap."
        )
    return "", ""


def resolve_source_table(
    *,
    cell_table: str,
    qualified_table: str,
    spoken_column: str,
    selected: list[str],
    catalog: dict[str, list[str]],
    form_default: str,
) -> tuple[str, str]:
    return resolve_catalog_table(
        cell_table=cell_table,
        qualified_table=qualified_table,
        spoken_column=spoken_column,
        selected=selected,
        catalog=catalog,
        form_default=form_default,
        noun="source",
    )


def lookup_type(column: str, table: str, types: dict[str, str] | None) -> str:
    """Prefer ``table.column`` then an unambiguous column key."""
    if not types or not column:
        return ""
    keys = [f"{table}.{column}"] if table else []
    keys.append(column)
    for key in keys:
        if key in types:
            return str(types[key])
        want = fold(key)
        for raw, value in types.items():
            if fold(raw) == want:
                return str(value)
    want = fold(column)
    hits = [
        (raw, value)
        for raw, value in types.items()
        if fold(split_qualified(raw)[1] or raw) == want
    ]
    if len(hits) == 1:
        return str(hits[0][1])
    if table:
        for raw, value in hits:
            owner, _ = split_qualified(raw)
            if owner and fold(owner) == fold(table):
                return str(value)
    return ""


def normalize_catalog(raw: dict[str, list[str]] | None) -> dict[str, list[str]]:
    return {
        str(table): [str(col) for col in cols if str(col).strip()]
        for table, cols in (raw or {}).items()
        if str(table).strip()
    }


def bind_columns_for_table(
    table: str,
    catalog: dict[str, list[str]],
    fallback: list[str],
) -> list[str]:
    """Columns the spoken source may bind against for this table."""
    if table and catalog:
        canon, cols = catalog_table(table, catalog)
        return cols if canon else []
    return list(fallback)
