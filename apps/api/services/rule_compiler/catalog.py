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


def resolve_source_table(
    *,
    cell_table: str,
    qualified_table: str,
    spoken_column: str,
    selected: list[str],
    catalog: dict[str, list[str]],
    form_default: str,
) -> tuple[str, str]:
    """Effective source table + reason when bind must stay in review.

    Unique owner of an unqualified name is allowed (Cupid unique winner).
    Several owners, an unnamed table among many selected, or a table that
    is not selected, stay unbound.
    """
    named = (cell_table or qualified_table or "").strip()
    if named:
        if selected and not any(fold(named) == fold(item) for item in selected):
            return named, (
                f"Source table “{named}” is not among the selected tables "
                f"({', '.join(selected)}). It was not applied."
            )
        if catalog:
            canon, cols = catalog_table(named, catalog)
            if not canon:
                return named, (
                    f"Source table “{named}” is not in the introspected catalog. "
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
            "Several source tables are selected. Name the source table "
            "(or Table.column) — a primary-stream guess is silent remap."
        )
    return "", ""


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
