"""Columns an overwrite recreate has to keep.

Schema policy does not silent-DROP COLUMN. A full refresh that drops the
destination and creates it from the new source shape used to delete a column
the source no longer has, even when the policy says to keep destination
history.
"""

from __future__ import annotations

from typing import Any


def mapped_target_names(mappings: list[Any] | None) -> set[str]:
    names: set[str] = set()
    for raw in mappings or []:
        if not isinstance(raw, dict):
            continue
        if raw.get("intentional_omit"):
            continue
        target = str(raw.get("target") or raw.get("source") or "").strip()
        if target:
            names.add(target.casefold())
    return names


def columns_to_keep(
    live: list[dict[str, Any]] | None,
    mappings: list[Any] | None,
) -> list[dict[str, str]]:
    """Live destination columns the new mapping does not write."""
    mapped = mapped_target_names(mappings)
    kept: list[dict[str, str]] = []
    seen: set[str] = set()
    for col in live or []:
        if not isinstance(col, dict):
            continue
        name = str(col.get("name") or "").strip()
        if not name or name.casefold() in mapped or name.casefold() in seen:
            continue
        ddl = str(col.get("ddl_type") or "").strip() or "TEXT"
        kept.append({"name": name, "ddl_type": ddl})
        seen.add(name.casefold())
    return kept


def append_kept_column_sql(
    col_defs: str,
    kept: list[dict[str, Any]] | None,
    *,
    dialect: str,
    existing: list[str],
) -> str:
    """Add kept columns to a CREATE body. They are nullable.

    The reload does not write them. A NOT NULL column with no default would
    reject every new row, so the recreated column allows NULL and keeps the
    type the destination already had.
    """
    from connectors.sql_identifiers import quote_sql_identifier

    quote = "`" if (dialect or "").lower() == "mysql" else '"'
    have = {str(name).casefold() for name in existing}
    parts: list[str] = []
    for col in kept or []:
        if not isinstance(col, dict):
            continue
        name = str(col.get("name") or "").strip()
        if not name or name.casefold() in have:
            continue
        ddl = str(col.get("ddl_type") or "").strip() or "TEXT"
        parts.append(f"{quote_sql_identifier(name, quote)} {ddl} NULL")
        have.add(name.casefold())
    if not parts:
        return col_defs
    extra = ", ".join(parts)
    text = col_defs or ""
    if not text.strip():
        return extra
    # Table constraints sit at the end of the CREATE body. A column after
    # PRIMARY KEY is not valid SQL.
    upper = text.upper()
    markers = (", PRIMARY KEY", ", UNIQUE", ", CONSTRAINT", ", CHECK")
    indexes = [upper.find(marker) for marker in markers]
    indexes = [index for index in indexes if index >= 0]
    if not indexes:
        return f"{text}, {extra}"
    at = min(indexes)
    return f"{text[:at]}, {extra}{text[at:]}"
