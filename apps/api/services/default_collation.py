"""A destination column collation equal to the database default is not a
declared carrier property.

A ``CREATE TABLE`` that spells no ``COLLATE`` gets the database default:
SQL Server ``SQL_Latin1_General_CP1_CI_AS``, MySQL ``@@collation_database``.
That is what every table DataFlow created before it stamped collations
carries, and what any operator who left the clause out carries. Reading it
back as a declared ``COLLATE`` on the live type made run 2 into DataFlow's
own table a fidelity collapse ("VARCHAR(400) COLLATE UTF8MB4_0900_AI_CI ->
NVARCHAR(400) COLLATE SQL_LATIN1_GENERAL_CP1_CI_AS") although every value
had landed exactly (D275). PostgreSQL introspection already skips its
``default`` collation for the same reason; this is the one rule for the
other two engines.

A collation that differs from the database default was chosen by someone,
stays on the carrier, and is still graded. The per-column ``collation`` fact
is kept beside the type for key-equality planning, which reads it directly;
the plan discloses the default instead of refusing.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

#: One statement per engine; the first column of the first row is the name.
_DEFAULT_COLLATION_SQL: dict[str, str] = {
    "mysql": "SELECT @@collation_database",
    "mariadb": "SELECT @@collation_database",
    "sqlserver": "SELECT CONVERT(NVARCHAR(128), DATABASEPROPERTYEX(DB_NAME(), 'Collation'))",
    "mssql": "SELECT CONVERT(NVARCHAR(128), DATABASEPROPERTYEX(DB_NAME(), 'Collation'))",
}


def database_default_collation(run_scalar: Callable[[str], Any], dialect: str) -> str:
    """The destination database's default collation, or ``""`` when unknown.

    ``run_scalar`` executes one statement and returns its first value (the
    caller owns the connection). Any failure reads as unknown: a collation is
    then kept on the carrier exactly as before.
    """
    sql = _DEFAULT_COLLATION_SQL.get((dialect or "").strip().lower())
    if not sql:
        return ""
    try:
        value = run_scalar(sql)
    except Exception:  # noqa: BLE001 - unknown default keeps today's grading
        return ""
    if isinstance(value, (tuple, list)):
        value = value[0] if value else ""
    return str(value or "").strip()


def default_collation_note(column: str, collation: str) -> str:
    """The disclosure a plan carries for a column at the database default."""
    return (
        f"'{column}' compares with the database default collation {collation} "
        "(what a CREATE without a COLLATE clause gets, as tables made by "
        "earlier Datawrap versions did); values land exactly, comparisons "
        "follow that default."
    )


def declared_collation_carrier(
    logical: str,
    collation: str,
    database_default: str,
    *,
    column: str = "",
    charset: str = "",
) -> tuple[str, str]:
    """``(type, note)``: the carrier with its declared ``COLLATE``, or bare + a note.

    The clause stays when the column's collation differs from the database
    default (someone chose it). At the default the type stays bare and the
    note says so, for the plan to disclose. On MySQL the collation name is
    also the measured character set (``utf8mb4_…``); dropping it must not
    drop that evidence, so the carrier keeps ``CHARACTER SET <charset>``.
    """
    coll = (collation or "").strip()
    if not coll:
        return logical, ""
    default = (database_default or "").strip()
    if default and coll.casefold() == default.casefold():
        cs = (charset or "").strip()
        if cs and "CHARACTER SET" not in logical.upper():
            logical = f"{logical} CHARACTER SET {cs}"
        return logical, default_collation_note(column, coll)
    return f"{logical} COLLATE {coll}", ""


def keep_key_collations(columns: list[dict[str, Any]], unique_meta: dict[str, Any] | None) -> None:
    """Put the ``COLLATE`` clause back on key columns, in place.

    A payload column at the database default compares differently but holds
    every value; a key column's collation *is* its identity: a CI default
    folds ``'A'`` and ``'a'`` into one key, and the D205 collision walk must
    keep seeing that on the catalog type. Only the disclosure note stays off.
    """
    meta = unique_meta or {}
    keyed: set[str] = {str(c).casefold() for c in (meta.get("primary_key_columns") or [])}
    for group in meta.get("unique_keys") or []:
        cols = group.get("columns") if isinstance(group, dict) else group
        keyed.update(str(c).casefold() for c in (cols or []))
    for col in columns or []:
        name = str(col.get("name") or "")
        coll = str(col.get("collation") or "").strip()
        if not coll or name.casefold() not in keyed or not col.get("collation_note"):
            continue
        typ = str(col.get("inferred_type") or "")
        if "COLLATE" not in typ.upper():
            col["inferred_type"] = f"{typ} COLLATE {coll}"
        col["collation_note"] = ""
