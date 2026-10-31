"""The equality a text key column DataFlow creates carries (D205 family, round 10).

A text key is unique *as the source compares it*. A CSV, an object store, an
API or a case-sensitive database tells ``'abc'`` from ``'ABC'``, ``'Straße'``
from ``'Strasse'`` and ``'😀'`` from ``'🚀'``. A key column created with no
COLLATE takes the server default instead: SQL Server's
``SQL_Latin1_General_CP1_CI_AS`` folds case, expands ``ß`` and gives no sort
weight at all to supplementary characters (so ``'𝔘𝔫𝔦𝔠𝔬𝔡𝔢'`` equals ``''``
and ``'  '``); MySQL's ``utf8mb4_0900_ai_ci`` folds case and accents. Two
distinct source rows then met as one key: the run died mid-write on a
duplicate key (SQL Server) or failed its row-count proof (MySQL).

Map already stamps the destination-native equality on the planned type
(:func:`services.collation_carry.apply_dest_native_collation`, e.g.
``NVARCHAR(MAX) COLLATE Latin1_General_BIN``) and the D205 key walk judges
collisions under that stamp. CREATE dropped it, so the walk and the table
disagreed. This module gives CREATE the same stamp for the key columns.
"""

from __future__ import annotations

from services.source_engine_scope import active_source_engine

_PROBE_CARRIER = {"mssql": "NVARCHAR(450)", "sqlserver": "NVARCHAR(450)"}


def created_key_collation(dialect_name: str, db_type: str = "", source_type: str = "") -> str:
    """COLLATE name for a text key column of a table DataFlow creates, or ``""``.

    The same answer Map stamped on the planned type: the source's equality in
    the destination's spelling. ``""`` when the source engine is unknown (the
    D205 walk then asks the destination itself) or the engine takes no COLLATE.
    """
    from services.collation_carry import apply_dest_native_collation
    from services.type_system import parse_collation

    dest = (db_type or dialect_name or "").strip().lower()
    carrier = _PROBE_CARRIER.get((dialect_name or "").strip().lower(), "VARCHAR(255)")
    stamped = apply_dest_native_collation(
        carrier,
        dest_dialect=dest,
        source_dialect=active_source_engine(),
        source_type=source_type or "VARCHAR",
    )
    return (parse_collation(stamped) or "").strip().strip('"')


def key_collation_suffix(
    column: str, target_columns: list[str], target_types: list[str], dest_dialect: str
) -> list[str]:
    """``["COLLATE <name>"]`` for a created text key column, else ``[]``.

    Only a character column that does not already spell its own collation or
    character set; the planner's other suffixes (NOT NULL) follow it.
    """
    import re

    from services.type_system import normalize_logical_type

    try:
        ddl = str(target_types[list(target_columns).index(column)] or "")
    except (ValueError, IndexError):
        return []
    if not ddl or normalize_logical_type(ddl) not in {"string", "text"}:
        return []
    if re.search(r"\b(?:COLLATE|CHARACTER\s+SET|CHARSET)\b", ddl, re.I):
        return []
    name = created_key_collation(dest_dialect, dest_dialect)
    return [f"COLLATE {name}"] if name else []
