"""Two mapped targets the destination's naming rule turns into one column.

``a b`` and ``a_b`` both land as ``a_b`` on a SQL destination (the writer's
identifier rule). The writer kept the first and dropped the other column's
values without a word, and Gate-8, which now reads back by the same names,
could not see it either. Validate (``ddl_compatibility``) and the writer
(``writer_common.resolve_target_columns``) ask this one check and refuse,
naming both source columns.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def _skipped_by_writer(mapping: dict[str, Any], table_exists: bool | None) -> bool:
    """The writer's own skips (``writer_common.resolve_target_columns``)."""
    from services.mapping_constraints import is_intentional_omit

    if not mapping.get("target") or is_intentional_omit(mapping):
        return True
    # Destination existence unknown at Map: the writer invents no column for it.
    return (
        str(mapping.get("assignment_strategy") or "") == "pending_dest_schema"
        and table_exists is not False
    )


def target_name_collisions(
    mappings: list[dict[str, Any]] | None,
    *,
    dest_type: str = "",
    identifier: Callable[[Any], str] | None = None,
    table_exists: bool | None = None,
) -> list[tuple[str, list[str]]]:
    """``(landed name, [source columns])`` for each column several sources land in.

    The planner may already have sanitised both targets to ``a_b``; the
    operator may have typed ``a b`` and ``a_b``, or ``Name`` and ``name``.
    Either way two source columns share one destination column. One source
    mapped twice is not a collision, and a mapping the writer skips is not
    written. Validate and the writer call this with the same inputs, so they
    decide alike whichever case rule the writer names with.
    """
    from connectors.destination_naming import stored_column_key, target_identifier

    name_of = identifier or target_identifier(dest_type) or (lambda name: str(name))
    landed_by_key: dict[str, str] = {}
    sources_by_key: dict[str, list[str]] = {}
    for mapping in mappings or []:
        if not isinstance(mapping, dict) or _skipped_by_writer(mapping, table_exists):
            continue
        landed = str(name_of(str(mapping["target"])))
        key = stored_column_key(dest_type, landed)
        landed_by_key.setdefault(key, landed)
        source = str(mapping.get("source") or mapping["target"])
        sources = sources_by_key.setdefault(key, [])
        if source not in sources:
            sources.append(source)
    return [
        (landed_by_key[key], sources) for key, sources in sources_by_key.items() if len(sources) > 1
    ]


def target_collision_message(collisions: list[tuple[str, list[str]]]) -> str:
    """Operator text naming every colliding source column; "" when none."""
    parts = [
        " and ".join(f"'{s}'" for s in sources) + f" would both land in destination column '{landed}'"
        for landed, sources in collisions
    ]
    if not parts:
        return ""
    return (
        "Target column name collision: " + "; ".join(parts) + ". The destination's column "
        "naming turns them into one name, so one column's values would be lost. Rename one "
        "of the targets in Map."
    )


def refuse_target_collisions(
    mappings: list[dict[str, Any]] | None, *, identifier: Callable[[Any], str] | None = None,
    dest_type: str = "", table_exists: bool | None = None,
) -> None:
    """Raise ``ValueError`` (the writer's refusal) when two targets collide."""
    message = target_collision_message(
        target_name_collisions(
            mappings, dest_type=dest_type, identifier=identifier, table_exists=table_exists
        )
    )
    if message:
        raise ValueError(message)
