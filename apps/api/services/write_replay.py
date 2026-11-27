"""One source cell as the writer wrote it: the writer's own pipeline, replayed.

Used wherever Datawrap must reproduce what a run wrote (Parity at Scale's
judge for crosswalked columns, the repair SQL it shows a reviewer): it calls
the writer's per-cell function (``connectors.writer_common.convert_mapped_cell``:
transform, code crosswalk, blank-cell rule, business-rule contracts) under the
run's number and date locales, rather than re-implementing any step. A value
the pipeline refuses comes back with its error and must not be written.
"""

from __future__ import annotations

from typing import Any

#: Mapping keys the writer's per-cell pipeline reads.
WRITE_KEYS = ("transform", "code_crosswalk", "code_crosswalk_system", "user_override", "userOverride",
              "transform_override", "rule_contracts", "omit", "omitted")


def write_mapping(mapping: dict[str, Any]) -> dict[str, Any]:
    """The part of a column mapping the write pipeline reads (stable, serialisable)."""
    return {k: mapping[k] for k in WRITE_KEYS if k in mapping and mapping[k] not in (None, "")}


def replay_write(value: Any, mapping: dict[str, Any], *, target: str, number_locale: str = "",
                 date_locale: str = "") -> tuple[Any, str | None]:
    """``(value as written, error)`` for one source cell under the run's locales."""
    from connectors.writer_common import convert_mapped_cell

    from services.transform_engine import (
        NUMBER_LOCALE_WIRE,
        reset_active_date_locale,
        reset_active_number_locale,
        set_active_date_locale,
        set_active_number_locale,
    )
    from services.value_serializer import cell_to_string

    raw = None if value is None else cell_to_string(value, preserve_sql_null=True)
    number_token = set_active_number_locale(number_locale or NUMBER_LOCALE_WIRE)
    date_token = set_active_date_locale(date_locale) if date_locale else None
    try:
        return convert_mapped_cell(raw, mapping.get("transform"), dict(mapping), empty_cells_as_null=False,
                                   dest_nullability={}, target=target)
    finally:
        if date_token is not None:
            reset_active_date_locale(date_token)
        reset_active_number_locale(number_token)
