"""D344 — a column added to a CDC source after the capture started.

A CDC run fixes its column list (headers, mappings, types) when it opens the
log. A later ``ALTER TABLE … ADD COLUMN`` on the source reaches the apply as a
new key on every row image, and the matrix built from the fixed headers
dropped it: the destination never gained the column, its values were applied
nowhere, and the job kept saying "CDC applied N change(s)".

Each row image is now checked against the columns the capture started with.
A new column is either

* propagated — when the run's policy arms the writer's ADD COLUMN lane
  (``backfill_new_fields``: ``schema_policy=propagate_columns`` / a backfill
  request, the same switch every batch mode evolves an existing destination
  with). The column joins the capture as the mapping a propagate policy adds
  (:func:`services.schema_drift.propagated_mapping_entry`) with the source
  catalog's type, and the writer adds it, nullable, before the event lands; or
* refused — the apply stops before the first event that carries it, and the
  error names the column. Nothing of that event is written and the log
  position is not advanced, so the events are read again once the policy or
  the mapping allows the column.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Iterable

logger = logging.getLogger(__name__)

#: Keys a CDC reader adds to a row image that are not source columns.
_ENVELOPE_KEYS = frozenset({"__op", "__deleted"})


class CdcAddedColumnRefused(ValueError):
    """A source column appeared mid-capture and the policy does not propagate it."""

    def __init__(self, message: str, columns: list[str]):
        super().__init__(message)
        self.columns = list(columns)


def added_source_columns(imaged: Iterable[str], known: Iterable[str]) -> list[str]:
    """Image columns the capture did not start with, in first-seen order."""
    seen = {str(c).casefold() for c in known if c}
    out: list[str] = []
    for name in imaged:
        text = str(name or "").strip()
        folded = text.casefold()
        if (
            not text
            or folded in seen
            or folded.startswith("_df_")
            or folded in _ENVELOPE_KEYS
        ):
            continue
        seen.add(folded)
        out.append(text)
    return out


def refusal_message(columns: list[str], *, table: str, dest_table: str) -> str:
    names = ", ".join(columns)
    return (
        f"CDC stopped: source column(s) {names} were added to {table} after this "
        f"capture started, and the schedule's schema policy does not add new "
        f"columns, so {dest_table} has no column for them. No change carrying "
        f"{names} was applied and the log position was not advanced: set the "
        "schedule's schema_policy to propagate_columns (DataFlow then adds the "
        f"column, nullable, to {dest_table}), or add {names} to the destination "
        "and the mapping yourself, then run the schedule again — the held "
        "changes are re-read from the log."
    )


def propagation_note(columns: list[str], *, table: str, dest_table: str) -> str:
    names = ", ".join(columns)
    return (
        f"Source column(s) {names} were added to {table} during this CDC capture; "
        f"DataFlow added them to {dest_table} (nullable, schema policy propagates "
        "new columns) and applies their values from the first change that "
        "carries them. Rows with no change since keep NULL there until they "
        "change or are repaired."
    )


def adopt_added_columns(
    added: list[str],
    *,
    mappings: list[dict[str, Any]],
    headers: list[str],
    column_types: dict[str, str],
    propagate: bool,
    table: str,
    dest_table: str,
    dest_db: str = "",
    source_types: Callable[[list[str]], dict[str, str]] | None = None,
    records: Iterable[Any] = (),
) -> tuple[list[dict[str, Any]], list[str], dict[str, str]]:
    """Extend the capture with ``added`` columns, or refuse them.

    Raises :class:`CdcAddedColumnRefused` when ``propagate`` is off. The
    destination type is stamped by the Decision Kernel exactly as a batch
    run's additive column is (``stamp_additive_mapping_types``): from the
    source catalog type, or from the values the change carries when the
    source has no catalog (a document store).
    """
    if not added:
        return mappings, headers, column_types
    if not propagate:
        raise CdcAddedColumnRefused(
            refusal_message(added, table=table, dest_table=dest_table), added
        )
    from services.schema_drift import propagated_mapping_entry

    types: dict[str, str] = {}
    if source_types is not None:
        try:
            types = {str(k): str(v) for k, v in (source_types(added) or {}).items() if v}
        except Exception as exc:  # noqa: BLE001 - the image still carries the values
            logger.warning("CDC added-column type lookup failed for %s: %s", table, exc)
    by_fold = {k.casefold(): v for k, v in types.items()}
    column_types = dict(column_types or {})
    headers = list(headers or [])
    entries: list[dict[str, Any]] = []
    samples: dict[str, list[Any]] = {name: [] for name in added}
    for record in records:
        if not isinstance(record, dict):
            continue
        for name in added:
            value = record.get(name)
            if value is not None and len(samples[name]) < 32:
                samples[name].append(value)
    for name in added:
        source_type = by_fold.get(name.casefold()) or ""
        entries.append(
            propagated_mapping_entry(
                name,
                reason="add_column",
                source_schema={name: source_type} if source_type else None,
            )
        )
        headers.append(name)
        column_types[name] = source_type or "string"
    if dest_db:
        from services.decision_kernel.invent import stamp_additive_mapping_types

        entries, _unstamped = stamp_additive_mapping_types(
            entries,
            dest_db=dest_db,
            source_types={n: by_fold[n.casefold()] for n in added if n.casefold() in by_fold},
            samples_by_source=samples,
            backfill_new_fields=True,
            dest_table_exists=True,
        )
    return [*(mappings or []), *entries], headers, column_types
