"""What the source looked like last time, so a recurring run can notice a change.

Schema drift is the largest single cause of pipeline incidents — 38% across a
published review of 50 postmortems — and the damaging half is not the pipeline
that crashes. It is the one that keeps succeeding: a column changes type, or
keeps its name and changes meaning, and the load completes while writing values
that are wrong. One postmortem describes a revenue figure silently doubling
because a currency field moved from ISO codes to a numeric enum and the
downstream logic multiplied by it.

The same review notes that most affected teams *had* a schema registry. It was
not enforced where the data actually moves. This module is that enforcement: a
schedule remembers the shape it read last time, and every later run compares
against it before moving anything.

The comparison and the policy already exist — :func:`classify_schema_change` and
:func:`resolve_schema_evolution` implement the Airbyte/Fivetran-class rules
(``propagate_columns``, ``pause_on_change``, ``type_locked``, ``manual_review``).
What was missing was the memory to feed them, so a scheduled run could not tell
a renamed column from one that had always been called that.

Only mapped columns can block. A new column nobody reads is additive by
definition, and failing a nightly load because the source team added an
unrelated field is the false alarm that teaches operators to disable the check —
which is how the permissive pipelines in those postmortems got that way.
"""

from __future__ import annotations

import logging
from typing import Any, NamedTuple

logger = logging.getLogger(__name__)

#: Verdicts. ``block`` refuses before any row moves; ``review`` lets the run
#: proceed while recording what changed; ``clear`` saw no relevant change.
BLOCK = "block"
REVIEW = "review"
CLEAR = "clear"


class SourceSchemaVerdict(NamedTuple):
    """What changed at the source since the last run, and what to do about it."""

    verdict: str
    summary: str
    breaking: list[dict[str, Any]]
    additive: list[dict[str, Any]]
    fingerprint: str

    @property
    def blocks(self) -> bool:
        return self.verdict == BLOCK


def fingerprint_source(
    columns: list[str], schema: dict[str, str] | None
) -> str:
    """Stable identity for a source shape, for cheap unchanged-since-last-run."""
    from services.schema_fingerprint import fingerprint_schema

    return fingerprint_schema(list(columns or []), dict(schema or {}))


def mapped_source_columns(mappings: list[dict[str, Any]] | None) -> set[str]:
    """Lower-cased source columns this transfer actually reads."""
    from services.mapping_constraints import is_intentional_omit

    out: set[str] = set()
    for mapping in mappings or []:
        if not isinstance(mapping, dict) or is_intentional_omit(mapping):
            continue
        source = str(mapping.get("source") or "").strip()
        if source:
            out.add(source.lower())
    return out


def evaluate_source_drift(
    *,
    previous_schema: dict[str, str] | None,
    current_schema: dict[str, str] | None,
    current_columns: list[str] | None = None,
    mappings: list[dict[str, Any]] | None = None,
    schema_policy: str = "manual_review",
    dest_db: str = "",
) -> SourceSchemaVerdict:
    """Compare this run's source shape against the remembered one.

    The first run has nothing to compare against and is always clear — it is
    establishing the baseline, not validating against one.
    """
    columns = list(current_columns or list((current_schema or {}).keys()))
    fingerprint = fingerprint_source(columns, current_schema)
    if not previous_schema:
        return SourceSchemaVerdict(CLEAR, "First run — baseline recorded.", [], [], fingerprint)

    from services.schema_drift import classify_schema_change

    change = classify_schema_change(
        dict(previous_schema), dict(current_schema or {}), dest_db=dest_db
    )
    read = mapped_source_columns(mappings)

    def _touches_mapped(entry: dict[str, Any]) -> bool:
        if not read:
            # No explicit mapping means every source column is carried.
            return True
        for key in ("column", "from", "to", "name"):
            value = str(entry.get(key) or "").strip().lower()
            if value and value in read:
                return True
        return False

    breaking = [e for e in (change.get("breaking") or []) if _touches_mapped(e)]
    additive = list(change.get("additive") or [])

    policy = (schema_policy or "manual_review").strip().lower()
    if breaking:
        return SourceSchemaVerdict(
            BLOCK,
            _describe(breaking),
            breaking,
            additive,
            fingerprint,
        )
    if additive and policy == "pause_on_change":
        return SourceSchemaVerdict(
            BLOCK,
            f"{len(additive)} new source column(s) and schema_policy=pause_on_change.",
            [],
            additive,
            fingerprint,
        )
    if additive:
        return SourceSchemaVerdict(
            REVIEW,
            f"{len(additive)} new source column(s), none of them mapped — carried forward unchanged.",
            [],
            additive,
            fingerprint,
        )
    return SourceSchemaVerdict(CLEAR, "Source shape unchanged since the last run.", [], [], fingerprint)


def _describe(breaking: list[dict[str, Any]]) -> str:
    """Name what changed, in the terms the operator has to act on.

    A verdict that only says "schema drift detected" makes the operator go and
    diff two catalogs by hand, which is the work this is supposed to remove.
    """
    parts: list[str] = []
    for entry in breaking[:5]:
        kind = str(entry.get("kind") or entry.get("type") or "changed")
        column = str(entry.get("column") or entry.get("from") or entry.get("name") or "?")
        before = entry.get("old_type") or entry.get("from_type")
        after = entry.get("new_type") or entry.get("to_type")
        target = entry.get("to") or entry.get("new_name")
        if kind == "rename" and target:
            # Naming both sides is the point: the operator has to decide whether
            # this is the same field under a new name or a different field.
            parts.append(f"{column} renamed to {target}")
        elif before and after:
            parts.append(f"{column}: {before} → {after} ({kind})")
        else:
            parts.append(f"{column} ({kind})")
    more = f" (+{len(breaking) - 5} more)" if len(breaking) > 5 else ""
    return (
        "Source schema changed since the last run in a way this transfer reads: "
        + "; ".join(parts)
        + more
    )
