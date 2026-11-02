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

The comparison and the policy live in :mod:`services.schema_drift`
(:func:`classify_from_column_maps` + :func:`resolve_schema_evolution`). This
module supplies the memory those functions need, filters to columns this
transfer actually reads, and tightens unattended runs: a mapped drop/rename is
dest-safe (never DROP COLUMN) but must not run overnight without propagate.

Only mapped columns can block on additive/type diffs. A new column nobody reads
is additive by definition, and failing a nightly load because the source team
added an unrelated field is the false alarm that teaches operators to disable
the check — which is how the permissive pipelines in those postmortems got that
way. Primary-key and cursor identity always count: they are the stream, not an
optional payload field.
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
    compatibility: str = "identical"
    action: str = "continue"

    @property
    def blocks(self) -> bool:
        return self.verdict == BLOCK


def fingerprint_source(
    columns: list[str], schema: dict[str, str] | None
) -> str:
    """Stable identity for a source shape, for cheap unchanged-since-last-run."""
    from services.schema_fingerprint import fingerprint_schema

    types = _flat_types(schema)
    cols = list(columns or list(types.keys()))
    return fingerprint_schema(cols, types)


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


def _flat_types(schema: dict[str, Any] | None) -> dict[str, str]:
    """Accept flat col→type maps or nested {columns: {...}} from classify."""
    schema = schema or {}
    nested = schema.get("columns")
    if isinstance(nested, dict):
        return {str(k): str(v) for k, v in nested.items()}
    return {
        str(k): str(v)
        for k, v in schema.items()
        if not str(k).startswith("_") and not isinstance(v, (dict, list))
    }


def _nested_pk(schema: dict[str, Any] | None) -> list[str]:
    schema = schema or {}
    raw = schema.get("primary_key") or schema.get("primary_keys") or []
    if isinstance(raw, str):
        return [raw] if raw.strip() else []
    return [str(p) for p in raw if str(p).strip()]


def _entry_touches_mapped(entry: dict[str, Any], read: set[str]) -> bool:
    """Whether this classified change affects the columns this transfer reads.

    Primary-key and cursor identity always count: they have no ``column`` key
    on PK diffs, and the CDC cursor is often not in the payload mapping.
    """
    kind = str(entry.get("kind") or "")
    if kind in {"primary_key_change", "cursor_removed", "primary_key_removed"}:
        return True
    if not read:
        return True
    names: list[str] = []
    for key in ("column", "from", "to", "name"):
        value = str(entry.get(key) or "").strip()
        if value:
            names.append(value)
    for key in ("old_primary_key", "new_primary_key"):
        raw = entry.get(key) or []
        if isinstance(raw, str):
            names.append(raw)
        else:
            names.extend(str(p) for p in raw)
    return any(name.lower() in read for name in names)


def evaluate_source_drift(
    *,
    previous_schema: dict[str, str] | None,
    current_schema: dict[str, str] | None,
    current_columns: list[str] | None = None,
    mappings: list[dict[str, Any]] | None = None,
    schema_policy: str = "manual_review",
    dest_db: str = "",
    source_db: str = "",
    previous_primary_key: list[str] | None = None,
    current_primary_key: list[str] | None = None,
    cursor_fields: list[str] | None = None,
    unattended: bool = True,
) -> SourceSchemaVerdict:
    """Compare this run's source shape against the remembered one.

    The first run has nothing to compare against and is always clear — it is
    establishing the baseline, not validating against one.

    ``unattended`` (schedules, default True) uses the same evolution kernel as
    Validate, then refuses mapped drop/rename unless the operator opted into
    ``propagate_*``. Interactive Validate still shows those as review.

    Dialect defaults belong to the *source* engine. Passing the destination
    (Snowflake ``TIMESTAMP_NTZ`` compared through MySQL FSP 0) invents
    ``TIMESTAMP_NTZ → TIMESTAMP_NTZ (narrow_type)`` on a column nobody changed.
    ``source_db`` wins; ``dest_db`` remains as a legacy alias for the same slot.
    """
    from services.schema_drift import (
        PROPAGATE_POLICIES,
        SOFT_NET_ADDITIVE_KINDS,
        classify_from_column_maps,
        resolve_schema_evolution,
    )

    prev_types = _flat_types(previous_schema)
    curr_types = _flat_types(current_schema)
    columns = list(current_columns or list(curr_types.keys()))
    fingerprint = fingerprint_source(columns, curr_types)
    if not prev_types:
        return SourceSchemaVerdict(
            CLEAR, "First run — baseline recorded.", [], [], fingerprint
        )

    prev_pk = list(previous_primary_key or _nested_pk(previous_schema))
    live_pk = list(current_primary_key or _nested_pk(current_schema))
    dialect = (source_db or dest_db or "").strip()
    change = classify_from_column_maps(
        list(prev_types.keys()),
        prev_types,
        columns,
        curr_types,
        old_pk=prev_pk or None,
        new_pk=live_pk or None,
        cursor_fields=cursor_fields,
        dest_db=dialect,
    )
    read = mapped_source_columns(mappings)

    breaking = [e for e in (change.get("breaking") or []) if _entry_touches_mapped(e, read)]
    additive = list(change.get("additive") or [])
    filtered = {
        "additive": additive,
        "breaking": breaking,
        "severity": (
            "breaking" if breaking else "additive" if additive else "none"
        ),
        "renamed": list(change.get("renamed") or []),
    }
    policy = (schema_policy or "manual_review").strip().lower()
    evolution = resolve_schema_evolution(
        filtered,
        schema_policy=policy,
        source_changed=bool(additive or breaking),
    )
    compat = str(evolution.get("compatibility") or "identical")
    action = str(evolution.get("action") or "continue")
    hard = list(evolution.get("hard_breaking") or [])
    soft = list(evolution.get("soft_net_additive") or [])

    if evolution.get("should_pause") or hard:
        return SourceSchemaVerdict(
            BLOCK,
            _describe(hard or breaking),
            breaking,
            additive,
            fingerprint,
            compat,
            action,
        )
    # Unattended mapped drop/rename: dest-safe, but not silent overnight unless
    # the operator opted into Fivetran-class propagate.
    if (
        unattended
        and soft
        and policy not in PROPAGATE_POLICIES
        and any(str(item.get("kind")) in SOFT_NET_ADDITIVE_KINDS for item in soft)
    ):
        return SourceSchemaVerdict(
            BLOCK,
            _describe(soft),
            breaking,
            additive,
            fingerprint,
            compat,
            action,
        )
    if action in {"review", "propagate"} or evolution.get("should_propagate"):
        summary = (
            f"{len(additive)} new source column(s), none of them mapped — "
            "carried forward unchanged."
            if additive and not breaking
            else str(evolution.get("compatibility_note") or "Schema changed.")
        )
        return SourceSchemaVerdict(
            REVIEW, summary, breaking, additive, fingerprint, compat, action
        )
    return SourceSchemaVerdict(
        CLEAR,
        "Source shape unchanged since the last run.",
        [],
        [],
        fingerprint,
        compat,
        action,
    )


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
        if kind == "primary_key_change":
            old_pk = entry.get("old_primary_key") or []
            new_pk = entry.get("new_primary_key") or []
            parts.append(f"primary key {list(old_pk)} → {list(new_pk)}")
        elif kind == "rename" and target:
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
