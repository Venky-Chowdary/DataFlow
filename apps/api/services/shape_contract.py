"""Dest-exists shape contract — one SSOT for Map / Validate / Execute / UI.

Competitors still leave dest-exists split across policy flags:

* Airbyte — all-or-nothing schema approve; some dests ``DROP COLUMN`` on
  removal (airbyte#74892, #78427; PR #82720 dest-dependent DROP).
* Fivetran — rename = new column + stale old (NULLs going forward).
* AWS DMS — dest-exists ``Do nothing`` does not refresh extra source columns
  until the task is restarted.
* dbt incremental — positional ``INSERT`` misalignment when a new column is
  not last (dbt-databricks#1289).
* Informatica CDI — rename treated as delete+add; Snowflake advanced mode
  jumbles columns unless ``column_mapping=name``.

DataFlow unique algorithm: classify the dest-exists *shape* once, project
writes by destination column *name* (never source position), and keep
dest-only columns off SET. G13 / G14 remain the blockers; this module is
the contract they describe. ``100%`` means a named fixture, not marketing.
"""

from __future__ import annotations

import re
from typing import Any

from services.destination_requirements_gate import unfilled_required_columns
from services.mapping_constraints import (
    classify_source_coverage,
    is_intentional_omit,
    write_mappings,
)

GATE_ID = "g15_dest_exists_shape"

SHAPE_EQUAL = "equal"
SHAPE_SOURCE_SUPERSET = "source_superset"
SHAPE_DEST_SUPERSET = "dest_superset"
SHAPE_OVERLAP = "overlap"
SHAPE_PENDING = "pending_schema"
SHAPE_CREATE_NEW = "create_new_table"
SHAPE_UNKNOWN = "dest_unknown"

COL_BIND = "bind"
COL_ADD = "add_proposed"
COL_PENDING = "pending"
COL_OMIT = "omit"
COL_UNACCOUNTED = "unaccounted"
COL_DEST_PRESERVE = "dest_only_preserve"
COL_DEST_REQUIRED = "dest_only_required"
COL_FALSE_FRIEND = "false_friend"

FALSE_FRIEND_KINDS = frozenset(
    {
        "measure_kind",
        "entity_identity",
        "dest_collision",
        "identity_leaf",
        "temporal_polarity",
    }
)

WRITE_BY_NAME = "name"

_INSERT_WITH_COLS = re.compile(
    r"insert\s+into\s+\S+\s*\(([^)]+)\)\s*values",
    re.IGNORECASE | re.DOTALL,
)


def _norm(name: str) -> str:
    return re.sub(r"[\s-]+", "_", str(name or "").strip().lower())


def _is_create_new_mapping(m: dict[str, Any]) -> bool:
    if m.get("create_new"):
        return True
    return str(m.get("assignment_strategy") or "") in {
        "create_compatible_new",
        "identity_passthrough",
    }


def _is_pending_mapping(m: dict[str, Any]) -> bool:
    return str(m.get("assignment_strategy") or "") == "pending_dest_schema"


def write_ready_mappings(mappings: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Mappings that may appear in INSERT/MERGE — omit and pending stay off the write.

    Dest-only columns are never invented here. Execute must use this list, not
    the raw Map array, so a pending extra source column cannot become a
    positional INSERT value (dbt-databricks#1289).
    """
    return [m for m in write_mappings(mappings) if not _is_pending_mapping(m)]


def insert_sql_is_name_addressed(sql: str) -> bool:
    """True when INSERT names dest columns — the dbt positional hole is closed.

    ``INSERT INTO t VALUES (...)`` (no column list) is fail-closed: a dest
    ADD COLUMN that is not last would shift values (dbt-databricks#1289).
    """
    text = str(sql or "").strip()
    if not text:
        return False
    return bool(_INSERT_WITH_COLS.search(text))


def project_named_write(
    *,
    source_row: dict[str, Any],
    mappings: list[dict[str, Any]] | None,
    dest_columns: list[str] | None,
) -> dict[str, Any]:
    """Project one source row onto dest columns by name.

    Source key order and dest physical order must not change values.
    Dest-only columns are omitted (not NULL-invented). Unaccounted, omitted,
    and pending-schema sources are withheld.
    """
    dest_l = {_norm(c): c for c in (dest_columns or []) if str(c).strip()}
    out: dict[str, Any] = {}
    for m in write_mappings(mappings):
        if _is_pending_mapping(m):
            continue
        src = str(m.get("source") or "").strip()
        tgt = str(m.get("target") or "").strip()
        if not src or not tgt or src not in source_row:
            continue
        if _is_create_new_mapping(m) or _norm(tgt) in dest_l:
            canon = dest_l.get(_norm(tgt), tgt)
            out[canon] = source_row[src]
    return out


def _mapping_column_kind(m: dict[str, Any], dest_l: set[str]) -> str:
    if is_intentional_omit(m):
        return COL_OMIT
    review = str(m.get("review_kind") or "").strip()
    if review in FALSE_FRIEND_KINDS:
        return COL_FALSE_FRIEND
    if _is_pending_mapping(m):
        return COL_PENDING
    if _is_create_new_mapping(m):
        return COL_ADD
    tgt = str(m.get("target") or "").strip()
    if tgt and _norm(tgt) in dest_l:
        return COL_BIND
    if tgt:
        return COL_ADD
    return COL_UNACCOUNTED


def classify_dest_exists_shape(
    *,
    destination_table_exists: bool | None,
    source_columns: list[str] | None,
    dest_columns: list[str] | None,
    mappings: list[dict[str, Any]] | None,
    column_nullability: dict[str, bool] | None = None,
    column_defaults: dict[str, str] | None = None,
    identity_columns: list[str] | None = None,
    generated_columns: list[str] | None = None,
) -> dict[str, Any]:
    """Single dest-exists shape + per-column verdicts.

    Map / Validate / Execute / UI must consume this object — not a parallel
    heuristic. G13/G14 stay the blockers; this is the contract they name.
    """
    sources = [str(c) for c in (source_columns or []) if str(c).strip()]
    dests = [str(c) for c in (dest_columns or []) if str(c).strip()]
    maps = list(mappings or [])
    dest_l = {_norm(c) for c in dests}
    coverage = classify_source_coverage(sources, maps)

    if destination_table_exists is False:
        shape = SHAPE_CREATE_NEW
    elif destination_table_exists is None:
        shape = SHAPE_UNKNOWN
    elif any(_is_pending_mapping(m) for m in maps) and not dests:
        shape = SHAPE_PENDING
    else:
        mapped_targets = {
            _norm(str(m.get("target") or ""))
            for m in write_mappings(maps)
            if str(m.get("target") or "").strip()
            and not _is_create_new_mapping(m)
            and not _is_pending_mapping(m)
        }
        source_extra = bool(coverage["unaccounted"]) or any(
            _is_create_new_mapping(m) or _is_pending_mapping(m) for m in maps
        )
        dest_fold: dict[str, list[str]] = {}
        for dest in dests:
            dest_fold.setdefault(_norm(dest), []).append(dest)
        dest_fold_collision = any(len(names) > 1 for names in dest_fold.values())
        dest_extra = bool(dest_l - mapped_targets) or dest_fold_collision if dests else False
        if source_extra and dest_extra:
            shape = SHAPE_OVERLAP
        elif source_extra:
            shape = SHAPE_SOURCE_SUPERSET
        elif dest_extra:
            shape = SHAPE_DEST_SUPERSET
        else:
            shape = SHAPE_EQUAL

    columns: list[dict[str, Any]] = []
    seen_src = set()
    for m in maps:
        src = str(m.get("source") or "").strip()
        if not src:
            continue
        seen_src.add(_norm(src))
        columns.append(
            {
                "source": src,
                "target": str(m.get("target") or ""),
                "kind": _mapping_column_kind(m, dest_l),
                "review_kind": m.get("review_kind"),
            }
        )
    for src in coverage["unaccounted"]:
        if _norm(src) in seen_src:
            continue
        columns.append(
            {
                "source": src,
                "target": "",
                "kind": COL_UNACCOUNTED,
                "review_kind": None,
            }
        )

    unfilled = (
        unfilled_required_columns(
            column_nullability=column_nullability,
            column_defaults=column_defaults,
            identity_columns=identity_columns,
            generated_columns=generated_columns,
            mappings=maps,
        )
        if destination_table_exists is True
        else []
    )
    filled_spellings = {
        str(m.get("target") or "")
        for m in write_mappings(maps)
        if str(m.get("target") or "").strip()
    }
    dest_fold_names: dict[str, list[str]] = {}
    for dest in dests:
        dest_fold_names.setdefault(_norm(dest), []).append(dest)
    engine_filled = {_norm(c) for c in (identity_columns or [])} | {
        _norm(c) for c in (generated_columns or [])
    }
    defaulted = {_norm(c) for c in (column_defaults or {})}
    dest_only: list[dict[str, Any]] = []
    for dest in dests:
        group = dest_fold_names.get(_norm(dest), [dest])
        if len(group) > 1:
            if dest not in filled_spellings:
                dest_only.append({"target": dest, "kind": COL_DEST_PRESERVE})
            continue
        if any(s.lower() == dest.lower() for s in filled_spellings):
            continue
        kind = COL_DEST_REQUIRED if dest in unfilled else COL_DEST_PRESERVE
        dest_only.append({"target": dest, "kind": kind})

    write_cols = [
        str(m.get("target") or "")
        for m in write_mappings(maps)
        if str(m.get("target") or "").strip()
        and not _is_pending_mapping(m)
        and (
            _is_create_new_mapping(m)
            or _norm(str(m.get("target") or "")) in dest_l
            or destination_table_exists is False
        )
    ]
    # Dest-canonical order when we know dest columns — never source order.
    if dests:
        dest_order = {_norm(c): i for i, c in enumerate(dests)}
        write_cols.sort(key=lambda c: dest_order.get(_norm(c), len(dests)))

    counts = {
        "bind": sum(1 for c in columns if c["kind"] == COL_BIND),
        "add_proposed": sum(1 for c in columns if c["kind"] == COL_ADD),
        "pending": sum(1 for c in columns if c["kind"] == COL_PENDING),
        "omit": sum(1 for c in columns if c["kind"] == COL_OMIT),
        "unaccounted": sum(1 for c in columns if c["kind"] == COL_UNACCOUNTED),
        "false_friend": sum(1 for c in columns if c["kind"] == COL_FALSE_FRIEND),
        "dest_only_preserve": sum(1 for c in dest_only if c["kind"] == COL_DEST_PRESERVE),
        "dest_only_required": len(unfilled),
    }
    headline, detail, primary = _shape_copy(shape, counts, unfilled)
    return {
        "shape": shape,
        "destination_table_exists": destination_table_exists,
        "write_by": WRITE_BY_NAME,
        "write_columns": write_cols,
        "columns": columns,
        "dest_only": dest_only,
        "unfilled_required": unfilled,
        "unaccounted_sources": list(coverage["unaccounted"]),
        "counts": counts,
        "headline": headline,
        "detail": detail,
        "primary_action": primary,
        "engine_filled": sorted(engine_filled),
        "defaulted": sorted(defaulted),
        "honesty": (
            "Named dest-exists fixture. Write is name-addressed. "
            "CDC remains at-least-once upsert. Not a catalog-breadth claim."
        ),
    }


def _shape_copy(
    shape: str,
    counts: dict[str, int],
    unfilled: list[str],
) -> tuple[str, str, str]:
    if shape == SHAPE_CREATE_NEW:
        return (
            "New destination table — types CREATE on first write",
            "No dest-exists contract yet. Identity maps still need operator Approve.",
            "review_map",
        )
    if shape == SHAPE_UNKNOWN:
        return (
            "Destination existence unproven — not treating as create-new",
            "Reload destination schema before ADD COLUMN or identity passthrough.",
            "reload_dest_schema",
        )
    if shape == SHAPE_PENDING:
        return (
            "Destination table exists but column types did not load",
            "Retry introspect. Names-only dest must not invent ADD COLUMN.",
            "reload_dest_schema",
        )
    if counts["unaccounted"]:
        return (
            f"{counts['unaccounted']} extra source column(s) are unaccounted",
            "Map them, declare Omit, or propose ADD COLUMN — DataFlow will not drop them silently.",
            "review_map",
        )
    if unfilled:
        return (
            f"{len(unfilled)} required dest column(s) have no filler",
            "Map a source, or the write fails row 1 (NOT NULL, no default).",
            "review_map",
        )
    if counts["false_friend"]:
        return (
            f"{counts['false_friend']} false-friend pair(s) need confirm",
            "Approve eligible will not clear qty≠amt / user≠customer / dest collision.",
            "confirm_or_remap",
        )
    if counts["add_proposed"]:
        return (
            f"{counts['add_proposed']} ADD COLUMN proposal(s) on an existing table",
            "Execute ADDs only after create-new / backfill / propagate — dest history is kept.",
            "confirm_add",
        )
    if shape == SHAPE_DEST_SUPERSET:
        return (
            f"{counts['dest_only_preserve']} dest-only column(s) stay off SET",
            "Insert/upsert will not NULL-wipe dest-only columns. Full overwrite is a different contract.",
            "continue_validate",
        )
    if shape == SHAPE_SOURCE_SUPERSET:
        return (
            "Source has extra columns with an explicit decision",
            "Extra source columns are mapped, omitted, or proposed ADD — not silently dropped.",
            "continue_validate",
        )
    return (
        "Dest-exists shape is bound — insert more into the existing table",
        "Writes are name-addressed. Dest-only columns stay off SET. At-least-once upsert.",
        "continue_validate",
    )


def build_shape_gate(contract: dict[str, Any]) -> dict[str, Any]:
    """Operator-visible G15. Does not duplicate G13/G14 blockers."""
    shape = str(contract.get("shape") or "")
    counts = contract.get("counts") or {}
    if shape == SHAPE_CREATE_NEW:
        status = "skip"
    elif shape in {SHAPE_UNKNOWN, SHAPE_PENDING}:
        status = "warn"
    elif counts.get("unaccounted") or counts.get("dest_only_required"):
        status = "warn"
    elif counts.get("false_friend") or counts.get("add_proposed"):
        status = "warn"
    else:
        status = "pass"
    return {
        "id": GATE_ID,
        "status": status,
        "message": contract.get("headline") or "Dest-exists shape classified",
        "duration_ms": 0,
        "details": {
            "shape": shape,
            "write_by": WRITE_BY_NAME,
            "write_columns": contract.get("write_columns") or [],
            "counts": counts,
            "primary_action": contract.get("primary_action"),
            "rule_id": f"{GATE_ID}.{shape}",
            "remediation_kind": contract.get("primary_action") or "review_mappings",
        },
    }
