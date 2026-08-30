"""Schema drift detection + auto-propagate — one evolution kernel.

Algorithms (primary sources: Confluent Schema Registry compatibility lattice,
Apache Iceberg schema evolution, Fivetran net-additive ``schema_change_handling``,
Airbyte propagate vs pause):

* Additive (nullable add, widen) → auto-apply under ``propagate_columns`` /
  ``propagate_all`` (Validate≡Execute share ``apply_propagate_mappings``).
* Net-additive drops/renames under propagate → keep dest history; map the new
  name; never silent DROP COLUMN (Fivetran / Iceberg metadata-only drop).
* Hard breaking (PK change, type narrow, NOT NULL add) → **always pause**,
  even when propagate is on (Airbyte rule + Confluent ``NONE``).
* ``pause_on_change`` → pause on any detected change.
* ``manual_review`` → continue with existing mappings only (ignore new cols
  until approved); still pause on hard breaking.
* ``type_locked`` → pause on any type change; additive columns still require
  propagate or explicit backfill.

Compatibility is policy-independent. Policy only chooses the action. Validate,
Execute, schedules, and signed contracts must call :func:`resolve_schema_evolution`
— never a parallel pause/continue table.
"""

from __future__ import annotations

import re
from typing import Any

from services.db_type_utils import SCHEMALESS_DESTS, ci_get, normalize_dest_kind
from services.schema_fingerprint import fingerprint_schema, schemas_match
from services.decision_kernel import (
    decimal_capacity_is_equal_or_wider,
    is_lossy_coercion,
    normalize_logical_type,
)

# Policies that auto-apply additive field evolution (Airbyte propagate_*).
PROPAGATE_POLICIES = frozenset({"propagate_columns", "propagate_all"})

#: Operator caption — same meaning as Studio ``schemaPolicyHonestyLine``.
SCHEMA_POLICY_HONESTY: dict[str, str] = {
    "manual_review": (
        "Validate blocks on new or renamed columns until you Acknowledge or remap. "
        "Execute does not ADD COLUMN. Hard type-narrow always pauses."
    ),
    "propagate_columns": (
        "Validate auto-maps additive columns; Execute issues ADD COLUMN. "
        "Type narrow, PK, and dest-only NOT NULL still pause — not a silent rewrite."
    ),
    "propagate_all": (
        "Same ADD COLUMN kernel as propagate columns — every selected stream on this job. "
        "Does not rewrite destination types. Hard-breaking changes still pause."
    ),
    "pause_on_change": (
        "Any detected change — including additive — pauses Validate and scheduled beats. "
        "Nothing is written until you change policy or remap."
    ),
    "type_locked": (
        "Widen and destination type changes pause. New columns need review. "
        "Execute does not silent-cast."
    ),
}


def schema_policy_honesty_line(policy: str) -> str:
    """Studio / G10 caption for the Advanced schema-change policy."""
    key = (policy or "manual_review").strip().lower()
    return SCHEMA_POLICY_HONESTY.get(key, SCHEMA_POLICY_HONESTY["manual_review"])

# Always pause — Airbyte breaking + Datawrap type-fidelity (no silent narrow).
HARD_BREAKING_KINDS = frozenset({
    "primary_key_change",
    "cursor_removed",
    "primary_key_removed",
    "narrow_type",
    "type_change",
    "add_not_null",
    "nullability_tighten",
})

# Soft under propagate (Fivetran net-additive): dest keeps old column / new name.
SOFT_NET_ADDITIVE_KINDS = frozenset({"drop", "rename"})

# Confluent-class lattice specialized for SQL transfer. Dest is the consumer of
# new source rows; dest history is never DROP COLUMN (Iceberg / Fivetran).
COMPAT_IDENTICAL = "identical"
COMPAT_FORWARD = "forward"      # source grew (add / widen)
COMPAT_BACKWARD = "backward"    # source dropped / renamed; dest keeps history
COMPAT_FULL = "full"            # optional add AND remove, no type/PK change
COMPAT_NONE = "none"            # hard-breaking — not auto-applicable

COMPATIBILITY_NOTES: dict[str, str] = {
    COMPAT_IDENTICAL: "No material schema change.",
    COMPAT_FORWARD: (
        "Source grew (nullable add or type widen). Destination can accept new "
        "rows after ADD/WIDEN, or by leaving new columns unmapped — never DROP "
        "destination columns."
    ),
    COMPAT_BACKWARD: (
        "Source dropped or renamed fields. Destination keeps history "
        "(net-additive). Mapped drops pause unattended runs unless propagate "
        "is on."
    ),
    COMPAT_FULL: (
        "Optional add and remove without type or primary-key change "
        "(Avro FULL-class)."
    ),
    COMPAT_NONE: (
        "Hard-breaking (narrow, type change, primary key, NOT NULL, cursor). "
        "Always pause — never auto-apply, never acknowledge-away."
    ),
}


def _norm_type(value: str | None) -> str:
    return (value or "VARCHAR").strip().upper()


def _type_length(type_name: str) -> int | None:
    match = re.search(r"\((\d+)", type_name or "")
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _unpack_schema(schema: dict[str, Any] | None) -> tuple[dict[str, str], dict[str, bool], list[str]]:
    """Accept flat col→type maps or nested {columns, nullable, primary_key}."""
    schema = schema or {}
    if "columns" in schema and isinstance(schema.get("columns"), dict):
        columns = {str(k): str(v) for k, v in schema["columns"].items()}
        nullable_raw = schema.get("nullable") or {}
        nullable = {str(k): bool(v) for k, v in nullable_raw.items()} if isinstance(nullable_raw, dict) else {}
        pk_raw = schema.get("primary_key") or schema.get("primary_keys") or []
        if isinstance(pk_raw, str):
            primary_key = [pk_raw] if pk_raw else []
        else:
            primary_key = [str(p) for p in pk_raw]
        return columns, nullable, primary_key

    columns = {str(k): str(v) for k, v in schema.items() if not str(k).startswith("_")}
    return columns, {}, []


def _is_type_widen(old_type: str, new_type: str, *, dest_db: str = "") -> bool:
    """True when new_type can hold all values of old_type without loss."""
    old_logical = normalize_logical_type(old_type)
    new_logical = normalize_logical_type(new_type)
    if old_logical == new_logical:
        old_len = _type_length(old_type)
        new_len = _type_length(new_type)
        return (
            old_len is not None
            and new_len is not None
            and new_len > old_len
        )
    # Differing logical types: safe (non-lossy) promotions count as widens.
    return not is_lossy_coercion(old_type, new_type, dest_db=dest_db)


def _is_type_narrow(old_type: str, new_type: str, *, dest_db: str = "") -> bool:
    """True when new_type can lose values of old_type (precision/range/domain).

    Same-logical pairs must still consult ``is_lossy_coercion`` — first ``(p``
    digit alone misses DECIMAL(10,4)→DECIMAL(10,2) and BIGINT→TINYINT.

    A declaration is not a narrow of itself. Coercion helpers read two identical
    unparameterized carriers (``TIMESTAMP_NTZ``) through ``dest_db`` defaults and
    invent dest-floor vs source-ceiling loss — that is a fidelity-gate concern
    against the real destination type, not source-vs-source drift.
    """
    if _norm_type(old_type) == _norm_type(new_type):
        return False
    old_logical = normalize_logical_type(old_type)
    new_logical = normalize_logical_type(new_type)
    if decimal_capacity_is_equal_or_wider(old_type, new_type, dest_db=dest_db):
        # A wider fixed-point carrier cannot lose a digit. It can still *invent*
        # a shape the source never declared (BigQuery bare BIGNUMERIC is 76,38),
        # which stays a fidelity/contract chip — but calling it drift-narrow
        # paused every second run into a table this product had just created.
        return False
    if is_lossy_coercion(old_type, new_type, dest_db=dest_db):
        return True
    if old_logical == new_logical:
        old_len = _type_length(old_type)
        new_len = _type_length(new_type)
        return (
            old_len is not None
            and new_len is not None
            and new_len < old_len
        )
    return False


def _semantic_rename_pairs(
    dropped: list[str],
    added: list[str],
    old_cols: dict[str, str],
    new_cols: dict[str, str],
    *,
    dest_db: str = "",
) -> tuple[list[str], list[str], list[tuple[str, str]]]:
    """Pair dropped↔added columns that are the same field under a new name.

    Type compatibility is necessary but not sufficient. ``AMT`` → ``quantity``
    is a drop+add, not a rename — Fivetran treats it as a new column and
    leaves the old one stale. We require the shared mapper to pin the pair
    without a measure/identity/entity false-friend.
    """
    from services.semantic_mapper import (
        _entity_conflict_requires_review,
        _identity_leaf_mismatch,
        _measure_kind_mismatch,
        map_columns,
    )

    remaining_dropped = list(dropped)
    remaining_added = list(added)
    if not remaining_dropped or not remaining_added:
        return remaining_dropped, remaining_added, []

    mapped = map_columns(remaining_dropped, remaining_added)
    by_source = {str(m.get("source")): m for m in mapped}
    used_added: set[str] = set()
    pairs: list[tuple[str, str]] = []
    for d in list(remaining_dropped):
        row = by_source.get(d) or {}
        a = str(row.get("target") or "")
        if not a or a not in remaining_added or a in used_added:
            continue
        if row.get("create_new"):
            continue
        if _is_type_narrow(old_cols[d], new_cols[a], dest_db=dest_db):
            continue
        if (
            _measure_kind_mismatch(d, a)
            or _identity_leaf_mismatch(d, a)
            or _entity_conflict_requires_review(d, a)
        ):
            continue
        if float(row.get("confidence") or 0) < 0.72:
            continue
        pairs.append((d, a))
        used_added.add(a)
    remaining_dropped = [c for c in remaining_dropped if c not in {p[0] for p in pairs}]
    remaining_added = [c for c in remaining_added if c not in used_added]
    return remaining_dropped, remaining_added, pairs


def compatibility_of(classification: dict[str, Any] | None) -> str:
    """Confluent BACKWARD/FORWARD/FULL/NONE for physical SQL transfer.

    Policy does not belong here. Dest-as-consumer:

    * ``identical`` — no additive or breaking diffs.
    * ``forward`` — old dest can still be written if we ADD/WIDEN or ignore new
      columns (nullable add, type widen / Iceberg promote).
    * ``backward`` — new source dropped or renamed fields; dest keeps columns
      (Fivetran net-additive / Iceberg metadata-only drop).
    * ``full`` — both optional add and remove, no hard type/PK change.
    * ``none`` — narrow, type change, PK, NOT NULL, cursor — not auto-applicable.

    Type widen is ``forward`` rather than ``none`` because writers can ALTER
    promote; ``type_locked`` still pauses it at the policy layer.
    """
    classification = classification or {}
    additive = list(classification.get("additive") or [])
    breaking = list(classification.get("breaking") or [])
    hard_or_unknown: list[dict[str, Any]] = []
    soft: list[dict[str, Any]] = []
    known = HARD_BREAKING_KINDS | SOFT_NET_ADDITIVE_KINDS
    for item in breaking:
        kind = str(item.get("kind") or "")
        if kind in SOFT_NET_ADDITIVE_KINDS:
            soft.append(item)
        elif kind in HARD_BREAKING_KINDS or kind not in known:
            hard_or_unknown.append(item)
    if hard_or_unknown:
        return COMPAT_NONE
    has_add = bool(additive)
    has_soft = bool(soft)
    if not has_add and not has_soft:
        return COMPAT_IDENTICAL
    if has_add and has_soft:
        return COMPAT_FULL
    if has_add:
        return COMPAT_FORWARD
    return COMPAT_BACKWARD


def classify_schema_change(
    old_schema: dict[str, Any] | None,
    new_schema: dict[str, Any] | None,
    *,
    dest_db: str = "",
) -> dict[str, Any]:
    """Classify a schema evolution as additive vs breaking.

    Additive: new nullable columns, widen types.
    Breaking: drop/rename/type-narrow/pk change / new NOT NULL columns.

    ``dest_db`` threads dialect rules into widen/narrow (ARRAY→MySQL JSON is
    representation, not false ``narrow_type``).
    """
    old_cols, old_null, old_pk = _unpack_schema(old_schema)
    new_cols, new_null, new_pk = _unpack_schema(new_schema)
    dest_db = (dest_db or "").strip()

    additive: list[dict[str, Any]] = []
    breaking: list[dict[str, Any]] = []

    old_names = set(old_cols)
    new_names = set(new_cols)
    added = sorted(new_names - old_names)
    dropped = sorted(old_names - new_names)

    # Semantic rename: pair dropped↔added by mapper score, not type-only.
    # Type-only pairing is the Fivetran hole (AMT drop + quantity add looks
    # like a rename because both are DECIMAL). Require a real name match and
    # refuse measure/identity/entity false-friends.
    renamed_pairs: list[tuple[str, str]] = []
    if dropped and added:
        remaining_dropped, remaining_added, renamed_pairs = _semantic_rename_pairs(
            dropped, added, old_cols, new_cols, dest_db=dest_db
        )
        for d, a in renamed_pairs:
            breaking.append({
                "kind": "rename",
                "column": d,
                "to": a,
                "old_type": old_cols[d],
                "new_type": new_cols[a],
            })
        dropped = remaining_dropped
        added = remaining_added

    for col in dropped:
        breaking.append({"kind": "drop", "column": col, "old_type": old_cols[col]})

    for col in added:
        nullable = new_null.get(col, True)
        entry = {
            "kind": "add_column",
            "column": col,
            "new_type": new_cols[col],
            "nullable": nullable,
        }
        if nullable:
            additive.append(entry)
        else:
            breaking.append({**entry, "kind": "add_not_null"})

    for col in sorted(old_names & new_names):
        old_t, new_t = old_cols[col], new_cols[col]
        same_logical = normalize_logical_type(old_t) == normalize_logical_type(new_t)
        same_length = _type_length(old_t) == _type_length(new_t)
        # A declaration is not drift against itself. Coercion helpers read the
        # two spellings through the destination's defaults, so an unparameterized
        # carrier (TIMESTAMP_NTZ, TIMESTAMP) resolves to the source ceiling on
        # one side and the destination floor on the other and accuses a column
        # nobody touched of narrowing. Loss against the destination is the
        # fidelity gate's concern, measured on real destination types; drift only
        # reports what changed between two schemas.
        unchanged_declaration = _norm_type(old_t) == _norm_type(new_t)
        # Never short-circuit on same logical + same first-number length alone —
        # DECIMAL(10,4)→DECIMAL(10,2) and BIGINT→TINYINT share that trap.
        if unchanged_declaration or (
            same_logical
            and same_length
            and not _is_type_narrow(old_t, new_t, dest_db=dest_db)
        ):
            # Same declared type; nullability tighten is breaking.
            if col in old_null and col in new_null and old_null[col] and not new_null[col]:
                breaking.append({
                    "kind": "nullability_tighten",
                    "column": col,
                    "old_type": old_t,
                    "new_type": new_t,
                })
            continue
        if _is_type_widen(old_t, new_t, dest_db=dest_db):
            additive.append({
                "kind": "widen_type",
                "column": col,
                "old_type": old_t,
                "new_type": new_t,
            })
        elif _is_type_narrow(old_t, new_t, dest_db=dest_db):
            breaking.append({
                "kind": "narrow_type",
                "column": col,
                "old_type": old_t,
                "new_type": new_t,
            })
        elif not same_logical:
            breaking.append({
                "kind": "type_change",
                "column": col,
                "old_type": old_t,
                "new_type": new_t,
            })

    if old_pk or new_pk:
        if [c.lower() for c in old_pk] != [c.lower() for c in new_pk]:
            breaking.append({
                "kind": "primary_key_change",
                "old_primary_key": old_pk,
                "new_primary_key": new_pk,
            })

    if breaking:
        severity = "breaking"
    elif additive:
        severity = "additive"
    else:
        severity = "none"

    return {
        "additive": additive,
        "breaking": breaking,
        "severity": severity,
        "renamed": [{"from": a, "to": b} for a, b in renamed_pairs],
    }



def _schema_dict_from_flat(
    columns: list[str],
    types: dict[str, str] | None,
    *,
    nullable: dict[str, bool] | None = None,
    primary_key: list[str] | None = None,
) -> dict[str, Any]:
    cols = {str(c): str((types or {}).get(c) or "VARCHAR") for c in columns}
    return {
        "columns": cols,
        "nullable": {c: (nullable or {}).get(c, True) for c in cols},
        "primary_key": list(primary_key or []),
    }


def is_same_declaration_narrow(entries: list[dict[str, Any]] | None) -> bool:
    """True when every row is a type-narrow of a declaration against itself.

    ``joining_date: TIMESTAMP_NTZ → TIMESTAMP_NTZ (narrow_type)`` is dest-floor
    vs source-ceiling invent, not a column anyone changed. A mixed list (a real
    drop plus this invent) is not a no-op — the operator still owes the drop.
    """
    rows = [e for e in (entries or []) if isinstance(e, dict)]
    if not rows:
        return False
    for entry in rows:
        kind = str(entry.get("kind") or "").strip().lower()
        if kind != "narrow_type":
            return False
        old_t = str(entry.get("old_type") or entry.get("from_type") or "")
        new_t = str(entry.get("new_type") or entry.get("to_type") or "")
        if not old_t or not new_t or _norm_type(old_t) != _norm_type(new_t):
            return False
    return True


def classify_from_column_maps(
    old_columns: list[str] | None,
    old_types: dict[str, str] | None,
    new_columns: list[str] | None,
    new_types: dict[str, str] | None,
    *,
    old_pk: list[str] | None = None,
    new_pk: list[str] | None = None,
    cursor_fields: list[str] | None = None,
    dest_db: str = "",
) -> dict[str, Any]:
    """Classify evolution from flat column maps (plan revisions / live introspect)."""
    old_columns = list(old_columns or [])
    new_columns = list(new_columns or [])
    if not old_columns and not new_columns:
        return {"additive": [], "breaking": [], "severity": "none", "renamed": []}
    report = classify_schema_change(
        _schema_dict_from_flat(old_columns, old_types, primary_key=old_pk),
        _schema_dict_from_flat(new_columns, new_types, primary_key=new_pk),
        dest_db=dest_db,
    )
    # Airbyte hard-break: cursor removed from source.
    cursors = [str(c).strip() for c in (cursor_fields or []) if str(c).strip()]
    if cursors and old_columns:
        old_lower = {c.lower() for c in old_columns}
        new_lower = {c.lower() for c in new_columns}
        for cur in cursors:
            if cur.lower() in old_lower and cur.lower() not in new_lower:
                report["breaking"].append({
                    "kind": "cursor_removed",
                    "column": cur,
                })
                report["severity"] = "breaking"
    return report


def resolve_schema_evolution(
    classification: dict[str, Any] | None,
    *,
    schema_policy: str = "manual_review",
    unmapped_sources: list[str] | None = None,
    source_changed: bool = False,
) -> dict[str, Any]:
    """Decide pause / propagate / review / continue — Validate and Execute SSOT.

    Airbyte: propagate applies non-breaking; breaking always pauses.
    Fivetran: net-additive drops/renames under propagate (keep dest history).
    Datawrap: also fail-closed on type narrow (no silent airbyte_meta soft-pass).
    """
    policy = (schema_policy or "manual_review").strip().lower()
    classification = classification or {
        "additive": [],
        "breaking": [],
        "severity": "none",
        "renamed": [],
    }
    additive = list(classification.get("additive") or [])
    breaking = list(classification.get("breaking") or [])
    unmapped = list(unmapped_sources or [])

    hard: list[dict[str, Any]] = []
    soft: list[dict[str, Any]] = []
    for item in breaking:
        kind = str(item.get("kind") or "")
        if kind in HARD_BREAKING_KINDS:
            hard.append(item)
        elif kind in SOFT_NET_ADDITIVE_KINDS and policy in PROPAGATE_POLICIES:
            soft.append(item)
        elif kind in SOFT_NET_ADDITIVE_KINDS and policy == "manual_review":
            soft.append(item)
        else:
            hard.append(item)

    if source_changed and not additive and not breaking and unmapped:
        for col in unmapped:
            additive.append({
                "kind": "add_column",
                "column": col,
                "new_type": "VARCHAR",
                "nullable": True,
                "inferred": True,
            })

    action = "continue"
    reasons: list[str] = []

    if policy == "pause_on_change" and (
        additive or hard or soft or source_changed or unmapped
    ):
        action = "pause"
        reasons.append("schema_policy=pause_on_change")
    elif hard:
        action = "pause"
        reasons.append(
            "hard_breaking:"
            + ",".join(sorted({str(h.get("kind")) for h in hard}))
        )
    elif policy == "type_locked" and any(
        str(a.get("kind")) == "widen_type" for a in additive
    ):
        action = "pause"
        reasons.append("type_locked_blocks_widen")
    elif policy in PROPAGATE_POLICIES and (additive or soft or unmapped):
        action = "propagate"
        reasons.append(f"auto_propagate under {policy}")
    elif policy == "manual_review" and (additive or soft or unmapped or source_changed):
        action = "review"
        reasons.append("manual_review_keep_existing_mappings")
    elif policy == "type_locked" and unmapped:
        action = "review"
        reasons.append("type_locked_new_columns_need_approval")

    severity = "none"
    if hard or action == "pause":
        severity = "breaking"
    elif additive or soft or (action == "propagate"):
        severity = "additive"
    elif action == "review":
        severity = "warning"

    compat = compatibility_of({
        "additive": additive,
        "breaking": breaking,
    })
    return {
        "action": action,
        "severity": severity,
        "policy": policy,
        "reasons": reasons,
        "additive": additive,
        "hard_breaking": hard,
        "soft_net_additive": soft,
        "unmapped_sources": unmapped,
        "should_pause": action == "pause",
        "should_propagate": action == "propagate",
        "compatibility": compat,
        "compatibility_note": COMPATIBILITY_NOTES[compat],
        "backfill_recommended": bool(
            action == "propagate"
            and any(
                str(a.get("kind")) in {"add_column", "rename"}
                for a in additive + soft
            )
        ),
    }


def classify_schema_evolution_report(
    old_schema: dict[str, Any] | None,
    new_schema: dict[str, Any] | None,
    *,
    dest_db: str = "",
    schema_policy: str = "manual_review",
) -> dict[str, Any]:
    """Classify + decide — the payload Validate, Contracts, and /schema-drift share."""
    classification = classify_schema_change(
        old_schema, new_schema, dest_db=dest_db
    )
    evolution = resolve_schema_evolution(
        classification, schema_policy=schema_policy
    )
    return {
        **classification,
        "schema_evolution": evolution,
        "compatibility": evolution["compatibility"],
        "compatibility_note": evolution["compatibility_note"],
        "hard_breaking": evolution["hard_breaking"],
        "soft_net_additive": evolution["soft_net_additive"],
        "summary": _evolution_summary(evolution),
    }


def _evolution_summary(evolution: dict[str, Any]) -> str:
    action = str(evolution.get("action") or "continue")
    compat = str(evolution.get("compatibility") or COMPAT_IDENTICAL)
    hard = evolution.get("hard_breaking") or []
    if action == "pause" and hard:
        kind = str(hard[0].get("kind") or "breaking")
        column = str(hard[0].get("column") or hard[0].get("to") or "")
        named = f"{kind} on {column}" if column else kind
        return f"Hard-breaking {named} — paused (compatibility={compat})."
    if action == "propagate":
        return f"Safe to auto-propagate (compatibility={compat})."
    if action == "review":
        return f"Review required before Execute (compatibility={compat})."
    return COMPATIBILITY_NOTES.get(compat, "")


def apply_propagate_mappings(
    mappings: list[dict[str, Any]] | None,
    *,
    source_columns: list[str],
    source_schema: dict[str, str] | None = None,
    evolution: dict[str, Any] | None = None,
    schema_policy: str = "manual_review",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Extend mappings for auto-propagate — shared by Validate and Execute.

    Returns ``(mappings, applied_changes)``. Under non-propagate policies the
    mapping list is unchanged (Airbyte approve-myself / Fivetran block).
    """
    mappings = [dict(m) for m in (mappings or [])]
    policy = (schema_policy or "manual_review").strip().lower()
    evolution = evolution or {}
    if policy not in PROPAGATE_POLICIES and not evolution.get("should_propagate"):
        return mappings, []

    existing_src = {
        str(m.get("source") or "").strip()
        for m in mappings
        if m.get("source")
    }
    existing_src_lower = {s.lower() for s in existing_src}
    source_schema = source_schema or {}
    applied: list[dict[str, Any]] = []

    def _add(col: str, *, reason: str, renamed_from: str | None = None) -> None:
        name = str(col or "").strip()
        if not name or name.lower() in existing_src_lower:
            return
        if name not in source_columns and name.lower() not in {
            c.lower() for c in source_columns
        }:
            match = next((c for c in source_columns if c.lower() == name.lower()), None)
            if not match:
                return
            name = match
        entry: dict[str, Any] = {
            "source": name,
            "target": name,
            "confidence": 1.0,
            "propagated": True,
            "assignment_strategy": "create_compatible_new",
            "propagate_reason": reason,
        }
        if name in source_schema:
            entry["source_type"] = source_schema[name]
        if renamed_from:
            entry["renamed_from"] = renamed_from
        mappings.append(entry)
        existing_src.add(name)
        existing_src_lower.add(name.lower())
        applied.append(entry)

    for change in list(evolution.get("additive") or []):
        if str(change.get("kind")) == "add_column":
            _add(str(change.get("column") or ""), reason="add_column")

    for change in list(evolution.get("soft_net_additive") or []):
        if str(change.get("kind")) == "rename":
            _add(
                str(change.get("to") or ""),
                reason="soft_rename",
                renamed_from=str(change.get("column") or ""),
            )

    for col in list(evolution.get("unmapped_sources") or []):
        _add(str(col), reason="unmapped_propagate")

    return mappings, applied


def detect_schema_drift(
    *,
    source_columns: list[str],
    source_schema: dict[str, str] | None,
    target_columns: list[str] | None,
    target_schema: dict[str, str] | None,
    stored_source_fp: str = "",
    stored_target_fp: str = "",
    mappings: list[dict[str, Any]] | None = None,
    destination_db_type: str = "",
    sample_rows: list[dict[str, Any]] | None = None,
    previous_source_columns: list[str] | None = None,
    previous_source_schema: dict[str, str] | None = None,
    previous_primary_key: list[str] | None = None,
    live_primary_key: list[str] | None = None,
    cursor_fields: list[str] | None = None,
    schema_policy: str = "manual_review",
    table_exists: bool | None = None,
    declared_source_columns: list[str] | None = None,
    declared_source_schema: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Compare live schemas to stored contracts; attach schema_evolution plan.

    Two different questions are asked of the source here, and an approved
    pre-load transform separates them:

    * *Did the source change under the stored mapping revision?* — answered by
      the **declared** source, because that is what the revision fingerprinted.
    * *Do the values this run will bind fit the destination carrier?* — answered
      by the **transformed** image, because that is what the writer receives.

    Conflating them blocks a correct run either way: judging the fingerprint on
    the transformed image reports the operator's own recipe as source drift,
    while judging the carrier on the declared type grades a column rounded to
    whole numbers as a ``DECIMAL(11,8) → INT4`` precision collapse for values
    that are now integers. ``declared_source_*`` default to the live arguments,
    so an unshaped run behaves exactly as before.
    """
    source_schema = source_schema or {}
    target_columns = target_columns or []
    target_schema = target_schema or {}
    mappings = mappings or []
    dest_kind = normalize_dest_kind(destination_db_type)
    schemaless = dest_kind in SCHEMALESS_DESTS
    create_new = table_exists is False
    # Only an introspected existing table is a live DDL contract. Unknown
    # (None) and create-new must not invent destination drift from Studio maps.
    live_ddl_contract = bool(table_exists is True and target_schema and not schemaless)

    # The revision signed the declared source, so the fingerprint questions read
    # it; every value/carrier question below reads the live (possibly
    # transformed) image.
    fp_source_columns = list(declared_source_columns or source_columns)
    fp_source_schema = dict(declared_source_schema or source_schema)

    live_source_fp = fingerprint_schema(fp_source_columns, fp_source_schema)
    live_target_fp = (
        fingerprint_schema(target_columns, target_schema)
        if target_columns and live_ddl_contract
        else ""
    )

    source_changed = bool(stored_source_fp) and not schemas_match(
        stored_source_fp, fp_source_columns, fp_source_schema
    )
    target_changed = bool(
        live_ddl_contract
        and stored_target_fp
        and target_columns
        and not schemas_match(stored_target_fp, target_columns, target_schema)
    )

    mapped_sources = {
        str(m.get("source")).lower() for m in mappings if m.get("source")
    }
    try:
        from services.mapping_constraints import is_intentional_omit, write_mappings

        active_mappings = write_mappings(mappings)
        intentional_omits = [
            str(m.get("source"))
            for m in mappings
            if is_intentional_omit(m) and m.get("source")
        ]
    except Exception:
        active_mappings = list(mappings)
        intentional_omits = []
    mapped_targets = {
        str(m.get("target")).lower()
        for m in active_mappings
        if m.get("target")
    }
    unmapped_sources = [c for c in source_columns if c.lower() not in mapped_sources]
    try:
        from services.scd2_engine import SCD2_COLUMNS

        system_targets = {c.lower() for c in SCD2_COLUMNS}
    except Exception:
        system_targets = {"valid_from", "valid_to", "is_current", "row_hash"}
    system_targets |= {"_df_lsn", "df_lsn"}
    orphan_targets = [
        c
        for c in target_columns
        if c.lower() not in mapped_targets and c.lower() not in system_targets
    ]
    if not live_ddl_contract:
        orphan_targets = []

    type_mismatches: list[dict[str, str]] = []
    if live_ddl_contract:
        from services.coercion_probe import samples_coerce_mapping

        for m in active_mappings:
            src = str(m.get("source") or "")
            tgt = str(m.get("target") or "")
            if not src or not tgt:
                continue
            src_type = (
                source_schema.get(src)
                or next(
                    (source_schema[k] for k in source_schema if k.lower() == src.lower()),
                    None,
                )
                or "VARCHAR"
            )
            # Prefer mapping stamp over invented VARCHAR when live schema lacks column.
            from services.decision_kernel import is_precision_collapse_coercion
            from services.type_system import resolve_mapping_target_type

            dest_db = str(destination_db_type or dest_kind or "")
            tgt_type = resolve_mapping_target_type(
                m,
                target_types=target_schema,
                source_type=str(src_type),
                dest_db_type=dest_db,
            ) or ci_get(target_schema, tgt) or ""
            if not tgt_type:
                type_mismatches.append({
                    "source": src,
                    "target": tgt,
                    "source_type": str(src_type).upper(),
                    "target_type": "",
                    "reason": "pending_dest_type",
                })
                continue
            if not is_lossy_coercion(src_type, tgt_type, dest_db=dest_db):
                continue
            if decimal_capacity_is_equal_or_wider(
                str(src_type), str(tgt_type), dest_db=dest_db
            ):
                # Drift asks whether the destination carrier can still take the
                # source. A wider fixed-point sink can. The invented shape
                # (BigQuery bare BIGNUMERIC is 76,38) stays a fidelity/contract
                # chip, so pausing the sync here only re-reported it as loss.
                continue

            if is_precision_collapse_coercion(src_type, tgt_type, dest_db=dest_db):
                type_mismatches.append({
                    "source": src,
                    "target": tgt,
                    "source_type": src_type.upper(),
                    "target_type": tgt_type.upper(),
                    "reason": "precision_collapse",
                })
                continue
            if sample_rows and samples_coerce_mapping(
                m,
                source_types=source_schema,
                target_types=target_schema,
                rows=sample_rows,
            ):
                continue
            type_mismatches.append({
                "source": src,
                "target": tgt,
                "source_type": src_type.upper(),
                "target_type": tgt_type.upper(),
            })

    classification: dict[str, Any] | None = None
    prev_cols = list(previous_source_columns or [])
    prev_types = dict(previous_source_schema or {})
    # Source PK evolution only — never wire destination DDL PK as live_primary_key.
    if prev_cols or prev_types:
        classification = classify_from_column_maps(
            prev_cols or list(prev_types.keys()),
            prev_types,
            fp_source_columns,
            fp_source_schema,
            old_pk=previous_primary_key,
            new_pk=live_primary_key,
            cursor_fields=cursor_fields,
            dest_db=str(destination_db_type or dest_kind or ""),
        )
    elif source_changed and mapped_sources:
        still_present = [c for c in source_columns if c.lower() in mapped_sources]
        dropped_mapped = [
            s for s in mapped_sources
            if not any(c.lower() == s for c in source_columns)
        ]
        # Only columns that are new vs previous fingerprint revision.
        if still_present and unmapped_sources and not dropped_mapped:
            classification = {
                "additive": [
                    {
                        "kind": "add_column",
                        "column": c,
                        "new_type": source_schema.get(c, "VARCHAR"),
                        "nullable": True,
                        "inferred": True,
                    }
                    for c in unmapped_sources
                ],
                "breaking": [],
                "severity": "additive",
                "renamed": [],
            }
        elif dropped_mapped:
            classification = {
                "additive": [
                    {
                        "kind": "add_column",
                        "column": c,
                        "new_type": source_schema.get(c, "VARCHAR"),
                        "nullable": True,
                        "inferred": True,
                    }
                    for c in unmapped_sources
                ],
                "breaking": [
                    {"kind": "drop", "column": c, "old_type": "VARCHAR"}
                    for c in dropped_mapped
                ],
                "severity": "breaking",
                "renamed": [],
            }

    if type_mismatches:
        classification = classification or {
            "additive": [],
            "breaking": [],
            "severity": "breaking",
            "renamed": [],
        }
        for tm in type_mismatches:
            classification["breaking"].append({
                "kind": (
                    "narrow_type"
                    if tm.get("reason") == "precision_collapse"
                    else "type_change"
                ),
                "column": tm.get("source"),
                "old_type": tm.get("source_type"),
                "new_type": tm.get("target_type"),
                "target": tm.get("target"),
            })
        classification["severity"] = "breaking"

    # Intentional subset maps (operator omitted columns) are not schema drift.
    # Only columns that appeared since the previous revision drive evolution.
    prev_lower = {str(c).lower() for c in prev_cols} | {str(c).lower() for c in prev_types}
    if prev_lower:
        evolution_unmapped = [c for c in unmapped_sources if c.lower() not in prev_lower]
    elif source_changed:
        evolution_unmapped = list(unmapped_sources)
    else:
        evolution_unmapped = []

    evolution = resolve_schema_evolution(
        classification,
        schema_policy=schema_policy,
        unmapped_sources=evolution_unmapped,
        source_changed=source_changed or target_changed,
    )

    issues: list[str] = []
    if source_changed:
        issues.append("Source schema changed since last mapping revision")
    if target_changed:
        issues.append("Destination schema changed since last mapping revision")
    if evolution_unmapped:
        issues.append(f"{len(evolution_unmapped)} new source column(s) have no mapping")
    elif unmapped_sources and not prev_lower and not source_changed:
        # Subset mapping on first revision — informational only (not a blocker).
        pass
    elif unmapped_sources and prev_lower and not evolution_unmapped:
        pass
    if orphan_targets:
        issues.append(f"{len(orphan_targets)} destination column(s) are unmapped")
    if type_mismatches:
        issues.append(f"{len(type_mismatches)} mapped column pair(s) have type mismatch")
    if evolution.get("hard_breaking"):
        issues.append(
            f"{len(evolution['hard_breaking'])} hard-breaking schema change(s) require review"
        )

    severity = evolution.get("severity") or "none"
    if severity == "none" and (evolution_unmapped or orphan_targets):
        severity = "warning"
    elif severity == "none" and issues:
        severity = "warning"

    return {
        "drift_detected": bool(issues) or evolution.get("action") not in {"continue", None},
        "severity": severity,
        "issues": issues,
        "source_fingerprint": live_source_fp,
        "target_fingerprint": live_target_fp,
        "source_changed": source_changed,
        "target_changed": target_changed,
        "unmapped_sources": unmapped_sources,
        "evolution_unmapped_sources": evolution_unmapped,
        "orphan_targets": orphan_targets,
        "type_mismatches": type_mismatches,
        "intentional_omits": intentional_omits,
        "mapping_coverage": round(
            len({str(m.get("source")).lower() for m in active_mappings if m.get("source")})
            / max(len(source_columns), 1),
            3,
        ),
        "classification": classification,
        "schema_evolution": evolution,
        "table_exists": table_exists,
        "create_new": create_new,
        "live_ddl_contract": live_ddl_contract,
    }
