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

import logging
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
_logger = logging.getLogger(__name__)

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
    "block_all": (
        "Any source change — including a nullable add — blocks until you "
        "Accept drift for this run. Destination-only columns that were never "
        "source-fed stay an audit note. Hard type-narrow cannot be accepted away."
    ),
}


def normalize_schema_policy(policy: str | None) -> str:
    """Canonical policy id.

    Empty means ``propagate_columns`` — the product default for a new
    connection (Fivetran Allow columns). ``block`` is ``block_all``.
    ``approve`` is the existing ``manual_review`` path. A stored policy is
    never rewritten.
    """
    key = (policy or "").strip().lower()
    if not key:
        return "propagate_columns"
    if key == "block":
        return "block_all"
    if key == "approve":
        return "manual_review"
    return key


def schema_policy_honesty_line(policy: str) -> str:
    """Studio / G10 caption for the Advanced schema-change policy."""
    key = normalize_schema_policy(policy) if (policy or "").strip() else "manual_review"
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
        if old_len is not None and new_len is not None:
            return new_len > old_len
        # Same logical family without ``(n)`` params — INT4→INT8 is a widen
        # the parametric check cannot see. The canonical capacity lattice
        # (connectors.schema_drift.is_wider_type) ranks integer widths,
        # NUMERIC(p,s), temporal fsp, etc. — reuse it, never a second table.
        from connectors.schema_drift import is_wider_type

        return is_wider_type(old_type, new_type, dest_db=dest_db)
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


def _name_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def _levenshtein(left: str, right: str) -> int:
    if left == right:
        return 0
    if not left:
        return len(right)
    if not right:
        return len(left)
    prev = list(range(len(right) + 1))
    for i, ch in enumerate(left, start=1):
        cur = [i]
        for j, other in enumerate(right, start=1):
            cur.append(min(
                prev[j] + 1,
                cur[j - 1] + 1,
                prev[j - 1] + (ch != other),
            ))
        prev = cur
    return prev[-1]


def _jaro(left: str, right: str) -> float:
    if left == right:
        return 1.0
    if not left or not right:
        return 0.0
    window = max(len(left), len(right)) // 2 - 1
    window = max(window, 0)
    left_matches = [False] * len(left)
    right_matches = [False] * len(right)
    matches = 0
    transpositions = 0
    for i, ch in enumerate(left):
        start = max(0, i - window)
        end = min(i + window + 1, len(right))
        for j in range(start, end):
            if right_matches[j] or ch != right[j]:
                continue
            left_matches[i] = True
            right_matches[j] = True
            matches += 1
            break
    if matches == 0:
        return 0.0
    k = 0
    for i, matched in enumerate(left_matches):
        if not matched:
            continue
        while not right_matches[k]:
            k += 1
        if left[i] != right[k]:
            transpositions += 1
        k += 1
    transpositions /= 2
    return (
        matches / len(left)
        + matches / len(right)
        + (matches - transpositions) / matches
    ) / 3


def _jaro_winkler(left: str, right: str) -> float:
    score = _jaro(left, right)
    prefix = 0
    for a, b in zip(left, right):
        if a != b or prefix == 4:
            break
        prefix += 1
    return score + prefix * 0.1 * (1 - score)


def _rename_name_evidence(dropped: str, added: str) -> bool:
    """Similarity evidence for a rename. Type evidence is checked separately.

    Underscore-insensitive equality, or Jaro-Winkler plus a short edit
    distance. Type-compatible names that are not actually similar
    (``AMT`` / ``quantity``) stay a drop plus an add.
    """
    left = _name_key(dropped)
    right = _name_key(added)
    if not left or not right or left == right:
        return bool(left and left == right)
    distance = _levenshtein(left, right)
    longest = max(len(left), len(right))
    ratio = 1 - (distance / longest)
    return _jaro_winkler(left, right) >= 0.94 and ratio >= 0.80 and distance <= 3


def _types_support_rename(old_type: str, new_type: str, *, dest_db: str = "") -> bool:
    """Type evidence: same logical type or a widen. A narrow is not a rename."""
    if _is_type_narrow(old_type, new_type, dest_db=dest_db):
        return False
    if normalize_logical_type(old_type) == normalize_logical_type(new_type):
        return True
    return _is_type_widen(old_type, new_type, dest_db=dest_db)


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
    # Second pass: the mapper misses underscore-only and near renames
    # (customer_name → customername). Similarity is not enough — the types
    # must agree, and measure/identity/entity false-friends stay unpaired.
    for d in list(remaining_dropped):
        if d in {p[0] for p in pairs}:
            continue
        best: str | None = None
        best_score = 0.0
        for a in remaining_added:
            if a in used_added:
                continue
            if not _types_support_rename(old_cols[d], new_cols[a], dest_db=dest_db):
                continue
            if (
                _measure_kind_mismatch(d, a)
                or _identity_leaf_mismatch(d, a)
                or _entity_conflict_requires_review(d, a)
            ):
                continue
            if not _rename_name_evidence(d, a):
                continue
            score = _jaro_winkler(_name_key(d), _name_key(a))
            if best is None or score > best_score:
                best = a
                best_score = score
        if not best:
            continue
        pairs.append((d, best))
        used_added.add(best)
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


def execute_must_pause(
    evolution: dict[str, Any] | None,
    *,
    schema_drift_acknowledged: bool = False,
) -> bool:
    """Whether Execute refuses this evolution.

    Hard-breaking (narrow, PK, compatibility none) cannot be acknowledged
    away. A non-hard pause (``block_all`` / ``pause_on_change`` on an additive
    change) yields for one run when the operator accepted drift.
    """
    evolution = evolution or {}
    if not evolution.get("should_pause"):
        return False
    if evolution.get("hard_breaking"):
        return True
    return not bool(schema_drift_acknowledged)


def resolve_schema_evolution(
    classification: dict[str, Any] | None,
    *,
    schema_policy: str = "propagate_columns",
    unmapped_sources: list[str] | None = None,
    source_changed: bool = False,
) -> dict[str, Any]:
    """Decide pause / propagate / review / continue — Validate and Execute SSOT.

    Airbyte: propagate applies non-breaking; breaking always pauses.
    Fivetran: net-additive drops/renames under propagate (keep dest history).
    Datawrap: also fail-closed on type narrow (no silent airbyte_meta soft-pass).
    ``block_all`` pauses any real source change. Destination-only columns are
    not a source change — callers must not pass them as ``source_changed``.
    """
    policy = normalize_schema_policy(schema_policy)
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

    if policy in {"pause_on_change", "block_all"} and (
        additive or hard or soft or source_changed or unmapped
    ):
        action = "pause"
        reasons.append(f"schema_policy={policy}")
    elif hard:
        action = "pause"
        reasons.append(
            "hard_breaking:"
            + ",".join(sorted({str(h.get("kind")) for h in hard}))
        )
    elif policy == "type_locked":
        # type_locked: block widen_type, review new columns, never auto-propagate
        if any(str(a.get("kind")) == "widen_type" for a in additive):
            action = "pause"
            reasons.append("type_locked_blocks_widen")
        elif unmapped:
            action = "review"
            reasons.append("type_locked_new_columns_need_approval")
        elif additive or soft:
            # Other additive changes under type_locked: review, don't auto-propagate.
            # An identical schema is not a change and must not demand review.
            action = "review"
            reasons.append("type_locked_review_required")
    elif policy in PROPAGATE_POLICIES and (additive or soft or unmapped):
        action = "propagate"
        reasons.append(f"auto_propagate under {policy}")
    elif policy == "manual_review" and (additive or soft or unmapped or source_changed):
        action = "review"
        reasons.append("manual_review_keep_existing_mappings")

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
        "audit": (
            [{
                "decision": "auto_pass",
                "policy": policy,
                "note": (
                    "Non-breaking drift auto-passed. Destination history is kept; "
                    "nothing was dropped."
                ),
                "additive": [str(a.get("kind") or "") for a in additive],
                "soft": [str(a.get("kind") or "") for a in soft],
            }]
            if action == "propagate"
            else []
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


_PARAM_TYPE = re.compile(r"^\s*([A-Za-z][A-Za-z0-9_ ]*?)\s*\(([^)]*)\)(.*)$")


def widened_dest_carrier(dest_type: str, source_type: str) -> str:
    """The destination's own carrier at the source's size.

    ``NVARCHAR(50) COLLATE Latin1_General_BIN`` widened for a ``VARCHAR(500)``
    source is ``NVARCHAR(500) COLLATE Latin1_General_BIN``: the size comes from
    the source, the family and collation stay the destination's. Writing the
    source's spelling (``VARCHAR(500)``) onto SQL Server would ALTER a Unicode
    column to a code-page one, which every gate then grades as the lossy pair
    the approval was meant to remove. Types without a matching parameter list
    (``INTEGER`` -> ``BIGINT``) take the source type.
    """
    dest_m = _PARAM_TYPE.match(str(dest_type or ""))
    src_m = _PARAM_TYPE.match(str(source_type or ""))
    if dest_m and src_m:
        dest_args = [a.strip() for a in dest_m.group(2).split(",")]
        src_args = [a.strip() for a in src_m.group(2).split(",")]
        if len(dest_args) == len(src_args) and all(a.isdigit() for a in src_args):
            suffix = dest_m.group(3).strip()
            carrier = f"{dest_m.group(1).strip()}({','.join(src_args)})"
            return f"{carrier} {suffix}".strip().upper()
    return str(source_type or "").upper()


def propose_dest_widens(
    mappings: list[dict[str, Any]] | None,
    *,
    source_schema: dict[str, str],
    target_schema: dict[str, str],
    dest_db: str = "",
) -> list[dict[str, Any]]:
    """D107: destination-side widen proposals for a live destination table.

    A source that declared a strictly *wider* type than the destination
    carrier can be ALTERed up losslessly — additive, never hard-breaking.
    ``detect_schema_drift``'s live-contract loop emits these inline; the
    engine's auto-propagate path (which never sets ``table_exists``) calls
    this directly so the same lane fires on Run.
    """
    if not target_schema:
        return []
    from connectors.schema_drift import is_wider_type
    from connectors.writer_common import stamp_is_operator_ceiling

    out: list[dict[str, Any]] = []
    for m in mappings or []:
        src = str(m.get("source") or "")
        tgt = str(m.get("target") or "")
        if not src or not tgt:
            continue
        src_type = str(ci_get(source_schema, src) or "")
        tgt_type = str(ci_get(target_schema, tgt) or "")
        if not src_type or not tgt_type:
            continue
        # Same logical family only: TEXT is a universal carrier the lattice
        # calls "wider" than NUMERIC, but retiping a numeric column to text
        # is a family change — a true narrow stays fail-closed (D107).
        if normalize_logical_type(src_type) != normalize_logical_type(tgt_type):
            continue
        if (
            is_wider_type(tgt_type, src_type, dest_db=dest_db)
            and not stamp_is_operator_ceiling(m)
        ):
            out.append({
                "kind": "widen_type",
                "column": src,
                "target": tgt,
                "old_type": tgt_type.upper(),
                "new_type": widened_dest_carrier(tgt_type, src_type),
                "reason": "dest_widen_available",
            })
    return out


def stamp_approved_widens(
    mappings: list[dict[str, Any]] | None,
    widen_changes: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Stamp approved destination widens onto the existing mappings.

    D107: an approved source/destination widen marks the mapping with
    ``dest_widen`` + the new declared type — the writer's widen path reads it
    and issues ALTER COLUMN before binding rows. ``target_type_origin`` is
    ``drift_widen`` so the stamp is never mistaken for an operator ceiling.
    Returns the applied widen entries.
    """
    applied: list[dict[str, Any]] = []
    for change in widen_changes or []:
        src_name = str(change.get("source") or change.get("column") or "")
        new_type = str(change.get("new_type") or "")
        if not src_name or not new_type:
            continue
        for m in mappings or []:
            if str(m.get("source") or "").lower() != src_name.lower():
                continue
            m["dest_widen"] = True
            m["target_type"] = new_type
            m["target_type_origin"] = "drift_widen"
            if m.get("review_kind") == "lossy":
                # Map raised that review against the narrower live carrier
                # (INT4 ← BIGINT). The approved widen removes its cause, and an
                # ambiguous-mapping block would otherwise outlive the decision.
                m["requires_review"] = False
                m.pop("review_kind", None)
                m["reasoning"] = str(m.get("reasoning") or "").replace(
                    " · lossy type pair", ""
                )
            applied.append({
                "source": src_name,
                "target": m.get("target") or src_name,
                "propagated": True,
                "propagate_reason": "widen_type",
                "new_type": new_type,
            })
    return applied


def effective_schema_policy(
    schema_policy: str | None, *, backfill_new_fields: bool = False
) -> str:
    """The policy this run is governed by — one rule for every gate.

    ``backfill_new_fields`` asks for ADD COLUMN, which is what propagate means.
    Under ``manual_review`` it used to align only G10, so drift detection kept
    the review policy and blocked the very column the operator asked to land
    (D20). ``type_locked`` / ``pause_on_change`` forbid silent ADD COLUMN and
    keep their policy; G10 names that conflict.
    """
    policy = (schema_policy or "manual_review").strip().lower() or "manual_review"
    if backfill_new_fields and policy == "manual_review":
        return "propagate_columns"
    return policy


def destination_evolution_approved(
    schema_policy: str | None, *, acknowledged: bool = False
) -> bool:
    """Whether additive destination evolution (widen, ADD COLUMN) is approved.

    Propagate policies approve it up front; ``manual_review`` approves it after
    the drift acknowledgement. ``type_locked`` / ``pause_on_change`` /
    ``block_all`` never ALTER — the run fails closed on bind instead. Validate
    and Execute both ask this, so they cannot disagree.
    """
    policy = normalize_schema_policy(schema_policy)
    return policy in PROPAGATE_POLICIES or (acknowledged and policy == "manual_review")


def plan_destination_evolution(
    mappings: list[dict[str, Any]] | None,
    *,
    source_columns: list[str],
    source_schema: dict[str, str],
    dest_types: dict[str, str],
    dest_db: str = "",
    adopt_unmapped: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, str], list[dict[str, Any]]]:
    """Apply approved destination evolution to the plan, once.

    Returns ``(mappings, dest_types, applied)`` where the destination types are
    the post-ALTER carriers: a widened column reads as its new, wider type and
    a mapped column the table lacks reads as the ADD COLUMN it will become.
    Every gate downstream then grades the source against the destination this
    run will leave behind, instead of each gate patching the live types itself
    (D20/D107). The caller decides approval
    (:func:`destination_evolution_approved`) and that the table exists.

    ``adopt_unmapped`` is a propagate policy's other half: a source column that
    is neither mapped nor declared omitted is a new column to add, not a
    column to skip. A replayed mapping set (a schedule's stored maps) predates
    the column, and without this the run was refused or the column silently
    never moved (D165). An acknowledgement does not adopt: it keeps the
    existing mappings and records the omission.
    """
    from services.mapping_constraints import write_mappings

    planned = [dict(m) for m in (mappings or []) if isinstance(m, dict)]
    types = dict(dest_types or {})
    active = write_mappings(planned)
    applied: list[dict[str, Any]] = []

    widens = propose_dest_widens(
        active, source_schema=source_schema, target_schema=types, dest_db=dest_db
    )
    applied.extend(stamp_approved_widens(planned, widens))
    by_target = {str(k).lower(): k for k in types}
    for change in widens:
        key = by_target.get(str(change.get("target") or "").lower())
        if key is not None:
            types[key] = str(change["new_type"])
    if widens:
        # Map stamped the fidelity verdict against the table as it stood
        # (BIGINT -> INT4, DECIMAL(18,4) -> DECIMAL(10,2)). A widened pair is
        # graded against the carrier it will have. Done here, not by a caller:
        # Validate re-derived it and Execute did not, so Execute's mapping
        # review refused the run Validate had passed (D200).
        from services.mapping_proof import stamp_mapping_fidelity

        widened = {str(c.get("column") or "").lower() for c in widens}
        planned = [
            stamp_mapping_fidelity(
                [m],
                source_types=source_schema,
                target_types=types,
                destination_db_type=dest_db,
                dest_table_exists=True,
            )[0]
            if str(m.get("source") or "").lower() in widened
            else m
            for m in planned
        ]
        active = write_mappings(planned)

    present = {str(c).lower() for c in source_columns}
    for m in active:
        src = str(m.get("source") or "")
        tgt = str(m.get("target") or "")
        if not src or not tgt or src.lower() not in present or tgt.lower() in by_target:
            continue
        added = str(m.get("target_type") or ci_get(source_schema, src) or "VARCHAR")
        types[tgt] = added
        by_target[tgt.lower()] = tgt
        # The same vocabulary Map uses for an ADD COLUMN proposal, so the
        # writer's backfill lane opens for this column and no other.
        m["create_new"] = True
        m["assignment_strategy"] = "create_compatible_new"
        applied.append({
            "source": src,
            "target": tgt,
            "propagated": True,
            "propagate_reason": "add_column",
            "new_type": added,
        })

    if adopt_unmapped:
        covered = {str(m.get("source") or "").lower() for m in planned}
        for col in source_columns:
            if not col or str(col).lower() in covered:
                continue
            entry = propagated_mapping_entry(
                str(col), reason="unmapped_propagate", source_schema=source_schema
            )
            entry["create_new"] = True
            planned.append(entry)
            covered.add(str(col).lower())
            types.setdefault(str(col), str(ci_get(source_schema, str(col)) or "VARCHAR"))
            applied.append({
                "source": str(col),
                "target": str(col),
                "propagated": True,
                "propagate_reason": "unmapped_propagate",
                "new_type": types[str(col)],
            })
    return planned, types, applied


def propagated_mapping_entry(
    name: str,
    *,
    reason: str,
    source_schema: dict[str, str] | None = None,
    renamed_from: str | None = None,
) -> dict[str, Any]:
    """The mapping a propagate policy adds for a source column nobody mapped."""
    entry: dict[str, Any] = {
        "source": name,
        "target": name,
        "confidence": 1.0,
        "propagated": True,
        "assignment_strategy": "create_compatible_new",
        "propagate_reason": reason,
    }
    if name in (source_schema or {}):
        entry["source_type"] = (source_schema or {})[name]
    if renamed_from:
        entry["renamed_from"] = renamed_from
    return entry


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
        entry = propagated_mapping_entry(
            name,
            reason=reason,
            source_schema=source_schema,
            renamed_from=renamed_from,
        )
        mappings.append(entry)
        existing_src.add(name)
        existing_src_lower.add(name.lower())
        applied.append(entry)

    for change in list(evolution.get("additive") or []):
        kind = str(change.get("kind"))
        if kind == "add_column":
            _add(str(change.get("column") or ""), reason="add_column")
        elif kind == "widen_type":
            # plan_destination_evolution already sized this column against the
            # live destination carrier. A source-contract widen names only the
            # source's spelling; re-stamping it overwrote NVARCHAR(500) with
            # VARCHAR(500) between Validate and the writer.
            col = str(change.get("source") or change.get("column") or "").lower()
            if any(
                m.get("dest_widen") and str(m.get("source") or "").lower() == col
                for m in mappings
            ):
                continue
            applied.extend(stamp_approved_widens(mappings, [change]))

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


def schema_review_copy(drift: dict[str, Any], *, policy: str, column_phrase: str = "") -> dict[str, str | bool]:
    """Operator copy for a manual-review schema block.

    A first mapping has no saved contract. Missing destination columns are
    ADD COLUMN proposals, not drift and not an acknowledgement. A later
    contract that no longer matches still says drift and can be acknowledged.
    """
    saved = bool((drift or {}).get("saved_contract", True))
    renames = [r for r in (drift or {}).get("auto_matched_renames") or [] if isinstance(r, dict)]
    if renames:
        pairs = ", ".join(
            f"{r.get('column')} → {r.get('to')} ({round(float(r.get('confidence') or 0) * 100)}% auto-match)"
            for r in renames[:6]
        )
        return {
            "gate_id": "schema_drift",
            "message": (
                f"Source column renamed: {pairs}. Policy '{policy}' needs your "
                "approval — open Map and approve or remap the column, or "
                "acknowledge drift for this run."
            ),
            "remediation_kind": "review_mappings",
            "ack_required": True,
            "rule_id": "schema_drift.rename",
        }
    if saved:
        return {
            "gate_id": "schema_drift",
            "message": (
                f"Schema change detected — policy '{policy}' requires review "
                "before Execute. Use Acknowledge drift for this run to keep "
                "the existing mappings, or open Map to include the changed columns."
                f"{column_phrase}"
            ),
            "remediation_kind": "acknowledge_schema_drift",
            "ack_required": True,
            "rule_id": "schema_drift.manual_review",
        }
    items = [
        item
        for item in ((drift or {}).get("schema_evolution") or {}).get("additive") or []
        if isinstance(item, dict)
    ]
    widens = [i for i in items if str(i.get("kind") or "") == "widen_type"]
    additive = [
        str(item.get("column") or item.get("source") or "")
        for item in items
        if item not in widens
    ]
    if widens and not any(additive):
        # A column the source widened is not a missing column (D191): name the
        # change and the one parameter that approves the ALTER.
        return {
            "gate_id": "schema_mismatch",
            "message": (
                "Source type widening needs approval: "
                + ", ".join(
                    f"{w.get('column') or w.get('source')} "
                    f"{w.get('old_type')} → {w.get('new_type')}"
                    for w in widens[:6]
                )
                + ". The destination column is narrower than the source until "
                "it is widened. Approve with schema_drift_acknowledged=true (or "
                "schema_policy=propagate_columns) and Execute ALTERs it before "
                "writing."
                f"{column_phrase}"
            ),
            "remediation_kind": "approve_type_widening",
            "ack_required": True,
            "rule_id": "schema_mismatch.type_widening",
        }
    names = ", ".join(name for name in additive if name) or "the missing columns"
    return {
        "gate_id": "schema_mismatch",
        "message": (
            "Source and destination schemas do not match. "
            f"Review ADD COLUMN proposals for {names} before Execute. "
            "Those columns stay unwritten until you add them. "
            "There is no saved mapping contract."
            f"{column_phrase}"
        ),
        "remediation_kind": "review_add_column",
        "ack_required": False,
        "rule_id": "schema_mismatch.add_column",
    }


from services.schema_drift_map_review import (  # noqa: E402
    approved_in_map,
    auto_matched_renames,
    rename_issue,
    strip_map_approved,
)


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
    schema_policy: str = "propagate_columns",
    table_exists: bool | None = None,
    declared_source_columns: list[str] | None = None,
    declared_source_schema: dict[str, str] | None = None,
    dest_recreated: bool = False,
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

    ``dest_recreated`` says this run drops the destination and recreates it from
    the source shape, which is what every overwrite sync does. The table
    standing there now is not the carrier the rows land in, so grading the
    source against it invents loss that cannot happen: ``TEXT → VARCHAR(64)``
    paused an overwrite whose own DDL creates ``LONGTEXT``. Existence reads as
    create-new here because that is what the run leaves behind.
    """
    source_schema = source_schema or {}
    target_columns = target_columns or []
    target_schema = target_schema or {}
    mappings = mappings or []
    if dest_recreated:
        target_columns = []
        target_schema = {}
        stored_target_fp = ""
        table_exists = False
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
    previously_fed_targets = {
        str(column).lower()
        for column in [
            *(previous_source_columns or []),
            *((previous_source_schema or {}).keys()),
        ]
    }
    if not live_ddl_contract:
        orphan_targets = []

    type_mismatches: list[dict[str, str]] = []
    risk_acknowledged: list[dict[str, str]] = []
    widen_proposals: list[dict[str, Any]] = []
    if live_ddl_contract:
        from services.coercion_probe import samples_coerce_mapping
        from services.migration_risk_contract import (
            mapping_has_clearing_risk_contract,
        )

        for m in active_mappings:
            src = str(m.get("source") or "")
            tgt = str(m.get("target") or "")
            if not src or not tgt:
                continue
            live_names = {str(c).lower() for c in target_columns}
            live_names.update(str(k).lower() for k in target_schema)
            # A mapped name the live table does not have is an ADD, judged
            # below. Grading it as a type change paused the run before the
            # operator could approve the column.
            if tgt.lower() not in live_names and tgt.lower() not in system_targets:
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
            from services.column_case import column_population

            population = column_population(sample_rows, src)
            if not is_lossy_coercion(
                src_type, tgt_type, dest_db=dest_db, population=population
            ):
                continue
            if decimal_capacity_is_equal_or_wider(
                str(src_type), str(tgt_type), dest_db=dest_db
            ):
                # Drift asks whether the destination carrier can still take the
                # source. A wider fixed-point sink can. The invented shape
                # (BigQuery bare BIGNUMERIC is 76,38) stays a fidelity/contract
                # chip, so pausing the sync here only re-reported it as loss.
                continue

            # A signed Migration Risk Contract is the operator's approval of
            # this exact pair. The fidelity gate
            # (services.type_coercion_validator) already honours it and stays
            # the authority on real loss, so drift must not pause a pair the
            # operator cleared — otherwise a contract-cleared dest-exists route
            # has no path to run at all. It is recorded, never dropped.
            if mapping_has_clearing_risk_contract(m):
                risk_acknowledged.append({
                    "source": src,
                    "target": tgt,
                    "source_type": src_type.upper(),
                    "target_type": tgt_type.upper(),
                    "reason": "risk_contract_signed",
                })
                continue

            # D107: a source that declared a strictly *wider* type than the
            # destination column is not a hard narrow_type — the destination
            # can be ALTERed up losslessly (Fivetran/Airbyte safe-change
            # widening). Classify it as additive widen_type so the evolution
            # policy can approve it and the writer's widen path carries it.
            # An explicit operator Map stamp is a ceiling — that pair stays a
            # real fidelity finding.
            from connectors.schema_drift import is_wider_type
            from connectors.writer_common import stamp_is_operator_ceiling

            if (
                is_wider_type(tgt_type, str(src_type), dest_db=dest_db)
                and normalize_logical_type(str(src_type))
                == normalize_logical_type(str(tgt_type))
                and not stamp_is_operator_ceiling(m)
            ):
                widen_proposals.append({
                    "kind": "widen_type",
                    "column": src,
                    "target": tgt,
                    "old_type": str(tgt_type).upper(),
                    "new_type": widened_dest_carrier(str(tgt_type), str(src_type)),
                    "reason": "dest_widen_available",
                })
                continue

            if is_precision_collapse_coercion(
                src_type, tgt_type, dest_db=dest_db, population=population
            ):
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
        from services.type_system import string_width_would_narrow

        classification = classification or {
            "additive": [],
            "breaking": [],
            "severity": "breaking",
            "renamed": [],
        }
        added_type_breaking = False
        for tm in type_mismatches:
            kind = (
                "narrow_type"
                if tm.get("reason") == "precision_collapse"
                else "type_change"
            )
            if (
                kind == "narrow_type"
                and string_width_would_narrow(
                    str(tm.get("source_type") or ""),
                    str(tm.get("target_type") or ""),
                )
                and not source_changed
                and not target_changed
            ):
                continue
            classification["breaking"].append({
                "kind": kind,
                "column": tm.get("source"),
                "old_type": tm.get("source_type"),
                "new_type": tm.get("target_type"),
                "target": tm.get("target"),
            })
            added_type_breaking = True
        if added_type_breaking:
            classification["severity"] = "breaking"

    # D107: destination-side widens approved by the ALTER path ride the same
    # additive lane as source-side widens — the evolution policy decides
    # (propagate auto-applies; manual_review asks; type_locked/pause_on_change
    # still pause).
    if widen_proposals:
        if not isinstance(classification, dict):
            classification = {
                "additive": [],
                "breaking": [],
                "severity": "none",
                "renamed": [],
            }
        additive = list(classification.get("additive") or [])
        have = {str(a.get("column") or "").lower() for a in additive}
        for prop in widen_proposals:
            if str(prop.get("column") or "").lower() not in have:
                additive.append(prop)
        classification["additive"] = additive
        if classification.get("severity") in (None, "", "none"):
            classification["severity"] = "additive"

    # Intentional subset maps (operator omitted columns) are not schema drift.
    # Only columns that appeared since the previous revision drive evolution.
    prev_lower = {str(c).lower() for c in prev_cols} | {str(c).lower() for c in prev_types}
    if prev_lower:
        evolution_unmapped = [c for c in unmapped_sources if c.lower() not in prev_lower]
    elif source_changed:
        evolution_unmapped = list(unmapped_sources)
    else:
        evolution_unmapped = []

    # QA MX3-17: a live destination column the source no longer feeds is a
    # drop, not a soft note — the policy decides (review / pause; propagate
    # keeps destination history).
    drop_targets = [
        column for column in orphan_targets if column.lower() in previously_fed_targets
    ]
    if live_ddl_contract and drop_targets:
        if not isinstance(classification, dict):
            classification = {
                "additive": [],
                "breaking": [],
                "severity": "none",
                "renamed": [],
            }
        breaking = list(classification.get("breaking") or [])
        named = {str(b.get("column") or "").lower() for b in breaking}
        target_types = {str(k).lower(): v for k, v in (target_schema or {}).items()}
        for col in drop_targets:
            if col.lower() in named:
                continue
            breaking.append({
                "kind": "drop",
                "column": col,
                "old_type": str(target_types.get(col.lower()) or "VARCHAR"),
                "reason": "destination_column_not_in_source",
            })
        classification["breaking"] = breaking
        classification["severity"] = "breaking"
        _logger.info(
            "schema drift: destination column(s) %s not in source — drop under policy %s",
            drop_targets,
            schema_policy,
        )

    # A mapped target that is not on the live table is an ADD, not a pass.
    # Preflight used to see no unmapped source and no type mismatch, then
    # INSERT named a column the relation does not have.
    if live_ddl_contract and active_mappings:
        known_targets = {str(c).lower() for c in target_columns}
        if not isinstance(classification, dict):
            classification = {
                "additive": [],
                "breaking": [],
                "severity": "none",
                "renamed": [],
            }
        additive = list(classification.get("additive") or [])
        present = {str(a.get("column") or "").lower() for a in additive}
        for mapping in active_mappings:
            src = str(mapping.get("source") or "").strip()
            tgt = str(mapping.get("target") or "").strip()
            if not src or not tgt:
                continue
            if tgt.lower() in known_targets or tgt.lower() in system_targets:
                continue
            if tgt.lower() in present:
                continue
            if not any(str(c).lower() == src.lower() for c in source_columns):
                continue
            typ = (
                source_schema.get(src)
                or next(
                    (source_schema[k] for k in source_schema if str(k).lower() == src.lower()),
                    None,
                )
                or "VARCHAR"
            )
            additive.append({
                "kind": "add_column",
                "column": tgt,
                "source": src,
                "new_type": str(typ),
                "nullable": True,
                "reason": "mapped_target_absent",
                # An approved create-new row in Map is the review manual_review
                # asks for; without it the gate re-derived the ADD and looped.
                "approved_in_map": approved_in_map(mapping),
            })
            present.add(tgt.lower())
        classification["additive"] = additive

    # A cross-name auto-match onto an existing column whose old name left the
    # source is a rename (dropped + new column), never a silent pass under a
    # policy that asks for manual approval (QA demo 1030: 82% semantic match).
    map_renames: list[dict[str, Any]] = []
    if live_ddl_contract and active_mappings and normalize_schema_policy(
        schema_policy
    ) in {"manual_review", "type_locked"}:
        map_renames = auto_matched_renames(
            active_mappings,
            source_columns=source_columns,
            target_columns=target_columns,
            source_schema=source_schema,
        )
        if map_renames:
            if not isinstance(classification, dict):
                classification = {
                    "additive": [], "breaking": [], "severity": "none", "renamed": [],
                }
            breaking = list(classification.get("breaking") or [])
            named = {str(b.get("column") or "").lower() for b in breaking}
            for item in map_renames:
                if str(item["column"]).lower() not in named:
                    breaking.append(item)
            classification["breaking"] = breaking
            classification["renamed"] = [
                *list(classification.get("renamed") or []),
                *[r for r in map_renames if not r.get("approved_in_map")],
            ]
            if any(not r.get("approved_in_map") for r in map_renames):
                classification["severity"] = "breaking"
    map_approved_changes = strip_map_approved(classification)

    # Destination-only columns the source never fed are not drift. A stored
    # fingerprint of the mapped columns alone used to call the extra destination
    # columns a target change, and manual_review then blocked a job that had
    # already succeeded (sample-orders.csv → QA_E2E_ds_mixed).
    dest_only_preserve = [
        column for column in orphan_targets
        if column.lower() not in previously_fed_targets
    ]
    if target_changed and dest_only_preserve and stored_target_fp:
        ignored = {column.lower() for column in dest_only_preserve}
        shared_cols = [c for c in target_columns if c.lower() not in ignored]
        shared_schema = {
            k: v
            for k, v in (target_schema or {}).items()
            if str(k).lower() not in ignored
        }
        if shared_cols and schemas_match(stored_target_fp, shared_cols, shared_schema):
            target_changed = False

    saved_contract = bool(
        stored_source_fp or stored_target_fp or previous_source_columns
    )
    evolution = resolve_schema_evolution(
        classification,
        schema_policy=schema_policy,
        unmapped_sources=evolution_unmapped,
        source_changed=source_changed or target_changed,
    )
    if not saved_contract and evolution.get("compatibility") == COMPAT_FORWARD:
        evolution["compatibility_note"] = (
            "Source and destination schemas differ. Review ADD COLUMN proposals "
            "for columns the destination does not have yet. There is no saved "
            "mapping contract."
        )
    non_breaking_audit: list[dict[str, Any]] = list(evolution.get("audit") or [])
    if map_approved_changes:
        non_breaking_audit.append({
            "decision": "approved_in_map",
            "changes": [
                {k: c.get(k) for k in ("kind", "column", "to", "source", "confidence") if c.get(k) is not None}
                for c in map_approved_changes
            ],
            "note": "The operator approved these schema changes in Map.",
        })
    if dest_only_preserve:
        non_breaking_audit.append({
            "decision": "auto_pass",
            "kind": "destination_only",
            "columns": list(dest_only_preserve),
            "note": (
                "Destination-only columns were never fed by the source. "
                "They stay off the write and do not block."
            ),
        })

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
    # A removed/renamed source column is surfaced by name — the destination
    # column keeps its history, so it is net-additive, not silent (D20).
    soft_drops = [
        str(c.get("column") or "")
        for c in (evolution.get("soft_net_additive") or [])
        if c.get("kind") == "drop" and c.get("column")
    ]
    if soft_drops:
        issues.append(
            f"Source column(s) removed: {', '.join(soft_drops[:8])} — "
            "destination column(s) keep their history and are not written"
        )
    pending_map_renames = [r for r in map_renames if not r.get("approved_in_map")]
    if pending_map_renames:
        issues.append(rename_issue(pending_map_renames))
    soft_renames = [
        (str(c.get("column") or ""), str(c.get("to") or ""))
        for c in (evolution.get("soft_net_additive") or [])
        if c.get("kind") == "rename" and c.get("column")
        and c.get("reason") != "auto_matched_rename"
    ]
    if soft_renames:
        pairs = ", ".join(f"{a}→{b}" for a, b in soft_renames[:8])
        issues.append(
            f"Source column(s) renamed: {pairs} — destination keeps the "
            "old column; the new name maps beside it"
        )
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
        "destination_only_columns": dest_only_preserve,
        "non_breaking_audit": non_breaking_audit,
        "type_mismatches": type_mismatches,
        "risk_acknowledged_pairs": risk_acknowledged,
        "intentional_omits": intentional_omits,
        "mapping_coverage": round(
            len({str(m.get("source")).lower() for m in active_mappings if m.get("source")})
            / max(len(source_columns), 1),
            3,
        ),
        "classification": classification,
        "schema_evolution": evolution,
        "auto_matched_renames": pending_map_renames,
        "approved_in_map": map_approved_changes,
        "saved_contract": saved_contract,
        "table_exists": table_exists,
        "create_new": create_new,
        "live_ddl_contract": live_ddl_contract,
    }
