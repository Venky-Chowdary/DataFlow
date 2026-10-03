"""Carry FOREIGN KEYs onto the destination, and prove it from its catalog.

A foreign key is the one schema aspect that cannot be carried by a single-table
create: the parent must exist and hold the referenced rows first. Every serious
migration tool therefore splits it in two — create and load in dependency
order, then add the constraints — because ``ALTER TABLE ADD CONSTRAINT``
validates the rows that were just loaded. That validation is the point: if the
constraint takes, the destination data is provably free of orphans; if it is
rejected, the transfer found a referential-integrity defect that a row-count and
a checksum would both have reported as green.

So this module:

* plans one ``ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY`` per measured source
  key, translating child columns through the job's mapping and the referenced
  table through the job's stream→destination naming;
* refuses, with the reason and the named object, when the reference cannot be
  reproduced faithfully — never by quietly dropping the key;
* keeps an ``ALTER`` that the destination rejected separate from a dialect that
  cannot express the key at all: the first is an RI finding about the data, the
  second is a capability statement;
* certifies ``carried`` only after re-reading the destination catalog, matching
  constraints structurally rather than by name, because engines rename.

Ordering lives here too (:func:`order_tables_by_dependency`): parents before
children, so a destination that *already* enforces the keys accepts the load.

Cycles (A↔B, A→B→C→A) have no parents-first order. That is not a reason to
drop the keys. Create-new already lands tables without FKs; post-load
``ALTER`` is the portable deferred strategy (MySQL / SQL Server / PG / Oracle).
PostgreSQL and Oracle also emit ``DEFERRABLE INITIALLY DEFERRED`` on cycle
edges so a later same-transaction upsert can insert both sides. A cycle
blocks the certificate only when an edge was not ``carried`` — never because
a cycle was detected.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from connectors.sql_identifiers import quote_sql_identifier
from services.dialect_profiles import quote_char_for
from services.foreign_key_identity import fk_identity, fold, same_relationship, select_job_table
from services.foreign_key_metadata import (
    ForeignKey,
    ForeignKeys,
    foreign_keys_from_payload,
    normalize_action,
    row_proof_gap,
    row_proof_reason,
)

logger = logging.getLogger(__name__)

# Engines that can add a foreign key to an existing table. SQLite cannot: its
# only route is a table rebuild, which would rewrite rows we have just proven.
ALTER_CAPABLE = frozenset({
    "postgresql",
    "redshift",
    "mysql",
    "mariadb",
    "sqlserver",
    "mssql",
    "oracle",
})

# True deferred constraints (checked at COMMIT). MySQL / SQL Server have none —
# their deferred strategy is the same post-load ALTER as everyone else.
DEFERRABLE_DIALECTS = frozenset({"postgresql", "oracle"})

# Referential actions each dialect accepts in DDL. MySQL parses SET DEFAULT and
# then ignores it under InnoDB, and Oracle has no ON UPDATE clause at all, so
# both are reported rather than emitted as something the engine would not honour.
_NO_ON_UPDATE = frozenset({"oracle"})
_UNSUPPORTED_ACTIONS = {
    "mysql": frozenset({"SET DEFAULT"}),
    "mariadb": frozenset({"SET DEFAULT"}),
    "oracle": frozenset({"RESTRICT", "SET DEFAULT", "NO ACTION"}),
}


@dataclass(frozen=True)
class ForeignKeyDecision:
    """What happens to one source foreign key, and why."""

    name: str
    status: str  # planned | carried | unsupported | unknown | skipped
    reason: str
    source_detail: str = ""
    dest_ddl: str = ""
    # Destination objects the statement touches, so a caller can order work and
    # a re-read knows what to look at.
    dest_table: str = ""
    #: Selected source stream for this child. Empty on older decisions; the
    #: cycle check then uses ``dest_table``.
    source_table: str = ""
    referenced_schema: str = ""
    referenced_table: str = ""
    #: Selected source stream this key points at. Empty means the parent is
    #: outside the job, which is not a cycle edge even when the leaf name
    #: matches a stream in the cycle.
    referenced_stream: str = ""
    on_delete: str = ""
    on_update: str = ""
    columns: tuple[str, ...] = ()
    referenced_columns: tuple[str, ...] = ()
    # True when the destination *rejected* the constraint because the loaded
    # rows violate it. That is a data finding, not a capability gap.
    integrity_violation: bool = False


@dataclass
class ForeignKeyPlan:
    """Planned constraint DDL plus a decision for every source key."""

    statements: list[str] = field(default_factory=list)
    decisions: list[ForeignKeyDecision] = field(default_factory=list)

    @property
    def planned(self) -> list[ForeignKeyDecision]:
        return [d for d in self.decisions if d.status == "planned"]


def _dialect(name: str) -> str:
    key = (name or "").strip().lower()
    aliases = {"mssql": "sqlserver", "mariadb": "mysql", "psycopg2": "postgresql"}
    return aliases.get(key, key)


def _quote(dialect: str, identifier: str) -> str:
    """Quote through the canonical dialect profile, never a local guess."""
    return quote_sql_identifier(identifier, quote_char_for(_dialect(dialect)) or '"')


def _qualified(dialect: str, schema: str, table: str) -> str:
    if schema:
        return f"{_quote(dialect, schema)}.{_quote(dialect, table)}"
    return _quote(dialect, table)


def _describe(fk: ForeignKey) -> str:
    ref = fk.referenced_table or "?"
    if fk.referenced_schema:
        ref = f"{fk.referenced_schema}.{ref}"
    detail = (
        f"{fk.name or 'fk'}: ({', '.join(fk.columns)}) -> "
        f"{ref}({', '.join(fk.referenced_columns)})"
    )
    actions = [
        f"ON DELETE {fk.on_delete}" if fk.on_delete else "",
        f"ON UPDATE {fk.on_update}" if fk.on_update else "",
    ]
    suffix = " ".join(a for a in actions if a)
    return f"{detail} {suffix}".strip()


def _map_lookup(maps: dict[str, dict[str, str]] | None, table: str) -> dict[str, str]:
    """Case-insensitive table → column map. Missing table means identity."""
    if not maps or not table:
        return {}
    if table in maps:
        return maps[table]
    lower = table.lower()
    for key, value in maps.items():
        if key.lower() == lower:
            return value
    return {}


def _alias(name: str, cmap: dict[str, str], dest_cols_lower: dict[str, str]) -> str | None:
    """Source column → destination column.

    An empty mapping document is identity (the load wrote source names). An
    explicit map that omits the column is a refusal, unless the destination
    column list still contains that name — the operator kept it un-renamed.
    """
    if not name:
        return None
    if name in cmap:
        return cmap[name]
    lower = name.lower()
    for key, value in cmap.items():
        if key.lower() == lower:
            return value
    if dest_cols_lower:
        return dest_cols_lower.get(lower)
    if not cmap:
        return name
    return None


def _constraint_name(dest_table: str, fk: ForeignKey, index: int) -> str:
    """Derive the destination constraint name from the destination table.

    The source name is deliberately not reused. Constraint names are unique per
    *schema* on MySQL, SQL Server and Oracle, so copying it fails the ALTER for
    a reason that has nothing to do with the data as soon as the source table
    lives in the same schema — the common case for a same-server migration. The
    destination table name is unique in its schema, so table + key columns is a
    name no other constraint can already hold; the source name stays in the
    decision's ``source_detail`` for the audit trail.
    """
    columns = "_".join(fk.columns) or str(index)
    base = f"fk_{dest_table}_{columns}"
    base = "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in base)
    # Oracle's identifier limit is 30 bytes before 12.2 and 128 after; 30 is the
    # safe common denominator across every engine we emit to. Truncation can
    # collide, so a digest of the full name replaces the tail.
    if len(base) > 30:
        # Non-security: 6-hex suffix that disambiguates a truncated identifier.
        # usedforsecurity=False documents intent and clears the weak-hash gate
        # without changing the digest value.
        digest = hashlib.sha1(  # nosec B324 - identifier shortening, not security
            base.encode("utf-8"), usedforsecurity=False
        ).hexdigest()[:6]
        base = f"{base[:23]}_{digest}"
    return base


def referential_actions_match(
    planned_delete: str,
    planned_update: str,
    measured_delete: str,
    measured_update: str,
) -> bool:
    """True when the destination enforces the source ON DELETE and ON UPDATE.

    An unreported catalog action matches only the engine default (NO ACTION).
    CASCADE on one side and NO ACTION on the other is a different rule:
    CASCADE deletes rows the source would have kept, and NO ACTION keeps rows
    the source would have removed.
    """

    def _one(planned: str, measured: str) -> bool:
        want = normalize_action(planned) or "NO ACTION"
        got = normalize_action(measured)
        if not got:
            return want == "NO ACTION"
        return want == got

    return _one(planned_delete, measured_delete) and _one(planned_update, measured_update)


def _is_cycle_edge(child: str, parent_stream: str, cycle_tables: set[str]) -> bool:
    """Self-ref or both selected streams sit in the detected cycle.

    ``parent_stream`` is the stream :func:`resolve_parent_stream` chose. A
    leaf name is not enough: ``customers`` in the cycle is not
    ``archive.customers``.
    """
    child_l = fold(child)
    parent_l = fold(parent_stream)
    if child_l and parent_l and child_l == parent_l:
        return True
    return bool(child_l in cycle_tables and parent_l in cycle_tables)


def classify_cycle_resolution(
    cycle: list[str] | None,
    decisions: list[Any],
) -> dict[str, Any]:
    """Did post-load ALTER recreate every cycle edge?

    Detection alone is not a blocker. ``resolved`` is True only when every
    planned edge whose both ends sit in ``cycle`` settled ``carried``.
    Missing ``cycle_resolved`` on an old job stays fail-closed at the
    certificate (treated as unresolved).
    """
    names = [str(t).strip() for t in (cycle or []) if str(t).strip()]
    empty = {
        "cycle": names,
        "strategy": "n/a" if not names else "post_load_alter",
        "resolved": True,
        "unresolved": [],
        "edge_count": 0,
        "note": "No FK cycle in the selected streams.",
    }
    if not names:
        return empty
    cycle_l = {t.lower() for t in names}
    edges: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    for raw in decisions or []:
        d = raw if isinstance(raw, dict) else getattr(raw, "__dict__", {})
        if not isinstance(d, dict):
            continue
        dest = str(d.get("source_table") or d.get("dest_table") or "").strip()
        if "referenced_stream" in d:
            ref = str(d.get("referenced_stream") or "").strip()
        else:
            ref = str(d.get("referenced_table") or "").strip()
        if not dest or not ref:
            continue
        if dest.lower() not in cycle_l or ref.lower() not in cycle_l:
            continue
        edge = {
            "dest_table": dest,
            "referenced_table": ref,
            "status": str(d.get("status") or ""),
            "name": d.get("name") or "",
        }
        edges.append(edge)
        if edge["status"] != "carried":
            unresolved.append(edge)
    if not edges:
        return {
            "cycle": names,
            "strategy": "post_load_alter",
            "resolved": False,
            "unresolved": [{"reason": "no cycle edges were planned or carried"}],
            "edge_count": 0,
            "note": (
                "A cycle was detected but no cycle-edge constraint was planned — "
                "the destination does not enforce the cycle."
            ),
        }
    resolved = not unresolved
    return {
        "cycle": names,
        "strategy": "post_load_alter",
        "resolved": resolved,
        "unresolved": unresolved,
        "edge_count": len(edges),
        "note": (
            "Post-load ALTER recreated every cycle edge; the destination engine "
            "validated the loaded rows."
            if resolved
            else (
                "Post-load ALTER did not recreate every cycle edge — "
                "the cycle is not fully enforced on the destination."
            )
        ),
    }


def _action_clause(dialect: str, fk: ForeignKey) -> tuple[str, str]:
    """Return (clause, refusal_reason). Empty reason means the clause is safe."""
    dial = _dialect(dialect)
    unsupported = _UNSUPPORTED_ACTIONS.get(dial, frozenset())
    parts: list[str] = []
    if fk.on_delete:
        if fk.on_delete in unsupported and fk.on_delete != "NO ACTION":
            return "", (
                f"Source declares ON DELETE {fk.on_delete}, which {dial} does not "
                "enforce; carrying the key without it would weaken the source rule."
            )
        if fk.on_delete != "NO ACTION":
            parts.append(f"ON DELETE {fk.on_delete}")
    if fk.on_update and fk.on_update != "NO ACTION":
        if dial in _NO_ON_UPDATE:
            return "", (
                f"Source declares ON UPDATE {fk.on_update}; {dial} has no ON UPDATE "
                "clause, so the rule cannot be reproduced."
            )
        if fk.on_update in unsupported:
            return "", (
                f"Source declares ON UPDATE {fk.on_update}, which {dial} parses but "
                "does not enforce."
            )
        parts.append(f"ON UPDATE {fk.on_update}")
    return (" " + " ".join(parts) if parts else ""), ""


def parent_relation_schema(
    *,
    source_schema: str,
    dest_schema: str,
    in_job: bool,
) -> str:
    """Schema the ``REFERENCES`` clause names.

    A parent this job loads lands in ``dest_schema``. A parent the catalog
    placed in another schema stays in that schema. An unnamed source schema
    uses the destination schema, which is how engines that omit the default
    schema are written.
    """
    if in_job or not str(source_schema or "").strip():
        return dest_schema or ""
    if fold(source_schema) == fold(dest_schema):
        return dest_schema or source_schema
    return source_schema


def parent_table_on_destination(
    *,
    schema: str,
    table: str,
    job_schema: str,
    in_job: bool,
    job_tables: set[str] | None,
    tables_by_schema: Mapping[str, set[str] | None] | None,
) -> bool | None:
    """Whether the parent relation is on the destination.

    ``True`` present, ``False`` absent, ``None`` when the catalog list that
    would answer was not read. A leaf in the job schema is not a parent the
    source catalog placed in a different schema.
    """
    if in_job:
        return True
    source = fold(schema)
    job = fold(job_schema)
    leaf = fold(table)
    cross = bool(source and job and source != job)
    if cross:
        if tables_by_schema is None:
            return None
        bucket: set[str] | None = None
        seen = False
        for key, value in tables_by_schema.items():
            if fold(key) == source:
                bucket = value
                seen = True
                break
        if not seen or bucket is None:
            return None
        return leaf in {fold(name) for name in bucket}
    if job_tables is None:
        return None
    names = {fold(name) for name in job_tables}
    return leaf in names or bool(source and f"{source}.{leaf}" in names)


def resolve_parent_stream(
    fk: ForeignKey,
    table_map: Mapping[str, str] | None,
    source_schema: str,
) -> str | None:
    """Selected source stream this foreign key points at, or None outside the job.

    A known job schema uses :func:`select_job_table`, so ``archive.customers``
    is not the local ``customers`` stream. When the job schema was not
    measured, a single stream whose name is the parent leaf still matches.
    That is the default-schema stamp (``public.customers`` → stream
    ``customers``). Two qualified names are not guessed.
    """
    selected = [str(key) for key in (table_map or {})]
    found = select_job_table(
        fk.referenced_schema,
        fk.referenced_table,
        selected,
        job_schema=source_schema,
    )
    if found:
        return found
    if fold(source_schema):
        return None
    leaf = fold(fk.referenced_table)
    bare = [key for key in selected if fold(key) == leaf]
    if len(bare) == 1:
        return bare[0]
    return None


def plan_foreign_keys(
    *,
    source_foreign_keys: Any,
    dest_dialect: str,
    dest_schema: str,
    dest_table: str,
    dest_columns: list[str],
    source_table: str = "",
    source_schema: str = "",
    column_map: dict[str, str] | None = None,
    table_map: dict[str, str] | None = None,
    dest_existing_tables: set[str] | None = None,
    dest_tables_by_schema: Mapping[str, set[str] | None] | None = None,
    referenced_column_maps: dict[str, dict[str, str]] | None = None,
    cycle_tables: list[str] | set[str] | None = None,
) -> ForeignKeyPlan:
    """Plan the destination constraints for one child table.

    ``column_map`` maps source column → destination column for this table;
    ``referenced_column_maps`` maps each source table → its column map, so a
    renamed parent key is referenced under the name the load actually wrote.
    ``table_map`` maps source table → destination table for the tables this job
    moves. ``dest_existing_tables`` are the tables already present in the job
    schema. ``dest_tables_by_schema`` lists every other schema a source key
    names. A leaf in the job schema is not a parent that lives in another
    schema. ``None`` means that list was not read, which is ``unknown``.
    ``cycle_tables`` are members of a detected FK cycle (and self-refs are
    treated as cycle edges even when omitted): PostgreSQL/Oracle emit
    DEFERRABLE INITIALLY DEFERRED on those edges.
    """
    plan = ForeignKeyPlan()
    dial = _dialect(dest_dialect)
    payload = source_foreign_keys or {}
    status = (
        str(payload.get("status") or "").strip().lower()
        if isinstance(payload, dict)
        else ""
    )
    if isinstance(payload, dict) and status and status != "measured":
        plan.decisions.append(
            ForeignKeyDecision(
                name="*",
                status="unknown",
                reason=(
                    "Foreign key catalog was not readable on the source; referential "
                    "integrity is unmeasured, not absent. "
                    + str(payload.get("detail") or "")
                ).strip(),
                dest_table=dest_table,
            )
        )
        return plan

    keys = foreign_keys_from_payload(payload)
    if not keys:
        plan.decisions.append(
            ForeignKeyDecision(
                name="*",
                status="skipped",
                reason="Source table declares no foreign keys (measured).",
                dest_table=dest_table,
            )
        )
        return plan

    if dial not in ALTER_CAPABLE:
        for fk in keys:
            plan.decisions.append(
                ForeignKeyDecision(
                    name=fk.name,
                    status="unsupported",
                    reason=(
                        f"{dial or 'this destination'} cannot add a FOREIGN KEY to an "
                        "existing table; the only route is a table rebuild, which "
                        "would rewrite rows this run already proved."
                    ),
                    source_detail=_describe(fk),
                    dest_table=dest_table,
                )
            )
        return plan

    cmap = {str(k): str(v) for k, v in (column_map or {}).items()}
    tmap = {str(k).lower(): str(v) for k, v in (table_map or {}).items()}
    dest_cols_lower = {c.lower(): c for c in dest_columns}
    known_tables = (
        None if dest_existing_tables is None else {t.lower() for t in dest_existing_tables}
    )
    cycle_set = {str(t).lower() for t in (cycle_tables or []) if str(t).strip()}

    for index, fk in enumerate(keys):
        detail = _describe(fk)
        child_cols: list[str] = []
        missing: list[str] = []
        for col in fk.columns:
            mapped = _alias(col, cmap, dest_cols_lower)
            if mapped:
                child_cols.append(mapped)
            else:
                missing.append(col)
        if missing:
            plan.decisions.append(
                ForeignKeyDecision(
                    name=fk.name,
                    status="unsupported",
                    reason=(
                        "Key column(s) "
                        + ", ".join(missing)
                        + " carry no mapping into the destination, so the reference "
                        "cannot be reproduced."
                    ),
                    source_detail=detail,
                    dest_table=dest_table,
                )
            )
            continue

        ref_source = fk.referenced_table
        child_stream = source_table or dest_table
        parent_stream = resolve_parent_stream(fk, table_map, source_schema)
        in_job = parent_stream is not None
        ref_dest = tmap.get(fold(parent_stream), parent_stream) if parent_stream else ref_source
        parent_schema = parent_relation_schema(
            source_schema=fk.referenced_schema,
            dest_schema=dest_schema,
            in_job=in_job,
        )
        if not in_job:
            present = parent_table_on_destination(
                schema=fk.referenced_schema,
                table=ref_dest,
                job_schema=dest_schema,
                in_job=False,
                job_tables=known_tables,
                tables_by_schema=dest_tables_by_schema,
            )
            qualified = (
                f"{fk.referenced_schema}.{ref_source}"
                if fk.referenced_schema
                else ref_source
            )
            cross = bool(
                fold(fk.referenced_schema)
                and fold(dest_schema)
                and fold(fk.referenced_schema) != fold(dest_schema)
            )
            if present is None:
                reason = (
                    f"Referenced table '{qualified}' is not part of this job, and "
                    f"destination schema {fk.referenced_schema} was not listed. "
                    f"The table {ref_source} in schema {dest_schema or '(default)'} "
                    "is a different relation, so the key stays unverified."
                    if cross
                    else (
                        f"Referenced table '{ref_source}' is not part of this job "
                        "and the destination table list could not be read, so the "
                        "key is unverified rather than absent."
                    )
                )
                plan.decisions.append(
                    ForeignKeyDecision(
                        name=fk.name,
                        status="unknown",
                        reason=reason,
                        source_detail=detail,
                        dest_table=dest_table,
                        source_table=child_stream,
                        referenced_schema=parent_schema,
                        referenced_table=ref_dest,
                        referenced_stream="",
                    )
                )
                continue
            if not present:
                reason = (
                    f"Referenced table '{qualified}' is neither in this transfer "
                    f"nor present in destination schema {fk.referenced_schema}. "
                    f"A table named {ref_source} in schema {dest_schema} is a "
                    "different relation — add the parent to the stream selection, "
                    "or create it in its own schema first."
                    if cross
                    else (
                        f"Referenced table '{ref_source}' is neither in this "
                        "transfer nor present on the destination — add it to the "
                        "stream selection, or create it first."
                    )
                )
                plan.decisions.append(
                    ForeignKeyDecision(
                        name=fk.name,
                        status="unsupported",
                        reason=reason,
                        source_detail=detail,
                        dest_table=dest_table,
                        source_table=child_stream,
                        referenced_schema=parent_schema,
                        referenced_table=ref_dest,
                        referenced_stream="",
                    )
                )
                continue

        parent_cmap = _map_lookup(
            referenced_column_maps, parent_stream or fk.referenced_table
        )
        ref_cols: list[str] = []
        missing_ref: list[str] = []
        for col in fk.referenced_columns:
            mapped = _alias(col, parent_cmap, {})
            if mapped:
                ref_cols.append(mapped)
            else:
                missing_ref.append(col)
        if missing_ref:
            plan.decisions.append(
                ForeignKeyDecision(
                    name=fk.name,
                    status="unsupported",
                    reason=(
                        "Referenced column(s) "
                        + ", ".join(missing_ref)
                        + " were remapped off the parent, so the reference cannot "
                        "be reproduced."
                    ),
                    source_detail=detail,
                    dest_table=dest_table,
                    source_table=child_stream,
                    referenced_schema=parent_schema,
                    referenced_table=ref_dest,
                    referenced_stream=parent_stream or "",
                )
            )
            continue

        if not ref_cols or len(ref_cols) != len(child_cols):
            plan.decisions.append(
                ForeignKeyDecision(
                    name=fk.name,
                    status="unsupported",
                    reason=(
                        "Referenced column list is incomplete in the source catalog; "
                        "a partially known reference would enforce the wrong pairs."
                    ),
                    source_detail=detail,
                    dest_table=dest_table,
                    source_table=child_stream,
                    referenced_schema=parent_schema,
                    referenced_table=ref_dest,
                    referenced_stream=parent_stream or "",
                )
            )
            continue

        clause, refusal = _action_clause(dial, fk)
        if refusal:
            plan.decisions.append(
                ForeignKeyDecision(
                    name=fk.name,
                    status="unsupported",
                    reason=refusal,
                    source_detail=detail,
                    dest_table=dest_table,
                    source_table=child_stream,
                    referenced_schema=parent_schema,
                    referenced_table=ref_dest,
                    referenced_stream=parent_stream or "",
                )
            )
            continue

        name = _constraint_name(dest_table, fk, index)
        defer = (
            " DEFERRABLE INITIALLY DEFERRED"
            if dial in DEFERRABLE_DIALECTS
            and _is_cycle_edge(child_stream, parent_stream or "", cycle_set)
            else ""
        )
        statement = (
            f"ALTER TABLE {_qualified(dial, dest_schema, dest_table)} "
            f"ADD CONSTRAINT {_quote(dial, name)} FOREIGN KEY "
            f"({', '.join(_quote(dial, c) for c in child_cols)}) "
            f"REFERENCES {_qualified(dial, parent_schema, ref_dest)} "
            f"({', '.join(_quote(dial, c) for c in ref_cols)}){clause}{defer}"
        )
        plan.statements.append(statement)
        plan.decisions.append(
            ForeignKeyDecision(
                name=name,
                status="planned",
                reason=(
                    "Constraint is added after the load so the destination validates "
                    "the rows it just received."
                ),
                source_detail=detail,
                dest_ddl=statement,
                dest_table=dest_table,
                source_table=child_stream,
                referenced_schema=parent_schema,
                referenced_table=ref_dest,
                referenced_stream=parent_stream or "",
                on_delete=fk.on_delete,
                on_update=fk.on_update,
                columns=tuple(child_cols),
                referenced_columns=tuple(ref_cols),
            )
        )
    return plan


def _is_violation(error: str) -> bool:
    """Does this ALTER failure mean the loaded rows break the reference?

    Distinguishing this from "the engine cannot" is the whole value of adding
    constraints after the load: one is a data defect the operator must see, the
    other is a capability statement about the destination. Missing indexes
    (MySQL 1215) and missing parents (42P01) are capability/order defects, not
    orphans.
    """
    text = error.lower()
    markers = (
        "violat",  # PG 23503 / Oracle ORA-02298 wording
        "foreign key constraint fails",  # MySQL 1452
        "cannot add or update a child row",  # MariaDB 1452 wording
        "conflicted with the foreign key",  # SQL Server 547
        "ora-02298",  # parent keys not found
        "ora-02291",
        "1452",
        "23503",
    )
    return any(m in text for m in markers)


def _is_already_present(error: str) -> bool:
    """Has this constraint already been applied (resume, nested single-table carry)?

    The ALTER is idempotent: a duplicate-object rejection is not a failure, it
    is an invitation to re-read the destination catalog and certify.
    """
    text = error.lower()
    markers = (
        "already exists",
        "duplicate foreign key",
        "duplicate object",
        "duplicate constraint",
        "duplicate key on write or update",  # MariaDB 1005/121: FK index already there
        "there is already an object named",
        "42710",  # PostgreSQL duplicate_object
        "1826",  # MySQL duplicate foreign key constraint name
    )
    return any(m in text for m in markers)


def apply_foreign_keys(
    plan: ForeignKeyPlan,
    execute: Any,
) -> list[ForeignKeyDecision]:
    """Run each planned ALTER, recording per-key outcome. Never raises.

    ``execute`` takes one SQL string. A failure downgrades that one key and
    leaves the rest of the plan running: refusing every remaining constraint
    because one parent has orphans would hide the keys that are clean.
    """
    out: list[ForeignKeyDecision] = []
    for decision in plan.decisions:
        if decision.status != "planned":
            out.append(decision)
            continue
        try:
            execute(decision.dest_ddl)
        except Exception as exc:  # noqa: BLE001 — the failure is the finding
            message = f"{type(exc).__name__}: {exc}"
            if _is_already_present(message):
                out.append(decision)
                continue
            violation = _is_violation(message)
            out.append(
                ForeignKeyDecision(
                    name=decision.name,
                    status="unsupported",
                    reason=(
                        "Destination rejected the constraint because the loaded rows "
                        f"violate it — orphan child rows exist. {message}"
                        if violation
                        else f"Destination rejected the constraint. {message}"
                    ),
                    source_detail=decision.source_detail,
                    dest_ddl=decision.dest_ddl,
                    dest_table=decision.dest_table,
                    source_table=decision.source_table,
                    referenced_schema=decision.referenced_schema,
                    referenced_table=decision.referenced_table,
                    referenced_stream=decision.referenced_stream,
                    on_delete=decision.on_delete,
                    on_update=decision.on_update,
                    columns=decision.columns,
                    referenced_columns=decision.referenced_columns,
                    integrity_violation=violation,
                )
            )
            continue
        out.append(decision)
    return out


def _relationship_fact(
    columns: tuple[str, ...] | list[str],
    schema: str,
    table: str,
    referenced: tuple[str, ...] | list[str],
) -> dict[str, Any]:
    return {
        "constrained_columns": list(columns),
        "referred_schema": schema,
        "referred_table": table,
        "referred_columns": list(referenced),
    }


def verify_foreign_keys(
    decisions: list[ForeignKeyDecision],
    dest_foreign_keys: ForeignKeys | None,
) -> list[ForeignKeyDecision]:
    """Settle planned keys against the destination catalog.

    Matching uses :func:`services.foreign_key_identity.same_relationship`:
    the parent relation, including schema, plus the set of column pairs.
    An engine may store the constraint under a name of its own, and DDL
    order is the same relationship. A same-named table in another schema
    is not. ON DELETE and ON UPDATE must be the source rule; a different
    action on the same columns is not carried. A catalog bit that says
    existing rows were not checked is not carried either.
    """
    out: list[ForeignKeyDecision] = []
    measured = dest_foreign_keys is not None and dest_foreign_keys.measured
    present = list(dest_foreign_keys.items) if measured and dest_foreign_keys is not None else []
    for decision in decisions:
        if decision.status != "planned":
            out.append(decision)
            continue
        if not measured:
            detail = (dest_foreign_keys.detail if dest_foreign_keys else "") or (
                "destination catalog not read"
            )
            out.append(
                ForeignKeyDecision(
                    name=decision.name,
                    status="unknown",
                    reason=(
                        "The ALTER was issued, but the destination foreign key "
                        f"catalog could not be re-read ({detail}), so the carry is "
                        "unverified — emitted DDL is not proof."
                    ),
                    source_detail=decision.source_detail,
                    dest_ddl=decision.dest_ddl,
                    dest_table=decision.dest_table,
                    source_table=decision.source_table,
                    referenced_schema=decision.referenced_schema,
                    referenced_table=decision.referenced_table,
                    referenced_stream=decision.referenced_stream,
                    on_delete=decision.on_delete,
                    on_update=decision.on_update,
                    columns=decision.columns,
                    referenced_columns=decision.referenced_columns,
                )
            )
            continue
        wanted = fk_identity(
            _relationship_fact(
                decision.columns,
                decision.referenced_schema,
                decision.referenced_table,
                decision.referenced_columns,
            )
        )
        matches = [
            fk
            for fk in present
            if same_relationship(
                wanted,
                fk_identity(
                    _relationship_fact(
                        fk.columns,
                        fk.referenced_schema,
                        fk.referenced_table,
                        fk.referenced_columns,
                    )
                ),
            )
        ]
        faithful = [
            fk
            for fk in matches
            if referential_actions_match(
                decision.on_delete, decision.on_update, fk.on_delete, fk.on_update
            )
        ]
        dest_dialect = dest_foreign_keys.dialect if dest_foreign_keys else ""
        covering = [
            fk for fk in faithful if row_proof_gap(dest_dialect, fk.validated) == ""
        ]
        if covering:
            status = "carried"
            if any(fk.validated is True for fk in covering):
                reason = (
                    "Destination catalog reports the constraint, and it records "
                    "that existing rows were checked."
                )
            else:
                reason = (
                    "Destination catalog reports the constraint, and the engine "
                    "validated the loaded rows when it was added."
                )
        elif faithful:
            gap = row_proof_gap(dest_dialect, faithful[0].validated)
            status = "unsupported"
            reason = row_proof_reason(gap) or row_proof_reason("not_checked")
        elif matches:
            got = matches[0]
            status = "unsupported"
            reason = (
                "Destination has this relationship with "
                f"ON DELETE {normalize_action(got.on_delete) or 'unreported'} "
                f"ON UPDATE {normalize_action(got.on_update) or 'unreported'}; "
                "the source rule is "
                f"ON DELETE {normalize_action(decision.on_delete) or 'NO ACTION'} "
                f"ON UPDATE {normalize_action(decision.on_update) or 'NO ACTION'}. "
                "A different referential action is not the source rule."
            )
        else:
            status = "unsupported"
            reason = (
                "Destination catalog does not report this reference after the "
                "ALTER; the key is not enforced there."
            )
        out.append(
            ForeignKeyDecision(
                name=decision.name,
                status=status,
                reason=reason,
                source_detail=decision.source_detail,
                dest_ddl=decision.dest_ddl,
                dest_table=decision.dest_table,
                source_table=decision.source_table,
                referenced_schema=decision.referenced_schema,
                referenced_table=decision.referenced_table,
                referenced_stream=decision.referenced_stream,
                on_delete=decision.on_delete,
                on_update=decision.on_update,
                columns=decision.columns,
                referenced_columns=decision.referenced_columns,
            )
        )
    return out


def order_tables_by_dependency(
    tables: list[str],
    dependencies: dict[str, set[str]],
) -> tuple[list[str], list[str]]:
    """Return (ordered tables, cycle members): parents before children.

    ``dependencies[child]`` is the set of tables that child references. Only
    edges between tables in ``tables`` are considered — a reference outside the
    job cannot be ordered and is handled by the planner instead.

    A cycle (mutual references, self-references) has no valid order; those
    tables keep their declared order and are returned so the caller can say so
    rather than pretending an order exists.
    """
    names = [t for t in tables]
    index = {t.lower(): i for i, t in enumerate(names)}
    remaining = {
        t.lower(): {
            d.lower()
            for d in dependencies.get(t, set()) | dependencies.get(t.lower(), set())
            if d.lower() in index and d.lower() != t.lower()
        }
        for t in names
    }
    ordered: list[str] = []
    done: set[str] = set()
    while remaining:
        ready = [t for t, deps in remaining.items() if not (deps - done)]
        if not ready:
            break
        # Stable: keep the operator's declared order among equally ready tables.
        ready.sort(key=lambda t: index[t])
        for table in ready:
            ordered.append(names[index[table]])
            done.add(table)
            del remaining[table]
    cycle = [names[index[t]] for t in sorted(remaining, key=lambda t: index[t])]
    ordered.extend(cycle)
    return ordered, cycle
