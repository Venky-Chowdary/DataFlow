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
from dataclasses import dataclass, field
from typing import Any

from connectors.sql_identifiers import quote_sql_identifier
from services.dialect_profiles import quote_char_for
from services.foreign_key_metadata import (
    ForeignKey,
    ForeignKeys,
    foreign_keys_from_payload,
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
    referenced_table: str = ""
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


def _is_cycle_edge(dest_table: str, referenced_table: str, cycle_tables: set[str]) -> bool:
    """Self-ref or both ends in the detected cycle → deferred / post-load edge."""
    dest = (dest_table or "").strip().lower()
    ref = (referenced_table or "").strip().lower()
    if dest and ref and dest == ref:
        return True
    return bool(dest and ref and dest in cycle_tables and ref in cycle_tables)


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
        dest = str(d.get("dest_table") or "").strip()
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


def plan_foreign_keys(
    *,
    source_foreign_keys: Any,
    dest_dialect: str,
    dest_schema: str,
    dest_table: str,
    dest_columns: list[str],
    column_map: dict[str, str] | None = None,
    table_map: dict[str, str] | None = None,
    dest_existing_tables: set[str] | None = None,
    referenced_column_maps: dict[str, dict[str, str]] | None = None,
    cycle_tables: list[str] | set[str] | None = None,
) -> ForeignKeyPlan:
    """Plan the destination constraints for one child table.

    ``column_map`` maps source column → destination column for this table;
    ``referenced_column_maps`` maps each source table → its column map, so a
    renamed parent key is referenced under the name the load actually wrote.
    ``table_map`` maps source table → destination table for the tables this job
    moves. ``dest_existing_tables`` are the tables already present on the
    destination (lower-cased), used when the parent is not part of the job.
    ``None`` means the destination catalog could not be listed, which is
    ``unknown`` — never "the parent is missing".
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
        ref_dest = tmap.get(ref_source.lower(), "")
        in_job = bool(ref_dest)
        if not ref_dest:
            ref_dest = ref_source
        if not in_job:
            if known_tables is None:
                plan.decisions.append(
                    ForeignKeyDecision(
                        name=fk.name,
                        status="unknown",
                        reason=(
                            f"Referenced table '{ref_source}' is not part of this job "
                            "and the destination table list could not be read, so the "
                            "key is unverified rather than absent."
                        ),
                        source_detail=detail,
                        dest_table=dest_table,
                        referenced_table=ref_dest,
                    )
                )
                continue
            if ref_dest.lower() not in known_tables:
                plan.decisions.append(
                    ForeignKeyDecision(
                        name=fk.name,
                        status="unsupported",
                        reason=(
                            f"Referenced table '{ref_source}' is neither in this "
                            "transfer nor present on the destination — add it to the "
                            "stream selection, or create it first."
                        ),
                        source_detail=detail,
                        dest_table=dest_table,
                        referenced_table=ref_dest,
                    )
                )
                continue

        parent_cmap = _map_lookup(referenced_column_maps, fk.referenced_table)
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
                    referenced_table=ref_dest,
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
                    referenced_table=ref_dest,
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
                    referenced_table=ref_dest,
                )
            )
            continue

        name = _constraint_name(dest_table, fk, index)
        defer = (
            " DEFERRABLE INITIALLY DEFERRED"
            if dial in DEFERRABLE_DIALECTS
            and _is_cycle_edge(dest_table, ref_dest, cycle_set)
            else ""
        )
        statement = (
            f"ALTER TABLE {_qualified(dial, dest_schema, dest_table)} "
            f"ADD CONSTRAINT {_quote(dial, name)} FOREIGN KEY "
            f"({', '.join(_quote(dial, c) for c in child_cols)}) "
            f"REFERENCES {_qualified(dial, dest_schema, ref_dest)} "
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
                referenced_table=ref_dest,
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
                    referenced_table=decision.referenced_table,
                    columns=decision.columns,
                    referenced_columns=decision.referenced_columns,
                    integrity_violation=violation,
                )
            )
            continue
        out.append(decision)
    return out


def _signature(columns: tuple[str, ...] | list[str], table: str,
               referenced: tuple[str, ...] | list[str]) -> tuple:
    return (
        tuple(c.lower() for c in columns),
        table.lower(),
        tuple(c.lower() for c in referenced),
    )


def verify_foreign_keys(
    decisions: list[ForeignKeyDecision],
    dest_foreign_keys: ForeignKeys | None,
) -> list[ForeignKeyDecision]:
    """Settle planned keys against the destination catalog.

    Matching is structural — child columns, parent table, parent columns —
    because an engine may store the constraint under a name of its own, and a
    name comparison would report a carried key as missing.
    """
    out: list[ForeignKeyDecision] = []
    measured = dest_foreign_keys is not None and dest_foreign_keys.measured
    present = (
        {
            _signature(fk.columns, fk.referenced_table, fk.referenced_columns)
            for fk in dest_foreign_keys.items
        }
        if measured and dest_foreign_keys is not None
        else set()
    )
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
                    referenced_table=decision.referenced_table,
                    columns=decision.columns,
                    referenced_columns=decision.referenced_columns,
                )
            )
            continue
        signature = _signature(
            decision.columns, decision.referenced_table, decision.referenced_columns
        )
        carried = signature in present
        out.append(
            ForeignKeyDecision(
                name=decision.name,
                status="carried" if carried else "unsupported",
                reason=(
                    "Destination catalog reports the constraint, and the engine "
                    "validated the loaded rows when it was added."
                    if carried
                    else (
                        "Destination catalog does not report this reference after the "
                        "ALTER; the key is not enforced there."
                    )
                ),
                source_detail=decision.source_detail,
                dest_ddl=decision.dest_ddl,
                dest_table=decision.dest_table,
                referenced_table=decision.referenced_table,
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
