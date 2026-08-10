"""Independent source/destination physical-state comparison.

A migration can move every row and still hand the client a broken database:
the primary key never made it, a unique constraint was dropped, a foreign key
is missing, an index the application depends on was never created, a NOT NULL
became nullable, or a column default was lost. None of that is visible to a
row-level checksum, so it is read here from the *catalog* — on a connection of
this module's own, never from writer bookkeeping.

Every aspect answers one of four honest states:

``carried``       present on both sides
``absent``        present on the source, missing on the destination
``extra``         present on the destination only (informational, never a pass)
``unreadable``    the catalog could not be read — never counted as carried

Aspects an engine cannot express (e.g. SQLite has no ALTER-able FK catalog on
some builds) come back ``unreadable`` with a reason rather than silently green.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

import sqlalchemy as sa

logger = logging.getLogger(__name__)

__all__ = [
    "PhysicalState",
    "read_physical_state",
    "compare_physical_state",
    "verify_physical_state",
    "resolve_stored_name",
]

# Aspects this module compares. Ordered as an operator reads a migration report.
ASPECTS: tuple[str, ...] = (
    "primary_key",
    "unique_constraints",
    "foreign_keys",
    "indexes",
    "not_null",
    "defaults",
    "check_constraints",
)

# Reported for the operator but never blocking: trigger bodies are procedural
# code in a dialect the destination may not even speak, so "not carried" is the
# expected outcome of a cross-engine move, not a defect the mover can fix.
ADVISORY_ASPECTS: tuple[str, ...] = ("triggers",)

# Longest first: "instead of" also contains no other timing, but "before each
# row" and "after insert" must not be reduced to the wrong token.
_TRIGGER_TIMINGS: tuple[str, ...] = ("instead of", "before", "after")
_TRIGGER_EVENTS: tuple[str, ...] = ("insert", "update", "delete")

# Parentheses an engine wraps around a lone identifier when it stores a CHECK.
_BARE_PARENS = re.compile(r"\(([a-z0-9_$#.]+)\)")

# Reflection hands back the dialect's own spelling of a name (SQLAlchemy folds
# Oracle's stored CHK_SRC to chk_src), so every catalog lookup compares folded.
_TRIGGER_SQL: dict[str, str] = {
    "postgresql": (
        "SELECT action_timing, event_manipulation FROM information_schema.triggers "
        "WHERE lower(event_object_table) = lower(:t) "
        "AND (:s = '' OR lower(event_object_schema) = lower(:s))"
    ),
    "mysql": (
        "SELECT action_timing, event_manipulation FROM information_schema.triggers "
        "WHERE lower(event_object_table) = lower(:t) "
        "AND (:s = '' OR lower(event_object_schema) = lower(:s))"
    ),
    "mssql": (
        "SELECT CASE WHEN OBJECTPROPERTY(tr.object_id, 'ExecIsInsteadOfTrigger') = 1 "
        "THEN 'INSTEAD OF' ELSE 'AFTER' END, te.type_desc "
        "FROM sys.triggers tr "
        "JOIN sys.trigger_events te ON te.object_id = tr.object_id "
        "WHERE lower(OBJECT_NAME(tr.parent_id)) = lower(:t) "
        "AND (:s = '' OR lower(OBJECT_SCHEMA_NAME(tr.parent_id)) = lower(:s))"
    ),
    "oracle": (
        "SELECT trigger_type, triggering_event FROM all_triggers "
        "WHERE upper(table_name) = upper(:t) "
        "AND (:s = '' OR upper(owner) = upper(:s))"
    ),
    "sqlite": (
        "SELECT sql, '' FROM sqlite_master "
        "WHERE type = 'trigger' AND lower(tbl_name) = lower(:t)"
    ),
}


_CHECK_SQL: dict[str, str] = {
    "mssql": (
        "SELECT cc.definition FROM sys.check_constraints cc "
        "WHERE lower(OBJECT_NAME(cc.parent_object_id)) = lower(:t) "
        "AND (:s = '' OR lower(OBJECT_SCHEMA_NAME(cc.parent_object_id)) = lower(:s))"
    ),
}


@dataclass(frozen=True)
class PhysicalState:
    """Catalog facts for one table, normalized for cross-engine comparison."""

    readable: bool = False
    found: bool = False
    reason: str = ""
    primary_key: tuple[str, ...] = ()
    unique_constraints: frozenset[tuple[str, ...]] = frozenset()
    foreign_keys: frozenset[tuple[str, ...]] = frozenset()
    indexes: frozenset[tuple[str, ...]] = frozenset()
    not_null: frozenset[str] = frozenset()
    defaults: frozenset[str] = frozenset()
    check_constraints: frozenset[str] = frozenset()
    triggers: frozenset[tuple[str, ...]] = frozenset()
    errors: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "readable": self.readable,
            "found": self.found,
            "reason": self.reason,
            "primary_key": list(self.primary_key),
            "unique_constraints": sorted("+".join(u) for u in self.unique_constraints),
            "foreign_keys": sorted("->".join(f) for f in self.foreign_keys),
            "indexes": sorted("+".join(i) for i in self.indexes),
            "not_null": sorted(self.not_null),
            "defaults": sorted(self.defaults),
            "check_constraints": sorted(self.check_constraints),
            "triggers": sorted(" ".join(t) for t in self.triggers),
            "errors": list(self.errors),
        }


@dataclass
class _Collector:
    """Partial reflection: what was read, and what refused to be read."""

    errors: list[str] = field(default_factory=list)

    def run(self, aspect: str, fn: Any) -> Any:
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 — a refused catalog is evidence
            logger.warning("physical state: %s unreadable: %s", aspect, exc)
            self.errors.append(f"{aspect}: {exc}")
            return None


def _fold(name: Any) -> str:
    """Identifier key for cross-engine comparison (Oracle folds upper, PG lower)."""
    return str(name or "").strip().casefold()


def _cols(values: Any) -> tuple[str, ...]:
    if not values:
        return ()
    return tuple(_fold(v) for v in values if str(v or "").strip())


def read_physical_state(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str = "",
    table: str,
) -> PhysicalState:
    """Reflect constraints, indexes, nullability and defaults from the catalog."""
    if not table:
        return PhysicalState(reason="no table name to inspect")

    from connectors.generic_sql import get_sqlalchemy_engine

    try:
        engine = get_sqlalchemy_engine({**cfg, "type": db_type})
    except Exception as exc:  # noqa: BLE001
        return PhysicalState(reason=f"cannot connect: {exc}")

    collector = _Collector()
    with engine.connect() as conn:
        inspector = sa.inspect(conn)
        args: dict[str, Any] = {"schema": schema or None}
        # Oracle and other upper-folding catalogs only match the stored spelling.
        name = _resolve_table_name(inspector, table, schema or None)
        if name is None:
            return PhysicalState(
                reason=f"table {schema + '.' if schema else ''}{table} not found in catalog"
            )

        pk = collector.run("primary_key", lambda: inspector.get_pk_constraint(name, **args))
        uniques = collector.run(
            "unique_constraints",
            lambda: _unique_constraints(inspector, name, args),
        )
        fks = collector.run("foreign_keys", lambda: inspector.get_foreign_keys(name, **args))
        indexes = collector.run("indexes", lambda: inspector.get_indexes(name, **args))
        columns = collector.run("columns", lambda: inspector.get_columns(name, **args))
        checks = collector.run(
            "check_constraints",
            lambda: _check_constraints(inspector, conn, db_type, name, args, schema),
        )
        triggers = collector.run(
            "triggers", lambda: _read_triggers(conn, db_type, name, schema)
        )

    not_null: set[str] = set()
    defaults: set[str] = set()
    for col in columns or []:
        col_name = _fold(col.get("name"))
        if not col_name:
            continue
        if col.get("nullable") is False:
            not_null.add(col_name)
        if col.get("default") is not None or col.get("computed") or col.get("identity"):
            defaults.add(col_name)

    unique_sets = {
        _cols(u.get("column_names")) for u in uniques or [] if u.get("column_names")
    }
    fk_sets = {
        (
            "+".join(_cols(f.get("constrained_columns"))),
            _fold(f.get("referred_table")),
            "+".join(_cols(f.get("referred_columns"))),
        )
        for f in fks or []
        if f.get("constrained_columns")
    }
    index_sets = {
        _cols(i.get("column_names")) for i in indexes or [] if i.get("column_names")
    }

    return PhysicalState(
        readable=not collector.errors,
        found=True,
        reason="" if not collector.errors else "partial catalog read",
        primary_key=_cols((pk or {}).get("constrained_columns")),
        unique_constraints=frozenset(unique_sets),
        foreign_keys=frozenset(fk_sets),
        indexes=frozenset(index_sets),
        not_null=frozenset(not_null),
        defaults=frozenset(defaults),
        check_constraints=frozenset(
            _normalize_predicate(c.get("sqltext"))
            for c in checks or []
            if _normalize_predicate(c.get("sqltext"))
        ),
        triggers=frozenset(triggers or ()),
        errors=tuple(collector.errors),
    )


def _normalize_predicate(sqltext: Any) -> str:
    """Strip the dialect's punctuation so ``("qty" > 0)`` and ``qty>0`` match.

    Two engines never spell the same CHECK identically; comparing raw text would
    report every constraint as missing. Whitespace, quoting styles and wrapping
    parentheses carry no meaning, so they go.
    """
    text = str(sqltext or "").strip().casefold()
    if not text:
        return ""
    for ch in ('"', "`", "[", "]", " ", "\t", "\n", "\r"):
        text = text.replace(ch, "")
    while text.startswith("(") and text.endswith(")"):
        text = text[1:-1]
    # ``("qty")>0`` and ``qty>0`` are the same rule; parentheses around a bare
    # identifier are the engine's own echo, not part of the predicate.
    text = _BARE_PARENS.sub(r"\1", text)
    # Oracle reflects every NOT NULL as a CHECK; the not_null aspect owns those,
    # and counting them here would report a phantom loss on every other engine.
    if text.endswith("isnotnull"):
        return ""
    return text


def _check_constraints(
    inspector: Any,
    conn: Any,
    db_type: str,
    table: str,
    args: dict[str, Any],
    schema: str,
) -> list[dict[str, Any]]:
    """CHECK predicates, from the catalog directly when reflection has no driver.

    SQLAlchemy's pyodbc dialect raises ``NotImplementedError`` here, and an
    unread CHECK would otherwise be indistinguishable from a dropped one.
    """
    try:
        return list(inspector.get_check_constraints(table, **args))
    except NotImplementedError:
        sql = _CHECK_SQL.get(str(db_type or "").strip().casefold())
        if not sql:
            raise
        rows = conn.execute(sa.text(sql), {"t": table, "s": schema or ""}).fetchall()
        return [{"sqltext": row[0]} for row in rows]


def _first_token(text: str, tokens: tuple[str, ...]) -> str:
    """Earliest token wins: a SQLite trigger body may mention other events."""
    hits = [(text.find(t), t) for t in tokens if t in text]
    return min(hits)[1] if hits else ""


def _trigger_behaviour(timing: Any, event: Any) -> tuple[str, str]:
    """Reduce each dialect's phrasing to (timing, event).

    Oracle says ``BEFORE EACH ROW``, SQLite hands back the whole CREATE
    statement, SQL Server names the event ``INSERT``; the portable fact is the
    same pair, so extract it rather than compare dialect prose.
    """
    text = f"{_fold(timing)} {_fold(event)}"
    return _first_token(text, _TRIGGER_TIMINGS), _first_token(text, _TRIGGER_EVENTS)


def _read_triggers(
    conn: Any, db_type: str, table: str, schema: str
) -> list[tuple[str, ...]]:
    """Trigger timing/event pairs; names are never portable, behaviour is."""
    sql = _TRIGGER_SQL.get(str(db_type or "").strip().casefold())
    if not sql:
        raise NotImplementedError(f"no trigger catalog query for {db_type}")
    params = {"t": table, "s": schema or ""}
    rows = conn.execute(sa.text(sql), params).fetchall()
    return [_trigger_behaviour(row[0], row[1]) for row in rows]


def _unique_constraints(inspector: Any, name: str, args: dict[str, Any]) -> list[dict]:
    """Unique constraints, falling back to unique indexes.

    SQL Server has no separate unique-constraint reflection in SQLAlchemy; it
    enforces uniqueness through a unique index, so an index-derived answer is
    the same guarantee and beats reporting the aspect unreadable.
    """
    try:
        return list(inspector.get_unique_constraints(name, **args))
    except NotImplementedError:
        return [
            {"column_names": idx.get("column_names")}
            for idx in inspector.get_indexes(name, **args)
            if idx.get("unique") and idx.get("column_names")
        ]


def resolve_stored_name(candidates: Iterable[str], wanted: str) -> str | None:
    """The catalog's own spelling of ``wanted``, or None when it is ambiguous.

    Folding a name to the engine's default case is a guess: Oracle and SQL
    Server happily store a quoted lowercase ``id`` that ``ID`` will never
    match. Only an exact or single case-insensitive hit is safe.
    """
    names = list(candidates)
    exact = [n for n in names if n == wanted]
    if exact:
        # The catalog's own object, not the caller's copy: SQLAlchemy's
        # ``quoted_name`` carries case-sensitivity that a plain str drops.
        return exact[0]
    folded = _fold(wanted)
    hits = [n for n in names if _fold(n) == folded]
    return hits[0] if len(hits) == 1 else None


def _resolve_table_name(inspector: Any, table: str, schema: str | None) -> str | None:
    """Stored spelling of ``table`` in this catalog, or None when absent."""
    return resolve_stored_name(inspector.get_table_names(schema=schema), table)


def _diff_sets(source: frozenset, dest: frozenset) -> dict[str, Any]:
    missing = sorted(_render(v) for v in source - dest)
    extra = sorted(_render(v) for v in dest - source)
    return {
        "status": "carried" if not missing else "absent",
        "missing": missing,
        "extra": extra,
        "source_count": len(source),
        "destination_count": len(dest),
    }


def _render(value: Any) -> str:
    if isinstance(value, tuple):
        return "->".join(v for v in value if v) if len(value) == 3 else "+".join(value)
    return str(value)


def compare_physical_state(
    source: PhysicalState, destination: PhysicalState
) -> dict[str, Any]:
    """Per-aspect verdict, fail-closed when either catalog could not be read."""
    for side, state in (("source", source), ("destination", destination)):
        if state.found:
            continue
        return {
            "verified": False,
            "reason": state.reason or f"{side} catalog unreadable",
            "source": source.to_dict(),
            "destination": destination.to_dict(),
        }

    aspects: dict[str, Any] = {
        "primary_key": _diff_sets(
            frozenset({source.primary_key} if source.primary_key else set()),
            frozenset({destination.primary_key} if destination.primary_key else set()),
        ),
        "unique_constraints": _diff_sets(
            source.unique_constraints, destination.unique_constraints
        ),
        "foreign_keys": _diff_sets(source.foreign_keys, destination.foreign_keys),
        "indexes": _diff_sets(source.indexes, destination.indexes),
        "not_null": _diff_sets(source.not_null, destination.not_null),
        "defaults": _diff_sets(source.defaults, destination.defaults),
        "check_constraints": _diff_sets(
            source.check_constraints, destination.check_constraints
        ),
    }
    advisory = {
        "triggers": {
            **_diff_sets(source.triggers, destination.triggers),
            "advisory": True,
            "note": (
                "Trigger bodies are not migrated; recreate them on the "
                "destination before cutover if the application relies on them."
            ),
        }
    }
    # A partial read cannot certify the aspects it failed on.
    unreadable = sorted(
        {e.split(":", 1)[0] for e in (*source.errors, *destination.errors)}
    )
    for aspect in unreadable:
        if aspect in aspects:
            aspects[aspect]["status"] = "unreadable"
        if aspect in advisory:
            advisory[aspect]["status"] = "unreadable"
    absent = [a for a, v in aspects.items() if v["status"] == "absent"]
    blocking_unreadable = [a for a in unreadable if a not in advisory]
    return {
        "verified": not absent and not blocking_unreadable,
        "aspects": {**aspects, **advisory},
        "absent": absent,
        "unreadable": blocking_unreadable,
        "advisory": {
            a: v["status"] for a, v in advisory.items() if v["status"] != "carried"
        },
        "source": source.to_dict(),
        "destination": destination.to_dict(),
    }


def verify_physical_state(
    *,
    source_db_type: str,
    source_cfg: dict[str, Any],
    source_schema: str = "",
    source_table: str,
    dest_db_type: str,
    dest_cfg: dict[str, Any],
    dest_schema: str = "",
    dest_table: str,
) -> dict[str, Any]:
    """Read both catalogs independently and report what survived the move."""
    src = read_physical_state(
        source_db_type, source_cfg, schema=source_schema, table=source_table
    )
    dst = read_physical_state(
        dest_db_type, dest_cfg, schema=dest_schema, table=dest_table
    )
    return compare_physical_state(src, dst)
