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
    "catalog_table_names",
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

# Reported for the operator but never blocking: trigger bodies and view SQL
# are dialect-specific and are not migrated. "Not carried" is the expected
# outcome of a cross-engine move — the certificate must *name* the objects
# so cutover recreates them, never pretend they were absent on the source.
ADVISORY_ASPECTS: tuple[str, ...] = ("triggers", "views", "routines")

_DIALECT_ALIASES = {
    "postgres": "postgresql",
    "mariadb": "mysql",
    "sqlserver": "mssql",
    "oracledb": "oracle",
}

# Longest first: "instead of" also contains no other timing, but "before each
# row" and "after insert" must not be reduced to the wrong token.
_TRIGGER_TIMINGS: tuple[str, ...] = ("instead of", "before", "after")
_TRIGGER_EVENTS: tuple[str, ...] = ("insert", "update", "delete")

# Parentheses an engine wraps around a lone identifier when it stores a CHECK.
_BARE_PARENS = re.compile(r"\(([a-z0-9_$#.]+)\)")

# ``::text``, ``::character varying(16)``, ``::public.my_domain`` — PostgreSQL
# records the cast it applied; the predicate is the same rule without it. Only
# words that continue a *type* name may be consumed: swallowing a bare word
# would eat the ``and`` of ``x::text and y`` and change the predicate.
_TYPE_TAIL_WORDS = "varying|precision|without|with|time|zone|local"
_CAST_SUFFIX = re.compile(
    rf"::\s*[a-z0-9_$#.]+(?:\s+(?:{_TYPE_TAIL_WORDS}))*(?:\s*\([^)]*\))?"
)

# ``_utf8mb4'x'`` — MySQL records the charset it resolved for a literal.
_CHARSET_INTRODUCER = re.compile(r"_[a-z0-9]+(?=')")

# Reflection hands back the dialect's own spelling of a name (SQLAlchemy folds
# Oracle's stored CHK_SRC to chk_src), so every catalog lookup compares folded.
_TRIGGER_SQL: dict[str, str] = {
    "postgresql": (
        "SELECT trigger_name, action_timing, event_manipulation "
        "FROM information_schema.triggers "
        "WHERE lower(event_object_table) = lower(:t) "
        "AND (:s = '' OR lower(event_object_schema) = lower(:s))"
    ),
    "mysql": (
        "SELECT trigger_name, action_timing, event_manipulation "
        "FROM information_schema.triggers "
        "WHERE lower(event_object_table) = lower(:t) "
        "AND (:s = '' OR lower(event_object_schema) = lower(:s))"
    ),
    "mssql": (
        "SELECT tr.name, "
        "CASE WHEN OBJECTPROPERTY(tr.object_id, 'ExecIsInsteadOfTrigger') = 1 "
        "THEN 'INSTEAD OF' ELSE 'AFTER' END, te.type_desc "
        "FROM sys.triggers tr "
        "JOIN sys.trigger_events te ON te.object_id = tr.object_id "
        "WHERE lower(OBJECT_NAME(tr.parent_id)) = lower(:t) "
        "AND (:s = '' OR lower(OBJECT_SCHEMA_NAME(tr.parent_id)) = lower(:s))"
    ),
    "oracle": (
        "SELECT trigger_name, trigger_type, triggering_event FROM all_triggers "
        "WHERE upper(table_name) = upper(:t) "
        "AND (:s = '' OR upper(owner) = upper(:s))"
    ),
    "sqlite": (
        "SELECT name, sql, '' FROM sqlite_master "
        "WHERE type = 'trigger' AND lower(tbl_name) = lower(:t)"
    ),
}

# Views / matviews that *depend on* the transferred table. Name presence only —
# body SQL is never compared and never emitted.
_VIEW_SQL: dict[str, str] = {
    "postgresql": (
        "SELECT DISTINCT view_name FROM information_schema.view_table_usage "
        "WHERE lower(table_name) = lower(:t) "
        "AND (:s = '' OR lower(table_schema) = lower(:s))"
    ),
    "mysql": (
        "SELECT DISTINCT table_name FROM information_schema.view_table_usage "
        "WHERE lower(table_name) = lower(:t) "
        "AND lower(table_schema) = lower(IFNULL(NULLIF(:s, ''), DATABASE()))"
    ),
    "mssql": (
        "SELECT DISTINCT v.name "
        "FROM sys.sql_expression_dependencies d "
        "JOIN sys.views v ON v.object_id = d.referencing_id "
        "WHERE lower(OBJECT_NAME(d.referenced_id)) = lower(:t) "
        "AND (:s = '' OR lower(OBJECT_SCHEMA_NAME(d.referenced_id)) = lower(:s))"
    ),
    "oracle": (
        "SELECT DISTINCT name FROM all_dependencies "
        "WHERE type IN ('VIEW', 'MATERIALIZED VIEW') "
        "AND referenced_type = 'TABLE' "
        "AND upper(referenced_name) = upper(:t) "
        "AND (:s = '' OR upper(referenced_owner) = upper(:s))"
    ),
    "sqlite": (
        "SELECT name, sql FROM sqlite_master WHERE type = 'view'"
    ),
}

# Procedures / functions that depend on the transferred table. Name only —
# body SQL is never compared and never emitted. Trigger functions are excluded
# so the trigger already listed under ``triggers`` is not double-counted.
#
# PostgreSQL SQL-language functions record ``pg_depend``; PL/pgSQL usually
# does not. Body identifier match (same algorithm as MySQL / SQLite views)
# is therefore the primary scan; ``pg_depend`` is a second source so a
# C-language or internal function that the catalog links still appears.
_ROUTINE_SQL: dict[str, str] = {
    "postgresql": (
        "SELECT p.proname, p.prosrc "
        "FROM pg_proc p "
        "JOIN pg_namespace pn ON pn.oid = p.pronamespace "
        "WHERE p.prorettype <> 'trigger'::regtype "
        "AND p.prokind IN ('f', 'p') "
        "AND (:s = '' OR lower(pn.nspname) = lower(:s))"
    ),
    "mysql": (
        "SELECT routine_name, routine_definition, routine_type "
        "FROM information_schema.routines "
        "WHERE lower(routine_schema) = lower(IFNULL(NULLIF(:s, ''), DATABASE())) "
        "AND routine_type IN ('PROCEDURE', 'FUNCTION')"
    ),
    "mssql": (
        "SELECT DISTINCT o.name "
        "FROM sys.sql_expression_dependencies d "
        "JOIN sys.objects o ON o.object_id = d.referencing_id "
        "WHERE o.type IN ('P', 'FN', 'IF', 'TF') "
        "AND lower(OBJECT_NAME(d.referenced_id)) = lower(:t) "
        "AND (:s = '' OR lower(OBJECT_SCHEMA_NAME(d.referenced_id)) = lower(:s))"
    ),
    "oracle": (
        "SELECT DISTINCT name FROM all_dependencies "
        "WHERE type IN ('PROCEDURE', 'FUNCTION', 'PACKAGE', 'PACKAGE BODY') "
        "AND referenced_type = 'TABLE' "
        "AND upper(referenced_name) = upper(:t) "
        "AND (:s = '' OR upper(referenced_owner) = upper(:s))"
    ),
}

_ROUTINE_DEPEND_SQL: dict[str, str] = {
    "postgresql": (
        "SELECT DISTINCT p.proname "
        "FROM pg_proc p "
        "JOIN pg_depend d ON d.classid = 'pg_proc'::regclass AND d.objid = p.oid "
        "JOIN pg_class t ON t.oid = d.refobjid "
        "JOIN pg_namespace n ON n.oid = t.relnamespace "
        "WHERE t.relkind IN ('r', 'p', 'f') "
        "AND lower(t.relname) = lower(:t) "
        "AND (:s = '' OR lower(n.nspname) = lower(:s)) "
        "AND p.prorettype <> 'trigger'::regtype "
        "AND p.prokind IN ('f', 'p')"
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
    views: frozenset[str] = frozenset()
    routines: frozenset[str] = frozenset()
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
            "triggers": sorted(_render_trigger(t) for t in self.triggers),
            "views": sorted(self.views),
            "routines": sorted(self.routines),
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


def _catalog_dialect(db_type: str) -> str:
    key = str(db_type or "").strip().casefold()
    return _DIALECT_ALIASES.get(key, key)


def _has_catalog_supplied_value(col: Any) -> bool:
    """Does the catalog supply this column's value when the writer sends none?

    Each engine records its generator in its own place: PostgreSQL as an identity
    or a ``nextval`` column default, MySQL as AUTO_INCREMENT with *no* default at
    all. Reading only ``default`` therefore reported a faithfully carried
    generator as a dropped default on every PostgreSQL→MySQL move. The counter's
    own health (next value past the migrated maximum) is proven separately by
    ``services.identity_watermark``; this aspect answers only whether the
    destination still fills the column in for the application.
    """
    return bool(
        col.get("default") is not None
        or col.get("computed")
        or col.get("identity")
        or col.get("autoincrement") is True
    )


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
        views = collector.run(
            "views", lambda: _read_dependent_views(conn, db_type, name, schema)
        )
        routines = collector.run(
            "routines", lambda: _read_dependent_routines(conn, db_type, name, schema)
        )

    not_null: set[str] = set()
    defaults: set[str] = set()
    for col in columns or []:
        col_name = _fold(col.get("name"))
        if not col_name:
            continue
        if col.get("nullable") is False:
            not_null.add(col_name)
        if _has_catalog_supplied_value(col):
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
        views=frozenset(views or ()),
        routines=frozenset(routines or ()),
        errors=tuple(collector.errors),
    )


def _strip_outer_parens(text: str) -> str:
    """Remove a single *matching* outermost paren pair, never a false wrapper.

    ``(qty>0)`` -> ``qty>0`` but ``(a>0)or(b>0)`` is left intact (its first ``(``
    closes mid-string, so the outer parens are not a wrapper)."""
    while len(text) >= 2 and text[0] == "(" and text[-1] == ")":
        depth = 0
        wraps = True
        for i, ch in enumerate(text):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0 and i != len(text) - 1:
                    wraps = False
                    break
        if wraps:
            text = text[1:-1]
        else:
            break
    return text


def _normalize_predicate(sqltext: Any) -> str:
    """Strip the dialect's punctuation so ``("qty" > 0)`` and ``qty>0`` match.

    Two engines never spell the same CHECK identically; comparing raw text would
    report every constraint as missing. Whitespace, quoting styles and wrapping
    parentheses carry no meaning, so they go.
    """
    raw = str(sqltext or "").strip().casefold()
    if not raw:
        return ""
    # Single pass: keep string-literal CONTENTS verbatim (so ``<> 'a'`` and
    # ``<> 'b'`` stay distinct and real CHECK drift is not hidden), but neutralize
    # parentheses *inside* literals to sentinels so a ``)`` in a literal cannot
    # skew the paren balancer. Insignificant punctuation outside literals is
    # stripped. ``'` -> ``\x01`` open / ``\x02`` close sentinels are consistent on
    # both sides, so equivalent predicates still compare equal.
    chars: list[str] = []
    i = 0
    n = len(raw)
    in_str = False
    while i < n:
        ch = raw[i]
        if in_str:
            if ch == "'":
                if i + 1 < n and raw[i + 1] == "'":
                    chars.append("''")
                    i += 2
                    continue
                chars.append("'")
                in_str = False
                i += 1
                continue
            if ch == "(":
                chars.append("\x01")
            elif ch == ")":
                chars.append("\x02")
            else:
                chars.append(ch)
            i += 1
            continue
        if ch == "'":
            chars.append("'")
            in_str = True
            i += 1
            continue
        # A cast is how one engine writes the type it already knows: PostgreSQL
        # stores ``status::text <> ''::text`` for the CHECK MySQL stores as
        # ``status <> ''``. The rule is identical, so the cast carries no meaning
        # here and comparing it reports a phantom dropped constraint.
        cast = _CAST_SUFFIX.match(raw, i)
        if cast is not None:
            i = cast.end()
            continue
        # MySQL prefixes a literal with the charset it resolved (``_utf8mb4''``).
        intro = _CHARSET_INTRODUCER.match(raw, i)
        if intro is not None:
            i = intro.end()
            continue
        if ch in '"`[] \t\n\r':
            i += 1
            continue
        chars.append(ch)
        i += 1
    text = "".join(chars)
    if not text:
        return ""
    # Balance stray parens (unmatched trailing ``)`` from the CREATE TABLE tail on
    # some SQLite/ODBC reflections); literal parens are sentinels and excluded.
    while text.endswith(")") and text.count(")") > text.count("("):
        text = text[:-1]
    while text.startswith("(") and text.count("(") > text.count(")"):
        text = text[1:]
    text = _strip_outer_parens(text)
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


def _render_trigger(value: tuple[str, ...]) -> str:
    """``name (after insert)`` — the object the operator recreates."""
    if not value:
        return ""
    if len(value) >= 3:
        name, timing, event = value[0], value[1], value[2]
        behave = " ".join(part for part in (timing, event) if part)
        return f"{name} ({behave})" if behave else name
    return " ".join(part for part in value if part)


def _read_triggers(
    conn: Any, db_type: str, table: str, schema: str
) -> list[tuple[str, ...]]:
    """Named trigger + portable (timing, event). Body SQL is not compared."""
    sql = _TRIGGER_SQL.get(_catalog_dialect(db_type))
    if not sql:
        raise NotImplementedError(f"no trigger catalog query for {db_type}")
    params = {"t": table, "s": schema or ""}
    rows = conn.execute(sa.text(sql), params).fetchall()
    out: list[tuple[str, ...]] = []
    for row in rows:
        name = _fold(row[0])
        timing, event = _trigger_behaviour(row[1], row[2] if len(row) > 2 else "")
        if name:
            out.append((name, timing, event))
    return out


def _sqlite_view_depends(sql: str, table: str) -> bool:
    """Identifier match, not a substring of another name."""
    folded_sql = _fold(sql)
    folded_table = _fold(table)
    if not folded_table or not folded_sql:
        return False
    token = re.compile(rf"(?<![a-z0-9_]){re.escape(folded_table)}(?![a-z0-9_])")
    return bool(token.search(folded_sql))


def _read_dependent_views(
    conn: Any, db_type: str, table: str, schema: str
) -> list[str]:
    """View / matview names that depend on ``table``. Body SQL is not read."""
    dialect = _catalog_dialect(db_type)
    sql = _VIEW_SQL.get(dialect)
    if not sql:
        raise NotImplementedError(f"no view catalog query for {db_type}")
    params = {"t": table, "s": schema or ""}
    try:
        rows = conn.execute(sa.text(sql), params).fetchall()
    except Exception:
        if dialect != "mysql":
            raise
        rows = conn.execute(
            sa.text(
                "SELECT table_name, view_definition FROM information_schema.views "
                "WHERE lower(table_schema) = lower(IFNULL(NULLIF(:s, ''), DATABASE()))"
            ),
            params,
        ).fetchall()
        names = []
        for row in rows:
            name, definition = row[0], row[1] if len(row) > 1 else ""
            if _sqlite_view_depends(str(definition or ""), table):
                folded = _fold(name)
                if folded:
                    names.append(folded)
        return names
    names: list[str] = []
    for row in rows:
        if dialect == "sqlite":
            name, view_sql = row[0], row[1] if len(row) > 1 else ""
            if not _sqlite_view_depends(str(view_sql or ""), table):
                continue
        else:
            name = row[0]
        folded = _fold(name)
        if folded:
            names.append(folded)
    return names


def _read_dependent_routines(
    conn: Any, db_type: str, table: str, schema: str
) -> list[str]:
    """Procedure / function names that depend on ``table``. Body SQL is not compared.

    SQLite has no stored routines — an empty list is measured absence, not
    an unreadable catalog.
    """
    dialect = _catalog_dialect(db_type)
    if dialect == "sqlite":
        return []
    sql = _ROUTINE_SQL.get(dialect)
    if not sql:
        raise NotImplementedError(f"no routine catalog query for {db_type}")
    params = {"t": table, "s": schema or ""}
    rows = conn.execute(sa.text(sql), params).fetchall()
    names: list[str] = []
    seen: set[str] = set()
    body_match = dialect in {"mysql", "postgresql"}
    for row in rows:
        if body_match:
            name, definition = row[0], row[1] if len(row) > 1 else ""
            if not _sqlite_view_depends(str(definition or ""), table):
                continue
        else:
            name = row[0]
        folded = _fold(name)
        if folded and folded not in seen:
            seen.add(folded)
            names.append(folded)
    depend_sql = _ROUTINE_DEPEND_SQL.get(dialect)
    if depend_sql:
        for row in conn.execute(sa.text(depend_sql), params).fetchall():
            folded = _fold(row[0])
            if folded and folded not in seen:
                seen.add(folded)
                names.append(folded)
    return names


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


def catalog_table_names(
    inspector: Any,
    schema: str | None,
    *,
    conn: Any = None,
    dialect: str = "",
) -> list[str]:
    """Table names in ``schema``, with an Oracle catalog fallback.

    SQLAlchemy's Oracle inspector hides every table stored in the SYSTEM /
    SYSAUX tablespaces, so a destination that lives there reflects as absent and
    each consumer reports its aspect unverifiable instead of reading it. The
    catalog itself is the authority when the inspector returns nothing.
    """
    try:
        names = [str(n) for n in inspector.get_table_names(schema=schema or None)]
    except Exception as exc:  # noqa: BLE001 — an unreadable catalog is evidence
        logger.debug("table listing failed for schema %s: %s", schema, exc)
        names = []
    if names or conn is None or (dialect or "").strip().lower() not in {
        "oracle",
        "oracledb",
    }:
        return names
    try:
        rows = conn.execute(
            sa.text(
                "SELECT table_name FROM all_tables "
                "WHERE owner = COALESCE(NULLIF(:own, ''), USER)"
            ),
            {"own": (schema or "").upper()},
        ).fetchall()
    except Exception as exc:  # noqa: BLE001
        logger.debug("oracle table catalog fallback failed: %s", exc)
        return names
    # Hand back the spelling the inspector would have produced: SQLAlchemy
    # reflects Oracle by its normalized (lower-case) name and reads a raw
    # catalog ``IDDST_X`` as a case-sensitive quoted identifier that no table
    # matches.
    normalize = getattr(conn.dialect, "normalize_name", None)
    return [
        str(normalize(str(r[0])) if callable(normalize) else r[0]) for r in (rows or [])
    ]


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
        if len(value) == 3 and (
            value[1] in _TRIGGER_TIMINGS or value[2] in _TRIGGER_EVENTS
        ):
            return _render_trigger(value)
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
        "triggers": _advisory_trigger_diff(source.triggers, destination.triggers),
        "views": {
            **_diff_sets(source.views, destination.views),
            "advisory": True,
            "note": (
                "Dependent views / materialized views are not created by table "
                "transfer. Name presence only — SQL body is not compared. "
                "Recreate them on the destination before cutover."
            ),
        },
        "routines": {
            **_diff_sets(source.routines, destination.routines),
            "advisory": True,
            "note": (
                "Stored procedures and functions that depend on this table are "
                "not migrated. Name presence only — body SQL is not compared. "
                "Recreate them on the destination before cutover."
            ),
        },
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
        "cutover_recreate": _cutover_recreate(advisory),
        "source": source.to_dict(),
        "destination": destination.to_dict(),
    }


def _trigger_behavior_key(value: tuple[str, ...]) -> tuple[str, ...]:
    """Portable (timing, event) — names are per-table and not required to match."""
    if len(value) >= 3:
        return (value[1], value[2])
    return tuple(value)


def _advisory_trigger_diff(
    source: frozenset[tuple[str, ...]],
    destination: frozenset[tuple[str, ...]],
) -> dict[str, Any]:
    """Behaviour class decides status; names are what cutover recreates."""
    src_behave = frozenset(_trigger_behavior_key(t) for t in source)
    dst_behave = frozenset(_trigger_behavior_key(t) for t in destination)
    diff = _diff_sets(src_behave, dst_behave)
    src_names = sorted({t[0] for t in source if t})
    dst_names = sorted({t[0] for t in destination if t})
    if diff["status"] == "absent":
        diff["missing"] = [_render_trigger(t) for t in sorted(source)]
    return {
        **diff,
        "source_names": src_names,
        "destination_names": dst_names,
        "advisory": True,
        "note": (
            "Trigger bodies are not migrated; recreate the named triggers "
            "on the destination before cutover if the application relies on them."
        ),
    }


def _cutover_recreate(advisory: dict[str, Any]) -> list[dict[str, str]]:
    """Named objects the mover did not create — recreate before cutover."""
    items: list[dict[str, str]] = []
    for aspect, info in advisory.items():
        if not isinstance(info, dict) or info.get("status") == "carried":
            continue
        kind = {
            "views": "view",
            "triggers": "trigger",
            "routines": "routine",
        }.get(aspect, aspect)
        for name in info.get("missing") or []:
            items.append(
                {
                    "kind": kind,
                    "name": str(name),
                    "action": "recreate_before_cutover",
                }
            )
        if not info.get("missing") and info.get("status") == "unreadable":
            items.append(
                {
                    "kind": kind,
                    "name": "*",
                    "action": "catalog_unreadable",
                }
            )
    return items


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
