"""Independent source/destination physical-state comparison.

A migration can move every row and still hand the client a broken database:
the primary key never made it, a unique constraint was dropped, a foreign key
is missing, an index the application depends on was never created, a NOT NULL
became nullable, or a column default was lost or rewritten. None of that is
visible to a row-level checksum, so it is read here from the *catalog* — on a
connection of this module's own, never from writer bookkeeping.

Every aspect answers one of these honest states:

``carried``       present on both sides. A measured foreign key, primary
                  key, or unique constraint also proves the engine checks rows.
                  When both catalogs named a foreign-key match type, carried
                  also means the destination rule keeps the source promise.
                  Unreported match is MATCH SIMPLE. An empty match list means
                  this comparison did not measure match. When both catalogs
                  named ON DELETE and ON UPDATE, carried also means those
                  actions keep the source rule. An empty action list means
                  this comparison did not measure them. Unreported is NO ACTION.
                  When both catalogs named a deferral mode, carried also means
                  the destination checks at the same time as the source.
                  An empty deferral list means this comparison did not measure
                  it. Unreported is NOT DEFERRABLE. A cycle load that adds
                  DEFERRABLE on the destination does not keep a source rule
                  that checks at the statement.
``absent``        present on the source, missing on the destination
``unchecked``     the object is present and does not prove the rows, or the
                  foreign-key match rule or referential action does not keep
                  the source promise
``extra``         present on the destination only (informational, never a pass)
``unreadable``    the catalog could not be read — never counted as carried

Aspects an engine cannot express (e.g. SQLite has no ALTER-able FK catalog on
some builds) come back ``unreadable`` with a reason rather than silently green.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, NamedTuple

import sqlalchemy as sa

from services.fk_tuple_scan import match_rule_disagreement, normalize_match
from services.foreign_key_identity import (
    fk_identity,
    foreign_key_wire,
    parse_foreign_key,
    render_foreign_key_fact,
    same_relationship,
)

logger = logging.getLogger(__name__)

__all__ = [
    "PhysicalState",
    "read_physical_state",
    "compare_physical_state",
    "verify_physical_state",
    "resolve_stored_name",
    "catalog_table_names",
    "decode_foreign_key_item",
    "foreign_keys_from_catalog_state",
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
        "SELECT DISTINCT view_name FROM information_schema.view_table_usage "
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
        "SELECT cc.definition, cc.is_disabled, cc.is_not_trusted "
        "FROM sys.check_constraints cc "
        "WHERE lower(OBJECT_NAME(cc.parent_object_id)) = lower(:t) "
        "AND (:s = '' OR lower(OBJECT_SCHEMA_NAME(cc.parent_object_id)) = lower(:s))"
    ),
}


def _reported_table_kind(value: str) -> str:
    """Operator spelling of a measured Snowflake table kind."""
    from services.foreign_key_metadata import normalize_snowflake_table_kind

    return normalize_snowflake_table_kind(value) if value else ""


@dataclass(frozen=True)
class PhysicalState:
    """Catalog facts for one table, normalized for cross-engine comparison."""

    readable: bool = False
    found: bool = False
    reason: str = ""
    primary_key: tuple[str, ...] = ()
    unique_constraints: frozenset[tuple[str, ...]] = frozenset()
    foreign_keys: frozenset[tuple[str, ...]] = frozenset()
    #: Structured relationships. Catalog diff and the orphan scan both compare
    #: these with :func:`services.foreign_key_identity.same_relationship`.
    #: ``foreign_keys`` is the rendered report wire. Each fact is
    #: ``(child columns, schema, table, parent columns)``.
    foreign_key_facts: tuple[
        tuple[tuple[str, ...], str, str, tuple[str, ...]], ...
    ] = ()
    #: Row-proof gap for each fact, same order as ``foreign_key_facts``.
    #: Empty string means this catalog fact proves existing rows. ``unenforced``,
    #: ``not_checked``, and ``unreported`` mean the relationship object may be
    #: present and is not that proof. An empty tuple means this read did not
    #: attach a gap; the diff then compares relationship identity only.
    foreign_key_proof: tuple[str, ...] = ()
    #: Match type for each fact, same order as ``foreign_key_facts``.
    #: ``""`` is unreported (MATCH SIMPLE when a scan runs). ``simple``,
    #: ``full``, ``partial``, and ``unknown`` are :func:`normalize_match`.
    #: An empty tuple means this read did not attach a match type. The
    #: relationship identity does not include it: ``MATCH FULL`` and
    #: ``MATCH SIMPLE`` are one relationship with two rules.
    foreign_key_match: tuple[str, ...] = ()
    #: ON DELETE and ON UPDATE for each fact, same order as
    #: ``foreign_key_facts``. Empty string is unreported (NO ACTION when the
    #: actions are compared). An empty tuple means this read did not attach
    #: actions. The relationship identity does not include them: CASCADE and
    #: NO ACTION are one relationship with two rules.
    foreign_key_on_delete: tuple[str, ...] = ()
    foreign_key_on_update: tuple[str, ...] = ()
    #: Deferral mode for each fact, same order as ``foreign_key_facts``.
    #: ``""`` is unreported (NOT DEFERRABLE when the modes are compared).
    #: ``not_deferrable``, ``immediate``, ``deferred``, and ``unknown`` are
    #: :func:`services.foreign_key_metadata.normalize_deferral`. An empty
    #: tuple means this read did not attach a mode. The relationship
    #: identity does not include it.
    foreign_key_deferral: tuple[str, ...] = ()
    #: Ordered key, uniqueness, predicate, covering columns, and access method.
    #: See :class:`CatalogIndex`. A unique, partial, covering, or gin index is
    #: not the plain column list.
    indexes: frozenset[CatalogIndex] = frozenset()
    not_null: frozenset[str] = frozenset()
    #: ``(column, normalized expression)``. A sequence or identity with no
    #: literal is the fill-in sentinel, not a second copy of some other default.
    defaults: frozenset[tuple[str, str]] = frozenset()
    check_constraints: frozenset[str] = frozenset()
    triggers: frozenset[tuple[str, ...]] = frozenset()
    views: frozenset[str] = frozenset()
    routines: frozenset[str] = frozenset()
    errors: tuple[str, ...] = ()
    #: Engine this catalog was read from. Foreign-key and uniqueness sentences
    #: name this engine. Empty when the caller built the state by hand.
    dialect: str = ""
    #: Measured Snowflake table kind. ``hybrid`` or ``standard`` when
    #: ``INFORMATION_SCHEMA.TABLES.IS_HYBRID`` answered. Empty when this
    #: read did not ask, or the catalog did not say. A dialect name is not
    #: this field.
    table_kind: str = ""
    #: Summarized ``SHOW INDEXES`` status for a Snowflake table.
    #: ``active`` only when every reported index is ``ACTIVE``. Empty when
    #: this read did not ask, or the command did not return a status.
    #: ``INFORMATION_SCHEMA`` enforcement is not this field.
    index_status: str = ""
    #: ``SHOW INDEXES.status_info`` for the worst index. Empty when the
    #: command did not return that cell, or every index is ``ACTIVE``.
    index_detail: str = ""
    #: Existing-row gap for each primary-key or unique column set.
    #: ``(folded columns, gap)``. Oracle ``""`` is ``VALIDATED`` and
    #: ``not_checked`` is ``NOT VALIDATED``. SQL Server ``""`` is an enabled
    #: unique index and ``not_checked`` is ``is_disabled = 1``. PostgreSQL
    #: ``""`` is ``indisvalid``. ``not_checked`` is an invalid index that
    #: still rejects a new row. ``not_ready`` is ``indisready`` false.
    #: ``unreported`` means this read did not see the bit. An empty tuple
    #: means this comparison did not measure it. A live read attaches one
    #: entry for each reflected key.
    uniqueness_proof: tuple[tuple[tuple[str, ...], str], ...] = ()
    #: Existing-row gap for each normalized CHECK predicate.
    #: ``(predicate, gap)``. PostgreSQL ``""`` means ``pg_get_constraintdef``
    #: did not say ``NOT VALID``. ``not_checked`` means it did. New rows are
    #: still rejected. SQL Server ``""`` is enabled and trusted.
    #: ``not_checked`` is ``is_not_trusted`` on an enabled check.
    #: ``disabled`` is ``is_disabled``. Oracle ``""`` is ``ENABLED`` and
    #: ``VALIDATED``. ``not_checked`` is ``ENABLED`` and ``NOT VALIDATED``.
    #: ``disabled`` is ``STATUS`` ``DISABLED``. ``unreported`` means this
    #: read did not see the flag. An empty tuple means this comparison did
    #: not measure it.
    check_proof: tuple[tuple[str, str], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        from services.foreign_key_metadata import (
            normalize_snowflake_index_status,
            snowflake_index_detail,
        )

        return {
            "readable": self.readable,
            "found": self.found,
            "reason": self.reason,
            "dialect": self.dialect,
            "table_kind": _reported_table_kind(self.table_kind),
            "index_status": normalize_snowflake_index_status(self.index_status),
            "index_detail": snowflake_index_detail(self.index_detail),
            "uniqueness_proof": [
                {"columns": list(columns), "gap": gap}
                for columns, gap in self.uniqueness_proof
            ],
            "primary_key": list(self.primary_key),
            "unique_constraints": sorted("+".join(u) for u in self.unique_constraints),
            "foreign_keys": sorted("->".join(f) for f in self.foreign_keys),
            "foreign_key_facts": [
                _foreign_key_fact_dict(
                    child,
                    schema,
                    table,
                    parent,
                    self._proof_at(index),
                    measured=bool(self.foreign_key_proof),
                    match=self._match_at(index),
                    actions=self._actions_at(index),
                    deferral=self._deferral_at(index),
                )
                for index, (child, schema, table, parent) in enumerate(
                    self.foreign_key_facts
                )
            ],
            "indexes": sorted(_render_index(i) for i in self.indexes),
            "not_null": sorted(self.not_null),
            "defaults": sorted(
                _render_default(column, expr) for column, expr in self.defaults
            ),
            "check_constraints": sorted(self.check_constraints),
            "check_proof": [
                {"predicate": predicate, "gap": gap}
                for predicate, gap in self.check_proof
            ],
            "triggers": sorted(_render_trigger(t) for t in self.triggers),
            "views": sorted(self.views),
            "routines": sorted(self.routines),
            "errors": list(self.errors),
        }

    def _proof_at(self, index: int) -> str:
        """Gap attached to fact ``index``, or "" when this read did not measure one."""
        if not self.foreign_key_proof:
            return ""
        if len(self.foreign_key_proof) != len(self.foreign_key_facts):
            return "unreported"
        return self.foreign_key_proof[index] or ""

    def _match_at(self, index: int) -> str | None:
        """Match attached to fact ``index``, or None when this read did not measure one.

        A list that does not line up with the facts is ``unknown``. A mis-attached
        spelling is not a yes, and it is not silently SIMPLE.
        """
        if not self.foreign_key_match:
            return None
        if len(self.foreign_key_match) != len(self.foreign_key_facts):
            return "unknown"
        return normalize_match(self.foreign_key_match[index])

    def _actions_at(self, index: int) -> tuple[str, str] | None:
        """Actions attached to fact ``index``, or None when this read did not measure them.

        A list that does not line up with the facts is ``unknown``. A
        mis-attached action is not NO ACTION.
        """
        if not self.foreign_key_on_delete and not self.foreign_key_on_update:
            return None
        count = len(self.foreign_key_facts)
        if (
            len(self.foreign_key_on_delete) != count
            or len(self.foreign_key_on_update) != count
        ):
            return "unknown", "unknown"
        from services.foreign_key_metadata import normalize_action

        return (
            normalize_action(self.foreign_key_on_delete[index]),
            normalize_action(self.foreign_key_on_update[index]),
        )

    def _deferral_at(self, index: int) -> str | None:
        """Deferral attached to fact ``index``, or None when this read did not measure one.

        A list that does not line up with the facts is ``unknown``. A
        mis-attached mode is not NOT DEFERRABLE.
        """
        if not self.foreign_key_deferral:
            return None
        if len(self.foreign_key_deferral) != len(self.foreign_key_facts):
            return "unknown"
        from services.foreign_key_metadata import normalize_deferral

        return normalize_deferral(spelling=self.foreign_key_deferral[index])


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


# A catalog that fills the column without a literal (identity, AUTO_INCREMENT,
# computed). Not a string a DEFAULT clause can normalize to.
_GENERATED_DEFAULT = "\x00generated"


def _default_sql(value: Any) -> str:
    """The SQL text of a reflected default, without the driver's wrapper object."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    text = getattr(value, "text", None)
    if isinstance(text, str) and text.strip():
        return text.strip()
    arg = getattr(value, "arg", None)
    if arg is not None and arg is not value:
        return _default_sql(arg)
    return str(value).strip()


def _is_sequence_default(expr: str) -> bool:
    return expr.startswith("nextval(")


def _is_fill_in(expr: str) -> bool:
    """Sequence, identity, or AUTO_INCREMENT: the column is filled, not a literal."""
    return expr == _GENERATED_DEFAULT or _is_sequence_default(expr)


def catalog_default_fact(col: Mapping[str, Any]) -> tuple[str, str] | None:
    """``(column, expression)`` the catalog stored, or None when it stores nothing.

    A literal is the normalized expression, so ``'N'`` and ``('n')`` are one
    fact. A generator with no literal is one fill-in fact: PostgreSQL identity
    and MySQL AUTO_INCREMENT both record that the engine supplies the value.
    The sequence name and the computed expression are not this fact.
    """
    name = _fold(col.get("name"))
    if not name:
        return None
    if col.get("default") is not None:
        from services.default_expression import normalize_default_expr

        return name, normalize_default_expr(_default_sql(col.get("default")))
    if col.get("identity") or col.get("autoincrement") is True or col.get("computed"):
        return name, _GENERATED_DEFAULT
    return None


def _render_default(column: str, expr: str) -> str:
    if expr == _GENERATED_DEFAULT:
        shown = "generated"
    elif expr == "":
        shown = "''"
    else:
        shown = expr
    return f"{column}={shown}"


def _has_catalog_supplied_value(col: Any) -> bool:
    """Does the catalog supply this column's value when the writer sends none?

    Each engine records its generator in its own place: PostgreSQL as an identity
    or a ``nextval`` column default, MySQL as AUTO_INCREMENT with *no* default at
    all. Reading only ``default`` therefore reported a faithfully carried
    generator as a dropped default on every PostgreSQL→MySQL move. The counter's
    own health (next value past the migrated maximum) is proven separately by
    ``services.identity_watermark``. Whether two literals are the same rule is
    :func:`catalog_default_fact`.
    """
    return catalog_default_fact(col) is not None


def _cols(values: Any) -> tuple[str, ...]:
    if not values:
        return ()
    return tuple(_fold(v) for v in values if str(v or "").strip())


def _foreign_key_fact_dict(
    child: tuple[str, ...],
    schema: str,
    table: str,
    parent: tuple[str, ...],
    gap: str,
    *,
    measured: bool,
    match: str | None = None,
    actions: tuple[str, str] | None = None,
    deferral: str | None = None,
) -> dict[str, Any]:
    """Report wire for one relationship.

    The gap is present only when this read measured one. ``match`` is present
    only when this read measured a match type, including ``""`` for unreported.
    Actions are present only when this read measured them, including empty
    strings for an unreported ON DELETE or ON UPDATE. ``deferral`` is present
    only when this read measured a mode, including ``""`` for unreported.
    """
    item = {
        "constrained_columns": list(child),
        "referred_schema": schema,
        "referred_table": table,
        "referred_columns": list(parent),
    }
    if measured:
        item["row_proof_gap"] = gap
    if match is not None:
        item["match"] = match
    if actions is not None:
        item["on_delete"] = actions[0]
        item["on_update"] = actions[1]
    if deferral is not None:
        item["deferral"] = deferral
    return item


def _fact_names_actions(item: Mapping[str, Any]) -> bool:
    """True when this payload carries an ON DELETE or ON UPDATE field."""
    return any(key in item for key in ("on_delete", "on_update", "ondelete", "onupdate"))


def _fact_names_match(item: Mapping[str, Any]) -> bool:
    """True when this payload carries a match field, including an empty one."""
    if "match" in item or "confmatchtype" in item:
        return True
    options = item.get("options")
    return isinstance(options, Mapping) and "match" in options


def decode_foreign_key_item(item: Any) -> tuple[dict[str, Any] | None, str]:
    """One catalog relationship, plus an error when the token cannot be read.

    Dicts from ``foreign_key_facts`` and the canonical ``ForeignKey`` shape
    are the structured form. The rendered wire ``child->parent->cols`` is
    accepted for older reports. A token that is not three fields is an error:
    dropping it would look like the source declared no foreign key.
    """
    if isinstance(item, Mapping):
        child = _cols(item.get("constrained_columns") or item.get("columns"))
        parent_cols = _cols(
            item.get("referred_columns") or item.get("referenced_columns")
        )
        table = _fold(item.get("referred_table") or item.get("referenced_table"))
        schema = _fold(item.get("referred_schema") or item.get("referenced_schema"))
        if not child or not table or len(child) != len(parent_cols):
            return None, "incomplete foreign key"
        relationship = {
            "constrained_columns": list(child),
            "referred_schema": schema,
            "referred_table": table,
            "referred_columns": list(parent_cols),
        }
        if _fact_names_match(item):
            parsed = parse_foreign_key(item)
            if parsed.conflict.startswith("Foreign key names two match types"):
                return None, parsed.conflict
            if not parsed.conflict:
                relationship["match"] = parsed.match
        if _fact_names_actions(item):
            from services.foreign_key_metadata import normalize_action

            relationship["on_delete"] = normalize_action(
                item.get("on_delete") if "on_delete" in item else item.get("ondelete")
            )
            relationship["on_update"] = normalize_action(
                item.get("on_update") if "on_update" in item else item.get("onupdate")
            )
        if "deferral" in item:
            from services.foreign_key_metadata import normalize_deferral

            relationship["deferral"] = normalize_deferral(spelling=item.get("deferral"))
        return relationship, ""
    text = str(item or "").strip()
    parts = text.split("->")
    if len(parts) != 3 or not all(part.strip() for part in parts):
        return None, text or "blank foreign key"
    child = _cols(parts[0].split("+"))
    parent_cols = _cols(parts[2].split("+"))
    schema, table = ("", _fold(parts[1]))
    if table.count(".") == 1:
        schema, table = table.split(".", 1)
    if not child or not table or len(child) != len(parent_cols):
        return None, text
    return (
        {
            "constrained_columns": list(child),
            "referred_schema": schema,
            "referred_table": table,
            "referred_columns": list(parent_cols),
        },
        "",
    )


def foreign_keys_from_catalog_state(
    source: Mapping[str, Any] | None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Relationships the source catalog stated, and tokens that did not parse.

    Structured facts win over the rendered diff strings. Both describe the
    same read; the facts keep the parent schema the string leaves off.
    """
    state = source if isinstance(source, Mapping) else {}
    facts = state.get("foreign_key_facts")
    rendered = state.get("foreign_keys") or []
    items = facts if isinstance(facts, list) and facts else rendered
    keys: list[dict[str, Any]] = []
    unparsed: list[str] = []
    for item in items:
        relationship, error = decode_foreign_key_item(item)
        if error:
            unparsed.append(error)
        elif relationship is not None:
            keys.append(relationship)
    return keys, unparsed


def _read_snowflake_table_kind(conn: Any, schema: str, table: str) -> str:
    """``IS_HYBRID`` for one table. Empty when the catalog did not answer."""
    from services.foreign_key_metadata import read_snowflake_table_kind

    return read_snowflake_table_kind(conn, schema, table)


def _reflect_foreign_keys(
    db_type: str,
    conn: Any,
    schema: str,
    table: str,
    inspector_fks: Any,
    *,
    table_kind: str = "",
    index_status: str = "",
) -> tuple[
    set[tuple[str, str, str]],
    list[tuple[tuple[str, ...], str, str, tuple[str, ...]]],
    tuple[str, ...],
    tuple[str, ...],
    tuple[str, ...],
    tuple[str, ...],
    tuple[str, ...],
]:
    """Relationship facts, row-proof gap, match, actions, and deferral.

    The gap is :func:`services.foreign_key_metadata.inspector_row_proof_gaps`.
    The match type is :func:`services.foreign_key_metadata.relationship_match_type`.
    The actions are :func:`services.foreign_key_metadata.relationship_actions`.
    The deferral mode is :func:`services.foreign_key_metadata.relationship_deferral`.
    Bit dialects read them from the metadata probe on this connection.
    Redshift is unenforced without that query. MySQL and SQLite record an
    empty gap: the constraint itself is the check those engines report. An
    empty match string means the catalog did not name one. An empty action
    string means the catalog did not name that action. An empty deferral
    string means the catalog did not name a mode.
    """
    from services.foreign_key_metadata import (
        catalog_probe_dialect,
        inspector_row_proof_gaps,
        probe_foreign_keys,
        relationship_actions,
        relationship_deferral,
        relationship_match_type,
    )

    kept: list[dict[str, Any]] = []
    originals: list[Mapping[str, Any]] = []
    fk_sets: set[tuple[str, str, str]] = set()
    fk_facts: list[tuple[tuple[str, ...], str, str, tuple[str, ...]]] = []
    inspector_rows = list(inspector_fks or [])
    for fk in inspector_rows:
        if not isinstance(fk, Mapping) or not fk.get("constrained_columns"):
            continue
        child_cols = _cols(fk.get("constrained_columns"))
        parent_cols = _cols(fk.get("referred_columns"))
        parent_table = _fold(fk.get("referred_table"))
        parent_schema = _fold(fk.get("referred_schema"))
        kept.append(
            {
                "constrained_columns": list(child_cols),
                "referred_schema": parent_schema,
                "referred_table": parent_table,
                "referred_columns": list(parent_cols),
            }
        )
        originals.append(fk)
        fk_sets.add(
            foreign_key_wire(child_cols, parent_schema, parent_table, parent_cols)
        )
        fk_facts.append((child_cols, parent_schema, parent_table, parent_cols))
    if not fk_facts:
        return fk_sets, fk_facts, (), (), (), (), ()
    catalog_dialect = catalog_probe_dialect(db_type)
    measured = (
        probe_foreign_keys(catalog_dialect, conn, schema or "", table)
        if catalog_dialect is not None
        else None
    )
    gaps = inspector_row_proof_gaps(
        db_type,
        kept,
        measured,
        table_kind=table_kind,
        index_status=index_status,
    )
    if len(gaps) != len(fk_facts):
        gaps = ["unreported"] * len(fk_facts)
    matches = tuple(
        relationship_match_type(fk_identity(fk), measured, inspector_rows)
        for fk in originals
    )
    action_pairs = tuple(
        relationship_actions(fk_identity(fk), measured, inspector_rows)
        for fk in originals
    )
    deferrals = tuple(
        relationship_deferral(fk_identity(fk), measured, inspector_rows)
        for fk in originals
    )
    return (
        fk_sets,
        fk_facts,
        tuple(gaps),
        matches,
        tuple(pair[0] for pair in action_pairs),
        tuple(pair[1] for pair in action_pairs),
        deferrals,
    )


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
        schema = _catalog_schema(inspector, conn, schema)
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
        if indexes is not None and _catalog_dialect(db_type) == "sqlite":
            try:
                omitted_names = _sqlite_omitted_index_names(conn, name, schema, indexes)
            except Exception as exc:  # noqa: BLE001 — an unread index is not "no index"
                collector.errors.append(f"indexes: {exc}")
            else:
                for omitted in omitted_names:
                    collector.errors.append(f"indexes: {omitted}")
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
        from services.foreign_key_metadata import _dialect_key

        table_kind = ""
        index_status = ""
        index_detail = ""
        if _dialect_key(db_type) == "snowflake":
            from services.foreign_key_metadata import read_snowflake_index_proof

            table_kind = _read_snowflake_table_kind(conn, schema, name)
            index_status, index_detail = read_snowflake_index_proof(
                conn, schema, name
            )
        oracle_rows: list[Any] | None = None
        oracle_check_rows: list[Any] | None = None
        sqlserver_rows: list[Any] | None = None
        postgres_rows: list[Any] | None = None
        if _dialect_key(db_type) == "oracle":
            from services.unique_key_introspect import read_oracle_uniqueness_rows

            oracle_rows = read_oracle_uniqueness_rows(conn, schema, name)
            oracle_check_rows = read_oracle_check_rows(conn, schema, name)
        from services.dialect_profiles import is_sqlserver_like
        from services.foreign_key_metadata import postgres_index_catalog

        if is_sqlserver_like(db_type):
            from services.unique_key_introspect import read_sqlserver_uniqueness_rows

            sqlserver_rows = read_sqlserver_uniqueness_rows(conn, schema, name)
        if postgres_index_catalog(db_type):
            from services.unique_key_introspect import read_postgres_uniqueness_rows

            postgres_rows = read_postgres_uniqueness_rows(conn, schema, name)
        (
            fk_sets,
            fk_facts,
            fk_proof,
            fk_match,
            fk_delete,
            fk_update,
            fk_deferral,
        ) = _reflect_foreign_keys(
            db_type,
            conn,
            schema,
            name,
            fks,
            table_kind=table_kind,
            index_status=index_status,
        )

    not_null: set[str] = set()
    defaults: set[tuple[str, str]] = set()
    for col in columns or []:
        col_name = _fold(col.get("name"))
        if not col_name:
            continue
        if col.get("nullable") is False:
            not_null.add(col_name)
        fact = catalog_default_fact(col)
        if fact is not None:
            defaults.add(fact)

    unique_sets = {
        _cols(u.get("column_names")) for u in uniques or [] if u.get("column_names")
    }
    index_sets = {
        fact
        for i in indexes or []
        if (fact := catalog_index_fact(i)) is not None
    }

    return PhysicalState(
        readable=not collector.errors,
        found=True,
        reason="" if not collector.errors else "partial catalog read",
        primary_key=_cols((pk or {}).get("constrained_columns")),
        unique_constraints=frozenset(unique_sets),
        foreign_keys=frozenset(fk_sets),
        foreign_key_facts=tuple(fk_facts),
        foreign_key_proof=fk_proof,
        foreign_key_match=fk_match,
        foreign_key_on_delete=fk_delete,
        foreign_key_on_update=fk_update,
        foreign_key_deferral=fk_deferral,
        indexes=frozenset(index_sets),
        not_null=frozenset(not_null),
        defaults=frozenset(defaults),
        check_constraints=frozenset(
            _normalize_predicate(c.get("sqltext"))
            for c in checks or []
            if _normalize_predicate(c.get("sqltext"))
        ),
        check_proof=_measured_check_proof(db_type, checks, oracle_check_rows),
        triggers=frozenset(triggers or ()),
        views=frozenset(views or ()),
        routines=frozenset(routines or ()),
        errors=tuple(collector.errors),
        dialect=str(db_type or ""),
        table_kind=table_kind,
        index_status=index_status,
        index_detail=index_detail,
        uniqueness_proof=_measured_uniqueness_proof(
            db_type,
            _cols((pk or {}).get("constrained_columns")),
            unique_sets,
            oracle_rows,
            sqlserver_rows,
            postgres_rows,
        ),
    )


def _measured_uniqueness_proof(
    db_type: str,
    primary_key: tuple[str, ...],
    unique_sets: set[tuple[str, ...]],
    oracle_rows: list[Any] | None,
    sqlserver_rows: list[Any] | None,
    postgres_rows: list[Any] | None = None,
) -> tuple[tuple[tuple[str, ...], str], ...]:
    """Oracle, SQL Server, or PostgreSQL uniqueness bits. Empty otherwise."""
    from services.dialect_profiles import is_sqlserver_like
    from services.foreign_key_metadata import _dialect_key, postgres_index_catalog

    if postgres_index_catalog(db_type):
        return _postgres_uniqueness_proof(
            db_type, primary_key, unique_sets, postgres_rows
        )
    if _dialect_key(db_type) == "oracle":
        return _oracle_uniqueness_proof(db_type, primary_key, unique_sets, oracle_rows)
    if is_sqlserver_like(db_type):
        return _sqlserver_uniqueness_proof(primary_key, unique_sets, sqlserver_rows)
    return ()


def _postgres_uniqueness_proof(
    db_type: str,
    primary_key: tuple[str, ...],
    unique_sets: set[tuple[str, ...]],
    rows: list[Any] | None,
) -> tuple[tuple[tuple[str, ...], str], ...]:
    """One gap per reflected PostgreSQL key.

    ``rows is None`` means ``indisvalid`` was not read. Each reflected key
    stays ``unreported``. ``indisvalid`` false is ``not_checked``.
    ``indisready`` false is ``not_ready``. A valid index is an empty gap.
    """
    from services.foreign_key_metadata import postgres_index_catalog
    from services.unique_key_introspect import postgres_uniqueness_proof

    if not postgres_index_catalog(db_type):
        return ()
    measured = postgres_uniqueness_proof(rows) if rows is not None else {}
    return _gaps_for_reflected_keys(primary_key, unique_sets, measured, rows is None)


def _sqlserver_uniqueness_proof(
    primary_key: tuple[str, ...],
    unique_sets: set[tuple[str, ...]],
    rows: list[Any] | None,
) -> tuple[tuple[tuple[str, ...], str], ...]:
    """One gap per reflected SQL Server key.

    ``rows is None`` means ``is_disabled`` was not read. Each reflected key
    stays ``unreported``. ``is_disabled = 1`` is ``not_checked``. An enabled
    index is an empty gap.
    """
    from services.unique_key_introspect import sqlserver_uniqueness_proof

    measured = sqlserver_uniqueness_proof(rows) if rows is not None else {}
    return _gaps_for_reflected_keys(primary_key, unique_sets, measured, rows is None)


def _gaps_for_reflected_keys(
    primary_key: tuple[str, ...],
    unique_sets: set[tuple[str, ...]],
    measured: dict[frozenset[str], str],
    unread: bool,
) -> tuple[tuple[tuple[str, ...], str], ...]:
    reflected = [primary_key] if primary_key else []
    reflected.extend(cols for cols in unique_sets if cols)
    proof: list[tuple[tuple[str, ...], str]] = []
    seen: set[frozenset[str]] = set()
    for cols in reflected:
        key = frozenset(cols)
        if not key or key in seen:
            continue
        seen.add(key)
        if unread or key not in measured:
            gap = "unreported"
        else:
            gap = measured[key]
        proof.append((tuple(sorted(key)), gap))
    return tuple(proof)


def _oracle_uniqueness_proof(
    db_type: str,
    primary_key: tuple[str, ...],
    unique_sets: set[tuple[str, ...]],
    rows: list[Any] | None,
) -> tuple[tuple[tuple[str, ...], str], ...]:
    """One gap per reflected Oracle key. Empty when this engine is not Oracle.

    ``rows is None`` means ``ALL_CONSTRAINTS.VALIDATED`` was not read. Each
    reflected key stays ``unreported``. A successful read that does not name
    a reflected column set is the same gap. ``NOT VALIDATED`` is
    ``not_checked``. ``VALIDATED`` is an empty gap.
    """
    from services.foreign_key_metadata import _dialect_key
    from services.unique_key_introspect import oracle_uniqueness_proof

    if _dialect_key(db_type) != "oracle":
        return ()
    measured = oracle_uniqueness_proof(rows) if rows is not None else {}
    return _gaps_for_reflected_keys(primary_key, unique_sets, measured, rows is None)


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


def _normalize_predicate(sqltext: Any, *, drop_not_null: bool = True) -> str:
    """Strip the dialect's punctuation so ``("qty" > 0)`` and ``qty>0`` match.

    Two engines never spell the same CHECK identically; comparing raw text would
    report every constraint as missing. Whitespace, quoting styles and wrapping
    parentheses carry no meaning, so they go.
    """
    raw = "" if sqltext is None else str(sqltext).strip().casefold()
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
    # Oracle reflects every NOT NULL as a CHECK; the not_null aspect owns those.
    # A partial index ``WHERE note IS NOT NULL`` keeps this predicate.
    if drop_not_null and text.endswith("isnotnull"):
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
    A one-column row did not ask for ``is_disabled``. Do not invent the bits.
    """
    try:
        return list(inspector.get_check_constraints(table, **args))
    except NotImplementedError:
        from services.dialect_profiles import is_sqlserver_like

        key = str(db_type or "").strip().casefold()
        if is_sqlserver_like(key):
            key = "mssql"
        sql = _CHECK_SQL.get(key)
        if not sql:
            raise
        rows = conn.execute(sa.text(sql), {"t": table, "s": schema or ""}).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            item: dict[str, Any] = {"sqltext": row[0]}
            if len(tuple(row)) > 2:
                item["disabled"] = row[1]
                item["not_trusted"] = row[2]
            out.append(item)
        return out


def read_oracle_check_rows(conn: Any, owner: str, table: str) -> list[Any] | None:
    """CHECK rows, or None when ``STATUS`` and ``VALIDATED`` were not read.

    An empty list is a successful read of no check constraint. The exact
    owner/table spelling is tried before upper case. ``search_condition_vc``
    is the predicate SQLAlchemy's LONG ``search_condition`` is compared
    against after normalization.
    """
    owner_u = (owner or "").upper()
    table_u = (table or "").upper()
    attempts = [(str(owner or ""), str(table or ""))]
    if (owner_u, table_u) != attempts[0]:
        attempts.append((owner_u, table_u))
    sql = sa.text(
        """
        SELECT search_condition_vc, status, validated
        FROM all_constraints
        WHERE constraint_type = 'C'
          AND table_name = :table
          AND owner = :owner
        """
    )
    try:
        rows: list[Any] = []
        for owner_try, table_try in attempts:
            rows = list(
                conn.execute(sql, {"owner": owner_try, "table": table_try}).fetchall()
            )
            if rows:
                return rows
        return []
    except Exception:
        return None


def _oracle_status_enabled(value: Any) -> bool | None:
    """``ALL_CONSTRAINTS.STATUS``. None when this cell did not say."""
    if value is None:
        return None
    text = str(value).strip().casefold()
    if text == "enabled":
        return True
    if text == "disabled":
        return False
    return None


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


class CatalogIndex(NamedTuple):
    """One index the catalog reflected, or an expression the driver spelled.

    ``keys`` keeps column order. A reported sort direction, opclass, or prefix
    length is part of that key (``email desc``, ``email text_pattern_ops``,
    ``email(10)``). An expression stands in for a column the driver left
    unnamed (``lower(email)``). ``predicate`` is the partial-index filter.
    ``include`` is the covering set. ``using`` is the access method when the
    driver reports one other than its default (``gin``, ``fulltext``). An
    omitted method is that default, so SQLite and a Postgres btree match.
    ``nulls_not_distinct`` is the Postgres unique-null rule. ``invalid`` is a
    Postgres index the catalog says is not usable.
    """

    unique: bool
    keys: tuple[str, ...]
    predicate: str
    nulls_not_distinct: bool
    include: tuple[str, ...] = ()
    using: str = ""
    invalid: bool = False


def _index_predicate(index: Mapping[str, Any]) -> str:
    options = index.get("dialect_options") or {}
    raw = None
    for key in ("sqlite_where", "postgresql_where", "mysql_where"):
        if key in options and options[key] is not None:
            raw = options[key]
            break
    return _normalize_predicate(raw, drop_not_null=False)


def _normalize_index_expression(expr: Any) -> str:
    """Casefold an index expression and drop identifier quotes.

    Parentheses stay. ``lower(email)`` is a call, and the CHECK normalizer
    treats parentheses around a name as the engine's own echo.
    """
    raw = "" if expr is None else str(expr).strip().casefold()
    return "".join(ch for ch in raw if ch not in '"`[] \t\n\r')


def _sort_suffix(name: str, sorting: Mapping[str, Any]) -> str:
    flags = sorting.get(name)
    if not flags:
        folded = _fold(name)
        for key, value in sorting.items():
            if _fold(key) == folded:
                flags = value
                break
    if not flags:
        return ""
    return " ".join(
        text for flag in flags if (text := str(flag).strip().casefold())
    )


def _column_option(options: Mapping[str, Any], name: str, *keys: str) -> str:
    """A per-column dialect option, matched on the catalog's own spelling."""
    folded = _fold(name)
    for key in keys:
        table = options.get(key) or {}
        if not isinstance(table, Mapping):
            continue
        if name in table and table[name] is not None:
            return str(table[name]).strip()
        for column, value in table.items():
            if _fold(column) == folded and value is not None:
                return str(value).strip()
    return ""


def _index_using(options: Mapping[str, Any]) -> str:
    """Access method when it is not the driver's default.

    Postgres omits btree. MySQL omits a plain ``KEY`` and records ``FULLTEXT``
    or ``SPATIAL`` on ``mysql_prefix``. A parser is part of that method.
    """
    method = _fold(
        options.get("postgresql_using")
        or options.get("mysql_prefix")
        or options.get("mariadb_prefix")
        or ""
    )
    parser = _fold(
        options.get("mysql_with_parser") or options.get("mariadb_with_parser") or ""
    )
    if parser:
        return f"{method} {parser}".strip()
    return method


def _index_include(options: Mapping[str, Any]) -> tuple[str, ...]:
    """Covering columns. Order does not change which columns are stored."""
    raw = options.get("postgresql_include") or ()
    names = sorted({_fold(column) for column in raw if str(column or "").strip()})
    return tuple(names)


def catalog_index_fact(index: Mapping[str, Any]) -> CatalogIndex | None:
    """The index identity, or None when a key column was not spelled.

    A ``None`` column without an expression is an index the driver did not
    describe. Dropping the expression and keeping the other columns would
    certify ``(lower(email), id)`` as a plain index on ``id``.
    """
    names = list(index.get("column_names") or [])
    expressions = list(index.get("expressions") or [])
    if not names and not expressions:
        return None
    sorting = index.get("column_sorting") or {}
    options = index.get("dialect_options") or {}
    keys: list[str] = []
    for i in range(max(len(names), len(expressions))):
        name = names[i] if i < len(names) else None
        expr = expressions[i] if i < len(expressions) else None
        if name:
            key = _fold(name)
            length = _column_option(
                options, str(name), "mysql_length", "mariadb_length"
            )
            if length:
                key = f"{key}({length})"
            suffix = _sort_suffix(str(name), sorting)
            opclass = _fold(
                _column_option(options, str(name), "postgresql_ops")
            )
            extras = " ".join(part for part in (suffix, opclass) if part)
            keys.append(f"{key} {extras}" if extras else key)
            continue
        spelled = _normalize_index_expression(expr)
        if not spelled:
            return None
        keys.append(spelled)
    if not keys:
        return None
    return CatalogIndex(
        unique=bool(index.get("unique")),
        keys=tuple(keys),
        predicate=_index_predicate(index),
        nulls_not_distinct=bool(options.get("postgresql_nulls_not_distinct")),
        include=_index_include(options),
        using=_index_using(options),
        invalid=bool(options.get("postgresql_invalid")),
    )


def _as_index(fact: tuple) -> CatalogIndex:
    if isinstance(fact, CatalogIndex):
        return fact
    return CatalogIndex(*fact)


def _render_index(fact: tuple) -> str:
    index = _as_index(fact)
    body = "+".join(index.keys)
    if index.unique:
        body = f"unique({body})"
    if index.nulls_not_distinct:
        body = f"{body} nulls not distinct"
    if index.include:
        body = f"{body} include {'+'.join(index.include)}"
    if index.using:
        body = f"{body} using {index.using}"
    if index.invalid:
        body = f"{body} invalid"
    if index.predicate:
        body = f"{body} where {index.predicate}"
    return body


def _diff_indexes(
    source: frozenset[CatalogIndex],
    destination: frozenset[CatalogIndex],
) -> dict[str, Any]:
    """Carried when each source index has the same access path on the destination.

    ``(b, a)`` is not ``(a, b)``. A unique index is not a plain index. A
    partial index is not a full index. ``email DESC`` is not ``email`` when
    the catalog reports the direction. ``lower(email)`` is not ``email``.
    A covering ``INCLUDE``, a ``gin`` or ``FULLTEXT`` method, a prefix
    length, and an invalid index are part of the same path.
    """
    missing = sorted(_render_index(fact) for fact in source - destination)
    extra = sorted(_render_index(fact) for fact in destination - source)
    return {
        "status": "carried" if not missing else "absent",
        "missing": missing,
        "extra": extra,
        "source_count": len(source),
        "destination_count": len(destination),
    }


def _sqlite_omitted_index_names(
    conn: Any,
    table: str,
    schema: str,
    reflected: list[Any],
) -> list[str]:
    """Index names SQLite stored and the driver did not return.

    SQLAlchemy drops expression indexes and warns. An omitted name must not
    look like a catalog with no such index, or ``lower(email)`` certifies as
    carried when the destination has no index at all.
    """
    from connectors.sql_identifiers import quote_sql_identifier

    reflected_names = {
        _fold(item.get("name"))
        for item in reflected
        if isinstance(item, Mapping) and item.get("name")
    }
    master = (
        f"{quote_sql_identifier(schema)}.sqlite_master" if schema else "sqlite_master"
    )
    rows = conn.execute(
        sa.text(
            f"SELECT name FROM {master} "
            "WHERE type = 'index' AND lower(tbl_name) = lower(:t) AND sql IS NOT NULL"
        ),
        {"t": table},
    ).fetchall()
    omitted: list[str] = []
    for row in rows:
        name = str(row[0] or "").strip()
        if name and _fold(name) not in reflected_names:
            omitted.append(f"index {name} was not reflected")
    return omitted


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


def _catalog_schema(inspector: Any, conn: Any, schema: str) -> str:
    """The qualifier this catalog actually knows, or "" for the default one.

    A writer reports ``schema or database`` as the place it wrote, which is the
    qualifier on MySQL but a *file path* on SQLite-backed engines (sqlite,
    generic_sql, duckdb over a file). SQLite reads a schema as an ATTACHed
    database name, so the path made every reflection query read
    ``"/tmp/x.db".sqlite_master`` and the whole structural attestation came
    back unreadable on a table that is right there.
    """
    if not schema:
        return ""
    if getattr(conn.dialect, "name", "") != "sqlite":
        return schema
    try:
        attached = {str(s) for s in inspector.get_schema_names()}
    except Exception:  # noqa: BLE001 — an unreadable list is not a qualifier
        return ""
    return schema if schema in attached else ""


def _resolve_table_name(inspector: Any, table: str, schema: str | None) -> str | None:
    """Stored spelling of ``table`` in this catalog, or None when absent."""
    return resolve_stored_name(inspector.get_table_names(schema=schema), table)


def _dest_row_proof_gap(proof: tuple[str, ...], index: int, count: int) -> str:
    """Gap on one destination fact.

    An empty proof tuple means the reader did not attach a gap, so identity
    comparison stays as it was. A proof tuple that does not line up with the
    facts is ``unreported``: a mis-attached bit is not a yes.
    """
    if not proof:
        return ""
    if len(proof) != count:
        return "unreported"
    return proof[index] or ""


def _measured_match(matches: tuple[str, ...], index: int, count: int) -> str | None:
    """Normalized match at ``index``, or None when this side did not measure one.

    A list that does not line up with the facts is ``unknown``. That is not
    a completed rule, and it is not silently MATCH SIMPLE.
    """
    if not matches:
        return None
    if len(matches) != count:
        return "unknown"
    return normalize_match(matches[index])


def _measured_actions(
    deletes: tuple[str, ...],
    updates: tuple[str, ...],
    index: int,
    count: int,
) -> tuple[str, str] | None:
    """Normalized actions at ``index``, or None when this side did not measure them.

    A list that does not line up with the facts is ``unknown``. That is not
    NO ACTION.
    """
    if not deletes and not updates:
        return None
    if len(deletes) != count or len(updates) != count:
        return "unknown", "unknown"
    from services.foreign_key_metadata import normalize_action

    return normalize_action(deletes[index]), normalize_action(updates[index])


def _measured_deferral(modes: tuple[str, ...], index: int, count: int) -> str | None:
    """Normalized deferral at ``index``, or None when this side did not measure one.

    A list that does not line up with the facts is ``unknown``. That is not
    a completed mode, and it is not silently NOT DEFERRABLE.
    """
    if not modes:
        return None
    if len(modes) != count:
        return "unknown"
    from services.foreign_key_metadata import normalize_deferral

    return normalize_deferral(spelling=modes[index])


def _diff_foreign_keys(
    source: tuple[tuple[tuple[str, ...], str, str, tuple[str, ...]], ...],
    destination: tuple[tuple[tuple[str, ...], str, str, tuple[str, ...]], ...],
    *,
    destination_proof: tuple[str, ...] = (),
    destination_dialect: str = "",
    source_match: tuple[str, ...] = (),
    destination_match: tuple[str, ...] = (),
    source_on_delete: tuple[str, ...] = (),
    source_on_update: tuple[str, ...] = (),
    destination_on_delete: tuple[str, ...] = (),
    destination_on_update: tuple[str, ...] = (),
    source_deferral: tuple[str, ...] = (),
    destination_deferral: tuple[str, ...] = (),
    destination_table_kind: str = "",
    index_detail: str = "",
) -> dict[str, Any]:
    """Carried only when the destination relationship proves the source rule.

    Set subtraction on the rendered string treats ``sales.parent`` and
    ``archive.parent`` as one key, and treats reversed column order as a
    different key. The orphan scan does neither. A match whose destination
    gap is ``unenforced``, ``not_checked``, or ``unreported`` stays listed:
    the object is present, and it is not row proof. That is ``unchecked``,
    not ``absent``.

    Match type is not part of the relationship identity. When both sides
    measured it, :func:`services.fk_tuple_scan.match_rule_disagreement`
    decides whether the destination rule keeps the source promise. An empty
    match list means that side did not measure one, so a hand-built state
    still compares identity and the row-proof gap. Unreported is MATCH SIMPLE.
    A destination MATCH FULL keeps a source MATCH SIMPLE promise. A
    destination MATCH SIMPLE does not keep a source MATCH FULL promise.

    ON DELETE and ON UPDATE are not part of the relationship identity either.
    When both sides measured them,
    :func:`services.foreign_key_carry.referential_action_disagreement` decides
    whether the destination actions keep the source rule.     An empty action list
    means that side did not measure them. Unreported is NO ACTION.

    Deferral is not part of the relationship identity either. When both
    sides measured it,
    :func:`services.foreign_key_metadata.deferral_disagreement` decides
    whether the destination checks at the same time as the source. An empty
    deferral list means that side did not measure one. Unreported is NOT
    DEFERRABLE. NOT DEFERRABLE, INITIALLY IMMEDIATE, and INITIALLY DEFERRED
    are three rules. A destination that checks sooner does not keep a source
    that waits until commit, and a destination that can postpone the check
    does not keep a source that cannot.
    """
    from services.foreign_key_carry import referential_action_disagreement
    from services.foreign_key_metadata import (
        deferral_disagreement,
        row_proof_reason,
        with_snowflake_index_detail,
    )

    def _as_mapping(fact: tuple[tuple[str, ...], str, str, tuple[str, ...]]) -> dict[str, Any]:
        child, schema, table, parent = fact
        return {
            "constrained_columns": list(child),
            "referred_schema": schema,
            "referred_table": table,
            "referred_columns": list(parent),
        }

    used: set[int] = set()
    missing: list[str] = []
    unchecked: list[tuple[str, list[tuple[str, str]]]] = []
    for source_index, fact in enumerate(source):
        ident = fk_identity(_as_mapping(fact))
        match = None
        for index, other in enumerate(destination):
            if index in used:
                continue
            if same_relationship(ident, fk_identity(_as_mapping(other))):
                match = index
                break
        if match is None:
            missing.append(render_foreign_key_fact(*fact))
            continue
        used.add(match)
        tagged: list[tuple[str, str]] = []
        gap = _dest_row_proof_gap(destination_proof, match, len(destination))
        if gap:
            tagged.append(
                (
                    "proof",
                    with_snowflake_index_detail(
                        row_proof_reason(
                            gap,
                            destination_dialect,
                            table_kind=destination_table_kind,
                        ),
                        index_detail,
                    ),
                )
            )
        planned = _measured_match(source_match, source_index, len(source))
        measured = _measured_match(destination_match, match, len(destination))
        if planned is not None and measured is not None:
            disagreement = match_rule_disagreement(planned, measured)
            if disagreement:
                tagged.append(("match", disagreement))
        planned_actions = _measured_actions(
            source_on_delete, source_on_update, source_index, len(source)
        )
        measured_actions = _measured_actions(
            destination_on_delete, destination_on_update, match, len(destination)
        )
        if planned_actions is not None and measured_actions is not None:
            action_disagreement = referential_action_disagreement(
                planned_actions[0],
                planned_actions[1],
                measured_actions[0],
                measured_actions[1],
            )
            if action_disagreement:
                tagged.append(("action", action_disagreement))
        planned_deferral = _measured_deferral(source_deferral, source_index, len(source))
        measured_deferral = _measured_deferral(
            destination_deferral, match, len(destination)
        )
        if planned_deferral is not None and measured_deferral is not None:
            defer_disagreement = deferral_disagreement(planned_deferral, measured_deferral)
            if defer_disagreement:
                tagged.append(("deferral", defer_disagreement))
        if tagged:
            unchecked.append((render_foreign_key_fact(*fact), tagged))
    extra = [
        render_foreign_key_fact(*fact)
        for index, fact in enumerate(destination)
        if index not in used
    ]
    pairs = sorted(unchecked, key=lambda item: item[0])
    proof_reasons = [
        reason for _wire, tagged in pairs for kind, reason in tagged if kind == "proof"
    ]
    match_reasons = [
        reason for _wire, tagged in pairs for kind, reason in tagged if kind == "match"
    ]
    action_reasons = [
        reason for _wire, tagged in pairs for kind, reason in tagged if kind == "action"
    ]
    deferral_reasons = [
        reason
        for _wire, tagged in pairs
        for kind, reason in tagged
        if kind == "deferral"
    ]
    if missing:
        status = "absent"
    elif pairs:
        status = "unchecked"
    else:
        status = "carried"
    return {
        "status": status,
        "missing": sorted(missing),
        "extra": sorted(extra),
        "unchecked": [wire for wire, _tagged in pairs],
        "reasons": [reason for _wire, tagged in pairs for _kind, reason in tagged],
        "proof_reasons": proof_reasons,
        "match_reasons": match_reasons,
        "action_reasons": action_reasons,
        "deferral_reasons": deferral_reasons,
        "source_count": len(source),
        "destination_count": len(destination),
    }


def _diff_uniqueness(
    source: frozenset[tuple[str, ...]],
    destination: frozenset[tuple[str, ...]],
    *,
    destination_dialect: str = "",
    table_kind: str = "",
    index_status: str = "",
    index_detail: str = "",
    uniqueness_proof: tuple[tuple[tuple[str, ...], str], ...] = (),
) -> dict[str, Any]:
    """Carried when the column sets match and existing rows were checked.

    A Snowflake hybrid table rejects a new duplicate when the key is
    enforced. That write rule is not this verdict. Existing rows are
    carried only when ``SHOW INDEXES`` status is ``ACTIVE``.

    An Oracle ``ENABLED`` key rejects a new duplicate. That write rule is
    not this verdict. Existing rows are carried only when
    ``ALL_CONSTRAINTS.VALIDATED`` is ``VALIDATED``. A SQL Server unique
    index with ``is_disabled = 1`` does not reject a new duplicate and is
    not existing-row proof. A PostgreSQL unique index proves existing rows
    only when ``pg_index.indisvalid`` is true. ``indisready`` false is not
    a write rule either. An empty proof tuple means this comparison did
    not measure that column.

    ``UNIQUE (b, a)`` is the same constraint as ``UNIQUE (a, b)``. A primary
    key is the same rule. Catalog ordinal is not a second key. Index order,
    uniqueness, and the partial predicate stay on the indexes aspect. On an
    engine that stores the key and does not check rows, a match stays listed
    as ``unchecked``. The object is present. It is not a duplicate-row count.
    """
    from services.dialect_profiles import is_sqlserver_like
    from services.foreign_key_metadata import (
        _dialect_key,
        oracle_uniqueness_validation_reason,
        postgres_index_catalog,
        postgres_unique_index_reason,
        sqlserver_disabled_unique_reason,
        uniqueness_proof_gap,
        uniqueness_proof_reason,
        with_snowflake_index_detail,
    )

    def _sets(groups: frozenset[tuple[str, ...]]) -> set[frozenset[str]]:
        return {frozenset(group) for group in groups if group}

    def _wire(group: frozenset[str]) -> str:
        return "+".join(sorted(group))

    src = _sets(source)
    dst = _sets(destination)
    missing = sorted(_wire(group) for group in src - dst)
    extra = sorted(_wire(group) for group in dst - src)
    dialect_gap = (
        uniqueness_proof_gap(
            destination_dialect,
            table_kind=table_kind,
            index_status=index_status,
        )
        if destination_dialect
        else ""
    )
    family = _dialect_key(destination_dialect)
    use_measured_proof = bool(uniqueness_proof) and (
        family == "oracle"
        or postgres_index_catalog(destination_dialect)
        or is_sqlserver_like(destination_dialect)
    )

    def _item_gap(group: frozenset[str]) -> str:
        if not use_measured_proof:
            return dialect_gap
        for columns, item_gap in uniqueness_proof:
            if frozenset(columns) == group:
                return item_gap
        return "unreported"

    matched_groups = sorted(src & dst, key=_wire)
    unchecked: list[str] = []
    reasons: list[str] = []
    for group in matched_groups:
        item_gap = _item_gap(group)
        if not item_gap:
            continue
        unchecked.append(_wire(group))
        if use_measured_proof and family == "oracle":
            reason = oracle_uniqueness_validation_reason(item_gap)
        elif use_measured_proof and postgres_index_catalog(destination_dialect):
            reason = postgres_unique_index_reason(item_gap)
        elif use_measured_proof and is_sqlserver_like(destination_dialect):
            reason = sqlserver_disabled_unique_reason(item_gap)
        else:
            reason = with_snowflake_index_detail(
                uniqueness_proof_reason(
                    destination_dialect,
                    table_kind=table_kind,
                    index_status=index_status,
                ),
                index_detail,
            )
        if reason and reason not in reasons:
            reasons.append(reason)
    if missing:
        status = "absent"
    elif unchecked:
        status = "unchecked"
    else:
        status = "carried"
    return {
        "status": status,
        "missing": missing,
        "extra": extra,
        "unchecked": unchecked,
        "reasons": reasons,
        "source_count": len(src),
        "destination_count": len(dst),
    }


def _defaults_equivalent(left: str, right: str) -> bool:
    """Same literal rule, or both sides still fill the column in."""
    if left == right or (_is_fill_in(left) and _is_fill_in(right)):
        return True
    from services.default_expression import default_exprs_equivalent

    return default_exprs_equivalent(left, right)


def _diff_defaults(
    source: frozenset[tuple[str, str]],
    destination: frozenset[tuple[str, str]],
) -> dict[str, Any]:
    """Carried when each source default has the same rule on that column.

    ``DEFAULT 'n'`` is not ``DEFAULT 'x'``. ``'N'``, ``('n')`` and ``n`` are
    one literal. ``CURRENT_TIMESTAMP`` and ``now()`` are one clock. A
    ``nextval`` default and an AUTO_INCREMENT with no literal are one fill-in:
    both engines supply the value. A literal replaced by that fill-in is absent.
    """
    src = list(source)
    dst = list(destination)
    used: set[int] = set()
    missing: list[str] = []
    for column, expr in src:
        match = None
        for index, (other_column, other_expr) in enumerate(dst):
            if index in used or other_column != column:
                continue
            if _defaults_equivalent(expr, other_expr):
                match = index
                break
        if match is None:
            missing.append(_render_default(column, expr))
        else:
            used.add(match)
    extra = [
        _render_default(column, expr)
        for index, (column, expr) in enumerate(dst)
        if index not in used
    ]
    return {
        "status": "carried" if not missing else "absent",
        "missing": sorted(missing),
        "extra": sorted(extra),
        "source_count": len(src),
        "destination_count": len(dst),
    }


_CHECK_GAP_RANK = {"": 0, "unreported": 1, "not_checked": 2, "disabled": 3}


def _worse_check_gap(proof: dict[str, str], predicate: str, gap: str) -> None:
    current = proof.get(predicate)
    if current is None or _CHECK_GAP_RANK[gap] > _CHECK_GAP_RANK[current]:
        proof[predicate] = gap


def _measured_check_proof(
    db_type: str,
    checks: list[Any] | None,
    oracle_rows: list[Any] | None = None,
) -> tuple[tuple[str, str], ...]:
    """CHECK existing-row gaps. Empty when this engine does not measure them.

    PostgreSQL uses ``dialect_options.not_valid``. SQL Server uses
    ``is_disabled`` and ``is_not_trusted`` on the check row. Oracle uses
    ``ALL_CONSTRAINTS`` rows. SQLite and Redshift stay empty.
    """
    from services.dialect_profiles import is_sqlserver_like
    from services.foreign_key_metadata import _dialect_key, postgres_index_catalog

    if postgres_index_catalog(db_type):
        return _postgres_check_proof(checks)
    if is_sqlserver_like(db_type):
        return _sqlserver_check_proof(checks)
    if _dialect_key(db_type) == "oracle":
        return _oracle_check_proof(checks, oracle_rows)
    return ()


def _postgres_check_proof(
    checks: list[Any] | None,
) -> tuple[tuple[str, str], ...]:
    """PostgreSQL ``NOT VALID`` gaps.

    SQLAlchemy sets ``dialect_options.not_valid`` only when
    ``pg_get_constraintdef`` says ``NOT VALID``. A check without that key
    was reflected as valid. Two predicates that normalize together keep
    the worse gap.
    """
    from services.foreign_key_metadata import postgres_check_validation_gap

    if not checks:
        return ()
    proof: dict[str, str] = {}
    for check in checks:
        if not isinstance(check, Mapping):
            continue
        predicate = _normalize_predicate(check.get("sqltext"))
        if not predicate:
            continue
        options = check.get("dialect_options") or {}
        if isinstance(options, Mapping) and "not_valid" in options:
            gap = postgres_check_validation_gap(bool(options.get("not_valid")))
        else:
            gap = ""
        _worse_check_gap(proof, predicate, gap)
    return tuple(sorted(proof.items()))


def _sqlserver_check_proof(
    checks: list[Any] | None,
) -> tuple[tuple[str, str], ...]:
    """One gap per reflected SQL Server check.

    A row that omits ``is_disabled`` and ``is_not_trusted`` is
    ``unreported``. Disabled wins over untrusted on the same predicate.
    """
    from services.foreign_key_metadata import (
        coerce_validated,
        sqlserver_check_validation_gap,
    )

    if not checks:
        return ()
    proof: dict[str, str] = {}
    for check in checks:
        if not isinstance(check, Mapping):
            continue
        predicate = _normalize_predicate(check.get("sqltext"))
        if not predicate:
            continue
        if "disabled" in check or "not_trusted" in check:
            gap = sqlserver_check_validation_gap(
                coerce_validated(check.get("disabled")) if "disabled" in check else None,
                coerce_validated(check.get("not_trusted"))
                if "not_trusted" in check
                else None,
            )
        else:
            gap = "unreported"
        _worse_check_gap(proof, predicate, gap)
    return tuple(sorted(proof.items()))


def _oracle_check_proof(
    checks: list[Any] | None,
    rows: list[Any] | None,
) -> tuple[tuple[str, str], ...]:
    """One gap per reflected Oracle check.

    ``rows is None`` means ``STATUS`` and ``VALIDATED`` were not read.
    Each reflected predicate stays ``unreported``. A successful read that
    does not name a reflected predicate is the same gap. ``ENABLED`` and
    ``NOT VALIDATED`` is ``not_checked``. ``DISABLED`` is ``disabled``.
    """
    from services.foreign_key_metadata import (
        coerce_validated,
        oracle_check_validation_gap,
    )

    if not checks:
        return ()
    measured: dict[str, str] = {}
    if rows is not None:
        for row in rows:
            fields = tuple(row)
            if len(fields) < 3:
                continue
            predicate = _normalize_predicate(fields[0])
            if not predicate:
                continue
            gap = oracle_check_validation_gap(
                _oracle_status_enabled(fields[1]),
                coerce_validated(fields[2]),
            )
            _worse_check_gap(measured, predicate, gap)
    proof: dict[str, str] = {}
    for check in checks:
        if not isinstance(check, Mapping):
            continue
        predicate = _normalize_predicate(check.get("sqltext"))
        if not predicate:
            continue
        if rows is None or predicate not in measured:
            gap = "unreported"
        else:
            gap = measured[predicate]
        _worse_check_gap(proof, predicate, gap)
    return tuple(sorted(proof.items()))


def _check_validation_reason(destination_dialect: str, gap: str) -> str:
    """Sentence for a measured check gap. Empty when this engine ignores it."""
    from services.dialect_profiles import is_sqlserver_like
    from services.foreign_key_metadata import (
        _dialect_key,
        oracle_check_validation_reason,
        postgres_check_validation_reason,
        postgres_index_catalog,
        sqlserver_check_validation_reason,
    )

    if postgres_index_catalog(destination_dialect):
        return postgres_check_validation_reason(gap)
    if is_sqlserver_like(destination_dialect):
        return sqlserver_check_validation_reason(gap)
    if _dialect_key(destination_dialect) == "oracle":
        return oracle_check_validation_reason(gap)
    return ""


def _check_proof_applies(destination_dialect: str) -> bool:
    from services.dialect_profiles import is_sqlserver_like
    from services.foreign_key_metadata import _dialect_key, postgres_index_catalog

    return (
        postgres_index_catalog(destination_dialect)
        or is_sqlserver_like(destination_dialect)
        or _dialect_key(destination_dialect) == "oracle"
    )


def _diff_check_constraints(
    source: frozenset,
    destination: frozenset,
    *,
    destination_dialect: str = "",
    check_proof: tuple[tuple[str, str], ...] = (),
) -> dict[str, Any]:
    """Carried when the predicates match and existing rows were checked.

    A ``NOT VALID``, untrusted, or ``NOT VALIDATED`` check still rejects a
    new row. A disabled check does not. Neither write rule is this verdict
    by itself: the certificate says whether existing rows were checked.
    An empty proof tuple means this comparison did not measure the flag,
    so a hand-built state stays on the older carried verdict.
    """
    base = _diff_sets(source, destination)
    if not check_proof or not _check_proof_applies(destination_dialect):
        return base
    proof = {predicate: gap for predicate, gap in check_proof}
    unchecked: list[str] = []
    reasons: list[str] = []
    for predicate in sorted(source & destination):
        gap = proof.get(str(predicate), "unreported")
        if not gap:
            continue
        unchecked.append(str(predicate))
        reason = _check_validation_reason(destination_dialect, gap)
        if reason and reason not in reasons:
            reasons.append(reason)
    if base["missing"]:
        status = "absent"
    elif unchecked:
        status = "unchecked"
    else:
        status = "carried"
    return {**base, "status": status, "unchecked": unchecked, "reasons": reasons}


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
        "primary_key": _diff_uniqueness(
            frozenset({source.primary_key} if source.primary_key else set()),
            frozenset({destination.primary_key} if destination.primary_key else set()),
            destination_dialect=destination.dialect,
            table_kind=destination.table_kind,
            index_status=destination.index_status,
            index_detail=destination.index_detail,
            uniqueness_proof=destination.uniqueness_proof,
        ),
        "unique_constraints": _diff_uniqueness(
            source.unique_constraints,
            destination.unique_constraints,
            destination_dialect=destination.dialect,
            table_kind=destination.table_kind,
            index_status=destination.index_status,
            index_detail=destination.index_detail,
            uniqueness_proof=destination.uniqueness_proof,
        ),
        "foreign_keys": _diff_foreign_keys(
            source.foreign_key_facts,
            destination.foreign_key_facts,
            destination_proof=destination.foreign_key_proof,
            destination_dialect=destination.dialect,
            destination_table_kind=destination.table_kind,
            index_detail=destination.index_detail,
            source_match=source.foreign_key_match,
            destination_match=destination.foreign_key_match,
            source_on_delete=source.foreign_key_on_delete,
            source_on_update=source.foreign_key_on_update,
            destination_on_delete=destination.foreign_key_on_delete,
            destination_on_update=destination.foreign_key_on_update,
            source_deferral=source.foreign_key_deferral,
            destination_deferral=destination.foreign_key_deferral,
        ),
        "indexes": _diff_indexes(source.indexes, destination.indexes),
        "not_null": _diff_sets(source.not_null, destination.not_null),
        "defaults": _diff_defaults(source.defaults, destination.defaults),
        "check_constraints": _diff_check_constraints(
            source.check_constraints,
            destination.check_constraints,
            destination_dialect=destination.dialect,
            check_proof=destination.check_proof,
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
    unchecked = [a for a, v in aspects.items() if v.get("unchecked")]
    blocking_unreadable = [a for a in unreadable if a not in advisory]
    return {
        "verified": not absent and not unchecked and not blocking_unreadable,
        "aspects": {**aspects, **advisory},
        "absent": absent,
        "unchecked": unchecked,
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
