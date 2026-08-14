"""Read source secondary indexes and carry the portable ones onto create-new.

Same split, and the same honesty contract, as ``services.check_constraints``:

``probe_secondary_indexes``
    Reads the source catalog. A catalog that cannot be read reports
    ``status="unavailable"`` — never an empty tuple, which would certify that a
    table has no indexes when we simply could not look.

``plan_index_carry``
    Decides, per index, whether the destination can hold the *same* index. An
    index is not only a performance object: a UNIQUE index is a data-integrity
    guarantee, and a filtered index enforces uniqueness over a subset. So a
    predicate we cannot re-render, an expression we cannot evaluate, or a
    covering clause the destination has no equivalent for is refused with a
    reason rather than degraded silently into "some index".

Primary keys and unique constraints are carried by the create-new planner as
constraints. Their backing indexes are recognised here and reported as skipped
so an operator reading the certificate does not see the same guarantee twice.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Literal

logger = logging.getLogger(__name__)

IndexStatus = Literal["measured", "unavailable"]

# Descending key columns: PostgreSQL, SQL Server and Oracle store a per-column
# direction; MySQL parses DESC and, from 8.0, honours it.
_DESC_DIALECTS = {"postgresql", "mysql", "sqlserver", "oracle", "sqlite"}
# Partial/filtered indexes.
_PREDICATE_DIALECTS = {"postgresql", "sqlserver", "sqlite"}
# Non-key/covering columns.
_INCLUDE_DIALECTS = {"postgresql", "sqlserver"}

_MAX_INDEX_NAME = {
    "postgresql": 63,
    "sqlite": 63,
    "mysql": 64,
    "sqlserver": 128,
    "oracle": 128,
}


@dataclass(frozen=True)
class IndexColumn:
    name: str
    descending: bool = False
    #: PostgreSQL operator class (``varchar_pattern_ops``, ``jsonb_path_ops``, …).
    #: Empty means the type default. Recreating the index without it is a
    #: different index, so this travels with the column rather than being
    #: stripped at parse time.
    opclass: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = {"name": self.name, "descending": self.descending}
        if self.opclass:
            payload["opclass"] = self.opclass
        return payload


@dataclass(frozen=True)
class SourceIndex:
    """One index as the source catalog reports it."""

    name: str
    columns: tuple[IndexColumn, ...]
    unique: bool = False
    predicate: str = ""
    include_columns: tuple[str, ...] = ()
    # Expression/functional indexes and engine-specific access methods carry
    # semantics no portable CREATE INDEX can reproduce.
    expression: str = ""
    method: str = ""
    # Backs a PRIMARY KEY or UNIQUE constraint the create-new planner emits.
    constraint_backed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "columns": [c.to_dict() for c in self.columns],
            "unique": self.unique,
            "predicate": self.predicate,
            "include_columns": list(self.include_columns),
            "expression": self.expression,
            "method": self.method,
            "constraint_backed": self.constraint_backed,
        }


@dataclass(frozen=True)
class SourceIndexes:
    """Secondary indexes for one table, or an explicit "could not read"."""

    dialect: str
    status: IndexStatus
    detail: str = ""
    items: tuple[SourceIndex, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "dialect": self.dialect,
            "status": self.status,
            "detail": self.detail,
            "items": [i.to_dict() for i in self.items],
        }


@dataclass
class IndexCarryDecision:
    """What happened to one source index, and why."""

    source: SourceIndex
    carried: bool
    dest_sql: str = ""
    dest_name: str = ""
    reason: str = ""
    skipped: bool = False
    dropped_include: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source.to_dict(),
            "carried": self.carried,
            "dest_sql": self.dest_sql,
            "dest_name": self.dest_name,
            "reason": self.reason,
            "skipped": self.skipped,
            "dropped_include": list(self.dropped_include),
        }


def canonical_dialect(dialect: str) -> str:
    d = (dialect or "").strip().lower()
    if d in {"postgres", "redshift", "timescale", "cockroach", "cockroachdb", "yugabytedb"}:
        return "postgresql"
    if d in {"mssql", "azuresql", "azure_sql"}:
        return "sqlserver"
    if d == "mariadb":
        return "mysql"
    if d == "oracledb":
        return "oracle"
    return d


# ---------------------------------------------------------------------------
# catalog probes
# ---------------------------------------------------------------------------


def _rows(cursor: Any, sql: str, params: tuple[Any, ...] | dict[str, Any]) -> list[tuple]:
    cursor.execute(sql, params)
    return list(cursor.fetchall() or [])


def probe_secondary_indexes(
    dialect: str,
    cursor: Any,
    schema: str,
    table: str,
) -> SourceIndexes:
    """Read indexes for ``schema.table``; never raise, never guess."""
    d = canonical_dialect(dialect)
    reader: Callable[[Any, str, str], SourceIndexes] | None = {
        "postgresql": _postgres_indexes,
        "mysql": _mysql_indexes,
        "sqlserver": _sqlserver_indexes,
        "oracle": _oracle_indexes,
        "sqlite": _sqlite_indexes,
    }.get(d)
    if reader is None:
        return SourceIndexes(
            dialect=d or "unknown",
            status="unavailable",
            detail=f"No index catalog reader for dialect {d or 'unknown'!r}.",
        )
    try:
        from services.physical_storage_metadata import as_driver_cursor

        return reader(as_driver_cursor(cursor), schema, table)
    except Exception as exc:  # noqa: BLE001 — a refused catalog is evidence
        return SourceIndexes(
            dialect=d,
            status="unavailable",
            detail=(
                f"{d} index catalog unreadable ({exc}); this is not proof that the "
                "table has no secondary indexes."
            ),
        )


def _postgres_indexes(cursor: Any, schema: str, table: str) -> SourceIndexes:
    rows = _rows(
        cursor,
        """
        SELECT i.relname,
               ix.indisunique,
               ix.indisprimary,
               (con.conname IS NOT NULL) AS constraint_backed,
               am.amname,
               pg_get_expr(ix.indpred, ix.indrelid, true) AS predicate,
               pg_get_indexdef(ix.indexrelid) AS definition,
               ix.indnkeyatts,
               ix.indnatts,
               ARRAY(
                 SELECT pg_get_indexdef(ix.indexrelid, k + 1, true)
                 FROM generate_series(0, ix.indnatts - 1) k
               ) AS key_exprs,
               ix.indoption,
               ARRAY(
                 SELECT CASE WHEN opc.opcdefault THEN '' ELSE opc.opcname END
                 FROM unnest(ix.indclass) WITH ORDINALITY AS k(oid, n)
                 JOIN pg_opclass opc ON opc.oid = k.oid
                 WHERE k.n <= GREATEST(ix.indnkeyatts, 1)
                 ORDER BY k.n
               ) AS opclasses
        FROM pg_index ix
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_am am ON am.oid = i.relam
        LEFT JOIN pg_constraint con
               ON con.conindid = ix.indexrelid AND con.contype IN ('p', 'u')
        WHERE t.relname = %s AND n.nspname = %s
        """,
        (table, schema or "public"),
    )
    items: list[SourceIndex] = []
    for (
        name,
        is_unique,
        is_primary,
        constraint_backed,
        method,
        predicate,
        _definition,
        n_key_atts,
        _n_atts,
        key_exprs,
        indoption,
        opclasses,
    ) in rows:
        exprs = list(key_exprs or [])
        options = _int_vector(indoption)
        opc_names = [str(x or "") for x in (opclasses or [])]
        keys = exprs[: int(n_key_atts or len(exprs))]
        include = tuple(_strip_quotes(e) for e in exprs[int(n_key_atts or len(exprs)) :])
        columns: list[IndexColumn] = []
        expression = ""
        for pos, raw in enumerate(keys):
            parsed = _split_index_key(raw)
            if parsed is None:
                expression = expression or raw
                continue
            ident, parsed_op = parsed
            # Catalog opclass is authoritative; pg_get_indexdef per attribute
            # strips it. Only a non-default class is stored (empty = type default).
            opclass = opc_names[pos] if pos < len(opc_names) and opc_names[pos] else parsed_op
            # indoption bit 0 is DESC.
            desc = bool(options[pos] & 1) if pos < len(options) else False
            columns.append(IndexColumn(ident, descending=desc, opclass=opclass))
        items.append(
            SourceIndex(
                name=str(name),
                columns=tuple(columns),
                unique=bool(is_unique),
                predicate=str(predicate or ""),
                include_columns=include,
                expression=expression,
                method=str(method or ""),
                constraint_backed=bool(constraint_backed) or bool(is_primary),
            )
        )
    return SourceIndexes(dialect="postgresql", status="measured", items=tuple(items))


def _int_vector(value: Any) -> list[int]:
    """``indoption`` is an int2vector; drivers hand it back as ``'0 1'`` or a list."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        items: list[Any] = list(value)
    else:
        items = str(value).replace(",", " ").split()
    out: list[int] = []
    for item in items:
        try:
            out.append(int(item))
        except (TypeError, ValueError):
            out.append(0)
    return out


#: Tokens that may follow a bare column in ``pg_get_indexdef`` without changing
#: the index into a different object (sort direction). An operator class is
#: captured separately — it *does* change the index.
_DIRECTION_TOKENS = frozenset({"desc", "asc", "nulls", "first", "last"})
#: Access methods PostgreSQL can reproduce with ``USING``.
_PG_ACCESS_METHODS = frozenset({"btree", "hash", "gist", "gin", "brin", "spgist"})


def _split_index_key(index_key_sql: str) -> tuple[str, str] | None:
    """``(column, opclass)`` when the key is a column, else ``None`` (expression).

    ``pg_get_indexdef`` for one attribute yields ``col``, ``"Col"``, ``col DESC``,
    ``col varchar_pattern_ops``, or an expression like ``lower(col)``. The
    operator class is part of the index identity and must not be dropped.
    """
    text = (index_key_sql or "").strip()
    if not text:
        return None
    if text.startswith('"'):
        end = text.find('"', 1)
        if end == -1:
            return None
        ident = text[1:end]
        rest = text[end + 1 :].strip()
    else:
        parts = text.split()
        head = parts[0]
        if "(" in head or ")" in head:
            return None
        ident = head
        rest = " ".join(parts[1:])
    opclass = ""
    leftovers: list[str] = []
    for tok in rest.split():
        if tok.lower() in _DIRECTION_TOKENS:
            continue
        leftovers.append(tok)
    if leftovers:
        if len(leftovers) != 1 or not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", leftovers[0]):
            return None
        opclass = leftovers[0]
    return ident, opclass


def _strip_quotes(ident: str) -> str:
    text = (ident or "").strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "`"}:
        return text[1:-1]
    if text.startswith("[") and text.endswith("]"):
        return text[1:-1]
    return text


def _mysql_indexes(cursor: Any, schema: str, table: str) -> SourceIndexes:
    rows = _rows(
        cursor,
        """
        SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME,
               COLLATION, INDEX_TYPE, EXPRESSION
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY INDEX_NAME, SEQ_IN_INDEX
        """,
        (schema, table),
    )
    grouped: dict[str, dict[str, Any]] = {}
    for name, non_unique, _seq, column, collation, index_type, expression in rows:
        entry = grouped.setdefault(
            str(name),
            {"unique": not int(non_unique or 0), "cols": [], "expr": "", "type": str(index_type or "")},
        )
        if expression:
            entry["expr"] = entry["expr"] or str(expression)
            continue
        entry["cols"].append(IndexColumn(str(column), descending=str(collation or "A").upper() == "D"))
    items = [
        SourceIndex(
            name=name,
            columns=tuple(v["cols"]),
            unique=bool(v["unique"]),
            expression=str(v["expr"]),
            method=str(v["type"]),
            # MySQL has no standalone unique constraint: PRIMARY and any
            # UNIQUE index is the constraint.
            constraint_backed=name == "PRIMARY" or bool(v["unique"]),
        )
        for name, v in grouped.items()
    ]
    return SourceIndexes(dialect="mysql", status="measured", items=tuple(items))


def _sqlserver_indexes(cursor: Any, schema: str, table: str) -> SourceIndexes:
    rows = _rows(
        cursor,
        """
        SELECT i.name, i.is_unique, i.is_primary_key, i.is_unique_constraint,
               i.type_desc, i.filter_definition,
               c.name, ic.is_descending_key, ic.is_included_column, ic.key_ordinal
        FROM sys.indexes i
        JOIN sys.index_columns ic
          ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN sys.columns c
          ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        JOIN sys.tables t ON t.object_id = i.object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE t.name = %s AND s.name = %s AND i.name IS NOT NULL
        ORDER BY i.name, ic.is_included_column, ic.key_ordinal
        """,
        (table, schema or "dbo"),
    )
    grouped: dict[str, dict[str, Any]] = {}
    for (
        name,
        is_unique,
        is_pk,
        is_uc,
        type_desc,
        filter_def,
        column,
        is_desc,
        is_included,
        _ordinal,
    ) in rows:
        entry = grouped.setdefault(
            str(name),
            {
                "unique": bool(is_unique),
                "constraint": bool(is_pk) or bool(is_uc),
                "type": str(type_desc or ""),
                "filter": str(filter_def or ""),
                "cols": [],
                "include": [],
            },
        )
        if int(is_included or 0):
            entry["include"].append(str(column))
        else:
            entry["cols"].append(IndexColumn(str(column), descending=bool(int(is_desc or 0))))
    items = [
        SourceIndex(
            name=name,
            columns=tuple(v["cols"]),
            unique=bool(v["unique"]),
            predicate=str(v["filter"]),
            include_columns=tuple(v["include"]),
            method=str(v["type"]),
            constraint_backed=bool(v["constraint"]),
        )
        for name, v in grouped.items()
    ]
    return SourceIndexes(dialect="sqlserver", status="measured", items=tuple(items))


def _oracle_indexes(cursor: Any, schema: str, table: str) -> SourceIndexes:
    owner = (schema or "").upper()
    rows = _rows(
        cursor,
        """
        SELECT i.index_name, i.uniqueness, i.index_type,
               c.column_name, c.descend, c.column_position,
               (SELECT COUNT(*) FROM all_constraints k
                 WHERE k.owner = i.owner AND k.index_name = i.index_name
                   AND k.constraint_type IN ('P', 'U')) AS constraint_backed
        FROM all_indexes i
        JOIN all_ind_columns c
          ON c.index_owner = i.owner AND c.index_name = i.index_name
        WHERE i.table_name = :tbl AND i.owner = :own
        ORDER BY i.index_name, c.column_position
        """,
        {"tbl": (table or "").upper(), "own": owner},
    )
    # Oracle materialises both DESC key columns and functional keys as hidden
    # SYS_NC columns, so the column name alone cannot tell them apart. The
    # expression text does: ``"REGION"`` is a descending column, ``LOWER(...)``
    # is a function.
    expressions: dict[tuple[str, int], str] = {}
    try:
        for idx_name, expr, pos in _rows(
            cursor,
            """
            SELECT index_name, column_expression, column_position
            FROM all_ind_expressions
            WHERE table_name = :tbl AND index_owner = :own
            """,
            {"tbl": (table or "").upper(), "own": owner},
        ):
            expressions[(str(idx_name), int(pos))] = str(expr or "")
    except Exception:  # noqa: BLE001 — absent expressions are not an error
        logger.debug("all_ind_expressions unreadable for %s.%s", owner, table, exc_info=True)

    grouped: dict[str, dict[str, Any]] = {}
    for name, uniqueness, index_type, column, descend, _pos, constraint_backed in rows:
        entry = grouped.setdefault(
            str(name),
            {
                "unique": str(uniqueness or "").upper() == "UNIQUE",
                "type": str(index_type or ""),
                "constraint": bool(int(constraint_backed or 0)),
                "cols": [],
                "expr": "",
            },
        )
        col = str(column)
        descending = str(descend or "ASC").upper() == "DESC"
        if col.startswith("SYS_NC"):
            plain = _oracle_plain_column(expressions.get((str(name), int(_pos or 0)), ""))
            if plain is None:
                entry["expr"] = entry["expr"] or (
                    expressions.get((str(name), int(_pos or 0))) or col
                )
                continue
            col = plain
        entry["cols"].append(IndexColumn(col, descending=descending))
    items = [
        SourceIndex(
            name=name,
            columns=tuple(v["cols"]),
            unique=bool(v["unique"]),
            expression=str(v["expr"]),
            method=str(v["type"]),
            constraint_backed=bool(v["constraint"]),
        )
        for name, v in grouped.items()
    ]
    return SourceIndexes(dialect="oracle", status="measured", items=tuple(items))


_ORACLE_PLAIN_COLUMN = re.compile(r'^"([^"]+)"$')


def _oracle_plain_column(expression: str) -> str | None:
    """``"REGION"`` is a DESC key column; ``LOWER("STATUS")`` is a function."""
    match = _ORACLE_PLAIN_COLUMN.match((expression or "").strip())
    return match.group(1) if match else None


def _sqlite_indexes(cursor: Any, schema: str, table: str) -> SourceIndexes:
    index_rows = _rows(cursor, f"PRAGMA index_list({_sqlite_quote(table)})", ())
    items: list[SourceIndex] = []
    for row in index_rows:
        # (seq, name, unique, origin, partial)
        name = str(row[1])
        unique = bool(int(row[2] or 0))
        origin = str(row[3] or "")
        partial = bool(int(row[4] or 0)) if len(row) > 4 else False
        info = _rows(cursor, f"PRAGMA index_xinfo({_sqlite_quote(name)})", ())
        columns: list[IndexColumn] = []
        expression = ""
        for entry in info:
            # (seqno, cid, name, desc, coll, key)
            if not int(entry[5] or 0):
                continue
            col_name = entry[2]
            if col_name is None:
                expression = expression or f"expression in {name}"
                continue
            columns.append(IndexColumn(str(col_name), descending=bool(int(entry[3] or 0))))
        items.append(
            SourceIndex(
                name=name,
                columns=tuple(columns),
                unique=unique,
                # PRAGMA reports that an index is partial but not its WHERE
                # clause; an unreadable predicate must refuse the carry.
                predicate="<partial predicate not exposed by PRAGMA>" if partial else "",
                expression=expression,
                constraint_backed=origin in {"pk", "u"},
            )
        )
    return SourceIndexes(dialect="sqlite", status="measured", items=tuple(items))


def _sqlite_quote(ident: str) -> str:
    return '"' + str(ident).replace('"', '""') + '"'


# ---------------------------------------------------------------------------
# carry planning
# ---------------------------------------------------------------------------


def plan_index_carry(
    indexes: SourceIndexes | None,
    *,
    dest_dialect: str,
    dest_table: str,
    dest_schema: str = "",
    column_map: dict[str, str],
    quote: Callable[[str], str],
    pk_columns: list[str] | None = None,
    unique_constraints: list[list[str]] | None = None,
    check_renderer: Callable[[str], tuple[str, str]] | None = None,
) -> list[IndexCarryDecision]:
    """Decide carry/refuse per index. Callers still emit an aspect when empty.

    ``column_map`` maps source column names to destination names; a column the
    transfer does not carry is absent, and an index over it cannot be created.
    """
    if indexes is None or indexes.status != "measured":
        return []

    dest = canonical_dialect(dest_dialect)
    pk = [c for c in (pk_columns or [])]
    # Oracle folds unquoted identifiers to upper case, so a catalog column is
    # ``STATUS`` while the mapped name is ``status``. A case mismatch must not
    # be read as "the transfer does not carry this column".
    column_map = dict(column_map)
    for src, dst in list(column_map.items()):
        column_map.setdefault(src.casefold(), dst)
    uniques = [sorted(u) for u in (unique_constraints or [])]
    decisions: list[IndexCarryDecision] = []
    used_names: set[str] = set()

    for item in indexes.items:
        decision = _plan_one(
            item,
            dest=dest,
            dest_table=dest_table,
            dest_schema=dest_schema,
            column_map=column_map,
            quote=quote,
            pk=pk,
            uniques=uniques,
            used_names=used_names,
            check_renderer=check_renderer,
        )
        decisions.append(decision)
    return decisions


def _using_clause(dest: str, method: str) -> tuple[str, str]:
    """``(USING fragment, refuse_reason)``. Empty reason means the dest can hold it.

    The identity of an index includes its access method. PostgreSQL's
    ``CREATE INDEX`` without ``USING`` is btree; emitting that for a gin/gist
    source would look carried and enforce a different rule. When the destination
    can name the method, we do; when it cannot, we refuse rather than degrade.
    """
    am = (method or "btree").strip().lower()
    if dest == "postgresql":
        if am not in _PG_ACCESS_METHODS:
            return "", (
                f"access method {method!r} is not a PostgreSQL method this planner "
                "can reproduce"
            )
        if am == "btree":
            return "", ""
        return f" USING {am}", ""
    if dest in {"mysql", "mariadb"}:
        if am in {"", "btree"}:
            return "", ""
        if am == "hash":
            return " USING HASH", ""
        return "", (
            f"access method {method!r} has no equivalent CREATE INDEX on {dest}"
        )
    if dest == "sqlite":
        if am in {"", "btree"}:
            return "", ""
        return "", f"SQLite cannot reproduce access method {method!r}"
    # SQL Server / Oracle use a different CREATE shape; unknown methods stay
    # as catalog type_desc and are not rewritten here.
    return "", ""


def _opclass_supported(dest: str, columns: list[IndexColumn]) -> str:
    """Refuse reason when an operator class cannot be reproduced on ``dest``."""
    ops = [c.opclass for c in columns if c.opclass]
    if not ops:
        return ""
    if dest != "postgresql":
        return (
            f"operator class {ops[0]!r} is PostgreSQL-specific; {dest} has no "
            "equivalent and recreating the index without it would be a different index"
        )
    return ""


def _key_sql(col: IndexColumn, quote: Callable[[str], str]) -> str:
    piece = quote(col.name)
    if col.opclass:
        # Operator classes are catalog identifiers, not string literals.
        from connectors.sql_identifiers import require_safe_identifier

        piece += " " + require_safe_identifier(col.opclass, preserve_case=True)
    if col.descending:
        piece += " DESC"
    return piece


def _plan_one(
    item: SourceIndex,
    *,
    dest: str,
    dest_table: str,
    dest_schema: str,
    column_map: dict[str, str],
    quote: Callable[[str], str],
    pk: list[str],
    uniques: list[list[str]],
    used_names: set[str],
    check_renderer: Callable[[str], tuple[str, str]] | None,
) -> IndexCarryDecision:
    def refuse(reason: str) -> IndexCarryDecision:
        return IndexCarryDecision(source=item, carried=False, reason=reason)

    def skip(reason: str) -> IndexCarryDecision:
        return IndexCarryDecision(source=item, carried=False, skipped=True, reason=reason)

    if item.expression:
        return refuse(
            f"Index {item.name!r} is an expression/functional index "
            f"({item.expression!r}); the expression cannot be proven equivalent "
            f"on {dest} and is not carried."
        )
    if not item.columns:
        return refuse(
            f"Index {item.name!r} reported no key columns; refuse to invent one."
        )

    dest_cols: list[IndexColumn] = []
    for col in item.columns:
        mapped = column_map.get(col.name) or column_map.get(col.name.casefold())
        if not mapped:
            return refuse(
                f"Index {item.name!r} covers source column {col.name!r}, which the "
                "transfer does not carry to the destination."
            )
        dest_cols.append(
            IndexColumn(mapped, descending=col.descending, opclass=col.opclass)
        )

    using_sql, method_why = _using_clause(dest, item.method)
    if method_why:
        return refuse(method_why)
    opclass_why = _opclass_supported(dest, dest_cols)
    if opclass_why:
        return refuse(opclass_why)

    key_names = [c.name for c in dest_cols]
    # ``constraint_backed`` alone is not a reason to skip: the source index may
    # back a constraint the create-new DDL did *not* carry, and skipping it
    # then drops the guarantee while the certificate says it was covered.
    if sorted(key_names) == sorted(pk) or sorted(key_names) in uniques:
        return skip(
            f"Index {item.name!r} covers the same columns as the PRIMARY KEY / "
            "UNIQUE constraint already carried in the create-new DDL; not "
            "emitted twice."
        )

    if any(c.descending for c in dest_cols) and dest not in _DESC_DIALECTS:
        return refuse(
            f"Index {item.name!r} has descending key columns and {dest} does not "
            "support a per-column direction."
        )

    predicate_sql = ""
    if item.predicate:
        if dest not in _PREDICATE_DIALECTS:
            return refuse(
                f"Index {item.name!r} is partial/filtered "
                f"(WHERE {item.predicate}); {dest} has no filtered-index equivalent, "
                "and an unfiltered index would enforce a different rule."
            )
        if check_renderer is None:
            return refuse(
                f"Index {item.name!r} is partial/filtered and no predicate renderer "
                "was supplied to prove the filter means the same thing on the "
                "destination."
            )
        predicate_sql, why = check_renderer(item.predicate)
        if not predicate_sql:
            return refuse(
                f"Index {item.name!r} filter {item.predicate!r} is not portable to "
                f"{dest}: {why}"
            )

    dropped_include: tuple[str, ...] = ()
    include_sql = ""
    if item.include_columns:
        mapped_include = [column_map.get(c, "") for c in item.include_columns]
        if dest in _INCLUDE_DIALECTS and all(mapped_include):
            include_sql = ", ".join(quote(c) for c in mapped_include)
        else:
            # Covering columns change read cost, not the rule the index
            # enforces; carry the key and name what was left behind.
            dropped_include = tuple(item.include_columns)

    name = _dest_index_name(item.name, dest_table, dest, used_names)
    used_names.add(name)

    key_sql = ", ".join(_key_sql(c, quote) for c in dest_cols)
    target = f"{quote(dest_schema)}.{quote(dest_table)}" if dest_schema else quote(dest_table)
    sql = (
        f"CREATE {'UNIQUE ' if item.unique else ''}INDEX {quote(name)} "
        f"ON {target}{using_sql} ({key_sql})"
    )
    if include_sql:
        sql += f" INCLUDE ({include_sql})"
    if predicate_sql:
        sql += f" WHERE {predicate_sql}"

    reason = f"Index {item.name!r} carried as {name!r}."
    if dropped_include:
        reason += (
            " Covering columns "
            + ", ".join(sorted(dropped_include))
            + f" were not carried: {dest} has no INCLUDE equivalent for them. "
            "The index enforces the same rule; reads may cost more."
        )
    return IndexCarryDecision(
        source=item,
        carried=True,
        dest_sql=sql,
        dest_name=name,
        reason=reason,
        dropped_include=dropped_include,
    )


def _dest_index_name(source_name: str, dest_table: str, dest: str, used: set[str]) -> str:
    """A destination-legal, collision-free index name.

    Oracle and SQL Server share index names across a schema, so a source name
    like ``idx_status`` on two tables would collide; qualify with the table.
    """
    from connectors.sql_identifiers import sanitize_identifier

    cap = _MAX_INDEX_NAME.get(dest, 63)
    base = sanitize_identifier(f"{dest_table}_{source_name}", preserve_case=False, max_len=cap)
    if not base:
        base = sanitize_identifier(f"ix_{dest_table}", preserve_case=False, max_len=cap) or "ix"
    candidate = base
    n = 1
    while candidate in used:
        suffix = f"_{n}"
        candidate = base[: max(1, cap - len(suffix))] + suffix
        n += 1
    return candidate
