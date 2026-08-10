"""FOREIGN KEY metadata — one measured shape for every SQL dialect.

Referential integrity is the one schema aspect a row-by-row transfer cannot
infer: it lives between tables, so a single-table create can neither carry nor
disprove it. Until now only PostgreSQL and MySQL introspection read foreign
keys at all, and the fidelity certificate had to say "not introspected" on SQL
Server, Oracle and SQLite — honest, but it also meant a destination could be
declared faithful while every parent/child relationship was gone.

This module measures the foreign keys of one table and says whether it managed
to. ``status="unavailable"`` keeps "the catalog could not be read" distinct
from "this table has no foreign keys": the second is proof, the first is not.

Shape mirrors ``services.physical_storage_metadata``: one connector-agnostic
entry point with a per-dialect catalog query, reused by source introspection,
by the carry planner and by the post-load destination re-read, so all three
compare like for like.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from services.physical_storage_metadata import as_driver_cursor

logger = logging.getLogger(__name__)

ForeignKeyStatus = Literal["measured", "unavailable"]

SUPPORTED_DIALECTS = frozenset({
    "postgresql",
    "redshift",
    "mysql",
    "mariadb",
    "sqlserver",
    "mssql",
    "oracle",
    "sqlite",
})

# Referential actions we reproduce verbatim. Anything else (SET DEFAULT on
# engines that only parse it, Oracle's absent ON UPDATE) is reported, never
# silently downgraded to NO ACTION.
KNOWN_ACTIONS = frozenset({"NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT"})


@dataclass(frozen=True)
class ForeignKey:
    """One foreign key as the source catalog holds it."""

    name: str
    columns: list[str]
    referenced_schema: str
    referenced_table: str
    referenced_columns: list[str]
    on_delete: str = ""
    on_update: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ForeignKeys:
    """Measured foreign keys of one table, or an honest failure to measure."""

    dialect: str
    status: ForeignKeyStatus
    table: str = ""
    schema: str = ""
    detail: str = ""
    items: list[ForeignKey] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dialect": self.dialect,
            "status": self.status,
            "table": self.table,
            "schema": self.schema,
            "detail": self.detail,
            "items": [i.to_dict() for i in self.items],
        }

    @property
    def measured(self) -> bool:
        return self.status == "measured"


def _unavailable(dialect: str, detail: str, schema: str, table: str) -> ForeignKeys:
    return ForeignKeys(
        dialect=dialect, status="unavailable", detail=detail, schema=schema, table=table
    )


def normalize_action(action: str | None) -> str:
    """Uppercase a referential action; empty when the catalog did not report one."""
    text = str(action or "").strip().upper().replace("_", " ")
    if not text or text == "NONE":
        return ""
    return text if text in KNOWN_ACTIONS else text


def _rows(cursor: Any, sql: str, params: tuple | dict) -> list[tuple]:
    cursor.execute(sql, params)
    return list(cursor.fetchall() or [])


def _rows_any_paramstyle(cursor: Any, sql: str, params: tuple) -> list[tuple]:
    """SQL Server arrives through pymssql (``%s``) and pyodbc (``?``) alike."""
    last: Exception | None = None
    for style in ("%s", "?"):
        try:
            return _rows(cursor, sql.replace("{p}", style), params)
        except Exception as exc:  # noqa: BLE001 — driver paramstyle fallback
            last = exc
    raise last if last else RuntimeError("no paramstyle attempted")


def _collect(
    rows: list[tuple],
) -> list[ForeignKey]:
    """Group ``(name, col, ref_schema, ref_table, ref_col, on_del, on_upd)`` rows.

    Rows must already be ordered by constraint then ordinal position: a
    composite key whose columns arrive out of order would build a constraint
    that references the wrong column pairs.
    """
    by_name: dict[str, dict[str, Any]] = {}
    for name, col, ref_schema, ref_table, ref_col, on_delete, on_update in rows:
        key = str(name or "").strip()
        if not key:
            continue
        bucket = by_name.setdefault(
            key,
            {
                "columns": [],
                "referenced_columns": [],
                "referenced_schema": str(ref_schema or "").strip(),
                "referenced_table": str(ref_table or "").strip(),
                "on_delete": normalize_action(on_delete),
                "on_update": normalize_action(on_update),
            },
        )
        col_s = str(col or "").strip()
        ref_s = str(ref_col or "").strip()
        if col_s:
            bucket["columns"].append(col_s)
        if ref_s:
            bucket["referenced_columns"].append(ref_s)
    return [
        ForeignKey(
            name=name,
            columns=list(b["columns"]),
            referenced_schema=str(b["referenced_schema"]),
            referenced_table=str(b["referenced_table"]),
            referenced_columns=list(b["referenced_columns"]),
            on_delete=str(b["on_delete"]),
            on_update=str(b["on_update"]),
        )
        for name, b in by_name.items()
    ]


_PG_SQL = """
SELECT con.conname,
       att.attname,
       nsp_ref.nspname,
       cls_ref.relname,
       att_ref.attname,
       con.confdeltype,
       con.confupdtype,
       ord.n
  FROM pg_constraint con
  JOIN pg_class cls ON cls.oid = con.conrelid
  JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
  JOIN pg_class cls_ref ON cls_ref.oid = con.confrelid
  JOIN pg_namespace nsp_ref ON nsp_ref.oid = cls_ref.relnamespace
  JOIN LATERAL generate_subscripts(con.conkey, 1) AS ord(n) ON TRUE
  JOIN pg_attribute att
    ON att.attrelid = con.conrelid AND att.attnum = con.conkey[ord.n]
  JOIN pg_attribute att_ref
    ON att_ref.attrelid = con.confrelid AND att_ref.attnum = con.confkey[ord.n]
 WHERE con.contype = 'f' AND nsp.nspname = %s AND cls.relname = %s
 ORDER BY con.conname, ord.n
"""

# pg_constraint stores the action as a single char; spelling it out here keeps
# the emitted DDL identical to what the source declared.
_PG_ACTIONS = {
    "a": "NO ACTION",
    "r": "RESTRICT",
    "c": "CASCADE",
    "n": "SET NULL",
    "d": "SET DEFAULT",
}


_DEFAULT_NAMESPACE_SQL = {
    "postgresql": "SELECT current_schema()",
    "mysql": "SELECT DATABASE()",
    "sqlserver": "SELECT SCHEMA_NAME()",
    "oracle": "SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM dual",
}


def _resolve_namespace(cursor: Any, dialect: str, schema: str) -> str:
    """The namespace the probe will actually read — never a blank one.

    A caller whose connector config keeps the namespace elsewhere (MySQL puts
    it in ``database``) used to hand an empty schema down here, and the catalog
    query then returned an empty *measured* answer: a carried foreign key read
    back as "not enforced on the destination". Resolving the session default is
    the only honest answer; an unresolvable one raises so the caller reports
    unknown rather than absent.
    """
    if schema:
        return schema
    sql = _DEFAULT_NAMESPACE_SQL.get(dialect)
    rows = _rows(cursor, sql, ()) if sql else []
    resolved = str(rows[0][0]) if rows and rows[0] and rows[0][0] else ""
    if not resolved:
        raise ValueError(
            f"No {dialect} schema/database bound on this connection; the foreign "
            "key catalog namespace is unknown, not empty."
        )
    return resolved


def _probe_postgres(cursor: Any, schema: str, table: str) -> ForeignKeys:
    schema = _resolve_namespace(cursor, "postgresql", schema)
    rows = _rows(cursor, _PG_SQL, (schema, table))
    mapped = [
        (
            name,
            col,
            ref_schema,
            ref_table,
            ref_col,
            _PG_ACTIONS.get(str(on_del or "").strip(), ""),
            _PG_ACTIONS.get(str(on_upd or "").strip(), ""),
        )
        for name, col, ref_schema, ref_table, ref_col, on_del, on_upd, _n in rows
    ]
    return ForeignKeys(
        dialect="postgresql",
        status="measured",
        schema=schema,
        table=table,
        items=_collect(mapped),
    )


_MYSQL_SQL = """
SELECT k.CONSTRAINT_NAME,
       k.COLUMN_NAME,
       k.REFERENCED_TABLE_SCHEMA,
       k.REFERENCED_TABLE_NAME,
       k.REFERENCED_COLUMN_NAME,
       r.DELETE_RULE,
       r.UPDATE_RULE
  FROM information_schema.KEY_COLUMN_USAGE k
  JOIN information_schema.REFERENTIAL_CONSTRAINTS r
    ON r.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
   AND r.CONSTRAINT_NAME = k.CONSTRAINT_NAME
   AND r.TABLE_NAME = k.TABLE_NAME
 WHERE k.TABLE_SCHEMA = %s
   AND k.TABLE_NAME = %s
   AND k.REFERENCED_TABLE_NAME IS NOT NULL
 ORDER BY k.CONSTRAINT_NAME, k.ORDINAL_POSITION
"""


def _probe_mysql(cursor: Any, schema: str, table: str) -> ForeignKeys:
    # MySQL has no schema layer above the database: the namespace lives in
    # ``database``, so an empty schema must resolve to the session's own.
    schema = _resolve_namespace(cursor, "mysql", schema)
    rows = _rows(cursor, _MYSQL_SQL, (schema, table))
    return ForeignKeys(
        dialect="mysql",
        status="measured",
        schema=schema,
        table=table,
        items=_collect([tuple(r) for r in rows]),
    )


_SQLSERVER_SQL = """
SELECT fk.name,
       cpa.name,
       SCHEMA_NAME(tref.schema_id),
       tref.name,
       cref.name,
       fk.delete_referential_action_desc,
       fk.update_referential_action_desc
  FROM sys.foreign_keys fk
  JOIN sys.tables t ON t.object_id = fk.parent_object_id
  JOIN sys.schemas s ON s.schema_id = t.schema_id
  JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
  JOIN sys.columns cpa
    ON cpa.object_id = fkc.parent_object_id
   AND cpa.column_id = fkc.parent_column_id
  JOIN sys.tables tref ON tref.object_id = fk.referenced_object_id
  JOIN sys.columns cref
    ON cref.object_id = fkc.referenced_object_id
   AND cref.column_id = fkc.referenced_column_id
 WHERE s.name = {p} AND t.name = {p}
 ORDER BY fk.name, fkc.constraint_column_id
"""


def _probe_sqlserver(cursor: Any, schema: str, table: str) -> ForeignKeys:
    schema = _resolve_namespace(cursor, "sqlserver", schema)
    rows = _rows_any_paramstyle(cursor, _SQLSERVER_SQL, (schema, table))
    return ForeignKeys(
        dialect="sqlserver",
        status="measured",
        schema=schema,
        table=table,
        items=_collect([tuple(r) for r in rows]),
    )


_ORACLE_SQL = """
SELECT c.constraint_name,
       cc.column_name,
       rc.owner,
       rc.table_name,
       rcc.column_name,
       c.delete_rule,
       'NO ACTION'
  FROM all_constraints c
  JOIN all_cons_columns cc
    ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
  JOIN all_constraints rc
    ON rc.owner = c.r_owner AND rc.constraint_name = c.r_constraint_name
  JOIN all_cons_columns rcc
    ON rcc.owner = rc.owner
   AND rcc.constraint_name = rc.constraint_name
   AND rcc.position = cc.position
 WHERE c.constraint_type = 'R' AND c.owner = :owner AND c.table_name = :tab
 ORDER BY c.constraint_name, cc.position
"""


def _probe_oracle(cursor: Any, schema: str, table: str) -> ForeignKeys:
    # Oracle folds unquoted identifiers to upper case in the catalog; a lower
    # case argument would silently measure "no foreign keys".
    schema = _resolve_namespace(cursor, "oracle", schema)
    rows = _rows(
        cursor, _ORACLE_SQL, {"owner": schema.upper(), "tab": table.upper()}
    )
    return ForeignKeys(
        dialect="oracle",
        status="measured",
        schema=schema,
        table=table,
        items=_collect([tuple(r) for r in rows]),
    )


def _probe_sqlite(cursor: Any, schema: str, table: str) -> ForeignKeys:
    """``PRAGMA foreign_key_list`` — id, seq, table, from, to, on_update, on_delete."""
    from connectors.writer_common import quote_sql_identifier

    rows = _rows(cursor, f"PRAGMA foreign_key_list({quote_sql_identifier(table)})", ())
    mapped: list[tuple] = []
    for row in rows:
        fk_id, _seq, ref_table, from_col, to_col, on_update, on_delete = list(row)[:7]
        mapped.append(
            (
                f"fk_{table}_{fk_id}",
                from_col,
                "",
                ref_table,
                # SQLite reports NULL for "references the parent's primary key".
                to_col if to_col is not None else "",
                on_delete,
                on_update,
            )
        )
    return ForeignKeys(
        dialect="sqlite",
        status="measured",
        schema=schema,
        table=table,
        items=_collect(mapped),
    )


_PROBES = {
    "postgresql": _probe_postgres,
    "redshift": _probe_postgres,
    "mysql": _probe_mysql,
    "mariadb": _probe_mysql,
    "sqlserver": _probe_sqlserver,
    "mssql": _probe_sqlserver,
    "oracle": _probe_oracle,
    "sqlite": _probe_sqlite,
}


def probe_foreign_keys(
    dialect: str, cursor_or_connection: Any, schema: str, table: str
) -> ForeignKeys:
    """Measure the foreign keys of ``schema.table``, or report why it could not."""
    key = (dialect or "").strip().lower()
    probe = _PROBES.get(key)
    if probe is None:
        return _unavailable(
            key,
            f"Foreign key catalog probe not implemented for '{key or 'unknown'}'.",
            schema,
            table,
        )
    if not table:
        return _unavailable(key, "No table name supplied.", schema, table)
    try:
        cursor = as_driver_cursor(cursor_or_connection)
        return probe(cursor, schema or "", table)
    except Exception as exc:  # noqa: BLE001 — an unreadable catalog is a state
        logger.debug("foreign key probe failed on %s: %s", key, exc, exc_info=exc)
        return _unavailable(key, f"{type(exc).__name__}: {exc}", schema, table)


def foreign_keys_from_payload(payload: Any) -> list[ForeignKey]:
    """Rebuild foreign keys from a serialized catalog payload.

    Accepts both this module's shape and the older introspect dicts that carry
    no referential actions, so an existing catalog keeps working rather than
    losing its keys to a schema mismatch.
    """
    items: Any = payload
    if isinstance(payload, dict):
        items = payload.get("items") or payload.get("foreign_keys") or []
    out: list[ForeignKey] = []
    for entry in items or []:
        if not isinstance(entry, dict):
            continue
        columns = [str(c) for c in (entry.get("columns") or []) if c]
        ref_columns = [str(c) for c in (entry.get("referenced_columns") or []) if c]
        if not columns:
            continue
        out.append(
            ForeignKey(
                name=str(entry.get("name") or entry.get("constraint") or ""),
                columns=columns,
                referenced_schema=str(entry.get("referenced_schema") or ""),
                referenced_table=str(entry.get("referenced_table") or ""),
                referenced_columns=ref_columns,
                on_delete=normalize_action(entry.get("on_delete")),
                on_update=normalize_action(entry.get("on_update")),
            )
        )
    return out
