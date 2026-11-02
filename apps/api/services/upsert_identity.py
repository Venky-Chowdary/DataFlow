"""How an upsert into one existing destination table will enforce key identity.

One answer, read by Validate's disclosure (``preflight_service`` gate
``g_upsert_dest_key``), the DDL writers (``postgresql_writer`` /
``mysql_writer``) and the root-cause remediation. The engine-level answer comes
from the capability registry (:func:`connectors.merge_registry.upsert_identity`);
this module turns it into the exact statements the writer will run on *this*
table, or a plain refusal when the key cannot be enforced at all:

* ``merge`` — the writer matches the key itself (Snowflake, SQL Server,
  BigQuery, Databricks, Oracle, ...). No DDL, ever: Snowflake has no unique
  indexes and does not enforce UNIQUE (D182).
* ``unique_index`` — PostgreSQL ``ON CONFLICT`` / MySQL ``ON DUPLICATE KEY``
  need a unique key on the full key value. The statements here are the ones the
  writer runs, so what Validate discloses is what the approval covers (D101).
  MySQL never gets a prefix index — two keys sharing the prefix would collide
  and overwrite each other (CR-D126): a TEXT key becomes ``VARCHAR(n)`` when its
  longest stored value fits the 3072-byte key limit.
* ``impossible`` — no full-value unique key can exist (a MySQL JSON/BLOB key, a
  TEXT key longer than the limit, a PostgreSQL ``json``/``xml`` key). Refused
  at Validate with the reason; no approval clears it.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from connectors.merge_registry import (
    UPSERT_IDENTITY_IMPOSSIBLE,
    UPSERT_IDENTITY_MERGE,
    UPSERT_IDENTITY_UNIQUE_INDEX,
    upsert_identity,
)
from connectors.sql_identifiers import sanitize_identifier
from services.brand import PRODUCT_NAME

#: ``query(sql, params) -> rows`` against the destination (DB-API ``%s`` style).
Query = Callable[[str, Sequence[Any]], list[tuple]]

# MySQL can't index these without a prefix length (error 1170), and a prefix
# is unique on the prefix only (CR-D126).
MYSQL_PREFIX_KEY_TYPES = frozenset(
    {"text", "tinytext", "mediumtext", "longtext", "blob", "tinyblob",
     "mediumblob", "longblob", "json"}
)
MYSQL_TEXT_KEY_TYPES = frozenset({"text", "tinytext", "mediumtext", "longtext"})
# InnoDB DYNAMIC/COMPRESSED index limit; utf8mb4 needs 4 bytes per character.
MYSQL_KEY_BYTES = 3072
MYSQL_KEY_CHAR_BYTES = 4

# PostgreSQL types with no default btree equality operator class: a unique
# index on them cannot be created ("could not identify an equality operator").
_PG_UNINDEXABLE_KEY_TYPES = frozenset({"json", "xml", "point", "polygon", "line", "circle"})


@dataclass(frozen=True)
class UpsertIdentityPlan:
    """What an upsert into one existing table needs, decided before any write."""

    mode: str
    ddl: tuple[str, ...] = ()
    reason: str = ""
    index_name: str = ""
    #: MySQL only: ``MODIFY`` clauses that make a TEXT key full-width.
    mysql_modify: tuple[str, ...] = field(default=())

    @property
    def disclosure(self) -> str:
        return "; ".join(self.ddl)


def upsert_index_name(table_name: str, key_columns: Sequence[str]) -> str:
    """The name the writers give the unique index an upsert creates."""
    return sanitize_identifier(f"uidx_{table_name}_{'_'.join(key_columns)}")


def _mysql_q(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def _pg_q(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def mysql_full_width_key_plan(
    query: Query,
    *,
    table_name: str,
    table_q: str,
    key_columns: Sequence[str],
) -> tuple[tuple[str, ...], str]:
    """``(MODIFY clauses, refusal)`` that make an existing MySQL key full-width.

    A TEXT key becomes ``VARCHAR(n)`` — ``n`` the widest the 3072-byte index
    limit allows — when every stored value fits. A longer value, or a JSON/BLOB
    key, is a refusal with the reason. Never a prefix.
    """
    rows = query(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_SET_NAME, "
        "COLLATION_NAME, CHARACTER_OCTET_LENGTH "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
        (table_name,),
    )
    info = {str(row[0]): row for row in rows}
    cols = list(key_columns)
    lob = [c for c in cols if c in info and str(info[c][1]).lower() in MYSQL_PREFIX_KEY_TYPES]
    if not lob:
        return (), ""
    key_label = ", ".join(cols)
    for col in lob:
        dtype = str(info[col][1]).lower()
        if dtype not in MYSQL_TEXT_KEY_TYPES:
            return (), (
                f"Upsert key ({key_label}) on existing table '{table_name}': "
                f"column {col} is {dtype.upper()}, which MySQL cannot index on "
                "its full value, and a prefix index would let distinct keys "
                "overwrite each other. Use a scalar key column (or a generated "
                "VARCHAR column extracted from it) as the upsert key."
            )
    other_bytes = sum(int(info[c][5] or 8) if c in info else 8 for c in cols if c not in lob)
    width = (MYSQL_KEY_BYTES - other_bytes) // MYSQL_KEY_CHAR_BYTES // len(lob)
    modify: list[str] = []
    for col in lob:
        quoted = _mysql_q(col)
        longest_rows = query(f"SELECT MAX(CHAR_LENGTH({quoted})) FROM {table_q}", ())  # nosec B608
        longest = int(((longest_rows or [(0,)])[0] or (0,))[0] or 0)
        if longest > width:
            return (), (
                f"Upsert key ({key_label}) on existing table '{table_name}': "
                f"{col} holds values up to {longest} characters, but MySQL can "
                f"enforce uniqueness on the full value only up to {width} "
                f"characters (VARCHAR({width}), {MYSQL_KEY_BYTES}-byte key "
                f"limit). {PRODUCT_NAME} will not index a prefix — distinct keys "
                "sharing it would overwrite each other. Use a shorter key column."
            )
        _name, _dtype, nullable, charset, collation, _octets = info[col]
        spec = f"VARCHAR({width})"
        if charset:
            spec += f" CHARACTER SET {charset}"
        if collation:
            spec += f" COLLATE {collation}"
        spec += " NULL" if str(nullable).upper() == "YES" else " NOT NULL"
        modify.append(f"MODIFY {quoted} {spec}")
    return tuple(modify), ""


def _pg_unindexable_key(
    query: Query, *, schema: str, table_name: str, key_columns: Sequence[str]
) -> str:
    rows = query(
        "SELECT column_name, udt_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table_name),
    )
    types = {str(r[0]): str(r[1] or "").lower() for r in rows}
    for col in key_columns:
        if types.get(col) in _PG_UNINDEXABLE_KEY_TYPES:
            return (
                f"Upsert key ({', '.join(key_columns)}) on existing table "
                f"'{schema}.{table_name}': column {col} is {types[col]}, which "
                "PostgreSQL cannot put in a unique index (no equality operator), "
                "so ON CONFLICT cannot key on it. Use a scalar key column, or "
                "change the column to jsonb."
            )
    return ""


def plan_upsert_identity(
    dest_kind: str,
    *,
    table_name: str,
    key_columns: Sequence[str],
    schema: str | None = None,
    query: Query | None = None,
) -> UpsertIdentityPlan:
    """The identity plan for an upsert into an existing table lacking a unique key.

    ``query`` reads the live destination; without it the plan carries only what
    the engine answer alone decides (an unmeasured key is not refused here —
    the writer re-plans against the live table before any DDL).
    """
    cols = [str(c) for c in key_columns if str(c or "").strip()]
    mode = upsert_identity(dest_kind)
    if mode == UPSERT_IDENTITY_MERGE or not cols:
        return UpsertIdentityPlan(mode=UPSERT_IDENTITY_MERGE)
    index_name = upsert_index_name(table_name, cols)
    kind = (dest_kind or "").strip().lower()
    if "mysql" in kind or "maria" in kind:
        table_q = _mysql_q(table_name)
        modify: tuple[str, ...] = ()
        if query is not None:
            modify, refusal = mysql_full_width_key_plan(
                query, table_name=table_name, table_q=table_q, key_columns=cols
            )
            if refusal:
                return UpsertIdentityPlan(
                    mode=UPSERT_IDENTITY_IMPOSSIBLE, reason=refusal, index_name=index_name
                )
        ddl: list[str] = []
        if modify:
            ddl.append(f"ALTER TABLE {table_q} {', '.join(modify)}")
        ddl.append(
            f"CREATE UNIQUE INDEX {_mysql_q(index_name)} ON {table_q} "
            f"({', '.join(_mysql_q(c) for c in cols)})"
        )
        return UpsertIdentityPlan(
            mode=UPSERT_IDENTITY_UNIQUE_INDEX,
            ddl=tuple(ddl),
            index_name=index_name,
            mysql_modify=modify,
        )
    sch = (schema or "public").strip() or "public"
    if query is not None:
        refusal = _pg_unindexable_key(query, schema=sch, table_name=table_name, key_columns=cols)
        if refusal:
            return UpsertIdentityPlan(
                mode=UPSERT_IDENTITY_IMPOSSIBLE, reason=refusal, index_name=index_name
            )
    return UpsertIdentityPlan(
        mode=UPSERT_IDENTITY_UNIQUE_INDEX,
        ddl=(
            f"CREATE UNIQUE INDEX {_pg_q(index_name)} ON {_pg_q(sch)}.{_pg_q(table_name)} "
            f"({', '.join(_pg_q(c) for c in cols)})",
        ),
        index_name=index_name,
    )


def plan_from_destination_config(
    dest_kind: str,
    cfg: dict[str, Any],
    *,
    table: str,
    key_columns: Sequence[str],
) -> UpsertIdentityPlan:
    """:func:`plan_upsert_identity` against the live destination ``cfg``."""
    if upsert_identity(dest_kind) == UPSERT_IDENTITY_MERGE:
        return UpsertIdentityPlan(mode=UPSERT_IDENTITY_MERGE)
    from connectors.generic_sql import _engine
    from connectors.sql_identifiers import split_qualified_table
    from services.sql_object_identity import resolve_object_identity

    engine = _engine(cfg)
    schema, bare = split_qualified_table(table, (cfg.get("schema") or "").strip() or None)
    ident = resolve_object_identity(engine, bare, schema, columns=list(key_columns))
    if ident.exists:
        bare = ident.table
        schema = ident.schema or schema
        key_columns = [str(ident.columns.get(c) or c) for c in key_columns]
    with engine.connect() as conn:

        def query(sql: str, params: Sequence[Any]) -> list[tuple]:
            return [tuple(r) for r in conn.exec_driver_sql(sql, tuple(params)).fetchall()]

        return plan_upsert_identity(
            dest_kind,
            table_name=bare,
            key_columns=key_columns,
            schema=schema,
            query=query,
        )


# The widest full-value key each engine can enforce on an unbounded text key:
# (key byte limit, bytes per character of the carrier). A text key column the
# source declares no width for is created at that width — never a prefix, never
# a LOB the engine refuses in a key (MySQL 1170, SQL Server 1919, ORA-02329).
_FULL_VALUE_KEY_BUDGET: dict[str, tuple[int, int]] = {
    "mysql": (MYSQL_KEY_BYTES, MYSQL_KEY_CHAR_BYTES),
    "mariadb": (MYSQL_KEY_BYTES, MYSQL_KEY_CHAR_BYTES),
    # Clustered index key limit; NVARCHAR stores 2 bytes per character.
    "mssql": (900, 2),
    "sqlserver": (900, 2),
    # VARCHAR2 limit with MAX_STRING_SIZE=STANDARD; AL32UTF8 up to 4 bytes/char.
    "oracle": (4000, 4),
}


def full_value_key_width(dialect: str, *, unbounded_keys: int, other_key_bytes: int = 0) -> int:
    """Characters an unbounded text key column gets, or 0 when none fit."""
    budget = _FULL_VALUE_KEY_BUDGET.get((dialect or "").strip().lower())
    if not budget or unbounded_keys <= 0:
        return 0
    limit, per_char = budget
    return max(0, (limit - max(0, other_key_bytes)) // per_char // unbounded_keys)


def unbounded_key_widths(
    dialect: str,
    key_columns: Sequence[str],
    type_of: Callable[[str], Any],
) -> dict[str, Any]:
    """Bounded SQLAlchemy key types for the unbounded text keys of a CREATE.

    ``type_of(col)`` is the type the writer would emit. A text key with no
    length (``VARCHAR(max)`` / ``CLOB``) is a LOB the engine refuses in a key;
    it gets the widest full-value width (:func:`full_value_key_width`), the
    other key columns' bytes taken out of the budget first. Never a prefix.
    """
    import sqlalchemy as sa

    d = (dialect or "").strip().lower()
    if d not in {"mssql", "oracle"} or not key_columns:
        return {}
    types = {c: type_of(c) for c in key_columns}
    unbounded = [
        c for c, t in types.items()
        if isinstance(t, (sa.String, sa.Text)) and getattr(t, "length", None) is None
    ]
    if not unbounded:
        return {}
    per_char = _FULL_VALUE_KEY_BUDGET[d][1]
    other_bytes = sum(
        int(getattr(t, "length", None) or 2) * per_char if isinstance(t, sa.String) else 8
        for c, t in types.items()
        if c not in unbounded
    )
    width = full_value_key_width(d, unbounded_keys=len(unbounded), other_key_bytes=other_bytes)
    if not width:
        return {}
    return {c: (sa.NVARCHAR(width) if d == "mssql" else sa.String(width)) for c in unbounded}
