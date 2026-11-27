"""Exactness checks and operator steps for putting an overwrite's pre-run rows back.

An overwrite keeps the destination's pre-run rows in a backup until its load
is accepted. Putting them back automatically is only right when the result is
exactly the pre-run table:

* MySQL copies the rows back with ``DELETE`` + ``INSERT ... SELECT`` inside the
  live table (its keys, CHECKs, FKs and triggers are the customer's). That
  fires the table's triggers again (a ``BEFORE INSERT`` rewrites a version or
  timestamp, an ``AFTER`` trigger writes audit rows) and leaves
  ``AUTO_INCREMENT`` where the cancelled load moved it. A table with triggers
  is therefore not restored automatically: the backup is kept and the
  operator gets the exact statements (as D122 refuses a PostgreSQL table whose
  triggers cannot be suspended). Otherwise the pre-run ``AUTO_INCREMENT``,
  carried on the backup table, is put back too.
* When the automatic restore is refused or fails, :func:`manual_restore_steps`
  names the backup and gives statements that work on the real table: quoted
  identifiers, an explicit list of the stored columns (a generated column
  cannot be inserted: MariaDB 1906, PostgreSQL GENERATED ALWAYS), and
  ``OVERRIDING SYSTEM VALUE`` for a PostgreSQL ``GENERATED ALWAYS`` identity.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def _bq(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def _dq(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


# --- MySQL / MariaDB ----------------------------------------------------------


def mysql_triggers(cur: Any, table: str) -> list[str]:
    """Names of the triggers defined on ``table`` in the current database."""
    cur.execute(
        "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS "
        "WHERE EVENT_OBJECT_SCHEMA = DATABASE() AND EVENT_OBJECT_TABLE = %s "
        "ORDER BY TRIGGER_NAME",
        (table,),
    )
    return [str(row[0]) for row in cur.fetchall()]


def mysql_auto_increment(cur: Any, table: str) -> int | None:
    """The table's next ``AUTO_INCREMENT`` value, read live (``None``: none)."""
    try:
        # MySQL 8 caches information_schema table statistics for a day;
        # MariaDB has no such variable and always reads live.
        cur.execute("SET SESSION information_schema_stats_expiry = 0")
    except Exception:  # noqa: BLE001 - MariaDB: the read below is live already
        logger.debug("information_schema_stats_expiry not settable", exc_info=True)
    cur.execute(
        "SELECT AUTO_INCREMENT FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
        (table,),
    )
    row = cur.fetchone()
    value = row[0] if row else None
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None  # no AUTO_INCREMENT column, or a value this check cannot read


def _auto_increment_sql(table: str, value: int) -> str:
    """``ALTER TABLE`` setting the counter: a quoted identifier and an integer only."""
    return "ALTER TABLE " + _bq(table) + " AUTO_INCREMENT = " + str(int(value))


def carry_mysql_auto_increment(cur: Any, live: str, backup: str) -> None:
    """Record the live table's pre-run ``AUTO_INCREMENT`` on its backup copy."""
    value = mysql_auto_increment(cur, live)
    if value is not None:
        cur.execute(_auto_increment_sql(backup, value))


def restore_mysql_auto_increment(cur: Any, live: str, backup: str) -> None:
    """Put the pre-run ``AUTO_INCREMENT`` (carried on the backup) back on ``live``."""
    value = mysql_auto_increment(cur, backup)
    if value is not None:
        cur.execute(_auto_increment_sql(live, value))


def mysql_restore_refusal(cur: Any, table: str) -> str:
    """Why copying the rows back would not reproduce the pre-run table ('' = exact)."""
    triggers = mysql_triggers(cur, table)
    if triggers:
        return (
            f"{table} has triggers ({', '.join(triggers)}): copying the pre-run rows "
            "back would fire them again"
        )
    return ""


def _mysql_steps(cur: Any, table: str, backup: str) -> list[str]:
    from connectors.table_manager import _mysql_stored_columns

    cols = ", ".join(_bq(c) for c in _mysql_stored_columns(cur, backup))
    triggers = mysql_triggers(cur, table)
    auto_inc = mysql_auto_increment(cur, backup)
    steps: list[str] = []
    if triggers:
        steps.append(
            "save each trigger's definition and drop it: "
            + "; ".join(f"SHOW CREATE TRIGGER {_bq(t)}; DROP TRIGGER {_bq(t)}" for t in triggers)
        )
    steps.append(
        f"START TRANSACTION; DELETE FROM {_bq(table)}; "
        f"INSERT INTO {_bq(table)} ({cols}) SELECT {cols} FROM {_bq(backup)}; COMMIT"
    )
    if auto_inc is not None:
        steps.append(f"ALTER TABLE {_bq(table)} AUTO_INCREMENT = {auto_inc}")
    if triggers:
        steps.append("re-create the triggers from the saved definitions")
    steps.append(f"DROP TABLE {_bq(backup)}")
    return steps


# --- PostgreSQL ---------------------------------------------------------------


def _pg_steps(cur: Any, schema: str, table: str, backup: str) -> list[str]:
    def columns(rel: str) -> list[tuple[str, str, str]]:
        cur.execute(
            "SELECT a.attname, a.attidentity, a.attgenerated FROM pg_attribute a "
            "WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped "
            "ORDER BY a.attnum",
            (f"{_dq(schema)}.{_dq(rel)}",),
        )
        return [(str(r[0]), str(r[1] or ""), str(r[2] or "")) for r in cur.fetchall()]

    live = {name: (ident, gen) for name, ident, gen in columns(table)}
    stored = [name for name, _i, gen in columns(backup) if not gen and not live.get(name, ("", ""))[1]]
    overriding = any(live.get(name, ("", ""))[0] == "a" for name in stored)
    cols = ", ".join(_dq(c) for c in stored)
    live_ref, bak_ref = f"{_dq(schema)}.{_dq(table)}", f"{_dq(schema)}.{_dq(backup)}"
    return [
        f"BEGIN; ALTER TABLE {live_ref} DISABLE TRIGGER USER; DELETE FROM {live_ref}; "
        f"INSERT INTO {live_ref} ({cols}) "
        + ("OVERRIDING SYSTEM VALUE " if overriding else "")
        + f"SELECT {cols} FROM {bak_ref}; "
        f"ALTER TABLE {live_ref} ENABLE TRIGGER USER; COMMIT",
        f"DROP TABLE {bak_ref}",
    ]


# --- generic SQL row copy (every other engine that keeps a row backup) -------

#: Engines with no triggers, identity or auto-increment state a row copy could
#: get wrong (a restore there is exact by construction).
NO_TRIGGER_ENGINES = frozenset(
    {"snowflake", "bigquery", "databricks", "clickhouse", "duckdb", "athena", "trino", "presto"}
)

#: ``{dialect: (triggers SQL, columns SQL)}``; columns rows are
#: ``(name, is_identity, is_generated)`` as text/bool/int.
_CATALOG_SQL: dict[str, tuple[str, str]] = {
    "postgresql": (
        "SELECT tgname FROM pg_trigger WHERE tgrelid = to_regclass(:ref) AND NOT tgisinternal "
        "ORDER BY tgname",
        "SELECT attname, attidentity <> '', attgenerated <> '' FROM pg_attribute "
        "WHERE attrelid = to_regclass(:ref) AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
    ),
    "sqlserver": (
        "SELECT name FROM sys.triggers WHERE parent_id = OBJECT_ID(:ref) ORDER BY name",
        "SELECT name, is_identity, is_computed FROM sys.columns "
        "WHERE object_id = OBJECT_ID(:ref) ORDER BY column_id",
    ),
    "oracle": (
        "SELECT trigger_name FROM all_triggers WHERE table_name = :name "
        "AND table_owner = COALESCE(:schema, USER) ORDER BY trigger_name",
        "SELECT column_name, CASE WHEN identity_column = 'YES' THEN 1 ELSE 0 END, "
        "CASE WHEN virtual_column = 'YES' THEN 1 ELSE 0 END FROM all_tab_cols "
        "WHERE table_name = :name AND owner = COALESCE(:schema, USER) "
        "AND hidden_column = 'NO' ORDER BY column_id",
    ),
    "mysql": (
        "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS WHERE "
        "EVENT_OBJECT_SCHEMA = COALESCE(:schema, DATABASE()) AND EVENT_OBJECT_TABLE = :name "
        "ORDER BY TRIGGER_NAME",
        "SELECT COLUMN_NAME, EXTRA LIKE '%auto_increment%', EXTRA LIKE '%GENERATED%' "
        "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = COALESCE(:schema, DATABASE()) "
        "AND TABLE_NAME = :name ORDER BY ORDINAL_POSITION",
    ),
    "sqlite": (
        "SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = :name ORDER BY name",
        "SELECT name, 0, hidden IN (2, 3) FROM pragma_table_xinfo(:name) ORDER BY cid",
    ),
}
_DIALECT_FAMILY = {
    "postgres": "postgresql", "redshift": "postgresql", "cockroachdb": "postgresql",
    "greenplum": "postgresql", "mssql": "sqlserver", "azure_sql": "sqlserver",
    "mariadb": "mysql", "tidb": "mysql",
}


def catalog_family(dialect: str) -> str:
    d = str(dialect or "").lower().strip()
    return _DIALECT_FAMILY.get(d, d)


def generic_catalog(conn: Any, dialect: str, schema: str | None, table: str) -> dict[str, Any]:
    """Triggers, identity and generated columns of ``table`` (SQLAlchemy ``conn``).

    ``{"triggers": [...], "identity": [...], "generated": [...]}``; with
    ``"error"`` when the catalog could not be read.
    """
    import sqlalchemy as sa

    family = catalog_family(dialect)
    out: dict[str, Any] = {"triggers": [], "identity": [], "generated": []}
    if family in NO_TRIGGER_ENGINES or family not in _CATALOG_SQL:
        out["none_by_engine"] = family in NO_TRIGGER_ENGINES
        return out
    trig_sql, col_sql = _CATALOG_SQL[family]
    if family == "sqlserver":
        ref = f"[{schema or 'dbo'}].[{table}]"
    elif family == "postgresql":
        ref = f"{_dq(schema or 'public')}.{_dq(table)}"
    else:
        ref = table
    params = {"ref": ref, "name": table, "schema": schema}
    try:
        out["triggers"] = [str(r[0]) for r in conn.execute(sa.text(trig_sql), params).fetchall()]
        for name, ident, gen in conn.execute(sa.text(col_sql), params).fetchall():
            if _truthy(ident):
                out["identity"].append(str(name))
            if _truthy(gen):
                out["generated"].append(str(name))
    except Exception as exc:  # noqa: BLE001 - unknown is reported, never guessed
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "t", "true", "yes", "y"}
    return bool(value)


def generic_restore_refusal(dialect: str, catalog: dict[str, Any]) -> str:
    """Why a row copy would not reproduce the pre-run table ('' = exact).

    Triggers fire again on the copy; identity / auto-increment values cannot
    be written back as they were (or their counter stays where the cancelled
    load left it); an unreadable catalog cannot be shown exact. Generated
    columns are not a reason: the copy leaves them out and they recompute.
    """
    if catalog.get("error"):
        return f"its triggers and identity columns could not be read ({catalog['error']})"
    if catalog.get("triggers"):
        return (
            f"it has triggers ({', '.join(catalog['triggers'])}): copying the pre-run "
            "rows back would fire them again"
        )
    if catalog.get("identity"):
        return (
            f"it has identity / auto-increment columns ({', '.join(catalog['identity'])}) "
            "whose values and counter a row copy cannot put back exactly"
        )
    return ""


def generic_manual_steps(
    dialect: str, schema: str | None, table: str, backup: str,
    columns: list[str], catalog: dict[str, Any],
) -> list[str]:
    """Statements for one dialect, identifiers quoted, stored columns only."""
    family = catalog_family(dialect)
    generated = {c.casefold() for c in catalog.get("generated") or []}
    stored = [c for c in columns if c.casefold() not in generated]
    triggers = list(catalog.get("triggers") or [])
    identity = list(catalog.get("identity") or [])
    if family == "sqlserver":
        q = lambda n: "[" + str(n).replace("]", "]]") + "]"  # noqa: E731
        live, bak = f"{q(schema or 'dbo')}.{q(table)}", f"{q(schema or 'dbo')}.{q(backup)}"
        cols = ", ".join(q(c) for c in stored)
        body = [f"BEGIN TRANSACTION; DELETE FROM {live}"]
        if triggers:
            body.insert(0, f"DISABLE TRIGGER ALL ON {live}")
        if identity:
            body.append(f"SET IDENTITY_INSERT {live} ON")
        body.append(f"INSERT INTO {live} ({cols}) SELECT {cols} FROM {bak}")
        if identity:
            body.append(f"SET IDENTITY_INSERT {live} OFF")
        body.append("COMMIT TRANSACTION")
        if triggers:
            body.append(f"ENABLE TRIGGER ALL ON {live}")
        steps = ["; ".join(body)]
        if identity:
            steps.append(f"DBCC CHECKIDENT ('{schema or 'dbo'}.{table}', RESEED)")
        steps.append(f"DROP TABLE {bak}")
        return steps
    if family == "oracle":
        q = _dq
        owner = f"{q(schema)}." if schema else ""
        live, bak = f"{owner}{q(table)}", f"{owner}{q(backup)}"
        cols = ", ".join(q(c) for c in stored)
        steps = []
        if triggers:
            steps.append(f"ALTER TABLE {live} DISABLE ALL TRIGGERS")
        steps += [f"ALTER TABLE {live} MODIFY ({q(c)} GENERATED BY DEFAULT AS IDENTITY)" for c in identity]
        steps.append(f"DELETE FROM {live}; INSERT INTO {live} ({cols}) SELECT {cols} FROM {bak}; COMMIT")
        steps += [
            f"ALTER TABLE {live} MODIFY ({q(c)} GENERATED ALWAYS AS IDENTITY (START WITH LIMIT VALUE))"
            for c in identity
        ]
        if triggers:
            steps.append(f"ALTER TABLE {live} ENABLE ALL TRIGGERS")
        steps.append(f"DROP TABLE {bak} PURGE")
        return steps
    if family == "sqlite":
        q = _dq
        cols = ", ".join(q(c) for c in stored)
        steps = []
        if triggers:
            steps.append(
                "save each trigger's statement (SELECT sql FROM sqlite_master WHERE "
                f"type = 'trigger' AND tbl_name = '{table}') and drop it: "
                + "; ".join(f"DROP TRIGGER {q(t)}" for t in triggers)
            )
        steps.append(
            f"BEGIN; DELETE FROM {q(table)}; INSERT INTO {q(table)} ({cols}) "
            f"SELECT {cols} FROM {q(backup)}; COMMIT"
        )
        if triggers:
            steps.append("re-create the triggers from the saved statements")
        steps.append(f"DROP TABLE {q(backup)}")
        return steps
    q = _bq if family in {"mysql", "databricks", "bigquery"} else _dq
    pre = f"{q(schema)}." if schema else ""
    cols = ", ".join(q(c) for c in stored)
    return [
        f"DELETE FROM {pre}{q(table)}; INSERT INTO {pre}{q(table)} ({cols}) "
        f"SELECT {cols} FROM {pre}{q(backup)}",
        f"DROP TABLE {pre}{q(backup)}",
    ]


# --- entry point --------------------------------------------------------------


def manual_restore_steps(
    *, engine: str, dest_type: str, cfg: dict[str, Any], table: str, backup: str,
    schema: str | None = None,
) -> str:
    """Plain statements that put ``backup``'s rows back into ``table``."""
    try:
        if engine == "mongodb":
            return (
                f'db.getCollection("{backup}").renameCollection("{table}", true) '
                "(replaces the partial collection in one step)"
            )
        if dest_type in {"mysql", "mariadb"}:
            from connectors.table_manager import _mysql_ddl_connection

            conn = _mysql_ddl_connection(cfg)
            try:
                with conn.cursor() as cur:
                    return "; then ".join(_mysql_steps(cur, table, backup))
            finally:
                conn.close()
        if dest_type in {"postgresql", "redshift"}:
            from connectors.postgresql_conn import get_connection

            conn = get_connection(
                host=cfg.get("host", ""), port=int(cfg.get("port") or 5432),
                database=cfg.get("database") or "", username=cfg.get("username") or "",
                password=cfg.get("password", ""), connection_string=cfg.get("connection_string", ""),
                ssl=bool(cfg.get("ssl", False)),
            )
            try:
                with conn.cursor() as cur:
                    return "; then ".join(_pg_steps(cur, schema or "public", table, backup))
            finally:
                conn.rollback()
                conn.close()
        if engine == "sql_row_copy":
            return "; then ".join(_generic_steps_live(dest_type, cfg, table, backup, schema))
    except Exception:  # noqa: BLE001 - the generic steps below still apply
        logger.warning("could not read %s's columns for the restore steps", table, exc_info=True)
    return (
        f"copy the rows of {backup} into {table}, listing {table}'s stored columns "
        "explicitly (leave out generated and identity columns), then drop "
        f"{backup}"
    )


def _generic_steps_live(dest_type: str, cfg: dict[str, Any], table: str, backup: str,
                        schema: str | None) -> list[str]:
    import sqlalchemy as sa

    from connectors.generic_sql import get_sqlalchemy_engine
    from connectors.table_manager import _generic_refs, _generic_sql_cfg
    from services.engine_pool import release_engine

    gcfg = _generic_sql_cfg(cfg, dest_type)
    dialect, _live_ref, bak_ref = _generic_refs(gcfg, table, schema, backup)
    engine = get_sqlalchemy_engine(gcfg)
    try:
        with engine.connect() as conn:
            catalog = generic_catalog(conn, dialect, schema or gcfg.get("schema"), table)
            probe = "SELECT * FROM " + bak_ref + " WHERE 1=0"  # nosec B608 - quoted identifier
            columns = [str(c) for c in conn.execute(sa.text(probe)).keys()]
        return generic_manual_steps(dialect, schema or gcfg.get("schema"), table, backup, columns, catalog)
    finally:
        release_engine(engine)
