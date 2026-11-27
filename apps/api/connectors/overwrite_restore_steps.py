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
    except Exception:  # noqa: BLE001 - the generic steps below still apply
        logger.warning("could not read %s's columns for the restore steps", table, exc_info=True)
    return (
        f"copy the rows of {backup} into {table}, listing {table}'s stored columns "
        "explicitly (leave out generated and identity columns), then drop "
        f"{backup}"
    )
