"""Remove a failed batch from a destination this run found empty.

Writers commit each chunk before the job checksum runs. A failed checksum
used to leave those rows in place (40 of 50) while the job was marked failed.
Deleting by the capped ``written_ids`` list is not a rollback: the list is not
the batch. Truncating a table that already held rows would destroy them.

The only safe restore is the one the copy paths already use. When the
pre-write count was measured as zero, every row now in the table came from
this failed run, so clearing the table returns it to that empty state. CDC,
SCD2, and mirror are not batch loads and are left untouched. An occupied
destination is left untouched and the job says so.
"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import closing
from typing import Any

from services.dest_precount import PRECOUNT_KEY, count_dialect, precount_table
from services.sync_cursor import normalize_sync_mode

logger = logging.getLogger(__name__)

_HISTORY_MODES = frozenset({"cdc", "scd2", "mirror"})


def undo_failed_batch_if_dest_was_empty(
    *,
    destination: Any,
    dest_summary: dict[str, Any],
    sync_mode: str,
) -> str:
    """Clear a partial batch or record why it was kept. Never raises.

    Stamps ``partial_batch_undo`` and ``partial_batch_undo_note`` on the
    summary. A second call is a no-op.
    """
    if dest_summary.get("partial_batch_undo"):
        return str(dest_summary.get("partial_batch_undo_note") or "")
    try:
        decision, note = _decide(destination, dest_summary, sync_mode)
        if decision == "clear":
            note = _clear(destination, dest_summary)
            decision = "cleared" if dest_summary.get("partial_batch_removed") else "retained"
        dest_summary["partial_batch_undo"] = decision
        dest_summary["partial_batch_undo_note"] = note
        return note
    except Exception as exc:  # noqa: BLE001 — undo must not hide the checksum failure
        logger.warning("Failed-batch undo did not run: %s", exc, exc_info=True)
        note = (
            "The failed batch was left in place because the undo could not run."
        )
        dest_summary["partial_batch_undo"] = "retained"
        dest_summary["partial_batch_undo_note"] = note
        return note


def _decide(
    destination: Any,
    dest_summary: dict[str, Any],
    sync_mode: str,
) -> tuple[str, str]:
    mode = normalize_sync_mode(sync_mode or dest_summary.get("sync_mode"), default="")
    if mode in _HISTORY_MODES or str(dest_summary.get("sync_mode") or "").lower() == "cdc":
        return (
            "retained",
            "CDC and history loads are not rolled back as a batch.",
        )
    if getattr(destination, "kind", "") != "database":
        return (
            "retained",
            "This destination is not a SQL table, so the failed batch was not deleted.",
        )
    before = dest_summary.get(PRECOUNT_KEY)
    if not isinstance(before, int):
        return (
            "retained",
            "Pre-write destination count was not measured, so the failed batch "
            "was left in place.",
        )
    if before != 0:
        return (
            "retained",
            f"Destination already held {before} row(s) before this run. "
            "The failed batch was left in place — deleting by a partial key "
            "list would not be a rollback.",
        )
    return ("clear", "")


def _clear(destination: Any, dest_summary: dict[str, Any]) -> str:
    from src.transfer.adapters import resolve_connector_config, resolve_dest_table

    cfg = resolve_connector_config(destination)
    db_type = count_dialect(str(cfg.get("type") or getattr(destination, "format", "") or ""))
    table = str(
        dest_summary.get("table")
        or dest_summary.get("collection")
        or resolve_dest_table(db_type, destination, "dt_import")
        or ""
    ).strip()
    if not table:
        return "The failed batch was left in place because the destination table was not named."
    from services.dialect_profiles import schema_from_cfg

    schema = schema_from_cfg(db_type, cfg)
    removed = _delete_all_rows(db_type, cfg, schema=schema, table_name=table)
    after = precount_table(db_type, cfg, table)
    dest_summary["partial_batch_rows_after_undo"] = after
    if after == 0 and removed:
        dest_summary["partial_batch_removed"] = True
        return (
            "The destination was empty before this run. The partial batch was "
            "removed, so the table is empty again."
        )
    if after == 0:
        dest_summary["partial_batch_removed"] = True
        return "The destination was empty before this run and is still empty."
    return (
        "The destination was empty before this run, but the partial batch "
        "could not be removed. The failed rows are still in the table."
    )


def _delete_all_rows(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
) -> bool:
    from connectors.sql_identifiers import quote_table_ref

    if db_type == "sqlite":
        database = str(cfg.get("database") or "")
        if not database:
            return False
        ref = quote_table_ref(table_name, dialect="sqlite")
        with closing(sqlite3.connect(database)) as conn:
            conn.execute(f"DELETE FROM {ref}")  # nosec B608
            conn.commit()
        return True

    if db_type == "postgresql":
        from connectors.postgresql_conn import get_connection

        ref = quote_table_ref(table_name, schema or "public", dialect="postgresql")
        conn = get_connection(
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 5432),
            database=str(cfg.get("database") or ""),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            connection_string=str(cfg.get("connection_string") or ""),
            ssl=bool(cfg.get("ssl", False)),
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {ref}")  # nosec B608
            conn.commit()
        finally:
            conn.close()
        return True

    if db_type == "mysql":
        from connectors.mysql_conn import get_connection

        ref = quote_table_ref(table_name, dialect="mysql")
        conn = get_connection(
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 3306),
            database=str(cfg.get("database") or ""),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            connection_string=str(cfg.get("connection_string") or ""),
            ssl=bool(cfg.get("ssl", False)),
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {ref}")  # nosec B608
            conn.commit()
        finally:
            conn.close()
        return True

    from services.dialect_profiles import warehouse_sql_quote_dialect
    from services.dest_precount import _warehouse_sql_engine, _warehouse_sql_table_ref

    dialect = warehouse_sql_quote_dialect(db_type)
    if dialect not in {"sqlserver", "oracle"}:
        return False
    import sqlalchemy as sa

    ref = _warehouse_sql_table_ref(dialect, cfg, schema, table_name)
    with _warehouse_sql_engine(db_type, cfg) as engine:
        with engine.begin() as conn:
            conn.execute(sa.text(f"DELETE FROM {ref}"))  # nosec B608
    return True
