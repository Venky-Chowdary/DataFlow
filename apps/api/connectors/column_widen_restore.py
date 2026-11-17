"""Widen an existing column, and put the original definition back if the write fails.

SQL Server ``ALTER COLUMN`` replaces nullability and collation unless the
statement restates them. MySQL ``MODIFY`` does the same, and MySQL commits
DDL implicitly, so a failed write cannot roll the widen back inside the
transaction that issued it. The restore is a second ``ALTER`` of the column
definition captured before the widen: type, ``NULL`` / ``NOT NULL``, and
collation. A restore that does not run fails the job and is written to the
audit log.

PostgreSQL can roll an uncommitted DDL statement back. This path commits the
widen before the rows are written, so the rollback the transaction would have
provided is not available. The compensating ``ALTER`` is the restore on every
engine, including PostgreSQL.
"""

from __future__ import annotations

import logging
import re
from contextvars import ContextVar
from typing import Any

import sqlalchemy as sa

from connectors.schema_drift import (
    _build_widen_ddl,
    _quote_col,
    existing_column_index,
    existing_carrier_holds_source,
    is_wider_type,
    raise_widen_refusal,
)

logger = logging.getLogger(__name__)

_RESTORE_BUCKET: ContextVar[list[str] | None] = ContextVar(
    "df_column_restore_sql", default=None
)
_RESTORE_CURSOR: ContextVar[Any] = ContextVar("df_column_restore_cursor", default=None)

_COLLATION_NAME = re.compile(r"[A-Za-z0-9_]+")


class WidenRestoreError(RuntimeError):
    """The compensating ALTER did not put the column back."""


def bind_restore_cursor(cursor: Any) -> None:
    """The writer cursor a failed run replays restore SQL on."""
    _RESTORE_CURSOR.set(cursor)


def note_restore(sql: str) -> None:
    bucket = _RESTORE_BUCKET.get()
    if bucket is not None and sql:
        bucket.append(sql)


def widen_restore_pending() -> bool:
    """True when this write widened a column and still holds the restore SQL.

    ``policy=quarantine`` never sets the writer's abort flag, including when
    every row was quarantined and nothing was committed (D368). The restore
    decision is this flag plus a committed row count of zero.
    """
    return bool(_RESTORE_BUCKET.get())


def writer_restore_guard(fn):
    """Hold restore SQL for the writer. The writer settles it on a live connection.

    Replaying after the function returns uses a cursor the writer has already
    closed, which hides the original error behind "Could not restore" (D368).
    """
    from functools import wraps

    @wraps(fn)
    def wrapped(*args, **kwargs):
        bucket: list[str] = []
        token_bucket = _RESTORE_BUCKET.set(bucket)
        token_cursor = _RESTORE_CURSOR.set(None)
        try:
            return fn(*args, **kwargs)
        finally:
            _RESTORE_BUCKET.reset(token_bucket)
            _RESTORE_CURSOR.reset(token_cursor)

    return wrapped


def settle_widen_restore(
    conn: Any,
    *,
    rows_written: int,
    failed: bool,
    original: BaseException | None = None,
) -> None:
    """Restore only when the failed run committed no rows.

    A run that already committed wide values leaves the column widened and
    records that. Narrowing it would truncate those rows on non-strict MySQL.
    The restore uses ``conn``, which the caller still has open. A restore
    failure raises :class:`WidenRestoreError` chained from ``original``.
    """
    bucket = _RESTORE_BUCKET.get()
    if not bucket:
        return
    statements = list(bucket)
    bucket.clear()
    if not failed or int(rows_written or 0) > 0:
        if int(rows_written or 0) > 0:
            _audit_widen_kept(statements, int(rows_written))
        return
    if conn is None:
        _fail_restore(statements, "no live connection for the restore", original)
    # The failed statement left the transaction aborted on PostgreSQL, so the
    # restore ALTER raised "current transaction is aborted". On MySQL the
    # same ALTER is DDL and implicitly commits the failed statement's rows.
    # Roll back first, then change the column (D368).
    try:
        conn.rollback()
    except Exception:
        logger.debug("rollback before widen restore failed", exc_info=True)
    try:
        cursor = conn.cursor()
        try:
            for stmt in statements:
                cursor.execute(stmt)
            conn.commit()
        finally:
            cursor.close()
    except Exception as exc:
        _fail_restore(statements, exc, original)


def _audit_widen_kept(statements: list[str], rows_written: int) -> None:
    try:
        from services.audit_log import append_audit_event

        append_audit_event(
            action="schema.widen_restore",
            resource="destination_column",
            actor="system",
            level="warning",
            outcome="kept",
            reason_code="widen_kept_rows_committed",
            details={
                "rows_written": rows_written,
                "statements": [str(s)[:300] for s in statements[:8]],
            },
        )
    except Exception:
        logger.exception("widen-kept audit record failed")


def column_null_and_collation(existing: dict[str, Any]) -> tuple[bool | None, str]:
    """``(nullable, collation)`` from a reflected column. Missing stays unset."""
    nullable = existing.get("nullable") if "nullable" in existing else None
    if not isinstance(nullable, bool):
        nullable = None
    collation = str(existing.get("collation") or "").strip()
    typ = existing.get("type")
    if not collation and typ is not None:
        collation = str(getattr(typ, "collation", None) or "").strip()
    if collation and not _COLLATION_NAME.fullmatch(collation):
        collation = ""
    return nullable, collation


def restate_column_ddl(
    dialect: str,
    schema: str | None,
    table_name: str,
    col: str,
    type_sql: str,
    *,
    nullable: bool | None,
    collation: str,
    existing_type: str | None = None,
) -> str:
    """ALTER that sets ``type_sql`` and restates nullability and collation.

    SQL Server drops both when they are absent from ``ALTER COLUMN``. MySQL
    ``MODIFY`` replaces the column definition. PostgreSQL ``SET``/``DROP NOT
    NULL`` is a second action on the same ``ALTER TABLE``.
    """
    ddl = _build_widen_ddl(
        dialect, schema, table_name, col, type_sql, existing_type or type_sql
    )
    name = (dialect or "").lower()
    coll = collation if collation and _COLLATION_NAME.fullmatch(collation) else ""
    col_q = _quote_col(name, col)
    if name in {"sqlserver", "mssql"}:
        # The compiled type already carries COLLATE (SQLAlchemy
        # ``NVARCHAR(n, collation=...)``). Appending it again is
        # ``COLLATE X COLLATE X``, which SQL Server rejects (D368).
        extra: list[str] = []
        if coll and not re.search(r"\bCOLLATE\b", ddl, re.I):
            extra.append(f"COLLATE {coll}")
        upper = ddl.upper()
        if nullable is False and "NOT NULL" not in upper:
            extra.append("NOT NULL")
        elif nullable is True and not re.search(r"\bNULL\b", upper):
            extra.append("NULL")
        return f"{ddl} {' '.join(extra)}".rstrip()
    if name in {"mysql", "mariadb"}:
        if coll:
            ddl += f" COLLATE {coll}"
        if nullable is False:
            ddl += " NOT NULL"
        elif nullable is True:
            ddl += " NULL"
        return ddl
    if name in {
        "postgresql",
        "postgres",
        "redshift",
        "cockroachdb",
        "yugabyte",
        "timescale",
        "supabase",
        "neon",
        "duckdb",
        "motherduck",
    }:
        if coll and " COLLATE " not in ddl.upper():
            if " USING " in ddl:
                head, using = ddl.split(" USING ", 1)
                ddl = f"{head} COLLATE {coll} USING {using}"
            else:
                ddl = f"{ddl} COLLATE {coll}"
        actions: list[str] = []
        if nullable is False:
            actions.append(f"ALTER COLUMN {col_q} SET NOT NULL")
        elif nullable is True:
            actions.append(f"ALTER COLUMN {col_q} DROP NOT NULL")
        if actions:
            ddl += ", " + ", ".join(actions)
        return ddl
    return ddl


def apply_column_restores(engine: Any, statements: list[str]) -> None:
    """Run each restore statement. A failure is an audit event and an error."""
    if not statements or engine is None:
        return
    try:
        with engine.connect() as conn:
            for stmt in statements:
                conn.execute(sa.text(stmt))
            conn.commit()
    except Exception as exc:
        _fail_restore(statements, exc, None)


def _fail_restore(
    statements: list[str],
    error: BaseException | str,
    original: BaseException | None,
) -> None:
    message = str(error)
    try:
        from services.audit_log import append_audit_event

        append_audit_event(
            action="schema.widen_restore",
            resource="destination_column",
            actor="system",
            level="error",
            outcome="failed",
            reason_code="widen_restore_failed",
            details={
                "error": message[:500],
                "statements": [str(s)[:300] for s in statements[:8]],
            },
        )
    except Exception:
        logger.exception("widen restore audit record failed")
    raised = WidenRestoreError(
        "Could not restore the destination column after a failed write: " + message[:300]
    )
    if original is not None and original is not error:
        raise raised from original
    raise raised


def widen_existing_columns_sa(
    conn: Any,
    engine: Any,
    dialect_name: str,
    schema: str | None,
    table_name: str,
    target_cols: list[str],
    target_column_types: dict[str, str],
    conflict_columns: list[str] | None = None,
    *,
    stamp_ceiling_by_col: dict[str, str] | None = None,
    refusals_out: list[dict[str, Any]] | None = None,
    source_types: dict[str, str] | None = None,
    restore_out: list[str] | None = None,
) -> list[str]:
    """Issue ALTER / MODIFY to widen columns that drifted wider.

    The desired type is the same carrier ``widened_dest_carrier`` proposes at
    plan time, so NVARCHAR(400) with a VARCHAR(800) source widens to
    NVARCHAR(800) in both places. The original type, nullability and collation
    are captured before the statement and replayed if the write commits no rows.
    """
    if not target_cols or not target_column_types:
        return []

    dialect_name = (dialect_name or "").lower()
    if dialect_name == "sqlite":
        return []

    skip_cols = set(conflict_columns or [])
    ceilings = {
        str(k): str(v)
        for k, v in (stamp_ceiling_by_col or {}).items()
        if k and v
    }

    try:
        inspector = sa.inspect(conn)
        existing_cols = inspector.get_columns(table_name, schema=schema)
    except Exception as exc:
        logger.debug("Could not reflect columns for widen: %s", exc, exc_info=exc)
        return []

    catalog_names = existing_column_index(
        dialect_name, (c["name"] for c in existing_cols)
    )
    log: list[str] = []
    for col in target_cols:
        if col in skip_cols:
            continue
        catalog_name = catalog_names.get(col) or catalog_names.get(col.casefold())
        existing = next(
            (c for c in existing_cols if c["name"] == catalog_name), None
        )
        if not existing:
            continue
        existing_type = str(existing["type"].compile(dialect=engine.dialect))
        proposed = target_column_types.get(col, existing_type)
        stamp = ceilings.get(col)
        desired_type = proposed
        source_type = ""
        if source_types:
            source_type = str(
                source_types.get(col) or source_types.get(col.casefold()) or ""
            ).strip()
        if stamp:
            if proposed and is_wider_type(stamp, proposed):
                refusal = {
                    "column": col,
                    "mapped_type": stamp,
                    "refused_wider": proposed,
                    "reason": "explicit_map_stamp_ceiling",
                }
                if refusals_out is not None:
                    refusals_out.append(refusal)
                else:
                    logger.info(
                        "generic_sql Map≡ALTER refusal (stamp ceiling): %s", refusal
                    )
            desired_type = stamp
        elif source_type:
            from services.schema_drift import widened_dest_carrier

            agreed = widened_dest_carrier(existing_type, source_type)
            if agreed:
                desired_type = agreed
        if not is_wider_type(existing_type, desired_type, dest_db=dialect_name):
            continue
        from connectors.schema_drift import character_ddl_is_unsized
        from services.type_system import national_charset_would_collapse

        if existing_carrier_holds_source(
            source_type, existing_type, dest_db=dialect_name
        ):
            continue
        if character_ddl_is_unsized(desired_type) or national_charset_would_collapse(
            existing_type, desired_type, dest_db=dialect_name
        ):
            logger.info(
                "Widen refused for %s.%s: %s -> %s would narrow or drop Unicode",
                table_name,
                col,
                existing_type,
                desired_type,
            )
            continue
        nullable, collation = column_null_and_collation(existing)
        try:
            ddl = restate_column_ddl(
                dialect_name,
                schema,
                table_name,
                col,
                desired_type,
                nullable=nullable,
                collation=collation,
                existing_type=existing_type,
            )
            restore_sql = restate_column_ddl(
                dialect_name,
                schema,
                table_name,
                col,
                existing_type,
                nullable=nullable,
                collation=collation,
                existing_type=existing_type,
            )
            conn.execute(sa.text(ddl))
            conn.commit()
            log.append(ddl)
            if restore_out is not None:
                restore_out.append(restore_sql)
            note_restore(restore_sql)
            logger.debug(
                "Widened %s.%s from %s to %s",
                table_name,
                col,
                existing_type,
                desired_type,
            )
        except Exception as exc:
            err = str(exc).lower()
            if any(
                phrase in err for phrase in ("already", "cannot alter", "not supported")
            ):
                logger.debug(
                    "Widen skipped for %s.%s: %s", table_name, col, exc, exc_info=exc
                )
                continue
            logger.warning(
                "Widen failed for %s.%s: %s", table_name, col, exc, exc_info=exc
            )
            raise_widen_refusal(col, existing_type, desired_type, exc)
            raise
    return log
