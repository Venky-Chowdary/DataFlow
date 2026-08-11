"""Source-read snapshot sessions for Property 3 (snapshot-consistent reads).

Transfer pagination historically opened a new connection per page under
READ COMMITTED, so concurrent source writers could make page N see a
different MVCC state than page 1. Checksums computed across those pages
(or against a second scan) could false-pass or false-fail.

This module owns a process-local ContextVar that transfer readers consult
so every page of a full-refresh read shares one REPEATABLE READ (or
engine-equivalent) transaction. Incremental/CDC paths intentionally do
not use this — they follow watermarks by design.
"""

from __future__ import annotations

import contextvars
import logging
from typing import Any

logger = logging.getLogger(__name__)

#: Active source snapshot connection for the current transfer (if any).
_SOURCE_SNAPSHOT_CONN: contextvars.ContextVar[Any | None] = contextvars.ContextVar(
    "dataflow_source_snapshot_conn",
    default=None,
)

#: Metadata stamped onto dest_summary / reconciliation for the certificate.
_SOURCE_SNAPSHOT_META: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "dataflow_source_snapshot_meta",
    default=None,
)


def get_source_snapshot_conn() -> Any | None:
    return _SOURCE_SNAPSHOT_CONN.get()


def get_source_snapshot_meta() -> dict[str, Any] | None:
    meta = _SOURCE_SNAPSHOT_META.get()
    return dict(meta) if isinstance(meta, dict) else None


def bind_source_snapshot(conn: Any, meta: dict[str, Any]) -> contextvars.Token:
    """Bind ``conn`` + ``meta`` for the duration of a transfer read."""
    _SOURCE_SNAPSHOT_META.set(dict(meta))
    return _SOURCE_SNAPSHOT_CONN.set(conn)


def reset_source_snapshot(token: contextvars.Token | None) -> None:
    if token is not None:
        _SOURCE_SNAPSHOT_CONN.reset(token)
    _SOURCE_SNAPSHOT_META.set(None)


#: Private state for transfer-scoped open/close (not for callers to mutate).
_ACTIVE_END: contextvars.ContextVar[Any | None] = contextvars.ContextVar(
    "dataflow_source_snapshot_end",
    default=None,
)
_ACTIVE_TOKEN: contextvars.ContextVar[contextvars.Token | None] = contextvars.ContextVar(
    "dataflow_source_snapshot_token",
    default=None,
)


def activate_snapshot(conn: Any, meta: dict[str, Any], end_fn: Any) -> None:
    """Bind snapshot for this transfer; :func:`release_active_snapshot` cleans up."""
    # Replace any prior bind in this context.
    release_active_snapshot(commit=False)
    token = bind_source_snapshot(conn, meta)
    _ACTIVE_TOKEN.set(token)
    _ACTIVE_END.set((conn, end_fn))


def release_active_snapshot(*, commit: bool = True) -> dict[str, Any] | None:
    """End the active transfer snapshot (idempotent). Returns meta if any."""
    meta = get_source_snapshot_meta()
    pair = _ACTIVE_END.get()
    token = _ACTIVE_TOKEN.get()
    _ACTIVE_END.set(None)
    _ACTIVE_TOKEN.set(None)
    if pair is not None:
        conn, end_fn = pair
        try:
            end_fn(conn, commit=commit)
        except Exception as exc:
            logger.warning("release_active_snapshot end_fn failed: %s", exc, exc_info=exc)
    reset_source_snapshot(token)
    return meta


# psycopg2 connections often reject arbitrary attributes — keep restore state here.
_PG_PREV_AUTOCOMMIT: dict[int, bool] = {}


def begin_postgresql_repeatable_read(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str = "",
    ssl: bool = False,
) -> tuple[Any, dict[str, Any]]:
    """Open one PG connection in REPEATABLE READ and capture the WAL LSN.

    Callers must :func:`end_postgresql_snapshot` (commit/rollback + close).
    Pattern mirrors ``PostgreSqlChangeStreamCdc.snapshot()``.
    """
    from connectors.postgresql_conn import get_connection

    conn = get_connection(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        connection_string=connection_string,
        ssl=ssl,
    )
    prev_autocommit = bool(getattr(conn, "autocommit", True))
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cur.execute("SELECT pg_current_wal_lsn()::text")
            row = cur.fetchone()
            lsn = str(row[0]) if row and row[0] else ""
            # Also export a named snapshot token when available (PG 9.3+).
            export_id = ""
            try:
                cur.execute("SELECT pg_export_snapshot()")
                erow = cur.fetchone()
                if erow and erow[0]:
                    export_id = str(erow[0])
            except Exception as exc:
                logger.debug("pg_export_snapshot unavailable: %s", exc)
        meta = {
            "engine": "postgresql",
            "isolation": "repeatable_read",
            "guarantee": "mvcc_repeatable_read",
            "snapshot_lsn": lsn,
            "export_snapshot": export_id,
            "note": (
                "All full-refresh source pages share this MVCC snapshot. "
                "Inline write-pass fingerprints describe this population."
            ),
        }
        _PG_PREV_AUTOCOMMIT[id(conn)] = prev_autocommit
        return conn, meta
    except Exception:
        try:
            conn.rollback()
        except Exception as exc:
            logger.warning("RR snapshot rollback failed: %s", exc, exc_info=exc)
        try:
            conn.autocommit = prev_autocommit
        except Exception as exc:
            logger.warning("RR snapshot autocommit restore failed: %s", exc, exc_info=exc)
        try:
            conn.close()
        except Exception as exc:
            logger.warning("RR snapshot close failed: %s", exc, exc_info=exc)
        raise


def end_postgresql_snapshot(conn: Any | None, *, commit: bool = True) -> None:
    """Commit or roll back the RR transaction and close the connection."""
    if conn is None:
        return
    prev = _PG_PREV_AUTOCOMMIT.pop(id(conn), True)
    try:
        if commit:
            conn.commit()
        else:
            conn.rollback()
    except Exception as exc:
        logger.warning("end_postgresql_snapshot txn end failed: %s", exc, exc_info=exc)
        try:
            conn.rollback()
        except Exception as exc2:
            logger.warning("end_postgresql_snapshot rollback failed: %s", exc2, exc_info=exc2)
    finally:
        try:
            conn.autocommit = prev
        except Exception as exc:
            logger.warning("end_postgresql_snapshot autocommit restore: %s", exc, exc_info=exc)
        try:
            conn.close()
        except Exception as exc:
            logger.warning("end_postgresql_snapshot close: %s", exc, exc_info=exc)


#: Journal modes this module switched to WAL, restored when the snapshot ends.
_SQLITE_PRIOR_JOURNAL_MODE: dict[int, str] = {}

#: PRAGMA takes a bare keyword, so the restore value is checked against SQLite's
#: fixed vocabulary rather than interpolated from whatever the file reported.
_SQLITE_JOURNAL_MODES = frozenset(
    {"delete", "truncate", "persist", "memory", "wal", "off"}
)


def begin_sqlite_snapshot(
    *,
    database: str = "",
    connection_string: str = "",
    host: str = "",
) -> tuple[Any, dict[str, Any]]:
    """Open one SQLite connection and BEGIN a deferred transaction.

    SQLite takes the snapshot at the first read in the transaction — all
    subsequent LIMIT/OFFSET pages on this connection see a consistent view.
    """
    import sqlite3

    from connectors.sqlite_common import sqlite_file_path

    path = sqlite_file_path(database, connection_string, host)
    if not path:
        raise ValueError("SQLite path is required for source snapshot")
    conn = sqlite3.connect(path, timeout=30)
    conn.isolation_level = None  # manual txn control
    # A rollback-journal reader holds SHARED for the whole transaction, so the
    # writer cannot take EXCLUSIVE at commit: a same-file source→destination job
    # (mirror/SCD2 into a table beside the source) waited out the busy timeout
    # and failed with "database is locked". WAL is SQLite's own answer to one
    # writer alongside snapshot readers; the previous mode is restored on close.
    prior_mode = ""
    wal_enabled = False
    try:
        row = conn.execute("PRAGMA journal_mode").fetchone()
        prior_mode = str(row[0]) if row else ""
        if prior_mode.lower() != "wal":
            row = conn.execute("PRAGMA journal_mode=WAL").fetchone()
            wal_enabled = bool(row) and str(row[0]).lower() == "wal"
        else:
            wal_enabled = True
    except Exception as exc:
        # Keep the snapshot; a concurrent same-file write will still block.
        logger.warning("sqlite snapshot WAL enable failed: %s", exc, exc_info=exc)
    if wal_enabled and prior_mode and prior_mode.lower() != "wal":
        _SQLITE_PRIOR_JOURNAL_MODE[id(conn)] = prior_mode
    conn.execute("BEGIN")
    meta = {
        "engine": "sqlite",
        "isolation": "deferred_transaction",
        "guarantee": "sqlite_transaction_snapshot",
        "snapshot_lsn": "",
        "export_snapshot": "",
        "path": str(path),
        "journal_mode": "wal" if wal_enabled else (prior_mode.lower() or "unknown"),
        "prior_journal_mode": prior_mode.lower(),
        "note": (
            "All full-refresh source pages share this SQLite transaction snapshot."
        ),
    }
    return conn, meta


def end_sqlite_snapshot(conn: Any | None, *, commit: bool = True) -> None:
    if conn is None:
        return
    prior_mode = _SQLITE_PRIOR_JOURNAL_MODE.pop(id(conn), "")
    if prior_mode.lower() not in _SQLITE_JOURNAL_MODES:
        prior_mode = ""
    try:
        conn.execute("COMMIT" if commit else "ROLLBACK")
    except Exception as exc:
        logger.warning("end_sqlite_snapshot txn end failed: %s", exc, exc_info=exc)
        try:
            conn.execute("ROLLBACK")
        except Exception as exc2:
            logger.warning("end_sqlite_snapshot rollback failed: %s", exc2, exc_info=exc2)
    finally:
        if prior_mode:
            # Best effort: leave the operator's database in the journal mode we
            # found it in. A still-open reader keeps WAL, which is safe.
            try:
                conn.execute(f"PRAGMA journal_mode={prior_mode}")
            except Exception as exc:
                logger.warning(
                    "end_sqlite_snapshot journal_mode restore to %s failed: %s",
                    prior_mode,
                    exc,
                    exc_info=exc,
                )
        try:
            conn.close()
        except Exception as exc:
            logger.warning("end_sqlite_snapshot close: %s", exc, exc_info=exc)
