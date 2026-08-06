"""Shared helpers for durable bulk writes over flaky public proxies.

Managed Postgres/MySQL proxies (Railway, Neon, cloud SQL) often drop idle or
long-lived sockets mid-transfer. Writers must:
  1. Keep TCP/TLS alive while mapping or DDL runs
  2. Reconnect and retry the failed chunk (not the whole batch) after a drop
  3. Prefer smaller commit sizes on public proxy hosts
  4. Record committed chunks in a durable ledger so ambiguous commits are
     skipped on retry (no silent duplicates on insert-mode CSV loads)
"""

from __future__ import annotations

import logging
import os
from services.brand_env import getenv_brand
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from connectors.writer_common import CHUNK_SIZE

CONNECTION_LOST_SIGNALS: tuple[str, ...] = (
    "server closed the connection",
    "connection reset",
    "broken pipe",
    "ssl syscall error",
    "ssl connection has been closed",
    "eof detected",
    "connection already closed",
    "lost connection",
    "gone away",
    "can't connect",
    "cannot connect",
    "connection refused",
    "could not connect",
    "terminating connection",
    "connection timed out",
    "timed out",
    "read timeout",
    "write timeout",
    "server has gone away",
    "connection is closed",
    "connection not open",
    "server closed the connection unexpectedly",
)

logger = logging.getLogger(__name__)

PUBLIC_PROXY_HOST_MARKERS: tuple[str, ...] = (
    "proxy.rlwy.net",
    ".rlwy.net",
    "amazonaws.com",
    "azure.com",
    "neon.tech",
    "supabase.co",
    "aivencloud.com",
    "digitalocean.com",
    "c.db.ondigitalocean.com",
)

_CHUNK_RECONNECT_ATTEMPTS = int(getenv_brand("WRITE_RECONNECT_ATTEMPTS", "12"))
# Public TCP proxies (Railway etc.) still need smaller commits than LAN COPY of
# 20k+, but 1000-row INSERT made 1M-row loads look like multi-hour jobs.
# 5k chunked COPY + ledger reconnect is the competitive default; override via env.
_PROXY_CHUNK_SIZE = int(getenv_brand("PROXY_CHUNK_SIZE", "5000"))
_RECONNECT_MAX_SECONDS = float(getenv_brand("WRITE_RECONNECT_MAX_SECONDS", "600"))

LEDGER_TABLE = "_dataflow_write_ledger"


def is_public_proxy_host(host: str | None) -> bool:
    host_l = (host or "").lower()
    if not host_l:
        return False
    return any(marker in host_l for marker in PUBLIC_PROXY_HOST_MARKERS)


def write_chunk_size(
    host: str | None,
    default: int | None = None,
    connection_string: str | None = None,
) -> int:
    """Smaller commits on public proxies reduce time-to-fail and replay cost."""
    base = default if default is not None else CHUNK_SIZE
    if is_public_proxy_host(host) or is_public_proxy_host(connection_string):
        return max(1, min(base, _PROXY_CHUNK_SIZE))
    return max(1, base)


def proxy_stream_batch_size(
    host: str | None,
    connection_string: str | None = None,
    default: int | None = None,
) -> int:
    """Align file/stream batch size to the writer commit size on public proxies."""
    return write_chunk_size(host, default=default, connection_string=connection_string)


def is_connection_lost(exc: BaseException | str) -> bool:
    text = str(exc).lower()
    name = type(exc).__name__.lower() if isinstance(exc, BaseException) else ""
    try:
        from connectors.sql_temporal import is_sql_data_error
    except ImportError:
        is_sql_data_error = lambda _e: False
    # Never treat bad cell values as a dropped socket — that burns reconnect budget
    # and fails the whole job instead of quarantining the row.
    if is_sql_data_error(exc):
        return False
    if any(token in name for token in ("operational", "interface", "timeout", "connection")):
        # Exclude contract/data errors that happen to use OperationalError wrappers.
        if any(
            bad in text
            for bad in (
                "syntax error",
                "undefined column",
                "does not exist",
                "duplicate key",
                "incorrect datetime",
                "data truncation",
                "out of range",
                "lock wait",
                "lock timeout",
            )
        ):
            return "server closed" in text or "connection reset" in text or "broken pipe" in text
        return True
    return any(sig in text for sig in CONNECTION_LOST_SIGNALS)


def chunk_reconnect_attempts(*, proxy: bool = False) -> int:
    base = max(1, _CHUNK_RECONNECT_ATTEMPTS)
    return max(base, 12) if proxy else base


def reconnect_backoff_seconds(attempt: int) -> float:
    """Exponential backoff with jitter between reconnect attempts."""
    import random

    base = min(15.0, 0.5 * (2 ** max(0, attempt - 1)))
    return base + random.uniform(0, 0.45)  # nosec B311


def should_retry_connection_lost(
    *,
    attempt: int,
    started_at: float,
    proxy: bool = False,
) -> bool:
    """Keep retrying proxy drops until attempt budget or wall-clock budget is hit."""
    if (time.monotonic() - started_at) >= _RECONNECT_MAX_SECONDS:
        return False
    return attempt < chunk_reconnect_attempts(proxy=proxy)


def close_quietly(conn: Any) -> None:
    if conn is None:
        return
    try:
        conn.close()
    except Exception as exc:
        logger.warning("Exception suppressed: %s", exc, exc_info=exc)


def apply_postgres_session_guards(conn: Any) -> None:
    """Disable aggressive statement/idle kills that abort long Railway transfers.

    ``statement_timeout`` and ``idle_in_transaction_session_timeout`` stay at 0
    so multi-million-row COPY / reconcile operations are not killed.  A finite
    ``lock_timeout`` (2 minutes) prevents the connection from waiting forever on
    a contended table lock, e.g. a concurrent DDL or an open transaction held by
    the operator.  DDL paths that need a shorter fail-fast window can ``SET LOCAL``
    a lower value and ``RESET`` afterwards.
    """
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")
            cur.execute("SET idle_in_transaction_session_timeout = 0")
            cur.execute("SET lock_timeout = 120000")
            cur.execute("SET application_name = 'dataflow'")
        conn.autocommit = False
    except Exception:
        try:
            conn.autocommit = False
        except Exception as exc:
            logger.warning("Exception suppressed: %s", exc, exc_info=exc)


def apply_mysql_session_guards(
    conn: Any,
    *,
    lock_wait_seconds: int = 120,
    require_strict_sql_mode: bool = True,
) -> None:
    """Raise MySQL session I/O / wait timeouts and enable fail-closed sql_mode.

    ``wait_timeout`` / ``interactive_timeout`` are raised so long-running transfers
    are not killed.  ``lock_wait_timeout`` / ``innodb_lock_wait_timeout`` default
    to 2 minutes so a contended metadata lock or row lock fails fast instead of
    hanging the transfer indefinitely. DDL/demo callers may pass a lower
    ``lock_wait_seconds`` (e.g. 30) so a 5-row overwrite cannot sit for minutes.

    STRICT_TRANS_TABLES (+ related modes) prevent silent truncation / invalid-date
    coercion that would otherwise look like a successful write with data loss.
    When ``require_strict_sql_mode`` is True (default), failure to enable STRICT
    modes raises — never continue a transfer that can silently truncate.
    """
    lock_s = max(5, min(int(lock_wait_seconds or 120), 600))
    try:
        with conn.cursor() as cur:
            cur.execute("SET SESSION wait_timeout = 28800")
            cur.execute("SET SESSION interactive_timeout = 28800")
            cur.execute("SET SESSION net_read_timeout = 600")
            cur.execute("SET SESSION net_write_timeout = 600")
            cur.execute(f"SET SESSION lock_wait_timeout = {lock_s}")
            cur.execute(f"SET SESSION innodb_lock_wait_timeout = {lock_s}")
            _ensure_mysql_strict_sql_mode(cur, require=require_strict_sql_mode)
    except RuntimeError:
        raise
    except Exception as exc:
        if require_strict_sql_mode:
            raise RuntimeError(
                f"MySQL session guards failed — refuse write without STRICT sql_mode: {exc}"
            ) from exc
        logger.warning("Exception suppressed: %s", exc, exc_info=exc)


_MYSQL_STRICT_MODES = (
    "STRICT_TRANS_TABLES",
    "STRICT_ALL_TABLES",
    "ERROR_FOR_DIVISION_BY_ZERO",
    "NO_ZERO_DATE",
    "NO_ZERO_IN_DATE",
    "NO_ENGINE_SUBSTITUTION",
)


def _ensure_mysql_strict_sql_mode(cur: Any, *, require: bool = True) -> None:
    """Append fail-closed sql_mode flags without wiping existing session modes."""
    try:
        cur.execute("SELECT @@SESSION.sql_mode")
        row = cur.fetchone()
        current = (row[0] if row else "") or ""
    except Exception as exc:
        if require:
            raise RuntimeError(
                f"Could not read MySQL sql_mode — refuse write without STRICT proof: {exc}"
            ) from exc
        logger.warning("Could not read MySQL sql_mode: %s", exc, exc_info=exc)
        return
    parts = [p.strip().upper() for p in str(current).split(",") if p.strip()]
    changed = False
    for mode in _MYSQL_STRICT_MODES:
        if mode not in parts:
            parts.append(mode)
            changed = True
    if not changed:
        return
    # Modes are fixed constants / server-returned tokens — not user input.
    new_mode = ",".join(parts)
    try:
        cur.execute("SET SESSION sql_mode = %s", (new_mode,))
    except Exception as exc:
        if require:
            raise RuntimeError(
                f"Could not enable MySQL STRICT sql_mode ({new_mode}) — "
                f"refuse silent truncation path: {exc}"
            ) from exc
        logger.warning(
            "Could not enable MySQL STRICT sql_mode (%s): %s",
            new_mode,
            exc,
            exc_info=exc,
        )


def apply_mssql_session_guards(
    conn: Any,
    *,
    require_ansi_warnings: bool = False,
) -> None:
    """Fail-closed SQL Server session: reject silent string truncation.

    With ``ANSI_WARNINGS OFF`` (common default on some drivers), oversized
    VARCHAR/NVARCHAR inserts truncate quietly. Force warnings ON so the
    engine errors; write-path ``quarantine_unfit_strings`` is the primary
    hold-out, this is defense-in-depth.
    When ``require_ansi_warnings`` is True (default), failure raises.
    """
    try:
        cur = getattr(conn, "cursor", None)
        if callable(cur):
            with conn.cursor() as c:
                c.execute("SET ANSI_WARNINGS ON")
                c.execute("SET ANSI_PADDING ON")
                try:
                    c.execute("SET CONCAT_NULL_YIELDS_NULL ON")
                except Exception:
                    pass
            return
        # SQLAlchemy Connection
        execute = getattr(conn, "execute", None)
        if callable(execute):
            import sqlalchemy as sa

            conn.execute(sa.text("SET ANSI_WARNINGS ON"))
            conn.execute(sa.text("SET ANSI_PADDING ON"))
            try:
                conn.execute(sa.text("SET CONCAT_NULL_YIELDS_NULL ON"))
            except Exception:
                pass
            return
        if require_ansi_warnings:
            raise RuntimeError(
                "SQL Server session guards could not obtain a cursor/execute handle"
            )
    except RuntimeError:
        raise
    except Exception as exc:
        if require_ansi_warnings:
            raise RuntimeError(
                f"SQL Server ANSI_WARNINGS guards failed — refuse silent truncate: {exc}"
            ) from exc
        logger.warning("Exception suppressed: %s", exc, exc_info=exc)


@dataclass(frozen=True)
class _RawLedgerSpec:
    """Per-dialect SQL for the chunk ledger on writers that use a raw cursor.

    Postgres, MySQL and SQLite each drive a DBAPI cursor directly rather than
    SQLAlchemy, so they cannot share ``sqlalchemy_ledger_table``. Describing the
    three differences that actually exist — column types, parameter marker, and
    the insert-ignore spelling — keeps one implementation instead of three
    copies that drift. The 'rows_written returns the recorded count' rule is the
    kind of correctness detail that only survives in a single implementation.
    """

    quote_char: str
    placeholder: str
    columns: str
    create_suffix: str
    insert_prefix: str
    insert_conflict: str


_RAW_LEDGER_SPECS: dict[str, _RawLedgerSpec] = {
    "postgresql": _RawLedgerSpec(
        quote_char='"',
        placeholder="%s",
        columns=(
            " job_id TEXT NOT NULL,"
            " batch_key TEXT NOT NULL,"
            " chunk_idx INTEGER NOT NULL,"
            " rows_written INTEGER NOT NULL DEFAULT 0,"
            " written_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
        ),
        create_suffix="",
        insert_prefix="INSERT INTO",
        insert_conflict=" ON CONFLICT DO NOTHING",
    ),
    "mysql": _RawLedgerSpec(
        quote_char="`",
        placeholder="%s",
        columns=(
            " job_id VARCHAR(128) NOT NULL,"
            " batch_key VARCHAR(255) NOT NULL,"
            " chunk_idx INT NOT NULL,"
            " rows_written INT NOT NULL DEFAULT 0,"
            " written_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        ),
        create_suffix=" ENGINE=InnoDB",
        insert_prefix="INSERT IGNORE INTO",
        insert_conflict="",
    ),
    "sqlite": _RawLedgerSpec(
        quote_char='"',
        placeholder="?",
        columns=(
            " job_id TEXT NOT NULL,"
            " batch_key TEXT NOT NULL,"
            " chunk_idx INTEGER NOT NULL,"
            " rows_written INTEGER NOT NULL DEFAULT 0,"
            " written_at TEXT NOT NULL DEFAULT (datetime('now')),"
        ),
        create_suffix="",
        insert_prefix="INSERT OR IGNORE INTO",
        insert_conflict="",
    ),
}


def _raw_ledger_spec(dialect: str) -> _RawLedgerSpec:
    spec = _RAW_LEDGER_SPECS.get((dialect or "").strip().lower())
    if spec is None:
        raise ValueError(f"No raw chunk ledger defined for dialect '{dialect}'")
    return spec


def _raw_ledger_ref(spec: _RawLedgerSpec, schema: str | None) -> str:
    """Fully-qualified, quoted ledger table reference."""
    from connectors.sql_identifiers import quote_sql_identifier

    table = quote_sql_identifier(LEDGER_TABLE, spec.quote_char)
    if schema:
        return f"{quote_sql_identifier(schema, spec.quote_char)}.{table}"
    return table


def ensure_raw_write_ledger(
    cur: Any, *, dialect: str, schema: str | None = None
) -> None:
    """Create the durable chunk ledger used to skip already-committed inserts."""
    spec = _raw_ledger_spec(dialect)
    ref = _raw_ledger_ref(spec, schema)
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {ref} ("  # nosec: B608 — identifiers are quoted, dialect SQL is a constant
        f"{spec.columns}"
        " PRIMARY KEY (job_id, batch_key, chunk_idx)"
        f"){spec.create_suffix}"
    )


def raw_chunk_rows_written(
    cur: Any,
    *,
    dialect: str,
    job_id: str,
    batch_key: str,
    chunk_idx: int,
    schema: str | None = None,
) -> int | None:
    """Rows this chunk committed in an earlier attempt, or ``None`` if it never did.

    Returning the recorded count rather than a boolean is what keeps a skipping
    retry honest. A chunk that quarantined some of its rows committed fewer rows
    than it held, so crediting ``len(batch)`` on replay would over-report the
    transfer and make reconcile disagree with the destination. Replaying the
    stored count reproduces exactly what the first attempt achieved.
    """
    spec = _raw_ledger_spec(dialect)
    ref = _raw_ledger_ref(spec, schema)
    ph = spec.placeholder
    cur.execute(
        f"SELECT rows_written FROM {ref} "  # nosec: B608 — identifiers are quoted, dialect SQL is a constant
        f"WHERE job_id = {ph} AND batch_key = {ph} AND chunk_idx = {ph}",
        (job_id, batch_key, chunk_idx),
    )
    row = cur.fetchone()
    if row is None:
        return None
    try:
        return max(0, int(row[0] or 0))
    except (TypeError, ValueError):
        return 0


def mark_raw_chunk_committed(
    cur: Any,
    *,
    dialect: str,
    job_id: str,
    batch_key: str,
    chunk_idx: int,
    rows_written: int,
    schema: str | None = None,
) -> None:
    """Record a committed chunk inside the same transaction as its data write."""
    spec = _raw_ledger_spec(dialect)
    ref = _raw_ledger_ref(spec, schema)
    ph = spec.placeholder
    cur.execute(
        f"{spec.insert_prefix} {ref} "  # nosec: B608 — identifiers are quoted, dialect SQL is a constant
        f"(job_id, batch_key, chunk_idx, rows_written) "
        f"VALUES ({ph}, {ph}, {ph}, {ph}){spec.insert_conflict}",
        (job_id, batch_key, chunk_idx, int(rows_written)),
    )


def sqlalchemy_ledger_table(metadata: Any, schema: str | None = None) -> Any:
    """Build the dialect-portable ``Table`` object for the chunk ledger.

    The Postgres and MySQL ledgers are hand-written DDL because those writers use
    raw DBAPI cursors. Every other SQL destination goes through SQLAlchemy, so
    modelling the ledger as a Core ``Table`` gets correct DDL, quoting, and type
    mapping for Snowflake, BigQuery, SQL Server, Oracle, DuckDB, Databricks and
    Synapse from one definition instead of seven dialect branches.

    ``VARCHAR`` lengths are explicit because Oracle and SQL Server reject
    unbounded ``VARCHAR`` in a primary key, and the composite key is what makes
    the "was this chunk already committed?" lookup exact.
    """
    import sqlalchemy as sa

    return sa.Table(
        LEDGER_TABLE,
        metadata,
        sa.Column("job_id", sa.String(128), primary_key=True, nullable=False),
        sa.Column("batch_key", sa.String(255), primary_key=True, nullable=False),
        sa.Column("chunk_idx", sa.Integer, primary_key=True, nullable=False),
        sa.Column("rows_written", sa.Integer, nullable=False, default=0),
        sa.Column(
            "written_at",
            sa.DateTime(timezone=True),
            nullable=False,
            default=lambda: datetime.now(timezone.utc),
        ),
        schema=schema or None,
        keep_existing=True,
    )


def ensure_sqlalchemy_write_ledger(
    conn: Any,
    *,
    schema: str | None = None,
) -> Any | None:
    """Create the chunk ledger for a SQLAlchemy destination if it is absent.

    Returns the ``Table`` object, or ``None`` when the ledger cannot be created.
    A ``None`` return is not fatal — the caller degrades to unguarded writes —
    but it *is* reported so the operator learns that retries on this destination
    may duplicate rows rather than discovering it from a row count later.
    """
    import sqlalchemy as sa

    try:
        metadata = sa.MetaData()
        table = sqlalchemy_ledger_table(metadata, schema)
        table.create(bind=conn, checkfirst=True)
        return table
    except Exception as exc:
        logger.warning(
            "Could not create write ledger%s: %s. Chunk retries on this "
            "destination cannot be de-duplicated.",
            f" in schema {schema}" if schema else "",
            exc,
        )
        return None


def sqlalchemy_chunk_rows_written(
    conn: Any,
    table: Any,
    *,
    job_id: str,
    batch_key: str,
    chunk_idx: int,
) -> int | None:
    """Rows this chunk committed in an earlier attempt, or ``None`` if it never did.

    Returning the recorded count rather than a boolean is what keeps the retry
    honest. A chunk that quarantined some rows committed fewer rows than it held,
    so a skipping retry that assumed "whole batch landed" would inflate the
    reported total and make reconcile disagree with the destination. Replaying
    the stored count reproduces exactly what the first attempt achieved.
    """
    import sqlalchemy as sa

    stmt = sa.select(table.c.rows_written).where(
        sa.and_(
            table.c.job_id == job_id,
            table.c.batch_key == batch_key,
            table.c.chunk_idx == chunk_idx,
        )
    )
    row = conn.execute(stmt).first()
    if row is None:
        return None
    try:
        return max(0, int(row[0] or 0))
    except (TypeError, ValueError):
        return 0


def mark_sqlalchemy_chunk_committed(
    conn: Any,
    table: Any,
    *,
    job_id: str,
    batch_key: str,
    chunk_idx: int,
    rows_written: int,
) -> None:
    """Record a committed chunk.

    Must be executed inside the same transaction as the chunk's data write, so
    the ledger row and the rows it vouches for commit or roll back together. A
    ledger entry written in a separate transaction could survive a rolled-back
    data write and cause the retry to skip a chunk that never landed — losing
    rows, which is worse than duplicating them.

    An insert conflict means a concurrent attempt already claimed the chunk;
    that is the idempotent outcome we want, so it is swallowed deliberately.
    """
    from datetime import datetime as _dt
    from datetime import timezone as _tz

    try:
        conn.execute(
            table.insert().values(
                job_id=job_id,
                batch_key=batch_key,
                chunk_idx=chunk_idx,
                rows_written=int(rows_written),
                written_at=_dt.now(_tz.utc),
            )
        )
    except Exception as exc:
        if _is_duplicate_key_error(exc):
            return
        raise


def _is_duplicate_key_error(exc: BaseException) -> bool:
    """Whether an exception is a primary-key / unique violation."""
    text = str(exc).lower()
    signals = (
        "duplicate key",
        "unique constraint",
        "uniqueness constraint",
        "already exists",
        "duplicate entry",
        "integrityerror",
        "unique index",
        "violation of primary key",
        "sqlstate[23",
    )
    return any(s in text for s in signals) or type(exc).__name__ == "IntegrityError"


def build_write_batch_key(
    *,
    table_name: str,
    file_batch_idx: int | None = None,
    extra: str | None = None,
) -> str:
    parts = [table_name or "table"]
    if file_batch_idx is not None:
        parts.append(str(file_batch_idx))
    if extra:
        parts.append(extra)
    return ":".join(parts)[:240]


def cleanup_write_ledger(
    *,
    dest_type: str,
    cfg: dict[str, Any],
    job_id: str | None,
) -> None:
    """Best-effort delete of per-job ledger rows after a successful transfer."""
    if not job_id:
        return
    dest = (dest_type or "").lower()
    try:
        if dest in {"postgresql", "redshift"}:
            from psycopg2 import sql

            from connectors.postgresql_conn import get_connection

            conn = get_connection(
                host=cfg.get("host", ""),
                port=int(cfg.get("port") or (5439 if dest == "redshift" else 5432)),
                database=cfg.get("database", ""),
                username=cfg.get("username", ""),
                password=cfg.get("password", ""),
                connection_string=cfg.get("connection_string", ""),
                ssl=bool(cfg.get("ssl", False)),
            )
            try:
                from services.dialect_profiles import default_schema_for

                schema = cfg.get("schema") or default_schema_for(dest) or "public"
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("DELETE FROM {}.{} WHERE job_id = %s").format(
                            sql.Identifier(schema),
                            sql.Identifier(LEDGER_TABLE),
                        ),
                        (job_id,),
                    )
                conn.commit()
            finally:
                close_quietly(conn)
        elif dest == "mysql":
            from connectors.mysql_conn import get_connection

            conn = get_connection(
                host=cfg.get("host", ""),
                port=int(cfg.get("port") or 3306),
                database=cfg.get("database", ""),
                username=cfg.get("username", ""),
                password=cfg.get("password", ""),
                connection_string=cfg.get("connection_string", ""),
                ssl=bool(cfg.get("ssl", False)),
            )
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"DELETE FROM `{LEDGER_TABLE}` WHERE job_id = %s",  # nosec: B608 — LEDGER_TABLE is a module constant
                        (job_id,),
                    )
                conn.commit()
            finally:
                close_quietly(conn)
        else:
            _cleanup_sqlalchemy_ledger(dest_type=dest, cfg=cfg, job_id=job_id)
    except Exception as exc:
        # Ledger cleanup must never fail a successful transfer.
        logger.warning("Exception suppressed: %s", exc, exc_info=exc)


def _cleanup_sqlalchemy_ledger(
    *,
    dest_type: str,
    cfg: dict[str, Any],
    job_id: str,
) -> None:
    """Drop this job's ledger rows on a SQLAlchemy destination.

    Skipped when the destination never had a ledger, so we do not open a
    connection just to look for a table that cannot exist.
    """
    from services.replay_safety import destination_has_chunk_ledger

    if not destination_has_chunk_ledger(dest_type):
        return

    import sqlalchemy as sa

    from connectors.generic_sql import _engine, _schema_name
    from services.engine_pool import release_engine

    engine = None
    try:
        engine = _engine({**cfg, "type": dest_type})
        schema = _schema_name({**cfg, "type": dest_type})
        table = sqlalchemy_ledger_table(sa.MetaData(), schema)
        with engine.connect() as conn:
            if not sa.inspect(conn).has_table(LEDGER_TABLE, schema=schema or None):
                return
            conn.execute(table.delete().where(table.c.job_id == job_id))
            conn.commit()
    finally:
        if engine is not None:
            release_engine(engine)
