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

import os
import time
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

_CHUNK_RECONNECT_ATTEMPTS = int(os.getenv("DATAFLOW_WRITE_RECONNECT_ATTEMPTS", "12"))
# Smaller commits on public TCP proxies (Railway etc.) — large COPY/INSERT
# windows are the #1 cause of "server closed the connection unexpectedly".
_PROXY_CHUNK_SIZE = int(os.getenv("DATAFLOW_PROXY_CHUNK_SIZE", "1000"))
_RECONNECT_MAX_SECONDS = float(os.getenv("DATAFLOW_WRITE_RECONNECT_MAX_SECONDS", "600"))

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
        is_sql_data_error = lambda _e: False  # noqa: E731
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
    return base + random.uniform(0, 0.45)


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
    except Exception:
        pass


def apply_postgres_session_guards(conn: Any) -> None:
    """Disable aggressive statement/idle kills that abort long Railway transfers."""
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")
            cur.execute("SET idle_in_transaction_session_timeout = 0")
            cur.execute("SET lock_timeout = 0")
            cur.execute("SET application_name = 'dataflow'")
        conn.autocommit = False
    except Exception:
        try:
            conn.autocommit = False
        except Exception:
            pass


def apply_mysql_session_guards(conn: Any) -> None:
    """Raise MySQL session I/O / wait timeouts for long bulk loads."""
    try:
        with conn.cursor() as cur:
            cur.execute("SET SESSION wait_timeout = 28800")
            cur.execute("SET SESSION interactive_timeout = 28800")
            cur.execute("SET SESSION net_read_timeout = 600")
            cur.execute("SET SESSION net_write_timeout = 600")
    except Exception:
        pass


def ensure_postgres_write_ledger(cur: Any, schema: str = "public") -> None:
    """Create the durable chunk ledger used to skip already-committed inserts."""
    from psycopg2 import sql

    cur.execute(
        sql.SQL(
            "CREATE TABLE IF NOT EXISTS {}.{} ("
            " job_id TEXT NOT NULL,"
            " batch_key TEXT NOT NULL,"
            " chunk_idx INTEGER NOT NULL,"
            " rows_written INTEGER NOT NULL DEFAULT 0,"
            " written_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
            " PRIMARY KEY (job_id, batch_key, chunk_idx)"
            ")"
        ).format(sql.Identifier(schema), sql.Identifier(LEDGER_TABLE))
    )


def postgres_chunk_committed(
    cur: Any,
    *,
    schema: str,
    job_id: str,
    batch_key: str,
    chunk_idx: int,
) -> bool:
    from psycopg2 import sql

    cur.execute(
        sql.SQL(
            "SELECT 1 FROM {}.{} WHERE job_id = %s AND batch_key = %s AND chunk_idx = %s"
        ).format(sql.Identifier(schema), sql.Identifier(LEDGER_TABLE)),
        (job_id, batch_key, chunk_idx),
    )
    return cur.fetchone() is not None


def mark_postgres_chunk_committed(
    cur: Any,
    *,
    schema: str,
    job_id: str,
    batch_key: str,
    chunk_idx: int,
    rows_written: int,
) -> None:
    from psycopg2 import sql

    cur.execute(
        sql.SQL(
            "INSERT INTO {}.{} (job_id, batch_key, chunk_idx, rows_written) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING"
        ).format(sql.Identifier(schema), sql.Identifier(LEDGER_TABLE)),
        (job_id, batch_key, chunk_idx, rows_written),
    )


def ensure_mysql_write_ledger(cur: Any) -> None:
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS `{LEDGER_TABLE}` ("
        " job_id VARCHAR(128) NOT NULL,"
        " batch_key VARCHAR(255) NOT NULL,"
        " chunk_idx INT NOT NULL,"
        " rows_written INT NOT NULL DEFAULT 0,"
        " written_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        " PRIMARY KEY (job_id, batch_key, chunk_idx)"
        ") ENGINE=InnoDB"
    )


def mysql_chunk_committed(
    cur: Any,
    *,
    job_id: str,
    batch_key: str,
    chunk_idx: int,
) -> bool:
    cur.execute(
        f"SELECT 1 FROM `{LEDGER_TABLE}` WHERE job_id = %s AND batch_key = %s AND chunk_idx = %s",
        (job_id, batch_key, chunk_idx),
    )
    return cur.fetchone() is not None


def mark_mysql_chunk_committed(
    cur: Any,
    *,
    job_id: str,
    batch_key: str,
    chunk_idx: int,
    rows_written: int,
) -> None:
    cur.execute(
        f"INSERT IGNORE INTO `{LEDGER_TABLE}` (job_id, batch_key, chunk_idx, rows_written) "
        "VALUES (%s, %s, %s, %s)",
        (job_id, batch_key, chunk_idx, rows_written),
    )


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
            from connectors.postgresql_conn import get_connection
            from psycopg2 import sql

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
                schema = cfg.get("schema") or "public"
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
                        f"DELETE FROM `{LEDGER_TABLE}` WHERE job_id = %s",
                        (job_id,),
                    )
                conn.commit()
            finally:
                close_quietly(conn)
    except Exception:
        # Ledger cleanup must never fail a successful transfer.
        pass
