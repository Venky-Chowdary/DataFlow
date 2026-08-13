"""Server-to-server COPY for routes whose types are proven identical.

The per-row Python path costs about 3,200 rows/sec on a box where a raw
``COPY`` between two PostgreSQL tables sustains 553,000. A 50M-row table — an
ordinary first customer table for a migration — is the difference between four
hours and three minutes. Profiling put the cost in per-cell work: mapping,
transform resolution, quarantine checks and fingerprinting, all of which exist
to reconcile two *different* type systems.

When the two sides declare the same type for every mapped column, none of that
work can change a value, so none of it needs to run. This path streams
``COPY (SELECT …) TO STDOUT (FORMAT binary)`` straight into
``COPY … FROM STDIN (FORMAT binary)`` and the rows never become Python objects.

Binary format is deliberate: it is PostgreSQL's own on-the-wire representation,
so an identical type on both ends round-trips without a text rendering in
between. Text format would reintroduce exactly the parse-and-render step this
path exists to remove, and with it every locale, precision and escaping question
that step brings.

**Proof.** Skipping per-row work also skips per-row fingerprints, so the run
would have no evidence at all unless the proof moves with it. The source digest
is therefore computed *inside the same transaction* that feeds the COPY. That is
not an optimization but the correctness argument: a digest taken afterwards on a
fresh connection sees rows written after the snapshot began and reports a
mismatch on a transfer that was right. Under ``REPEATABLE READ`` the digest and
the copied rows are the same population by construction.

The path declines rather than guesses. Anything it cannot prove — a differing
type, a transform, a filter, a non-PostgreSQL end — falls back to the row path,
which knows how to reconcile those cases.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Any, NamedTuple

logger = logging.getLogger(__name__)

#: Pipe buffer between the two COPY cursors. Large enough that the reader is not
#: woken per row, small enough that a stalled destination applies backpressure to
#: the source instead of buffering a whole table in memory.
_PIPE_CHUNK = 1 << 20


class FastPathResult(NamedTuple):
    """What the copy moved, and the evidence that it arrived intact."""

    rows_copied: int
    source_rows: int
    source_checksum: str
    target_rows: int
    target_checksum: str

    @property
    def verified(self) -> bool:
        return (
            self.source_rows == self.target_rows
            and bool(self.source_checksum)
            and self.source_checksum == self.target_checksum
        )


class FastPathUnavailable(Exception):
    """Raised when the route cannot be proven identical — caller falls back."""


def _quote(name: str) -> str:
    from connectors.sql_identifiers import quote_sql_identifier, require_safe_identifier

    return quote_sql_identifier(require_safe_identifier(name, preserve_case=True))


def _table_ref(schema: str, table: str) -> str:
    from connectors.sql_identifiers import quote_table_ref

    return quote_table_ref(table, schema or "public", dialect="postgresql")


def source_column_types(
    cur: Any, schema: str, table: str, columns: list[str]
) -> dict[str, str]:
    """Declared type of each column, as PostgreSQL itself spells it.

    ``format_type`` is used rather than ``information_schema`` so a type modifier
    survives: ``numeric(12,2)`` and unconstrained ``numeric`` are different
    carriers and must not be reported as one.
    """
    cur.execute(
        """
        SELECT a.attname, format_type(a.atttypid, a.atttypmod)
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = %s AND c.relname = %s
          AND a.attnum > 0 AND NOT a.attisdropped
        """,
        (schema or "public", table),
    )
    live = {str(name): str(typ) for name, typ in cur.fetchall()}
    wanted = {c.lower() for c in columns}
    return {k: v for k, v in live.items() if k.lower() in wanted}


def create_destination_like_source(
    cur: Any,
    schema: str,
    table: str,
    pairs: list[tuple[str, str]],
    source_types: dict[str, str],
) -> None:
    """Create the destination with the source's own declared types.

    The copy is only legal because the two sides agree on every column type, so
    the destination is built from the source's declaration rather than from an
    invented mapping. Types are taken from ``format_type`` output, which is
    already a valid type expression, and column names are quoted identifiers.
    """
    lowered = {k.lower(): v for k, v in source_types.items()}
    cols: list[str] = []
    for source_col, target_col in pairs:
        declared = lowered.get(source_col.lower())
        if not declared:
            raise FastPathUnavailable(
                f"source column {source_col!r} has no declared type"
            )
        cols.append(f"{_quote(target_col)} {declared}")
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {_table_ref(schema, table)} ({', '.join(cols)})"  # nosec B608
    )


def _connect(cfg: dict[str, Any]) -> Any:
    from connectors.postgresql_conn import get_connection

    return get_connection(
        host=cfg.get("host", ""),
        port=int(cfg.get("port") or 5432),
        database=cfg.get("database", ""),
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        connection_string=cfg.get("connection_string", ""),
        ssl=bool(cfg.get("ssl", False)),
    )


def _stream_copy(
    source_cur: Any,
    dest_cur: Any,
    source_sql: str,
    dest_sql: str,
) -> None:
    """Pipe one COPY into the other without holding the table in memory.

    The source runs on a thread writing into an OS pipe while the destination
    reads from it, so a slow destination blocks the source through the pipe
    rather than accumulating rows. Both ends are closed on every exit path: an
    unclosed write end leaves the reader waiting for EOF that never comes.
    """
    read_fd, write_fd = os.pipe()
    failure: list[BaseException] = []

    def _pump() -> None:
        try:
            with os.fdopen(write_fd, "wb", _PIPE_CHUNK) as writer:
                source_cur.copy_expert(source_sql, writer)
        except BaseException as exc:  # noqa: BLE001 — re-raised on the main thread
            failure.append(exc)

    pump = threading.Thread(target=_pump, name="copy-fast-path", daemon=True)
    pump.start()
    try:
        with os.fdopen(read_fd, "rb", _PIPE_CHUNK) as reader:
            dest_cur.copy_expert(dest_sql, reader)
    except BaseException:
        # Drain so the writer's blocked write() returns and the thread can exit
        # instead of holding the source transaction open.
        try:
            os.close(read_fd)
        except OSError:
            pass
        pump.join(timeout=30)
        raise
    finally:
        pump.join(timeout=30)
    if failure:
        raise failure[0]


def copy_between_postgres(
    *,
    source_cfg: dict[str, Any],
    source_schema: str,
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_schema: str,
    dest_table: str,
    pairs: list[tuple[str, str]],
    replace_destination: bool = True,
) -> FastPathResult:
    """Copy a population between two PostgreSQL tables and prove it arrived.

    ``pairs`` are ordered ``(source_column, target_column)``; the caller has
    already established that each pair shares a declared type. Renames are free
    because both sides project their own names in the same order.
    """
    if not pairs:
        raise FastPathUnavailable("no comparable columns")
    from services.engine_checksum import postgresql_engine_checksum

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    source_ref = _table_ref(source_schema, source_table)
    dest_ref = _table_ref(dest_schema, dest_table)
    source_list = ", ".join(_quote(c) for c in source_cols)
    target_list = ", ".join(_quote(c) for c in target_cols)

    source_conn = _connect(source_cfg)
    dest_conn = _connect(dest_cfg)
    try:
        source_conn.autocommit = False
        dest_conn.autocommit = False
        with source_conn.cursor() as src_cur, dest_conn.cursor() as dst_cur:
            # One snapshot for the whole read: the digest below and the rows the
            # COPY emits have to be the same population, or the proof describes
            # something other than what was copied.
            src_cur.execute(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )
            declared = source_column_types(
                src_cur, source_schema, source_table, source_cols
            )
            missing = [c for c in source_cols if c.lower() not in
                       {k.lower() for k in declared}]
            if missing:
                raise FastPathUnavailable(
                    f"source columns absent from catalog: {', '.join(missing)}"
                )
            create_destination_like_source(
                dst_cur, dest_schema, dest_table, pairs, declared
            )
            if replace_destination:
                # A full refresh replaces rather than adds. TRUNCATE rather than
                # DELETE: this runs in the same transaction as the load, so a
                # failure rolls back to the previous contents.
                dst_cur.execute(f"TRUNCATE TABLE {dest_ref}")  # nosec B608

            source_digest = postgresql_engine_checksum(
                src_cur, source_ref, source_cols
            )
            if source_digest is None:
                raise FastPathUnavailable("source digest unavailable")

            _stream_copy(
                src_cur,
                dst_cur,
                f"COPY (SELECT {source_list} FROM {source_ref}) "  # nosec B608
                "TO STDOUT (FORMAT binary)",
                f"COPY {dest_ref} ({target_list}) FROM STDIN (FORMAT binary)",  # nosec B608
            )
            rows_copied = int(dst_cur.rowcount or 0)

            dest_digest = postgresql_engine_checksum(dst_cur, dest_ref, target_cols)
            if dest_digest is None:
                raise FastPathUnavailable("destination digest unavailable")

        result = FastPathResult(
            rows_copied=rows_copied,
            source_rows=source_digest.row_count,
            source_checksum=source_digest.checksum,
            target_rows=dest_digest.row_count,
            target_checksum=dest_digest.checksum,
        )
        if not result.verified:
            # The destination transaction has not committed, so refusing here
            # leaves the table as it was rather than half-replaced.
            dest_conn.rollback()
            source_conn.rollback()
            raise ValueError(
                "COPY fast path refused: destination does not match the source "
                f"snapshot (source {result.source_rows} rows / "
                f"{result.source_checksum}, destination {result.target_rows} rows / "
                f"{result.target_checksum})"
            )
        dest_conn.commit()
        source_conn.commit()
        return result
    except Exception:
        for conn in (dest_conn, source_conn):
            try:
                conn.rollback()
            except Exception as exc:
                logger.debug("rollback after fast-path failure: %s", exc)
        raise
    finally:
        for conn in (dest_conn, source_conn):
            try:
                conn.close()
            except Exception as exc:
                logger.debug("close after fast path: %s", exc)
