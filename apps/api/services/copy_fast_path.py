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

**Structure travels too.** A copy that moved only values would hand back a table
that enforces fewer rules than its source. The primary key, nullability and
defaults are applied at CREATE; secondary indexes — including the UNIQUE ones
that are data-integrity guarantees, not optimisations — are reproduced after the
bulk load, when building each index once over the finished population is both
faster and safer than maintaining it per COPYed row. An index this path cannot
reproduce identically (an expression index, a filtered predicate it cannot
prove equivalent) declines the whole route rather than shipping a destination
that quietly enforces a different rule.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
from contextvars import ContextVar, Token
from typing import Any, NamedTuple

logger = logging.getLogger(__name__)

#: Pipe buffer between the two COPY cursors. Large enough that the reader is not
#: woken per row, small enough that a stalled destination applies backpressure to
#: the source instead of buffering a whole table in memory.
_PIPE_CHUNK = 1 << 22
_COPY_DECLINE: ContextVar[list[str] | None] = ContextVar("df_copy_decline", default=None)


class FastPathResult(NamedTuple):
    """What the copy moved, and the evidence that it arrived intact."""

    rows_copied: int
    source_rows: int
    source_checksum: str
    target_rows: int
    target_checksum: str
    #: Evidence for the snapshot the read ran under. The digest is only
    #: comparable because the rows came from this snapshot, so the claim travels
    #: with the result rather than being asserted about it.
    source_snapshot: dict[str, Any] = {}
    #: Secondary indexes reproduced on the destination after the bulk load. A
    #: value copy that left these behind would hand back a table that enforces
    #: fewer rules and reads more slowly than its source, so they travel with the
    #: result the same way the primary key and defaults do.
    indexes_carried: tuple[str, ...] = ()
    #: What the checksum proves. Mapped column values, not triggers, RLS,
    #: unmapped columns, or destination objects this path declined.
    proof_scope: str = "mapped_columns"

    @property
    def verified(self) -> bool:
        return (
            self.source_rows == self.target_rows
            and bool(self.source_checksum)
            and self.source_checksum == self.target_checksum
        )


def begin_copy_decline_capture(sink: list[str] | None = None) -> tuple[Token, list[str]]:
    """Record FastPathUnavailable reasons for the operator dest_summary."""
    bucket = sink if sink is not None else []
    token = _COPY_DECLINE.set(bucket)
    return token, bucket


def reset_copy_decline_capture(token: Token) -> None:
    _COPY_DECLINE.reset(token)


def note_copy_decline(reason: str, *, log: bool = True) -> None:
    """Append a COPY decline reason. Duplicate text in one capture is skipped."""
    text = str(reason or "").strip()
    if not text:
        return
    if log:
        logger.info("COPY fast path declined: %s", text)
    sink = _COPY_DECLINE.get()
    if sink is not None and text not in sink:
        sink.append(text)


class FastPathUnavailable(Exception):
    """Raised when the route cannot be proven identical — caller falls back."""

    def __init__(self, message: str = "") -> None:
        super().__init__(message)
        note_copy_decline(str(message), log=False)


def declared_copy_carrier(
    item: dict[str, Any],
    schema: dict[str, str],
    source_col: str,
    target_col: str,
) -> str:
    """The carrier a fast-path CREATE declares for one mapped column.

    A Map mapping states the destination carrier as ``target_type`` and the
    source's as ``source_type``; only a schema-derived mapping carries ``type``.
    Consulting ``type`` and the introspected schema alone meant a caller that
    passes no schema — the multi-stream contract route passes an empty one and
    re-introspects inside each stream — fell through to the ``TEXT`` default for
    every column, so a declared BIGINT key landed as SQLite ``TEXT`` and its
    values were stored as text.
    """
    return str(
        item.get("type")
        or item.get("target_type")
        or item.get("source_type")
        or schema.get(source_col)
        or schema.get(target_col)
        or ""
    )


def fifo_streaming_supported() -> bool:
    """Does this host have named pipes the COPY routes stream through?"""
    return hasattr(os, "mkfifo")


def require_fifo_streaming(route: str) -> None:
    """Decline a FIFO-streamed route on a host without named pipes.

    ``os.mkfifo`` is POSIX-only. Discovering that inside the copy is a *job*
    failure, not a decline: by then the destination may already have been
    recreated, so the row writer never gets its turn and the route is simply
    unusable on Windows. Refusing here is the same contract as every other
    unmet precondition — the caller falls back and the rows still land.
    """
    if not fifo_streaming_supported():
        raise FastPathUnavailable(
            f"{route} COPY streams through a named pipe (os.mkfifo), "
            f"which {sys.platform} does not provide"
        )


def skip_complete_identity_copy(
    *,
    source_count: int,
    dest_count: int,
    shard_mode: str,
    extra_snapshot: dict[str, Any] | None = None,
) -> FastPathResult:
    """Occupied dest whose COUNT already equals source COUNT — skip write, keep proof.

    Identity COPY engines share this result shape so skip-complete cannot drift
    per connector. Proof remains dest COUNT, never upsert ack.
    """
    proof = f"dest_count:{dest_count}"
    snapshot = {
        "copy_workers": 1,
        "copy_split": "skip",
        "copy_partitions": 1,
        "partitions_skipped": 1,
        "partitions_loaded": 0,
        "shard_mode": shard_mode,
        **(extra_snapshot or {}),
    }
    return FastPathResult(
        rows_copied=source_count,
        source_rows=source_count,
        source_checksum=proof,
        target_rows=dest_count,
        target_checksum=proof,
        source_snapshot=snapshot,
        proof_scope="dest_count_equals_source_snapshot_count",
    )


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


class SourceShape(NamedTuple):
    """The source table's columns and the structure attached to them."""

    types: dict[str, str]
    not_null: set[str]
    defaults: dict[str, str]
    primary_key: list[str]
    #: Non-default collations, keyed by column, already quoted for ``COLLATE``.
    #: A column whose collation is the type default is absent — carrying the
    #: default would pin dest to a collation the source did not declare.
    collations: dict[str, str]


def source_table_shape(cur: Any, schema: str, table: str, columns: list[str]) -> SourceShape:
    """Read the columns *and* the structure that must travel with them.

    A copy that moved only values would leave the destination without its keys,
    nullability or defaults — structure the row path carries and Property 6
    re-reads from the destination catalog. Losing it quietly is worse than being
    slow, so it is read here and applied at CREATE.
    """
    cur.execute(
        """
        SELECT a.attname,
               format_type(a.atttypid, a.atttypmod),
               a.attnotnull,
               pg_get_expr(ad.adbin, ad.adrelid),
               CASE
                 WHEN a.attcollation <> 0
                  AND a.attcollation <> t.typcollation
                 THEN quote_ident(nc.nspname) || '.' || quote_ident(col.collname)
                 ELSE NULL
               END
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
        LEFT JOIN pg_catalog.pg_collation col ON col.oid = a.attcollation
        LEFT JOIN pg_catalog.pg_namespace nc ON nc.oid = col.collnamespace
        LEFT JOIN pg_catalog.pg_attrdef ad
          ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
        WHERE n.nspname = %s AND c.relname = %s
          AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
        """,
        (schema or "public", table),
    )
    wanted = {c.lower() for c in columns}
    types: dict[str, str] = {}
    not_null: set[str] = set()
    defaults: dict[str, str] = {}
    collations: dict[str, str] = {}
    for name, declared, notnull, default, collation in cur.fetchall():
        if str(name).lower() not in wanted:
            continue
        types[str(name)] = str(declared)
        if notnull:
            not_null.add(str(name))
        if default:
            defaults[str(name)] = str(default)
        if collation:
            collations[str(name)] = str(collation)

    cur.execute(
        """
        SELECT a.attname
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class c ON c.oid = i.indrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_attribute a
          ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
        WHERE n.nspname = %s AND c.relname = %s AND i.indisprimary
        ORDER BY array_position(i.indkey, a.attnum)
        """,
        (schema or "public", table),
    )
    primary_key = [str(r[0]) for r in cur.fetchall()]
    return SourceShape(types, not_null, defaults, primary_key, collations)


def unsupported_structure(cur: Any, schema: str, table: str) -> str:
    """Name any structure this path cannot carry, so it can decline instead.

    Unique and check constraints, foreign keys, identity and generated columns
    all change what the destination table *is*. Carrying values without them
    would hand back a table that looks right and behaves differently, so a
    source that has them belongs on the row path, which knows how to reproduce
    them.
    """
    cur.execute(
        """
        SELECT DISTINCT con.contype
        FROM pg_catalog.pg_constraint con
        JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s AND con.contype <> 'p'
        """,
        (schema or "public", table),
    )
    kinds = {
        "u": "unique constraint",
        "c": "check constraint",
        "f": "foreign key",
        "x": "exclusion constraint",
    }
    found = [kinds.get(str(r[0]), str(r[0])) for r in cur.fetchall()]
    cur.execute(
        """
        SELECT count(*)
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0
          AND NOT a.attisdropped
          AND (a.attidentity <> '' OR a.attgenerated <> '')
        """,
        (schema or "public", table),
    )
    if int(cur.fetchone()[0] or 0):
        found.append("identity or generated column")
    cur.execute(
        """
        SELECT count(*)
        FROM pg_catalog.pg_trigger t
        JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s AND NOT t.tgisinternal
        """,
        (schema or "public", table),
    )
    if int(cur.fetchone()[0] or 0):
        found.append("trigger")
    cur.execute(
        """
        SELECT c.relrowsecurity
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
        """,
        (schema or "public", table),
    )
    row = cur.fetchone()
    if row and bool(row[0]):
        found.append("row level security")
    return ", ".join(sorted(set(found)))


def create_destination_like_source(
    cur: Any,
    schema: str,
    table: str,
    pairs: list[tuple[str, str]],
    shape: SourceShape,
) -> None:
    """Create the destination with the source's types, keys, nullability and defaults.

    Built from the source's own declarations rather than an invented mapping,
    which is what makes "identical" true by construction. ``format_type`` and
    ``pg_get_expr`` both return valid SQL expressions; identifiers are quoted.
    """
    lowered = {k.lower(): v for k, v in shape.types.items()}
    not_null = {c.lower() for c in shape.not_null}
    defaults = {k.lower(): v for k, v in shape.defaults.items()}
    collations = {k.lower(): v for k, v in shape.collations.items()}
    rename = {s.lower(): t for s, t in pairs}

    cols: list[str] = []
    for source_col, target_col in pairs:
        declared = lowered.get(source_col.lower())
        if not declared:
            raise FastPathUnavailable(
                f"source column {source_col!r} has no declared type"
            )
        piece = f"{_quote(target_col)} {declared}"
        collation = collations.get(source_col.lower())
        if collation:
            piece += f" COLLATE {collation}"
        default = defaults.get(source_col.lower())
        if default:
            if "nextval(" in default.lower():
                raise FastPathUnavailable(
                    f"source column {source_col!r} DEFAULT references a sequence "
                    "that cannot be proven present on the destination"
                )
            piece += f" DEFAULT {default}"
        if source_col.lower() in not_null:
            piece += " NOT NULL"
        cols.append(piece)

    # The key only carries when every one of its columns is being copied; a
    # partial key is not the same constraint and must not be invented.
    pk_targets = [rename.get(c.lower()) for c in shape.primary_key]
    if shape.primary_key and all(pk_targets):
        cols.append(
            "PRIMARY KEY (" + ", ".join(_quote(c) for c in pk_targets if c) + ")"
        )
    cur.execute(
        f"CREATE TABLE {_table_ref(schema, table)} ({', '.join(cols)})"  # nosec B608
    )


def plan_secondary_index_carry(
    cur: Any,
    *,
    source_schema: str,
    source_table: str,
    dest_schema: str,
    dest_table: str,
    pairs: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    """Portable ``(index_name, CREATE INDEX …)`` for every source secondary index.

    A secondary index is not only a read optimisation: a UNIQUE index is a
    data-integrity guarantee and a partial index scopes that guarantee to a
    subset of rows. Copying the values without it hands back a table that
    *looks* right and enforces a different rule — the same silent structure loss
    the primary key, nullability and defaults are carried here to avoid.

    The plan is built from the shared ``services.secondary_indexes`` planner so
    the fast path and the row path decide index carry the same way. It is read
    under the caller's snapshot cursor, so the catalog it sees is the one that
    describes the rows being copied.

    Raises :class:`FastPathUnavailable` — sending the whole route to the row
    path — whenever an index cannot be reproduced faithfully (an unreadable
    catalog, an expression index, an unrenderable partial predicate). Declining
    is deliberate: a fast copy that quietly dropped a UNIQUE guarantee would be
    worse than a slower one that kept it.
    """
    from dataclasses import replace

    from services.secondary_indexes import plan_index_carry, probe_secondary_indexes

    indexes = probe_secondary_indexes(
        "postgresql", cur, source_schema or "public", source_table
    )
    if indexes.status != "measured":
        # An unreadable catalog is not proof the table has no indexes, so it is
        # not safe to certify "identical" — hand the route to the row path.
        raise FastPathUnavailable(
            f"source index catalog unreadable ({indexes.detail or 'no detail'}); "
            "cannot prove the destination would carry every index"
        )
    if not indexes.items:
        return []

    column_map = {s: t for s, t in pairs}
    copied = {s.lower() for s, _ in pairs}

    # Two kinds of index are handled elsewhere and must not reach the planner:
    #
    # * A PRIMARY KEY / UNIQUE constraint's backing index — the primary key is
    #   emitted at CREATE and a unique *constraint* already declined the route in
    #   ``unsupported_structure``. Carrying it here would either duplicate the
    #   key or, for a partial key whose columns were not all copied, wrongly
    #   decline a route the create step deliberately allows without the key.
    # * A plain-column index over a column the transfer intentionally drops — the
    #   rule it enforced cannot exist once its column is gone, exactly as a
    #   partial primary key is not reinvented. Skip it rather than decline.
    #
    # An expression index has no plain key columns to check against ``copied``;
    # it is deliberately *not* filtered out here so the planner sees it and
    # refuses, which declines the route. Silently skipping it would drop a
    # case-insensitive UNIQUE guarantee — the very loss this carry exists to
    # prevent.
    def _skip(item: Any) -> bool:
        if item.constraint_backed:
            return True
        if item.expression:
            return False
        return any(c.name.lower() not in copied for c in item.columns)

    carryable = tuple(item for item in indexes.items if not _skip(item))
    if not carryable:
        return []

    decisions = plan_index_carry(
        replace(indexes, items=carryable),
        dest_dialect="postgresql",
        dest_table=dest_table,
        dest_schema=dest_schema or "public",
        column_map=column_map,
        quote=_quote,
    )
    carried: list[tuple[str, str]] = []
    for decision in decisions:
        if decision.carried and decision.dest_sql:
            carried.append((decision.dest_name, decision.dest_sql))
        elif decision.skipped:
            # Backs the PRIMARY KEY already emitted at CREATE — not carried
            # twice, and not a reason to decline.
            continue
        else:
            # A refused index means the destination would enforce a different
            # rule than the source. Decline rather than ship the difference.
            raise FastPathUnavailable(
                f"source index cannot be carried identically: {decision.reason}"
            )
    return carried


def _snapshot_evidence(cur: Any) -> dict[str, Any]:
    """Record which snapshot the read ran under, in the shared shape.

    Taking the WAL position is also what forces the transaction snapshot to be
    established here rather than at some later statement, so the evidence and
    the rows it describes cannot drift apart.
    """
    lsn = ""
    try:
        cur.execute("SELECT pg_current_wal_lsn()::text")
        row = cur.fetchone()
        lsn = str(row[0]) if row and row[0] else ""
    except Exception as exc:
        logger.debug("snapshot lsn unavailable: %s", exc)
    return {
        "engine": "postgresql",
        "isolation": "repeatable_read",
        "guarantee": "mvcc_repeatable_read",
        "snapshot_lsn": lsn,
        "export_snapshot": "",
        "note": (
            "Rows and the source digest were both read under this MVCC "
            "snapshot, so the digest describes exactly what was copied."
        ),
    }


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


def _norm_pg_host(host: str) -> str:
    h = (host or "").strip().lower()
    if h in {"localhost", "127.0.0.1", "::1", "0.0.0.0"}:
        return "127.0.0.1"
    return h


def postgres_same_relation(
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    source_schema: str,
    source_table: str,
    dest_schema: str,
    dest_table: str,
) -> bool:
    """True when COPY would read and write the same PostgreSQL table."""
    src_host = _norm_pg_host(str(src_cfg.get("host") or ""))
    dest_host = _norm_pg_host(str(dest_cfg.get("host") or ""))
    if src_host != dest_host:
        return False
    if int(src_cfg.get("port") or 5432) != int(dest_cfg.get("port") or 5432):
        return False
    src_db = str(src_cfg.get("database") or src_cfg.get("dbname") or "").lower()
    dest_db = str(dest_cfg.get("database") or dest_cfg.get("dbname") or "").lower()
    if src_db and dest_db and src_db != dest_db:
        return False
    return (
        (source_schema or "public").lower() == (dest_schema or "public").lower()
        and source_table.lower() == dest_table.lower()
    )


def _pg_dest_range_count(
    cur: Any, dest_ref: str, dest_ident: str, part: dict[str, Any]
) -> int:
    from services.copy_pg_mysql import _pg_quoted_literal, pk_range_predicate

    if part.get("null_shard"):
        pred = f"{dest_ident} IS NULL"
    else:
        lo_sql = (
            _pg_quoted_literal(cur, part["lo"]) if part.get("lo") is not None else None
        )
        hi_sql = (
            _pg_quoted_literal(cur, part["hi"]) if part.get("hi") is not None else None
        )
        pred = pk_range_predicate(dest_ident, lo_sql, hi_sql)
    if not pred:
        cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
    else:
        cur.execute(f"SELECT COUNT(*) FROM {dest_ref} WHERE {pred}")  # nosec B608
    return int(cur.fetchone()[0])


def _plan_pg_pk_partitions(
    src_cur: Any,
    source_ref: str,
    src_ident: str,
    pk_declared: str,
    n_parts: int,
    source_count: int,
) -> list[dict[str, Any]]:
    from services.copy_pg_mysql import (
        _INTEGER_PK_BASES,
        _pg_base,
        _pg_quoted_literal,
        fetch_integer_pk_cuts,
        fetch_pk_interior_cuts,
        key_ranges_from_cuts,
        pk_range_predicate,
    )

    if n_parts <= 1:
        key_ranges: list[tuple[Any | None, Any | None]] = [(None, None)]
    elif _pg_base(pk_declared) in _INTEGER_PK_BASES:
        cuts = fetch_integer_pk_cuts(src_cur, source_ref, src_ident, n_parts)
        key_ranges = key_ranges_from_cuts(cuts)
    else:
        cuts = fetch_pk_interior_cuts(src_cur, source_ref, src_ident, n_parts)
        key_ranges = key_ranges_from_cuts(cuts)
    src_cur.execute(
        f"SELECT COUNT(*) FROM {source_ref} WHERE {src_ident} IS NULL"  # nosec B608
    )
    nulls = int(src_cur.fetchone()[0])
    unbounded = len(key_ranges) == 1 and key_ranges[0] == (None, None)
    plan: list[tuple[str, Any, Any, bool]] = []
    if nulls and not unbounded:
        plan.append((f"{src_ident} IS NULL", None, None, True))
    for lo, hi in key_ranges:
        lo_sql = _pg_quoted_literal(src_cur, lo) if lo is not None else None
        hi_sql = _pg_quoted_literal(src_cur, hi) if hi is not None else None
        pred = pk_range_predicate(src_ident, lo_sql, hi_sql)
        plan.append((pred, lo, hi, False))
    partitions: list[dict[str, Any]] = []
    for pred, lo, hi, is_null in plan:
        if pred:
            src_cur.execute(
                f"SELECT COUNT(*) FROM {source_ref} WHERE {pred}"  # nosec B608
            )
        else:
            src_cur.execute(f"SELECT COUNT(*) FROM {source_ref}")  # nosec B608
        expected = int(src_cur.fetchone()[0])
        partitions.append({
            "lo": lo,
            "hi": hi,
            "null_shard": is_null,
            "source_count": expected,
            "predicate": pred,
            "action": "load",
        })
    accounted = sum(int(p["source_count"]) for p in partitions)
    if accounted != source_count:
        raise ValueError(
            f"PK range source COUNTs {accounted} != snapshot {source_count}"
        )
    return partitions


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
    source_where: str = "",
) -> FastPathResult:
    """Copy a population between two PostgreSQL tables and prove it arrived.

    ``pairs`` are ordered ``(source_column, target_column)``; the caller has
    already established that each pair shares a declared type. Renames are free
    because both sides project their own names in the same order.
    """
    if not pairs:
        raise FastPathUnavailable("no comparable columns")
    if postgres_same_relation(
        source_cfg, dest_cfg, source_schema, source_table, dest_schema, dest_table
    ):
        raise FastPathUnavailable("refusing COPY onto the same PostgreSQL table")
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
            # Dest WAL flush per COPY row is the 10M-row tax. LOCAL lasts this
            # transaction; commit still writes the catalog + heap, just not
            # fsync-per-record. Crash between commit and OS flush can lose the
            # dest table — the source snapshot is still the proof, and the
            # operator re-runs overwrite.
            dst_cur.execute("SET LOCAL synchronous_commit = off")
            snapshot = _snapshot_evidence(src_cur)
            blocked = unsupported_structure(src_cur, source_schema, source_table)
            if blocked:
                # Values without the structure that governs them is a different
                # table. Decline so the row path reproduces it properly.
                raise FastPathUnavailable(
                    f"source carries structure this path cannot: {blocked}"
                )
            shape = source_table_shape(
                src_cur, source_schema, source_table, source_cols
            )
            missing = [
                c for c in source_cols
                if c.lower() not in {k.lower() for k in shape.types}
            ]
            if missing:
                raise FastPathUnavailable(
                    f"source columns absent from catalog: {', '.join(missing)}"
                )
            # Decide index carry before the load so an index this path cannot
            # reproduce declines the whole route while the destination is still
            # empty, rather than after a full table has been copied.
            index_plan = plan_secondary_index_carry(
                src_cur,
                source_schema=source_schema,
                source_table=source_table,
                dest_schema=dest_schema,
                dest_table=dest_table,
                pairs=pairs,
            )
            if replace_destination:
                # DROP + CREATE, not CREATE IF NOT EXISTS + TRUNCATE: a stale
                # dest shell (wrong types, missing collation, leftover indexes)
                # would otherwise survive the load and still checksum-verify
                # over the mapped columns.
                try:
                    dst_cur.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
                except Exception as exc:  # noqa: BLE001 — DROP failure is a catalog/lock condition
                    raise FastPathUnavailable(
                        f"cannot drop destination {dest_ref} to replace it: {exc}"
                    ) from exc
                create = True
            else:
                dst_cur.execute(
                    "SELECT to_regclass(%s)",
                    (f"{dest_schema or 'public'}.{dest_table}",),
                )
                create = dst_cur.fetchone()[0] is None
            if create:
                try:
                    create_destination_like_source(
                        dst_cur, dest_schema, dest_table, pairs, shape
                    )
                except FastPathUnavailable:
                    raise
                except Exception as exc:  # noqa: BLE001 — CREATE failure is a catalog/lock condition
                    raise FastPathUnavailable(
                        f"cannot create destination like source: {exc}"
                    ) from exc

            dest_occupied = False
            if not create:
                dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
                dest_occupied = int(dst_cur.fetchone()[0]) > 0

            from services.copy_pg_mysql import (
                _jsonable_bound,
                mapped_single_pk,
                pg_mysql_copy_partitions,
                pg_mysql_copy_workers,
                pk_range_predicate,
            )

            pk_map = mapped_single_pk(list(shape.primary_key or []), pairs)
            cursor_where = (source_where or "").strip()
            if dest_occupied and pk_map is None and not cursor_where:
                raise FastPathUnavailable(
                    "append into non-empty PostgreSQL dest stays on the row path"
                )
            if cursor_where and dest_occupied:
                raise FastPathUnavailable(
                    "filtered COPY into occupied dest stays on the incremental staging path"
                )

            where_sql = f" WHERE {cursor_where}" if cursor_where else ""
            src_cur.execute(f"SELECT COUNT(*) FROM {source_ref}{where_sql}")  # nosec B608
            source_count = int(src_cur.fetchone()[0])
            workers = 1 if cursor_where else pg_mysql_copy_workers(source_count)
            n_parts = 1 if cursor_where else pg_mysql_copy_partitions(source_count, workers)
            partitions: list[dict[str, Any]] = []
            shard_mode = "serial"
            copy_split = "binary"
            predicates: list[str] = [""]
            skip_all = False

            if cursor_where:
                shard_mode = "cursor"
                copy_split = "cursor"
                predicates = [cursor_where]
                partitions = [{
                    "lo": None,
                    "hi": None,
                    "null_shard": False,
                    "source_count": source_count,
                    "predicate": cursor_where,
                    "action": "load",
                }]
            elif pk_map is not None:
                src_pk, dest_pk = pk_map
                src_ident = _quote(src_pk)
                dest_ident = _quote(dest_pk)
                shard_mode = "pk"
                pk_declared = shape.types.get(src_pk) or ""
                if not pk_declared:
                    live_l = {k.lower(): v for k, v in shape.types.items()}
                    pk_declared = live_l.get(src_pk.lower()) or ""
                partitions = _plan_pg_pk_partitions(
                    src_cur, source_ref, src_ident, pk_declared, n_parts, source_count
                )
                if dest_occupied:
                    copy_split = "pk"
                    for part in partitions:
                        already = _pg_dest_range_count(
                            dst_cur, dest_ref, dest_ident, part
                        )
                        expected = int(part["source_count"])
                        if already == expected:
                            part["action"] = "skip"
                            part["dest_count"] = already
                        elif already == 0:
                            part["action"] = "load"
                        else:
                            from services.copy_pg_mysql import _pg_quoted_literal

                            pred = pk_range_predicate(
                                dest_ident,
                                _pg_quoted_literal(dst_cur, part["lo"])
                                if part.get("lo") is not None
                                else None,
                                _pg_quoted_literal(dst_cur, part["hi"])
                                if part.get("hi") is not None
                                else None,
                                null_shard=bool(part.get("null_shard")),
                            )
                            if not pred:
                                raise FastPathUnavailable(
                                    "refusing unbounded dest DELETE on resume"
                                )
                            dst_cur.execute(
                                f"DELETE FROM {dest_ref} WHERE {pred}"  # nosec B608
                            )
                            part["action"] = "reload"
                    predicates = [
                        str(p.get("predicate") or "")
                        for p in partitions
                        if p.get("action") in {"load", "reload"}
                    ]
                    skip_all = not predicates
                else:
                    predicates = [""]

            need_checksum = not skip_all
            source_digest = None
            if need_checksum:
                source_digest = postgresql_engine_checksum(
                    src_cur, source_ref, source_cols, where=cursor_where
                )
                if source_digest is None:
                    raise FastPathUnavailable("source digest unavailable")
                if int(source_digest.row_count) != source_count:
                    raise ValueError(
                        "COPY fast path refused: digest count "
                        f"{source_digest.row_count} != snapshot {source_count}"
                    )

            dest_copy_sql = (
                f"COPY {dest_ref} ({target_list}) FROM STDIN (FORMAT binary)"  # nosec B608
            )
            copied_any = False
            for pred in predicates:
                where = f" WHERE {pred}" if pred else ""
                _stream_copy(
                    src_cur,
                    dst_cur,
                    f"COPY (SELECT {source_list} FROM {source_ref}{where}) "  # nosec B608
                    "TO STDOUT (FORMAT binary)",
                    dest_copy_sql,
                )
                copied_any = True
            if copied_any:
                driver_rows = int(dst_cur.rowcount or 0)
                if driver_rows > 0 and len(predicates) == 1 and not predicates[0]:
                    if driver_rows != source_count:
                        raise ValueError(
                            "COPY fast path refused: driver rowcount "
                            f"{driver_rows} disagrees with the source snapshot "
                            f"({source_count} rows)"
                        )

            rows_copied = source_count

            # Build indexes after the bulk load, not before: on an empty table
            # each COPYed row would pay index maintenance, and building once over
            # the finished population is both faster and how a bulk restore does
            # it. A UNIQUE index that the source satisfied cannot fail here on
            # rows copied verbatim; if one somehow did, the surrounding
            # transaction rolls the whole load back rather than half-applying it.
            indexes_carried: list[str] = []
            if create:
                for index_name, index_sql in index_plan:
                    dst_cur.execute(index_sql)  # nosec B608 — identifiers quoted by planner
                    indexes_carried.append(index_name)

            dest_digest = None
            if need_checksum:
                dest_digest = postgresql_engine_checksum(dst_cur, dest_ref, target_cols)
                if dest_digest is None:
                    raise FastPathUnavailable("destination digest unavailable")
            dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
            dest_count = int(dst_cur.fetchone()[0])
            if dest_count != source_count:
                raise ValueError(
                    "COPY fast path refused: dest COUNT(*) "
                    f"{dest_count} != source snapshot {source_count}"
                )
            if shard_mode == "pk" and pk_map is not None:
                dest_ident = _quote(pk_map[1])
                for part in partitions:
                    dest_part = _pg_dest_range_count(
                        dst_cur, dest_ref, dest_ident, part
                    )
                    part["dest_count"] = dest_part
                    if dest_part != int(part["source_count"]):
                        raise ValueError(
                            "PK range dest COUNT "
                            f"{dest_part} != source {part['source_count']} "
                            f"(lo={part['lo']!r} hi={part['hi']!r})"
                        )

        proof_count = f"dest_count:{dest_count}"
        source_checksum = (
            source_digest.checksum if source_digest is not None else proof_count
        )
        target_checksum = (
            dest_digest.checksum if dest_digest is not None else proof_count
        )
        partition_proof = [
            {
                "lo": _jsonable_bound(p.get("lo")),
                "hi": _jsonable_bound(p.get("hi")),
                "null_shard": bool(p.get("null_shard")),
                "source_count": int(p["source_count"]),
                "dest_count": int(p.get("dest_count") or 0),
                "action": str(p.get("action") or "load"),
            }
            for p in partitions
        ]
        snapshot["copy_split"] = copy_split
        snapshot["shard_mode"] = shard_mode if partitions else "serial"
        snapshot["copy_workers"] = 1
        snapshot["copy_partitions"] = len(partitions) or 1
        snapshot["partitions_skipped"] = sum(
            1 for p in partitions if p.get("action") == "skip"
        )
        snapshot["partition_proof"] = partition_proof
        proof_scope = (
            "partition_dest_count_equals_source_snapshot"
            if skip_all
            else (
                "mapped_column_checksum_and_dest_count_equals_source_snapshot"
                if not partition_proof
                else "mapped_column_checksum_and_partition_dest_count"
            )
        )
        result = FastPathResult(
            rows_copied=rows_copied,
            source_rows=source_count,
            source_checksum=source_checksum,
            target_rows=dest_count,
            target_checksum=target_checksum,
            source_snapshot=snapshot,
            indexes_carried=tuple(indexes_carried),
            proof_scope=proof_scope,
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
