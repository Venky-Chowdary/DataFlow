"""PostgreSQL logical decoding CDC reader.

Production path uses ``pg_logical_slot_peek_changes`` + ``ack()`` via
``pg_replication_slot_advance`` so WAL is never consumed before destination
apply (at-least-once). Default plugin is ``test_decoding`` (text). ``pgoutput``
is a real binary path via :mod:`connectors.pgoutput_decoder` when selected.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

from connectors.postgresql_conn import get_connection
from services.cdc_engine import ChangeBatch
from services.cdc_schema_history import (
    connection_fingerprint,
    last_ddl_at,
    rebuild_schema,
    record_ddl,
)

_logger = logging.getLogger(__name__)

# test_decoding value rendering uses type suffixes like [text]:'value' or [int4]:1.
# The first colon separates column info from value; the value may itself contain colons.
_VALUE_RE = re.compile(r"^\s*(\w+)\[(\w+)\]:(.+)$")
_OLD_KEY_PREFIX = "old-key:"
_NEW_TUPLE_PREFIX = "new-tuple:"


def _publication_name(database: str, table: str, cursor_key: str) -> str:
    """Stable publication name for pgoutput (must match slot scoping)."""
    digest = hashlib.sha1(  # noqa: S324 — non-crypto publication name digest
        f"{database}|{table}|{cursor_key}".encode("utf-8"),
        usedforsecurity=False,
    ).hexdigest()[:10]
    raw = f"df_pub_{database}_{table}_{digest}".lower()
    return re.sub(r"[^a-z0-9_]", "_", raw)[:63]


def _slot_name(database: str, table: str, cursor_key: str) -> str:
    token = hashlib.sha256(cursor_key.encode()).hexdigest()[:8]
    raw = f"df_{database}_{table}_{token}".lower()
    return re.sub(r"[^a-z0-9_]", "_", raw)[:63]


def encode_pg_resume_token(
    slot: str,
    *,
    lsn: str | None = None,
    phase: str = "streaming",
) -> str:
    """Compact watermark: slot + optional consistent-point LSN + phase.

    Phase is ``snapshot`` while the initial table dump is in progress and
    ``streaming`` once the dump finishes and logical decoding owns the cursor.
    Legacy bare slot names remain valid inputs via :func:`decode_pg_resume_token`.
    """
    parts = [f"slot={slot}", f"phase={phase}"]
    if lsn:
        parts.append(f"lsn={lsn}")
    return "|".join(parts)


def decode_pg_resume_token(
    token: str | None,
    *,
    database: str,
    table: str,
    cursor_key: str,
) -> tuple[str, str | None, str]:
    """Return ``(slot_name, lsn_or_none, phase)`` from a watermark or legacy slot."""
    default_slot = _slot_name(database, table, cursor_key)
    if not token:
        return default_slot, None, "initial"
    raw = str(token).strip()
    if not raw:
        return default_slot, None, "initial"
    if "=" not in raw and "|" not in raw:
        # Legacy: watermark was the bare replication slot name.
        return raw[:63], None, "streaming"
    slot = default_slot
    lsn: str | None = None
    phase = "streaming"
    for part in raw.split("|"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip().lower()
        value = value.strip()
        if key == "slot" and value:
            slot = value[:63]
        elif key == "lsn" and value:
            lsn = value
        elif key == "phase" and value:
            phase = value
    return slot, lsn, phase


def _parse_value(raw: str) -> str:
    """Strip PostgreSQL test_decoding quotes and null markers."""
    if raw == "null" or raw == "None":
        return ""
    if len(raw) >= 2 and raw.startswith("'") and raw.endswith("'"):
        return raw[1:-1].replace("''", "'")
    return raw


def _parse_columns(payload: str) -> dict[str, str]:
    """Parse a space-separated list of ``col[type]:value`` tokens."""
    result: dict[str, str] = {}
    if not payload:
        return result

    # test_decoding separates tokens by spaces, but string values may contain spaces.
    # It quotes values with spaces, e.g. data[text]:'hello world'. Split on the
    # pattern `` col[type]:`` while respecting quoted segments.
    tokens: list[str] = []
    current = ""
    in_quote = False
    for char in payload:
        if char == "'":
            in_quote = not in_quote
            current += char
        elif char == " " and not in_quote:
            if current.strip():
                tokens.append(current.strip())
            current = ""
        else:
            current += char
    if current.strip():
        tokens.append(current.strip())

    for token in tokens:
        match = _VALUE_RE.match(token)
        if not match:
            continue
        col, _dtype, raw = match.group(1), match.group(2), match.group(3)
        result[col] = _parse_value(raw)
    return result


def _parse_change_line(line: str, schema: str, table: str) -> tuple[str, dict[str, str] | None, dict[str, str] | None] | None:
    """Return (operation, old_key_dict_or_none, new_tuple_dict_or_none) for a line."""
    prefix = f"table {schema}.{table}:"
    if not line.startswith(prefix):
        return None
    rest = line[len(prefix):].strip()

    if rest.startswith("INSERT:"):
        payload = rest[len("INSERT:"):].strip()
        return "insert", None, _parse_columns(payload)

    if rest.startswith("UPDATE:"):
        payload = rest[len("UPDATE:"):].strip()
        old_key: dict[str, str] | None = None
        new_tuple: dict[str, str] | None = None
        if payload.startswith(_OLD_KEY_PREFIX):
            payload = payload[len(_OLD_KEY_PREFIX):].strip()
            split_at = payload.find(_NEW_TUPLE_PREFIX)
            if split_at >= 0:
                old_key = _parse_columns(payload[:split_at].strip())
                new_tuple = _parse_columns(payload[split_at + len(_NEW_TUPLE_PREFIX):].strip())
            else:
                old_key = _parse_columns(payload)
        else:
            new_tuple = _parse_columns(payload)
        return "update", old_key, new_tuple

    if rest.startswith("DELETE:"):
        payload = rest[len("DELETE:"):].strip()
        return "delete", _parse_columns(payload), None

    return None


class PostgreSqlChangeStreamCdc:
    """Log-based CDC for PostgreSQL using logical decoding.

    Prefers the ``pgoutput`` plugin when the server can create that slot;
    falls back to ``test_decoding`` (text parse path). Query CDC remains the
    outer fallback in ``cdc_transfer``.
    """

    def __init__(
        self,
        cfg: dict[str, Any],
        table: str,
        primary_key: str,
        cursor_key: str,
        schema: str = "public",
        columns: list[str] | None = None,
        resume_token: str | None = None,
        batch_size: int = 1000,
        output_plugin: str | None = None,
    ) -> None:
        self.cfg = cfg
        self.schema = schema
        self.table = table
        self.primary_key = primary_key
        self.cursor_key = cursor_key
        self.columns = columns
        self.batch_size = batch_size
        self.database = cfg.get("database") or "postgres"
        slot, lsn, phase = decode_pg_resume_token(
            resume_token,
            database=self.database,
            table=table,
            cursor_key=cursor_key,
        )
        self.slot_name = slot
        self.consistent_point_lsn = lsn
        self.phase = phase if phase != "initial" else "snapshot"
        self.output_plugin = output_plugin or self._select_plugin()
        self.source_key = connection_fingerprint(
            {**cfg, "type": "postgresql"},
            connector_id=str(cfg.get("connector_id") or ""),
        )
        self.decode_schema: dict[str, Any] = {}
        self.last_ddl_at: str | None = None
        self._last_event_at: datetime | None = None
        self._last_heartbeat_at: datetime | None = None
        self._schema_ready = False
        self._pending_ack_lsn: str | None = None
        self._pgoutput_decoder = None
        self.publication_name = _publication_name(self.database, table, cursor_key)

    def _select_plugin(self) -> str:
        """Select logical decoding plugin.

        ``test_decoding`` is the default text path. ``pgoutput`` is used when
        ``logical_decoding_plugin=pgoutput`` or ``DATAFLOW_PGOUTPUT_DECODER`` is
        enabled — decoded via :mod:`connectors.pgoutput_decoder` (binary, real).
        """
        preferred = (self.cfg.get("logical_decoding_plugin") or "").strip().lower()
        env_flag = str(
            self.cfg.get("pgoutput_decoder") or os.getenv("DATAFLOW_PGOUTPUT_DECODER", "")
        ).strip().lower()
        if preferred == "pgoutput" or env_flag in {"1", "true", "experimental", "on", "pgoutput"}:
            return "pgoutput"
        if preferred == "test_decoding":
            return preferred
        return "test_decoding"

    def _conn(self):
        return get_connection(
            host=self.cfg.get("host") or "localhost",
            port=self.cfg.get("port") or 5432,
            database=self.database,
            username=self.cfg.get("username") or "",
            password=self.cfg.get("password") or "",
            connection_string=self.cfg.get("connection_string") or "",
            ssl=bool(self.cfg.get("ssl")),
        )

    def is_available(self) -> bool:
        """Check logical replication is enabled and the user can create a slot."""
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SHOW wal_level")
                    row = cur.fetchone()
                    if not row or row[0] != "logical":
                        return False
                    cur.execute(
                        "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                        (self.slot_name,),
                    )
                    exists = cur.fetchone() is not None
                    if exists:
                        return True
                    test_slot = f"{self.slot_name}_avail_test"[:63]
                    plugin = self.output_plugin or "test_decoding"
                    cur.execute(
                        "SELECT pg_create_logical_replication_slot(%s, %s)",
                        (test_slot, plugin),
                    )
                    cur.execute("SELECT pg_drop_replication_slot(%s)", (test_slot,))
                conn.commit()
            return True
        except Exception:
            return False

    def _resume_token(self, *, phase: str | None = None) -> str:
        return encode_pg_resume_token(
            self.slot_name,
            lsn=self.consistent_point_lsn,
            phase=phase or self.phase,
        )

    def _read_slot_lsn(self, cur) -> str | None:
        """Return confirmed_flush_lsn or restart_lsn for this slot."""
        cur.execute(
            """
            SELECT confirmed_flush_lsn::text, restart_lsn::text
            FROM pg_replication_slots
            WHERE slot_name = %s
            """,
            (self.slot_name,),
        )
        row = cur.fetchone()
        if not row:
            return None
        value = row[0] or row[1]
        return str(value) if value else None

    def _ensure_slot(self) -> str | None:
        """Create the logical slot if needed; return consistent-point LSN.

        Slot is created *before* the initial snapshot so WAL from the snapshot
        window is retained (Debezium / PG logical-decoding handoff pattern).
        Semantics remain at-least-once; destination upserts must be idempotent.
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT plugin FROM pg_replication_slots WHERE slot_name = %s",
                    (self.slot_name,),
                )
                row = cur.fetchone()
                if row is None:
                    cur.execute(
                        "SELECT lsn::text FROM pg_create_logical_replication_slot(%s, %s)",
                        (self.slot_name, self.output_plugin),
                    )
                    created = cur.fetchone()
                    lsn = str(created[0]) if created and created[0] else None
                else:
                    # Honor existing slot plugin (cannot change without drop).
                    self.output_plugin = row[0] or self.output_plugin
                    lsn = self._read_slot_lsn(cur)
            conn.commit()
        if lsn and not self.consistent_point_lsn:
            self.consistent_point_lsn = lsn
        return self.consistent_point_lsn

    def replication_lag_bytes(self) -> int | None:
        """Return WAL lag for this slot, or None if unavailable."""
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
                        FROM pg_replication_slots
                        WHERE slot_name = %s
                        """,
                        (self.slot_name,),
                    )
                    row = cur.fetchone()
                    if row and row[0] is not None:
                        return int(row[0])
        except Exception:
            return None
        return None

    def replication_lag_seconds(self) -> float | None:
        """Seconds since the last decoded event / heartbeat, when known."""
        anchor = self._last_event_at or self._last_heartbeat_at
        if anchor is None:
            return None
        return max(0.0, (datetime.now(timezone.utc) - anchor).total_seconds())

    def heartbeat(self) -> None:
        """Record a poll heartbeat for lag SLO measurement."""
        self._last_heartbeat_at = datetime.now(timezone.utc)

    def _qualified_table(self) -> str:
        return f"{self.schema}.{self.table}"

    def _fetch_live_schema(self) -> dict[str, Any]:
        """Load column types / nullability / PK from information_schema."""
        columns: dict[str, str] = {}
        nullable: dict[str, bool] = {}
        primary_key: list[str] = []
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                        ORDER BY ordinal_position
                        """,
                        (self.schema, self.table),
                    )
                    for name, data_type, is_nullable in cur.fetchall():
                        columns[str(name)] = str(data_type or "text")
                        nullable[str(name)] = str(is_nullable or "").upper() == "YES"
                    cur.execute(
                        """
                        SELECT kcu.column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                          ON tc.constraint_name = kcu.constraint_name
                         AND tc.table_schema = kcu.table_schema
                        WHERE tc.constraint_type = 'PRIMARY KEY'
                          AND tc.table_schema = %s AND tc.table_name = %s
                        ORDER BY kcu.ordinal_position
                        """,
                        (self.schema, self.table),
                    )
                    primary_key = [str(r[0]) for r in cur.fetchall()]
        except Exception:
            _logger.debug("PostgreSQL live schema fetch failed", exc_info=True)
        return {"columns": columns, "nullable": nullable, "primary_key": primary_key}

    def _schema_fingerprint(self, snapshot: dict[str, Any]) -> str:
        cols = snapshot.get("columns") or {}
        nulls = snapshot.get("nullable") or {}
        pk = snapshot.get("primary_key") or []
        parts = [f"{k}:{cols[k]}:{int(bool(nulls.get(k, True)))}" for k in sorted(cols)]
        parts.append("pk=" + ",".join(pk))
        return "|".join(parts)

    def _ensure_decode_schema(self, *, resume_offset: Any = None) -> dict[str, Any]:
        """Rebuild decode schema from history (or seed from live catalog)."""
        if self._schema_ready and self.decode_schema:
            return self.decode_schema

        rebuilt = rebuild_schema(self.source_key, self._qualified_table(), resume_offset)
        if rebuilt:
            self.decode_schema = rebuilt
        else:
            live = self._fetch_live_schema()
            if live.get("columns"):
                record_ddl(
                    self.source_key,
                    self._qualified_table(),
                    ddl="SNAPSHOT",
                    offset=resume_offset or self.slot_name,
                    schema_snapshot=live,
                )
                self.decode_schema = live
        self.last_ddl_at = last_ddl_at(self.source_key, self._qualified_table())
        self._schema_ready = True
        return self.decode_schema

    def _maybe_record_schema_change(self, *, offset: Any = None) -> None:
        """Compare live catalog to decode schema; persist DDL history on drift."""
        live = self._fetch_live_schema()
        if not live.get("columns"):
            return
        if self._schema_fingerprint(live) == self._schema_fingerprint(self.decode_schema):
            return
        entry = record_ddl(
            self.source_key,
            self._qualified_table(),
            ddl="ALTER TABLE (detected)",
            offset=offset or self.slot_name,
            schema_snapshot=live,
        )
        self.decode_schema = live
        self.last_ddl_at = str(entry.get("recorded_at") or "") or self.last_ddl_at
        _logger.info(
            "Recorded PostgreSQL CDC schema change for %s v%s",
            self._qualified_table(),
            entry.get("version"),
        )

    def snapshot(self) -> Iterator[ChangeBatch]:
        """Initial dump after slot create, then hand off to streaming at the LSN.

        Order matches industry CDC practice:
        1. Create logical slot (consistent point LSN reserved).
        2. Read the table under ``REPEATABLE READ`` on one connection so the
           dump is a single MVCC snapshot (not N independent page reads).
        3. Persist ``phase=streaming`` + LSN so poll resumes without a gap window
           outside the slot (duplicates during the dump are possible; upserts OK).
        """
        from psycopg2 import sql

        from connectors.postgresql_reader import _cell, _order_by_clause

        self._ensure_slot()
        self.phase = "snapshot"
        self._ensure_decode_schema(resume_offset=self.slot_name)
        self.heartbeat()

        with self._conn() as conn:
            # One RR transaction = one consistent table view for the dump.
            prev_autocommit = getattr(conn, "autocommit", True)
            try:
                conn.autocommit = False
                with conn.cursor() as cur:
                    cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    cur.execute("SELECT pg_current_wal_lsn()::text")
                    snap_lsn_row = cur.fetchone()
                    if snap_lsn_row and snap_lsn_row[0]:
                        # Prefer the dump's WAL position when newer than slot create.
                        self.consistent_point_lsn = str(snap_lsn_row[0])
                    order_by = _order_by_clause(cur, self.schema, self.table, self.columns)
                    if self.columns:
                        col_sql = sql.SQL(", ").join(map(sql.Identifier, self.columns))
                        query = sql.SQL(
                            "SELECT {} FROM {}.{} ORDER BY " + order_by + " LIMIT %s OFFSET %s"
                        ).format(
                            col_sql,
                            sql.Identifier(self.schema),
                            sql.Identifier(self.table),
                        )
                    else:
                        query = sql.SQL(
                            "SELECT * FROM {}.{} ORDER BY " + order_by + " LIMIT %s OFFSET %s"
                        ).format(
                            sql.Identifier(self.schema),
                            sql.Identifier(self.table),
                        )
                    offset = 0
                    headers: list[str] = list(self.columns or [])
                    while True:
                        cur.execute(query, (self.batch_size, offset))
                        fetched = cur.fetchall()
                        if not fetched:
                            break
                        if cur.description:
                            headers = [desc[0] for desc in cur.description]
                        records = [
                            {headers[i]: _cell(v) for i, v in enumerate(row)}
                            for row in fetched
                        ]
                        yield ChangeBatch(
                            inserts=records,
                            resume_token=self._resume_token(phase="snapshot"),
                        )
                        offset += len(fetched)
                        if len(fetched) < self.batch_size:
                            break
                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise
            finally:
                try:
                    conn.autocommit = prev_autocommit
                except Exception:
                    pass

        self.phase = "streaming"
        yield ChangeBatch(resume_token=self._resume_token(phase="streaming"))

    def _pk_value(self, record: dict[str, str]) -> str:
        return record.get(self.primary_key, "") if record else ""

    def _qualified_table(self) -> str:
        from connectors.sql_identifiers import quote_table_ref

        return quote_table_ref(self.table, self.schema or "public", dialect="postgresql")

    def _ensure_publication(self) -> None:
        """Create a FOR TABLE publication required by the pgoutput plugin."""
        if self.output_plugin != "pgoutput":
            return
        from connectors.sql_identifiers import require_safe_identifier

        qualified = self._qualified_table()
        # Publication names are [a-z0-9_]; reject anything that escaped hashing.
        pub = require_safe_identifier(self.publication_name, preserve_case=False)
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_publication WHERE pubname = %s",
                    (pub,),
                )
                if cur.fetchone() is None:
                    cur.execute(
                        f"CREATE PUBLICATION {pub} FOR TABLE {qualified}"
                    )
                else:
                    # Ensure the table is in the publication (idempotent best-effort).
                    try:
                        cur.execute(
                            f"ALTER PUBLICATION {pub} ADD TABLE {qualified}"
                        )
                    except Exception:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
            conn.commit()

    def _ensure_replica_identity(self) -> None:
        """Require FULL replica identity so UPDATE/DELETE emit old keys."""
        qualified = self._qualified_table()
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"ALTER TABLE {qualified} REPLICA IDENTITY FULL")
                conn.commit()
        except Exception as exc:
            _logger.debug("Could not set REPLICA IDENTITY FULL on %s: %s", qualified, exc)

    def ack(self, resume_token: Any = None) -> None:
        """Advance the slot confirmed_flush_lsn after successful destination apply.

        Poll uses ``peek_changes`` so WAL is not consumed before apply. Without
        ``ack``, the slot retains WAL and re-delivers (at-least-once). Calling
        ``ack`` after watermark persist makes progress durable.
        """
        lsn = self._pending_ack_lsn
        if resume_token:
            _, token_lsn, _ = decode_pg_resume_token(
                str(resume_token),
                database=self.database,
                table=self.table,
                cursor_key=self.cursor_key,
            )
            if token_lsn:
                lsn = token_lsn
        if not lsn:
            return
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_replication_slot_advance(%s, %s::pg_lsn)",
                    (self.slot_name, lsn),
                )
            conn.commit()
        self.consistent_point_lsn = lsn
        self._pending_ack_lsn = None

    def _fetch_incremental_chunk(self, sig: Any) -> tuple[list[dict[str, Any]], str | None, bool]:
        """PK-ordered chunk reader for Debezium-style incremental snapshots."""
        from connectors.sql_identifiers import quote_sql_identifier, require_safe_identifier

        pk = quote_sql_identifier(
            require_safe_identifier(sig.primary_key or self.primary_key, preserve_case=True)
        )
        qualified = self._qualified_table()
        limit = int(sig.chunk_size or self.batch_size)
        last_pk = sig.last_pk or ""
        with self._conn() as conn:
            with conn.cursor() as cur:
                if last_pk:
                    cur.execute(
                        f"SELECT * FROM {qualified} WHERE {pk} > %s ORDER BY {pk} LIMIT %s",
                        (last_pk, limit),
                    )
                else:
                    cur.execute(
                        f"SELECT * FROM {qualified} ORDER BY {pk} LIMIT %s",
                        (limit,),
                    )
                cols = [d[0] for d in (cur.description or [])]
                rows = cur.fetchall() or []
            conn.commit()
        records = [
            {cols[i]: "" if row[i] is None else str(row[i]) for i in range(len(cols))}
            for row in rows
        ]
        new_last = records[-1].get(sig.primary_key or self.primary_key) if records else last_pk
        done = len(records) < limit
        return records, str(new_last) if new_last is not None else last_pk, done

    def poll(self) -> Iterator[ChangeBatch]:
        """Peek WAL with txn buffering (Debezium-class) + incremental snapshots.

        At-least-once: caller must ``ack`` after destination apply.
        """
        from services.cdc_incremental_runner import interleave_incremental_snapshot
        from services.cdc_transaction_buffer import TransactionBuffer

        # 1) Interleave incremental snapshot chunks (signal-driven).
        yield from interleave_incremental_snapshot(
            self.source_key,
            table=self.table,
            fetch_chunk=self._fetch_incremental_chunk,
            max_chunks_per_poll=1,
        )

        self._ensure_slot()
        if self.output_plugin == "pgoutput":
            self._ensure_publication()
            self._ensure_replica_identity()
        self.phase = "streaming"
        self._ensure_decode_schema(resume_offset=self.slot_name)
        self._maybe_record_schema_change(offset=self.slot_name)
        self.heartbeat()

        with self._conn() as conn:
            with conn.cursor() as cur:
                if self.output_plugin == "pgoutput":
                    cur.execute(
                        """
                        SELECT location::text, data
                        FROM pg_logical_slot_peek_changes(
                            %s, NULL, %s,
                            'proto_version', '1',
                            'publication_names', %s
                        )
                        """,
                        (self.slot_name, self.batch_size, self.publication_name),
                    )
                else:
                    # include-xids=1 so BEGIN/COMMIT carry xid for txn buffering
                    cur.execute(
                        """
                        SELECT location::text, data
                        FROM pg_logical_slot_peek_changes(
                            %s, NULL, %s, 'include-xids', '1'
                        )
                        """,
                        (self.slot_name, self.batch_size),
                    )
                rows = cur.fetchall()
            conn.commit()

        buf = TransactionBuffer()
        max_lsn: str | None = None
        emitted = False

        def _token_at(lsn: str | None) -> str:
            if lsn:
                self.consistent_point_lsn = lsn
                self._pending_ack_lsn = lsn
            return self._resume_token(phase="streaming")

        if self.output_plugin == "pgoutput":
            from connectors.pgoutput_decoder import PgOutputDecoder, changes_for_table

            if self._pgoutput_decoder is None:
                self._pgoutput_decoder = PgOutputDecoder()
            decoder = self._pgoutput_decoder
            for location, payload in rows:
                lsn = str(location) if location else None
                if lsn:
                    max_lsn = lsn
                for change in changes_for_table(
                    decoder, payload, schema=self.schema, table=self.table
                ):
                    self._last_event_at = datetime.now(timezone.utc)
                    if change.op == "begin":
                        buf.begin(change.xid, lsn=lsn)
                    elif change.op == "commit":
                        batch = buf.commit(lsn=lsn, resume_token=_token_at(lsn))
                        if batch is not None:
                            emitted = True
                            yield batch
                    elif change.op == "insert" and change.new_tuple:
                        buf.insert(change.new_tuple, lsn=lsn)
                    elif change.op == "update" and change.new_tuple:
                        buf.update(change.new_tuple, lsn=lsn)
                    elif change.op == "delete" and change.old_tuple:
                        pk = self._pk_value(change.old_tuple)
                        if pk:
                            buf.delete(pk, lsn=lsn)
        else:
            for location, line in rows:
                lsn = str(location) if location else None
                if lsn:
                    max_lsn = lsn
                text = (
                    line.decode("utf-8", errors="replace")
                    if isinstance(line, (bytes, memoryview))
                    else (line or "")
                )
                upper = text.upper().strip()
                if upper.startswith("BEGIN"):
                    parts = text.split()
                    xid = parts[1] if len(parts) > 1 else None
                    buf.begin(xid, lsn=lsn)
                    continue
                if upper.startswith("COMMIT"):
                    batch = buf.commit(lsn=lsn, resume_token=_token_at(lsn))
                    if batch is not None:
                        emitted = True
                        yield batch
                    continue
                if upper.startswith("ROLLBACK") or upper.startswith("ABORT"):
                    buf.rollback()
                    continue
                if "ALTER TABLE" in upper or ": DDL:" in upper:
                    self._maybe_record_schema_change(offset=self.slot_name)
                    continue
                parsed = _parse_change_line(text, self.schema, self.table)
                if parsed is None:
                    continue
                op, old_key, new_tuple = parsed
                self._last_event_at = datetime.now(timezone.utc)
                if op == "insert" and new_tuple:
                    buf.insert(new_tuple, lsn=lsn)
                elif op == "update" and new_tuple:
                    buf.update(new_tuple, lsn=lsn)
                elif op == "delete" and old_key:
                    pk = self._pk_value(old_key)
                    if pk:
                        buf.delete(pk, lsn=lsn)

        # Peek window may end mid-transaction — flush deferred DML so progress moves.
        if buf.open_xid is not None:
            batch = buf.flush_open(resume_token=_token_at(max_lsn))
            if batch is not None:
                emitted = True
                yield batch
        elif max_lsn:
            self.consistent_point_lsn = max_lsn
            self._pending_ack_lsn = max_lsn
            if not emitted:
                yield ChangeBatch(resume_token=self._resume_token(phase="streaming"))
        elif self.consistent_point_lsn and not emitted:
            yield ChangeBatch(resume_token=self._resume_token(phase="streaming"))
