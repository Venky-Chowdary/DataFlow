"""PostgreSQL logical decoding CDC reader.

Production path uses ``pg_logical_slot_peek_changes`` + ``ack()`` via
``pg_replication_slot_advance`` so WAL is never consumed before destination
apply (at-least-once). Default plugin is ``pgoutput`` (Debezium-class binary
via :mod:`connectors.pgoutput_decoder`); falls back to ``test_decoding``.
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


def _publication_name(database: str, table: str | list[str], cursor_key: str) -> str:
    """Stable publication name for pgoutput (must match slot scoping)."""
    if isinstance(table, (list, tuple)):
        from services.cdc_multi_table import tables_digest

        tbl = f"mt_{tables_digest(list(table))}"
    else:
        tbl = str(table)
    digest = hashlib.sha1(  # noqa: S324 — non-crypto publication name digest
        f"{database}|{tbl}|{cursor_key}".encode("utf-8"),
        usedforsecurity=False,
    ).hexdigest()[:10]
    raw = f"df_pub_{database}_{tbl}_{digest}".lower()
    return re.sub(r"[^a-z0-9_]", "_", raw)[:63]


def _slot_name(database: str, table: str | list[str], cursor_key: str) -> str:
    if isinstance(table, (list, tuple)):
        from services.cdc_multi_table import tables_digest

        tbl = f"mt_{tables_digest(list(table))}"
    else:
        tbl = str(table)
    token = hashlib.sha256(cursor_key.encode()).hexdigest()[:8]
    raw = f"df_{database}_{tbl}_{token}".lower()
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
    table: str | list[str],
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

    Defaults to ``pgoutput`` (Debezium 2.x/3.x industry default). Opt out with
    ``logical_decoding_plugin=test_decoding`` or ``DATAFLOW_PGOUTPUT_DECODER=0``.
    Falls back to ``test_decoding`` when pgoutput slot creation fails.
    Query CDC remains the outer fallback in ``cdc_transfer``.
    """

    def __init__(
        self,
        cfg: dict[str, Any],
        table: str | list[str],
        primary_key: str,
        cursor_key: str,
        schema: str = "public",
        columns: list[str] | None = None,
        resume_token: str | None = None,
        batch_size: int = 1000,
        output_plugin: str | None = None,
        primary_keys: dict[str, str] | None = None,
    ) -> None:
        from services.cdc_multi_table import normalize_table_list

        self.cfg = cfg
        self.schema = schema
        self.tables = normalize_table_list(table)
        if not self.tables:
            raise ValueError("PostgreSQL CDC requires at least one table")
        self.table = self.tables[0]
        self.primary_keys = {
            t: str((primary_keys or {}).get(t) or primary_key or "id")
            for t in self.tables
        }
        self.primary_key = self.primary_keys[self.table]
        self.cursor_key = cursor_key
        self.columns = columns
        self.batch_size = batch_size
        self.database = cfg.get("database") or "postgres"
        slot_table: str | list[str] = self.tables if len(self.tables) > 1 else self.table
        slot, lsn, phase = decode_pg_resume_token(
            resume_token,
            database=self.database,
            table=slot_table,
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
        self.publication_name = _publication_name(self.database, slot_table, cursor_key)
        self._processed_signal_ids: set[str] = set()
        self.signal_table = str(cfg.get("signal_table") or "dataflow_signal")
        self._signal_table_ready = False
        self._last_signal_poll_at = 0.0
        self._signal_poll_interval_sec = float(
            os.getenv("DATAFLOW_CDC_SIGNAL_POLL_SEC", cfg.get("signal_poll_interval_sec") or 15)
        )
        from services.cdc_lease import CdcLeaseGuard

        holder = str(
            cfg.get("lease_holder_id") or os.getenv("DATAFLOW_CDC_LEASE_HOLDER") or ""
        )
        self._lease = CdcLeaseGuard(
            cursor_key=cursor_key,
            resource=f"pg_slot:{self.slot_name}",
            holder_id=holder,
            job_id=str(cfg.get("job_id") or ""),
            meta={
                "plugin": self.output_plugin,
                "tables": list(self.tables),
                "engine": "postgresql",
                "shared_reader": len(self.tables) > 1,
            },
        )

    @property
    def lease_holder_id(self) -> str:
        return self._lease.holder_id

    @lease_holder_id.setter
    def lease_holder_id(self, value: str) -> None:
        self._lease.holder_id = value

    @property
    def _lease_acquired(self) -> bool:
        return self._lease.acquired

    def _acquire_cdc_lease(self) -> None:
        """Fail-fast if another worker already owns this slot / cursor_key."""
        self._lease.ensure()

    def close(self) -> None:
        """Release the CDC lease so another worker can attach."""
        self._lease.release()

    def _select_plugin(self) -> str:
        """Select logical decoding plugin.

        Default is ``pgoutput`` (binary, Debezium-class). Opt out via
        ``logical_decoding_plugin=test_decoding`` or env
        ``DATAFLOW_PGOUTPUT_DECODER=0|false|off|test_decoding``.
        """
        preferred = (self.cfg.get("logical_decoding_plugin") or "").strip().lower()
        env_flag = str(
            self.cfg.get("pgoutput_decoder") or os.getenv("DATAFLOW_PGOUTPUT_DECODER", "")
        ).strip().lower()
        if preferred == "test_decoding" or env_flag in {"0", "false", "off", "test_decoding"}:
            return "test_decoding"
        if preferred in {"pgoutput", ""} or env_flag in {
            "",
            "1",
            "true",
            "on",
            "pgoutput",
            "experimental",
        }:
            return "pgoutput"
        return preferred or "pgoutput"

    def cdc_metadata(self) -> dict[str, Any]:
        """Operator-visible CDC status for Job Theater / Validate."""
        return {
            "plugin": self.output_plugin,
            "slot_name": self.slot_name,
            "publication_name": self.publication_name if self.output_plugin == "pgoutput" else None,
            "phase": self.phase,
            "consistent_point_lsn": self.consistent_point_lsn,
            "replication_lag_bytes": self.replication_lag_bytes(),
            "replication_lag_seconds": self.replication_lag_seconds(),
            "delivery": "at-least-once",
            **self._lease.theater_fields(),
        }

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
                    plugin = self.output_plugin or "pgoutput"
                    try:
                        cur.execute(
                            "SELECT pg_create_logical_replication_slot(%s, %s)",
                            (test_slot, plugin),
                        )
                        cur.execute("SELECT pg_drop_replication_slot(%s)", (test_slot,))
                    except Exception:
                        if plugin == "pgoutput":
                            # Fall back to test_decoding for availability probe.
                            self.output_plugin = "test_decoding"
                            cur.execute(
                                "SELECT pg_create_logical_replication_slot(%s, %s)",
                                (test_slot, "test_decoding"),
                            )
                            cur.execute("SELECT pg_drop_replication_slot(%s)", (test_slot,))
                        else:
                            raise
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
        self._acquire_cdc_lease()
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT plugin FROM pg_replication_slots WHERE slot_name = %s",
                    (self.slot_name,),
                )
                row = cur.fetchone()
                if row is None:
                    try:
                        cur.execute(
                            "SELECT lsn::text FROM pg_create_logical_replication_slot(%s, %s)",
                            (self.slot_name, self.output_plugin),
                        )
                    except Exception:
                        if self.output_plugin == "pgoutput":
                            self.output_plugin = "test_decoding"
                            cur.execute(
                                "SELECT lsn::text FROM pg_create_logical_replication_slot(%s, %s)",
                                (self.slot_name, "test_decoding"),
                            )
                        else:
                            raise
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
        """Record poll heartbeat and emit WAL so idle slots can advance (Debezium-class).

        Rate-limited: under many concurrent CDC jobs, emitting on every poll
        balloons WAL/slot retention. Only emit when idle (no pending ack) and
        the prior heartbeat is older than ``DATAFLOW_CDC_HEARTBEAT_SEC``.
        """
        now = datetime.now(timezone.utc)
        interval = float(os.getenv("DATAFLOW_CDC_HEARTBEAT_SEC", "10"))
        if self._last_heartbeat_at is not None:
            age = (now - self._last_heartbeat_at).total_seconds()
            if age < max(1.0, interval):
                return
        self._last_heartbeat_at = now
        if self._pending_ack_lsn:
            return
        if self._lease.acquired:
            try:
                self._lease.renew()
            except Exception:
                pass
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_logical_emit_message(true, %s, %s)",
                        ("dataflow.heartbeat", self.slot_name or "dataflow"),
                    )
                conn.commit()
        except Exception as exc:
            _logger.debug("Postgres CDC heartbeat emit skipped: %s", exc)

    def _poll_signal_table(self) -> None:
        """Debezium-compatible signal table → incremental snapshot enqueue."""
        import time as _time

        now = _time.monotonic()
        if (
            self._signal_table_ready
            and (now - self._last_signal_poll_at) < max(1.0, self._signal_poll_interval_sec)
        ):
            return
        from services.cdc_signal_table import ensure_signal_table, poll_signal_table

        try:
            with self._conn() as conn:
                if not self._signal_table_ready:
                    ensure_signal_table(conn, table=self.signal_table, dialect="postgresql")
                    self._signal_table_ready = True
                _, self._processed_signal_ids = poll_signal_table(
                    conn,
                    source_key=self.source_key,
                    table=self.signal_table,
                    default_table=self.table,
                    primary_key=self.primary_key,
                    processed_ids=self._processed_signal_ids,
                    dialect="postgresql",
                )
            self._last_signal_poll_at = now
        except Exception as exc:
            _logger.debug("Postgres CDC signal table poll skipped: %s", exc)

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
        1. Create publication (pgoutput) then logical slot (consistent point LSN).
        2. Read the table under ``REPEATABLE READ`` on one connection so the
           dump is a single MVCC snapshot (not N independent page reads).
        3. Persist ``phase=streaming`` + LSN so poll resumes without a gap window
           outside the slot (duplicates during the dump are possible; upserts OK).
        """
        from connectors.postgresql_reader import _cell, _order_by_clause
        from connectors.sql_identifiers import quote_column_list, quote_table_ref

        # pgoutput requires the publication before the slot retains WAL for it.
        if self.output_plugin == "pgoutput":
            self._ensure_publication()
            self._ensure_replica_identity()
        self._ensure_slot()
        self.phase = "snapshot"
        self._ensure_decode_schema(resume_offset=self.slot_name)
        self.heartbeat()

        with self._conn() as conn:
            # One RR transaction spans all tables so the multi-table dump shares
            # a consistent MVCC snapshot (Debezium initial-sync pattern).
            prev_autocommit = getattr(conn, "autocommit", True)
            try:
                conn.autocommit = False
                with conn.cursor() as cur:
                    cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    cur.execute("SELECT pg_current_wal_lsn()::text")
                    snap_lsn_row = cur.fetchone()
                    if snap_lsn_row and snap_lsn_row[0]:
                        self.consistent_point_lsn = str(snap_lsn_row[0])
                    for table_name in self.tables:
                        order_by = _order_by_clause(
                            cur, self.schema, table_name, self.columns
                        )
                        table_ref = quote_table_ref(
                            table_name,
                            self.schema,
                            dialect="postgresql",
                            preserve_case=True,
                        )
                        col_sql = quote_column_list(self.columns, quote_char='"')
                        query = (
                            f"SELECT {col_sql} FROM {table_ref} "
                            f"ORDER BY {order_by} LIMIT %s OFFSET %s"
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
                                table=table_name,
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
        yield ChangeBatch(
            resume_token=self._resume_token(phase="streaming"),
            ack_barrier=True,
        )

    def _pk_value(self, record: dict[str, str], *, table: str | None = None) -> str:
        if not record:
            return ""
        pk_col = self.primary_keys.get(table or self.table, self.primary_key)
        return str(record.get(pk_col, "") or "")

    def _qualified_table(self, table: str | None = None) -> str:
        from connectors.sql_identifiers import quote_table_ref

        return quote_table_ref(
            table or self.table, self.schema or "public", dialect="postgresql"
        )

    def _ensure_publication(self) -> None:
        """Create a FOR TABLE publication required by the pgoutput plugin.

        Multi-table shared reader: one publication listing every captured table
        (Debezium-class single slot / N tables).
        """
        if self.output_plugin != "pgoutput":
            return
        from connectors.sql_identifiers import require_safe_identifier

        pub = require_safe_identifier(self.publication_name, preserve_case=False)
        self.publication_name = pub
        qualified_list = [self._qualified_table(t) for t in self.tables]
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_publication WHERE pubname = %s",
                    (pub,),
                )
                if cur.fetchone() is None:
                    tables_sql = ", ".join(qualified_list)
                    cur.execute(f"CREATE PUBLICATION {pub} FOR TABLE {tables_sql}")
                else:
                    for qualified in qualified_list:
                        try:
                            cur.execute(f"ALTER PUBLICATION {pub} ADD TABLE {qualified}")
                        except Exception:
                            try:
                                conn.rollback()
                            except Exception:
                                pass
            conn.commit()
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (pub,))
                if cur.fetchone() is None:
                    raise RuntimeError(f"pgoutput publication {pub} was not created")
            conn.commit()

    def _ensure_replica_identity(self) -> None:
        """Require FULL replica identity so UPDATE/DELETE emit old keys."""
        for table in self.tables:
            qualified = self._qualified_table(table)
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
                table=self.tables if len(self.tables) > 1 else self.table,
                cursor_key=self.cursor_key,
            )
            if token_lsn:
                lsn = token_lsn
        if not lsn:
            return
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_replication_slot_advance(%s, %s::pg_lsn)",
                        (self.slot_name, lsn),
                    )
                conn.commit()
            self.consistent_point_lsn = lsn
            self._pending_ack_lsn = None
        except Exception as exc:
            _logger.warning(
                "Postgres CDC ack failed for slot %s at %s: %s",
                self.slot_name,
                lsn,
                exc,
            )
            raise

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

    def _peek_stream_events_during_chunk(self, sig: Any) -> list[dict[str, Any]]:
        """Peek WAL (no ack) for DDD-3 stream-wins during an incremental snapshot chunk."""
        try:
            self._ensure_slot()
            if self.output_plugin == "pgoutput":
                self._ensure_publication()
        except Exception:
            return []
        events: list[dict[str, Any]] = []
        peek_limit = min(int(sig.chunk_size or self.batch_size), 500)
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    if self.output_plugin == "pgoutput":
                        cur.execute(
                            """
                            SELECT lsn::text, data
                            FROM pg_logical_slot_peek_binary_changes(
                                %s, NULL, %s,
                                'proto_version', '1',
                                'publication_names', %s
                            )
                            """,
                            (self.slot_name, peek_limit, self.publication_name),
                        )
                    else:
                        cur.execute(
                            """
                            SELECT lsn::text, data
                            FROM pg_logical_slot_peek_changes(
                                %s, NULL, %s, 'include-xids', '1'
                            )
                            """,
                            (self.slot_name, peek_limit),
                        )
                    rows = cur.fetchall() or []
                conn.commit()
        except Exception:
            return []

        pk_col = sig.primary_key or self.primary_key
        if self.output_plugin == "pgoutput":
            from connectors.pgoutput_decoder import PgOutputDecoder, changes_for_table

            if self._pgoutput_decoder is None:
                self._pgoutput_decoder = PgOutputDecoder()
            decoder = self._pgoutput_decoder
            for _location, payload in rows:
                for change in changes_for_table(
                    decoder, payload, schema=self.schema, table=self.table
                ):
                    if change.op == "insert" and change.new_tuple:
                        events.append({"op": "c", "row": dict(change.new_tuple)})
                    elif change.op == "update" and change.new_tuple:
                        events.append({"op": "u", "row": dict(change.new_tuple)})
                    elif change.op == "delete" and change.old_tuple:
                        pk = self._pk_value(change.old_tuple)
                        if pk:
                            events.append({"op": "d", "pk": pk, "row": {pk_col: pk}})
            return events

        for _location, line in rows:
            text = (
                line.decode("utf-8", errors="replace")
                if isinstance(line, (bytes, memoryview))
                else (line or "")
            )
            upper = text.upper().strip()
            if upper.startswith(("BEGIN", "COMMIT", "ROLLBACK", "ABORT")):
                continue
            parsed = _parse_change_line(text, self.schema, self.table)
            if parsed is None:
                continue
            op, old_key, new_tuple = parsed
            if op == "insert" and new_tuple:
                events.append({"op": "c", "row": dict(new_tuple)})
            elif op == "update" and new_tuple:
                events.append({"op": "u", "row": dict(new_tuple)})
            elif op == "delete" and old_key:
                pk = self._pk_value(old_key)
                if pk:
                    events.append({"op": "d", "pk": pk, "row": {pk_col: pk}})
        return events

    def poll(self) -> Iterator[ChangeBatch]:
        """Peek WAL with txn buffering (Debezium-class) + incremental snapshots.

        Multi-table mode demuxes one slot into per-table batches that share an
        LSN resume token; callers must ``ack`` only after ``ack_barrier`` batches.
        """
        from services.cdc_incremental_runner import interleave_incremental_snapshot
        from services.cdc_multi_table import (
            MultiTableTransactionBuffer,
            parse_test_decoding_table,
        )

        self._poll_signal_table()

        for table_name in self.tables:
            yield from interleave_incremental_snapshot(
                self.source_key,
                table=table_name,
                fetch_chunk=self._fetch_incremental_chunk,
                stream_events_during_chunk=self._peek_stream_events_during_chunk,
                max_chunks_per_poll=1,
            )

        if self.output_plugin == "pgoutput":
            self._ensure_publication()
            self._ensure_replica_identity()
        self._ensure_slot()
        self.phase = "streaming"
        self._ensure_decode_schema(resume_offset=self.slot_name)
        self._maybe_record_schema_change(offset=self.slot_name)
        self.heartbeat()

        with self._conn() as conn:
            with conn.cursor() as cur:
                if self.output_plugin == "pgoutput":
                    cur.execute(
                        """
                        SELECT lsn::text, data
                        FROM pg_logical_slot_peek_binary_changes(
                            %s, NULL, %s,
                            'proto_version', '1',
                            'publication_names', %s
                        )
                        """,
                        (self.slot_name, self.batch_size, self.publication_name),
                    )
                else:
                    cur.execute(
                        """
                        SELECT lsn::text, data
                        FROM pg_logical_slot_peek_changes(
                            %s, NULL, %s, 'include-xids', '1'
                        )
                        """,
                        (self.slot_name, self.batch_size),
                    )
                rows = cur.fetchall()
            conn.commit()

        buf = MultiTableTransactionBuffer()
        emitted = False
        table_set = {t.lower() for t in self.tables}
        table_by_lower = {t.lower(): t for t in self.tables}

        def _token_at(lsn: str | None) -> str:
            if lsn:
                self.consistent_point_lsn = lsn
                self._pending_ack_lsn = lsn
            return self._resume_token(phase="streaming")

        def _emit_commit(lsn: str | None):
            nonlocal emitted
            for batch in buf.commit(
                lsn=lsn,
                resume_token=_token_at(lsn),
                table_order=self.tables,
            ):
                emitted = True
                yield batch

        if self.output_plugin == "pgoutput":
            from connectors.pgoutput_decoder import PgOutputDecoder, changes_for_tables

            if self._pgoutput_decoder is None:
                self._pgoutput_decoder = PgOutputDecoder()
            decoder = self._pgoutput_decoder
            for location, payload in rows:
                lsn = str(location) if location else None
                for change in changes_for_tables(
                    decoder,
                    payload,
                    schema=self.schema,
                    tables=table_set,
                ):
                    self._last_event_at = datetime.now(timezone.utc)
                    if change.op == "begin":
                        buf.begin(change.xid, lsn=lsn)
                    elif change.op == "commit":
                        yield from _emit_commit(lsn)
                    elif change.op == "insert" and change.new_tuple:
                        tbl = table_by_lower.get(
                            (change.relation or "").lower(), change.relation or self.table
                        )
                        buf.insert(tbl, change.new_tuple, lsn=lsn)
                    elif change.op == "update" and change.new_tuple:
                        tbl = table_by_lower.get(
                            (change.relation or "").lower(), change.relation or self.table
                        )
                        buf.update(tbl, change.new_tuple, lsn=lsn)
                    elif change.op == "delete" and change.old_tuple:
                        tbl = table_by_lower.get(
                            (change.relation or "").lower(), change.relation or self.table
                        )
                        pk = self._pk_value(change.old_tuple, table=tbl)
                        if pk:
                            buf.delete(tbl, pk, lsn=lsn)
        else:
            for location, line in rows:
                lsn = str(location) if location else None
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
                    yield from _emit_commit(lsn)
                    continue
                if upper.startswith("ROLLBACK") or upper.startswith("ABORT"):
                    buf.rollback()
                    continue
                if "ALTER TABLE" in upper or ": DDL:" in upper:
                    self._maybe_record_schema_change(offset=self.slot_name)
                    continue
                parsed_tbl = parse_test_decoding_table(text)
                if not parsed_tbl:
                    continue
                schema_name, relation = parsed_tbl
                if schema_name.lower() != (self.schema or "").lower():
                    continue
                if relation.lower() not in table_set:
                    continue
                tbl = table_by_lower[relation.lower()]
                parsed = _parse_change_line(text, self.schema, tbl)
                if parsed is None:
                    continue
                op, old_key, new_tuple = parsed
                self._last_event_at = datetime.now(timezone.utc)
                if op == "insert" and new_tuple:
                    buf.insert(tbl, new_tuple, lsn=lsn)
                elif op == "update" and new_tuple:
                    buf.update(tbl, new_tuple, lsn=lsn)
                elif op == "delete" and old_key:
                    pk = self._pk_value(old_key, table=tbl)
                    if pk:
                        buf.delete(tbl, pk, lsn=lsn)

        if buf.open_xid is not None:
            if not emitted:
                yield ChangeBatch(
                    resume_token={
                        "phase": "streaming",
                        "txn_held": True,
                        "open_xid": buf.open_xid,
                        "token": self._resume_token(phase="streaming"),
                    }
                )
            return

        if not emitted:
            yield ChangeBatch(
                resume_token=_token_at(self.consistent_point_lsn),
                ack_barrier=True,
            )
