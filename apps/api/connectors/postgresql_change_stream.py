"""PostgreSQL logical decoding CDC reader.

Default output plugin is ``test_decoding`` (text parse path). ``pgoutput`` can be
selected via ``logical_decoding_plugin`` once a binary decoder is available.
Replication lag is exposed via ``replication_lag_bytes()`` and
``replication_lag_seconds()``.
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

from connectors.postgresql_conn import get_connection
from connectors.postgresql_reader import read_table_batch
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


def _slot_name(database: str, table: str, cursor_key: str) -> str:
    token = hashlib.sha256(cursor_key.encode()).hexdigest()[:8]
    raw = f"df_{database}_{table}_{token}".lower()
    return re.sub(r"[^a-z0-9_]", "_", raw)[:63]


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
        self.slot_name = resume_token or _slot_name(self.database, table, cursor_key)
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

    def _select_plugin(self) -> str:
        """Select logical decoding plugin.

        ``test_decoding`` remains the production parse path. ``pgoutput`` can be
        forced via ``logical_decoding_plugin=pgoutput`` once a binary decoder ships;
        probing alone is insufficient without row decoding.
        """
        preferred = (self.cfg.get("logical_decoding_plugin") or "").strip().lower()
        if preferred in {"pgoutput", "test_decoding"}:
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

    def _ensure_slot(self) -> None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT plugin FROM pg_replication_slots WHERE slot_name = %s",
                    (self.slot_name,),
                )
                row = cur.fetchone()
                if row is None:
                    cur.execute(
                        "SELECT pg_create_logical_replication_slot(%s, %s)",
                        (self.slot_name, self.output_plugin),
                    )
                else:
                    # Honor existing slot plugin (cannot change without drop).
                    self.output_plugin = row[0] or self.output_plugin
            conn.commit()

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
        """Yield the full table as INSERT batches and create the logical slot."""
        self._ensure_slot()
        self._ensure_decode_schema(resume_offset=self.slot_name)
        self.heartbeat()
        offset = 0
        while True:
            batch = read_table_batch(
                host=self.cfg.get("host") or "localhost",
                port=self.cfg.get("port") or 5432,
                database=self.database,
                username=self.cfg.get("username") or "",
                password=self.cfg.get("password") or "",
                schema=self.schema,
                connection_string=self.cfg.get("connection_string") or "",
                ssl=bool(self.cfg.get("ssl")),
                table=self.table,
                columns=self.columns,
                offset=offset,
                limit=self.batch_size,
            )
            if not batch.rows:
                break
            records = [dict(zip(batch.headers, row)) for row in batch.rows]
            yield ChangeBatch(inserts=records, resume_token=self.slot_name)
            offset += len(batch.rows)
            if len(batch.rows) < self.batch_size:
                break
        yield ChangeBatch(resume_token=self.slot_name)

    def _pk_value(self, record: dict[str, str]) -> str:
        return record.get(self.primary_key, "") if record else ""

    def poll(self) -> Iterator[ChangeBatch]:
        """Consume a batch of changes from the logical slot (test_decoding text)."""
        self._ensure_slot()
        self._ensure_decode_schema(resume_offset=self.slot_name)
        self._maybe_record_schema_change(offset=self.slot_name)
        self.heartbeat()
        if self.output_plugin == "pgoutput":
            # Binary pgoutput decoder not yet shipped — do not consume opaque WAL.
            return
            yield  # pragma: no cover

        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT data FROM pg_logical_slot_get_changes(%s, NULL, %s, 'include-xids', '0')",
                    (self.slot_name, self.batch_size),
                )
                rows = cur.fetchall()
            conn.commit()

        inserts: list[dict[str, str]] = []
        updates: list[dict[str, str]] = []
        deletes: list[str] = []

        for (line,) in rows:
            text = line or ""
            if text in ("BEGIN", "COMMIT"):
                continue
            # test_decoding may surface DDL-ish lines; treat as schema refresh signal.
            upper = text.upper()
            if "ALTER TABLE" in upper or ": DDL:" in upper:
                self._maybe_record_schema_change(offset=self.slot_name)
                continue
            parsed = _parse_change_line(text, self.schema, self.table)
            if parsed is None:
                continue
            op, old_key, new_tuple = parsed
            self._last_event_at = datetime.now(timezone.utc)
            if op == "insert" and new_tuple:
                inserts.append(new_tuple)
            elif op == "update" and new_tuple:
                updates.append(new_tuple)
            elif op == "delete" and old_key:
                pk = self._pk_value(old_key)
                if pk:
                    deletes.append(pk)

        if inserts or updates or deletes:
            yield ChangeBatch(
                inserts=inserts,
                updates=updates,
                deletes=deletes,
                resume_token=self.slot_name,
            )
