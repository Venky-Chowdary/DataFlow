"""PostgreSQL logical decoding CDC reader.

Default output plugin is ``test_decoding`` (text parse path). ``pgoutput`` can be
selected via ``logical_decoding_plugin`` once a binary decoder is available.
Replication lag is exposed via ``replication_lag_bytes()``.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterator
from typing import Any

from connectors.postgresql_conn import get_connection
from connectors.postgresql_reader import read_table_batch
from services.cdc_engine import ChangeBatch

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

    def snapshot(self) -> Iterator[ChangeBatch]:
        """Yield the full table as INSERT batches and create the logical slot."""
        self._ensure_slot()
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
            parsed = _parse_change_line(text, self.schema, self.table)
            if parsed is None:
                continue
            op, old_key, new_tuple = parsed
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
