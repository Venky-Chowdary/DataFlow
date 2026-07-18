"""MySQL binlog CDC reader using python-mysql-replication.

Requires ``binlog_format=ROW`` and a user with ``REPLICATION SLAVE`` and
``REPLICATION CLIENT`` privileges. Falls back to query-based CDC when the
deployment does not expose the binlog.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

from connectors.mysql_conn import get_connection
from connectors.mysql_reader import _cell, read_table_batch
from services.cdc_engine import ChangeBatch


def _serialize(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    return _cell(value)


class MySqlChangeStreamCdc:
    """Log-based CDC for MySQL using the binlog stream."""

    def __init__(
        self,
        cfg: dict[str, Any],
        table: str,
        primary_key: str,
        columns: list[str] | None = None,
        resume_token: dict[str, Any] | str | None = None,
        batch_size: int = 1000,
        max_wait_seconds: float = 30.0,
    ) -> None:
        self.cfg = cfg
        self.database = cfg.get("database") or cfg.get("schema") or ""
        self.table = table
        self.primary_key = primary_key
        self.columns = columns
        self.batch_size = batch_size
        self.max_wait_seconds = max_wait_seconds
        self._column_names_cache: list[str] | None = None
        if isinstance(resume_token, str) and resume_token:
            try:
                self.resume_token = json.loads(resume_token)
            except Exception:
                self.resume_token = None
        else:
            self.resume_token = resume_token or None

    def _conn(self):
        return get_connection(
            host=self.cfg.get("host") or "localhost",
            port=self.cfg.get("port") or 3306,
            database=self.database,
            username=self.cfg.get("username") or "",
            password=self.cfg.get("password") or "",
            connection_string=self.cfg.get("connection_string") or "",
            ssl=bool(self.cfg.get("ssl")),
        )

    def is_available(self) -> bool:
        try:
            conn = self._conn()
            with conn.cursor() as cur:
                cur.execute("SHOW VARIABLES LIKE 'log_bin'")
                row = cur.fetchone()
                if not row or str(row[1]).lower() not in {"on", "1", "true"}:
                    return False
                cur.execute("SHOW VARIABLES LIKE 'binlog_format'")
                row = cur.fetchone()
                if not row or (row[1] or "").upper() != "ROW":
                    return False
            conn.close()

            kwargs = self._binlog_kwargs(blocking=False, only_events=[])
            from pymysqlreplication import BinLogStreamReader

            stream = BinLogStreamReader(**kwargs)
            stream.close()
            return True
        except Exception:
            return False

    def _binlog_kwargs(self, blocking: bool, only_events: list[type]) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "connection_settings": {
                "host": self.cfg.get("host") or "localhost",
                "port": self.cfg.get("port") or 3306,
                "user": self.cfg.get("username") or "",
                "password": self.cfg.get("password") or "",
            },
            "server_id": 10001,
            "blocking": blocking,
            "only_schemas": self.database if self.database else None,
            "only_tables": self.table if self.table else None,
        }
        # An empty ``only_events`` list is an allowlist matching NOTHING (the
        # reader would silently yield zero events). Only set it when non-empty;
        # otherwise leave it unset so BinLogStreamReader streams all events.
        if only_events:
            kwargs["only_events"] = only_events
        if self.resume_token and self.resume_token.get("file") and self.resume_token.get("pos") is not None:
            kwargs["log_file"] = self.resume_token["file"]
            kwargs["log_pos"] = self.resume_token["pos"]
        return kwargs

    def snapshot(self) -> Iterator[ChangeBatch]:
        # Capture binlog file/pos BEFORE the snapshot so poll() starts from a
        # consistent handoff point (at-least-once; duplicates possible, no gaps).
        start_pos = self._current_binlog_position()
        offset = 0
        while True:
            batch = read_table_batch(
                host=self.cfg.get("host") or "localhost",
                port=self.cfg.get("port") or 3306,
                database=self.database,
                username=self.cfg.get("username") or "",
                password=self.cfg.get("password") or "",
                schema="",
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
            yield ChangeBatch(inserts=records)
            offset += len(batch.rows)
            if len(batch.rows) < self.batch_size:
                break
        yield ChangeBatch(resume_token=start_pos or {"table": self.table, "file": None, "pos": None})

    def _current_binlog_position(self) -> dict[str, Any] | None:
        try:
            conn = self._conn()
            try:
                with conn.cursor() as cur:
                    for sql in ("SHOW MASTER STATUS", "SHOW BINARY LOG STATUS"):
                        try:
                            cur.execute(sql)
                            row = cur.fetchone()
                            if row:
                                return {"file": row[0], "pos": int(row[1]), "table": self.table}
                        except Exception:
                            continue
            finally:
                conn.close()
        except Exception:
            return None
        return None

    def _ordered_columns(self) -> list[str]:
        """Ordered column names from information_schema.

        MySQL 8.0+/9.x only embed column names in the binlog when
        ``binlog_row_metadata=FULL``; with the default MINIMAL the reader yields
        positional ``UNKNOWN_COL0..N`` keys. Resolving names by ordinal here keeps
        CDC correct regardless of the server's metadata setting.
        """
        if self._column_names_cache is not None:
            return self._column_names_cache
        cols: list[str] = []
        try:
            conn = self._conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COLUMN_NAME FROM information_schema.columns "
                        "WHERE table_schema = %s AND table_name = %s "
                        "ORDER BY ORDINAL_POSITION",
                        (self.database, self.table),
                    )
                    cols = [str(r[0]) for r in cur.fetchall()]
            finally:
                conn.close()
        except Exception:
            cols = []
        self._column_names_cache = cols
        return cols

    def _remap_positional(self, row: dict[str, Any]) -> dict[str, Any]:
        """Map positional ``UNKNOWN_COL{n}`` keys back to real column names."""
        if not any(isinstance(k, str) and k.startswith("UNKNOWN_COL") for k in row):
            return row
        names = self._ordered_columns()
        if not names:
            return row
        remapped: dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(key, str) and key.startswith("UNKNOWN_COL"):
                try:
                    idx = int(key[len("UNKNOWN_COL"):])
                except ValueError:
                    remapped[key] = value
                    continue
                remapped[names[idx] if 0 <= idx < len(names) else key] = value
            else:
                remapped[key] = value
        return remapped

    def _row_to_record(self, row: dict[str, Any]) -> dict[str, str]:
        row = self._remap_positional(row)
        return {k: _serialize(v) for k, v in row.items()}

    def _pk_value(self, row: dict[str, Any]) -> str:
        row = self._remap_positional(row)
        return _serialize(row.get(self.primary_key))

    def poll(self) -> Iterator[ChangeBatch]:
        from pymysqlreplication import BinLogStreamReader
        from pymysqlreplication.event import RotateEvent
        from pymysqlreplication.row_event import (
            DeleteRowsEvent,
            UpdateRowsEvent,
            WriteRowsEvent,
        )

        inserts: list[dict[str, str]] = []
        updates: list[dict[str, str]] = []
        deletes: list[str] = []
        last_position: dict[str, Any] | None = None
        deadline = datetime.now(timezone.utc).timestamp() + self.max_wait_seconds

        # Only stream the events we act on (row changes) plus rotation so we can
        # track the binlog file handoff for the resume token.
        kwargs = self._binlog_kwargs(
            blocking=False,
            only_events=[RotateEvent, WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
        )
        stream = BinLogStreamReader(**kwargs)
        try:
            for binlog_event in stream:
                if datetime.now(timezone.utc).timestamp() > deadline:
                    break
                if isinstance(binlog_event, RotateEvent):
                    last_position = {"file": binlog_event.next_binlog, "pos": binlog_event.position}
                    continue
                if getattr(binlog_event, "schema", "") != self.database:
                    continue
                if getattr(binlog_event, "table", "") != self.table:
                    continue

                if isinstance(binlog_event, WriteRowsEvent):
                    for row in getattr(binlog_event, "rows", []):
                        # pymysqlreplication wraps write rows as {"values": {...}};
                        # tolerate a flat dict too for forward/backward compatibility.
                        values = (
                            row.get("values")
                            if isinstance(row, dict) and "values" in row
                            else row
                        )
                        inserts.append(self._row_to_record(values))
                elif isinstance(binlog_event, UpdateRowsEvent):
                    for row in getattr(binlog_event, "rows", []):
                        after = row.get("after_values") if isinstance(row, dict) else getattr(row, "after_values", {})
                        updates.append(self._row_to_record(after))
                elif isinstance(binlog_event, DeleteRowsEvent):
                    for row in getattr(binlog_event, "rows", []):
                        values = row.get("values") if isinstance(row, dict) else getattr(row, "values", {})
                        pk = self._pk_value(values)
                        if pk:
                            deletes.append(pk)

                if stream.log_pos:
                    last_position = {"file": getattr(stream, "log_file", ""), "pos": stream.log_pos}

                if len(inserts) + len(updates) + len(deletes) >= self.batch_size:
                    break
        finally:
            stream.close()

        if inserts or updates or deletes or last_position:
            yield ChangeBatch(
                inserts=inserts,
                updates=updates,
                deletes=deletes,
                resume_token=last_position,
            )
