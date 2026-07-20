"""MySQL binlog CDC reader using python-mysql-replication.

Requires ``binlog_format=ROW`` and a user with ``REPLICATION SLAVE`` and
``REPLICATION CLIENT`` privileges. Falls back to query-based CDC when the
deployment does not expose the binlog.

Schema history is persisted on DDL observation so decode schemas can be rebuilt
after restart. Lag is exposed via ``replication_lag_seconds()``.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

from connectors.mysql_conn import get_connection
from connectors.mysql_reader import _cell, read_table_batch
from services.cdc_engine import ChangeBatch
from services.cdc_schema_history import (
    connection_fingerprint,
    last_ddl_at,
    rebuild_schema,
    record_ddl,
)

_logger = logging.getLogger(__name__)
_DDL_RE = re.compile(
    r"\b(ALTER|CREATE|DROP|RENAME)\s+TABLE\b",
    re.IGNORECASE,
)


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
        self.source_key = connection_fingerprint(
            {**cfg, "type": "mysql"},
            connector_id=str(cfg.get("connector_id") or ""),
        )
        self.decode_schema: dict[str, Any] = {}
        self.last_ddl_at: str | None = None
        self._last_event_at: datetime | None = None
        self._last_heartbeat_at: datetime | None = None
        self._schema_ready = False
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
        # Unique server_id per connector/table so multi-stream CDC does not collide.
        configured = self.cfg.get("server_id") or self.cfg.get("binlog_server_id")
        if configured is not None:
            server_id = int(configured)
        else:
            import hashlib

            digest = hashlib.sha1(  # noqa: S324 — non-crypto server_id bookkeeping
                f"{self.cfg.get('host')}|{self.database}|{self.table}".encode(),
                usedforsecurity=False,
            ).hexdigest()
            server_id = 10_000 + (int(digest[:6], 16) % 1_000_000)
        kwargs: dict[str, Any] = {
            "connection_settings": {
                "host": self.cfg.get("host") or "localhost",
                "port": self.cfg.get("port") or 3306,
                "user": self.cfg.get("username") or "",
                "password": self.cfg.get("password") or "",
            },
            "server_id": server_id,
            "blocking": blocking,
            "only_schemas": self.database if self.database else None,
            "only_tables": self.table if self.table else None,
        }
        # An empty ``only_events`` list is an allowlist matching NOTHING (the
        # reader would silently yield zero events). Only set it when non-empty;
        # otherwise leave it unset so BinLogStreamReader streams all events.
        if only_events:
            kwargs["only_events"] = only_events
        # Prefer GTID auto-position (Debezium-class); fall back to file/pos.
        gtid = None
        if isinstance(self.resume_token, dict):
            gtid = self.resume_token.get("gtid") or self.resume_token.get("gtid_set")
        if gtid:
            kwargs["auto_position"] = True
            try:
                # pymysqlreplication >=0.45
                kwargs["gtid_set"] = gtid
            except Exception:
                pass
        elif self.resume_token and self.resume_token.get("file") and self.resume_token.get("pos") is not None:
            kwargs["log_file"] = self.resume_token["file"]
            kwargs["log_pos"] = self.resume_token["pos"]
        return kwargs

    def snapshot(self) -> Iterator[ChangeBatch]:
        # Capture binlog file/pos BEFORE the snapshot so poll() starts from a
        # consistent handoff point (at-least-once; duplicates possible, no gaps).
        start_pos = self._current_binlog_position() or {
            "table": self.table,
            "file": None,
            "pos": None,
        }
        self._ensure_decode_schema(resume_offset=start_pos)
        self.heartbeat()
        # Resume mid-snapshot when watermark carries phase/offset.
        offset = 0
        if isinstance(self.resume_token, dict) and self.resume_token.get("phase") == "snapshot":
            offset = int(self.resume_token.get("offset") or 0)
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
            offset += len(batch.rows)
            # Every snapshot batch carries a resume token so a crash cannot
            # overwrite the binlog handoff with a primary-key cursor.
            yield ChangeBatch(
                inserts=records,
                resume_token={
                    **start_pos,
                    "phase": "snapshot",
                    "offset": offset,
                    "table": self.table,
                },
            )
            if len(batch.rows) < self.batch_size:
                break
        yield ChangeBatch(
            resume_token={
                **start_pos,
                "phase": "streaming",
                "offset": 0,
                "table": self.table,
            }
        )

    def _current_gtid_executed(self, cur) -> str | None:
        try:
            cur.execute("SELECT @@GLOBAL.gtid_executed")
            row = cur.fetchone()
            if row and row[0]:
                return str(row[0])
        except Exception:
            pass
        try:
            cur.execute("SHOW GLOBAL VARIABLES LIKE 'gtid_executed'")
            row = cur.fetchone()
            if row and len(row) > 1 and row[1]:
                return str(row[1])
        except Exception:
            pass
        return None

    def _current_binlog_position(self) -> dict[str, Any] | None:
        try:
            conn = self._conn()
            try:
                with conn.cursor() as cur:
                    gtid = self._current_gtid_executed(cur)
                    for sql in ("SHOW MASTER STATUS", "SHOW BINARY LOG STATUS"):
                        try:
                            cur.execute(sql)
                            row = cur.fetchone()
                            if row:
                                pos = {
                                    "file": row[0],
                                    "pos": int(row[1]),
                                    "table": self.table,
                                }
                                if gtid:
                                    pos["gtid"] = gtid
                                return pos
                        except Exception:
                            continue
                    if gtid:
                        return {"gtid": gtid, "file": None, "pos": None, "table": self.table}
            finally:
                conn.close()
        except Exception:
            return None
        return None

    def replication_lag_bytes(self) -> int | None:
        """Best-effort binlog byte lag vs current master position."""
        try:
            current = self._current_binlog_position()
            if not current or not self.resume_token:
                return None
            if current.get("file") != self.resume_token.get("file"):
                return None
            cur_pos = int(current.get("pos") or 0)
            resume_pos = int(self.resume_token.get("pos") or 0)
            return max(0, cur_pos - resume_pos)
        except Exception:
            return None

    def replication_lag_seconds(self) -> float | None:
        """Seconds since the last binlog event / heartbeat, when known."""
        anchor = self._last_event_at or self._last_heartbeat_at
        if anchor is None:
            return None
        return max(0.0, (datetime.now(timezone.utc) - anchor).total_seconds())

    def heartbeat(self) -> None:
        self._last_heartbeat_at = datetime.now(timezone.utc)

    def _fetch_live_schema(self) -> dict[str, Any]:
        columns: dict[str, str] = {}
        nullable: dict[str, bool] = {}
        primary_key: list[str] = []
        try:
            conn = self._conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY "
                        "FROM information_schema.columns "
                        "WHERE table_schema = %s AND table_name = %s "
                        "ORDER BY ORDINAL_POSITION",
                        (self.database, self.table),
                    )
                    for name, col_type, is_nullable, column_key in cur.fetchall():
                        col = str(name)
                        columns[col] = str(col_type or "text")
                        nullable[col] = str(is_nullable or "").upper() == "YES"
                        if str(column_key or "").upper() == "PRI":
                            primary_key.append(col)
            finally:
                conn.close()
        except Exception:
            _logger.debug("MySQL live schema fetch failed", exc_info=True)
        return {"columns": columns, "nullable": nullable, "primary_key": primary_key}

    def _schema_fingerprint(self, snapshot: dict[str, Any]) -> str:
        cols = snapshot.get("columns") or {}
        nulls = snapshot.get("nullable") or {}
        pk = snapshot.get("primary_key") or []
        parts = [f"{k}:{cols[k]}:{int(bool(nulls.get(k, True)))}" for k in sorted(cols)]
        parts.append("pk=" + ",".join(pk))
        return "|".join(parts)

    def _ensure_decode_schema(self, *, resume_offset: Any = None) -> dict[str, Any]:
        if self._schema_ready and self.decode_schema:
            return self.decode_schema
        rebuilt = rebuild_schema(self.source_key, self.table, resume_offset)
        if rebuilt:
            self.decode_schema = rebuilt
            # Keep positional remap aligned with rebuilt history.
            cols = list((rebuilt.get("columns") or {}).keys())
            if cols:
                self._column_names_cache = cols
        else:
            live = self._fetch_live_schema()
            if live.get("columns"):
                record_ddl(
                    self.source_key,
                    self.table,
                    ddl="SNAPSHOT",
                    offset=resume_offset or self.resume_token,
                    schema_snapshot=live,
                )
                self.decode_schema = live
                self._column_names_cache = list(live["columns"].keys())
        self.last_ddl_at = last_ddl_at(self.source_key, self.table)
        self._schema_ready = True
        return self.decode_schema

    def _record_schema_change(self, *, ddl: str, offset: Any = None) -> None:
        live = self._fetch_live_schema()
        if not live.get("columns"):
            return
        if self.decode_schema and self._schema_fingerprint(live) == self._schema_fingerprint(self.decode_schema):
            return
        entry = record_ddl(
            self.source_key,
            self.table,
            ddl=ddl or "ALTER TABLE (detected)",
            offset=offset or self.resume_token,
            schema_snapshot=live,
        )
        self.decode_schema = live
        self._column_names_cache = list(live["columns"].keys())
        self.last_ddl_at = str(entry.get("recorded_at") or "") or self.last_ddl_at
        _logger.info(
            "Recorded MySQL CDC schema change for %s.%s v%s",
            self.database,
            self.table,
            entry.get("version"),
        )

    def _ddl_targets_table(self, query: str) -> bool:
        if not query or not _DDL_RE.search(query):
            return False
        # Match `db.table` or bare `table` references in common DDL forms.
        pattern = re.compile(
            rf"(?:`?{re.escape(self.database)}`?\.)?`?{re.escape(self.table)}`?\b",
            re.IGNORECASE,
        )
        return bool(pattern.search(query))

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
        # Incomplete initial sync must finish before binlog streaming.
        if isinstance(self.resume_token, dict) and self.resume_token.get("phase") == "snapshot":
            yield from self.snapshot()
            return

        from pymysqlreplication import BinLogStreamReader
        from pymysqlreplication.event import QueryEvent, RotateEvent
        from pymysqlreplication.row_event import (
            DeleteRowsEvent,
            UpdateRowsEvent,
            WriteRowsEvent,
        )

        self._ensure_decode_schema(resume_offset=self.resume_token)
        self.heartbeat()

        inserts: list[dict[str, str]] = []
        updates: list[dict[str, str]] = []
        deletes: list[str] = []
        last_position: dict[str, Any] | None = None
        deadline = datetime.now(timezone.utc).timestamp() + self.max_wait_seconds

        # Row changes + rotation + QueryEvent (DDL) for schema history.
        kwargs = self._binlog_kwargs(
            blocking=False,
            only_events=[RotateEvent, QueryEvent, WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
        )
        stream = BinLogStreamReader(**kwargs)
        try:
            for binlog_event in stream:
                if datetime.now(timezone.utc).timestamp() > deadline:
                    break
                if isinstance(binlog_event, RotateEvent):
                    last_position = {"file": binlog_event.next_binlog, "pos": binlog_event.position}
                    continue
                if isinstance(binlog_event, QueryEvent):
                    query = getattr(binlog_event, "query", "") or ""
                    if self._ddl_targets_table(query):
                        pos = {
                            "file": getattr(stream, "log_file", "") or (self.resume_token or {}).get("file"),
                            "pos": getattr(stream, "log_pos", None) or getattr(binlog_event, "packet", None),
                            "table": self.table,
                        }
                        if stream.log_pos:
                            pos["pos"] = stream.log_pos
                        self._record_schema_change(ddl=query.strip()[:2000], offset=pos)
                        self._last_event_at = datetime.now(timezone.utc)
                    continue
                if getattr(binlog_event, "schema", "") != self.database:
                    continue
                if getattr(binlog_event, "table", "") != self.table:
                    continue

                event_ts = getattr(binlog_event, "timestamp", None)
                if isinstance(event_ts, (int, float)) and event_ts > 0:
                    self._last_event_at = datetime.fromtimestamp(event_ts, tz=timezone.utc)
                else:
                    self._last_event_at = datetime.now(timezone.utc)

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

        if last_position:
            self.resume_token = last_position

        if inserts or updates or deletes or last_position:
            yield ChangeBatch(
                inserts=inserts,
                updates=updates,
                deletes=deletes,
                resume_token=last_position,
            )
