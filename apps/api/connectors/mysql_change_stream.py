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
        table: str | list[str],
        primary_key: str,
        columns: list[str] | None = None,
        resume_token: dict[str, Any] | str | None = None,
        batch_size: int = 1000,
        max_wait_seconds: float = 30.0,
        cursor_key: str = "",
        primary_keys: dict[str, str] | None = None,
    ) -> None:
        from services.cdc_multi_table import normalize_table_list

        self.cfg = cfg
        self.database = cfg.get("database") or cfg.get("schema") or ""
        self.tables = normalize_table_list(table)
        if not self.tables:
            raise ValueError("MySQL CDC requires at least one table")
        self.table = self.tables[0]
        self.primary_keys = {
            t: str((primary_keys or {}).get(t) or primary_key or "id")
            for t in self.tables
        }
        self.primary_key = self.primary_keys[self.table]
        self.columns = columns
        self.batch_size = batch_size
        self.max_wait_seconds = max_wait_seconds
        self.cursor_key = cursor_key or (
            f"mysql:{self.database}:{','.join(self.tables)}"
            if len(self.tables) > 1
            else f"mysql:{self.database}:{self.table}"
        )
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
        self._processed_signal_ids: set[str] = set()
        self.signal_table = str(cfg.get("signal_table") or "dataflow_signal")
        self._signal_table_ready = False
        self._last_signal_poll_at = 0.0
        try:
            import os as _os

            self._signal_poll_interval_sec = float(
                _os.getenv(
                    "DATAFLOW_CDC_SIGNAL_POLL_SEC",
                    str(cfg.get("signal_poll_interval_sec") or 15),
                )
            )
        except Exception:
            self._signal_poll_interval_sec = 15.0
        if isinstance(resume_token, str) and resume_token:
            try:
                self.resume_token = json.loads(resume_token)
            except Exception:
                self.resume_token = None
        else:
            self.resume_token = resume_token or None
        from services.cdc_lease import CdcLeaseGuard

        self._lease = CdcLeaseGuard(
            cursor_key=self.cursor_key,
            resource=f"mysql_server_id:{self._mysql_server_id()}",
            holder_id=str(cfg.get("lease_holder_id") or ""),
            job_id=str(cfg.get("job_id") or ""),
            meta={
                "tables": list(self.tables),
                "database": self.database,
                "engine": "mysql",
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

    def _mysql_server_id(self) -> int:
        configured = self.cfg.get("server_id") or self.cfg.get("binlog_server_id")
        if configured is not None:
            return int(configured)
        import hashlib

        # Shared multi-table readers must use one server_id for the whole set
        # (Debezium-class); per-table hashing would open N concurrent consumers.
        table_key = ",".join(sorted(t.lower() for t in self.tables))
        digest = hashlib.sha1(  # noqa: S324
            f"{self.cfg.get('host')}|{self.database}|{table_key}|{self.cursor_key}".encode(),
            usedforsecurity=False,
        ).hexdigest()
        return 10_000 + (int(digest[:6], 16) % 1_000_000)

    def _acquire_cdc_lease(self) -> None:
        self._lease.ensure()

    def close(self) -> None:
        self._lease.release()

    def is_available(self) -> bool:
        """True when binlog is ON + ROW format and pymysqlreplication is importable.

        Stream open is best-effort: missing REPLICATION privileges still return
        True when server vars are correct so CI/integration can proceed; poll()
        surfaces privilege errors clearly.
        """
        try:
            import pymysqlreplication  # noqa: F401
        except ImportError:
            return False
        try:
            conn = self._conn()
            with conn.cursor() as cur:
                cur.execute("SHOW VARIABLES LIKE 'log_bin'")
                row = cur.fetchone()
                if not row or str(row[1]).lower() not in {"on", "1", "true"}:
                    conn.close()
                    return False
                cur.execute("SHOW VARIABLES LIKE 'binlog_format'")
                row = cur.fetchone()
                if not row or (row[1] or "").upper() != "ROW":
                    conn.close()
                    return False
            conn.close()

            try:
                kwargs = self._binlog_kwargs(blocking=False, only_events=[])
                from pymysqlreplication import BinLogStreamReader

                stream = BinLogStreamReader(**kwargs)
                stream.close()
            except Exception:
                # Vars OK — treat as available; poll will raise with detail.
                pass
            return True
        except Exception:
            return False

    def _binlog_kwargs(self, blocking: bool, only_events: list[type]) -> dict[str, Any]:
        # Unique server_id per connector/table so multi-stream CDC does not collide.
        server_id = self._mysql_server_id()
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
            # pymysqlreplication accepts a list for multi-table single-reader.
            "only_tables": list(self.tables) if len(self.tables) > 1 else (self.table or None),
        }
        # An empty ``only_events`` list is an allowlist matching NOTHING (the
        # reader would silently yield zero events). Only set it when non-empty;
        # otherwise leave it unset so BinLogStreamReader streams all events.
        if only_events:
            kwargs["only_events"] = only_events
        # Prefer GTID auto-position (Debezium-class); fall back to file/pos.
        # pymysqlreplication: ``auto_position`` accepts the GTID set string directly
        # (older builds have no separate ``gtid_set`` kwarg).
        gtid = None
        if isinstance(self.resume_token, dict):
            gtid = self.resume_token.get("gtid") or self.resume_token.get("gtid_set")
        if gtid:
            kwargs["auto_position"] = gtid
        elif (
            isinstance(self.resume_token, dict)
            and self.resume_token.get("file")
            and self.resume_token.get("pos") is not None
        ):
            kwargs["log_file"] = self.resume_token["file"]
            kwargs["log_pos"] = self.resume_token["pos"]
        return kwargs

    def snapshot(self) -> Iterator[ChangeBatch]:
        # Capture binlog file/pos BEFORE the snapshot so poll() starts from a
        # consistent handoff point (at-least-once; duplicates possible, no gaps).
        self._acquire_cdc_lease()
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

        conn = None
        try:
            conn = self._conn()
            if not self._signal_table_ready:
                ensure_signal_table(conn, table=self.signal_table, dialect="mysql")
                self._signal_table_ready = True
            _, self._processed_signal_ids = poll_signal_table(
                conn,
                source_key=self.source_key,
                table=self.signal_table,
                default_table=self.table,
                primary_key=self.primary_key,
                processed_ids=self._processed_signal_ids,
                dialect="mysql",
            )
            self._last_signal_poll_at = now
        except Exception as exc:
            _logger.debug("MySQL CDC signal table poll skipped: %s", exc)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

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
        for table in self.tables:
            pattern = re.compile(
                rf"(?:`?{re.escape(self.database)}`?\.)?`?{re.escape(table)}`?\b",
                re.IGNORECASE,
            )
            if pattern.search(query):
                return True
        return False

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

    def _pk_value(self, row: dict[str, Any], *, table: str | None = None) -> str:
        row = self._remap_positional(row)
        pk = self.primary_keys.get(table or self.table, self.primary_key)
        return _serialize(row.get(pk))

    def _table_allowed(self, table: str) -> bool:
        wanted = {t.lower() for t in self.tables}
        return (table or "").lower() in wanted

    def _canonical_table(self, table: str) -> str:
        by_lower = {t.lower(): t for t in self.tables}
        return by_lower.get((table or "").lower(), table or self.table)

    def _fetch_incremental_chunk(self, sig: Any) -> tuple[list[dict[str, Any]], str | None, bool]:
        """PK-ordered chunk reader for Debezium-style incremental snapshots."""
        from connectors.sql_identifiers import quote_sql_identifier, require_safe_identifier

        pk_name = sig.primary_key or self.primary_key
        pk = quote_sql_identifier(require_safe_identifier(pk_name, preserve_case=True))
        table = quote_sql_identifier(require_safe_identifier(self.table, preserve_case=True))
        db = quote_sql_identifier(require_safe_identifier(self.database, preserve_case=True)) if self.database else ""
        qualified = f"{db}.{table}" if db else table
        limit = int(sig.chunk_size or self.batch_size)
        last_pk = sig.last_pk or ""
        conn = self._conn()
        try:
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
        finally:
            conn.close()
        records = [
            {cols[i]: "" if row[i] is None else str(row[i]) for i in range(len(cols))}
            for row in rows
        ]
        new_last = records[-1].get(pk_name) if records else last_pk
        done = len(records) < limit
        return records, str(new_last) if new_last is not None else last_pk, done

    def _peek_stream_events_during_chunk(self, sig: Any) -> list[dict[str, Any]]:
        """Non-acking binlog peek for DDD-3 stream-wins during incremental snapshot."""
        events: list[dict[str, Any]] = []
        try:
            from pymysqlreplication import BinLogStreamReader
            from pymysqlreplication.row_event import (
                DeleteRowsEvent,
                UpdateRowsEvent,
                WriteRowsEvent,
            )
        except ImportError:
            return []
        kwargs = self._binlog_kwargs(
            blocking=False,
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
        )
        # Distinct server_id so peek does not collide with the durable poll session.
        kwargs["server_id"] = int(kwargs.get("server_id") or 10_000) + 7
        # Bound peek — do not advance durable resume_token.
        peek_limit = min(int(sig.chunk_size or self.batch_size), 200)
        stream = BinLogStreamReader(**kwargs)
        try:
            count = 0
            for binlog_event in stream:
                if getattr(binlog_event, "schema", "") != self.database:
                    continue
                if not self._table_allowed(getattr(binlog_event, "table", "") or ""):
                    continue
                if isinstance(binlog_event, WriteRowsEvent):
                    for row in getattr(binlog_event, "rows", []):
                        values = (
                            row.get("values")
                            if isinstance(row, dict) and "values" in row
                            else row
                        )
                        events.append({"op": "c", "row": self._row_to_record(values)})
                        count += 1
                elif isinstance(binlog_event, UpdateRowsEvent):
                    for row in getattr(binlog_event, "rows", []):
                        after = row.get("after_values") if isinstance(row, dict) else getattr(row, "after_values", {})
                        events.append({"op": "u", "row": self._row_to_record(after)})
                        count += 1
                elif isinstance(binlog_event, DeleteRowsEvent):
                    for row in getattr(binlog_event, "rows", []):
                        values = row.get("values") if isinstance(row, dict) else getattr(row, "values", {})
                        pk = self._pk_value(values)
                        if pk:
                            events.append({"op": "d", "pk": pk, "row": {self.primary_key: pk}})
                            count += 1
                if count >= peek_limit:
                    break
        except Exception:
            return events
        finally:
            try:
                stream.close()
            except Exception:
                pass
        return events

    def poll(self) -> Iterator[ChangeBatch]:
        # Incomplete initial sync must finish before binlog streaming.
        if isinstance(self.resume_token, dict) and self.resume_token.get("phase") == "snapshot":
            yield from self.snapshot()
            return

        self._acquire_cdc_lease()
        self._poll_signal_table()

        # Signal-driven incremental snapshot (DDD-3 window via shared runner).
        from services.cdc_incremental_runner import interleave_incremental_snapshot

        yield from interleave_incremental_snapshot(
            self.source_key,
            table=self.table,
            fetch_chunk=self._fetch_incremental_chunk,
            stream_events_during_chunk=self._peek_stream_events_during_chunk,
            max_chunks_per_poll=1,
        )

        from pymysqlreplication import BinLogStreamReader
        from pymysqlreplication.event import QueryEvent, RotateEvent, XidEvent
        from pymysqlreplication.row_event import (
            DeleteRowsEvent,
            UpdateRowsEvent,
            WriteRowsEvent,
        )
        from services.cdc_multi_table import MultiTableTransactionBuffer

        self._ensure_decode_schema(resume_offset=self.resume_token)
        self.heartbeat()

        last_position: dict[str, Any] | None = None
        deadline = datetime.now(timezone.utc).timestamp() + self.max_wait_seconds
        buf = MultiTableTransactionBuffer()
        emitted = False
        event_count = 0

        def _pos_now() -> dict[str, Any]:
            pos = {
                "file": getattr(stream, "log_file", "") or (self.resume_token or {}).get("file"),
                "pos": getattr(stream, "log_pos", None),
                "tables": list(self.tables),
            }
            return {k: v for k, v in pos.items() if v is not None}

        def _token_at(pos: dict[str, Any] | None) -> dict[str, Any]:
            token = dict(pos or {"tables": list(self.tables)})
            try:
                current = self._current_binlog_position() or {}
                if current.get("gtid"):
                    token["gtid"] = current["gtid"]
                if not token.get("file") and current.get("file"):
                    token["file"] = current["file"]
                    token["pos"] = current.get("pos")
            except Exception:
                pass
            return token

        def _emit_commit():
            nonlocal emitted
            for batch in buf.commit(
                resume_token=_token_at(last_position),
                table_order=self.tables,
            ):
                emitted = True
                self.resume_token = batch.resume_token
                yield batch

        # Row changes + rotation + QueryEvent (DDL/BEGIN) + XidEvent (COMMIT).
        kwargs = self._binlog_kwargs(
            blocking=False,
            only_events=[
                RotateEvent,
                QueryEvent,
                XidEvent,
                WriteRowsEvent,
                UpdateRowsEvent,
                DeleteRowsEvent,
            ],
        )
        stream = BinLogStreamReader(**kwargs)
        try:
            for binlog_event in stream:
                if datetime.now(timezone.utc).timestamp() > deadline:
                    break
                if isinstance(binlog_event, RotateEvent):
                    last_position = {
                        "file": binlog_event.next_binlog,
                        "pos": binlog_event.position,
                        "tables": list(self.tables),
                    }
                    continue
                if isinstance(binlog_event, QueryEvent):
                    query = (getattr(binlog_event, "query", "") or "").strip()
                    upper = query.upper()
                    if upper.startswith("BEGIN") or upper == "BEGIN":
                        buf.begin(lsn=str(getattr(stream, "log_pos", "") or ""))
                        continue
                    if upper.startswith("ROLLBACK") or upper.startswith("ABORT"):
                        buf.rollback()
                        continue
                    if self._ddl_targets_table(query):
                        pos = _pos_now()
                        if stream.log_pos:
                            pos["pos"] = stream.log_pos
                        self._record_schema_change(ddl=query.strip()[:2000], offset=pos)
                        self._last_event_at = datetime.now(timezone.utc)
                    continue
                if isinstance(binlog_event, XidEvent):
                    if stream.log_pos:
                        last_position = {
                            "file": getattr(stream, "log_file", ""),
                            "pos": stream.log_pos,
                            "tables": list(self.tables),
                        }
                    yield from _emit_commit()
                    continue
                if getattr(binlog_event, "schema", "") != self.database:
                    continue
                event_table = getattr(binlog_event, "table", "") or ""
                if not self._table_allowed(event_table):
                    continue
                tbl = self._canonical_table(event_table)

                event_ts = getattr(binlog_event, "timestamp", None)
                if isinstance(event_ts, (int, float)) and event_ts > 0:
                    self._last_event_at = datetime.fromtimestamp(event_ts, tz=timezone.utc)
                else:
                    self._last_event_at = datetime.now(timezone.utc)

                if isinstance(binlog_event, WriteRowsEvent):
                    for row in getattr(binlog_event, "rows", []):
                        values = (
                            row.get("values")
                            if isinstance(row, dict) and "values" in row
                            else row
                        )
                        buf.insert(tbl, self._row_to_record(values), lsn=str(stream.log_pos or ""))
                        event_count += 1
                elif isinstance(binlog_event, UpdateRowsEvent):
                    for row in getattr(binlog_event, "rows", []):
                        after = (
                            row.get("after_values")
                            if isinstance(row, dict)
                            else getattr(row, "after_values", {})
                        )
                        buf.update(tbl, self._row_to_record(after), lsn=str(stream.log_pos or ""))
                        event_count += 1
                elif isinstance(binlog_event, DeleteRowsEvent):
                    for row in getattr(binlog_event, "rows", []):
                        values = (
                            row.get("values")
                            if isinstance(row, dict)
                            else getattr(row, "values", {})
                        )
                        pk = self._pk_value(values, table=tbl)
                        if pk:
                            buf.delete(tbl, pk, lsn=str(stream.log_pos or ""))
                            event_count += 1

                if stream.log_pos:
                    last_position = {
                        "file": getattr(stream, "log_file", ""),
                        "pos": stream.log_pos,
                        "tables": list(self.tables),
                    }

                if event_count >= self.batch_size and buf.open_xid is None:
                    break
        finally:
            stream.close()

        # Mid-window open txn: hold (do not flush). Next poll re-reads from resume.
        if buf.open_xid is not None:
            if not emitted:
                yield ChangeBatch(
                    resume_token={
                        "txn_held": True,
                        "open_xid": buf.open_xid,
                        "token": _token_at(last_position or (self.resume_token if isinstance(self.resume_token, dict) else None)),
                    }
                )
            return

        token = _token_at(last_position)
        if token.get("file") or token.get("gtid") or token.get("pos") is not None:
            self.resume_token = token
        if not emitted and (token.get("file") or token.get("gtid") or token.get("pos") is not None):
            yield ChangeBatch(resume_token=token)
