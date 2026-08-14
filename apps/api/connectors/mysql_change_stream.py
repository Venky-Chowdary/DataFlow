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

from services.brand_env import getenv_brand
from services.cdc_engine import ChangeBatch
from services.cdc_schema_history import (
    connection_fingerprint,
    last_ddl_at,
    rebuild_schema,
    record_ddl,
)

from connectors.mysql_conn import get_connection
from connectors.mysql_reader import read_table_batch
from connectors.sql_identifiers import quote_table_ref
from services.cdc_cursor_gap import CdcBinlogGapError

_DDL_RE = re.compile(
    r"\b(ALTER|CREATE|DROP|RENAME)\s+TABLE\b",
    re.IGNORECASE,
)

_logger = logging.getLogger(__name__)


def _serialize(value: Any) -> str:
    from services.value_serializer import SQL_NULL_SENTINEL, cell_to_string

    if value is None:
        return SQL_NULL_SENTINEL
    if isinstance(value, datetime):
        return value.isoformat()
    return cell_to_string(value, preserve_sql_null=True)


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
        from services.cdc_identity import require_cdc_primary_keys_map

        self.primary_keys = require_cdc_primary_keys_map(
            self.tables, primary_key=primary_key, primary_keys=primary_keys
        )
        self.primary_key = self.primary_keys[self.table]
        self.columns = columns
        self.batch_size = batch_size
        self.max_wait_seconds = max_wait_seconds
        self.cursor_key = cursor_key or (
            f"mysql:{self.database}:{','.join(self.tables)}"
            if len(self.tables) > 1
            else f"mysql:{self.database}:{self.table}"
        )
        # Per-table ordinal → name map. A single shared list keyed on tables[0]
        # remapped every captured table's positional UNKNOWN_COLn keys into
        # the wrong columns, so deletes removed the wrong destination rows.
        self._column_names_cache: dict[str, list[str]] = {}
        self.source_key = connection_fingerprint(
            {**cfg, "type": "mysql"},
            connector_id=str(cfg.get("connector_id") or ""),
        )
        self.decode_schema: dict[str, Any] = {}
        self.last_ddl_at: str | None = None
        self._last_event_at: datetime | None = None
        self._last_event_commit_at: datetime | None = None
        self._last_heartbeat_at: datetime | None = None
        self._lag_observation: dict | None = None
        self._schema_ready = False
        self._processed_signal_ids: set[str] = set()
        self.signal_table = str(cfg.get("signal_table") or "dataflow_signal")
        self._signal_table_ready = False
        self._last_signal_poll_at = 0.0
        try:
            import os as _os

            self._signal_poll_interval_sec = float(
                getenv_brand(
                    "CDC_SIGNAL_POLL_SEC",
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
        self._binlog_catalog_cache: dict[str, Any] | None = None
        self._binlog_catalog_cache_at: float = 0.0

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
                # Debezium-class: MINIMAL/PARTIAL after-images omit unchanged cols
                # and would NULL-wipe on SQL upsert — refuse until FULL.
                cur.execute("SHOW VARIABLES LIKE 'binlog_row_image'")
                row = cur.fetchone()
                if row and (row[1] or "").upper() not in {"FULL", ""}:
                    conn.close()
                    return False
            conn.close()

            try:
                kwargs = self._binlog_kwargs(blocking=False, only_events=[])
                from pymysqlreplication import BinLogStreamReader

                stream = BinLogStreamReader(**kwargs)
                stream.close()
            except Exception as exc:
                # Vars OK — treat as available; poll will raise with detail.
                _logger.warning("Exception suppressed: %s", exc, exc_info=exc)
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
            "resume_stream": True,
            "blocking": blocking,
            # Do not filter at the pymysqlreplication packet level. We filter
            # by schema/table below; packet-level filtering can drop TableMap
            # events needed to decode WriteRows events after a RotateEvent.
            "only_schemas": None,
            "only_tables": None,
        }
        # An empty ``only_events`` list is an allowlist matching NOTHING (the
        # reader would silently yield zero events). Only set it when non-empty;
        # otherwise leave it unset so BinLogStreamReader streams all events.
        if only_events:
            kwargs["only_events"] = only_events
        # Prefer file/pos resume when present; mixing GTID claims with file/pos
        # causes the next BinLogStreamReader to skip events that were committed
        # while the (non-blocking) window was open. GTID auto-position is used
        # only when the token is explicitly GTID-based with no file/pos.
        token = self.resume_token if isinstance(self.resume_token, dict) else {}
        has_file_pos = bool(token.get("file") and token.get("pos") is not None)
        if has_file_pos:
            kwargs["log_file"] = token["file"]
            kwargs["log_pos"] = token["pos"]
        elif token.get("gtid") or token.get("gtid_set"):
            kwargs["auto_position"] = token.get("gtid") or token.get("gtid_set")
        return kwargs

    def snapshot(self) -> Iterator[ChangeBatch]:
        # Use a locked MySQL session for a consistent, gap-free snapshot handoff.
        # LOCK TABLES blocks writers, SHOW MASTER STATUS captures the exact binlog
        # position for the read view, then we read all tables on the same session.
        # Poll() resumes after that position, so concurrent writes are delivered
        # at-least-once; duplicates are acceptable, gaps are not.
        self._acquire_cdc_lease()
        self._ensure_decode_schema(resume_offset=self.resume_token)
        self.heartbeat()

        lock_conn = None
        locked = False
        start_pos: dict[str, Any] = {
            "table": self.table,
            "tables": list(self.tables),
        }
        try:
            lock_conn = get_connection(
                host=self.cfg.get("host") or "localhost",
                port=self.cfg.get("port") or 3306,
                database=self.database,
                username=self.cfg.get("username") or "",
                password=self.cfg.get("password") or "",
                connection_string=self.cfg.get("connection_string") or "",
                ssl=bool(self.cfg.get("ssl")),
            )
            # Use a global read lock when we can (Debezium-style snapshot); it
            # freezes the binlog so the read view has no backlog to catch up after
            # the lock is released.  Falls back to per-table READ locks.
            lock_conn.autocommit(True)

            try:
                with lock_conn.cursor() as cur:
                    cur.execute("FLUSH TABLES WITH READ LOCK")
                locked = True
            except Exception as exc:
                _logger.warning(
                    "FLUSH TABLES WITH READ LOCK unavailable (%s); falling back to per-table LOCK TABLES", exc
                )
            if not locked:
                table_refs = [quote_table_ref(t, dialect="mysql") for t in self.tables]
                lock_clause = ", ".join(f"{ref} READ" for ref in table_refs)
                try:
                    with lock_conn.cursor() as cur:
                        cur.execute(f"LOCK TABLES {lock_clause}")
                    locked = True
                except Exception as exc:
                    _logger.warning(
                        "Could not LOCK TABLES for consistent CDC snapshot: %s", exc
                    )

            if locked:
                with lock_conn.cursor() as cur:
                    # Capture GTID on the same locked session (Debezium-class
                    # handoff). File/pos alone is weaker when binlogs rotate or
                    # poll prefers auto_position — at-least-once upserts still apply.
                    gtid = self._current_gtid_executed(cur)
                    for sql in ("SHOW MASTER STATUS", "SHOW BINARY LOG STATUS"):
                        try:
                            cur.execute(sql)
                            row = cur.fetchone()
                            if row:
                                start_pos = {
                                    "file": row[0],
                                    "pos": int(row[1]),
                                    "table": self.table,
                                    "tables": list(self.tables),
                                }
                                if gtid:
                                    start_pos["gtid"] = gtid
                                break
                        except Exception as exc:
                            _logger.debug("CDC binlog status query failed: %s", exc)
                    if gtid and not start_pos.get("gtid"):
                        start_pos["gtid"] = gtid
        except Exception as exc:
            _logger.warning(
                "Could not acquire MySQL lock connection for CDC snapshot: %s", exc
            )
            locked = False
            lock_conn = None

        if not start_pos.get("file"):
            # Fallback when binlog position cannot be captured while locked.
            start_pos = self._current_binlog_position() or start_pos

        # Resume mid-snapshot from the recorded table/offset.
        offset = 0
        resume_table = self.table
        if (
            isinstance(self.resume_token, dict)
            and self.resume_token.get("phase") == "snapshot"
        ):
            offset = int(self.resume_token.get("offset") or 0)
            resume_table = self.resume_token.get("table") or self.table

        if resume_table in self.tables:
            tables_to_snapshot = self.tables[self.tables.index(resume_table) :]
        else:
            tables_to_snapshot = list(self.tables)

        try:
            for table in tables_to_snapshot:
                table_offset = offset if table == resume_table else 0
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
                        table=table,
                        columns=self.columns,
                        offset=table_offset,
                        limit=self.batch_size,
                        conn=lock_conn if locked else None,
                    )
                    if not batch.rows:
                        break
                    records = [dict(zip(batch.headers, row)) for row in batch.rows]
                    table_offset += len(batch.rows)
                    yield ChangeBatch(
                        inserts=records,
                        resume_token={
                            **start_pos,
                            "phase": "snapshot",
                            "offset": table_offset,
                            "table": table,
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
            # Adopt the captured consistent point as the live resume. Poll after
            # a when_needed gap recovery must stream from this tip, not the
            # purged file:pos the adapter was constructed with.
            self.resume_token = {
                **start_pos,
                "phase": "streaming",
                "offset": 0,
                "table": self.table,
            }
        finally:
            if lock_conn:
                if locked:
                    try:
                        with lock_conn.cursor() as cur:
                            cur.execute("UNLOCK TABLES")
                    except Exception as exc:
                        _logger.warning("UNLOCK TABLES failed: %s", exc)
                try:
                    lock_conn.close()
                except Exception as exc:
                    _logger.debug("Error closing MySQL lock connection: %s", exc)

    def _binlog_file_pos_on(self, cur) -> str | None:
        """Return ``file:pos`` from the open cursor for snapshot row stamps.

        Must stay in the same LSN family as streaming events (which stamp from
        ``stream.log_file`` / ``stream.log_pos``). GTID is captured separately
        for the peek filter and must not become the row stamp.
        """
        from connectors.writer_common import _format_file_pos_lsn

        for sql in ("SHOW MASTER STATUS", "SHOW BINARY LOG STATUS"):
            try:
                cur.execute(sql)
                row = cur.fetchone()
                if row and row[0] is not None and row[1] is not None:
                    return _format_file_pos_lsn(str(row[0]), row[1])
            except Exception as exc:
                _logger.debug("CDC binlog status query failed: %s", exc)
        return None

    def _current_gtid_executed(self, cur) -> str | None:
        try:
            cur.execute("SELECT @@GLOBAL.gtid_executed")
            row = cur.fetchone()
            if row and row[0]:
                return str(row[0])
        except Exception as exc:
            _logger.warning("Exception suppressed: %s", exc, exc_info=exc)
        try:
            cur.execute("SHOW GLOBAL VARIABLES LIKE 'gtid_executed'")
            row = cur.fetchone()
            if row and len(row) > 1 and row[1]:
                return str(row[1])
        except Exception as exc:
            _logger.warning("Exception suppressed: %s", exc, exc_info=exc)
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
                        except (ValueError, IndexError) as exc:
                            _logger.warning("Could not parse MySQL binlog position row: %s", exc)
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
        """Proven CDC lag seconds — never heartbeat age (Debezium-class honesty)."""
        from services.cdc_lag_honesty import observe_cdc_lag

        obs = observe_cdc_lag(
            last_event_commit_at=self._last_event_commit_at,
            last_heartbeat_at=self._last_heartbeat_at,
            replication_lag_bytes=self.replication_lag_bytes(),
        )
        self._lag_observation = obs
        return obs.get("cdc_lag_seconds")

    def heartbeat(self) -> None:
        self._last_heartbeat_at = datetime.now(timezone.utc)

    def _binlog_catalog_status(self, *, max_age_sec: float = 2.0) -> dict[str, Any]:
        """Live binlog catalog proof for Theater / Freshness (PG slot parity).

        Returns ``slot_exists`` (log_bin), ``active`` (lease), ``restart_lsn``
        (oldest retained file:pos), ``confirmed_flush_lsn`` (resume file:pos),
        ``wal_status`` ∈ {reserved, unreserved, lost}, expire vars, gtid_purged.
        """
        import time as _time

        now = _time.monotonic()
        if (
            self._binlog_catalog_cache is not None
            and (now - float(self._binlog_catalog_cache_at or 0.0)) < max(0.25, float(max_age_sec))
        ):
            return dict(self._binlog_catalog_cache)

        out: dict[str, Any] = {
            "plugin": "mysql-binlog",
            "slot_exists": False,
            "active": bool(getattr(self._lease, "acquired", False)),
            "restart_lsn": None,
            "confirmed_flush_lsn": None,
            "wal_status": None,
            "binary_logs": [],
            "oldest_file": None,
            "current_file": None,
            "current_pos": None,
            "gtid_purged": "",
            "gtid_executed": "",
            "binlog_expire_logs_seconds": None,
            "expire_logs_days": None,
            "server_id": self._mysql_server_id(),
            "log_bin": False,
            "binlog_format": None,
            "binlog_row_image": None,
        }
        token = self.resume_token if isinstance(self.resume_token, dict) else {}
        resume_file = str(token.get("file") or "").strip()
        resume_pos = token.get("pos")
        resume_gtid = str(token.get("gtid") or "").strip()
        if resume_file:
            out["confirmed_flush_lsn"] = (
                f"{resume_file}:{resume_pos}" if resume_pos is not None else resume_file
            )
        elif resume_gtid:
            out["confirmed_flush_lsn"] = resume_gtid[:120]

        try:
            conn = self._conn()
            try:
                with conn.cursor() as cur:
                    def _var(name: str) -> str | None:
                        try:
                            cur.execute(f"SHOW VARIABLES LIKE '{name}'")
                            row = cur.fetchone()
                            if row and len(row) > 1 and row[1] is not None:
                                return str(row[1])
                        except Exception:
                            return None
                        return None

                    log_bin = (_var("log_bin") or "").lower()
                    out["log_bin"] = log_bin in {"on", "1", "true"}
                    out["slot_exists"] = bool(out["log_bin"])
                    out["binlog_format"] = _var("binlog_format")
                    out["binlog_row_image"] = _var("binlog_row_image")
                    expire_sec = _var("binlog_expire_logs_seconds")
                    if expire_sec is not None:
                        try:
                            out["binlog_expire_logs_seconds"] = int(expire_sec)
                        except (TypeError, ValueError):
                            out["binlog_expire_logs_seconds"] = expire_sec
                    expire_days = _var("expire_logs_days")
                    if expire_days is not None:
                        try:
                            out["expire_logs_days"] = int(float(expire_days))
                        except (TypeError, ValueError):
                            out["expire_logs_days"] = expire_days

                    logs: list[str] = []
                    try:
                        cur.execute("SHOW BINARY LOGS")
                        for row in cur.fetchall() or []:
                            if row and row[0]:
                                logs.append(str(row[0]))
                    except Exception as exc:
                        out["probe_error"] = f"SHOW BINARY LOGS: {exc}"[:200]
                    out["binary_logs"] = logs
                    if logs:
                        out["oldest_file"] = logs[0]
                        out["restart_lsn"] = f"{logs[0]}:4"

                    for sql in ("SHOW MASTER STATUS", "SHOW BINARY LOG STATUS"):
                        try:
                            cur.execute(sql)
                            row = cur.fetchone()
                            if row:
                                out["current_file"] = str(row[0]) if row[0] else None
                                try:
                                    out["current_pos"] = int(row[1])
                                except (TypeError, ValueError, IndexError):
                                    out["current_pos"] = row[1] if len(row) > 1 else None
                                break
                        except Exception:
                            continue

                    try:
                        cur.execute("SELECT @@GLOBAL.gtid_purged")
                        row = cur.fetchone()
                        if row and row[0]:
                            out["gtid_purged"] = str(row[0])
                    except Exception:
                        pass
                    try:
                        cur.execute("SELECT @@GLOBAL.gtid_executed")
                        row = cur.fetchone()
                        if row and row[0]:
                            out["gtid_executed"] = str(row[0])
                    except Exception:
                        pass

                    gtid_in_purged: bool | None = None
                    if resume_gtid and out.get("gtid_purged"):
                        try:
                            cur.execute(
                                "SELECT GTID_SUBSET(%s, @@GLOBAL.gtid_purged)",
                                (resume_gtid,),
                            )
                            row = cur.fetchone()
                            if row is not None and row[0] is not None:
                                gtid_in_purged = bool(int(row[0]))
                        except Exception:
                            gtid_in_purged = None
                    out["gtid_in_purged"] = gtid_in_purged

                from services.cdc_retention_probe import classify_binlog_retention

                retention = classify_binlog_retention(
                    resume_file,
                    resume_pos,
                    logs,
                    resume_gtid=resume_gtid,
                    gtid_purged=str(out.get("gtid_purged") or ""),
                    gtid_in_purged=gtid_in_purged,
                    cursor_key=self.cursor_key,
                )
                out["retention_status"] = retention.status
                if retention.status == "gap":
                    out["wal_status"] = "lost"
                elif retention.status == "at_risk":
                    out["wal_status"] = "unreserved"
                elif retention.status == "ok":
                    out["wal_status"] = "reserved"
                elif out["slot_exists"]:
                    out["wal_status"] = "reserved"
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception as exc:
            _logger.debug("binlog catalog probe failed: %s", exc)
            out["probe_error"] = str(exc)[:200]

        out["active"] = bool(getattr(self._lease, "acquired", False))
        self._binlog_catalog_cache = dict(out)
        self._binlog_catalog_cache_at = now
        return out

    def cdc_metadata(self) -> dict[str, Any]:
        """Operator-visible MySQL CDC status for Job Theater / Validate."""
        lag_sec = self.replication_lag_seconds()
        obs = dict(self._lag_observation or {})
        catalog = self._binlog_catalog_status()
        lease_fields: dict[str, Any] = {}
        try:
            lease_fields = dict(self._lease.theater_fields() or {})
        except Exception:
            lease_fields = {}
        phase = "snapshot"
        if isinstance(self.resume_token, dict) and self.resume_token.get("phase") == "snapshot":
            phase = "snapshot"
        elif self.resume_token:
            phase = "streaming"
        return {
            "plugin": "mysql-binlog",
            "slot_name": f"server_id:{catalog.get('server_id') or self._mysql_server_id()}",
            "phase": phase,
            "replication_lag_bytes": obs.get(
                "replication_lag_bytes", self.replication_lag_bytes()
            ),
            "replication_lag_seconds": lag_sec,
            "cdc_lag_basis": obs.get("cdc_lag_basis"),
            "cdc_heartbeat_age_sec": obs.get("cdc_heartbeat_age_sec"),
            "freshness_severity": obs.get("freshness_severity"),
            "active": catalog.get("active"),
            "slot_exists": catalog.get("slot_exists"),
            "restart_lsn": catalog.get("restart_lsn"),
            "confirmed_flush_lsn": catalog.get("confirmed_flush_lsn"),
            "wal_status": catalog.get("wal_status"),
            "binlog_expire_logs_seconds": catalog.get("binlog_expire_logs_seconds"),
            "gtid_purged": catalog.get("gtid_purged"),
            "retention_status": catalog.get("retention_status"),
            "delivery": "at-least-once",
            **lease_fields,
        }

    def _assert_resume_within_retention(self) -> None:
        """Fail-closed when resume file/GTID is before retained binary logs."""
        if isinstance(self.resume_token, dict) and self.resume_token.get("phase") == "snapshot":
            return
        token = self.resume_token if isinstance(self.resume_token, dict) else {}
        resume_file = str(token.get("file") or "").strip()
        resume_gtid = str(token.get("gtid") or "").strip()
        if not resume_file and not resume_gtid:
            return
        catalog = self._binlog_catalog_status(max_age_sec=0)
        if catalog.get("retention_status") != "gap" and catalog.get("wal_status") != "lost":
            return
        raise CdcBinlogGapError(
            (
                f"MySQL CDC resume is before retained binary logs "
                f"(resume={catalog.get('confirmed_flush_lsn') or resume_file or resume_gtid}, "
                f"oldest={catalog.get('oldest_file') or catalog.get('gtid_purged') or '?'}). "
                "Reset watermark and re-snapshot — continuous CDC across the gap is not claimed."
            ),
            resume_file=resume_file,
            resume_pos=token.get("pos") if resume_file else "",
            oldest_file=str(catalog.get("oldest_file") or ""),
            resume_gtid=resume_gtid,
            gtid_purged=str(catalog.get("gtid_purged") or ""),
            cursor_key=self.cursor_key,
        )

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
                except Exception as exc:
                    _logger.warning("Exception suppressed: %s", exc, exc_info=exc)

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
                self._column_names_cache[self.table] = cols
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
                self._column_names_cache[self.table] = list(live["columns"].keys())
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
        # Only invalidate the table whose schema we just refreshed. Wiping the
        # whole cache would force every sibling table back through an ordinal
        # lookup against tables[0]'s columns.
        self._column_names_cache[self.table] = list(live["columns"].keys())
        self.last_ddl_at = str(entry.get("recorded_at") or "") or self.last_ddl_at
        try:
            from services.cdc_mapping_review import flag_mapping_review

            flag_mapping_review(
                source_key=self.source_key,
                table=self.table,
                reason="cdc_schema_drift",
                schema_version=int(entry.get("version") or 0) or None,
                ddl=str(entry.get("ddl") or ""),
                column_names=list(live["columns"].keys()),
            )
        except Exception:
            _logger.exception("Failed to flag CDC mapping review after MySQL schema drift")
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

    def _ordered_columns(self, table: str | None = None) -> list[str]:
        """Ordered column names from information_schema for one captured table.

        MySQL 8.0+/9.x only embed column names in the binlog when
        ``binlog_row_metadata=FULL``; with the default MINIMAL the reader yields
        positional ``UNKNOWN_COL0..N`` keys. Resolving names by ordinal here keeps
        CDC correct regardless of the server's metadata setting — but the lookup
        **must** be per-table. A shared cache keyed on ``tables[0]`` remapped
        every sibling table's positional keys into the wrong columns.
        """
        tbl = table or self.table
        cached = self._column_names_cache.get(tbl)
        if cached is not None:
            return cached
        cols: list[str] = []
        try:
            conn = self._conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COLUMN_NAME FROM information_schema.columns "
                        "WHERE table_schema = %s AND table_name = %s "
                        "ORDER BY ORDINAL_POSITION",
                        (self.database, tbl),
                    )
                    cols = [str(r[0]) for r in cur.fetchall()]
            finally:
                conn.close()
        except Exception:
            cols = []
        self._column_names_cache[tbl] = cols
        return cols

    def _remap_positional(
        self, row: dict[str, Any], *, table: str | None = None
    ) -> dict[str, Any]:
        """Map positional ``UNKNOWN_COL{n}`` keys back to real column names."""
        if not any(isinstance(k, str) and k.startswith("UNKNOWN_COL") for k in row):
            return row
        names = self._ordered_columns(table)
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

    def _row_to_record(
        self, row: dict[str, Any], *, table: str | None = None
    ) -> dict[str, str]:
        row = self._remap_positional(row, table=table)
        return {k: _serialize(v) for k, v in row.items()}

    def _pk_columns_for(self, table: str | None = None) -> list[str]:
        from services.cdc_snapshot_window import _pk_columns

        pk = self.primary_keys.get(table or self.table, self.primary_key)
        return _pk_columns(pk)

    def _pk_value(self, row: dict[str, Any], *, table: str | None = None) -> str:
        from services.cdc_snapshot_window import _pk_value as composite_pk_value

        row = self._remap_positional(row, table=table)
        key = composite_pk_value(row, self._pk_columns_for(table))
        return "" if key is None else key

    def _table_allowed(self, table: str) -> bool:
        wanted = {t.lower() for t in self.tables}
        return (table or "").lower() in wanted

    def _canonical_table(self, table: str) -> str:
        by_lower = {t.lower(): t for t in self.tables}
        return by_lower.get((table or "").lower(), table or self.table)

    def _fetch_incremental_chunk(self, sig: Any) -> tuple[list[dict[str, Any]], str | None, bool]:
        """PK-ordered chunk reader for Debezium-style incremental snapshots.

        Stamps MySQL ``gtid_executed`` low/high watermarks on the signal
        (DBZ-3577 read-only window). Composite primary keys use lexicographic
        ``(c1, c2, …)`` ordering — never invent a single-column ORDER BY.
        """
        from connectors.sql_identifiers import (
            quote_sql_identifier,
            require_safe_identifier,
        )
        from services.cdc_incremental_snapshot import (
            snapshot_records_from_rows,
            update_signal,
        )
        from services.cdc_snapshot_window import (
            _pk_columns,
            _pk_value,
            keyset_successor_predicate,
        )

        pk_cols = _pk_columns(sig.primary_key or self.primary_key)
        pk_quoted = [
            quote_sql_identifier(require_safe_identifier(c, preserve_case=True))
            for c in pk_cols
        ]
        # Read the table the signal names. In shared multi-table mode `self.table`
        # is pinned to tables[0], so trusting it here backfilled the wrong table.
        sig_table = (getattr(sig, "table", "") or "").strip() or self.table
        table = quote_sql_identifier(require_safe_identifier(sig_table, preserve_case=True))
        db = quote_sql_identifier(require_safe_identifier(self.database, preserve_case=True)) if self.database else ""
        qualified = f"{db}.{table}" if db else table
        limit = int(sig.chunk_size or self.batch_size)
        last_pk = sig.last_pk or ""
        order_sql = ", ".join(pk_quoted)
        conn = self._conn()
        gtid_low = ""
        gtid_high = ""
        binlog_low = ""
        binlog_high = ""
        try:
            with conn.cursor() as cur:
                # Capture both watermarks. GTID brackets the peek filter
                # (DBZ-3577); binlog file:pos stamps `_df_lsn` so streaming
                # events stay in the same LSN family and can update the row.
                gtid_low = self._current_gtid_executed(cur) or ""
                binlog_low = self._binlog_file_pos_on(cur) or ""
                if last_pk:
                    where, params = keyset_successor_predicate(pk_quoted, last_pk)
                    cur.execute(
                        f"SELECT * FROM {qualified} WHERE {where} "  # nosec B608
                        f"ORDER BY {order_sql} LIMIT %s",
                        (*params, limit),
                    )
                else:
                    cur.execute(
                        f"SELECT * FROM {qualified} ORDER BY {order_sql} LIMIT %s",  # nosec B608
                        (limit,),
                    )
                cols = [d[0] for d in (cur.description or [])]
                rows = cur.fetchall() or []
                gtid_high = self._current_gtid_executed(cur) or ""
                binlog_high = self._binlog_file_pos_on(cur) or ""
            conn.commit()
        finally:
            conn.close()
        records = snapshot_records_from_rows(cols, rows)
        if records:
            new_last = _pk_value(records[-1], pk_cols)
        else:
            new_last = last_pk
        done = len(records) < limit
        # Mutate claimed signal so runner resume_token can surface watermarks.
        # Only persist non-empty captures. update_signal writes every provided
        # field, so an empty string from a transient SHOW MASTER STATUS failure
        # would wipe a previously good binlog watermark and fall the row stamp
        # back to GTID — freezing every later streaming update to that PK.
        persist: dict[str, str] = {}
        try:
            if gtid_low:
                sig.gtid_low = gtid_low
                persist["gtid_low"] = gtid_low
            if gtid_high:
                sig.gtid_high = gtid_high
                persist["gtid_high"] = gtid_high
            if binlog_low:
                sig.lsn_low = binlog_low
                persist["lsn_low"] = binlog_low
            if binlog_high:
                sig.lsn_high = binlog_high
                persist["lsn_high"] = binlog_high
        except Exception:
            pass
        if persist:
            update_signal(sig.id, **persist)
        return records, str(new_last) if new_last is not None else last_pk, done

    def _peek_stream_events_during_chunk(self, sig: Any) -> list[dict[str, Any]]:
        """Non-acking binlog peek for DDD-3 stream-wins during incremental snapshot.

        When GTID low/high watermarks are present (DBZ-3577), drop row events
        whose GTID is still contained in the low watermark (older than chunk
        SELECT start) — never invent stream-wins from stale binlog.
        """
        events: list[dict[str, Any]] = []
        try:
            from pymysqlreplication import BinLogStreamReader
            from pymysqlreplication.event import GtidEvent, RotateEvent
            from pymysqlreplication.row_event import (
                DeleteRowsEvent,
                TableMapEvent,
                UpdateRowsEvent,
                WriteRowsEvent,
            )
        except ImportError:
            return []
        from connectors.writer_common import gtid_set_contains
        from services.cdc_snapshot_window import (
            _pk_columns,
            _pk_row_dict,
            _pk_value,
        )

        # Same key the SnapshotWindow buffers under — composite aware.
        peek_pk_cols = _pk_columns(sig.primary_key or self.primary_key)
        # A peek during an incremental snapshot of table T must only decode T.
        # Remapping sibling tables through T's ordinals would invent collisions
        # against the wrong PKs and suppress legitimate snapshot rows.
        peek_table = self._canonical_table(str(getattr(sig, "table", "") or self.table))

        # TableMapEvent is mandatory: pymysqlreplication filters at the packet
        # level, so omitting it leaves table_map empty and every RowsEvent is
        # silently dropped (_processed=False). That made every peek return []
        # and let stale snapshot READs overwrite mid-window UPDATEs (DBZ-3577).
        # RotateEvent keeps the decoder alive across a binlog rotation mid-peek.
        kwargs = self._binlog_kwargs(
            blocking=False,
            only_events=[
                GtidEvent,
                RotateEvent,
                TableMapEvent,
                WriteRowsEvent,
                UpdateRowsEvent,
                DeleteRowsEvent,
            ],
        )
        # Distinct server_id so peek does not collide with the durable poll session.
        kwargs["server_id"] = int(kwargs.get("server_id") or 10_000) + 7
        # Bound peek — do not advance durable resume_token.
        peek_limit = min(int(sig.chunk_size or self.batch_size), 200)
        gtid_low = str(getattr(sig, "gtid_low", "") or "")
        current_gtid = ""
        stream = BinLogStreamReader(**kwargs)
        try:
            count = 0
            for binlog_event in stream:
                if isinstance(binlog_event, GtidEvent):
                    current_gtid = str(
                        getattr(binlog_event, "gtid", None)
                        or getattr(binlog_event, "gset", None)
                        or ""
                    )
                    continue
                if getattr(binlog_event, "schema", "") != self.database:
                    continue
                event_table = getattr(binlog_event, "table", "") or ""
                if self._canonical_table(event_table) != peek_table:
                    continue
                # Stale vs chunk start: GTID already in low watermark.
                if (
                    gtid_low
                    and current_gtid
                    and gtid_set_contains(gtid_low, current_gtid)
                ):
                    continue
                if isinstance(binlog_event, WriteRowsEvent):
                    for row in getattr(binlog_event, "rows", []):
                        values = (
                            row.get("values")
                            if isinstance(row, dict) and "values" in row
                            else row
                        )
                        events.append(
                            {
                                "op": "c",
                                "row": self._row_to_record(values, table=peek_table),
                                "gtid": current_gtid,
                            }
                        )
                        count += 1
                elif isinstance(binlog_event, UpdateRowsEvent):
                    for row in getattr(binlog_event, "rows", []):
                        after = row.get("after_values") if isinstance(row, dict) else getattr(row, "after_values", {})
                        events.append(
                            {
                                "op": "u",
                                "row": self._row_to_record(after, table=peek_table),
                                "gtid": current_gtid,
                            }
                        )
                        count += 1
                elif isinstance(binlog_event, DeleteRowsEvent):
                    for row in getattr(binlog_event, "rows", []):
                        values = row.get("values") if isinstance(row, dict) else getattr(row, "values", {})
                        # The window buffers snapshot rows under the signal's
                        # composite key. A single-column pk here never collides,
                        # so a row deleted mid-chunk would still be emitted as a
                        # snapshot READ and land at the destination.
                        record = self._row_to_record(values, table=peek_table)
                        pk = _pk_value(record, peek_pk_cols)
                        if pk:
                            events.append(
                                {
                                    "op": "d",
                                    "pk": pk,
                                    "row": _pk_row_dict(peek_pk_cols, pk),
                                    "gtid": current_gtid,
                                }
                            )
                            count += 1
                if count >= peek_limit:
                    break
        except Exception:
            return events
        finally:
            try:
                stream.close()
            except Exception as exc:
                _logger.warning("Exception suppressed: %s", exc, exc_info=exc)
        return events

    def poll(self) -> Iterator[ChangeBatch]:
        # Incomplete initial sync must finish before binlog streaming.
        if isinstance(self.resume_token, dict) and self.resume_token.get("phase") == "snapshot":
            yield from self.snapshot()
            return

        self._acquire_cdc_lease()
        self._poll_signal_table()
        # Fail-closed before opening the stream — purged binlog/GTID is silent loss.
        self._assert_resume_within_retention()

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
            TableMapEvent,
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
            # Only advance the resume token to a position we actually read.
            # If a poll window reads no events, falling back to the previous
            # resume token prevents BinLogStreamReader from jumping past events
            # that were committed while the (non-blocking) stream was open.
            fallback = self.resume_token if isinstance(self.resume_token, dict) else {}
            token = dict(fallback)
            if pos:
                token.update(pos)
            if not token:
                token = {"tables": list(self.tables)}
            try:
                current = self._current_binlog_position() or {}
                # Keep GTID as metadata, but _binlog_kwargs uses file/pos when
                # present so the next BinLogStreamReader never jumps past unread
                # rows based on a GTID that raced ahead during the poll window.
                if current.get("gtid"):
                    token["gtid"] = current["gtid"]
                if not token.get("file") and current.get("file"):
                    token["file"] = current["file"]
                    token["pos"] = current.get("pos")
            except Exception as exc:
                _logger.warning("Exception suppressed: %s", exc, exc_info=exc)
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
                TableMapEvent,
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
                    # pymysqlreplication updates stream.log_file/log_pos internally.
                    # Do NOT use the RotateEvent payload (position is the *start*
                    # of the next file, often position 4 for the current file in
                    # heartbeat/artificial rotates) as a resume point; rely on
                    # the row/Xid events we actually consume to advance the token.
                    continue
                if isinstance(binlog_event, TableMapEvent):
                    # Table map is processed internally by pymysqlreplication; we
                    # only need it decoded so WriteRows events get schema/table info.
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
                    commit_at = datetime.fromtimestamp(event_ts, tz=timezone.utc)
                    self._last_event_commit_at = commit_at
                    self._last_event_at = commit_at
                else:
                    self._last_event_at = datetime.now(timezone.utc)

                if isinstance(binlog_event, WriteRowsEvent):
                    for row in getattr(binlog_event, "rows", []):
                        values = (
                            row.get("values")
                            if isinstance(row, dict) and "values" in row
                            else row
                        )
                        rec = self._row_to_record(values, table=tbl)
                        # Explicit PK is required so the net-effect coalescer
                        # cannot guess from the first non-empty column and
                        # collapse two distinct rows that share that value.
                        pk = self._pk_value(values, table=tbl)
                        buf.insert(tbl, rec, pk=pk or None, lsn=str(stream.log_pos or ""))
                        event_count += 1
                elif isinstance(binlog_event, UpdateRowsEvent):
                    for row in getattr(binlog_event, "rows", []):
                        after = (
                            row.get("after_values")
                            if isinstance(row, dict)
                            else getattr(row, "after_values", {})
                        )
                        rec = self._row_to_record(after, table=tbl)
                        pk = self._pk_value(after, table=tbl)
                        buf.update(tbl, rec, pk=pk or None, lsn=str(stream.log_pos or ""))
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

        # Mid-window open txn: hold only when BEGIN was seen (explicit txn).
        # Implicit autocommit windows (row events without BEGIN) flush at end of poll.
        if buf.open_xid is not None:
            if not buf.explicit_txn:
                yield from _emit_commit()
                return
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
