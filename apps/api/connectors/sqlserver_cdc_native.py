"""SQL Server *native* CDC (cdc schema) — Debezium-class path.

Uses ``sys.sp_cdc_enable_table`` capture instances and
``cdc.fn_cdc_get_all_changes_<capture>`` with LSN watermarks. Change Tracking
(``sqlserver_change_stream.py``) remains the lighter fallback when native CDC
is not enabled on the database.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Iterator

from services.cdc_engine import ChangeBatch

logger = logging.getLogger(__name__)


def encode_mssql_cdc_token(lsn: bytes | str, *, table: str, phase: str = "streaming") -> str:
    if isinstance(lsn, (bytes, bytearray)):
        lsn_hex = bytes(lsn).hex()
    else:
        lsn_hex = str(lsn)
    return json.dumps(
        {"kind": "mssql-cdc", "table": table, "lsn": lsn_hex, "phase": phase},
        separators=(",", ":"),
    )


def decode_mssql_cdc_token(token: str | None) -> dict[str, Any]:
    if not token:
        return {"lsn": "", "phase": "initial", "table": ""}
    try:
        data = json.loads(str(token))
        if isinstance(data, dict) and data.get("kind") == "mssql-cdc":
            return {
                "lsn": str(data.get("lsn") or ""),
                "phase": str(data.get("phase") or "streaming"),
                "table": str(data.get("table") or ""),
            }
    except Exception:
        pass
    return {"lsn": "", "phase": "initial", "table": ""}


def _hex_to_lsn(hex_str: str) -> bytes:
    h = (hex_str or "").strip()
    if h.startswith("0x"):
        h = h[2:]
    if not h:
        return b""
    return bytes.fromhex(h)


class SqlServerNativeCdc:
    """Native SQL Server CDC via capture instance + LSN ranges."""

    def __init__(
        self,
        cfg: dict[str, Any],
        *,
        table: str,
        primary_key: str,
        schema: str = "dbo",
        batch_size: int = 500,
        resume_token: str | None = None,
        capture_instance: str = "",
        cursor_key: str = "",
    ) -> None:
        self.cfg = cfg
        self.table = table
        self.schema = schema or "dbo"
        self.primary_key = primary_key or "id"
        self.batch_size = max(1, int(batch_size or 500))
        self.capture_instance = capture_instance or f"{self.schema}_{self.table}"
        state = decode_mssql_cdc_token(resume_token)
        self.start_lsn = state.get("lsn") or ""
        self.phase = state.get("phase") or "initial"
        self._last_event_at: datetime | None = None
        from services.cdc_schema_history import connection_fingerprint

        self.source_key = connection_fingerprint(
            {**cfg, "type": "sqlserver"},
            connector_id=str(cfg.get("connector_id") or ""),
        )
        database = str(cfg.get("database") or "master")
        self.cursor_key = (
            cursor_key
            or f"mssql-cdc:{database}:{self.schema}.{self.table}"
        )
        from services.cdc_lease import CdcLeaseGuard, mssql_cdc_resource

        self._lease = CdcLeaseGuard(
            cursor_key=self.cursor_key,
            resource=mssql_cdc_resource(database, self.schema, self.table, mode="cdc"),
            holder_id=str(cfg.get("lease_holder_id") or ""),
            job_id=str(cfg.get("job_id") or ""),
            meta={
                "engine": "sqlserver_native",
                "capture_instance": self.capture_instance,
                "table": self.table,
            },
        )

    def _acquire_cdc_lease(self) -> None:
        self._lease.ensure()

    def close(self) -> None:
        self._lease.release()

    def cdc_metadata(self) -> dict[str, Any]:
        return {
            "plugin": "sqlserver_native_cdc",
            "phase": self.phase,
            "delivery": "at-least-once",
            "capture_instance": self.capture_instance,
            **self._lease.theater_fields(),
        }

    def _conn(self):
        from connectors.generic_sql import get_connection

        return get_connection(
            host=self.cfg.get("host") or "localhost",
            port=self.cfg.get("port") or 1433,
            database=self.cfg.get("database") or "master",
            username=self.cfg.get("username") or "",
            password=self.cfg.get("password") or "",
            connection_string=self.cfg.get("connection_string") or "",
            ssl=bool(self.cfg.get("ssl")),
            db_type="sqlserver",
        )

    def is_available(self) -> bool:
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()"
                    )
                    row = cur.fetchone()
                    if not row or not row[0]:
                        return False
                    cur.execute(
                        """
                        SELECT 1 FROM cdc.change_tables ct
                        JOIN sys.tables t ON t.object_id = ct.source_object_id
                        JOIN sys.schemas s ON s.schema_id = t.schema_id
                        WHERE t.name = %s AND s.name = %s
                        """,
                        (self.table, self.schema),
                    )
                    return cur.fetchone() is not None
        except Exception as exc:
            logger.debug("SQL Server native CDC unavailable: %s", exc)
            return False

    def _max_lsn(self, cur) -> str:
        cur.execute("SELECT sys.fn_cdc_get_max_lsn()")
        row = cur.fetchone()
        if not row or row[0] is None:
            return ""
        val = row[0]
        return val.hex() if isinstance(val, (bytes, bytearray)) else str(val)

    def _min_lsn(self, cur) -> str:
        cur.execute("SELECT sys.fn_cdc_get_min_lsn(%s)", (self.capture_instance,))
        row = cur.fetchone()
        if not row or row[0] is None:
            return ""
        val = row[0]
        return val.hex() if isinstance(val, (bytes, bytearray)) else str(val)

    def snapshot(self) -> Iterator[ChangeBatch]:
        """Initial dump + LSN handoff (Debezium initial snapshot)."""
        self._acquire_cdc_lease()
        qualified = f"[{self.schema}].[{self.table}]"
        pk = self.primary_key
        offset = 0
        handoff = ""
        with self._conn() as conn:
            with conn.cursor() as cur:
                handoff = self._max_lsn(cur) or self._min_lsn(cur)
                while True:
                    cur.execute(
                        f"""
                        SELECT *
                        FROM {qualified}
                        ORDER BY [{pk}]
                        OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
                        """,
                        (offset, self.batch_size),
                    )
                    cols = [d[0] for d in (cur.description or [])]
                    rows = cur.fetchall() or []
                    if not rows:
                        break
                    records = [
                        {cols[i]: "" if row[i] is None else str(row[i]) for i in range(len(cols))}
                        for row in rows
                    ]
                    offset += len(rows)
                    self._last_event_at = datetime.now(timezone.utc)
                    yield ChangeBatch(
                        inserts=records,
                        resume_token=encode_mssql_cdc_token(
                            handoff, table=self.table, phase="snapshot"
                        ),
                    )
                    if len(rows) < self.batch_size:
                        break
        self.start_lsn = handoff
        self.phase = "streaming"
        yield ChangeBatch(
            resume_token=encode_mssql_cdc_token(self.start_lsn, table=self.table, phase="streaming")
        )

    def _fetch_incremental_chunk(self, sig: Any) -> tuple[list[dict[str, Any]], str | None, bool]:
        """PK-ordered chunk for signal-driven incremental snapshots."""
        from connectors.sql_identifiers import require_safe_identifier

        pk_name = require_safe_identifier(sig.primary_key or self.primary_key, preserve_case=True)
        pk = f"[{pk_name.replace(']', ']]')}]"
        qualified = f"[{self.schema}].[{self.table}]"
        limit = int(sig.chunk_size or self.batch_size)
        last_pk = sig.last_pk or ""
        with self._conn() as conn:
            with conn.cursor() as cur:
                if last_pk:
                    cur.execute(
                        f"""
                        SELECT TOP ({limit}) *
                        FROM {qualified}
                        WHERE {pk} > %s
                        ORDER BY {pk}
                        """,
                        (last_pk,),
                    )
                else:
                    cur.execute(
                        f"""
                        SELECT TOP ({limit}) *
                        FROM {qualified}
                        ORDER BY {pk}
                        """
                    )
                cols = [d[0] for d in (cur.description or [])]
                rows = cur.fetchall() or []
        records = [
            {cols[i]: "" if row[i] is None else str(row[i]) for i in range(len(cols))}
            for row in rows
        ]
        new_last = records[-1].get(pk_name) if records else last_pk
        done = len(records) < limit
        return records, str(new_last) if new_last is not None else last_pk, done

    def _peek_stream_events_during_chunk(self, sig: Any) -> list[dict[str, Any]]:
        """Non-acking CDC LSN peek for DDD-3 stream-wins during incremental snapshot."""
        events: list[dict[str, Any]] = []
        if not self.start_lsn:
            return events
        fn = f"cdc.fn_cdc_get_all_changes_{self.capture_instance}"
        peek_limit = min(int(sig.chunk_size or self.batch_size), 200)
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    max_lsn = self._max_lsn(cur)
                    if not max_lsn or max_lsn == self.start_lsn:
                        return events
                    cur.execute(
                        f"""
                        SELECT TOP ({peek_limit}) *
                        FROM {fn}(%s, %s, 'all')
                        ORDER BY __$start_lsn
                        """,
                        (_hex_to_lsn(self.start_lsn), _hex_to_lsn(max_lsn)),
                    )
                    cols = [d[0] for d in (cur.description or [])]
                    for row in cur.fetchall() or []:
                        rec = {cols[i]: row[i] for i in range(len(cols))}
                        op = rec.get("__$operation")
                        clean = {
                            k: "" if v is None else str(v)
                            for k, v in rec.items()
                            if not str(k).startswith("__$")
                        }
                        key = clean.get(self.primary_key, "")
                        if op == 1 and key:
                            events.append({"op": "d", "pk": key, "row": {self.primary_key: key}})
                        elif op == 2:
                            events.append({"op": "c", "row": clean})
                        elif op == 4:
                            events.append({"op": "u", "row": clean})
        except Exception:
            return events
        return events

    def poll(self) -> Iterator[ChangeBatch]:
        self._acquire_cdc_lease()
        if self.phase != "streaming" or not self.start_lsn:
            yield from self.snapshot()
            return

        from services.cdc_incremental_runner import interleave_incremental_snapshot

        yield from interleave_incremental_snapshot(
            self.source_key,
            table=self.table,
            fetch_chunk=self._fetch_incremental_chunk,
            stream_events_during_chunk=self._peek_stream_events_during_chunk,
            max_chunks_per_poll=1,
        )

        fn = f"cdc.fn_cdc_get_all_changes_{self.capture_instance}"
        inserts: list[dict[str, Any]] = []
        updates: list[dict[str, Any]] = []
        deletes: list[str] = []
        next_lsn = self.start_lsn
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    max_lsn = self._max_lsn(cur)
                    if not max_lsn or max_lsn == self.start_lsn:
                        yield ChangeBatch(
                            resume_token=encode_mssql_cdc_token(
                                self.start_lsn, table=self.table, phase="streaming"
                            )
                        )
                        return
                    # from_lsn is exclusive in all changes; advance past start
                    cur.execute(
                        f"""
                        SELECT TOP ({self.batch_size}) *
                        FROM {fn}(%s, %s, 'all')
                        ORDER BY __$start_lsn
                        """,
                        (_hex_to_lsn(self.start_lsn), _hex_to_lsn(max_lsn)),
                    )
                    cols = [d[0] for d in (cur.description or [])]
                    for row in cur.fetchall() or []:
                        rec = {cols[i]: row[i] for i in range(len(cols))}
                        op = rec.get("__$operation")
                        start = rec.get("__$start_lsn")
                        if isinstance(start, (bytes, bytearray)):
                            next_lsn = start.hex()
                        clean = {
                            k: "" if v is None else str(v)
                            for k, v in rec.items()
                            if not str(k).startswith("__$")
                        }
                        self._last_event_at = datetime.now(timezone.utc)
                        key = clean.get(self.primary_key, "")
                        # 1=delete, 2=insert, 3=update before, 4=update after
                        if op == 1:
                            if key:
                                deletes.append(key)
                        elif op == 2:
                            inserts.append(clean)
                        elif op == 4:
                            updates.append(clean)
                    self.start_lsn = next_lsn or max_lsn
        except Exception as exc:
            logger.warning("SQL Server native CDC poll failed: %s", exc)
            return

        token = encode_mssql_cdc_token(self.start_lsn, table=self.table, phase="streaming")
        if inserts or updates or deletes:
            yield ChangeBatch(inserts=inserts, updates=updates, deletes=deletes, resume_token=token)
        else:
            yield ChangeBatch(resume_token=token)

    def ack(self, resume_token: Any = None) -> None:
        if resume_token:
            state = decode_mssql_cdc_token(str(resume_token))
            if state.get("lsn"):
                self.start_lsn = state["lsn"]

    def lag_seconds(self) -> float | None:
        if self._last_event_at is None:
            return None
        return max(0.0, (datetime.now(timezone.utc) - self._last_event_at).total_seconds())

    def replication_lag_seconds(self) -> float | None:
        return self.lag_seconds()
