"""SQL Server *native* CDC (cdc schema) — Debezium-class path.

Uses ``sys.sp_cdc_enable_table`` capture instances and
``cdc.fn_cdc_get_all_changes_<capture>`` with LSN watermarks. Change Tracking
(``sqlserver_change_stream.py``) remains the lighter fallback when native CDC
is not enabled on the database.

Robustness
----------
- Capture instance is **discovered** from ``cdc.change_tables`` (not assumed
  ``schema_table``).
- Poll batches never split a transaction LSN group (``__$start_lsn`` boundary).
- Snapshot tokens carry resumable ``offset``.
- Without SQL Agent, callers may invoke ``force_cdc_scan()`` (tests/CI do).
- Delivery remains **at-least-once**; leases prevent concurrent consumers.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Iterator

from services.cdc_cursor_gap import CdcLsnGapError
from services.cdc_engine import ChangeBatch

logger = logging.getLogger(__name__)

# Re-export for existing ``from connectors.sqlserver_cdc_native import CdcLsnGapError``.
__all_gap__ = ("CdcLsnGapError",)


def encode_mssql_cdc_token(
    lsn: bytes | str,
    *,
    table: str,
    phase: str = "streaming",
    offset: int = 0,
    seqval: bytes | str | None = None,
    capture_instance: str = "",
) -> str:
    if isinstance(lsn, (bytes, bytearray)):
        lsn_hex = bytes(lsn).hex()
    else:
        lsn_hex = str(lsn or "")
    if isinstance(seqval, (bytes, bytearray)):
        seq_hex = bytes(seqval).hex()
    else:
        seq_hex = str(seqval or "")
    payload: dict[str, Any] = {
        "kind": "mssql-cdc",
        "table": table,
        "lsn": lsn_hex,
        "phase": phase,
    }
    if offset:
        payload["offset"] = int(offset)
    if seq_hex:
        payload["seqval"] = seq_hex
    if capture_instance:
        payload["capture_instance"] = capture_instance
    return json.dumps(payload, separators=(",", ":"))


def decode_mssql_cdc_token(token: str | None) -> dict[str, Any]:
    if not token:
        return {
            "lsn": "",
            "phase": "initial",
            "table": "",
            "offset": 0,
            "seqval": "",
            "capture_instance": "",
        }
    try:
        data = json.loads(str(token))
        if isinstance(data, dict) and data.get("kind") == "mssql-cdc":
            return {
                "lsn": str(data.get("lsn") or ""),
                "phase": str(data.get("phase") or "streaming"),
                "table": str(data.get("table") or ""),
                "offset": int(data.get("offset") or 0),
                "seqval": str(data.get("seqval") or ""),
                "capture_instance": str(data.get("capture_instance") or ""),
            }
    except Exception:
        pass
    return {
        "lsn": "",
        "phase": "initial",
        "table": "",
        "offset": 0,
        "seqval": "",
        "capture_instance": "",
    }


def _hex_to_lsn(hex_str: str) -> bytes:
    h = (hex_str or "").strip()
    if h.startswith("0x") or h.startswith("0X"):
        h = h[2:]
    if not h:
        return b""
    if len(h) % 2:
        h = "0" + h
    return bytes.fromhex(h)


def _lsn_to_hex(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, (bytes, bytearray)):
        return bytes(val).hex()
    text = str(val).strip()
    if text.startswith("0x") or text.startswith("0X"):
        text = text[2:]
    return text


def compare_mssql_hex_lsn(left: str, right: str) -> int:
    """Compare SQL Server LSN hex strings as binary values (pad to equal length)."""
    a = _hex_to_lsn(left)
    b = _hex_to_lsn(right)
    n = max(len(a), len(b))
    a = a.rjust(n, b"\x00")
    b = b.rjust(n, b"\x00")
    if a < b:
        return -1
    if a > b:
        return 1
    return 0


def assert_resume_lsn_in_retention(
    resume_lsn: str,
    min_lsn: str,
    *,
    cursor_key: str = "",
) -> None:
    """Raise :class:`CdcLsnGapError` when resume is strictly before retention min.

    Models AG failover / cleanup where the secondary's CDC retention window no
    longer covers the consumer cursor (Debezium-class InvalidLSN posture).
    Equal min_lsn is allowed (exclusive-from semantics start at the retained edge).
    """
    resume = _lsn_to_hex(resume_lsn)
    retained = _lsn_to_hex(min_lsn)
    if not resume or not retained:
        return
    if compare_mssql_hex_lsn(resume, retained) < 0:
        raise CdcLsnGapError(
            "SQL Server CDC resume LSN is before capture retention min_lsn "
            f"(resume={resume}, min_lsn={retained}). Likely CDC cleanup or "
            "Availability Group failover gap — re-snapshot / reset watermark; "
            "do not claim continuous CDC across the gap.",
            resume_lsn=resume,
            min_lsn=retained,
            cursor_key=cursor_key,
        )


# Row-filter / change-mode for CDC TVFs (Microsoft docs).
ROW_FILTER_ALL = "all"
ROW_FILTER_ALL_UPDATE_OLD = "all update old"
ROW_FILTER_NET = "net"


def normalize_mssql_row_filter(value: str | None) -> str:
    """Map config aliases to a canonical CDC row_filter_option."""
    raw = (value or ROW_FILTER_ALL).strip().lower().replace("-", " ").replace("_", " ")
    raw = " ".join(raw.split())
    if raw in {"all", ""}:
        return ROW_FILTER_ALL
    if raw in {"all update old", "allupdateold", "update old", "before image", "before"}:
        return ROW_FILTER_ALL_UPDATE_OLD
    if raw in {"net", "net changes", "netchanges"}:
        return ROW_FILTER_NET
    return ROW_FILTER_ALL


def classify_mssql_cdc_rows(
    rows: list[dict[str, Any]],
    *,
    primary_key: str,
    row_filter: str = ROW_FILTER_ALL,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """Map ``__$operation`` rows into inserts / updates / deletes.

    - 1 delete, 2 insert, 3 update-before, 4 update-after
    Before images (op 3) pair with the next after image (op 4) for the same PK
    and attach as ``_df_before`` when ``row_filter`` is ``all update old``.
    """
    inserts: list[dict[str, Any]] = []
    updates: list[dict[str, Any]] = []
    deletes: list[str] = []
    pending_before: dict[str, dict[str, Any]] = {}
    use_before = row_filter == ROW_FILTER_ALL_UPDATE_OLD

    for rec in rows:
        op = rec.get("__$operation")
        try:
            op_i = int(op) if op is not None else -1
        except (TypeError, ValueError):
            op_i = -1
        clean = {
            k: "" if v is None else str(v)
            for k, v in rec.items()
            if not str(k).startswith("__$")
        }
        key = str(clean.get(primary_key, "") or "")
        if op_i == 1:
            if key:
                deletes.append(key)
                pending_before.pop(key, None)
        elif op_i == 2:
            inserts.append(clean)
            pending_before.pop(key, None)
        elif op_i == 3 and use_before:
            if key:
                pending_before[key] = clean
        elif op_i == 4:
            if use_before and key and key in pending_before:
                clean = {**clean, "_df_before": dict(pending_before.pop(key))}
            updates.append(clean)
    return inserts, updates, deletes


class SqlServerNativeCdc:
    """Native SQL Server CDC via capture instance + LSN ranges.

    Pass ``table`` as a list (and optional ``primary_keys``) to share one
    connection / LSN cursor across capture instances — same Debezium-class
    demux pattern as Postgres multi-table logical decoding and MySQL binlog.
    Delivery remains **at-least-once**; ``ack_barrier`` gates shared progress.
    """

    def __init__(
        self,
        cfg: dict[str, Any],
        *,
        table: str | list[str],
        primary_key: str = "id",
        primary_keys: dict[str, str] | None = None,
        schema: str = "dbo",
        batch_size: int = 500,
        resume_token: str | None = None,
        capture_instance: str = "",
        cursor_key: str = "",
        row_filter: str = "",
    ) -> None:
        from services.cdc_multi_table import normalize_table_list, tables_digest

        self.cfg = cfg
        self.tables = normalize_table_list(table)
        if not self.tables:
            raise ValueError("SQL Server CDC requires at least one table")
        self.table = self.tables[0]
        self.schema = schema or "dbo"
        self.primary_keys: dict[str, str] = {
            t: str((primary_keys or {}).get(t) or primary_key or "id")
            for t in self.tables
        }
        self.primary_key = self.primary_keys[self.table]
        self.batch_size = max(1, int(batch_size or 500))
        self._shared = len(self.tables) > 1
        self._captures: dict[str, str] = {}
        state = decode_mssql_cdc_token(resume_token)
        self.capture_instance = (
            capture_instance
            or state.get("capture_instance")
            or f"{self.schema}_{self.table}"
        )
        self._capture_resolved = bool(capture_instance or state.get("capture_instance"))
        if self._shared and self.capture_instance:
            self._captures[self.table] = self.capture_instance
        self.row_filter = normalize_mssql_row_filter(
            row_filter
            or cfg.get("cdc_row_filter")
            or cfg.get("row_filter")
            or ROW_FILTER_ALL
        )
        self.start_lsn = state.get("lsn") or ""
        self.start_seqval = state.get("seqval") or ""
        self.phase = state.get("phase") or "initial"
        self.snapshot_offset = int(state.get("offset") or 0)
        self._last_event_at: datetime | None = None
        self._last_schema_fingerprint: str = ""
        from services.cdc_schema_history import connection_fingerprint

        self.source_key = connection_fingerprint(
            {**cfg, "type": "sqlserver"},
            connector_id=str(cfg.get("connector_id") or ""),
        )
        database = str(cfg.get("database") or "master")
        digest = tables_digest(self.tables)
        self.cursor_key = cursor_key or (
            f"mssql-cdc-shared:{database}:{self.schema}:{digest}"
            if self._shared
            else f"mssql-cdc:{database}:{self.schema}.{self.table}"
        )
        from services.cdc_lease import (
            CdcLeaseGuard,
            mssql_cdc_resource,
            mssql_cdc_shared_resource,
        )

        resource = (
            mssql_cdc_shared_resource(database, self.schema, self.tables, mode="cdc")
            if self._shared
            else mssql_cdc_resource(database, self.schema, self.table, mode="cdc")
        )
        self._lease = CdcLeaseGuard(
            cursor_key=self.cursor_key,
            resource=resource,
            holder_id=str(cfg.get("lease_holder_id") or ""),
            job_id=str(cfg.get("job_id") or ""),
            meta={
                "engine": "sqlserver_native",
                "capture_instance": self.capture_instance,
                "table": self.table,
                "tables": list(self.tables),
                "row_filter": self.row_filter,
                "shared_reader": self._shared,
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
            "captures": dict(self._captures),
            "tables": list(self.tables),
            "cdc_row_filter": self.row_filter,
            "shared_reader": self._shared,
            **self._lease.theater_fields(),
        }

    def _changes_tvf_for(self, capture_instance: str) -> str:
        """Return the CDC TVF for a capture instance + row filter."""
        cap = (capture_instance or self.capture_instance or "").strip()
        if self.row_filter == ROW_FILTER_NET:
            return f"cdc.fn_cdc_get_net_changes_{cap}"
        return f"cdc.fn_cdc_get_all_changes_{cap}"

    def _changes_tvf(self) -> str:
        """Return the CDC table-valued function for the configured row filter."""
        return self._changes_tvf_for(self.capture_instance)

    def _row_filter_sql_arg(self) -> str:
        if self.row_filter == ROW_FILTER_ALL_UPDATE_OLD:
            return "all update old"
        # Net TVF ignores filter text but still requires an option; 'all' is valid.
        return "all"

    def _maybe_record_capture_schema(self, cur, *, offset: str = "") -> None:
        """Persist captured column list when it changes (schema history)."""
        try:
            cur.execute(
                """
                SELECT cc.column_name, cc.column_type, cc.column_ordinal
                FROM cdc.captured_columns cc
                JOIN cdc.change_tables ct ON ct.object_id = cc.object_id
                WHERE ct.capture_instance = %s
                ORDER BY cc.column_ordinal
                """,
                (self.capture_instance,),
            )
            cols = [
                {
                    "name": str(r[0]),
                    "type": str(r[1]) if r[1] is not None else "",
                    "ordinal": int(r[2] or 0),
                }
                for r in (cur.fetchall() or [])
            ]
            if not cols:
                return
            fingerprint = json.dumps(cols, separators=(",", ":"), sort_keys=True)
            if fingerprint == self._last_schema_fingerprint:
                return
            from services.cdc_schema_history import record_ddl

            record_ddl(
                self.source_key,
                f"{self.schema}.{self.table}",
                ddl=f"cdc.capture_instance={self.capture_instance}",
                offset=offset or self.start_lsn or self.capture_instance,
                schema_snapshot={
                    "capture_instance": self.capture_instance,
                    "row_filter": self.row_filter,
                    "columns": cols,
                },
            )
            self._last_schema_fingerprint = fingerprint
        except Exception as exc:
            logger.debug("SQL Server CDC schema history skipped: %s", exc)

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

    def _resolve_capture_for_table(self, cur, table: str) -> str:
        """Prefer the live capture instance name from ``cdc.change_tables``."""
        cur.execute(
            """
            SELECT TOP 1 ct.capture_instance
            FROM cdc.change_tables ct
            JOIN sys.tables t ON t.object_id = ct.source_object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE t.name = %s AND s.name = %s
            ORDER BY ct.create_date DESC
            """,
            (table, self.schema),
        )
        row = cur.fetchone()
        capture = str(row[0]) if row and row[0] else f"{self.schema}_{table}"
        self._captures[table] = capture
        if table == self.table:
            self.capture_instance = capture
            self._capture_resolved = True
            self._lease.meta["capture_instance"] = self.capture_instance
        return capture

    def _resolve_capture_instance(self, cur) -> str:
        return self._resolve_capture_for_table(cur, self.table)

    def _resolve_all_captures(self, cur) -> dict[str, str]:
        for t in self.tables:
            self._resolve_capture_for_table(cur, t)
        self._lease.meta["captures"] = dict(self._captures)
        return self._captures

    def force_cdc_scan(self) -> None:
        """Populate change tables when SQL Agent is unavailable (Docker/CI)."""
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("EXEC sys.sp_cdc_scan")
                conn.commit()
        except Exception as exc:
            logger.debug("sp_cdc_scan skipped: %s", exc)

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
                    self._resolve_all_captures(cur)
                    for t in self.tables:
                        cur.execute(
                            """
                            SELECT 1 FROM cdc.change_tables ct
                            JOIN sys.tables t ON t.object_id = ct.source_object_id
                            JOIN sys.schemas s ON s.schema_id = t.schema_id
                            WHERE t.name = %s AND s.name = %s
                            """,
                            (t, self.schema),
                        )
                        if cur.fetchone() is None:
                            return False
                    return True
        except Exception as exc:
            logger.debug("SQL Server native CDC unavailable: %s", exc)
            return False

    def _max_lsn(self, cur) -> str:
        cur.execute("SELECT sys.fn_cdc_get_max_lsn()")
        row = cur.fetchone()
        if not row or row[0] is None:
            return ""
        return _lsn_to_hex(row[0])

    def _min_lsn(self, cur) -> str:
        return self._min_lsn_for(cur, self.capture_instance)

    def _min_lsn_for(self, cur, capture_instance: str) -> str:
        cur.execute("SELECT sys.fn_cdc_get_min_lsn(%s)", (capture_instance,))
        row = cur.fetchone()
        if not row or row[0] is None:
            return ""
        return _lsn_to_hex(row[0])

    def _token_table_label(self) -> str:
        if self._shared:
            return ",".join(self.tables)
        return self.table

    def _token(
        self,
        *,
        lsn: str,
        phase: str,
        offset: int = 0,
        seqval: str = "",
        table: str | None = None,
        capture_instance: str | None = None,
    ) -> str:
        return encode_mssql_cdc_token(
            lsn,
            table=table or self._token_table_label(),
            phase=phase,
            offset=offset,
            seqval=seqval or None,
            capture_instance=capture_instance
            if capture_instance is not None
            else ("" if self._shared else self.capture_instance),
        )

    def snapshot(self) -> Iterator[ChangeBatch]:
        """Initial dump + LSN handoff (Debezium initial snapshot)."""
        self._acquire_cdc_lease()
        if self._shared:
            yield from self._snapshot_shared()
            return
        qualified = f"[{self.schema}].[{self.table}]"
        pk = self.primary_key
        offset = int(self.snapshot_offset or 0)
        handoff = ""
        with self._conn() as conn:
            with conn.cursor() as cur:
                self._resolve_capture_instance(cur)
                handoff = self._max_lsn(cur) or self._min_lsn(cur)
                self._maybe_record_capture_schema(cur, offset=handoff)
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
                        {
                            cols[i]: "" if row[i] is None else str(row[i])
                            for i in range(len(cols))
                        }
                        for row in rows
                    ]
                    offset += len(rows)
                    self.snapshot_offset = offset
                    self._last_event_at = datetime.now(timezone.utc)
                    yield ChangeBatch(
                        inserts=records,
                        resume_token=self._token(
                            lsn=handoff, phase="snapshot", offset=offset
                        ),
                        table=self.table,
                    )
                    if len(rows) < self.batch_size:
                        break
        self.start_lsn = handoff
        self.start_seqval = ""
        self.phase = "streaming"
        self.snapshot_offset = 0
        yield ChangeBatch(
            resume_token=self._token(lsn=self.start_lsn, phase="streaming"),
            table=self.table,
        )

    def _snapshot_shared(self) -> Iterator[ChangeBatch]:
        """Multi-table initial dump under one LSN handoff (at-least-once)."""
        handoff = ""
        with self._conn() as conn:
            with conn.cursor() as cur:
                self._resolve_all_captures(cur)
                handoff = self._max_lsn(cur)
                if not handoff:
                    for t in self.tables:
                        handoff = self._min_lsn_for(cur, self._captures.get(t, "")) or handoff
                for table_name in self.tables:
                    cap = self._captures.get(table_name, "")
                    if cap:
                        prev_cap = self.capture_instance
                        self.capture_instance = cap
                        try:
                            self._maybe_record_capture_schema(cur, offset=handoff)
                        finally:
                            self.capture_instance = prev_cap
                    pk = self.primary_keys.get(table_name, self.primary_key)
                    qualified = f"[{self.schema}].[{table_name}]"
                    offset = 0
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
                            {
                                cols[i]: "" if row[i] is None else str(row[i])
                                for i in range(len(cols))
                            }
                            for row in rows
                        ]
                        offset += len(rows)
                        self._last_event_at = datetime.now(timezone.utc)
                        yield ChangeBatch(
                            inserts=records,
                            resume_token=self._token(
                                lsn=handoff, phase="snapshot", offset=offset, table=table_name
                            ),
                            table=table_name,
                            ack_barrier=False,
                        )
                        if len(rows) < self.batch_size:
                            break
        self.start_lsn = handoff
        self.start_seqval = ""
        self.phase = "streaming"
        self.snapshot_offset = 0
        yield ChangeBatch(
            resume_token=self._token(lsn=self.start_lsn, phase="streaming"),
            ack_barrier=True,
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
        fn = self._changes_tvf()
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
                        FROM {fn}(%s, %s, %s)
                        ORDER BY __$start_lsn, __$seqval
                        """,
                        (
                            _hex_to_lsn(self.start_lsn),
                            _hex_to_lsn(max_lsn),
                            self._row_filter_sql_arg(),
                        ),
                    )
                    cols = [d[0] for d in (cur.description or [])]
                    raw = [
                        {cols[i]: row[i] for i in range(len(cols))}
                        for row in (cur.fetchall() or [])
                    ]
                    inserts, updates, deletes = classify_mssql_cdc_rows(
                        raw, primary_key=self.primary_key, row_filter=self.row_filter
                    )
                    for row in inserts:
                        events.append({"op": "c", "row": row})
                    for row in updates:
                        events.append({"op": "u", "row": row})
                    for pk in deletes:
                        events.append(
                            {"op": "d", "pk": pk, "row": {self.primary_key: pk}}
                        )
        except Exception:
            return events
        return events

    @staticmethod
    def _truncate_at_lsn_boundary(
        rows: list[tuple],
        cols: list[str],
        batch_size: int,
    ) -> tuple[list[tuple], str, str]:
        """Keep only complete ``__$start_lsn`` groups within ``batch_size``.

        Returns (rows_to_apply, next_lsn_hex, next_seqval_hex).
        """
        if not rows:
            return [], "", ""
        try:
            lsn_idx = cols.index("__$start_lsn")
        except ValueError:
            last = rows[-1]
            return rows, _lsn_to_hex(last[0] if last else ""), ""
        try:
            seq_idx = cols.index("__$seqval")
        except ValueError:
            seq_idx = -1

        if len(rows) <= batch_size:
            last = rows[-1]
            return (
                rows,
                _lsn_to_hex(last[lsn_idx]),
                _lsn_to_hex(last[seq_idx]) if seq_idx >= 0 else "",
            )

        # Look-ahead row may share LSN with the batch edge — drop incomplete group.
        edge = rows[batch_size - 1][lsn_idx]
        if rows[batch_size][lsn_idx] == edge:
            keep: list[tuple] = []
            for row in rows[:batch_size]:
                if row[lsn_idx] == edge:
                    break
                keep.append(row)
            if not keep:
                # Entire window is one LSN — process the full window (must not lose
                # mid-txn rows). Caller should use a larger batch if this is common.
                keep = list(rows[: batch_size + 1])
                # Include all fetched rows for this LSN in the look-ahead window.
                for row in rows[batch_size + 1 :]:
                    if row[lsn_idx] != edge:
                        break
                    keep.append(row)
            last = keep[-1]
            return (
                keep,
                _lsn_to_hex(last[lsn_idx]),
                _lsn_to_hex(last[seq_idx]) if seq_idx >= 0 else "",
            )

        keep = list(rows[:batch_size])
        last = keep[-1]
        return (
            keep,
            _lsn_to_hex(last[lsn_idx]),
            _lsn_to_hex(last[seq_idx]) if seq_idx >= 0 else "",
        )

    def poll(self) -> Iterator[ChangeBatch]:
        self._acquire_cdc_lease()
        if self.phase != "streaming" or not self.start_lsn:
            yield from self.snapshot()
            return

        if self._shared:
            yield from self._poll_shared_multi()
            return

        from services.cdc_incremental_runner import interleave_incremental_snapshot

        yield from interleave_incremental_snapshot(
            self.source_key,
            table=self.table,
            fetch_chunk=self._fetch_incremental_chunk,
            stream_events_during_chunk=self._peek_stream_events_during_chunk,
            max_chunks_per_poll=1,
        )

        fn = self._changes_tvf()
        inserts: list[dict[str, Any]] = []
        updates: list[dict[str, Any]] = []
        deletes: list[str] = []
        next_lsn = self.start_lsn
        next_seq = self.start_seqval
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    self._resolve_capture_instance(cur)
                    fn = self._changes_tvf()
                    self._maybe_record_capture_schema(cur, offset=self.start_lsn)
                    assert_resume_lsn_in_retention(
                        self.start_lsn,
                        self._min_lsn(cur),
                        cursor_key=self.cursor_key,
                    )
                    max_lsn = self._max_lsn(cur)
                    if not max_lsn or max_lsn == self.start_lsn:
                        yield ChangeBatch(
                            resume_token=self._token(
                                lsn=self.start_lsn,
                                phase="streaming",
                                seqval=self.start_seqval,
                            ),
                            table=self.table,
                        )
                        return
                    # from_lsn is exclusive of the last fully applied LSN.
                    cur.execute(
                        f"""
                        SELECT TOP ({self.batch_size + 1}) *
                        FROM {fn}(%s, %s, %s)
                        ORDER BY __$start_lsn, __$seqval
                        """,
                        (
                            _hex_to_lsn(self.start_lsn),
                            _hex_to_lsn(max_lsn),
                            self._row_filter_sql_arg(),
                        ),
                    )
                    cols = [d[0] for d in (cur.description or [])]
                    raw_rows = list(cur.fetchall() or [])
                    rows, next_lsn, next_seq = self._truncate_at_lsn_boundary(
                        raw_rows, cols, self.batch_size
                    )
                    records = [
                        {cols[i]: row[i] for i in range(len(cols))} for row in rows
                    ]
                    inserts, updates, deletes = classify_mssql_cdc_rows(
                        records,
                        primary_key=self.primary_key,
                        row_filter=self.row_filter,
                    )
                    if inserts or updates or deletes:
                        self._last_event_at = datetime.now(timezone.utc)
                    if next_lsn:
                        self.start_lsn = next_lsn
                        self.start_seqval = next_seq
                    elif max_lsn:
                        self.start_lsn = max_lsn
        except CdcLsnGapError:
            raise
        except Exception as exc:
            logger.warning("SQL Server native CDC poll failed: %s", exc)
            return

        token = self._token(
            lsn=self.start_lsn, phase="streaming", seqval=self.start_seqval
        )
        if inserts or updates or deletes:
            yield ChangeBatch(
                inserts=inserts,
                updates=updates,
                deletes=deletes,
                resume_token=token,
                table=self.table,
            )
        else:
            yield ChangeBatch(resume_token=token, table=self.table)

    def _poll_shared_multi(self) -> Iterator[ChangeBatch]:
        """Merge LSN-ordered changes across capture instances; demux by table.

        Same commit LSN can touch multiple tables — group by ``__$start_lsn``,
        then emit one :class:`ChangeBatch` per touched table with shared
        ``ack_barrier`` on the last (at-least-once upsert).
        """
        from itertools import groupby

        from services.cdc_multi_table import MultiTableTransactionBuffer

        tagged: list[tuple[str, str, str, dict[str, Any]]] = []
        max_lsn = ""
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    self._resolve_all_captures(cur)
                    # Shared reader: any capture's min_lsn past resume is a gap.
                    for _t, cap in (self._captures or {}).items():
                        if cap:
                            assert_resume_lsn_in_retention(
                                self.start_lsn,
                                self._min_lsn_for(cur, cap),
                                cursor_key=self.cursor_key,
                            )
                    max_lsn = self._max_lsn(cur)
                    if not max_lsn or max_lsn == self.start_lsn:
                        yield ChangeBatch(
                            resume_token=self._token(
                                lsn=self.start_lsn,
                                phase="streaming",
                                seqval=self.start_seqval,
                            ),
                            ack_barrier=True,
                        )
                        return
                    # Per-table look-ahead so merge can form complete LSN groups.
                    per_limit = max(self.batch_size + 1, 64)
                    from_lsn = _hex_to_lsn(self.start_lsn)
                    to_lsn = _hex_to_lsn(max_lsn)
                    filter_arg = self._row_filter_sql_arg()
                    for table_name in self.tables:
                        cap = self._captures.get(table_name) or ""
                        if not cap:
                            continue
                        fn = self._changes_tvf_for(cap)
                        cur.execute(
                            f"""
                            SELECT TOP ({per_limit}) *
                            FROM {fn}(%s, %s, %s)
                            ORDER BY __$start_lsn, __$seqval
                            """,
                            (from_lsn, to_lsn, filter_arg),
                        )
                        cols = [d[0] for d in (cur.description or [])]
                        for row in cur.fetchall() or []:
                            rec = {cols[i]: row[i] for i in range(len(cols))}
                            lsn_h = _lsn_to_hex(rec.get("__$start_lsn"))
                            seq_h = _lsn_to_hex(rec.get("__$seqval"))
                            if not lsn_h:
                                continue
                            tagged.append((lsn_h, seq_h, table_name, rec))
        except CdcLsnGapError:
            raise
        except Exception as exc:
            logger.warning("SQL Server shared CDC poll failed: %s", exc)
            return

        if not tagged:
            self.start_lsn = max_lsn or self.start_lsn
            yield ChangeBatch(
                resume_token=self._token(
                    lsn=self.start_lsn, phase="streaming", seqval=self.start_seqval
                ),
                ack_barrier=True,
            )
            return

        tagged.sort(key=lambda x: (x[0], x[1], x[2]))
        tagged = self._truncate_tagged_at_lsn_boundary(tagged, self.batch_size)

        buf = MultiTableTransactionBuffer()
        last_lsn = self.start_lsn
        last_seq = self.start_seqval
        emitted = False

        for lsn_h, group_iter in groupby(tagged, key=lambda x: x[0]):
            group = list(group_iter)
            buf.begin(lsn_h, lsn=lsn_h)
            # Classify per table so update before/after pairs stay together.
            by_table: dict[str, list[dict[str, Any]]] = {}
            for _, seq_h, table_name, rec in group:
                by_table.setdefault(table_name, []).append(rec)
                last_seq = seq_h or last_seq
            for table_name, records in by_table.items():
                pk = self.primary_keys.get(table_name, self.primary_key)
                inserts, updates, deletes = classify_mssql_cdc_rows(
                    records, primary_key=pk, row_filter=self.row_filter
                )
                for row in inserts:
                    buf.insert(table_name, row, lsn=lsn_h)
                for row in updates:
                    buf.update(table_name, row, lsn=lsn_h)
                for pk_val in deletes:
                    buf.delete(table_name, pk_val, lsn=lsn_h)
            last_lsn = lsn_h
            token = self._token(lsn=last_lsn, phase="streaming", seqval=last_seq)
            for batch in buf.commit(
                lsn=lsn_h, resume_token=token, table_order=self.tables
            ):
                emitted = True
                self._last_event_at = datetime.now(timezone.utc)
                yield batch

        self.start_lsn = last_lsn
        self.start_seqval = last_seq
        if not emitted:
            yield ChangeBatch(
                resume_token=self._token(
                    lsn=self.start_lsn, phase="streaming", seqval=self.start_seqval
                ),
                ack_barrier=True,
            )

    @staticmethod
    def _truncate_tagged_at_lsn_boundary(
        tagged: list[tuple[str, str, str, dict[str, Any]]],
        batch_size: int,
    ) -> list[tuple[str, str, str, dict[str, Any]]]:
        """Keep complete ``__$start_lsn`` groups within ``batch_size`` events."""
        if not tagged or len(tagged) <= batch_size:
            return tagged
        edge = tagged[batch_size - 1][0]
        if tagged[batch_size][0] != edge:
            return tagged[:batch_size]
        # Look-ahead shares LSN with batch edge — drop incomplete group.
        keep: list[tuple[str, str, str, dict[str, Any]]] = []
        for item in tagged[:batch_size]:
            if item[0] == edge:
                break
            keep.append(item)
        if keep:
            return keep
        # Entire window is one LSN — take the full group (must not split).
        keep = list(tagged[: batch_size + 1])
        for item in tagged[batch_size + 1 :]:
            if item[0] != edge:
                break
            keep.append(item)
        return keep

    def ack(self, resume_token: Any = None) -> None:
        if resume_token:
            state = decode_mssql_cdc_token(str(resume_token))
            if state.get("lsn"):
                self.start_lsn = state["lsn"]
            if state.get("seqval") is not None:
                self.start_seqval = str(state.get("seqval") or "")
            if state.get("capture_instance"):
                self.capture_instance = str(state["capture_instance"])
                self._capture_resolved = True
                if self.table:
                    self._captures[self.table] = self.capture_instance

    def lag_seconds(self) -> float | None:
        if self._last_event_at is None:
            return None
        return max(0.0, (datetime.now(timezone.utc) - self._last_event_at).total_seconds())

    def replication_lag_seconds(self) -> float | None:
        return self.lag_seconds()
