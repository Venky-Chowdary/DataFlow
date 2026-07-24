"""Oracle LogMiner CDC — Debezium-class redo log mining.

Uses ``DBMS_LOGMNR`` + ``V$LOGMNR_CONTENTS`` for INSERT/UPDATE/DELETE with SCN
watermarks. Flashback versions (``oracle_change_stream.py``) remain the
fallback when LogMiner privileges or supplemental logging are unavailable.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Iterator

from services.cdc_cursor_gap import CdcScnGapError
from services.cdc_engine import ChangeBatch

logger = logging.getLogger(__name__)

_OP_MAP = {
    "INSERT": "insert",
    "UPDATE": "update",
    "DELETE": "delete",
}


def encode_logminer_token(scn: int, *, table: str, phase: str = "streaming") -> str:
    return json.dumps(
        {"kind": "oracle-logminer", "table": table, "scn": int(scn), "phase": phase},
        separators=(",", ":"),
    )


def decode_logminer_token(token: str | None) -> dict[str, Any]:
    if not token:
        return {"scn": 0, "phase": "initial", "table": ""}
    try:
        data = json.loads(str(token))
        if isinstance(data, dict) and data.get("kind") == "oracle-logminer":
            return {
                "scn": int(data.get("scn") or 0),
                "phase": str(data.get("phase") or "streaming"),
                "table": str(data.get("table") or ""),
            }
    except Exception:
        pass
    return {"scn": 0, "phase": "initial", "table": ""}


# LogMiner / redo missing-file class errors (Oracle).
_ORA_REDO_GAP_CODES = (
    "ORA-01291",  # missing logfile
    "ORA-01292",  # no log file found
    "ORA-01284",  # file cannot be opened
    "ORA-01332",  # LogMiner dictionary build from redo failed / SCN out of range
)


def is_oracle_redo_gap_error(exc: BaseException) -> bool:
    msg = str(exc).upper()
    return any(code in msg for code in _ORA_REDO_GAP_CODES)


def assert_resume_scn_in_redo(
    resume_scn: int,
    oldest_available_scn: int | None,
    *,
    cursor_key: str = "",
) -> None:
    """Raise :class:`CdcScnGapError` when resume is strictly before retained redo.

    ``oldest_available_scn`` is the min ``FIRST_CHANGE#`` across online + archived
    redo still present. ``None`` / ``0`` means undetermined — do not false-positive.
    """
    resume = int(resume_scn or 0)
    oldest = int(oldest_available_scn or 0)
    if resume <= 0 or oldest <= 0:
        return
    if resume < oldest:
        raise CdcScnGapError(
            "Oracle LogMiner resume SCN is before available redo "
            f"(resume={resume}, oldest_available={oldest}). Likely archived log "
            "purge or RAC/Data Guard failover gap — re-snapshot / reset watermark; "
            "do not claim continuous CDC across the gap.",
            resume_scn=resume,
            oldest_scn=oldest,
            cursor_key=cursor_key,
        )


def fetch_oldest_available_scn(cur: Any) -> int | None:
    """Return oldest ``FIRST_CHANGE#`` still present in online/archived redo.

    Fail-open (``None``) when ``V$LOG`` / ``V$ARCHIVED_LOG`` are unavailable so
    privilege gaps do not block CDC; gap detection still covers LogMiner ORA-*.
    """
    candidates: list[int] = []
    try:
        cur.execute("SELECT MIN(FIRST_CHANGE#) FROM V$LOG")
        row = cur.fetchone()
        if row and row[0] is not None:
            candidates.append(int(row[0]))
    except Exception as exc:
        logger.debug("Oracle V$LOG FIRST_CHANGE# unavailable: %s", exc)
    try:
        cur.execute(
            """
            SELECT MIN(FIRST_CHANGE#)
            FROM V$ARCHIVED_LOG
            WHERE DELETED = 'NO'
            """
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            candidates.append(int(row[0]))
    except Exception as exc:
        logger.debug("Oracle V$ARCHIVED_LOG FIRST_CHANGE# unavailable: %s", exc)
    if not candidates:
        return None
    return min(candidates)


_SET_RE = re.compile(r'"?(\w+)"?\s*=\s*(?:\'([^\']*)\'|([^\s,]+))')


def _split_sql_csv_aware(text: str) -> list[str]:
    """Split a SQL list on commas outside quotes and parentheses.

    Handles ``'a,b'``, ``''`` escapes, and ``TO_DATE('…','…')`` so LogMiner
    INSERT/UPDATE parsing does not corrupt valid CDC rows (Airbyte-class gap).
    """
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    in_str = False
    i = 0
    while i < len(text):
        ch = text[i]
        if in_str:
            buf.append(ch)
            if ch == "'":
                if i + 1 < len(text) and text[i + 1] == "'":
                    buf.append(text[i + 1])
                    i += 2
                    continue
                in_str = False
            i += 1
            continue
        if ch == "'":
            in_str = True
            buf.append(ch)
            i += 1
            continue
        if ch == "(":
            depth += 1
            buf.append(ch)
            i += 1
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            buf.append(ch)
            i += 1
            continue
        if ch == "," and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail or parts:
        parts.append(tail)
    return parts


def _unquote_sql_literal(value: str) -> str:
    v = (value or "").strip()
    if not v or v.upper() == "NULL":
        return ""
    if len(v) >= 2 and v[0] == "'" and v[-1] == "'":
        return v[1:-1].replace("''", "'")
    return v


def _parse_sql_redo(sql_redo: str, *, op: str) -> dict[str, str]:
    """Column extraction from LogMiner SQL_REDO text (quoted / function-aware)."""
    out: dict[str, str] = {}
    if not sql_redo:
        return out
    text = sql_redo
    if op == "insert" and "VALUES" in text.upper():
        # INSERT INTO t("A","B") VALUES('1','a,b') / TO_DATE(...)
        cols_m = re.search(r"\((.*)\)\s*VALUES\s*\((.*)\)\s*$", text, re.I | re.S)
        if not cols_m:
            # Fallback: first (...) VALUES (...) pair (legacy LogMiner shapes).
            cols_m = re.search(r"\(([^;]+)\)\s*VALUES\s*\(([^;]+)\)", text, re.I | re.S)
        if cols_m:
            cols = [c.strip().strip('"') for c in _split_sql_csv_aware(cols_m.group(1))]
            vals = [_unquote_sql_literal(v) for v in _split_sql_csv_aware(cols_m.group(2))]
            if len(cols) != len(vals):
                # Refuse to invent misaligned columns — surface as unparsed.
                return {
                    "_df_unparsed_sql_redo": "1",
                    "_df_parse_error": f"insert col/val mismatch ({len(cols)} vs {len(vals)})",
                }
            for c, v in zip(cols, vals):
                if c:
                    out[c.upper()] = v
            return out
    # UPDATE … SET "COL"=… / DELETE … WHERE "COL"=…
    # Parse WHERE first (row identity / old values) then SET (new values) so a
    # column updated by SET overwrites the WHERE value, while still surfacing
    # primary-key columns from the WHERE clause for updates.
    set_m = re.search(r"\bSET\s+(.+?)(?:\s+WHERE\b|$)", text, re.I | re.S)
    where_m = re.search(r"\bWHERE\s+(.+)$", text, re.I | re.S)
    chunks: list[str] = []
    if where_m and op in {"update", "delete"}:
        chunks.extend(_split_sql_csv_aware(where_m.group(1)))
    if set_m:
        chunks.extend(_split_sql_csv_aware(set_m.group(1)))
    if chunks:
        for chunk in chunks:
            m = re.match(r'"?(\w+)"?\s*=\s*(.+)$', chunk.strip(), re.I | re.S)
            if not m:
                continue
            col = m.group(1).upper()
            out[col] = _unquote_sql_literal(m.group(2).strip())
        if out:
            return out
    for m in _SET_RE.finditer(text):
        col = m.group(1).upper()
        val = m.group(2) if m.group(2) is not None else m.group(3)
        out[col] = "" if val is None or str(val).upper() == "NULL" else str(val)
    return out


class OracleLogMinerCdc:
    """Continuous LogMiner mining between SCN watermarks.

    Pass ``table`` as a list (and optional ``primary_keys``) to share one
    LogMiner session across N tables — Debezium-class demux with ``ack_barrier``.
    Delivery remains **at-least-once**.
    """

    def __init__(
        self,
        cfg: dict[str, Any],
        *,
        table: str | list[str],
        primary_key: str = "ID",
        primary_keys: dict[str, str] | None = None,
        schema: str = "",
        batch_size: int = 500,
        resume_token: str | None = None,
        cursor_key: str = "",
    ) -> None:
        from services.cdc_multi_table import normalize_table_list, tables_digest

        self.cfg = cfg
        raw_tables = normalize_table_list(table)
        if not raw_tables:
            raise ValueError("Oracle LogMiner CDC requires at least one table")
        self.tables = [t.upper() for t in raw_tables]
        self.table = self.tables[0]
        self.schema = (schema or cfg.get("schema") or cfg.get("username") or "").upper()
        self.primary_keys: dict[str, str] = {
            t: str((primary_keys or {}).get(t) or (primary_keys or {}).get(t.lower()) or primary_key or "ID").upper()
            for t in self.tables
        }
        # Also accept original-case keys from callers.
        if primary_keys:
            for k, v in primary_keys.items():
                if k and v:
                    self.primary_keys[str(k).upper()] = str(v).upper()
        self.primary_key = self.primary_keys.get(self.table, (primary_key or "ID").upper())
        self.batch_size = max(1, int(batch_size or 500))
        self._shared = len(self.tables) > 1
        state = decode_logminer_token(resume_token)
        self.scn = int(state.get("scn") or 0)
        self.phase = str(state.get("phase") or "initial")
        self._last_event_at: datetime | None = None
        from services.cdc_schema_history import connection_fingerprint

        self.source_key = connection_fingerprint(
            {**cfg, "type": "oracle"},
            connector_id=str(cfg.get("connector_id") or ""),
        )
        digest = tables_digest(self.tables)
        self.cursor_key = cursor_key or (
            f"oracle-logminer-shared:{self.schema}:{digest}"
            if self._shared
            else f"oracle-logminer:{self.schema}.{self.table}"
        )
        from services.cdc_lease import (
            CdcLeaseGuard,
            oracle_cdc_resource,
            oracle_cdc_shared_resource,
        )

        host = str(cfg.get("host") or "")
        resource = (
            oracle_cdc_shared_resource(
                self.schema, self.tables, mode="logminer", host=host
            )
            if self._shared
            else oracle_cdc_resource(
                self.schema, self.table, mode="logminer", host=host
            )
        )
        self._lease = CdcLeaseGuard(
            cursor_key=self.cursor_key,
            resource=resource,
            holder_id=str(cfg.get("lease_holder_id") or ""),
            job_id=str(cfg.get("job_id") or ""),
            meta={
                "engine": "oracle_logminer",
                "table": self.table,
                "tables": list(self.tables),
                "shared_reader": self._shared,
            },
        )

    def _acquire_cdc_lease(self) -> None:
        self._lease.ensure()

    def close(self) -> None:
        self._lease.release()

    def cdc_metadata(self) -> dict[str, Any]:
        return {
            "plugin": "oracle_logminer",
            "phase": self.phase,
            "delivery": "at-least-once",
            "tables": list(self.tables),
            "shared_reader": self._shared,
            **self._lease.theater_fields(),
        }

    def _conn(self):
        from connectors.generic_sql import get_connection

        return get_connection(
            host=self.cfg.get("host") or "localhost",
            port=self.cfg.get("port") or 1521,
            database=self.cfg.get("database") or self.cfg.get("service_name") or "ORCL",
            username=self.cfg.get("username") or "",
            password=self.cfg.get("password") or "",
            connection_string=self.cfg.get("connection_string") or "",
            ssl=bool(self.cfg.get("ssl")),
            db_type="oracle",
        )

    def _qualified(self, table: str | None = None) -> str:
        tbl = (table or self.table).upper()
        if self.schema:
            return f'"{self.schema}"."{tbl}"'
        return f'"{tbl}"'

    def _token_table_label(self) -> str:
        if self._shared:
            return ",".join(self.tables)
        return self.table

    def _table_in_sql(self) -> str:
        """Safe IN-list for LogMiner TABLE_NAME filter (uppercased identifiers)."""
        from connectors.sql_identifiers import require_safe_identifier

        parts = []
        for t in self.tables:
            safe = require_safe_identifier(t, preserve_case=True).upper()
            parts.append(f"'{safe}'")
        return ", ".join(parts)

    def is_available(self) -> bool:
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT current_scn FROM v$database")
                    if not cur.fetchone():
                        return False
                    # Privilege / dictionary probe
                    cur.execute("SELECT COUNT(*) FROM v$logmnr_contents WHERE ROWNUM < 1")
                    cur.fetchone()
                    return True
        except Exception as exc:
            logger.debug("Oracle LogMiner unavailable: %s", exc)
            return False

    def snapshot(self) -> Iterator[ChangeBatch]:
        self._acquire_cdc_lease()
        if self._shared:
            yield from self._snapshot_shared()
            return
        offset = 0
        handoff = 0
        pk = self.primary_key
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_scn FROM v$database")
                row = cur.fetchone()
                handoff = int(row[0] or 0) if row else 0
                while True:
                    cur.execute(
                        f"""
                        SELECT * FROM (
                          SELECT t.*, ROW_NUMBER() OVER (ORDER BY t."{pk}") AS df_rn
                          FROM {self._qualified()} t
                        ) WHERE df_rn > :off AND df_rn <= :lim
                        """,
                        {"off": offset, "lim": offset + self.batch_size},
                    )
                    cols = [d[0] for d in (cur.description or [])]
                    rows = cur.fetchall() or []
                    if not rows:
                        break
                    clean_cols = [c for c in cols if str(c).upper() != "DF_RN"]
                    rn_idx = next((i for i, c in enumerate(cols) if str(c).upper() == "DF_RN"), None)
                    records = []
                    for row in rows:
                        values = [row[i] for i in range(len(cols)) if i != rn_idx]
                        records.append(
                            {
                                str(clean_cols[i]).upper(): "" if values[i] is None else str(values[i])
                                for i in range(len(clean_cols))
                            }
                        )
                    offset += len(rows)
                    yield ChangeBatch(
                        inserts=records,
                        resume_token=encode_logminer_token(
                            handoff, table=self.table, phase="snapshot"
                        ),
                        table=self.table,
                    )
                    if len(rows) < self.batch_size:
                        break
        self.scn = handoff
        self.phase = "streaming"
        yield ChangeBatch(
            resume_token=encode_logminer_token(
                self.scn, table=self._token_table_label(), phase="streaming"
            ),
            table=self.table,
        )

    def _snapshot_shared(self) -> Iterator[ChangeBatch]:
        """Multi-table initial dump under one SCN handoff (at-least-once)."""
        handoff = 0
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_scn FROM v$database")
                row = cur.fetchone()
                handoff = int(row[0] or 0) if row else 0
                for table_name in self.tables:
                    pk = self.primary_keys.get(table_name, self.primary_key)
                    offset = 0
                    while True:
                        cur.execute(
                            f"""
                            SELECT * FROM (
                              SELECT t.*, ROW_NUMBER() OVER (ORDER BY t."{pk}") AS df_rn
                              FROM {self._qualified(table_name)} t
                            ) WHERE df_rn > :off AND df_rn <= :lim
                            """,
                            {"off": offset, "lim": offset + self.batch_size},
                        )
                        cols = [d[0] for d in (cur.description or [])]
                        rows = cur.fetchall() or []
                        if not rows:
                            break
                        clean_cols = [c for c in cols if str(c).upper() != "DF_RN"]
                        rn_idx = next(
                            (i for i, c in enumerate(cols) if str(c).upper() == "DF_RN"),
                            None,
                        )
                        records = []
                        for row in rows:
                            values = [row[i] for i in range(len(cols)) if i != rn_idx]
                            records.append(
                                {
                                    str(clean_cols[i]).upper(): (
                                        "" if values[i] is None else str(values[i])
                                    )
                                    for i in range(len(clean_cols))
                                }
                            )
                        offset += len(rows)
                        self._last_event_at = datetime.now(timezone.utc)
                        yield ChangeBatch(
                            inserts=records,
                            resume_token=encode_logminer_token(
                                handoff, table=table_name, phase="snapshot"
                            ),
                            table=table_name,
                            ack_barrier=False,
                        )
                        if len(rows) < self.batch_size:
                            break
        self.scn = handoff
        self.phase = "streaming"
        yield ChangeBatch(
            resume_token=encode_logminer_token(
                self.scn, table=self._token_table_label(), phase="streaming"
            ),
            ack_barrier=True,
        )

    def _fetch_incremental_chunk(self, sig: Any) -> tuple[list[dict[str, Any]], str | None, bool]:
        """PK-ordered chunk for signal-driven incremental snapshots."""
        pk = (sig.primary_key or self.primary_key or "ID").upper()
        limit = int(sig.chunk_size or self.batch_size)
        last_pk = sig.last_pk or ""
        with self._conn() as conn:
            with conn.cursor() as cur:
                if last_pk:
                    cur.execute(
                        f"""
                        SELECT * FROM (
                          SELECT t.*, ROW_NUMBER() OVER (ORDER BY t."{pk}") AS df_rn
                          FROM {self._qualified()} t
                          WHERE t."{pk}" > :last_pk
                        ) WHERE df_rn <= :lim
                        """,
                        {"last_pk": last_pk, "lim": limit},
                    )
                else:
                    cur.execute(
                        f"""
                        SELECT * FROM (
                          SELECT t.*, ROW_NUMBER() OVER (ORDER BY t."{pk}") AS df_rn
                          FROM {self._qualified()} t
                        ) WHERE df_rn <= :lim
                        """,
                        {"lim": limit},
                    )
                cols = [d[0] for d in (cur.description or [])]
                rows = cur.fetchall() or []
        clean_cols = [c for c in cols if str(c).upper() != "DF_RN"]
        rn_idx = next((i for i, c in enumerate(cols) if str(c).upper() == "DF_RN"), None)
        records = []
        for row in rows:
            values = [row[i] for i in range(len(cols)) if i != rn_idx]
            records.append(
                {
                    str(clean_cols[i]).upper(): "" if values[i] is None else str(values[i])
                    for i in range(len(clean_cols))
                }
            )
        new_last = records[-1].get(pk) if records else last_pk
        done = len(records) < limit
        return records, str(new_last) if new_last is not None else last_pk, done

    def _peek_stream_events_during_chunk(self, sig: Any) -> list[dict[str, Any]]:
        """Non-acking LogMiner peek for DDD-3 stream-wins (does not advance SCN)."""
        events: list[dict[str, Any]] = []
        if self.scn <= 0:
            return events
        peek_limit = min(int(sig.chunk_size or self.batch_size), 200)
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT current_scn FROM v$database")
                    head = cur.fetchone()
                    end_scn = int(head[0] or self.scn) if head else self.scn
                    if end_scn <= self.scn:
                        return events
                    cur.execute(
                        """
                        BEGIN
                          DBMS_LOGMNR.START_LOGMNR(
                            STARTSCN => :start_scn,
                            ENDSCN => :end_scn,
                            OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
                                      + DBMS_LOGMNR.CONTINUOUS_MINE
                          );
                        END;
                        """,
                        {"start_scn": self.scn + 1, "end_scn": end_scn},
                    )
                    cur.execute(
                        """
                        SELECT SCN, OPERATION, SQL_REDO
                        FROM v$logmnr_contents
                        WHERE SEG_OWNER = :owner
                          AND TABLE_NAME = :tbl
                          AND OPERATION IN ('INSERT','UPDATE','DELETE')
                          AND SCN > :start_scn
                          AND ROWNUM <= :lim
                        ORDER BY SCN
                        """,
                        {
                            "owner": self.schema,
                            "tbl": self.table,
                            "start_scn": self.scn,
                            "lim": peek_limit,
                        },
                    )
                    for _scn, operation, sql_redo in cur.fetchall() or []:
                        op = _OP_MAP.get(str(operation or "").upper())
                        if not op:
                            continue
                        row = _parse_sql_redo(sql_redo or "", op=op)
                        key = row.get(self.primary_key, "")
                        if op == "delete" and key:
                            events.append({"op": "d", "pk": key, "row": {self.primary_key: key}})
                        elif op == "insert":
                            events.append({"op": "c", "row": row})
                        else:
                            events.append({"op": "u", "row": row})
                    try:
                        cur.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;")
                    except Exception:
                        pass
        except Exception:
            return events
        return events

    def poll(self) -> Iterator[ChangeBatch]:
        self._acquire_cdc_lease()
        if self.phase != "streaming" or self.scn <= 0:
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

        inserts: list[dict[str, Any]] = []
        updates: list[dict[str, Any]] = []
        deletes: list[str] = []
        next_scn = self.scn
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    assert_resume_scn_in_redo(
                        self.scn,
                        fetch_oldest_available_scn(cur),
                        cursor_key=self.cursor_key,
                    )
                    cur.execute("SELECT current_scn FROM v$database")
                    head = cur.fetchone()
                    end_scn = int(head[0] or self.scn) if head else self.scn
                    if end_scn <= self.scn:
                        yield ChangeBatch(
                            resume_token=encode_logminer_token(
                                self.scn, table=self.table, phase="streaming"
                            ),
                            table=self.table,
                        )
                        return
                    # Start LogMiner on online redo for the SCN window.
                    cur.execute(
                        """
                        BEGIN
                          DBMS_LOGMNR.START_LOGMNR(
                            STARTSCN => :start_scn,
                            ENDSCN => :end_scn,
                            OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
                                      + DBMS_LOGMNR.CONTINUOUS_MINE
                          );
                        END;
                        """,
                        {"start_scn": self.scn + 1, "end_scn": end_scn},
                    )
                    cur.execute(
                        """
                        SELECT SCN, OPERATION, SQL_REDO, TABLE_NAME, SEG_OWNER
                        FROM v$logmnr_contents
                        WHERE SEG_OWNER = :owner
                          AND TABLE_NAME = :tbl
                          AND OPERATION IN ('INSERT','UPDATE','DELETE')
                          AND SCN > :start_scn
                          AND ROWNUM <= :lim
                        ORDER BY SCN
                        """,
                        {
                            "owner": self.schema,
                            "tbl": self.table,
                            "start_scn": self.scn,
                            "lim": self.batch_size,
                        },
                    )
                    for scn, operation, sql_redo, _tbl, _owner in cur.fetchall() or []:
                        next_scn = max(next_scn, int(scn or 0))
                        op = _OP_MAP.get(str(operation or "").upper())
                        if not op:
                            continue
                        self._last_event_at = datetime.now(timezone.utc)
                        row = _parse_sql_redo(sql_redo or "", op=op)
                        key = row.get(self.primary_key, "")
                        if op == "delete":
                            if key:
                                deletes.append(key)
                        elif op == "insert":
                            inserts.append(row)
                        else:
                            updates.append(row)
                    try:
                        cur.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;")
                    except Exception:
                        pass
                    self.scn = max(next_scn, end_scn)
        except CdcScnGapError:
            raise
        except Exception as exc:
            if is_oracle_redo_gap_error(exc):
                raise CdcScnGapError(
                    f"Oracle LogMiner redo unavailable for resume SCN {self.scn}: {exc}",
                    resume_scn=self.scn,
                    cursor_key=self.cursor_key,
                ) from exc
            logger.warning("Oracle LogMiner poll failed: %s", exc)
            return

        token = encode_logminer_token(self.scn, table=self.table, phase="streaming")
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
        """One LogMiner session for N tables; demux by XID/SCN with ack_barrier."""
        from itertools import groupby

        from services.cdc_multi_table import MultiTableTransactionBuffer

        table_set = {t.upper() for t in self.tables}
        table_by_lower = {t.lower(): t for t in self.tables}
        tagged: list[tuple[str, int, str, str, dict[str, Any]]] = []
        # (xid_key, scn, table, op, row)
        end_scn = self.scn
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    assert_resume_scn_in_redo(
                        self.scn,
                        fetch_oldest_available_scn(cur),
                        cursor_key=self.cursor_key,
                    )
                    cur.execute("SELECT current_scn FROM v$database")
                    head = cur.fetchone()
                    end_scn = int(head[0] or self.scn) if head else self.scn
                    if end_scn <= self.scn:
                        yield ChangeBatch(
                            resume_token=encode_logminer_token(
                                self.scn,
                                table=self._token_table_label(),
                                phase="streaming",
                            ),
                            ack_barrier=True,
                        )
                        return
                    cur.execute(
                        """
                        BEGIN
                          DBMS_LOGMNR.START_LOGMNR(
                            STARTSCN => :start_scn,
                            ENDSCN => :end_scn,
                            OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
                                      + DBMS_LOGMNR.CONTINUOUS_MINE
                          );
                        END;
                        """,
                        {"start_scn": self.scn + 1, "end_scn": end_scn},
                    )
                    in_list = self._table_in_sql()
                    # Look-ahead past batch_size so we can keep complete XID groups.
                    lim = max(self.batch_size + 1, 64) * max(1, len(self.tables))
                    cur.execute(
                        f"""
                        SELECT SCN, OPERATION, SQL_REDO, TABLE_NAME, SEG_OWNER,
                               XIDUSN, XIDSLT, XIDSEQ
                        FROM v$logmnr_contents
                        WHERE SEG_OWNER = :owner
                          AND TABLE_NAME IN ({in_list})
                          AND OPERATION IN ('INSERT','UPDATE','DELETE')
                          AND SCN > :start_scn
                          AND ROWNUM <= :lim
                        ORDER BY SCN, XIDUSN, XIDSLT, XIDSEQ
                        """,
                        {
                            "owner": self.schema,
                            "start_scn": self.scn,
                            "lim": lim,
                        },
                    )
                    for row in cur.fetchall() or []:
                        scn = int(row[0] or 0)
                        operation = row[1]
                        sql_redo = row[2]
                        tbl_raw = str(row[3] or "").upper()
                        xid_key = f"{row[5] or 0}.{row[6] or 0}.{row[7] or 0}"
                        if not xid_key or xid_key == "0.0.0":
                            xid_key = f"scn:{scn}"
                        op = _OP_MAP.get(str(operation or "").upper())
                        if not op or tbl_raw not in table_set:
                            continue
                        table_name = table_by_lower.get(tbl_raw.lower(), tbl_raw)
                        parsed = _parse_sql_redo(sql_redo or "", op=op)
                        tagged.append((xid_key, scn, table_name, op, parsed))
                    try:
                        cur.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;")
                    except Exception:
                        pass
        except CdcScnGapError:
            raise
        except Exception as exc:
            if is_oracle_redo_gap_error(exc):
                raise CdcScnGapError(
                    f"Oracle LogMiner redo unavailable for resume SCN {self.scn}: {exc}",
                    resume_scn=self.scn,
                    cursor_key=self.cursor_key,
                ) from exc
            logger.warning("Oracle shared LogMiner poll failed: %s", exc)
            return

        if not tagged:
            self.scn = max(self.scn, end_scn)
            yield ChangeBatch(
                resume_token=encode_logminer_token(
                    self.scn, table=self._token_table_label(), phase="streaming"
                ),
                ack_barrier=True,
            )
            return

        tagged = self._truncate_tagged_at_xid_boundary(tagged, self.batch_size)
        buf = MultiTableTransactionBuffer()
        max_scn = self.scn
        emitted = False

        for xid_key, group_iter in groupby(tagged, key=lambda x: x[0]):
            group = list(group_iter)
            group_scn = max(item[1] for item in group)
            buf.begin(xid_key, lsn=str(group_scn))
            for _xid, scn, table_name, op, row in group:
                pk = self.primary_keys.get(table_name, self.primary_key)
                key = row.get(pk, "")
                if op == "delete":
                    if key:
                        buf.delete(table_name, str(key), lsn=str(scn))
                elif op == "insert":
                    buf.insert(table_name, row, lsn=str(scn))
                else:
                    buf.update(table_name, row, lsn=str(scn))
            max_scn = max(max_scn, group_scn)
            token = encode_logminer_token(
                max_scn, table=self._token_table_label(), phase="streaming"
            )
            for batch in buf.commit(
                lsn=str(group_scn), resume_token=token, table_order=self.tables
            ):
                emitted = True
                self._last_event_at = datetime.now(timezone.utc)
                yield batch

        self.scn = max(max_scn, end_scn) if emitted else max(self.scn, end_scn)
        if not emitted:
            yield ChangeBatch(
                resume_token=encode_logminer_token(
                    self.scn, table=self._token_table_label(), phase="streaming"
                ),
                ack_barrier=True,
            )

    @staticmethod
    def _truncate_tagged_at_xid_boundary(
        tagged: list[tuple[str, int, str, str, dict[str, Any]]],
        batch_size: int,
    ) -> list[tuple[str, int, str, str, dict[str, Any]]]:
        """Keep complete XID groups within ``batch_size`` events."""
        if not tagged or len(tagged) <= batch_size:
            return tagged
        edge = tagged[batch_size - 1][0]
        if tagged[batch_size][0] != edge:
            return tagged[:batch_size]
        keep: list[tuple[str, int, str, str, dict[str, Any]]] = []
        for item in tagged[:batch_size]:
            if item[0] == edge:
                break
            keep.append(item)
        if keep:
            return keep
        keep = list(tagged[: batch_size + 1])
        for item in tagged[batch_size + 1 :]:
            if item[0] != edge:
                break
            keep.append(item)
        return keep

    def ack(self, resume_token: Any = None) -> None:
        if resume_token:
            state = decode_logminer_token(str(resume_token))
            self.scn = int(state.get("scn") or self.scn)

    def lag_seconds(self) -> float | None:
        if self._last_event_at is None:
            return None
        return max(0.0, (datetime.now(timezone.utc) - self._last_event_at).total_seconds())

    def replication_lag_seconds(self) -> float | None:
        return self.lag_seconds()
