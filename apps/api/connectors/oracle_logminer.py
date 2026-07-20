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


_SET_RE = re.compile(r'"?(\w+)"?\s*=\s*(?:\'([^\']*)\'|([^\s,]+))')


def _parse_sql_redo(sql_redo: str, *, op: str) -> dict[str, str]:
    """Best-effort column extraction from LogMiner SQL_REDO text."""
    out: dict[str, str] = {}
    if not sql_redo:
        return out
    text = sql_redo
    if op == "insert" and "VALUES" in text.upper():
        # INSERT INTO t("A","B") VALUES('1','2')
        cols_m = re.search(r"\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)", text, re.I)
        if cols_m:
            cols = [c.strip().strip('"') for c in cols_m.group(1).split(",")]
            vals = [v.strip().strip("'") for v in cols_m.group(2).split(",")]
            for c, v in zip(cols, vals):
                out[c.upper()] = "" if v.upper() == "NULL" else v
            return out
    for m in _SET_RE.finditer(text):
        col = m.group(1).upper()
        val = m.group(2) if m.group(2) is not None else m.group(3)
        out[col] = "" if val is None or str(val).upper() == "NULL" else str(val)
    return out


class OracleLogMinerCdc:
    """Continuous LogMiner mining between SCN watermarks."""

    def __init__(
        self,
        cfg: dict[str, Any],
        *,
        table: str,
        primary_key: str,
        schema: str = "",
        batch_size: int = 500,
        resume_token: str | None = None,
        cursor_key: str = "",
    ) -> None:
        self.cfg = cfg
        self.table = table.upper()
        self.schema = (schema or cfg.get("schema") or cfg.get("username") or "").upper()
        self.primary_key = (primary_key or "ID").upper()
        self.batch_size = max(1, int(batch_size or 500))
        state = decode_logminer_token(resume_token)
        self.scn = int(state.get("scn") or 0)
        self.phase = str(state.get("phase") or "initial")
        self._last_event_at: datetime | None = None
        from services.cdc_schema_history import connection_fingerprint

        self.source_key = connection_fingerprint(
            {**cfg, "type": "oracle"},
            connector_id=str(cfg.get("connector_id") or ""),
        )
        self.cursor_key = cursor_key or f"oracle-logminer:{self.schema}.{self.table}"
        from services.cdc_lease import CdcLeaseGuard, oracle_cdc_resource

        self._lease = CdcLeaseGuard(
            cursor_key=self.cursor_key,
            resource=oracle_cdc_resource(
                self.schema,
                self.table,
                mode="logminer",
                host=str(cfg.get("host") or ""),
            ),
            holder_id=str(cfg.get("lease_holder_id") or ""),
            job_id=str(cfg.get("job_id") or ""),
            meta={"engine": "oracle_logminer", "table": self.table},
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

    def _qualified(self) -> str:
        if self.schema:
            return f'"{self.schema}"."{self.table}"'
        return f'"{self.table}"'

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
                    )
                    if len(rows) < self.batch_size:
                        break
        self.scn = handoff
        self.phase = "streaming"
        yield ChangeBatch(
            resume_token=encode_logminer_token(self.scn, table=self.table, phase="streaming")
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
                    cur.execute("SELECT current_scn FROM v$database")
                    head = cur.fetchone()
                    end_scn = int(head[0] or self.scn) if head else self.scn
                    if end_scn <= self.scn:
                        yield ChangeBatch(
                            resume_token=encode_logminer_token(
                                self.scn, table=self.table, phase="streaming"
                            )
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
        except Exception as exc:
            logger.warning("Oracle LogMiner poll failed: %s", exc)
            return

        token = encode_logminer_token(self.scn, table=self.table, phase="streaming")
        if inserts or updates or deletes:
            yield ChangeBatch(inserts=inserts, updates=updates, deletes=deletes, resume_token=token)
        else:
            yield ChangeBatch(resume_token=token)

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
