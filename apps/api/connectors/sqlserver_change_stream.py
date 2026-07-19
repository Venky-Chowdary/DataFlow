"""SQL Server Change Tracking CDC — full initial snapshot + incremental CT.

Airbyte-class path:
  1. ``snapshot()`` dumps the table in PK-ordered batches and records
     ``CHANGE_TRACKING_CURRENT_VERSION()`` as the handoff watermark.
  2. ``poll()`` reads ``CHANGETABLE(CHANGES …)`` and hydrates I/U rows from
     the live table; deletes emit PK tombstones.
  3. Watermark advance after destination apply is the ack (at-least-once).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Iterator

from services.cdc_engine import ChangeBatch

logger = logging.getLogger(__name__)


def encode_sqlserver_resume_token(
    version: int,
    *,
    table: str,
    phase: str = "streaming",
    offset: int = 0,
) -> str:
    return json.dumps(
        {
            "kind": "mssql-ct",
            "table": table,
            "version": int(version),
            "phase": phase,
            "offset": int(offset),
        },
        separators=(",", ":"),
    )


def decode_sqlserver_resume_token(token: str | None) -> dict[str, Any]:
    if not token:
        return {"version": 0, "phase": "initial", "offset": 0, "table": ""}
    raw = str(token).strip()
    if raw.startswith("mssql-ct:"):
        # Legacy compact form: mssql-ct:{table}:{version}
        try:
            version = int(raw.rsplit(":", 1)[-1])
        except Exception:
            version = 0
        return {"version": version, "phase": "streaming", "offset": 0, "table": ""}
    try:
        data = json.loads(raw)
        if isinstance(data, dict) and data.get("kind") == "mssql-ct":
            return {
                "version": int(data.get("version") or 0),
                "phase": str(data.get("phase") or "streaming"),
                "offset": int(data.get("offset") or 0),
                "table": str(data.get("table") or ""),
            }
    except Exception:
        pass
    try:
        return {"version": int(raw), "phase": "streaming", "offset": 0, "table": ""}
    except Exception:
        return {"version": 0, "phase": "initial", "offset": 0, "table": ""}


class SqlServerChangeTrackingCdc:
    """Change Tracking CDC with real initial table dump."""

    def __init__(
        self,
        cfg: dict[str, Any],
        *,
        table: str,
        primary_key: str,
        schema: str = "dbo",
        batch_size: int = 500,
        resume_token: str | None = None,
        columns: list[str] | None = None,
    ) -> None:
        self.cfg = cfg
        self.table = table
        self.schema = schema or "dbo"
        self.primary_key = primary_key or "id"
        self.batch_size = max(1, int(batch_size or 500))
        self.columns = columns
        state = decode_sqlserver_resume_token(resume_token)
        self.version = int(state.get("version") or 0)
        self.phase = str(state.get("phase") or "initial")
        self.snapshot_offset = int(state.get("offset") or 0)
        self._last_event_at: datetime | None = None

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

    def _qualified(self) -> str:
        return f"[{self.schema}].[{self.table}]"

    def _current_version(self, cur) -> int:
        cur.execute("SELECT CHANGE_TRACKING_CURRENT_VERSION()")
        row = cur.fetchone()
        return int(row[0] or 0) if row else 0

    def is_available(self) -> bool:
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID()"
                    )
                    if not cur.fetchone():
                        return False
                    cur.execute(
                        """
                        SELECT 1
                        FROM sys.change_tracking_tables ct
                        JOIN sys.tables t ON t.object_id = ct.object_id
                        JOIN sys.schemas s ON s.schema_id = t.schema_id
                        WHERE t.name = %s AND s.name = %s
                        """,
                        (self.table, self.schema),
                    )
                    return cur.fetchone() is not None
        except Exception as exc:
            logger.debug("SQL Server CT unavailable for %s.%s: %s", self.schema, self.table, exc)
            return False

    def _row_to_record(self, cols: list[str], row: tuple) -> dict[str, str]:
        return {cols[i]: "" if row[i] is None else str(row[i]) for i in range(len(cols))}

    def snapshot(self) -> Iterator[ChangeBatch]:
        """Full table dump + CT version handoff (Airbyte initial sync)."""
        qualified = self._qualified()
        pk = self.primary_key
        offset = self.snapshot_offset if self.phase == "snapshot" else 0
        handoff_version = 0
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    handoff_version = self._current_version(cur)
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
                        records = [self._row_to_record(cols, row) for row in rows]
                        offset += len(rows)
                        self._last_event_at = datetime.now(timezone.utc)
                        yield ChangeBatch(
                            inserts=records,
                            resume_token=encode_sqlserver_resume_token(
                                handoff_version,
                                table=self.table,
                                phase="snapshot",
                                offset=offset,
                            ),
                        )
                        if len(rows) < self.batch_size:
                            break
        except Exception as exc:
            logger.warning("SQL Server CT snapshot failed for %s: %s", qualified, exc)
            raise

        self.version = handoff_version
        self.phase = "streaming"
        self.snapshot_offset = 0
        yield ChangeBatch(
            resume_token=encode_sqlserver_resume_token(
                self.version, table=self.table, phase="streaming", offset=0
            ),
        )

    def poll(self) -> Iterator[ChangeBatch]:
        # Resume incomplete snapshot before streaming.
        if self.phase == "snapshot" or (self.version <= 0 and self.phase != "streaming"):
            yield from self.snapshot()
            return

        inserts: list[dict[str, Any]] = []
        updates: list[dict[str, Any]] = []
        deletes: list[str] = []
        next_version = self.version
        qualified = self._qualified()
        pk = self.primary_key

        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT TOP ({self.batch_size})
                            CT.SYS_CHANGE_VERSION,
                            CT.SYS_CHANGE_OPERATION,
                            CT.[{pk}] AS pk_val
                        FROM CHANGETABLE(CHANGES {qualified}, %s) AS CT
                        ORDER BY CT.SYS_CHANGE_VERSION
                        """,
                        (self.version,),
                    )
                    rows = cur.fetchall() or []
                    for ver, op, pk_val in rows:
                        next_version = max(next_version, int(ver or 0))
                        self._last_event_at = datetime.now(timezone.utc)
                        key = "" if pk_val is None else str(pk_val)
                        op_u = (op or "").upper()
                        if op_u == "D":
                            if key:
                                deletes.append(key)
                        elif op_u == "I":
                            inserts.append({pk: key})
                        else:
                            updates.append({pk: key})

                    keys = [r[pk] for r in inserts + updates if r.get(pk)]
                    if keys:
                        placeholders = ",".join(["%s"] * len(keys))
                        cur.execute(
                            f"SELECT * FROM {qualified} WHERE [{pk}] IN ({placeholders})",
                            tuple(keys),
                        )
                        cols = [d[0] for d in (cur.description or [])]
                        by_pk: dict[str, dict[str, Any]] = {}
                        for row in cur.fetchall() or []:
                            rec = self._row_to_record(cols, row)
                            by_pk[str(rec.get(pk, ""))] = rec
                        inserts = [by_pk[k] for k in [r[pk] for r in inserts] if k in by_pk]
                        updates = [by_pk[k] for k in [r[pk] for r in updates] if k in by_pk]
        except Exception as exc:
            logger.warning("SQL Server CT poll failed for %s: %s", qualified, exc)
            return

        self.version = next_version
        self.phase = "streaming"
        token = encode_sqlserver_resume_token(self.version, table=self.table, phase="streaming")
        if inserts or updates or deletes:
            yield ChangeBatch(inserts=inserts, updates=updates, deletes=deletes, resume_token=token)
        else:
            yield ChangeBatch(resume_token=token)

    def ack(self, resume_token: Any = None) -> None:
        """Watermark persistence is the ack for Change Tracking (no server consume)."""
        if resume_token:
            state = decode_sqlserver_resume_token(str(resume_token))
            self.version = int(state.get("version") or self.version)
            self.phase = str(state.get("phase") or self.phase)

    def lag_seconds(self) -> float | None:
        if self._last_event_at is None:
            return None
        return max(0.0, (datetime.now(timezone.utc) - self._last_event_at).total_seconds())

    def replication_lag_seconds(self) -> float | None:
        return self.lag_seconds()
