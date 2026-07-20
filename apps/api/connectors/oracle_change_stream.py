"""Oracle flashback CDC — full initial snapshot + SCN-versioned incremental.

Uses ``FLASHBACK VERSION QUERY`` for incremental changes after a consistent
table dump. LogMiner/XStream remain future depth; this path is production-usable
when the source grants FLASHBACK and undo retention covers the lag window.

Apply semantics: **at-least-once** upsert (watermark advances only after apply).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Iterator

from services.cdc_engine import ChangeBatch

logger = logging.getLogger(__name__)


def encode_oracle_resume_token(
    scn: int,
    *,
    table: str,
    phase: str = "streaming",
    offset: int = 0,
) -> str:
    return json.dumps(
        {
            "kind": "oracle-scn",
            "table": table,
            "scn": int(scn),
            "phase": phase,
            "offset": int(offset),
        },
        separators=(",", ":"),
    )


def decode_oracle_resume_token(token: str | None) -> dict[str, Any]:
    if not token:
        return {"scn": 0, "phase": "initial", "offset": 0, "table": ""}
    raw = str(token).strip()
    if raw.startswith("oracle-scn:"):
        try:
            scn = int(raw.rsplit(":", 1)[-1])
        except Exception:
            scn = 0
        return {"scn": scn, "phase": "streaming", "offset": 0, "table": ""}
    try:
        data = json.loads(raw)
        if isinstance(data, dict) and data.get("kind") == "oracle-scn":
            return {
                "scn": int(data.get("scn") or 0),
                "phase": str(data.get("phase") or "streaming"),
                "offset": int(data.get("offset") or 0),
                "table": str(data.get("table") or ""),
            }
    except Exception:
        pass
    try:
        return {"scn": int(raw), "phase": "streaming", "offset": 0, "table": ""}
    except Exception:
        return {"scn": 0, "phase": "initial", "offset": 0, "table": ""}


class OracleFlashbackCdc:
    """SCN-watermarked change capture with real initial table dump."""

    def __init__(
        self,
        cfg: dict[str, Any],
        *,
        table: str,
        primary_key: str,
        schema: str = "",
        batch_size: int = 500,
        resume_token: str | None = None,
        columns: list[str] | None = None,
        cursor_key: str = "",
    ) -> None:
        self.cfg = cfg
        self.table = table
        self.schema = (schema or cfg.get("schema") or cfg.get("username") or "").upper()
        self.primary_key = (primary_key or "ID").upper()
        self.batch_size = max(1, int(batch_size or 500))
        self.columns = columns
        state = decode_oracle_resume_token(resume_token)
        self.scn = int(state.get("scn") or 0)
        self.phase = str(state.get("phase") or "initial")
        self.snapshot_offset = int(state.get("offset") or 0)
        self._last_event_at: datetime | None = None
        self.cursor_key = (
            cursor_key or f"oracle-flashback:{self.schema}.{self.table.upper()}"
        )
        from services.cdc_lease import CdcLeaseGuard, oracle_cdc_resource

        self._lease = CdcLeaseGuard(
            cursor_key=self.cursor_key,
            resource=oracle_cdc_resource(
                self.schema,
                self.table.upper(),
                mode="flashback",
                host=str(cfg.get("host") or ""),
            ),
            holder_id=str(cfg.get("lease_holder_id") or ""),
            job_id=str(cfg.get("job_id") or ""),
            meta={"engine": "oracle_flashback", "table": self.table},
        )

    def _acquire_cdc_lease(self) -> None:
        self._lease.ensure()

    def close(self) -> None:
        self._lease.release()

    def cdc_metadata(self) -> dict[str, Any]:
        return {
            "plugin": "oracle_flashback",
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
            return f'"{self.schema}"."{self.table.upper()}"'
        return f'"{self.table.upper()}"'

    def is_available(self) -> bool:
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT current_scn FROM v$database")
                    row = cur.fetchone()
                    if not row:
                        return False
                    cur.execute(
                        f"SELECT COUNT(*) FROM {self._qualified()} VERSIONS BETWEEN SCN MINVALUE AND MAXVALUE "
                        f"WHERE ROWNUM <= 1"
                    )
                    cur.fetchone()
                    return True
        except Exception as exc:
            logger.debug("Oracle flashback CDC unavailable for %s: %s", self.table, exc)
            return False

    def _row_to_record(self, cols: list[str], row: tuple) -> dict[str, str]:
        return {str(cols[i]).upper(): "" if row[i] is None else str(row[i]) for i in range(len(cols))}

    def snapshot(self) -> Iterator[ChangeBatch]:
        """Full table dump at current SCN, then hand off to flashback versions."""
        self._acquire_cdc_lease()
        qualified = self._qualified()
        pk = self.primary_key
        offset = self.snapshot_offset if self.phase == "snapshot" else 0
        handoff_scn = 0
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT current_scn FROM v$database")
                    head = cur.fetchone()
                    handoff_scn = int(head[0] or 0) if head else 0
                    while True:
                        cur.execute(
                            f"""
                            SELECT * FROM (
                              SELECT t.*, ROW_NUMBER() OVER (ORDER BY t."{pk}") AS df_rn
                              FROM {qualified} t
                            )
                            WHERE df_rn > :off AND df_rn <= :lim
                            """,
                            {"off": offset, "lim": offset + self.batch_size},
                        )
                        cols = [d[0] for d in (cur.description or [])]
                        rows = cur.fetchall() or []
                        if not rows:
                            break
                        # Drop synthetic rn column
                        clean_cols = [c for c in cols if str(c).upper() != "DF_RN"]
                        rn_idx = next(
                            (i for i, c in enumerate(cols) if str(c).upper() == "DF_RN"),
                            None,
                        )
                        records = []
                        for row in rows:
                            if rn_idx is not None:
                                values = [row[i] for i in range(len(cols)) if i != rn_idx]
                            else:
                                values = list(row)
                            records.append(self._row_to_record(clean_cols, tuple(values)))
                        offset += len(rows)
                        self._last_event_at = datetime.now(timezone.utc)
                        yield ChangeBatch(
                            inserts=records,
                            resume_token=encode_oracle_resume_token(
                                handoff_scn,
                                table=self.table,
                                phase="snapshot",
                                offset=offset,
                            ),
                        )
                        if len(rows) < self.batch_size:
                            break
        except Exception as exc:
            logger.warning("Oracle flashback snapshot failed for %s: %s", qualified, exc)
            raise

        self.scn = handoff_scn
        self.phase = "streaming"
        self.snapshot_offset = 0
        yield ChangeBatch(
            resume_token=encode_oracle_resume_token(
                self.scn, table=self.table, phase="streaming", offset=0
            ),
        )

    def poll(self) -> Iterator[ChangeBatch]:
        self._acquire_cdc_lease()
        if self.phase == "snapshot" or (self.scn <= 0 and self.phase != "streaming"):
            yield from self.snapshot()
            return

        inserts: list[dict[str, Any]] = []
        updates: list[dict[str, Any]] = []
        deletes: list[str] = []
        next_scn = self.scn
        pk = self.primary_key
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT current_scn FROM v$database")
                    head = cur.fetchone()
                    head_scn = int(head[0] or self.scn) if head else self.scn
                    cur.execute(
                        f"""
                        SELECT * FROM (
                          SELECT t.*, VERSIONS_OPERATION AS df_op, VERSIONS_STARTSCN AS df_scn
                          FROM {self._qualified()} t
                          VERSIONS BETWEEN SCN :start_scn AND :end_scn
                          ORDER BY VERSIONS_STARTSCN
                        ) WHERE ROWNUM <= :lim
                        """,
                        {"start_scn": self.scn + 1, "end_scn": head_scn, "lim": self.batch_size},
                    )
                    cols = [d[0] for d in (cur.description or [])]
                    for row in cur.fetchall() or []:
                        rec = {cols[i]: row[i] for i in range(len(cols))}
                        op = rec.pop("DF_OP", None) or rec.pop("df_op", None) or "U"
                        scn_val = rec.pop("DF_SCN", None) or rec.pop("df_scn", None) or head_scn
                        next_scn = max(next_scn, int(scn_val or 0))
                        self._last_event_at = datetime.now(timezone.utc)
                        clean = {str(k).upper(): "" if v is None else str(v) for k, v in rec.items()}
                        key = clean.get(pk, "")
                        if str(op).upper() == "D":
                            if key:
                                deletes.append(key)
                        elif str(op).upper() == "I":
                            inserts.append(clean)
                        else:
                            updates.append(clean)
                    self.scn = max(next_scn, head_scn)
        except Exception as exc:
            logger.warning("Oracle flashback poll failed for %s: %s", self.table, exc)
            return

        self.phase = "streaming"
        token = encode_oracle_resume_token(self.scn, table=self.table, phase="streaming")
        if inserts or updates or deletes:
            yield ChangeBatch(inserts=inserts, updates=updates, deletes=deletes, resume_token=token)
        else:
            yield ChangeBatch(resume_token=token)

    def ack(self, resume_token: Any = None) -> None:
        if resume_token:
            state = decode_oracle_resume_token(str(resume_token))
            self.scn = int(state.get("scn") or self.scn)
            self.phase = str(state.get("phase") or self.phase)

    def lag_seconds(self) -> float | None:
        if self._last_event_at is None:
            return None
        return max(0.0, (datetime.now(timezone.utc) - self._last_event_at).total_seconds())

    def replication_lag_seconds(self) -> float | None:
        return self.lag_seconds()
