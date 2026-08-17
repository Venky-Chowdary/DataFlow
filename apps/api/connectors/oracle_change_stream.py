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

from connectors.sql_identifiers import quote_sql_identifier
from services.cdc_engine import ChangeBatch
from services.value_serializer import SQL_NULL_SENTINEL, cell_to_string

logger = logging.getLogger(__name__)


def encode_oracle_resume_token(
    scn: int,
    *,
    table: str,
    phase: str = "streaming",
    offset: int = 0,
    last_pk: str = "",
) -> str:
    payload: dict[str, Any] = {
        "kind": "oracle-scn",
        "table": table,
        "scn": int(scn),
        "phase": phase,
        "offset": int(offset),
    }
    if last_pk and phase == "snapshot":
        payload["last_pk"] = str(last_pk)
    return json.dumps(payload, separators=(",", ":"))


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
                "last_pk": str(data.get("last_pk") or ""),
            }
    except Exception as exc:
        logger.warning("Exception suppressed: %s", exc, exc_info=exc)
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
        from services.cdc_identity import require_cdc_primary_key

        self.primary_key = require_cdc_primary_key(primary_key, table=table).upper()
        self.batch_size = max(1, int(batch_size or 500))
        self.columns = columns
        state = decode_oracle_resume_token(resume_token)
        self.scn = int(state.get("scn") or 0)
        self.phase = str(state.get("phase") or "initial")
        self.snapshot_offset = int(state.get("offset") or 0)
        self.snapshot_last_pk = str(state.get("last_pk") or "")
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
        tbl = quote_sql_identifier(self.table.upper())
        if self.schema:
            return f"{quote_sql_identifier(self.schema)}.{tbl}"
        return tbl

    def is_available(self) -> bool:
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT current_scn FROM v$database")
                    row = cur.fetchone()
                    if not row:
                        return False
                    # _qualified() double-quotes identifiers; the f-string only contains that quoted table reference.
                    cur.execute(  # nosec B608
                        f"SELECT COUNT(*) FROM {self._qualified()} VERSIONS BETWEEN SCN MINVALUE AND MAXVALUE "  # nosec B608
                        f"WHERE ROWNUM <= 1"
                    )
                    cur.fetchone()
                    return True
        except Exception as exc:
            logger.debug("Oracle flashback CDC unavailable for %s: %s", self.table, exc)
            return False

    def _row_to_record(self, cols: list[str], row: tuple) -> dict[str, str]:
        return {
            str(cols[i]).upper(): (
                SQL_NULL_SENTINEL
                if row[i] is None
                else cell_to_string(row[i], preserve_sql_null=True)
            )
            for i in range(len(cols))
        }

    def snapshot(self) -> Iterator[ChangeBatch]:
        """Full table dump at current SCN, then hand off to flashback versions."""
        self._acquire_cdc_lease()
        from connectors.sql_snapshot_scan import fetch_scan_page
        from services.cdc_snapshot_resume import (
            classify_snapshot_resume,
            last_pk_from_records,
            quoted_pk_columns,
            snapshot_keyset_sql,
        )
        from services.cdc_snapshot_window import _pk_columns

        qualified = self._qualified()
        pk_cols = _pk_columns(self.primary_key)
        quoted = quoted_pk_columns(pk_cols, '"')
        order_sql = ", ".join(quoted)
        offset = self.snapshot_offset if self.phase == "snapshot" else 0
        last_pk = self.snapshot_last_pk if self.phase == "snapshot" else ""
        mode = classify_snapshot_resume(last_pk=last_pk, offset=offset)
        # Mid-dump resume keeps the original SCN (not a new tip).
        handoff_scn = self.scn if (self.phase == "snapshot" and self.scn) else 0
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    if not handoff_scn:
                        cur.execute("SELECT current_scn FROM v$database")
                        head = cur.fetchone()
                        handoff_scn = int(head[0] or 0) if head else 0
                    if mode == "scan":
                        # Held scan: one ordered SELECT paged with fetchmany.
                        # ROW_NUMBER() here would rank every row of the table
                        # before the first page returns, and the rank itself is
                        # unused — resume seeks on the last PK.
                        cur.execute(
                            f"SELECT t.* FROM {qualified} t ORDER BY {order_sql}"  # nosec B608
                        )
                    while True:
                        if mode == "scan":
                            rows = fetch_scan_page(cur, self.batch_size)
                        elif mode == "keyset":
                            sql, params = snapshot_keyset_sql(
                                table_ref=qualified,
                                quoted_pk_columns=quoted,
                                last_pk=last_pk,
                                limit=self.batch_size,
                                dialect="oracle",
                            )
                            cur.execute(sql, params)
                            rows = cur.fetchall() or []
                        else:
                            pk = quote_sql_identifier(self.primary_key)
                            cur.execute(
                                f"""
                                SELECT * FROM (
                                  SELECT t.*, ROW_NUMBER() OVER (ORDER BY t.{pk}) AS df_rn
                                  FROM {qualified} t
                                )
                                WHERE df_rn > :off AND df_rn <= :lim
                                """,  # nosec B608
                                {"off": offset, "lim": offset + self.batch_size},
                            )
                            rows = cur.fetchall() or []
                        cols = [d[0] for d in (cur.description or [])]
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
                        last_pk = last_pk_from_records(records, pk_cols) or last_pk
                        self._last_event_at = datetime.now(timezone.utc)
                        yield ChangeBatch(
                            inserts=records,
                            resume_token=encode_oracle_resume_token(
                                handoff_scn,
                                table=self.table,
                                phase="snapshot",
                                offset=offset,
                                last_pk=last_pk,
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
        self.snapshot_last_pk = ""
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
                        """,  # nosec B608
                        {"start_scn": self.scn + 1, "end_scn": head_scn, "lim": self.batch_size},
                    )
                    cols = [d[0] for d in (cur.description or [])]
                    rows = cur.fetchall() or []
                    # Full page without compound SCN+PK continuation would advance
                    # the watermark to head_scn and silently skip remaining changes.
                    if len(rows) >= self.batch_size:
                        raise RuntimeError(
                            "Oracle Flashback CDC page reached batch_size; refusing to "
                            "advance the SCN watermark because remaining changes could "
                            "be skipped. Use a smaller change window or enable compound "
                            "SCN+PK continuation."
                        )
                    for row in rows:
                        rec = {cols[i]: row[i] for i in range(len(cols))}
                        op = rec.pop("DF_OP", None) or rec.pop("df_op", None) or "U"
                        scn_val = rec.pop("DF_SCN", None) or rec.pop("df_scn", None) or head_scn
                        next_scn = max(next_scn, int(scn_val or 0))
                        self._last_event_at = datetime.now(timezone.utc)
                        clean = {
                            str(k).upper(): (
                                SQL_NULL_SENTINEL
                                if v is None
                                else cell_to_string(v, preserve_sql_null=True)
                            )
                            for k, v in rec.items()
                        }
                        key = clean.get(pk, "")
                        if str(op).upper() == "D":
                            from services.cdc_identity import is_present_cdc_row_key

                            if is_present_cdc_row_key(key):
                                deletes.append(key)
                        elif str(op).upper() == "I":
                            inserts.append(clean)
                        else:
                            updates.append(clean)
                    # Incomplete page means the SCN window was fully drained.
                    self.scn = max(next_scn, head_scn)
        except RuntimeError:
            raise
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
