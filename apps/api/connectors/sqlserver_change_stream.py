"""SQL Server Change Tracking CDC — full initial snapshot + incremental CT.

Airbyte-class path:
  1. ``snapshot()`` dumps the table in PK-ordered batches and records
     ``CHANGE_TRACKING_CURRENT_VERSION()`` as the handoff watermark.
  2. ``poll()`` validates ``last_sync_version`` against
     ``CHANGE_TRACKING_MIN_VALID_VERSION`` (Microsoft: do not call
     ``CHANGETABLE`` when the cursor is below min valid — the result set is
     not valid), then reads ``CHANGETABLE(CHANGES …)`` and hydrates I/U rows
     from the live table; deletes emit PK tombstones.
  3. Watermark advance after destination apply is the ack (at-least-once).

Retention
---------
Default ``CHANGE_RETENTION`` is 2 days. Azure SQL / Managed Instance cleanup
and ``TRUNCATE`` both raise min_valid. A stale poll that swallows the error
or skips the check is silent CDC data loss. Gap recovery is ``when_needed``
blocking snapshot of **current** source keys — not continuous CDC across the
purged window, not ``migration_proven``.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Iterator

from connectors.sql_identifiers import quote_sql_identifier, quote_table_ref
from services.cdc_cursor_gap import CdcCtGapError, CdcCursorGapError
from services.cdc_engine import ChangeBatch

logger = logging.getLogger(__name__)


def assert_resume_version_in_retention(
    resume_version: int | str | None,
    min_valid_version: int | str | None,
    *,
    cursor_key: str = "",
    ct_enabled: bool | None = True,
) -> None:
    """Raise :class:`CdcCtGapError` when last_sync_version is below min valid.

    Microsoft requires this check **before** ``CHANGETABLE``. Equal min_valid
    is allowed (changes after last_sync). Empty resume is a snapshot, not a gap.
    """
    from services.cdc_retention_probe import classify_ct_version_retention

    probe = classify_ct_version_retention(
        resume_version,
        min_valid_version,
        ct_enabled=ct_enabled,
        cursor_key=cursor_key,
    )
    if probe.status != "gap":
        return
    raise CdcCtGapError(
        probe.message,
        resume_version=probe.resume,
        min_valid_version=probe.retained,
        cursor_key=cursor_key,
    )


def encode_sqlserver_resume_token(
    version: int,
    *,
    table: str,
    phase: str = "streaming",
    offset: int = 0,
    last_pk: str = "",
) -> str:
    payload: dict[str, Any] = {
        "kind": "mssql-ct",
        "table": table,
        "version": int(version),
        "phase": phase,
        "offset": int(offset),
    }
    if last_pk and phase == "snapshot":
        payload["last_pk"] = str(last_pk)
    return json.dumps(payload, separators=(",", ":"))


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
                "last_pk": str(data.get("last_pk") or ""),
            }
    except Exception as exc:
        logger.warning("Exception suppressed: %s", exc, exc_info=exc)
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
        cursor_key: str = "",
    ) -> None:
        self.cfg = cfg
        self.table = table
        self.schema = schema or "dbo"
        from services.cdc_identity import require_cdc_primary_key

        self.primary_key = require_cdc_primary_key(primary_key, table=table)
        self.batch_size = max(1, int(batch_size or 500))
        self.columns = columns
        state = decode_sqlserver_resume_token(resume_token)
        self.version = int(state.get("version") or 0)
        self.phase = str(state.get("phase") or "initial")
        self.snapshot_offset = int(state.get("offset") or 0)
        self.snapshot_last_pk = str(state.get("last_pk") or "")
        self._last_event_at: datetime | None = None
        database = str(cfg.get("database") or "master")
        self.cursor_key = cursor_key or f"mssql-ct:{database}:{self.schema}.{self.table}"
        from services.cdc_lease import CdcLeaseGuard, mssql_cdc_resource

        self._lease = CdcLeaseGuard(
            cursor_key=self.cursor_key,
            resource=mssql_cdc_resource(database, self.schema, self.table, mode="ct"),
            holder_id=str(cfg.get("lease_holder_id") or ""),
            job_id=str(cfg.get("job_id") or ""),
            meta={"engine": "sqlserver_ct", "table": self.table},
        )
        self._ct_catalog_cache: dict[str, Any] | None = None
        self._ct_catalog_cache_at: float = 0.0

    @property
    def _resume_expected(self) -> bool:
        return self.phase == "streaming" or self.version > 0

    def _acquire_cdc_lease(self) -> None:
        self._lease.ensure()

    def close(self) -> None:
        self._lease.release()

    def cdc_metadata(self) -> dict[str, Any]:
        return {
            "plugin": "sqlserver_change_tracking",
            "phase": self.phase,
            "delivery": "at-least-once",
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

    def _qualified(self) -> str:
        # sanitize=False: the operator's real object names must survive verbatim;
        # bracket escaping is what makes them safe.
        return quote_table_ref(self.table, self.schema, dialect="sqlserver", sanitize=False)

    def _bracket(self, column: str) -> str:
        """A source column name is data: ``]`` in it must not end the bracket."""
        return quote_sql_identifier(column, "[")

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

    def _min_valid_and_current(self, cur) -> tuple[int | None, int | None, bool]:
        """Return (min_valid_version, current_version, ct_enabled).

        Looks up ``object_id`` from ``sys.change_tracking_tables`` — the same
        catalog ``is_available`` uses — so identifier quoting cannot skip the
        Microsoft min-valid check.
        """
        cur.execute(
            """
            SELECT CHANGE_TRACKING_MIN_VALID_VERSION(ct.object_id),
                   CHANGE_TRACKING_CURRENT_VERSION()
            FROM sys.change_tracking_tables ct
            JOIN sys.tables t ON t.object_id = ct.object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE t.name = %s AND s.name = %s
            """,
            (self.table, self.schema),
        )
        row = cur.fetchone()
        if not row:
            return None, None, False
        min_valid = None if row[0] is None else int(row[0])
        current = None if row[1] is None else int(row[1])
        return min_valid, current, True

    def _ct_catalog_status(self, *, max_age_sec: float = 2.0) -> dict[str, Any]:
        """Live min_valid / current version for Theater and ``attach_cdc_retention``."""
        import time as _time

        now = _time.monotonic()
        if (
            self._ct_catalog_cache is not None
            and (now - float(self._ct_catalog_cache_at or 0.0))
            < max(0.25, float(max_age_sec))
        ):
            return dict(self._ct_catalog_cache)

        out: dict[str, Any] = {
            "plugin": "sqlserver_change_tracking",
            "ct_enabled": None,
            "min_valid_version": None,
            "current_version": None,
            "resume_version": int(self.version) if self.phase == "streaming" else None,
            "table": self.table,
            "schema": self.schema,
        }
        if self.phase == "streaming" or self.version > 0:
            out["resume_version"] = int(self.version)
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    min_valid, current, enabled = self._min_valid_and_current(cur)
                    out["ct_enabled"] = enabled
                    out["min_valid_version"] = min_valid
                    out["current_version"] = current
        except Exception as exc:
            logger.debug("Change Tracking catalog probe failed: %s", exc)
            out["error"] = str(exc)[:300]
        self._ct_catalog_cache = dict(out)
        self._ct_catalog_cache_at = now
        return dict(out)

    def _assert_version_within_retention(self, cur) -> None:
        min_valid, _current, enabled = self._min_valid_and_current(cur)
        if not self._resume_expected:
            return
        assert_resume_version_in_retention(
            self.version,
            min_valid,
            cursor_key=self.cursor_key,
            ct_enabled=enabled,
        )

    def _row_to_record(self, cols: list[str], row: tuple) -> dict[str, str]:
        from services.value_serializer import SQL_NULL_SENTINEL, cell_to_string

        return {
            cols[i]: (
                SQL_NULL_SENTINEL
                if row[i] is None
                else cell_to_string(row[i], preserve_sql_null=True)
            )
            for i in range(len(cols))
        }

    def snapshot(self) -> Iterator[ChangeBatch]:
        """Full table dump + CT version handoff (Airbyte initial sync)."""
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
        quoted = quoted_pk_columns(pk_cols, "[")
        order_sql = ", ".join(quoted)
        offset = self.snapshot_offset if self.phase == "snapshot" else 0
        last_pk = self.snapshot_last_pk if self.phase == "snapshot" else ""
        mode = classify_snapshot_resume(last_pk=last_pk, offset=offset)
        # Mid-dump resume keeps the original CT version (not a new tip).
        handoff_version = self.version if (self.phase == "snapshot" and self.version) else 0
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    if not handoff_version:
                        handoff_version = self._current_version(cur)
                    if mode == "scan":
                        cur.execute(
                            f"SELECT * FROM {qualified} ORDER BY {order_sql}"  # nosec B608
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
                                dialect="sqlserver",
                            )
                            cur.execute(sql, params)
                            rows = cur.fetchall() or []
                        else:
                            cur.execute(
                                f"""
                                SELECT *
                                FROM {qualified}
                                ORDER BY {order_sql}
                                OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
                                """,  # nosec B608
                                (offset, self.batch_size),
                            )
                            rows = cur.fetchall() or []
                        cols = [d[0] for d in (cur.description or [])]
                        if not rows:
                            break
                        records = [self._row_to_record(cols, row) for row in rows]
                        offset += len(rows)
                        last_pk = last_pk_from_records(records, pk_cols) or last_pk
                        self._last_event_at = datetime.now(timezone.utc)
                        yield ChangeBatch(
                            inserts=records,
                            resume_token=encode_sqlserver_resume_token(
                                handoff_version,
                                table=self.table,
                                phase="snapshot",
                                offset=offset,
                                last_pk=last_pk,
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
        self.snapshot_last_pk = ""
        yield ChangeBatch(
            resume_token=encode_sqlserver_resume_token(
                self.version, table=self.table, phase="streaming", offset=0
            ),
        )

    def poll(self) -> Iterator[ChangeBatch]:
        self._acquire_cdc_lease()
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
                    self._assert_version_within_retention(cur)
                    cur.execute(
                        f"""
                        SELECT TOP ({self.batch_size})
                            CT.SYS_CHANGE_VERSION,
                            CT.SYS_CHANGE_OPERATION,
                            CT.{self._bracket(pk)} AS pk_val
                        FROM CHANGETABLE(CHANGES {qualified}, %s) AS CT
                        ORDER BY CT.SYS_CHANGE_VERSION
                        """,  # nosec B608
                        (self.version,),
                    )
                    rows = cur.fetchall() or []
                    # TOP(batch_size) ordered only by version can end mid-version;
                    # resuming at that version excludes remaining same-version rows.
                    if len(rows) >= self.batch_size:
                        raise RuntimeError(
                            "SQL Server CT page reached batch_size; refusing to advance "
                            "the change-tracking watermark because same-version rows "
                            "could be skipped. Increase batch_size or enable compound "
                            "CT+PK continuation."
                        )
                    for ver, op, pk_val in rows:
                        next_version = max(next_version, int(ver or 0))
                        self._last_event_at = datetime.now(timezone.utc)
                        key = "" if pk_val is None else str(pk_val)
                        op_u = (op or "").upper()
                        if op_u == "D":
                            from services.cdc_identity import is_present_cdc_row_key

                            if is_present_cdc_row_key(key):
                                deletes.append(key)
                        elif op_u == "I":
                            inserts.append({pk: key})
                        else:
                            updates.append({pk: key})

                    keys = [r[pk] for r in inserts + updates if r.get(pk)]
                    if keys:
                        placeholders = ",".join(["%s"] * len(keys))
                        cur.execute(
                            f"SELECT * FROM {qualified} "  # nosec B608
                            f"WHERE {self._bracket(pk)} IN ({placeholders})",
                            tuple(keys),
                        )
                        cols = [d[0] for d in (cur.description or [])]
                        by_pk: dict[str, dict[str, Any]] = {}
                        for row in cur.fetchall() or []:
                            rec = self._row_to_record(cols, row)
                            by_pk[str(rec.get(pk, ""))] = rec
                        inserts = [by_pk[k] for k in [r[pk] for r in inserts] if k in by_pk]
                        updates = [by_pk[k] for k in [r[pk] for r in updates] if k in by_pk]
        except CdcCursorGapError:
            raise
        except RuntimeError:
            raise
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
