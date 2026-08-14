"""CDC retention health — proactive watermark vs retained LSN/SCN.

Honesty
-------
``ok`` / ``at_risk`` / ``gap`` classify resume against live retention. A ``gap``
means continuous CDC across the window is impossible. PostgreSQL
``wal_status=lost`` or a dropped slot is the same class as a purged binlog —
recreating the slot at current WAL skips the lost window. ``when_needed``
recovers by blocking-snapshot of current source keys (see ``cdc_snapshot_mode``);
``initial`` / ``never`` stay fail-closed.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class RetentionProbeResult:
    status: str  # ok | at_risk | gap | unknown | n_a | no_watermark
    dialect: str
    resume: str = ""
    retained: str = ""
    cursor_key: str = ""
    capture_instance: str = ""
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def job_fields(self) -> dict[str, Any]:
        return {
            "cdc_retention_status": self.status,
            "cdc_retention_resume": self.resume or None,
            "cdc_retention_retained": self.retained or None,
            "cdc_retention_message": self.message or None,
            "cdc_retention_dialect": self.dialect or None,
        }


def classify_lsn_retention(
    resume_lsn: str,
    min_lsn: str,
    *,
    cursor_key: str = "",
    dialect: str = "sqlserver",
) -> RetentionProbeResult:
    """Compare resume LSN to capture ``min_lsn``.

    - ``gap``: resume < retained (fail-closed class)
    - ``at_risk``: resume == retained (next cleanup drops the cursor)
    - ``ok``: resume > retained
    """
    from connectors.sqlserver_cdc_native import _lsn_to_hex, compare_mssql_hex_lsn

    resume = _lsn_to_hex(resume_lsn)
    retained = _lsn_to_hex(min_lsn)
    if not resume:
        return RetentionProbeResult(
            status="no_watermark",
            dialect=dialect,
            retained=retained,
            cursor_key=cursor_key,
            message="No resume LSN — next run will snapshot.",
        )
    if not retained:
        return RetentionProbeResult(
            status="unknown",
            dialect=dialect,
            resume=resume,
            cursor_key=cursor_key,
            message="Could not read capture min_lsn.",
        )
    cmp = compare_mssql_hex_lsn(resume, retained)
    if cmp < 0:
        return RetentionProbeResult(
            status="gap",
            dialect=dialect,
            resume=resume,
            retained=retained,
            cursor_key=cursor_key,
            message=(
                f"Resume LSN {resume} is before retention min_lsn {retained}. "
                "Reset watermark and re-snapshot — continuous CDC across the gap is not claimed."
            ),
        )
    if cmp == 0:
        return RetentionProbeResult(
            status="at_risk",
            dialect=dialect,
            resume=resume,
            retained=retained,
            cursor_key=cursor_key,
            message=(
                f"Resume sits on retention edge (min_lsn={retained}). "
                "Next CDC cleanup may force a gap — consider when_needed snapshot readiness."
            ),
        )
    return RetentionProbeResult(
        status="ok",
        dialect=dialect,
        resume=resume,
        retained=retained,
        cursor_key=cursor_key,
        message=f"Resume LSN is within retention (resume={resume}, min_lsn={retained}).",
    )


def classify_scn_retention(
    resume_scn: int | str,
    oldest_scn: int | str | None,
    *,
    cursor_key: str = "",
    at_risk_headroom: int = 10_000,
) -> RetentionProbeResult:
    """Compare resume SCN to oldest available redo."""
    try:
        resume = int(resume_scn or 0)
    except (TypeError, ValueError):
        resume = 0
    try:
        oldest = int(oldest_scn or 0)
    except (TypeError, ValueError):
        oldest = 0
    if resume <= 0:
        return RetentionProbeResult(
            status="no_watermark",
            dialect="oracle",
            retained=str(oldest) if oldest else "",
            cursor_key=cursor_key,
            message="No resume SCN — next run will snapshot.",
        )
    if oldest <= 0:
        return RetentionProbeResult(
            status="unknown",
            dialect="oracle",
            resume=str(resume),
            cursor_key=cursor_key,
            message="Oldest available SCN undetermined (privilege or view unavailable).",
        )
    if resume < oldest:
        return RetentionProbeResult(
            status="gap",
            dialect="oracle",
            resume=str(resume),
            retained=str(oldest),
            cursor_key=cursor_key,
            message=(
                f"Resume SCN {resume} is before oldest redo {oldest}. "
                "Reset watermark and re-snapshot — continuous CDC across the gap is not claimed."
            ),
        )
    headroom = resume - oldest
    if headroom <= max(0, int(at_risk_headroom)):
        return RetentionProbeResult(
            status="at_risk",
            dialect="oracle",
            resume=str(resume),
            retained=str(oldest),
            cursor_key=cursor_key,
            message=(
                f"Resume SCN is within {headroom} of oldest redo ({oldest}). "
                "Archive purge may create a gap soon."
            ),
            details={"headroom": headroom, "at_risk_headroom": at_risk_headroom},
        )
    return RetentionProbeResult(
        status="ok",
        dialect="oracle",
        resume=str(resume),
        retained=str(oldest),
        cursor_key=cursor_key,
        message=f"Resume SCN within redo window (headroom={headroom}).",
        details={"headroom": headroom},
    )


def classify_binlog_retention(
    resume_file: str,
    resume_pos: int | str | None,
    binary_logs: list[str],
    *,
    resume_gtid: str = "",
    gtid_purged: str = "",
    gtid_in_purged: bool | None = None,
    cursor_key: str = "",
    dialect: str = "mysql",
) -> RetentionProbeResult:
    """Compare resume file:pos / GTID to retained binary logs.

    - ``gap``: resume file missing from ``SHOW BINARY LOGS``, or GTID ⊆ gtid_purged
    - ``at_risk``: resume sits on the oldest retained file (next purge may gap)
    - ``ok``: resume file is still retained and not the oldest edge
    """
    logs = [str(f).strip() for f in (binary_logs or []) if str(f).strip()]
    resume_f = str(resume_file or "").strip()
    resume_g = str(resume_gtid or "").strip()
    try:
        pos_s = "" if resume_pos in (None, "") else str(int(resume_pos))
    except (TypeError, ValueError):
        pos_s = str(resume_pos or "")
    resume_label = f"{resume_f}:{pos_s}" if resume_f and pos_s else (resume_f or resume_g)
    oldest = logs[0] if logs else ""
    retained_label = oldest or (str(gtid_purged)[:120] if gtid_purged else "")

    if gtid_in_purged is True:
        return RetentionProbeResult(
            status="gap",
            dialect=dialect,
            resume=resume_label or resume_g,
            retained=retained_label,
            cursor_key=cursor_key,
            message=(
                f"Resume GTID is contained in gtid_purged. "
                "Reset watermark and re-snapshot — continuous CDC across the gap is not claimed."
            ),
            details={"gtid_purged": str(gtid_purged)[:200], "resume_gtid": resume_g[:200]},
        )

    if not resume_f and not resume_g:
        return RetentionProbeResult(
            status="no_watermark",
            dialect=dialect,
            retained=retained_label,
            cursor_key=cursor_key,
            message="No resume binlog file/GTID — next run will snapshot or start at current.",
        )

    if resume_f:
        if not logs:
            return RetentionProbeResult(
                status="unknown",
                dialect=dialect,
                resume=resume_label,
                cursor_key=cursor_key,
                message="Could not list binary logs (privilege or log_bin off).",
            )
        if resume_f not in logs:
            return RetentionProbeResult(
                status="gap",
                dialect=dialect,
                resume=resume_label,
                retained=retained_label,
                cursor_key=cursor_key,
                message=(
                    f"Resume binlog {resume_label} is not in SHOW BINARY LOGS "
                    f"(oldest retained={oldest or '?'}). "
                    "Reset watermark and re-snapshot — continuous CDC across the gap is not claimed."
                ),
                details={"binary_logs": logs[:20], "oldest_file": oldest},
            )
        if oldest and resume_f == oldest:
            return RetentionProbeResult(
                status="at_risk",
                dialect=dialect,
                resume=resume_label,
                retained=retained_label,
                cursor_key=cursor_key,
                message=(
                    f"Resume sits on oldest retained binlog ({oldest}). "
                    "Next expire_logs purge may force a gap — consider when_needed snapshot readiness."
                ),
                details={"oldest_file": oldest},
            )
        return RetentionProbeResult(
            status="ok",
            dialect=dialect,
            resume=resume_label,
            retained=retained_label,
            cursor_key=cursor_key,
            message=f"Resume binlog is within retention (resume={resume_label}, oldest={oldest}).",
            details={"oldest_file": oldest},
        )

    # GTID-only resume without server-side purged subset proof.
    if resume_g and gtid_purged and gtid_in_purged is None:
        return RetentionProbeResult(
            status="unknown",
            dialect=dialect,
            resume=resume_g[:200],
            retained=str(gtid_purged)[:120],
            cursor_key=cursor_key,
            message="GTID resume present but GTID_SUBSET(gtid_purged) was not evaluated.",
        )
    return RetentionProbeResult(
        status="ok",
        dialect=dialect,
        resume=resume_g[:200] if resume_g else resume_label,
        retained=retained_label,
        cursor_key=cursor_key,
        message="GTID resume retained (not reported in gtid_purged).",
    )


def classify_pg_slot_retention(
    *,
    slot_exists: bool | None = None,
    wal_status: str = "",
    restart_lsn: str = "",
    confirmed_flush_lsn: str = "",
    watermark: Any = None,
    resume_expected: bool | None = None,
    cursor_key: str = "",
    slot_name: str = "",
) -> RetentionProbeResult:
    """Classify a PostgreSQL logical slot against ``wal_status`` / existence.

    PG13+ ``pg_replication_slots.wal_status``:

    - ``lost`` — required WAL was recycled (``max_slot_wal_keep_size``). Gap.
    - ``unreserved`` / ``extended`` — next checkpoint may invalidate. At-risk.
    - ``reserved`` — restart_lsn is retained. Ok.
    - empty (PG12) — slot existence retains WAL; treat existing slot as ok.

    A **dropped** slot while a watermark/resume is present is the same gap
    class: ``pg_create_logical_replication_slot`` would start at *current* WAL
    and skip the lost window. That is silent CDC data loss. Snapshot recovery
    (``when_needed``) may recreate the slot; poll/resume must not.
    """
    from services.cdc_snapshot_mode import watermark_present

    wal = str(wal_status or "").strip().lower()
    exists = bool(slot_exists)
    expected = (
        bool(resume_expected)
        if resume_expected is not None
        else watermark_present(watermark)
    )
    resume = str(confirmed_flush_lsn or watermark or slot_name or "").strip()
    retained = wal or restart_lsn or ("slot_missing" if not exists else "")
    lost_note = (
        "Recreating the slot at current WAL would skip the lost window. "
        "when_needed snapshots current source keys then streams from the new tip. "
        "Not continuous CDC, not migration_proven."
    )

    if not exists:
        if expected:
            return RetentionProbeResult(
                status="gap",
                dialect="postgresql",
                resume=resume,
                retained="slot_missing",
                cursor_key=cursor_key,
                capture_instance=slot_name,
                message=(
                    f"PostgreSQL replication slot {slot_name or '(unnamed)'} is missing "
                    f"while a CDC watermark is present. {lost_note}"
                ),
                details={"slot_exists": False, "wal_status": wal, "slot_name": slot_name},
            )
        return RetentionProbeResult(
            status="no_watermark",
            dialect="postgresql",
            retained=retained,
            cursor_key=cursor_key,
            capture_instance=slot_name,
            message="No PostgreSQL slot or watermark — next run will snapshot and create the slot.",
            details={"slot_exists": False, "slot_name": slot_name},
        )

    if wal == "lost":
        return RetentionProbeResult(
            status="gap",
            dialect="postgresql",
            resume=resume,
            retained="lost",
            cursor_key=cursor_key,
            capture_instance=slot_name,
            message=(
                f"PostgreSQL slot {slot_name or '(unnamed)'} wal_status=lost "
                f"(restart_lsn={restart_lsn or '?'}, confirmed_flush={confirmed_flush_lsn or '?'}). "
                f"{lost_note}"
            ),
            details={
                "slot_exists": True,
                "wal_status": "lost",
                "restart_lsn": restart_lsn,
                "confirmed_flush_lsn": confirmed_flush_lsn,
                "slot_name": slot_name,
            },
        )
    if wal in {"unreserved", "extended"}:
        return RetentionProbeResult(
            status="at_risk",
            dialect="postgresql",
            resume=resume,
            retained=wal,
            cursor_key=cursor_key,
            capture_instance=slot_name,
            message=(
                f"PostgreSQL slot {slot_name or '(unnamed)'} wal_status={wal}. "
                "max_slot_wal_keep_size may invalidate the slot at the next checkpoint. "
                "when_needed snapshot readiness is the recovery path — not continuous CDC."
            ),
            details={
                "slot_exists": True,
                "wal_status": wal,
                "restart_lsn": restart_lsn,
                "slot_name": slot_name,
            },
        )
    return RetentionProbeResult(
        status="ok",
        dialect="postgresql",
        resume=resume,
        retained=wal or restart_lsn,
        cursor_key=cursor_key,
        capture_instance=slot_name,
        message=(
            f"PostgreSQL slot {slot_name or '(unnamed)'} retains WAL "
            f"(wal_status={wal or 'n/a-pg12'}, restart_lsn={restart_lsn or '?'})."
        ),
        details={
            "slot_exists": True,
            "wal_status": wal,
            "restart_lsn": restart_lsn,
            "confirmed_flush_lsn": confirmed_flush_lsn,
            "slot_name": slot_name,
        },
    )


def _resume_binlog_from_watermark(watermark: str | None) -> dict[str, Any]:
    """Extract file/pos/gtid from a MySQL CDC watermark JSON or bare file:pos."""
    out: dict[str, Any] = {"file": "", "pos": None, "gtid": ""}
    if not watermark:
        return out
    text = str(watermark).strip()
    try:
        import json

        if text.startswith("{"):
            data = json.loads(text)
            out["file"] = str(data.get("file") or "").strip()
            if data.get("pos") is not None:
                try:
                    out["pos"] = int(data["pos"])
                except (TypeError, ValueError):
                    out["pos"] = data.get("pos")
            out["gtid"] = str(data.get("gtid") or "").strip()
            return out
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    if ":" in text and not text.startswith("{"):
        # bare mysql-bin.000003:1234
        file_part, _, pos_part = text.rpartition(":")
        if file_part and pos_part.isdigit():
            out["file"] = file_part
            out["pos"] = int(pos_part)
    return out


def _resume_lsn_from_watermark(watermark: str | None) -> str:
    if not watermark:
        return ""
    try:
        from connectors.sqlserver_cdc_native import decode_mssql_cdc_token

        token = decode_mssql_cdc_token(watermark)
        lsn = str(token.get("lsn") or "").strip()
        if lsn:
            return lsn
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    # Plain hex / 0x… watermark
    text = str(watermark).strip()
    if text.startswith("{"):
        return ""
    return text


def _resume_scn_from_watermark(watermark: str | None) -> int:
    if not watermark:
        return 0
    text = str(watermark).strip()
    try:
        import json

        if text.startswith("{"):
            data = json.loads(text)
            for key in ("scn", "resume_scn", "watermark"):
                if data.get(key) is not None:
                    return int(data[key])
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    try:
        return int(text)
    except (TypeError, ValueError):
        return 0


def probe_sqlserver_retention(
    cfg: dict[str, Any],
    *,
    table: str,
    schema: str = "dbo",
    cursor_key: str = "",
    watermark: str | None = None,
) -> RetentionProbeResult:
    """Live probe: watermark vs ``sys.fn_cdc_get_min_lsn`` for the capture instance."""
    from connectors.sqlserver_cdc_native import SqlServerNativeCdc

    from services.sync_cursor import get_watermark

    ck = (cursor_key or "").strip()
    wm = watermark if watermark is not None else (get_watermark(ck) if ck else None)
    resume = _resume_lsn_from_watermark(wm)
    if not table:
        return RetentionProbeResult(
            status="n_a",
            dialect="sqlserver",
            cursor_key=ck,
            message="table is required for SQL Server retention probe.",
        )
    try:
        from services.cdc_identity import require_cdc_primary_key

        cdc = SqlServerNativeCdc(
            {**cfg, "type": cfg.get("type") or "sqlserver"},
            table=table,
            primary_key=require_cdc_primary_key(cfg.get("primary_key"), table=table),
            schema=schema or "dbo",
            cursor_key=ck or None,
        )
        if not cdc.is_available():
            return RetentionProbeResult(
                status="unknown",
                dialect="sqlserver",
                resume=resume,
                cursor_key=ck,
                message="SQL Server native CDC capture not available for this table.",
            )
        with cdc._conn() as conn:
            with conn.cursor() as cur:
                min_lsn = cdc._min_lsn(cur)
        result = classify_lsn_retention(resume, min_lsn, cursor_key=ck)
        result.capture_instance = str(getattr(cdc, "capture_instance", "") or "")
        result.details["table"] = table
        result.details["schema"] = schema
        return result
    except Exception as exc:
        return RetentionProbeResult(
            status="unknown",
            dialect="sqlserver",
            resume=resume,
            cursor_key=ck,
            message=f"Retention probe failed: {exc}",
            details={"error": str(exc)[:300]},
        )


def probe_oracle_retention(
    cfg: dict[str, Any],
    *,
    cursor_key: str = "",
    watermark: str | None = None,
    at_risk_headroom: int = 10_000,
) -> RetentionProbeResult:
    """Live probe: watermark SCN vs oldest available redo."""
    import sqlalchemy as sa
    from connectors.generic_sql import _engine

    from services.sync_cursor import get_watermark

    ck = (cursor_key or "").strip()
    wm = watermark if watermark is not None else (get_watermark(ck) if ck else None)
    resume = _resume_scn_from_watermark(wm)
    try:
        engine = _engine({**cfg, "type": "oracle"})
        candidates: list[int] = []
        with engine.connect() as conn:
            for sql in (
                "SELECT MIN(FIRST_CHANGE#) FROM V$LOG",
                "SELECT MIN(FIRST_CHANGE#) FROM V$ARCHIVED_LOG WHERE DELETED = 'NO'",
            ):
                try:
                    row = conn.execute(sa.text(sql)).fetchone()
                    if row and row[0] is not None:
                        candidates.append(int(row[0]))
                except sa.exc.SQLAlchemyError as exc:
                    logger.debug("Oracle retention probe query failed: %s", exc)
                    continue
        oldest = min(candidates) if candidates else None
        return classify_scn_retention(
            resume, oldest, cursor_key=ck, at_risk_headroom=at_risk_headroom
        )
    except Exception as exc:
        return RetentionProbeResult(
            status="unknown",
            dialect="oracle",
            resume=str(resume) if resume else "",
            cursor_key=ck,
            message=f"Retention probe failed: {exc}",
            details={"error": str(exc)[:300]},
        )


def probe_mysql_retention(
    cfg: dict[str, Any],
    *,
    cursor_key: str = "",
    watermark: str | None = None,
) -> RetentionProbeResult:
    """Live probe: watermark file:pos / GTID vs ``SHOW BINARY LOGS`` + gtid_purged."""
    from connectors.mysql_conn import get_connection

    from services.sync_cursor import get_watermark

    ck = (cursor_key or "").strip()
    wm = watermark if watermark is not None else (get_watermark(ck) if ck else None)
    resume = _resume_binlog_from_watermark(wm)
    dialect = "mariadb" if "maria" in str(cfg.get("type") or "").lower() else "mysql"
    database = cfg.get("database") or cfg.get("schema") or ""
    try:
        conn = get_connection(
            host=cfg.get("host") or "localhost",
            port=cfg.get("port") or 3306,
            database=database,
            username=cfg.get("username") or "",
            password=cfg.get("password") or "",
            connection_string=cfg.get("connection_string") or "",
            ssl=bool(cfg.get("ssl")),
        )
        try:
            with conn.cursor() as cur:
                logs: list[str] = []
                try:
                    cur.execute("SHOW BINARY LOGS")
                    for row in cur.fetchall() or []:
                        if row and row[0]:
                            logs.append(str(row[0]))
                except Exception as exc:
                    logger.debug("SHOW BINARY LOGS failed: %s", exc)
                gtid_purged = ""
                try:
                    cur.execute("SELECT @@GLOBAL.gtid_purged")
                    row = cur.fetchone()
                    if row and row[0]:
                        gtid_purged = str(row[0])
                except Exception as exc:
                    logger.debug("gtid_purged read failed: %s", exc)
                gtid_in_purged: bool | None = None
                resume_gtid = str(resume.get("gtid") or "")
                if resume_gtid and gtid_purged:
                    try:
                        cur.execute(
                            "SELECT GTID_SUBSET(%s, @@GLOBAL.gtid_purged)",
                            (resume_gtid,),
                        )
                        row = cur.fetchone()
                        if row is not None:
                            gtid_in_purged = bool(int(row[0])) if row[0] is not None else False
                    except Exception as exc:
                        logger.debug("GTID_SUBSET(gtid_purged) failed: %s", exc)
            return classify_binlog_retention(
                str(resume.get("file") or ""),
                resume.get("pos"),
                logs,
                resume_gtid=resume_gtid,
                gtid_purged=gtid_purged,
                gtid_in_purged=gtid_in_purged,
                cursor_key=ck,
                dialect=dialect,
            )
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as exc:
        return RetentionProbeResult(
            status="unknown",
            dialect=dialect,
            resume=(
                f"{resume.get('file')}:{resume.get('pos')}"
                if resume.get("file")
                else str(resume.get("gtid") or "")
            ),
            cursor_key=ck,
            message=f"Retention probe failed: {exc}",
            details={"error": str(exc)[:300]},
        )


_PG_RETENTION_DIALECTS = frozenset(
    {
        "postgresql",
        "postgres",
        "pg",
        "amazon_rds_postgresql",
        "aurora_postgres",
        "alloydb",
        "supabase",
        "timescaledb",
        "cloudsql_postgres",
    }
)


def probe_postgres_retention(
    cfg: dict[str, Any],
    *,
    table: str = "",
    cursor_key: str = "",
    watermark: str | None = None,
    slot_name: str = "",
) -> RetentionProbeResult:
    """Live probe: ``pg_replication_slots`` existence + ``wal_status`` (PG13+)."""
    from connectors.postgresql_change_stream import _slot_name
    from connectors.postgresql_conn import get_connection

    from services.sync_cursor import get_watermark

    ck = (cursor_key or "").strip()
    wm = watermark if watermark is not None else (get_watermark(ck) if ck else None)
    database = str(cfg.get("database") or "postgres")
    slot = (slot_name or "").strip() or (
        _slot_name(database, table, ck) if (table or ck) else ""
    )
    if not slot:
        return RetentionProbeResult(
            status="n_a",
            dialect="postgresql",
            cursor_key=ck,
            message="slot_name or table+cursor_key required for PostgreSQL retention probe.",
        )
    try:
        conn = get_connection(
            host=cfg.get("host") or "localhost",
            port=cfg.get("port") or 5432,
            database=database,
            username=cfg.get("username") or "",
            password=cfg.get("password") or "",
            connection_string=cfg.get("connection_string") or "",
            ssl=bool(cfg.get("ssl")),
        )
        try:
            exists = False
            wal = ""
            restart = ""
            confirmed = ""
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        """
                        SELECT restart_lsn::text,
                               confirmed_flush_lsn::text,
                               wal_status
                        FROM pg_replication_slots
                        WHERE slot_name = %s
                        """,
                        (slot,),
                    )
                    row = cur.fetchone()
                    if row:
                        exists = True
                        restart = str(row[0] or "")
                        confirmed = str(row[1] or "")
                        wal = str(row[2] or "")
                except Exception as col_exc:
                    msg = str(col_exc).lower()
                    if "wal_status" not in msg and "undefined" not in msg:
                        raise
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    cur.execute(
                        """
                        SELECT restart_lsn::text, confirmed_flush_lsn::text
                        FROM pg_replication_slots
                        WHERE slot_name = %s
                        """,
                        (slot,),
                    )
                    row = cur.fetchone()
                    if row:
                        exists = True
                        restart = str(row[0] or "")
                        confirmed = str(row[1] or "")
            return classify_pg_slot_retention(
                slot_exists=exists,
                wal_status=wal,
                restart_lsn=restart,
                confirmed_flush_lsn=confirmed,
                watermark=wm,
                cursor_key=ck,
                slot_name=slot,
            )
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as exc:
        return RetentionProbeResult(
            status="unknown",
            dialect="postgresql",
            resume=str(wm or ""),
            cursor_key=ck,
            capture_instance=slot,
            message=f"PostgreSQL slot retention probe failed: {exc}",
            details={"error": str(exc)[:300], "slot_name": slot},
        )


def probe_cdc_retention(
    cfg: dict[str, Any],
    *,
    table: str = "",
    schema: str = "",
    cursor_key: str = "",
    watermark: str | None = None,
) -> RetentionProbeResult:
    """Dispatch retention probe by dialect."""
    dialect = str(cfg.get("type") or cfg.get("format") or "").lower()
    if dialect in {"mssql", "sql_server", "microsoft_sql_server", "azure_sql_database", "amazon_rds_sql_server"}:
        dialect = "sqlserver"
    if dialect == "sqlserver":
        return probe_sqlserver_retention(
            cfg,
            table=table,
            schema=schema or "dbo",
            cursor_key=cursor_key,
            watermark=watermark,
        )
    if dialect == "oracle":
        return probe_oracle_retention(
            cfg, cursor_key=cursor_key, watermark=watermark
        )
    if dialect in {"mysql", "mariadb", "amazon_rds_mysql", "azure_mysql", "mysql8"}:
        return probe_mysql_retention(
            cfg, cursor_key=cursor_key, watermark=watermark
        )
    if dialect in _PG_RETENTION_DIALECTS:
        return probe_postgres_retention(
            cfg,
            table=table,
            cursor_key=cursor_key,
            watermark=watermark,
        )
    return RetentionProbeResult(
        status="n_a",
        dialect=dialect or "unknown",
        cursor_key=cursor_key,
        message=f"Retention probe not applicable for dialect '{dialect}'.",
    )


def attach_cdc_retention(cdc: Any, src_cfg: dict[str, Any] | None, *, table: str = "") -> RetentionProbeResult | None:
    """Probe once and stash on the CDC adapter for checkpoint/job fields."""
    if not src_cfg:
        return None
    dialect = str(src_cfg.get("type") or "").lower()
    if dialect not in {
        "sqlserver",
        "mssql",
        "oracle",
        "mysql",
        "mariadb",
        "amazon_rds_mysql",
        "azure_mysql",
        "mysql8",
        "sql_server",
        "microsoft_sql_server",
        "azure_sql_database",
        "amazon_rds_sql_server",
    } | _PG_RETENTION_DIALECTS:
        return None
    table_name = table or str(getattr(cdc, "table", "") or src_cfg.get("table") or "")
    if isinstance(table_name, (list, tuple)):
        table_name = str(table_name[0]) if table_name else ""
    cursor_key = str(getattr(cdc, "cursor_key", "") or src_cfg.get("cursor_key") or "")
    schema = str(src_cfg.get("schema") or getattr(cdc, "schema", "") or "")
    if dialect in _PG_RETENTION_DIALECTS and cdc is not None and hasattr(
        cdc, "_slot_catalog_status"
    ):
        catalog: dict[str, Any] = {}
        try:
            catalog = dict(cdc._slot_catalog_status(max_age_sec=0) or {})
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
        from services.sync_cursor import get_watermark

        wm = getattr(cdc, "consistent_point_lsn", None) or (
            get_watermark(cursor_key) if cursor_key else None
        )
        resume_expected = (
            bool(cdc._resume_expected) if hasattr(cdc, "_resume_expected") else None
        )
        probe = classify_pg_slot_retention(
            slot_exists=catalog.get("slot_exists"),
            wal_status=str(catalog.get("wal_status") or ""),
            restart_lsn=str(catalog.get("restart_lsn") or ""),
            confirmed_flush_lsn=str(catalog.get("confirmed_flush_lsn") or ""),
            watermark=wm,
            resume_expected=resume_expected,
            cursor_key=cursor_key,
            slot_name=str(getattr(cdc, "slot_name", "") or ""),
        )
        try:
            setattr(cdc, "_cdc_retention", probe)
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
        return probe
    probe = probe_cdc_retention(
        src_cfg,
        table=table_name,
        schema=schema,
        cursor_key=cursor_key,
    )
    try:
        setattr(cdc, "_cdc_retention", probe)
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    return probe


def retention_lag_fields(cdc: Any) -> dict[str, Any]:
    probe = getattr(cdc, "_cdc_retention", None)
    if probe is None:
        return {}
    try:
        return dict(probe.job_fields())
    except Exception:
        return {}
