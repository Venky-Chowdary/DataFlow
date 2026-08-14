"""CDC cursor gap — fail-closed when resume is before retained redo/LSN/binlog.

Honesty
-------
SQL Server ``min_lsn``, Oracle oldest redo, MySQL purged binlog/GTID, and
PostgreSQL ``pg_replication_slots.wal_status=lost`` (or a dropped slot) mean
continuous CDC across the gap is impossible. Recreating a logical slot at
current WAL while holding an old watermark **skips the lost window** — that is
silent data loss. ``when_needed`` recovers by dropping an invalidated slot,
blocking-snapshot of **current** source keys, then streaming from the new tip.
``initial`` / ``never`` stay fail-closed. Not exactly-once. Not continuous CDC.
"""

from __future__ import annotations

from typing import Any

GAP_ERROR_CODES = frozenset(
    {
        "cdc_cursor_gap",
        "cdc_lsn_gap",
        "cdc_scn_gap",
        "cdc_binlog_gap",
        "cdc_slot_gap",
    }
)


def job_has_cursor_gap(job: dict[str, Any] | None) -> bool:
    """True when the job failed (or is failing) on a retention / failover gap."""
    if not isinstance(job, dict):
        return False
    if job.get("cdc_cursor_gap"):
        return True
    return str(job.get("error_code") or "") in GAP_ERROR_CODES


class CdcCursorGapError(RuntimeError):
    """Resume position is before retained CDC/log history."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "cdc_cursor_gap",
        dialect: str = "",
        resume: str = "",
        retained: str = "",
        cursor_key: str = "",
        snapshot_plan: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code or "cdc_cursor_gap"
        self.dialect = dialect or ""
        self.resume = resume or ""
        self.retained = retained or ""
        self.cursor_key = cursor_key or ""
        self.snapshot_plan = dict(snapshot_plan or {})

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "code": self.code,
            "dialect": self.dialect,
            "resume": self.resume,
            "retained": self.retained,
            "cursor_key": self.cursor_key,
            "message": str(self),
        }
        if self.snapshot_plan:
            out["snapshot_plan"] = dict(self.snapshot_plan)
        return out


class CdcLsnGapError(CdcCursorGapError):
    """SQL Server: resume LSN before capture retention ``min_lsn``."""

    def __init__(
        self,
        message: str,
        *,
        resume_lsn: str = "",
        min_lsn: str = "",
        cursor_key: str = "",
    ) -> None:
        super().__init__(
            message,
            code="cdc_lsn_gap",
            dialect="sqlserver",
            resume=resume_lsn,
            retained=min_lsn,
            cursor_key=cursor_key,
        )
        self.resume_lsn = resume_lsn
        self.min_lsn = min_lsn


class CdcScnGapError(CdcCursorGapError):
    """Oracle: resume SCN before available redo."""

    def __init__(
        self,
        message: str,
        *,
        resume_scn: int | str = "",
        oldest_scn: int | str = "",
        cursor_key: str = "",
    ) -> None:
        super().__init__(
            message,
            code="cdc_scn_gap",
            dialect="oracle",
            resume=str(resume_scn or ""),
            retained=str(oldest_scn or ""),
            cursor_key=cursor_key,
        )
        self.resume_scn = resume_scn
        self.oldest_scn = oldest_scn


class CdcBinlogGapError(CdcCursorGapError):
    """MySQL/MariaDB: resume file:pos or GTID before retained binary logs."""

    def __init__(
        self,
        message: str,
        *,
        resume_file: str = "",
        resume_pos: int | str = "",
        oldest_file: str = "",
        resume_gtid: str = "",
        gtid_purged: str = "",
        cursor_key: str = "",
    ) -> None:
        resume = ""
        if resume_file:
            resume = f"{resume_file}:{resume_pos}" if resume_pos not in ("", None) else str(resume_file)
        elif resume_gtid:
            resume = str(resume_gtid)
        retained = oldest_file or (str(gtid_purged)[:120] if gtid_purged else "")
        super().__init__(
            message,
            code="cdc_binlog_gap",
            dialect="mysql",
            resume=resume,
            retained=retained,
            cursor_key=cursor_key,
        )
        self.resume_file = resume_file
        self.resume_pos = resume_pos
        self.oldest_file = oldest_file
        self.resume_gtid = resume_gtid
        self.gtid_purged = gtid_purged


class CdcSlotGapError(CdcCursorGapError):
    """PostgreSQL: slot missing or ``wal_status=lost`` — WAL for resume is gone."""

    def __init__(
        self,
        message: str,
        *,
        slot_name: str = "",
        wal_status: str = "",
        restart_lsn: str = "",
        confirmed_flush_lsn: str = "",
        cursor_key: str = "",
        snapshot_plan: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            message,
            code="cdc_slot_gap",
            dialect="postgresql",
            resume=confirmed_flush_lsn or slot_name,
            retained=wal_status or restart_lsn or "slot_missing",
            cursor_key=cursor_key,
            snapshot_plan=snapshot_plan,
        )
        self.slot_name = slot_name
        self.wal_status = wal_status
        self.restart_lsn = restart_lsn
        self.confirmed_flush_lsn = confirmed_flush_lsn
