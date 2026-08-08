"""CDC cursor gap — fail-closed when resume is before retained redo/LSN/binlog.

Honesty
-------
SQL Server ``min_lsn``, Oracle oldest redo, and MySQL purged binlog/GTID gaps
mean continuous CDC across the gap is impossible. Operators must clear the
watermark and re-snapshot (``when_needed`` / ``initial``). This is not
exactly-once recovery.
"""

from __future__ import annotations

from typing import Any


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
    ) -> None:
        super().__init__(message)
        self.code = code or "cdc_cursor_gap"
        self.dialect = dialect or ""
        self.resume = resume or ""
        self.retained = retained or ""
        self.cursor_key = cursor_key or ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "dialect": self.dialect,
            "resume": self.resume,
            "retained": self.retained,
            "cursor_key": self.cursor_key,
            "message": str(self),
        }


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
