"""CDC retention health — proactive watermark vs retained LSN/SCN.

Honesty
-------
``ok`` / ``at_risk`` / ``gap`` classify resume against live retention. A ``gap``
means continuous CDC across the window is impossible — clear watermark and
re-snapshot (``when_needed`` / ``initial``). This does not invent dual-node AG
failover; single-node cleanup produces the same gap class.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


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


def _resume_lsn_from_watermark(watermark: str | None) -> str:
    if not watermark:
        return ""
    try:
        from connectors.sqlserver_cdc_native import decode_mssql_cdc_token

        token = decode_mssql_cdc_token(watermark)
        lsn = str(token.get("lsn") or "").strip()
        if lsn:
            return lsn
    except Exception:
        pass
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
    except Exception:
        pass
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
        cdc = SqlServerNativeCdc(
            {**cfg, "type": cfg.get("type") or "sqlserver"},
            table=table,
            primary_key=str(cfg.get("primary_key") or "id"),
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
                except Exception:
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
        "sql_server",
        "microsoft_sql_server",
        "azure_sql_database",
        "amazon_rds_sql_server",
    }:
        return None
    table_name = table or str(getattr(cdc, "table", "") or src_cfg.get("table") or "")
    if isinstance(table_name, (list, tuple)):
        table_name = str(table_name[0]) if table_name else ""
    cursor_key = str(getattr(cdc, "cursor_key", "") or src_cfg.get("cursor_key") or "")
    schema = str(src_cfg.get("schema") or getattr(cdc, "schema", "") or "")
    probe = probe_cdc_retention(
        src_cfg,
        table=table_name,
        schema=schema,
        cursor_key=cursor_key,
    )
    try:
        setattr(cdc, "_cdc_retention", probe)
    except Exception:
        pass
    return probe


def retention_lag_fields(cdc: Any) -> dict[str, Any]:
    probe = getattr(cdc, "_cdc_retention", None)
    if probe is None:
        return {}
    try:
        return dict(probe.job_fields())
    except Exception:
        return {}
