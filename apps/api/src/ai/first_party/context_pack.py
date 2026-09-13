"""One evidence schema for train and serve — the LM's working memory.

The model is not given a regex per question. It is given a packed
context: today's UTC date, live workspace counts, and any retrieved or
tool text. Training examples use the same keys so serve is not a
different language from train.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def utc_today_spoken(now: datetime | None = None) -> str:
    stamp = now or datetime.now(timezone.utc)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp.astimezone(timezone.utc).strftime("%A, %d %B %Y")


def pack_context(
    *,
    today_utc: str = "",
    connector_count: int | None = None,
    job_count: int | None = None,
    failed_jobs: int | None = None,
    pipeline_count: int | None = None,
    parked_count: int | None = None,
    parked_names: list[str] | None = None,
    enabled_count: int | None = None,
    create_connection: str = "",
    evidence: str = "",
) -> str:
    """Stable key: value lines the decoder is trained to read."""
    lines: list[str] = []
    if today_utc:
        lines.append(f"TODAY_UTC: {today_utc}")
    if connector_count is not None:
        lines.append(f"CONNECTORS: {int(connector_count)}")
    if job_count is not None:
        lines.append(f"JOBS: {int(job_count)}")
    if failed_jobs is not None:
        lines.append(f"FAILED_JOBS: {int(failed_jobs)}")
    if pipeline_count is not None:
        lines.append(f"PIPELINES: {int(pipeline_count)}")
    if parked_count is not None:
        names = [n for n in (parked_names or []) if n]
        extra = f" ({', '.join(names[:4])})" if names else ""
        lines.append(f"PARKED: {int(parked_count)}{extra}")
    if enabled_count is not None:
        lines.append(f"ENABLED: {int(enabled_count)}")
    if create_connection:
        lines.append(f"CREATE_CONNECTION: {create_connection}")
    body = (evidence or "").strip()
    if body:
        lines.append("EVIDENCE:")
        lines.append(body)
    return "\n".join(lines)


def pack_from_workspace(
    ctx: dict[str, Any] | None = None,
    *,
    evidence: str = "",
    now: datetime | None = None,
) -> str:
    """Serve-time pack from the live greeting/briefing context."""
    ctx = ctx or {}
    connectors = ctx.get("connectors") or []
    jobs = ctx.get("recent_jobs") or []
    n_conn = len(connectors) if isinstance(connectors, list) else 0
    n_jobs = len(jobs) if isinstance(jobs, list) else 0
    failed = 0
    if isinstance(jobs, list):
        failed = sum(
            1
            for j in jobs
            if isinstance(j, dict)
            and str(j.get("status") or "").lower() in {"failed", "error"}
        )
    return pack_context(
        today_utc=utc_today_spoken(now),
        connector_count=n_conn or None,
        job_count=n_jobs or None,
        failed_jobs=failed or None,
        create_connection="confirm_gated",
        evidence=evidence,
    )
