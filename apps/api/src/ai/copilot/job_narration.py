"""Operator-facing prose for a `list_jobs` read.

A "how many jobs do I have?" question is answered from the whole history the
tool counted, never from the page of rows it could show — a five-row excerpt
presented as the answer contradicted the Jobs page and lost the operator's trust.
"""

FAILED_STATUSES = frozenset({"failed", "cancelled", "canceled", "error"})

_FAILURE_WORDS = ("fail", "failed", "failure", "error", "broken")

_SHOW_LIMIT = 5


def _bullet(job: dict) -> str:
    return (
        f"• `{job.get('id', '?')}` · {job.get('source', '?')} -> "
        f"{job.get('destination', '?')}: "
        f"**{job.get('status')}** ({job.get('records', 0):,} records)"
    )


def _window_note(shown: int, total: int) -> str:
    """Say which slice of the history the bullets are, when they are not all of it."""
    if total <= shown:
        return ""
    return f"Showing the {shown} most recent — open **Jobs** for all {total:,}."


def narrate_jobs(output: dict, message: str | None) -> str:
    """Answer a jobs question with the counted total, then the recent rows read."""
    jobs = output.get("jobs") or []
    status_counts = output.get("status_counts") or {}
    # `total` is the counted history; fall back to the window only if a caller
    # (or an older stored turn) never counted.
    total = int(output.get("total") or 0) or len(jobs)

    if not jobs:
        if total:
            return (
                f"You have **{total:,}** transfer job(s), but none in the window I read. "
                "Open **Jobs** for the full history."
            )
        return (
            "No transfer jobs yet. Ask me to **plan** or **start** a transfer "
            "(Confirm required), or open **Transfer Studio**."
        )

    failed_total = sum(
        int(n or 0)
        for status, n in status_counts.items()
        if str(status).lower() in FAILED_STATUSES
    )
    failed_here = [
        j for j in jobs if str(j.get("status") or "").lower() in FAILED_STATUSES
    ]

    if any(w in (message or "").lower() for w in _FAILURE_WORDS):
        if failed_total or failed_here:
            counted = failed_total or len(failed_here)
            lines = [f"**{counted:,}** of your **{total:,}** job(s) failed:"]
            show = (failed_here or jobs)[:_SHOW_LIMIT]
            lines += [_bullet(j) for j in show]
            note = _window_note(len(show), counted)
            if note:
                lines.append(note)
            return "\n".join(lines)
        lines = [f"None of your **{total:,}** job(s) failed. Most recent:"]
        lines += [_bullet(j) for j in jobs[:_SHOW_LIMIT]]
        return "\n".join(lines)

    lines = [f"You have **{total:,}** transfer job(s). Most recent:"]
    lines += [_bullet(j) for j in jobs[:_SHOW_LIMIT]]
    note = _window_note(min(len(jobs), _SHOW_LIMIT), total)
    if note:
        lines.append(note)
    return "\n".join(lines)
