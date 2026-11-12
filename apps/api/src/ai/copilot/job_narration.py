"""Operator-facing prose for a `list_jobs` read.

Every number here is one the job store counted for the scope, status and time
window the question named (``job_ledger``); the bullets are rows the store
returned for that same filter. A page of recent rows is never presented as the
answer to a count, a row that does not match is never listed under a status
header, and a ledger that could not be read is reported as unread — not as an
empty workspace (audit C1, C2, C3, H2, H3).
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from services.job_list_filters import RUNNING_STATUSES, status_values

from .job_questions import LAST_JOB_RE, JobAsk, asked_status, parse_job_ask

#: Failed on the Jobs page is exactly the ``failed`` status.
FAILED_STATUSES = frozenset(status_values("failed") or ("failed",))

_SHOW_LIMIT = 5
_CANON = {"canceled": "cancelled", "success": "completed", "succeeded": "completed"}
_LEDGER_UNREAD = (
    "I could not read the job ledger just now — the job store did not answer — "
    "so I can't give you job counts or a job list I would stand behind."
)


def _class_statuses(status: str) -> tuple[str, ...]:
    """The stored statuses a Jobs-page pivot covers (exact for anything else)."""
    return tuple(status_values(status) or (status,))


def _count(status_counts: dict, statuses: tuple[str, ...]) -> int:
    wanted = set(statuses)
    return sum(
        int(n or 0)
        for raw, n in (status_counts or {}).items()
        if _CANON.get(str(raw).lower(), str(raw).lower()) in wanted or str(raw) in wanted
    )


def _in_class(job: dict, statuses: tuple[str, ...]) -> bool:
    raw = str(job.get("status") or "")
    return raw in statuses or _CANON.get(raw.lower(), raw.lower()) in statuses


def _bullet(job: dict) -> str:
    verdict = " — verification failed" if job.get("verification_passed") is False else ""
    return (
        f"• `{job.get('id', '?')}` · {job.get('source', '?')} -> "
        f"{job.get('destination', '?')}: "
        f"**{job.get('status')}**{verdict} ({job.get('records', 0):,} records)"
    )


def _window_note(shown: int, total: int) -> str:
    """Say which slice of the history the bullets are, when they are not all of it."""
    if total <= shown:
        return ""
    return f"Showing the {shown} most recent — open **Jobs** for all {total:,}."


def _as_of(output: dict) -> str:
    """The instant the ledger was read, in the window's time zone."""
    window = (output.get("filter") or {}).get("window") or {}
    raw = str(output.get("as_of") or "")
    if not (window and raw):
        return ""
    try:
        zone = str(window.get("timezone") or "UTC")
        stamp = datetime.fromisoformat(raw).astimezone(ZoneInfo(zone))
    except (ValueError, KeyError, OSError):
        return ""
    return f"As of {stamp.strftime('%Y-%m-%d %H:%M')} {zone}."


def _finish(lines: list[str], output: dict) -> str:
    stamp = _as_of(output)
    return "\n".join([*lines, stamp] if stamp else lines)


def _duration_text(seconds: float) -> str:
    seconds = int(max(0, seconds))
    hours, rest = divmod(seconds, 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return f"{hours}h {minutes}m {secs}s"
    return f"{minutes}m {secs}s" if minutes else f"{secs}s"


def job_duration_line(job: dict) -> str:
    """"took 17m 0s (started … → completed …)" from a job's own timestamps, or ""."""
    started = str(job.get("started_at") or job.get("created_at") or "")
    completed = str(job.get("completed_at") or "")
    if not (started and completed):
        return ""
    try:
        begin = datetime.fromisoformat(started.replace("Z", "+00:00"))
        end = datetime.fromisoformat(completed.replace("Z", "+00:00"))
    except ValueError:
        return ""
    if end < begin:
        return ""
    return (
        f"took **{_duration_text((end - begin).total_seconds())}** "
        f"(started {begin.strftime('%Y-%m-%d %H:%M:%S')} UTC → completed {end.strftime('%H:%M:%S')} UTC)"
    )


def _degraded(output: dict) -> str:
    jobs = output.get("jobs") or []
    lines = [_LEDGER_UNREAD]
    if jobs:
        lines.append(
            f"The only runs I can see are the {len(jobs)} this API process holds locally — "
            "not your history:"
        )
        lines += [_bullet(j) for j in jobs[:_SHOW_LIMIT]]
    lines.append("Try again in a moment; **Jobs** reads the same store.")
    return "\n".join(lines)


def _success_rate(census: dict, scope: str, output: dict) -> str:
    counts = census["status_counts"]
    completed = _count(counts, _class_statuses("completed"))
    failed = _count(counts, tuple(FAILED_STATUSES))
    finished = completed + failed
    if not finished:
        return _finish([f"No job has finished{scope}, so there is no success rate to state."], output)
    rate = 100.0 * completed / finished
    other = int(census["total"]) - finished
    line = (
        f"**{completed:,}** of **{finished:,}** finished job(s) completed{scope} — "
        f"a **{rate:.1f}%** success rate; {failed:,} failed."
    )
    if other > 0:
        line += f" {other:,} other job(s) (in flight or cancelled) are not counted."
    return _finish([line], output)


def _rows_moved(output: dict, census: dict, scope: str) -> str:
    jobs = output.get("jobs") or []
    if output.get("truncated"):
        return _finish(
            [
                f"I can't total rows{scope}: **{int(census['total']):,}** job(s) match and I can "
                f"read only the latest {len(jobs)} here, so a sum would be a partial one. "
                "Open **Jobs** and filter the same window for the full figure."
            ],
            output,
        )
    if not jobs:
        return _finish([f"No jobs started{scope}, so no rows were moved in it."], output)
    rows = sum(int(j.get("records") or 0) for j in jobs)
    return _finish(
        [f"**{rows:,}** row(s) were processed by the **{len(jobs):,}** job(s) started{scope}."],
        output,
    )


def _last_job(jobs: list[dict], ask: JobAsk | None, status: str, census: dict) -> str:
    latest = jobs[0]
    state = str(latest.get("status") or "?")
    route = f"{latest.get('source', '?')} → {latest.get('destination', '?')}"
    records = f"{int(latest.get('records') or 0):,} records"
    if ask and ask.metric == "duration":
        took = job_duration_line(latest)
        if took:
            return f"Your last transfer job `{latest.get('id', '?')}` (**{state.upper()}**, {route}) {took}."
        return (
            f"Your last transfer job `{latest.get('id', '?')}` is **{state.upper()}** and has no "
            "completion time recorded, so I can't state how long it took."
        )
    if status == "failed" and state not in FAILED_STATUSES:
        failed = _count(census["status_counts"], tuple(FAILED_STATUSES))
        line = (
            f"Your last transfer job `{latest.get('id', '?')}` did not fail — it is "
            f"**{state.upper()}** ({route}, {records})."
        )
        if failed:
            line += f" **{failed:,}** job(s) have failed overall; ask *show me my failed jobs* to list them."
        return line
    lines = [
        f"Your last transfer job `{latest.get('id', '?')}` is "
        f"**{state.upper()}** — {route} ({records})."
    ]
    if latest.get("verification_passed") is False:
        # A completed status is not a clean bill: say what the Jobs page flags.
        lines.append("Its Gate-8 verification **failed** — open the job's Trust Report before relying on it.")
    err = str(latest.get("error") or "").strip()
    if err:
        lines.append(f"Error: {err}")
    rest = jobs[1:_SHOW_LIMIT]
    if rest:
        lines.append("Earlier jobs:")
        lines += [_bullet(j) for j in rest]
    return "\n".join(lines)


def _status_answer(status: str, output: dict, census: dict, scope: str, windowed: bool) -> str:
    jobs = output.get("jobs") or []
    counts = census["status_counts"]
    total = int(census["total"])
    population = f"the **{total:,}** job(s) started{scope}" if windowed else f"your **{total:,}** job(s)"
    if status == "attention":
        counted = int(census["attention_count"])
        here = [
            j for j in jobs
            if str(j.get("status") or "") in FAILED_STATUSES or j.get("verification_passed") is False
        ]
        if not counted:
            return _finish([f"None of {population} need attention."], output)
        failed = _count(counts, tuple(FAILED_STATUSES))
        lead = (
            f"**{counted:,}** of {population} need attention: {failed:,} failed, "
            f"{max(counted - failed, 0):,} completed but failed verification."
        )
    else:
        statuses = _class_statuses(status)
        counted = _count(counts, statuses)
        here = [j for j in jobs if _in_class(j, statuses)]
        verb = "are" if status in {"running", "queued", "pending"} else "were"
        phrase = "failed" if status == "failed" else f"{verb} {status}"
        if not counted:
            return _finish([f"None of {population} {phrase}."], output)
        lead = f"**{counted:,}** of {population} {phrase}"
        if status == "running":
            parts = [f"{int(counts.get(s) or 0)} {s}" for s in RUNNING_STATUSES if counts.get(s)]
            if parts and parts != [f"{counted} running"]:
                lead += f" ({', '.join(parts)})"
        lead += ":"
    lines = [lead]
    show = here[:_SHOW_LIMIT]
    lines += [_bullet(j) for j in show]
    if not show:
        # Never fall back to rows that do not match the status asked for.
        lines.append(
            f"None of them are in the {len(jobs)} most recent row(s) I read — "
            "filter **Jobs** by status to see them."
        )
    else:
        note = _window_note(len(show), counted)
        if note:
            lines.append(note)
    return _finish(lines, output)


def _window_answer(output: dict, census: dict, scope: str) -> str:
    jobs = output.get("jobs") or []
    total = int(census["total"])
    if not total:
        return _finish([f"No jobs started{scope}."], output)
    counts = census["status_counts"]
    parts = [
        f"{n:,} {label}"
        for label, n in (
            ("completed", _count(counts, _class_statuses("completed"))),
            ("failed", _count(counts, tuple(FAILED_STATUSES))),
            ("in flight", _count(counts, RUNNING_STATUSES)),
        )
        if n
    ]
    lines = [f"**{total:,}** job(s) started{scope}" + (f": {', '.join(parts)}." if parts else ".")]
    lines += [_bullet(j) for j in jobs[:_SHOW_LIMIT]]
    note = _window_note(min(len(jobs), _SHOW_LIMIT), total)
    if note:
        lines.append(note)
    return _finish(lines, output)


def unapplied_filter_answer(message: str | None) -> str | None:
    """A named refusal when the question names a filter Pilot cannot apply.

    Answering such a question with the unfiltered count is how "how many jobs
    failed in 2025" came back as the all-time failure count (review C2).
    """
    ask = parse_job_ask(message or "")
    if ask is None or not ask.unapplied:
        return None
    from .job_window import SUPPORTED_WINDOWS_HINT

    lead = (
        f"I can't apply the time window “{ask.unapplied}” to the job ledger — I can't filter jobs by it yet"
        if ask.unapplied_kind == "time"
        else f"I can't filter jobs by “{ask.unapplied}” yet"
    )
    return (
        f"{lead}, so I won't give you a number for it — "
        "the unfiltered count would not answer what you asked.\n"
        "Here is what I can do: count or list jobs by status (failed, running, completed, "
        f"needs attention) and by time window ({SUPPORTED_WINDOWS_HINT}). "
        "The **Jobs** page also filters by source type, destination type and sync mode."
    )


def narrate_jobs(output: dict, message: str | None) -> str:
    """Answer a jobs question from the counted ledger, then the rows read."""
    if output.get("degraded"):
        return _degraded(output)
    jobs = output.get("jobs") or []
    status_counts = output.get("status_counts") or {}
    # `total` is the counted history; fall back to the window only if a caller
    # (or an older stored turn) never counted.
    total = int(output.get("total") or 0) or len(jobs)
    window = (output.get("filter") or {}).get("window") or {}
    in_window = output.get("window_counts") if window else None
    census = {
        "total": int((in_window or {}).get("total") or 0) if in_window is not None else total,
        "status_counts": (in_window or {}).get("status_counts") or {} if in_window is not None else status_counts,
        "attention_count": int((in_window if in_window is not None else output).get("attention_count") or 0),
    }
    scope = f" {window.get('label')}" if window else ""

    if not total and not jobs:
        return (
            "No transfer jobs yet. Ask me to **plan** or **start** a transfer "
            "(Confirm required), or open **Transfer Studio**."
        )

    ask = parse_job_ask(message or "")
    status = (ask.status if ask else "") or asked_status(message or "")
    if ask and ask.metric == "success_rate":
        return _success_rate(census, scope, output)
    if ask and ask.metric == "rows_moved":
        return _rows_moved(output, census, scope)
    if ask and ask.metric == "duration" and not ask.last:
        return (
            "I can state how long one job took — ask about your last job, or give me a job id. "
            "I don't have a duration summary across jobs."
        )
    unfiltered_page = (output.get("filter") or {}).get("status", "all") == "all"
    if LAST_JOB_RE.search(message or "") and jobs and unfiltered_page:
        # The newest row of the ledger is the last job only when no status filter cut the page.
        return _last_job(jobs, ask, status, census)
    if status:
        return _status_answer(status, output, census, scope, bool(window))
    if window:
        return _window_answer(output, census, scope)

    if not jobs:
        return (
            f"You have **{total:,}** transfer job(s), but none in the window I read. "
            "Open **Jobs** for the full history."
        )
    lines = [f"You have **{total:,}** transfer job(s). Most recent:"]
    lines += [_bullet(j) for j in jobs[:_SHOW_LIMIT]]
    note = _window_note(min(len(jobs), _SHOW_LIMIT), total)
    if note:
        lines.append(note)
    return "\n".join(lines)
