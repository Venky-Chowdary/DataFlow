"""Pipeline (schedule) list questions: counted totals, state filters, next run.

``list_schedules`` returned one capped page and the answer stated the page
length as "You have N pipeline schedule(s)" — 20 for a workspace with 30
(audit H3). The counts here are taken over every pipeline the caller may see;
the page is only what is listed, and ``truncated`` says so. "When does X run
next" is answered from that pipeline's own record (``get_schedule``) instead
of from documentation (audit H7).
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Callable

_ENABLED_RE = re.compile(r"\b(?:enabled|active|running|on)\b", re.I)
_PAUSED_RE = re.compile(r"\b(?:paused|disabled|inactive|off|stopped|not\s+enabled|not\s+active)\b", re.I)
_SUBJECT_RE = re.compile(r"\b(?:pipelines?|schedules?|recurring\s+syncs?)\b", re.I)
_WHICH_RE = re.compile(r"\b(?:which|list|show|what)\b", re.I)
_NEXT_RUN_RE = re.compile(
    r"\bnext\s+(?:run|tick|fire|execution|sync)\b"
    r"|\b(?:when|what\s+time)\b[^?]*\b(?:runs?|fires?|ticks?|due|executes?|scheduled|kicks?\s+off)\b[^?]*\b(?:next|again)\b"
    r"|\bwhen\s+is\b[^?]*\b(?:due|scheduled)\b",
    re.I,
)
_PARKED_RE = re.compile(
    r"\b(?:parked|awaiting\s+approval|on\s+approval|needs?\s+approval|pending\s+approval|held\s+for\s+approval)\b",
    re.I,
)
_FAILED_RE = re.compile(r"\b(?:fail\w*|errored|erroring|broken)\b", re.I)
STATES = ("", "enabled", "paused", "parked", "failed")
#: The operator's own pipelines, not pipelines in general.
_OWN_PIPELINES_RE = re.compile(r"\b(?:my|our|how\s+many|which|list|show)\b", re.I)
_CONCEPT_RE = re.compile(r"\b(?:what\s+happens|if|when|whenever|explain|how\s+(?:do|does|can))\b", re.I)


def schedule_state_asked(message: str) -> str:
    """``enabled`` / ``paused`` / ``parked`` / ``failed`` (last run) when named."""
    text = message or ""
    if not _SUBJECT_RE.search(text):
        return ""
    if _PAUSED_RE.search(text):
        return "paused"
    if _PARKED_RE.search(text):
        return "parked"
    if _FAILED_RE.search(text):
        return "failed"
    return "enabled" if _ENABLED_RE.search(text) else ""


def _failed_last(row: dict[str, Any]) -> bool:
    return str(row.get("last_status") or "").lower() in {"failed", "error"}


def _parked(row: dict[str, Any]) -> bool:
    return bool(row.get("needs_approval") or row.get("approval_finding"))


def _next_due(rows: list[dict[str, Any]]) -> dict[str, str] | None:
    """The soonest future run over every pipeline, not the newest pipeline's."""
    now = datetime.now(timezone.utc)
    best: tuple[datetime, dict[str, Any]] | None = None
    for row in rows:
        raw = str(row.get("next_run_at") or "").strip()
        if not raw or not row.get("enabled") or _parked(row):
            continue
        try:
            when = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        when = when if when.tzinfo else when.replace(tzinfo=timezone.utc)
        if when > now and (best is None or when < best[0]):
            best = (when, row)
    if best is None:
        return None
    return {"name": str(best[1].get("name") or ""), "at": str(best[1].get("next_run_at") or "")}


def schedules_output(
    visible: list[Any], summarize: Callable[[Any], dict], limit: int = 20, state: str = ""
) -> tuple[dict[str, Any] | None, str]:
    """``(output, "")`` for a pipelines read, or ``(None, refusal)``."""
    wanted = str(state or "").strip().lower()
    if wanted not in STATES:
        return None, "state must be one of: enabled, paused, parked, failed."
    cap = max(1, min(int(limit or 20), 100))
    # Health is computed over every pipeline the caller may see, never over
    # the page: failing or parked pipelines beyond row 20 still count (H3).
    summaries = [summarize(s) for s in visible]
    enabled = [s for s in visible if getattr(s, "enabled", False)]
    failed_last = [r for r in summaries if _failed_last(r)]
    parked = [r for r in summaries if _parked(r)]
    matching = (
        [r for r in summaries if r.get("enabled")] if wanted == "enabled"
        else [r for r in summaries if not r.get("enabled")] if wanted == "paused"
        else parked if wanted == "parked"
        else failed_last if wanted == "failed"
        else summaries
    )
    rows = matching[:cap]
    return {
        "failed_last_count": len(failed_last),
        "failed_last_names": [str(r.get("name") or "") for r in failed_last[:4]],
        "parked_count": len(parked),
        "parked_names": [str(r.get("name") or "") for r in parked[:4]],
        "next_due": _next_due(summaries),
        "schedules": rows,
        "count": len(rows),
        # Every pipeline the caller may see — the Pipelines page population.
        "total": len(visible),
        "enabled_count": len(enabled),
        "paused_count": len(visible) - len(enabled),
        "state": wanted,
        "filtered_total": len(matching),
        "truncated": len(matching) > len(rows),
        "degraded": False,
        "as_of": datetime.now(timezone.utc).isoformat(),
    }, ""


def unread_schedules_output(reason: str) -> dict[str, Any]:
    """A pipelines read that did not reach the configured store: no counts."""
    return {
        "schedules": [],
        "count": 0,
        "total": 0,
        "degraded": True,
        "unread": True,
        "degraded_reason": reason,
        "as_of": datetime.now(timezone.utc).isoformat(),
    }


UNREAD_PIPELINES = (
    "I could not read your pipelines just now — {reason} — so I can't tell you how many "
    "you have or whether they are running. Open **Pipelines** or try again shortly."
)


def schedule_list_lead(output: dict[str, Any]) -> str:
    """The counted sentence that opens a pipelines list."""
    rows = output.get("schedules") or []
    total = int(output.get("total") or len(rows))
    if "enabled_count" not in output:
        return f"You have **{total} pipeline schedule(s)**."
    enabled = int(output.get("enabled_count") or 0)
    paused = int(output.get("paused_count") or 0)
    lead = f"You have **{total}** pipeline schedule(s): **{enabled}** enabled, **{paused}** paused."
    state = str(output.get("state") or "")
    if state in ("failed", "parked"):
        matched = int(output.get("filtered_total") or 0)
        phrase = "failed their last run" if state == "failed" else "are parked on approval"
        if not matched:
            return f"None of your **{total}** pipeline(s) {phrase}."
        return f"**{matched}** of your **{total}** pipeline(s) {phrase}:"
    if state:
        lead += f" The {state} ones:" if rows else f" None are {state}."
    return lead


def health_lead(output: dict[str, Any]) -> str:
    """Pipeline health over every pipeline: parked, failed last run, next due."""
    total = int(output.get("total") or 0)
    enabled = int(output.get("enabled_count") or 0)
    lead = f"You have **{total}** pipeline(s), **{enabled}** enabled."
    parked = int(output.get("parked_count") or 0)
    failed = int(output.get("failed_last_count") or 0)
    if parked:
        names = ", ".join(f"**{n}**" for n in output.get("parked_names") or [] if n)
        lead += (
            f" **{parked}** parked on approval" + (f" ({names})" if names else "")
            + " — they will not tick until someone approves."
        )
    if failed:
        names = ", ".join(f"**{n}**" for n in output.get("failed_last_names") or [] if n)
        lead += f" **{failed}** failed their last run" + (f" ({names})" if names else "") + "."
    if not enabled:
        lead += " None are enabled, so they will not fire."
    nxt = output.get("next_due") or {}
    if nxt.get("at"):
        lead += f" Next due: **{nxt.get('name')}** at {nxt.get('at')}."
    return lead


def schedule_list_note(output: dict[str, Any], shown: int) -> str:
    population = int(output.get("filtered_total") or output.get("total") or 0)
    if population <= shown:
        return ""
    return f"Showing {shown} of {population:,} — open **Pipelines** for the rest."


def paused_marker(row: dict[str, Any]) -> str:
    return " · paused" if row.get("enabled") is False else ""


def render_schedule_list(
    output: dict[str, Any], *, health: bool, bind_phrase: Callable[[dict], str]
) -> str:
    """The pipelines answer: counted lead, then the page of rows read.

    ``health`` is the "why are my schedules not running" register; ``bind_phrase``
    is the agent's contract/breaker phrase for one row.
    """
    if output.get("unread"):
        return UNREAD_PIPELINES.format(reason=output.get("degraded_reason") or "the store did not answer")
    rows = output.get("schedules", [])
    if rows:
        if health:
            lead = health_lead(output)
            lines = [lead]
        else:
            lines = [schedule_list_lead(output)]
        for s in rows[:8]:
            bind = bind_phrase(s)
            lines.append(
                f"• **{s.get('name')}** · {s.get('interval')}"
                f"{' · cron ' + s['cron'] if s.get('cron') else ''}"
                + paused_marker(s)
                + (f" · {bind}" if bind else "")
                + f" · next `{s.get('next_run_at') or '—'}`"
                + f" · last **{s.get('last_status') or 'never'}** ({s.get('run_count', 0)} runs)"
            )
        note = schedule_list_note(output, min(len(rows), 8))
        if note:
            lines.append(note)
        return ("\n".join(lines))
    elif int(output.get("total") or 0):
        # A state filter matched nothing; the workspace still has pipelines.
        return (schedule_list_lead(output))
    else:
        if health:
            return (
                "No pipelines are scheduled yet, so nothing is running "
                "on a cadence. Create one on **Pipelines** after a transfer."
            )
        else:
            return ("No pipeline schedules yet. Create one from **Pipelines** or after a transfer.")


def asks_next_run(message: str) -> bool:
    return bool(_NEXT_RUN_RE.search(message or ""))


def next_run_lead(schedule: dict[str, Any]) -> str:
    """"**X** runs next at …" from the pipeline's own record, or why it has none."""
    name = schedule.get("name") or "This pipeline"
    if schedule.get("needs_approval"):
        return f"**{name}** is parked on an approval, so it has no next run until that is decided."
    if schedule.get("enabled") is False:
        return f"**{name}** is paused, so it has no next run. Activate it on **Pipelines** to schedule one."
    nxt = str(schedule.get("next_run_at") or "").strip()
    if not nxt:
        return f"**{name}** is enabled but has no next run recorded."
    zone = str(schedule.get("timezone") or "UTC")
    return f"**{name}** runs next at `{nxt}` (cadence evaluated in {zone})."


def _named_schedule(message: str) -> str:
    """The saved pipeline whose name appears in the question (longest match)."""
    from services.schedule_store import list_schedules
    from services.workspace_filter import current_workspace_filter

    text = (message or "").casefold()
    names = [str(s.name or "") for s in list_schedules(current_workspace_filter())]
    hits = [n for n in names if n and re.search(rf"(?<![\w-]){re.escape(n.casefold())}(?![\w-])", text)]
    return max(hits, key=len) if hits else ""


def ground_schedule_plan(message: str, planned: list[tuple[str, dict]]) -> list[tuple[str, dict]]:
    """Route a next-run ask to the pipeline's record; filter a list by the state asked."""
    names = {name for name, _ in planned}
    if asks_next_run(message) and names <= {"explain_product", "search_knowledge", "list_schedules", "list_jobs"}:
        try:
            named = _named_schedule(message)
        except Exception:  # noqa: BLE001 - the read below reports the outage
            named = ""
        if named:
            return [("get_schedule", {"name": named})]
        if _SUBJECT_RE.search(message or ""):
            return [("list_schedules", {"state": "enabled"})]
    state = schedule_state_asked(message)
    if state in ("parked", "failed") and _OWN_PIPELINES_RE.search(message or "") and not _CONCEPT_RE.search(
        message or ""
    ) and names <= {
        "explain_product", "search_knowledge", "list_schedules", "list_jobs", "brief_workspace",
    }:
        # "how many pipelines failed their last run" is a pipelines read, not the job ledger.
        return [("list_schedules", {"state": state})]
    if state and _WHICH_RE.search(message or "") and "list_schedules" in names:
        return [(n, {**a, "state": state}) if n == "list_schedules" else (n, a) for n, a in planned]
    return planned
