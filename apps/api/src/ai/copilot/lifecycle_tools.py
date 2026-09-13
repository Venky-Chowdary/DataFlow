"""Lifecycle operations Pilot can stage: job cancel/retry/resume/replay, connector
test/delete, schedule pause/resume/delete.

Every mutation here follows the same contract as ``run_schedule_now``: the tool
resolves the object, writes a redacted preview, stages an ack on the server
ledger and returns ``requires_confirm``. Nothing changes until the operator
presses Confirm, and the Confirm endpoint performs the write through the same
REST handler the screen uses — so Pilot never grows a second cancel or delete
implementation with its own rules.

Resolution is deliberately conservative. A job is picked by id, or by a
status word ("the failed job", "the running transfer") only when exactly one
job carries that status in the recent window; a connector or schedule is
picked by the shared fuzzy matcher and any tie becomes a question.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from .schema_tools import AmbiguousConnectorError, _connector_dict, _tool_result

if TYPE_CHECKING:
    from .tools import ToolResult

_JOB_ID = re.compile(
    r"\b([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[0-9a-f]{24})\b"
    r"|\b(?:job|run|transfer)\s+(?:id\s+)?#?([A-Za-z0-9][A-Za-z0-9_-]{7,})\b",
    re.I,
)


def short_job_id(job_id: str) -> str:
    return job_id if len(job_id) <= 24 else job_id[:8]

_TERMINAL = {"completed", "completed_with_quarantine", "failed", "cancelled"}
_ACTIVE = {"pending", "queued", "running", "starting", "paused"}

# Status words an operator uses to point at a job without pasting its id.
_STATUS_WORDS: dict[str, set[str]] = {
    "failed": {"failed"},
    "running": {"running", "starting", "pending", "queued"},
    "paused": {"paused"},
    "cancelled": {"cancelled"},
    "quarantine": {"completed_with_quarantine"},
    "quarantined": {"completed_with_quarantine"},
}


LIFECYCLE_TOOL_DEFINITIONS: list[dict] = [
    {
        "name": "cancel_job",
        "description": (
            "Stage cancellation of a running or queued transfer job. Returns a pending "
            "action — nothing stops until the operator confirms."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {"type": "string"},
                "selector": {
                    "type": "string",
                    "description": "'last', 'running' or 'failed' when no id was given",
                },
            },
            "required": [],
        },
    },
    {
        "name": "retry_job",
        "description": (
            "Stage a from-zero re-run of a failed transfer as a new job. Pending "
            "action; the REST retry rules (duplicate protection) apply on Confirm."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"job_id": {"type": "string"}, "selector": {"type": "string"}},
            "required": [],
        },
    },
    {
        "name": "resume_job",
        "description": (
            "Stage resumption of a failed or paused transfer from its last committed "
            "checkpoint. Pending action until Confirm."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"job_id": {"type": "string"}, "selector": {"type": "string"}},
            "required": [],
        },
    },
    {
        "name": "replay_quarantine",
        "description": (
            "Stage a replay of a job's open quarantine rows through the destination "
            "with the original mapping. Pending action until Confirm."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"job_id": {"type": "string"}, "selector": {"type": "string"}},
            "required": [],
        },
    },
    {
        "name": "test_connector",
        "description": (
            "Probe a saved connector's connectivity now (same as the Test button) and "
            "report the result. Read-only apart from the last-tested badge."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"connector_id": {"type": "string"}, "name": {"type": "string"}},
            "required": [],
        },
    },
    {
        "name": "delete_connector",
        "description": (
            "Stage deletion of one saved connector by name or id. Destructive; pending "
            "action until Confirm. Never deletes more than one connector per ask."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"connector_id": {"type": "string"}, "name": {"type": "string"}},
            "required": [],
        },
    },
    {
        "name": "set_schedule_enabled",
        "description": (
            "Stage pausing (enabled=false) or resuming (enabled=true) a pipeline "
            "schedule. Pending action until Confirm."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "schedule_id": {"type": "string"},
                "name": {"type": "string"},
                "enabled": {"type": "boolean"},
            },
            "required": ["enabled"],
        },
    },
    {
        "name": "delete_schedule",
        "description": (
            "Stage deletion of one pipeline schedule by name or id. Destructive; "
            "pending action until Confirm."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"schedule_id": {"type": "string"}, "name": {"type": "string"}},
            "required": [],
        },
    },
]

LIFECYCLE_TOOL_NAMES: frozenset[str] = frozenset(t["name"] for t in LIFECYCLE_TOOL_DEFINITIONS)

#: tool -> ack kind. The kind is what Confirm dispatches on and what the
#: permission table re-checks at confirm time.
ACK_KIND_BY_TOOL: dict[str, str] = {
    "cancel_job": "cancel_job",
    "retry_job": "retry_job",
    "resume_job": "resume_job",
    "replay_quarantine": "replay_quarantine",
    "delete_connector": "delete_connector",
    "set_schedule_enabled": "set_schedule_enabled",
    "delete_schedule": "delete_schedule",
}


def _job_brief(job: dict[str, Any]) -> dict[str, Any]:
    req = job.get("transfer_request") or {}
    src = req.get("source") or {}
    dst = req.get("destination") or {}
    return {
        "job_id": str(job.get("id") or job.get("job_id") or job.get("_id") or ""),
        "status": str(job.get("status") or ""),
        "source": str(src.get("connector_name") or src.get("name") or src.get("type") or ""),
        "destination": str(dst.get("connector_name") or dst.get("name") or dst.get("type") or ""),
        "table": str(src.get("table") or req.get("source_table") or ""),
        "rows_written": job.get("rows_written"),
        "rejected_rows": job.get("rejected_rows"),
        "progress_pct": job.get("progress_pct"),
    }


def resolve_job(job_id: str = "", selector: str = "") -> tuple[dict[str, Any] | None, str]:
    """Return ``(job, clarification)``. A clarification is a question, never a guess."""
    from .job_reads import list_transfer_jobs, read_transfer_job

    jid = (job_id or "").strip()
    if jid:
        job = read_transfer_job(jid)
        if job:
            return job, ""
        return None, f"Job '{jid}' was not found. Copy the id from Jobs / Job Theater."
    sel = (selector or "").strip().lower()
    jobs, _counts, _store = list_transfer_jobs(limit=25)
    if not jobs:
        return None, "There are no transfer jobs in this workspace yet."
    if sel in {"", "last", "latest", "recent", "most recent", "newest"}:
        newest = jobs[0]
        full = read_transfer_job(str(newest.get("id") or newest.get("job_id") or ""))
        return (full or newest), ""
    wanted = _STATUS_WORDS.get(sel)
    if not wanted:
        return None, f"I don't know which job “{sel}” means — give the job id, or say “the last job” or “the failed job”."
    matches = [j for j in jobs if str(j.get("status") or "").lower() in wanted]
    if not matches:
        return None, f"No {sel} job in the last {len(jobs)} jobs."
    if len(matches) > 1:
        ids = ", ".join(f"**{short_job_id(str(j.get('id') or j.get('job_id') or ''))}**" for j in matches[:5])
        return None, f"More than one job is {sel}. Which one? {ids}"
    full = read_transfer_job(str(matches[0].get("id") or matches[0].get("job_id") or ""))
    return (full or matches[0]), ""


def _stage(tool: str, *, payload: dict[str, Any], preview: dict[str, Any], label: str, destructive: bool) -> ToolResult:
    from .ack_ledger import get_ack_ledger

    kind = ACK_KIND_BY_TOOL[tool]
    ack_id = get_ack_ledger().put(kind=kind, payload=payload, preview=preview)
    return _tool_result(tool,
        success=True,
        output={
            "action": kind,
            "label": label,
            "risk": "mutate",
            "destructive": destructive,
            "requires_confirm": True,
            "ack_id": ack_id,
            "preview": preview,
            **{k: v for k, v in payload.items() if k not in preview},
        },
    )


def _job_tool(tool: str, job_id: str, selector: str) -> ToolResult:
    job, clarify = resolve_job(job_id, selector)
    if not job:
        return _tool_result(tool, success=False, output=None, error=clarify)
    brief = _job_brief(job)
    status = brief["status"].lower()
    jid = brief["job_id"]
    short = short_job_id(jid)
    if tool == "cancel_job":
        if status in _TERMINAL:
            return _tool_result(tool, success=False, output=None,
                error=f"Job {short} is already {status} — there is nothing to cancel.",
            )
        return _stage(tool, payload={"job_id": jid}, preview=brief,
                      label=f"Cancel job {short} ({status})", destructive=False)
    if tool == "retry_job":
        if status not in {"failed", "cancelled"}:
            return _tool_result(tool, success=False, output=None,
                error=f"Job {short} is {status}; only a failed or cancelled job can be retried from zero. Use resume for a paused job.",
            )
        return _stage(tool, payload={"job_id": jid}, preview=brief,
                      label=f"Retry job {short} from the beginning", destructive=True)
    if tool == "resume_job":
        if status not in {"failed", "paused", "cancelled"}:
            return _tool_result(tool, success=False, output=None,
                error=f"Job {short} is {status}; only a failed, cancelled or paused job can be resumed from its checkpoint.",
            )
        return _stage(tool, payload={"job_id": jid}, preview=brief,
                      label=f"Resume job {short} from its last checkpoint", destructive=False)
    if tool == "replay_quarantine":
        if not (job.get("rejected_rows") or status == "completed_with_quarantine" or job.get("rejected_details")):
            return _tool_result(tool, success=False, output=None,
                error=f"Job {short} has no quarantined rows to replay.",
            )
        return _stage(tool, payload={"job_id": jid}, preview=brief,
                      label=f"Replay quarantine rows of job {short}", destructive=False)
    return _tool_result(tool, success=False, output=None, error=f"Unknown job tool {tool}")


def _connector_brief(conn: dict[str, Any]) -> dict[str, Any]:
    return {
        "connector_id": str(conn.get("id") or conn.get("connector_id") or ""),
        "name": str(conn.get("name") or ""),
        "type": str(conn.get("type") or conn.get("format") or ""),
        "host": str(conn.get("host") or ""),
        "database": str(conn.get("database") or ""),
        "role": str(conn.get("role") or ""),
    }


def _connector(tool: str, connector_id: str, name: str) -> tuple[dict[str, Any] | None, ToolResult | None]:
    if not (connector_id or "").strip() and not (name or "").strip():
        return None, _tool_result(tool, success=False, output=None,
                                error="Which connector? Name a saved connector from Connectors.")
    try:
        conn = _connector_dict(connector_id, name)
    except AmbiguousConnectorError as exc:
        return None, _tool_result(tool, success=False, output=None, error=exc.message)
    if not conn:
        return None, _tool_result(tool, success=False, output=None,
            error=f"No connector matched “{name or connector_id}”. Name a saved connector from Connectors.",
        )
    return conn, None


def test_connector(connector_id: str = "", name: str = "") -> ToolResult:
    conn, err = _connector("test_connector", connector_id, name)
    if err:
        return err
    assert conn is not None
    brief = _connector_brief(conn)
    from services.connector_probe import probe_saved_connector
    from services.connector_store import mark_tested

    ok, message, _cfg = probe_saved_connector(brief["connector_id"], workspace_id=conn.get("workspace_id") or None)
    mark_tested(brief["connector_id"], ok)
    return _tool_result("test_connector",
        success=True,
        output={**brief, "ok": bool(ok), "message": str(message or ""), "risk": "safe"},
    )


def delete_connector(connector_id: str = "", name: str = "") -> ToolResult:
    conn, err = _connector("delete_connector", connector_id, name)
    if err:
        return err
    assert conn is not None
    brief = _connector_brief(conn)
    dependents = _schedules_bound_to(brief["connector_id"])
    preview = {**brief, "bound_schedules": dependents}
    return _stage(
        "delete_connector",
        payload={"connector_id": brief["connector_id"], "name": brief["name"]},
        preview=preview,
        label=f"Delete connector “{brief['name']}”",
        destructive=True,
    )


def _schedules_bound_to(connector_id: str) -> list[str]:
    try:
        from services.schedule_store import list_schedules
    except ImportError:
        return []
    names: list[str] = []
    for s in list_schedules():
        if connector_id and connector_id in {
            str(getattr(s, "source_connector_id", "") or ""),
            str(getattr(s, "dest_connector_id", "") or ""),
        }:
            names.append(str(s.name or s.id))
    return names[:10]


def _schedule(tool: str, resolver: Any, schedule_id: str, name: str) -> tuple[Any, "ToolResult | None"]:
    sched, clarify = resolver(schedule_id, name)
    if clarify:
        return None, _tool_result(tool, success=False, output=None, error=clarify)
    if not sched:
        return None, _tool_result(tool, success=False, output=None,
                                error="Which pipeline? Give a schedule name or id from Pipelines.")
    return sched, None


def set_schedule_enabled(resolver: Any, schedule_id: str = "", name: str = "", enabled: bool = True) -> ToolResult:
    sched, err = _schedule("set_schedule_enabled", resolver, schedule_id, name)
    if err:
        return err
    currently = bool(getattr(sched, "enabled", True))
    verb = "Resume" if enabled else "Pause"
    if currently == bool(enabled):
        state = "enabled" if currently else "paused"
        return _tool_result("set_schedule_enabled", success=False, output=None,
            error=f"Pipeline “{sched.name}” is already {state}.",
        )
    preview = {
        "schedule_id": sched.id,
        "name": sched.name,
        "enabled_now": currently,
        "enabled_after": bool(enabled),
        "next_run_at": getattr(sched, "next_run_at", "") or "",
    }
    return _stage(
        "set_schedule_enabled",
        payload={"schedule_id": sched.id, "name": sched.name, "enabled": bool(enabled)},
        preview=preview,
        label=f"{verb} pipeline “{sched.name}”",
        destructive=False,
    )


def delete_schedule(resolver: Any, schedule_id: str = "", name: str = "") -> ToolResult:
    sched, err = _schedule("delete_schedule", resolver, schedule_id, name)
    if err:
        return err
    preview = {
        "schedule_id": sched.id,
        "name": sched.name,
        "sync_mode": str(getattr(sched, "sync_mode", "") or ""),
        "enabled": bool(getattr(sched, "enabled", True)),
        "runs_recorded": len(getattr(sched, "run_history", []) or []),
    }
    return _stage(
        "delete_schedule",
        payload={"schedule_id": sched.id, "name": sched.name},
        preview=preview,
        label=f"Delete pipeline “{sched.name}”",
        destructive=True,
    )


# ----------------------------------------------------------------------------
# Deterministic planner: verb + object grammar, not a phrase table.
# ----------------------------------------------------------------------------

_JOB_NOUN = r"(?:job|transfer|run|migration|sync|load)"
_CONN_NOUN = r"(?:connector|connection|source|destination|database\s+connection)"
_SCHED_NOUN = r"(?:schedule|pipeline|cron\s+job|cron)"

_CANCEL = r"(?:cancel|stop|abort|kill|halt|terminate)"
_RETRY = r"(?:retry|re-?run|rerun|run\s+again|restart\s+from\s+(?:zero|scratch|the\s+beginning))"
_RESUME_JOB = r"(?:resume|continue|pick\s+up|restart)"
_REPLAY = r"(?:replay|re-?write|re-?send|re-?submit|re-?process|push)"
_TEST = r"(?:test|probe|ping|check\s+(?:the\s+)?connection\s+(?:to|of|for)|verify\s+(?:the\s+)?connection\s+(?:to|of|for)|is\s+.+\s+reachable)"
_DELETE = r"(?:delete|remove|drop|destroy|get\s+rid\s+of|erase)"
_PAUSE = r"(?:pause|disable|suspend|turn\s+off|switch\s+off|stop|halt)"
_ENABLE = r"(?:resume|enable|unpause|re-?enable|turn\s+on|switch\s+on|reactivate)"

_ART = r"(?:the\s+|my\s+|this\s+|that\s+|our\s+)?"
_SELECTOR = r"(?P<sel>last|latest|recent|most\s+recent|newest|running|failed|paused|cancelled|quarantined?)"

_HOWTO = re.compile(
    r"^\s*(?:how\s+(?:do|can|would|should)\s+(?:i|we|you)|how\s+to|what\s+(?:is|does|happens)|can\s+(?:i|you|we)|is\s+it\s+possible|"
    r"should\s+i|when\s+(?:should|do)|why)\b",
    re.I,
)
_BULK = re.compile(
    r"\b(?:all|every|each|any|everything)\s+(?:of\s+)?(?:my\s+|the\s+|our\s+)?(?:single\s+)?(?:\w+\s+)?"
    r"(?:connectors?|connections?|schedules?|pipelines?|jobs?|transfers?)\b"
    r"|\b(?:connectors|connections|schedules|pipelines|jobs|transfers)\b",
    re.I,
)


def _named(text: str, m: "re.Match[str]") -> str:
    """The object phrase with the operator's own casing (connector names are case-sensitive labels)."""
    return _strip_object(text[m.start("name"):m.end("name")])


def _strip_object(raw: str) -> str:
    s = re.sub(r"^(?:to|for|of|called|named)\s+", "", raw.strip(), flags=re.I)
    s = re.sub(r"^(?:the|my|this|that|our|a|an)\s+", "", s, flags=re.I)
    s = re.sub(r"^(?:paused|disabled|enabled|running|failed|broken|old|existing)\s*", "", s, flags=re.I)
    if s.lower() in {"the", "my", "this", "that", "our", "a", "an", "it", ""}:
        return ""
    s = re.sub(r"\s+(?:connector|connection|schedule|pipeline|job|transfer|please|now|right\s+now|immediately|for\s+me)\s*$", "", s, flags=re.I)
    s = re.sub(r"\s+(?:connector|connection|schedule|pipeline)\s*$", "", s, flags=re.I)
    return s.strip(" .,!?\"'“”")


def _verb_object(lower: str, verb: str, noun: str) -> "re.Match[str] | None":
    """``<verb> [the] <noun> [called] <name>`` first, then ``<verb> [the] <name> <noun>``.

    Noun-first is tried first so "delete pipeline Nightly Orders" names the
    pipeline, not the word "pipeline".
    """
    return re.search(
        rf"\b{verb}\s+{_ART}{noun}\s+(?:called\s+|named\s+|for\s+)?(?P<name>.+?)\s*[.!?]*\s*$", lower
    ) or re.search(rf"\b{verb}\s+{_ART}(?P<name>.+?)\s+{noun}\b", lower) or re.search(
        rf"\b{verb}\s+{_ART}{noun}\s*[.!?]*\s*$(?P<name>)", lower
    )


def plan_lifecycle_operation(message: str) -> list[tuple[str, dict[str, Any]]] | None:
    """Return a tool plan for a lifecycle operation, or ``None`` when the turn is
    not one (a how-to question, a bulk ask, or something else entirely)."""
    text = (message or "").strip()
    lower = text.lower()
    if not lower or _HOWTO.match(lower):
        return None
    if _BULK.search(lower):
        return None

    # --- jobs -----------------------------------------------------------
    jid_m = _JOB_ID.search(text)
    jid = (jid_m.group(1) or jid_m.group(2) or "") if jid_m else ""

    def _job_plan(tool: str, verb: str) -> list[tuple[str, dict[str, Any]]] | None:
        m = re.search(rf"\b{verb}\s+{_ART}(?:{_SELECTOR}\s+)?{_JOB_NOUN}s?\b", lower) or re.search(
            rf"\b{verb}\s+{_ART}{_JOB_NOUN}\b", lower
        )
        if not m and not (jid and re.search(rf"\b{verb}\b", lower)):
            return None
        sel = (m.group("sel") if m and "sel" in m.groupdict() and m.group("sel") else "")
        if not sel:
            tail = re.search(rf"{_JOB_NOUN}\s+(?:that|which|is)?\s*(failed|running|paused|cancelled|quarantined?)\b", lower)
            sel = tail.group(1) if tail else ""
        args: dict[str, Any] = {}
        if jid:
            args["job_id"] = jid
        elif sel:
            args["selector"] = sel
        else:
            args["selector"] = "last"
        return [(tool, args)]

    if re.search(rf"\b{_REPLAY}\b.{{0,40}}\bquarantin", lower) or re.search(rf"\bquarantin\w*\b.{{0,30}}\b{_REPLAY}\b", lower):
        args = {"job_id": jid} if jid else {"selector": "quarantine"}
        sel_m = re.search(rf"\b{_SELECTOR}\b", lower)
        if not jid and sel_m:
            args = {"selector": sel_m.group("sel")}
        return [("replay_quarantine", args)]
    for tool, verb in (("cancel_job", _CANCEL), ("retry_job", _RETRY), ("resume_job", _RESUME_JOB)):
        # "resume the nightly pipeline" is a schedule verb, handled below.
        if tool == "resume_job" and re.search(rf"\b{_SCHED_NOUN}\b", lower):
            continue
        if tool == "cancel_job" and re.search(rf"\b{_SCHED_NOUN}\b", lower) and not re.search(rf"\b{_JOB_NOUN}\b", lower):
            continue
        plan = _job_plan(tool, verb)
        if plan:
            return plan

    # --- connectors -----------------------------------------------------
    m = _verb_object(lower, _TEST, _CONN_NOUN) or re.search(
        rf"\bis\s+{_ART}(?P<name>.+?)\s+(?:{_CONN_NOUN}\s+)?(?:reachable|online|up|healthy|connected)\??\s*$", lower
    )
    if m:
        return [("test_connector", {"name": _named(text, m)})]
    m = _verb_object(lower, _DELETE, _CONN_NOUN)
    if m:
        name = _named(text, m)
        return [("delete_connector", {"name": name} if name else {})]

    # --- schedules ------------------------------------------------------
    for tool, verb, enabled in (
        ("set_schedule_enabled", _PAUSE, False),
        ("set_schedule_enabled", _ENABLE, True),
        ("delete_schedule", _DELETE, None),
    ):
        m = _verb_object(lower, verb, _SCHED_NOUN)
        if not m:
            continue
        name = _named(text, m)
        args = {"name": name} if name else {}
        if enabled is not None:
            args["enabled"] = enabled
        return [(tool, args)]
    return None
