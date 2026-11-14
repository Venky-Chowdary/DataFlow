"""Workspace asks the keyword router misreads, resolved before any tool runs.

Two failure shapes kept reaching operators. A job-status question with no data
object in it — "how many completed", "and failed?", "which of those failed" —
was read as an aggregate over a table called ``completed`` and answered
"Connector not found". And a transform request had nothing that could act on
it, so Pilot recited documentation. Both are decided here from the turn and
the subject of the conversation so far, never from a guess at a table name.
"""
from __future__ import annotations

import re
from typing import Any

_STATUS_WORDS: dict[str, str] = {
    "completed": "completed", "complete": "completed", "succeeded": "completed",
    "successful": "completed", "finished": "completed", "done": "completed",
    "failed": "failed", "failing": "failed", "failures": "failed",
    "running": "running", "in progress": "running",
    "cancelled": "cancelled", "canceled": "cancelled",
    "queued": "queued", "pending": "pending",
}
_STATUS_RE = re.compile(
    r"\b(completed|complete|succeeded|successful|finished|failed|failing|failures|"
    r"running|in\s+progress|cancell?ed|canceled|queued|pending)\b",
    re.I,
)
# A question that points at data ("failed rows in orders", "status column")
# belongs to the data tools, not the jobs ledger.
_DATA_SCOPE_RE = re.compile(
    r"\b(?:rows?|records?|columns?|fields?|tables?|collections?|where|values?)\b",
    re.I,
)
_SCHEDULE_STATE_RE = re.compile(r"\b(paused|disabled|enabled|active|inactive)\b", re.I)
_SUBJECTS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("jobs", re.compile(r"\b(?:jobs?|runs?|transfers?)\b", re.I)),
    ("pipelines", re.compile(r"\b(?:pipelines?|schedules?|recurring\s+syncs?)\b", re.I)),
    ("connectors", re.compile(r"\b(?:connectors?|connections?)\b", re.I)),
)
_HOW_TO_RE = re.compile(r"^\s*(?:how\s+(?:do|can|would|should)\s+(?:i|we|you)|how\s+to)\b", re.I)
_WORKSPACE_SUMMARY_RE = re.compile(
    r"\b(?:overview|summary|status|state|health)\s+of\s+(?:my|the|our)\s+workspace\b", re.I
)
# Plans that read nothing about the workspace, so rerouting cannot lose a
# question the router already understood.
_WEAK_PLANS = ({"explain_product"}, {"list_jobs"}, set())


def subject_of(text: str) -> str:
    for name, pattern in _SUBJECTS:
        if pattern.search(text or ""):
            return name
    return ""


def conversation_subject(history: list[dict] | None) -> str:
    """The workspace object the operator was last asking about."""
    for turn in reversed(history or []):
        if str(turn.get("role") or "") != "user":
            continue
        found = subject_of(str(turn.get("content") or ""))
        if found:
            return found
    return ""


def job_status_asked(message: str) -> str:
    m = _STATUS_RE.search(message or "")
    if not m:
        return ""
    return _STATUS_WORDS.get(re.sub(r"\s+", " ", m.group(1).lower()), "")


def _names_connector(args: dict[str, Any]) -> bool:
    return any(str(args.get(k) or "").strip() for k in ("name", "connector_id", "connector_name"))


def ground_plan(
    message: str,
    planned: list[tuple[str, dict]],
    history: list[dict] | None = None,
) -> list[tuple[str, dict]]:
    """Reroute status asks to the ledger that records the status."""
    names = {n for n, _ in planned}
    # "how do I move a csv into postgres" asks for the procedure. Planning the
    # move as well read "postgres" as a saved connector and led with its miss.
    if "explain_product" in names and _HOW_TO_RE.match(message or ""):
        return [(n, a) for n, a in planned if n == "explain_product"]
    # "overview of my workspace" asks for the briefing, not the Overview tab.
    if names == {"navigate"} and _WORKSPACE_SUMMARY_RE.search(message or ""):
        return [("brief_workspace", {})]
    followed = _failed_job_followup(message, history) or _template_switch(message, history)
    if followed:
        return followed
    here = subject_of(message)
    prior = conversation_subject(history)

    # Review 2, M3: a pipeline that failed is a pipeline's state, not a job
    # count; "pipelines that need my attention" are the health read; "my
    # uploads" is the dataset listing — never documentation, never jobs.
    owned = _owned_subject_route(message, here)
    if owned is not None:
        return owned

    if _SCHEDULE_STATE_RE.search(message or "") and (
        here == "pipelines" or (not here and prior == "pipelines")
    ):
        if names in _WEAK_PLANS:
            return [("list_schedules", {})]

    status = job_status_asked(message)
    if not status or here not in ("", "jobs") or _DATA_SCOPE_RE.search(message or ""):
        return planned
    if here != "jobs" and prior not in ("", "jobs"):
        return planned
    misread_aggregate = names == {"aggregate_data"} and not any(
        _names_connector(a) for _, a in planned
    )
    if names in _WEAK_PLANS or misread_aggregate:
        return [("list_jobs", {"limit": 25})]
    # "which jobs failed" led with the failure-handling docs above the ledger.
    if (
        names == {"explain_product", "list_jobs"}
        and _LEDGER_ASK_RE.search(message or "")
        and not _CONCEPT_RE.search(message or "")
    ):
        return [("list_jobs", {"limit": 25})]
    return planned


_PIPELINE_FAIL_RE = re.compile(r"\bfail\w*|\bbroken\b|\berrored\b", re.I)
_PIPELINE_ATTENTION_RE = re.compile(
    r"\b(?:need\w*|requir\w*|want\w*)\s+(?:my\s+|your\s+|our\s+|any\s+)?attention\b"
    r"|\b(?:problems?|issues?|trouble|unhealthy|health(?:y)?|attention)\b",
    re.I,
)
_MY_UPLOADS_RE = re.compile(
    r"\b(?:datasets?|uploads?|uploaded\s+files?|files)\b[^?.!]*\b(?:mine|my|i\s+(?:have\s+)?upload\w*|i\s+have)\b"
    r"|\b(?:my|mine|i\s+(?:have\s+)?upload\w*|have\s+i\s+upload\w*|do\s+i\s+have)\b[^?.!]*"
    r"\b(?:datasets?|uploads?|uploaded|files)\b",
    re.I,
)


def _owned_subject_route(message: str, here: str) -> list[tuple[str, dict]] | None:
    """The tool that owns a pipeline-state or my-uploads question, or None."""
    text = message or ""
    if _HOW_TO_RE.match(text):
        return None
    if here == "pipelines" and _PIPELINE_FAIL_RE.search(text):
        return [("list_schedules", {"state": "failed"})]
    if here == "pipelines" and _PIPELINE_ATTENTION_RE.search(text):
        return [("list_schedules", {})]
    if not here and _MY_UPLOADS_RE.search(text) and not _DATA_SCOPE_RE.search(text):
        return [("list_datasets", {})]
    return None


_LEDGER_ASK_RE = re.compile(r"\b(?:which|how\s+many|list|show|any|count|number\s+of)\b", re.I)
_CONCEPT_RE = re.compile(
    r"\b(?:what\s+(?:does|do|is|are)|means?|explain|why\s+(?:do|does|would)|how\s+(?:does|do)\s+(?:a|the))\b",
    re.I,
)


_CREATE_VERB_RE = re.compile(
    r"^\s*(?:please\s+)?(?:(?:can|could|will|would)\s+you\s+)?(?:please\s+)?"
    r"(?:create|make|build|add|set\s*up|setup|clone|use|start\s+with)\b",
    re.I,
)
_TRANSFORM_NOUN_RE = re.compile(r"\b(?:transform(?:ation)?s?|recipes?|templates?)\b", re.I)
_SYNONYMS = {
    "dedup": "dedupe", "deduplicate": "dedupe", "duplicates": "dedupe", "duplicate": "dedupe",
    "scd": "scd2", "scd-2": "scd2", "slowly": "scd2", "history": "history",
    "emails": "email", "hashing": "hash", "pii": "pii", "mask": "hash",
    "numeric": "numerics", "numbers": "numerics", "casting": "cast",
    "trimming": "trim", "titlecase": "title", "name": "names",
}
_STOP = frozenset({"transform", "safe", "keep", "the", "and", "a", "an", "null", "case", "dates", "effective", "surrogate", "standardise", "plus"})


def _tokens(text: str) -> set[str]:
    out = set()
    for tok in re.findall(r"[a-z0-9]+(?:-[0-9])?", (text or "").lower()):
        tok = _SYNONYMS.get(tok, tok)
        if tok not in _STOP and len(tok) > 2:
            out.add(tok)
    return out


def match_transform_template(message: str) -> str:
    """Template key a "create the dedupe transform" request names, or ""."""
    if not (_CREATE_VERB_RE.search(message or "") and _TRANSFORM_NOUN_RE.search(message or "")):
        return ""
    return _template_by_tokens(message)


def _template_by_tokens(message: str) -> str:
    from services.transform_recipe_templates import TEMPLATES

    asked = _tokens(message)
    best, score = "", 0
    for key, spec in TEMPLATES.items():
        overlap = len(asked & _tokens(f"{key} {spec.get('name', '')}"))
        if overlap > score:
            best, score = key, overlap
    return best


_FAILED_FOLLOWUP_RE = re.compile(
    r"^\s*(?:(?:ok(?:ay)?|so|and|then)[,\s]+)?(?:"
    r"how\s+(?:do|can|should)\s+(?:i|we)\s+fix\s+(?:it|that|this|them)"
    r"|(?:can\s+you\s+)?fix\s+(?:it|that|this)"
    r"|(?:can\s+you\s+)?(?:retry|re-?run|restart|run)\s+(?:it|that|this)(?:\s+again)?"
    r"|what\s+(?:was|is)\s+the\s+(?:error|problem|issue|cause)"
    r"|what\s+went\s+wrong"
    r"|what\s+happened"
    r"|what\s+caused\s+(?:it|that|this|the\s+failure)"
    r"|why\s+(?:did|does|is|was)\s+(?:it|that|this|the\s+job)\s+(?:fail(?:ed|ing)?|break|broken|error)"
    r"|why(?:\s+(?:not|so))?(?=\s*\?*\s*$)"
    r")\b",
    re.I,
)
_FAILED_JOB_LINE_RE = re.compile(r"`([a-f0-9]{20,26})`[^\n]*\bfailed\b", re.I)


def _failed_job_followup(message: str, history: list[dict] | None) -> list[tuple[str, dict]]:
    """"how do I fix it" after a failed-jobs list is about the failed job on screen."""
    if not _FAILED_FOLLOWUP_RE.match(message or ""):
        return []
    from .followup import last_assistant_content

    hit = _FAILED_JOB_LINE_RE.search(last_assistant_content(history) or "")
    return [("get_job", {"job_id": hit.group(1)})] if hit else []


_SWITCH_RE = re.compile(
    r"\b(?:instead|rather|make\s+it|switch\s+to|go\s+with|use|pick|the\s+\w+(?:\s+\w+)?\s+one)\b", re.I
)


def _template_switch(message: str, history: list[dict] | None) -> list[tuple[str, dict]]:
    """"actually make it scd2 history instead" while a transform is on screen."""
    if not _SWITCH_RE.search(message or ""):
        return []
    from .followup import last_assistant_content

    if "transform" not in (last_assistant_content(history) or "").lower():
        return []
    key = _template_by_tokens(message)
    return [("create_transform_recipe", {"template_key": key})] if key else []
