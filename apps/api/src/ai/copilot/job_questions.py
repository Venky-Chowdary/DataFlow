"""One reading of a question about the job ledger, shared by planner and composer.

The keyword planner knew two things about a jobs question — that it mentions
jobs, and whether it contains a failure word. Everything else the operator
said (a status, "today", "need attention", "success rate") was dropped, and
the answer was whatever ``list_jobs`` returned for the whole history, stated
as fact. ``parse_job_ask`` reads those parts once; ``ground_job_plan`` turns
them into ``list_jobs`` arguments so the store filters and counts, and
``job_narration`` renders the same reading.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from .job_phrases import narrowing_in, read_filters
from .job_window import window_args_from_message

LAST_JOB_RE = re.compile(
    r"\b(?:last|latest|most\s+recent|newest)\s+(?:transfer|job|run|sync)\b"
    r"|\bstatus of (?:my\s+|the\s+)?(?:last\s+|latest\s+)?(?:transfer|job|run)\b",
    re.I,
)
_JOBS_SUBJECT_RE = re.compile(r"\b(?:jobs?|runs?|transfers?|syncs?)\b", re.I)
_OTHER_SUBJECT_RE = re.compile(
    r"\b(?:pipelines?|schedules?|recurring\s+syncs?|connectors?|connections?|contracts?|"
    r"datasets?|gates?|preflight|validat\w+)\b",
    re.I,
)
#: A question that asks the ledger for a count or a list.
_LEDGER_ASK_RE = re.compile(r"\b(?:how\s+many|which|list|show|count|number\s+of|any|did|status)\b", re.I)
_ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_JOB_ID_RE = re.compile(r"\b[0-9a-f]{24}\b|\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}\b", re.I)
_HOW_TO_RE = re.compile(r"^\s*(?:how\s+(?:do|can|would|should|to)\b|what\s+(?:does|do)\b|explain\b)", re.I)
# A question about jobs in general — "what happens if a job fails halfway",
# "when a transfer fails, does it resume" — is a documentation question. The
# indefinite article is the tell: the operator's own ledger is "my jobs".
_CONCEPT_RE = re.compile(
    r"\b(?:what\s+(?:does|do)\s|what\s+is\s+(?:a|an|the\s+difference)\b|means?\b|explain\b|"
    r"why\s+(?:do|does|would)\b|how\s+(?:does|do)\s+(?:a|the|datawrap|jobs?)\b|"
    r"what\s+(?:happens|if)\b|what\s+would\s+happen\b|is\s+it\s+possible\b|"
    r"(?:a|an|any\s+given)\s+(?:\w+\s+)?(?:job|transfer|run|sync)\b|"
    r"(?:if|when|whenever|once|after|before)\s+(?:a|the|my|our)?\s*(?:job|transfer|run|sync)s?\s+"
    r"(?:fails?|is|are|gets?|completes?|finishes|runs?)\b)",
    re.I,
)
_COMMAND_RE = re.compile(
    r"^\s*(?:please\s+)?(?:(?:can|could|will|would)\s+you\s+)?(?:please\s+)?"
    r"(?:cancel|retry|re-?run|resume|replay|restart|start|run|create|make|schedule|delete|"
    r"remove|pause|stop|kill|transfer|copy|move|sync|load|migrate|open|go\s+to|take\s+me)\b",
    re.I,
)
_DATA_SCOPE_RE = re.compile(
    r"\b(?:rows?|records?|columns?|fields?|tables?|collections?|where|values?)\b", re.I
)
_ATTENTION_RE = re.compile(
    r"\b(?:needs?|needing|requires?|requiring|want(?:s|ing)?)\s+(?:my\s+|your\s+|our\s+|any\s+)?attention\b",
    re.I,
)
_SUCCESS_RATE_RE = re.compile(
    r"\b(?:success|failure|pass|completion)\s+rate\b"
    r"|\bwhat\s+(?:percent(?:age)?|share|fraction|proportion)\s+of\b",
    re.I,
)
_DURATION_RE = re.compile(
    r"\bhow\s+long\s+(?:did|does|has|have|is|was|were)\b|\bduration\b|\bhow\s+much\s+time\b"
    r"|\b(?:elapsed|run)\s*time\b",
    re.I,
)
_ROWS_MOVED_RE = re.compile(
    r"\bhow\s+many\s+(?:rows|records)\b[^?]*\b(?:moved?|transferr?(?:ed)?|load(?:ed)?|"
    r"sync(?:ed)?|process(?:ed)?|migrat(?:e|ed)|cop(?:y|ied))\b",
    re.I,
)

#: A plan made only of these read nothing the ledger read does not replace.
_REPLACEABLE = frozenset({"explain_product", "search_knowledge", "describe_pilot", "list_jobs"})


@dataclass(frozen=True)
class JobAsk:
    """What a jobs question asks the ledger for."""

    status: str = ""  # failed | running | completed | attention | an exact status word
    metric: str = ""  # success_rate | duration | rows_moved
    last: bool = False
    window: dict[str, str] = field(default_factory=dict)
    #: A time phrase or qualifier the question names that no filter applies.
    #: Such a question is refused by name, never answered for the whole history.
    unapplied: str = ""
    #: ``time`` when ``unapplied`` is a time phrase, else ``filter``.
    unapplied_kind: str = ""

    def tool_args(self) -> dict[str, Any]:
        """Arguments that make ``list_jobs`` filter and count server-side."""
        if self.unapplied:
            # The tool refuses an unknown window by name: never a silent drop.
            return {"window": self.unapplied}
        args: dict[str, Any] = dict(self.window)
        if self.metric == "rows_moved":
            args["limit"] = 100
        # The last job is the newest row of the unfiltered ledger, whatever
        # status the question assumed it has.
        if not self.last and not self.metric and self.status in ("failed", "running", "completed", "attention"):
            args["status"] = self.status
        return args


def parse_job_ask(message: str) -> JobAsk | None:
    """The ledger question in ``message``, or None when it is not one."""
    text = message or ""
    if not text.strip() or _JOB_ID_RE.search(text):
        return None
    if _HOW_TO_RE.search(text) or _CONCEPT_RE.search(text) or _COMMAND_RE.search(text):
        return None
    rows_moved = bool(_ROWS_MOVED_RE.search(text))
    about_jobs = bool(_JOBS_SUBJECT_RE.search(text))
    if _OTHER_SUBJECT_RE.search(text) and not rows_moved:
        return None
    reading = read_filters(text)
    window = reading.window
    if rows_moved:
        metric = "rows_moved" if (window or about_jobs) else ""
        if not metric:
            return None
        return JobAsk(metric=metric, window=window, unapplied=reading.unapplied, unapplied_kind=reading.kind)
    if not about_jobs or _DATA_SCOPE_RE.search(text):
        return None
    last = bool(LAST_JOB_RE.search(text))
    if _SUCCESS_RATE_RE.search(text):
        metric = "success_rate"
    elif _DURATION_RE.search(text):
        # Only the last job's duration is on a ledger row; anything wider is
        # declined by the composer rather than answered with a job list.
        metric = "duration"
    else:
        metric = ""
    status = "" if metric else asked_status(text)
    unapplied, kind = reading.unapplied, reading.kind
    if unapplied and not (status or metric or last or _LEDGER_ASK_RE.search(text)):
        # "if I run the same CDC change twice is it safe" is not a ledger read.
        return None
    if not unapplied and not last and (status or metric or window):
        # "failed jobs on Local Postgres", "did not fail": a connector filter
        # and a negated count are not computed.
        unapplied, kind = narrowing_in(text), "filter"
    if not (status or window or metric or unapplied):
        return None
    return JobAsk(status=status, metric=metric, last=last, window=window,
                  unapplied=unapplied, unapplied_kind=kind if unapplied else "")


def asked_status(message: str) -> str:
    """The status a question names, with bare failure words read as ``failed``."""
    from .workspace_intents import job_status_asked

    if _ATTENTION_RE.search(message or ""):
        return "attention"
    status = job_status_asked(message or "")
    if not status and re.search(r"\b(?:fail\w*|errors?|errored|broken)\b", message or "", re.I):
        return "failed"
    return status


def _followup_ask(message: str) -> JobAsk:
    return JobAsk(
        status=asked_status(message),
        last=bool(LAST_JOB_RE.search(message or "")),
        window=window_args_from_message(message or ""),
    )


def ground_job_plan(message: str, planned: list[tuple[str, dict]]) -> list[tuple[str, dict]]:
    """Make every ledger read in ``planned`` carry the question's filters."""
    ask = parse_job_ask(message)
    names = {name for name, _ in planned}
    if ask is None:
        # Not a ledger question on its own ("and failed?", "which of those
        # failed"), but a ledger read was planned for it: the status and the
        # window it names still filter that read.
        if "list_jobs" not in names or _HOW_TO_RE.search(message or "") or _CONCEPT_RE.search(message or ""):
            return planned
        ask = _followup_ask(message)
        return [
            (name, {**args, **ask.tool_args()}) if name == "list_jobs" else (name, args)
            for name, args in planned
        ]
    # "failed on 2026-11-08" reads to the data planner as a connector named
    # 2026-11-08: a date is never a connector name.
    misread_aggregate = names == {"aggregate_data"} and not any(
        str(args.get(key) or "").strip() and not _ISO_DATE.fullmatch(str(args.get(key) or "").strip())
        for _, args in planned
        for key in ("name", "connector_id", "connector_name")
    )
    if names <= _REPLACEABLE or misread_aggregate:
        planned = [("list_jobs", {"limit": 5 if ask.last else 25})]
    return [
        (name, {**args, **ask.tool_args()}) if name == "list_jobs" else (name, args)
        for name, args in planned
    ]
