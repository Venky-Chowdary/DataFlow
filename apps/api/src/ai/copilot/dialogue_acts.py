"""Dialogue-act classifier for Datawrap Pilot.

Keyword routing still owns tool selection. This module names the *kind of
turn* so the composer can speak like a copilot instead of dumping a
template: greet, brief the workspace, summarize the last answer, explain
it more simply, or ask what to do next.

Acts never invent facts. They only choose how existing evidence is told.
"""

from __future__ import annotations

import re
from typing import Literal

DialogueAct = Literal[
    "greeting",
    "briefing",
    "summarize_last",
    "explain_simpler",
    "next_action",
    "thanks",
    "general",
    "workspace",
]

_GREETING = re.compile(
    r"^\s*(?:hi|hello|hey|yo|howdy|hiya|good\s+(?:morning|afternoon|evening)|"
    r"hello\s+(?:there|pilot|datawrap)|hey\s+(?:there|pilot)|"
    r"what'?s\s+up|how\s+are\s+you)\s*[.!?]*\s*$",
    re.I,
)

_THANKS = re.compile(
    r"^\s*(?:thanks|thank\s+you|thx|ty|cheers|great|perfect|awesome|got\s+it)\s*[.!?]*\s*$",
    re.I,
)

_BRIEFING = re.compile(
    r"\b(?:"
    r"what(?:'s| is)\s+going\s+on|"
    r"what(?:'s| is)\s+(?:the\s+)?(?:status|state|health)"
    r"(?:\s+of\s+(?:my\s+|the\s+)?(?:workspace|platform|environment|everything))?"
    r"(?!\s+of\b)|"
    r"(?:give\s+me|write|draft)\s+(?:a\s+)?(?:briefing|status\s+report|sitrep|summary)|"
    r"(?:workspace|ops|operational)\s+(?:briefing|summary|status|overview)|"
    r"summarize\s+(?:my\s+)?(?:workspace|pipelines?|jobs?|connectors?|everything)|"
    r"how\s+(?:are|is)\s+(?:we|my\s+(?:workspace|data|platform)|everything)\s+doing|"
    r"morning\s+briefing|stand-?up\s+(?:update|summary)|"
    r"catch\s+me\s+up|bring\s+me\s+up\s+to\s+speed|"
    r"what\s+needs\s+(?:my\s+)?(?:attention|review)"
    r")\b|"
    r"^\s*tell\s+me\s+everything(?:\s+about\s+(?:my\s+)?(?:workspace|platform))?\s*[.!?]*$|"
    r"^\s*any\s+(?:failures?|problems?|issues?)\s*(?:today|right\s+now)?\s*[.!?]*$",
    re.I,
)

_SUMMARIZE_LAST = re.compile(
    r"^\s*(?:"
    r"summarize\s+(?:that|this|it|what\s+you\s+(?:just\s+)?said)|"
    r"(?:tl;?dr|tldr)|"
    r"in\s+(?:a\s+)?(?:sentence|nutshell|few\s+words)|"
    r"short\s+version|"
    r"recap(?:\s+that)?"
    r")\s*[.!?]*\s*$",
    re.I,
)

_EXPLAIN_SIMPLER = re.compile(
    r"\b(?:"
    r"explain\s+(?:that|this|it)\s+(?:more\s+)?(?:simply|simpler|in\s+plain\s+(?:english|language))|"
    r"eli5|like\s+i(?:'m|\s+am)\s+(?:five|new)|"
    r"what\s+does\s+that\s+mean|"
    r"i\s+don'?t\s+understand|"
    r"say\s+that\s+again(?:\s+slower)?"
    r")\b",
    re.I,
)

_NEXT_ACTION = re.compile(
    r"\b(?:"
    r"what\s+should\s+i\s+do\s+(?:next|now)|"
    r"what(?:'s| is)\s+next|"
    r"next\s+(?:step|action|move)|"
    r"so\s+what(?:'s| is)\s+the\s+(?:fix|plan)|"
    r"recommend\s+(?:a\s+)?(?:next\s+)?(?:step|action)"
    r")\b",
    re.I,
)

# Product / workspace nouns — if present, this is not "general internet chat".
_CALENDAR = re.compile(
    r"^\s*(?:"
    r"(?:what(?:'s| is)\s+)?(?:the\s+)?(?:today'?s|todays)\s+date"
    r"|what\s+(?:day|date)\s+is\s+(?:it|today)"
    r"|(?:what\s+is\s+)?date\s+today"
    r"|today'?s\s+date"
    r"|what\s+is\s+today"
    r"|tell\s+me\s+(?:the\s+)?(?:today'?s\s+)?(?:date|day)"
    r"|what\s+day\s+is\s+it"
    r")\s*[.!?]*\s*$",
    re.I,
)

_SCHEDULE_HEALTH = re.compile(
    r"\b(?:"
    r"(?:why\s+)?(?:are|is|aren'?t|isn'?t|are\s+not|is\s+not)\s+"
    r"(?:my\s+|the\s+)?"
    r"(?:schedules?|pipelines?)\s*"
    r"(?:working|running|ok|okay|fine|broken|failing|down|parked|stuck|"
    r"not\s+working|not\s+running)?"
    r"|(?:why\s+)?(?:schedules?|pipelines?)\s+(?:are|is|aren'?t|isn'?t)\s+"
    r"(?:not\s+)?(?:working|running|ok|okay|fine|broken|failing|down|parked|stuck)"
    r"|(?:schedules?|pipelines?)\s+(?:not\s+working|not\s+running|broken|failing|stuck|down)"
    r"|why\s+(?:aren'?t|are\s+not|isn'?t|is\s+not|won'?t)\s+(?:my\s+|the\s+)?"
    r"(?:schedules?|pipelines?)"
    r")\b",
    re.I,
)

_CREATE_CONNECTION_CAPABILITY = re.compile(
    r"^\s*(?:can|could|will)\s+you\s+"
    r"(?:create|add|make|set\s*up|setup|save)\s+"
    r"(?:a\s+|an\s+|the\s+|new\s+)?"
    r"(?:connection|connector)\s*[.!?]*\s*$",
    re.I,
)

_ROUTE_PLAN_CAPABILITY = re.compile(
    r"^\s*plan\s+source\s*[→\->]{1,3}\s*destination\s+routes?"
    r"(?:\s+and\s+sync\s+modes?)?\s*[.!?]*\s*$",
    re.I,
)

_HOW_TO_OR_DELETE_SCHEDULE = re.compile(
    r"\b(?:how\s+(?:do|can|to)|what\s+happens|delete|drop|export|yaml|gitops|"
    r"cdc\s+schedule)\b",
    re.I,
)


def is_calendar_question(message: str) -> bool:
    """Clock/calendar — not a DATE-column or transform question."""
    return bool(_CALENDAR.match((message or "").strip()))


def is_schedule_health_question(message: str) -> bool:
    """Live pipeline health — not the CDC-delete or GitOps procedure."""
    text = (message or "").strip()
    if not text or _HOW_TO_OR_DELETE_SCHEDULE.search(text):
        return False
    return bool(_SCHEDULE_HEALTH.search(text))


def is_create_connection_capability_ask(message: str) -> bool:
    """Bare 'can you create a connection' — no host, so do not demand credentials."""
    return bool(_CREATE_CONNECTION_CAPABILITY.match((message or "").strip()))


def is_route_plan_capability_paste(message: str) -> bool:
    """Capability-list line pasted as a turn, not a named source→dest."""
    return bool(_ROUTE_PLAN_CAPABILITY.match((message or "").strip()))


_WORKSPACE_MARKERS = re.compile(
    r"\b(?:"
    r"connectors?|pipelines?|schedules?|jobs?|transfers?|validate|preflight|quarantine|"
    r"mapping|schema|tables?|warehouse|snowflake|postgres|mysql|cdc|contracts?|"
    r"workspace|datawrap|pilot|sync\s+mode|incremental|reconcile|"
    r"export|download|delete|drop|remove"
    r")\b",
    re.I,
)


def turn_text(msg: dict | None) -> str:
    """One owner for a chat turn's spoken text.

    The Pilot page stores display rows as ``text`` and API history as
    ``content``. Clients and tests mix both. Every reader must use this
    helper — ``content``-only lookups silently drop a turn and break
    coreference / recap.
    """
    if not isinstance(msg, dict):
        return ""
    return str(msg.get("content") or msg.get("text") or "").strip()


def classify_dialogue_act(message: str, *, history: list[dict] | None = None) -> DialogueAct:
    text = (message or "").strip()
    if not text:
        return "greeting"
    if _GREETING.match(text):
        return "greeting"
    if _THANKS.match(text):
        return "thanks"
    if _SUMMARIZE_LAST.match(text) and history:
        return "summarize_last"
    if _EXPLAIN_SIMPLER.search(text) and history:
        return "explain_simpler"
    if _NEXT_ACTION.search(text):
        return "next_action"
    if _BRIEFING.search(text):
        return "briefing"
    # "tell me everything about airports" is a dataset/object ask, not a sitrep
    # and not general-web chat.
    if re.search(r"tell\s+me\s+everything\s+about\b", text, re.I):
        return "workspace"
    if _WORKSPACE_MARKERS.search(text):
        return "workspace"
    # Short follow-ups stay in-workspace only when they are elliptical edits
    # ("and by region?", "only paid"). Off-topic shorts with history stay
    # general — otherwise "capital of France" after a job list skipped refusal.
    if history and len(text.split()) <= 8:
        from .followup import looks_like_elliptical_edit

        if looks_like_elliptical_edit(text):
            return "workspace"
    return "general"


def last_assistant_text(history: list[dict] | None) -> str:
    for item in reversed(history or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("role") or "").lower() != "assistant":
            continue
        text = turn_text(item)
        if text:
            return text
    return ""


def last_user_text(history: list[dict] | None) -> str:
    for item in reversed(history or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("role") or "").lower() != "user":
            continue
        text = turn_text(item)
        if text:
            return text
    return ""
