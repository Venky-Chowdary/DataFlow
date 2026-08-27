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
    r"what(?:'s| is)\s+(?:the\s+)?(?:status|state|health)|"
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
_WORKSPACE_MARKERS = re.compile(
    r"\b(?:"
    r"connectors?|pipelines?|schedules?|jobs?|transfers?|validate|preflight|quarantine|"
    r"mapping|schema|tables?|warehouse|snowflake|postgres|mysql|cdc|contracts?|"
    r"workspace|datawrap|pilot|sync\s+mode|incremental|reconcile|"
    r"export|download|delete|drop|remove"
    r")\b",
    re.I,
)


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
    # Short follow-ups after a live turn stay in workspace (followup.py owns slots).
    if history and len(text.split()) <= 8:
        return "workspace"
    return "general"


def last_assistant_text(history: list[dict] | None) -> str:
    for item in reversed(history or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("role") or "").lower() != "assistant":
            continue
        text = str(item.get("content") or item.get("text") or "").strip()
        if text:
            return text
    return ""


def last_user_text(history: list[dict] | None) -> str:
    for item in reversed(history or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("role") or "").lower() != "user":
            continue
        text = str(item.get("content") or item.get("text") or "").strip()
        if text:
            return text
    return ""
