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
    "recall_ask",
    "repair_unclear",
    "thanks",
    "trouble_vague",
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
    r"what\s+needs\s+(?:my\s+)?(?:attention|review)|"
    # Parked pipelines and unsigned contracts are the two things the briefing
    # already reports as waiting on the operator. Asked without a named object,
    # "is anything parked" was refused as undocumented.
    r"(?:is|are)\s+(?:there\s+)?(?:anything|any(?:thing)?\s+\w+|something)\s+"
    r"(?:parked|blocked|waiting|pending|stuck|due)|"
    r"what(?:'s| is)?\s+(?:parked|blocked|pending|waiting\s+on\s+(?:me|approval))|"
    r"anything\s+(?:waiting|pending)\s+on\s+(?:me|approval)|"
    # The operator's *own* workspace, not the hosted-tenant article. "what's my
    # workspace look like" and "how is my workspace" both answered with "Datawrap
    # is delivered as a hosted enterprise workspace at your tenant URL", which is
    # true of every tenant and says nothing about theirs.
    r"(?:what(?:'s| is)|how(?:'s| is)|how\s+are)\s+"
    r"(?:my|our|the)\s+workspace\b|"
    r"what(?:'s| is)\s+(?:my|our)\s+(?:status|state|health)"
    r")\b|"
    # Bare "status" is the one-word form of the same ask.
    r"^\s*(?:status|sitrep|overview)\s*[.!?]*$|"
    r"^\s*tell\s+me\s+everything(?:\s+about\s+(?:my\s+)?(?:workspace|platform))?\s*[.!?]*$|"
    r"^\s*any\s+(?:failures?|problems?|issues?)\s*(?:today|right\s+now)?\s*[.!?]*$",
    re.I,
)

_SUMMARIZE_LAST = re.compile(
    r"^\s*(?:"
    r"summarize\s+(?:that|this|it|what\s+you\s+(?:just\s+)?said)|"
    r"(?:tl;?dr|tldr)|"
    r"in\s+(?:a\s+|one\s+|1\s+)?(?:sentence|nutshell|few\s+words)|"
    r"short\s+version|"
    # A bare "shorter" is the whole turn: it asks for the same answer, smaller.
    # It reached no act at all and was refused as undocumented.
    r"(?:much\s+)?shorter|too\s+long|less\s+detail|"
    r"(?:keep\s+it|make\s+it|be)\s+(?:brief|short|shorter|concise)|"
    r"condense(?:\s+(?:that|it))?|"
    r"recap(?:\s+that)?"
    r")\s*[.!?]*\s*$",
    re.I,
)

# A length instruction wrapped around a real question: "in one sentence, what is
# append mode" was answered with three paragraphs, which reads as though the
# instruction was not heard. The question still routes normally; only the
# composed answer is trimmed.
_BREVITY_MODIFIER = re.compile(
    r"\bin\s+(?:just\s+)?(?:one|1|a\s+single)\s+(?:sentence|line)\b"
    r"|\bone[- ]sentence\b|\bone\s+line\b"
    r"|\b(?:keep\s+it|make\s+it|be)\s+(?:brief|short|concise)\b"
    r"|\bbriefly\b|\bshort\s+answer\b|\bin\s+short\b"
    r"|\bjust\s+the\s+(?:gist|headline|summary)\b",
    re.I,
)


def wants_brief_answer(message: str) -> bool:
    """Whether the turn asked for its answer short, on top of asking something."""
    return bool(_BREVITY_MODIFIER.search((message or "").strip()))

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

# Clock vs DATE-type: warehouse nouns mean a column/cast question.
_DATE_TYPE_ASK = re.compile(
    r"\b(?:"
    r"column|columns|type|types|cast|coerce|coercion|transform|transforms|"
    r"format|formats|field|schema|mapping|logical\s+type|"
    r"postgres|postgresql|mysql|sqlite|snowflake|bigquery|mongodb|"
    r"destination|source\s+type"
    r")\b",
    re.I,
)

# Open clock English — word order varies ("the date today" vs "todays date").
_CLOCK_ASK = re.compile(
    r"\b(?:"
    r"(?:what(?:'s|s| is)|tell\s+me|give\s+me|whats)\s+(?:the\s+)?(?:current\s+)?"
    r"(?:date|day)(?:\s+(?:is\s+it|is\s+today|today|now))?"
    r"|(?:today'?s|todays|current)\s+date"
    r"|date\s+today"
    r"|what\s+(?:day|date)\s+is\s+(?:it|today)"
    r"|what\s+is\s+today"
    r")\b",
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

# "can you setup schedule" is a capability ask, not the YAML-export procedure.
_SCHEDULE_CAPABILITY = re.compile(
    r"^\s*(?:can|could|will|are\s+you\s+able\s+to)\s+you?\s*"
    r"(?:set\s*up|setup|create|make|add|configure|schedule)\s+"
    r"(?:a\s+|an\s+|the\s+|new\s+|my\s+)?"
    r"(?:schedule|pipeline|cadence|cron|nightly\s+run)s?\s*[.!?]*\s*$",
    re.I,
)

# A vague trouble report with nothing named. "this is broken" was answered "That
# is outside what the Datawrap documentation covers", which is technically true
# and useless: the operator has a problem and the reply tells them nothing about
# how to hand it over. Anything that names an object routes normally instead.
_TROUBLE_SUBJECT = (
    r"(?:it|this|that|everything|nothing|the\s+whole\s+thing|your\s+\w+|"
    r"you|the\s+app|the\s+ui|the\s+page)"
)
_TROUBLE_PREFIX = (
    r"^\s*(?:why\s+(?:is|are|does|do|did)\s+)?"
    rf"(?:{_TROUBLE_SUBJECT}(?:'s|'re|s)?\s+)?"
)
_VAGUE_TROUBLE_PATTERNS = (
    # A negative state says something is wrong on its own: "this is broken",
    # "it keeps failing", "why are you so useless".
    re.compile(
        _TROUBLE_PREFIX
        + r"(?:(?:is|are|was|were|has|have|keeps?|so|just|all)\s+)*"
        r"(?:broken|broke|failing|failed|stuck|hanging|hung|frozen|useless|"
        r"garbage|rubbish|terrible|awful|a\s+mess|crashing|crashed|erroring|"
        r"down)\b[\s.!?]*$",
        re.I,
    ),
    # "working" only reports trouble when it is negated.
    re.compile(
        _TROUBLE_PREFIX
        + r"(?:(?:is|are|was|were)\s+)?"
        r"(?:isn'?t|aren'?t|does\s*n[o']?t|do\s*n[o']?t|did\s*n[o']?t|won'?t|"
        r"wont|never|not|no\s+longer)\s+work(?:s|ing|ed)?\b[\s.!?]*$",
        re.I,
    ),
    # A negative subject carries the negation itself: "nothing works".
    re.compile(r"^\s*nothing\s+(?:is\s+)?work(?:s|ing|ed)?\b[\s.!?]*$", re.I),
    # Inverted: "why doesn't it work".
    re.compile(
        r"^\s*why\s+(?:does\s*n[o']?t|do\s*n[o']?t|isn'?t|won'?t|wont)\s+"
        rf"{_TROUBLE_SUBJECT}\s+work(?:s|ing|ed)?\b[\s.!?]*$",
        re.I,
    ),
)


def is_vague_trouble_report(message: str) -> bool:
    """A complaint with no job, connector, table or error named."""
    text = (message or "").strip()
    if not text or _WORKSPACE_MARKERS.search(text):
        return False
    return any(p.match(text) for p in _VAGUE_TROUBLE_PATTERNS)


# The other complaint an operator brings: the numbers are wrong, or rows are
# missing. It mentions workspace nouns without naming an object, so the vague
# check above rules it out, and retrieval then answered a long narrative with a
# connector list. What it needs is the quarantine and reconcile evidence.
_DISCREPANCY = re.compile(
    r"\b(?:numbers?|counts?|totals?|figures?|amounts?|values?|data|rows?|records?)\b"
    r"[^.?!]{0,40}?"
    r"\b(?:look|looks|looked|seem|seems|seemed|are|is|were|was|feel|feels)\b"
    r"\s*(?:a\s+bit\s+|kind\s+of\s+|really\s+|very\s+|all\s+)?"
    r"\b(?:off|wrong|weird|odd|bad|different|incorrect|missing|short)\b"
    r"|\b(?:rows?|records?|data)\s+(?:are\s+|is\s+|went\s+|got\s+|have\s+)?"
    r"(?:missing|lost|dropped|disappeared|gone|vanished)\b"
    r"|\b(?:missing|lost|dropped|losing)\s+"
    r"(?:some\s+|a\s+few\s+|a\s+couple\s+of\s+)?(?:rows?|records?|data)\b"
    r"|\bdoesn'?t\s+match\b|\bdo\s*n[o']?t\s+match\b|\bout\s+of\s+sync\b"
    r"|\bnot\s+adding\s+up\b|\bdon'?t\s+add\s+up\b",
    re.I,
)

# The same words inside a how-to, a consequence or a count frame are a question
# the documentation and the job tools already answer well.
_DISCREPANCY_IS_A_QUESTION = re.compile(
    r"\bhow\s+(?:do|does|can|to|should|would|many|much)\b"
    r"|\bwhat\s+happens\b|\bwhat\s+if\b|\bwhat\s+does\b"
    r"|\bwhy\s+(?:does|do|is|are)\s+(?:a|an|the)\b"
    r"|\bwhere\s+do\b|\bmeans?\b|\bdefinition\b",
    re.I,
)


def is_data_discrepancy_report(message: str) -> bool:
    """A complaint that rows or numbers are wrong, with no run or table named."""
    text = (message or "").strip()
    if not text or _DISCREPANCY_IS_A_QUESTION.search(text):
        return False
    if not _DISCREPANCY.search(text):
        return False
    # A pasted run id is the handle the intake would ask for, so the job tools
    # own that turn instead.
    if re.search(r"\b(?:job|pf|run|pipeline)_[A-Za-z0-9]{2,}\b", text, re.I):
        return False
    try:
        from ..rag.evidence import names_identifier

        if names_identifier(text):
            return False
    except Exception:
        pass
    return True


_RECALL_ASK = re.compile(
    r"\bwhat\s+did\s+i\s+(?:just\s+)?(?:ask|say|type|write)\b"
    r"|\bwhat\s+was\s+my\s+(?:last|previous|first)\s+(?:question|ask|message)\b"
    r"|\brepeat\s+my\s+(?:question|last\s+question|ask)\b"
    r"|\bwhat\s+(?:question\s+)?did\s+i\s+ask\s+you\b",
    re.I,
)

_ROUTE_PLAN_CAPABILITY = re.compile(
    r"^\s*plan\s+source\s*[→\->]{1,3}\s*destination\s+routes?"
    r"(?:\s+and\s+sync\s+modes?)?\s*[.!?]*\s*$",
    re.I,
)

_HOW_TO_OR_DELETE_SCHEDULE = re.compile(
    r"\b(?:how\s+(?:do|can|to)|what\s+happens|delete|drop|export|yaml|gitops|"
    r"cdc\s+schedule"
    # Passive voice asks for the procedure: "how are pipelines scheduled" wants
    # the create-a-pipeline steps, while "how are my pipelines running" is still
    # health. The participle is what separates them.
    r"|how\s+(?:is|are|was|were)\s+(?:a\s+|an\s+|the\s+|my\s+|our\s+)?"
    r"(?:schedules?|pipelines?)\s+"
    r"(?:scheduled|created|defined|configured|set\s*up|setup|exported|"
    r"triggered|built|written|versioned)"
    r")\b",
    re.I,
)


def is_calendar_question(message: str) -> bool:
    """Host clock — not a DATE-column, cast, or transform question."""
    text = (message or "").strip()
    if not text or _DATE_TYPE_ASK.search(text):
        return False
    return bool(_CLOCK_ASK.search(text))


def is_schedule_health_question(message: str) -> bool:
    """Live pipeline health — not the CDC-delete or GitOps procedure."""
    text = (message or "").strip()
    if not text or _HOW_TO_OR_DELETE_SCHEDULE.search(text):
        return False
    return bool(_SCHEDULE_HEALTH.search(text))


def is_create_connection_capability_ask(message: str) -> bool:
    """Bare 'can you create a connection' — no host, so do not demand credentials."""
    return bool(_CREATE_CONNECTION_CAPABILITY.match((message or "").strip()))


def is_schedule_setup_capability_ask(message: str) -> bool:
    """Bare 'can you setup schedule' — answer the capability, not GitOps export."""
    return bool(_SCHEDULE_CAPABILITY.match((message or "").strip()))


def is_route_plan_capability_paste(message: str) -> bool:
    """Capability-list line pasted as a turn, not a named source→dest."""
    return bool(_ROUTE_PLAN_CAPABILITY.match((message or "").strip()))


# An operator opening a transfer without naming endpoints yet. "plan a
# transfer", "i want to copy a table" and "can you migrate my database"
# retrieved Azure Test Plans, Iceberg merge-on-read and an Azure Migrate denial
# respectively — three unrelated capability rows instead of the one thing that
# moves the task forward, which is naming two saved connectors and a table.
# Deliberately excludes "how do i move data", which the documentation answers
# well with the real Transfer Studio steps.
_TRANSFER_CAPABILITY = re.compile(
    r"^\s*(?:hey\s+|hi\s+|ok\s+|so\s+|please\s+|pls\s+)*"
    r"(?:"
    r"(?:can|could|will|would)\s+(?:you|u)\s+(?:please\s+)?"
    r"(?:help\s+me\s+)?(?:copy|move|migrate|transfer|sync|replicate|load)\b"
    r"|(?:i|we)\s+(?:want|need|would\s+like|wanna|gotta|have)\s+to\s+"
    r"(?:copy|move|migrate|transfer|sync|replicate|load)\b"
    r"|help\s+me\s+(?:copy|move|migrate|transfer|sync|replicate|load)\b"
    r"|(?:plan|start|stage|set\s*up|setup|create|do|run|begin)\s+"
    r"(?:a\s+|an\s+|the\s+|my\s+|new\s+)?"
    r"(?:transfer|migration|data\s+move|data\s+transfer|copy|load|sync)\b"
    r"|(?:how\s+fast|throughput)\b[^.?!]*\b(?:copy|move|migrate|transfer|sync)\b"
    r")",
    re.I,
)

# Naming an endpoint means the operator is past the opening ask, so the real
# route planner should run instead of the sketch.
_NAMES_ROUTE_ENDPOINTS = re.compile(
    r"\bfrom\s+\S+\s+(?:to|into)\s+\S|\b(?:to|into)\s+\S+\s+from\s+\S|→|->",
    re.I,
)


def is_transfer_capability_ask(message: str) -> bool:
    """Opening a transfer with no endpoints named yet — answer with the next step."""
    text = (message or "").strip()
    if not text or _NAMES_ROUTE_ENDPOINTS.search(text):
        return False
    return bool(_TRANSFER_CAPABILITY.match(text))


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
    # "what did i just ask you" is a question about the transcript, and the only
    # place the answer exists is the transcript. Retrieval answered it with the
    # three closest Help headings and a refusal.
    if history and _RECALL_ASK.search(text):
        return "recall_ask"
    # A bare "that's not what i meant" carries no correction to re-plan, and
    # replaying the answer it objects to is the one reply guaranteed to be wrong.
    if history:
        from .followup import repair_correction

        if repair_correction(text) == "":
            return "repair_unclear"
    if is_vague_trouble_report(text) or is_data_discrepancy_report(text):
        return "trouble_vague"
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
