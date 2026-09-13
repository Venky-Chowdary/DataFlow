"""Context resolution for Datawrap Pilot follow-up turns.

Separate from raw-history replay on purpose. The 2026 multi-turn text-to-SQL
memory study found that resolving referenced context into an *authoritative
structured block* beats handing a model the transcript and hoping: the resolved
values are exact, auditable, and cheap. We do the same thing deterministically —
a follow-up is parsed as an **edit** of the previous query state
(``PilotFocus``), which is the CoE-SQL "query evolution" framing.

Four turn shapes are handled, matching the CoSQL dialogue acts:

1. ``answer to a clarification`` — "Local Postgres" after "which connector?"
2. ``edit`` — "and by region?", "average instead", "top 3", "same for products"
3. ``self-contained but under-specified`` — "average price in products" with the
   connector implied by the previous turn (slot inheritance)
4. ``no relation`` — return nothing and let normal routing run

Every resolved edit still goes through ``aggregate_data``, so the live schema
remains the authority on table and column names: inheritance never invents a
column, it only reuses one the user already confirmed by asking about it.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

from .aggregate_tools import (
    _METRICS,
    _PLATFORM_NOUNS,
    _TEMPORAL_GRAINS,
    AggregationRequest,
    parse_aggregation_request,
)
from .working_memory import PendingSlot, PilotFocus

# Pronouns / definite descriptions that point at the previous subject.
_COREFERENCE_RE = re.compile(
    r"\b(?:it|that|this|these|those|them|there|"
    r"same|the same|that one|the other one|the other ones|"
    r"the table|that table|this table|"
    r"the collection|that collection|the result|that result)\b",
    re.IGNORECASE,
)

# Metric words, longest first so "count distinct" beats "count".
_METRIC_EDIT_PHRASES: tuple[tuple[str, str], ...] = (
    (r"(?:count\s+)?(?:distinct|unique)(?:\s+count)?", "count_distinct"),
    (r"(?:average|avg|mean)", "avg"),
    (r"(?:sum|total)", "sum"),
    (r"(?:minimum|min|lowest|smallest|earliest)", "min"),
    (r"(?:maximum|max|highest|largest|biggest|latest)", "max"),
    (r"(?:count|how\s+many|number)", "count"),
)

# "instead", "rather than that" — an explicit replacement of one slot.
_INSTEAD_RE = re.compile(r"\b(?:instead|rather|not|actually)\b", re.IGNORECASE)

_ASC_RE = re.compile(
    r"\b(?:ascending|asc|lowest\s+first|smallest\s+first|bottom|least|fewest)\b", re.I
)
_DESC_RE = re.compile(
    r"\b(?:descending|desc|highest\s+first|largest\s+first|top|most|biggest)\b", re.I
)

# A follow-up is short and leans on prior context. Long sentences are new asks.
_MAX_FOLLOWUP_WORDS = 14

_AFFIRMATIVE = frozenset({
    "yes", "yep", "yeah", "y", "sure", "ok", "okay", "please", "do it",
    "go ahead", "correct", "right", "that one", "the first", "first one",
})


# Words that open an instruction rather than name a value, so "just tell me the
# number" and "only show the total" are read as asks, not as filters.
#
# Rank words belong here for the same reason: "only the top 2" is a limit, and
# read as a filter it became ``status = 'top 2'`` — a predicate on a column the
# operator never named, against a value that is not in any row.
_NOT_A_FILTER_VALUE = re.compile(
    r"^(?:tell|show|give|list|get|say|read|explain|describe|do|make|run|open|"
    r"answer|repeat|me|us|it|that|this|one|number|count|total|sum|average|"
    r"top|bottom|first|last|latest|newest|oldest|highest|lowest|biggest|"
    r"smallest|best|worst|most|least|few|rest|others?|ones?)\b",
    re.I,
)

# A turn that asks for the same query again, with nothing changed.
_REPEAT_RE = re.compile(
    r"^(?:again|same\s+again|one\s+more\s+time|once\s+more|"
    r"(?:do|run)\s+(?:that|it|this)\s+again|re-?run(?:\s+(?:that|it|this))?|"
    r"repeat(?:\s+(?:that|it|this))?|refresh(?:\s+(?:that|it|this))?|"
    r"check\s+(?:that|it)\s+again)$",
    re.I,
)


def _extract_edit_where(message: str, focus: PilotFocus | None) -> str:
    """Parse \"only paid\" / \"where status = paid\" into a filter clause."""
    text = _clean(message)
    if not text:
        return ""
    where_m = re.search(r"\b(?:where|filter(?:\s+where)?)\s+(.+)$", text, re.I)
    if where_m:
        return where_m.group(1).strip()
    only_m = re.match(
        r"^(?:only|just)\s+(.+?)(?:\s+ones?|\s+rows?|\s+records?)?$",
        text,
        re.I,
    )
    if not only_m:
        return ""
    val = only_m.group(1).strip().strip("\"'")
    val = re.sub(r"^(?:the|a|an)\s+", "", val, flags=re.I).strip()
    if not val or val.lower() in _PLATFORM_NOUNS:
        return ""
    # A filter value is a literal, not a sentence. "just tell me the number"
    # became ``status = 'tell me the number'`` and, because the clause was then
    # remembered, failed every following turn on a column the table lacks.
    if len(val.split()) > 2 or _NOT_A_FILTER_VALUE.match(val):
        return ""
    preferred = ("status", "state", "type", "region", "category", "tier", "channel")
    cols = [c.lower() for c in ((focus.columns if focus else None) or [])]
    col = "status"
    if cols:
        col = next((c for c in preferred if c in cols), cols[0])
    if re.fullmatch(r"-?\d+(?:\.\d+)?", val):
        return f"{col} = {val}"
    return f"{col} = '{val}'"


def _clean(text: str) -> str:
    return (text or "").strip().strip("?!.,;:").strip()


def _words(text: str) -> list[str]:
    return [w for w in re.split(r"\s+", _clean(text)) if w]


def last_assistant_content(history: list[dict] | None) -> str:
    from .dialogue_acts import turn_text

    for m in reversed(history or []):
        if not isinstance(m, dict):
            continue
        if str(m.get("role") or "").lower() != "assistant":
            continue
        text = turn_text(m)
        if text:
            return text
    return ""


_CDC_PRIOR = re.compile(
    r"\b(?:cdc|wal_level|wal|binlog|replication\s+slot|pgoutput|"
    r"pre-image|replica\s+identity|change[\s-]?stream)\b",
    re.I,
)
_ENGINE_FOLLOWUP = re.compile(
    r"^\s*(?:"
    r"(?:what|how)\s+about|"
    r"and(?:\s+(?:for|what\s+about))?|"
    r"same\s+(?:for|thing(?:\s+but)?\s+for|question(?:\s+but)?\s+for)|"
    r"how\s+about"
    r")\s+"
    r"(?:for\s+)?"
    r"(?P<eng>mysql|maria(?:db)?|postgres(?:ql)?|pg|mongo(?:db)?|"
    r"sql\s*server|mssql|oracle)\s*[?.!]?\s*$",
    re.I,
)
_ENGINE_CANON = {
    "pg": "postgres",
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "mariadb": "mysql",
    "maria": "mysql",
    "mongo": "mongo",
    "mongodb": "mongo",
    "sql server": "sql server",
    "sqlserver": "sql server",
    "mssql": "sql server",
    "oracle": "oracle",
}
_ENGINE_CDC_QUESTION = {
    "postgres": "do I need wal_level logical for postgres CDC",
    "mysql": "do I need binlog_format ROW for mysql CDC",
    "mongo": "does Mongo CDC need change-stream pre-images",
    "sql server": "do you support SQL Server CDC",
    "oracle": "do you support Oracle CDC",
}

# ChatGPT keeps the CDC topic and swaps the *aspect*. "what about deletes?"
# after a wal_level answer is the delete sentence, not a new job list.
_SUBJECT_FOLLOWUP = re.compile(
    r"^\s*(?:"
    r"(?:what|how)\s+about|"
    r"and(?:\s+(?:for|what\s+about))?|"
    r"same\s+(?:for|thing(?:\s+but)?\s+for|question(?:\s+but)?\s+for)|"
    r"how\s+about"
    r")\s+"
    r"(?:for\s+)?"
    r"(?P<sub>deletes?|lag|toast|replica\s+identity|pgoutput|plugin|"
    r"replication\s+slots?|slots?|pre-?images?|gtid|publications?|"
    r"wal_level|binlog(?:_format)?)\s*[?.!]?\s*$",
    re.I,
)
_SUBJECT_CDC_QUESTION = {
    "delete": "what happens to a delete in CDC",
    "deletes": "what happens to a delete in CDC",
    "lag": "how do I see CDC lag",
    "toast": "do unchanged TOAST columns get dropped on a CDC update",
    "replica identity": "do I need REPLICA IDENTITY FULL for postgres CDC",
    "pgoutput": "what plugin does postgres CDC use",
    "plugin": "what plugin does postgres CDC use",
    "replication slot": "what is a replication slot",
    "replication slots": "what is a replication slot",
    "slot": "what is a replication slot",
    "slots": "what is a replication slot",
    "pre-image": "does Mongo CDC need change-stream pre-images",
    "pre-images": "does Mongo CDC need change-stream pre-images",
    "preimage": "does Mongo CDC need change-stream pre-images",
    "preimages": "does Mongo CDC need change-stream pre-images",
    "gtid": "what is GTID on a MySQL CDC route",
    "publication": "what is a publication",
    "publications": "what is a publication",
    "wal_level": "do I need wal_level logical for postgres CDC",
    "binlog": "do I need binlog_format ROW for mysql CDC",
    "binlog_format": "do I need binlog_format ROW for mysql CDC",
}
_NEED_THAT = re.compile(
    r"^\s*(?:do\s+i\s+need\s+that|is\s+that\s+(?:required|needed|necessary))"
    r"(?:\s+for\s+\w+)?\s*[?.!]?\s*$",
    re.I,
)


def _rewrite_followup_text(message: str) -> str:
    try:
        from ..rag.query_analysis import rewrite_operator_question
    except Exception:
        return (message or "").strip()
    return rewrite_operator_question(message)


def resolve_knowledge_engine_followup(
    message: str,
    history: list[dict] | None,
) -> str | None:
    """'what about mysql?' after a CDC answer is the MySQL prerequisite.

    ChatGPT keeps the prior subject and swaps the engine *or* the aspect
    (deletes, lag, toast). We do the same only when the last answer was
    already about capture — otherwise this stays a table/job follow-up
    and we do not invent a CDC question.
    """
    text = _rewrite_followup_text(message)
    prior = last_assistant_content(history)
    if not prior or not _CDC_PRIOR.search(prior):
        return None
    match = _ENGINE_FOLLOWUP.match(text)
    if match:
        raw = re.sub(r"\s+", " ", (match.group("eng") or "").strip().lower())
        engine = _ENGINE_CANON.get(raw)
        if engine:
            return _ENGINE_CDC_QUESTION.get(engine)
    sub = _SUBJECT_FOLLOWUP.match(text)
    if sub:
        key = re.sub(r"\s+", " ", (sub.group("sub") or "").strip().lower())
        return _SUBJECT_CDC_QUESTION.get(key)
    if _NEED_THAT.match(text):
        named = re.search(
            r"\b(mysql|maria(?:db)?|postgres(?:ql)?|pg|mongo(?:db)?|"
            r"sql\s*server|mssql|oracle)\b",
            text,
            re.I,
        )
        if named:
            raw = re.sub(r"\s+", " ", named.group(1).strip().lower())
            engine = _ENGINE_CANON.get(raw)
            if engine:
                return _ENGINE_CDC_QUESTION.get(engine)
        if re.search(r"wal_level", prior, re.I):
            return "do I need wal_level logical for postgres CDC"
        if re.search(r"binlog", prior, re.I):
            return "do I need binlog_format ROW for mysql CDC"
        if re.search(r"replica\s+identity", prior, re.I):
            return "do I need REPLICA IDENTITY FULL for postgres CDC"
        if re.search(r"pre-?image", prior, re.I):
            return "does Mongo CDC need change-stream pre-images"
    return None


# A repair turn: the operator says the last answer read their question wrong and
# supplies the correction. "no i meant the failed ones" was refused as
# undocumented, and "wait, that's not what i meant" replayed the same connector
# list it was objecting to — the two turns that most make an assistant look like
# it is not listening.
_REPAIR_OPENER = re.compile(
    r"^\s*(?:(?:no+|nope|nah|wait|sorry|actually|hmm+|oops|ugh)\b[\s,.!-]*)*"
    r"(?:that(?:'s| is|s)?\s+not\s+(?:what\s+i\s+(?:meant|asked)|it|right)"
    r"|not\s+(?:that|those|them|what\s+i\s+(?:meant|asked))"
    r"|i\s+(?:meant|mean)|i\s+said"
    r"|i\s+was\s+asking\s+(?:about|for)"
    r"|i\s+asked\s+(?:about|for))"
    r"[\s,:;.-]*",
    re.I,
)

# A bare objection with no correction attached. "that's not what i meant" full
# stop cannot be re-planned — the honest move is to ask which part was wrong.
_BARE_REPAIR = re.compile(r"^[\s,.!?-]*$")

# Filler the operator puts in front of the correction itself.
_CORRECTION_FILLER = re.compile(
    r"^(?:the\s+|a\s+|an\s+|about\s+|for\s+|just\s+|only\s+|like\s+)+", re.I
)


def repair_correction(message: str) -> str | None:
    """The correction in a repair turn: "" when bare, ``None`` when not a repair."""
    text = _clean(message)
    if not text:
        return None
    match = _REPAIR_OPENER.match(text)
    if not match:
        return None
    rest = text[match.end():].strip()
    if _BARE_REPAIR.match(rest):
        return ""
    return _CORRECTION_FILLER.sub("", rest).strip(" ,.!?;:-")


def resolve_repair(message: str, history: list[dict] | None) -> str | None:
    """Re-state the previous question with the operator's correction applied.

    The correction replaces a constraint on a subject the operator already named,
    so appending it to the question being corrected is what recovers the real
    ask: "how many connectors do i have" + "failed ones" is the failed-connector
    bucket, which the router already knows how to read.
    """
    correction = repair_correction(message)
    if not correction:
        return None
    prior = _last_substantive_user_text(history)
    if not prior:
        return None
    # A correction that names its own subject *replaces* the previous one:
    # "i meant pipelines" after a connector count is the pipeline count, and
    # appending it planned both lists and answered with two contradicting ones.
    old, new = _repair_subject(prior), _repair_subject(correction)
    if old and new and old.rstrip("s") != new.rstrip("s"):
        return re.sub(rf"\b{re.escape(old)}\b", new, prior, flags=re.I)
    return f"{prior} {correction}"


# Subjects a repair can swap out. Longest first, so a plural is preferred over
# the singular hiding inside it.
_REPAIR_SUBJECTS: tuple[str, ...] = tuple(
    sorted(
        (
            "collections", "collection", "columns", "column",
            "connections", "connection", "connectors", "connector",
            "contracts", "contract", "datasets", "dataset",
            "jobs", "job", "pipelines", "pipeline",
            "schedules", "schedule", "tables", "table",
            "transfers", "transfer",
        ),
        key=len,
        reverse=True,
    )
)


def _repair_subject(text: str) -> str:
    """The workspace noun this turn is about, or "" when it names none."""
    low = (text or "").lower()
    for noun in _REPAIR_SUBJECTS:
        if re.search(rf"\b{noun}\b", low):
            return noun
    return ""


def _last_substantive_user_text(history: list[dict] | None) -> str:
    """The most recent user turn that asked something, skipping repairs.

    A bare "that's not what i meant" is itself a user turn, so correcting it on
    the next turn built "that's not what i meant pipelines" and answered with the
    job list. The question being corrected is the last one that carried a subject.
    """
    from .dialogue_acts import turn_text

    for item in reversed(history or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("role") or "").lower() != "user":
            continue
        text = turn_text(item)
        if text and repair_correction(text) is None:
            return text
    return ""


def resolve_platform_coreference(
    message: str,
    history: list[dict] | None,
) -> list[tuple[str, dict[str, Any]]] | None:
    """those jobs / which of those failed → list_jobs from prior turn context."""
    text = _clean(message)
    if not text or not _COREFERENCE_RE.search(text):
        return None
    from .tools import connector_engine_filter

    # "how many of those are postgres" points squarely at the list the previous
    # turn printed. The documentation names PostgreSQL too, so retrieval won the
    # turn and answered about Azure and RDS drivers instead of the two saved rows
    # the operator was looking at. Only fires when a list really was printed.
    if re.search(r"\b(?:of|among)\s+(?:those|them|these)\b", text, re.I):
        engine, excluded = connector_engine_filter(text)
        if engine and offered_names(last_assistant_content(history)):
            return [
                ("list_connectors", {"engine": engine, "engine_excluded": excluded})
            ]
    # "how do I get the list of rows that failed" contains ``that`` as a
    # relative pronoun, not as a pointer at a previous turn, and ``failed`` as
    # part of its own subject. Read as a coreference it became "list the failed
    # jobs", which on an empty workspace answered a documented question with
    # "No transfer jobs yet".
    if has_own_question_frame(text):
        return None
    # "If I run the same CDC change twice is it safe" matches ``same`` / ``it``
    # and used to look like a jobs follow-up because ``runs?`` also matches the
    # verb *run*. A delivery-semantics question is never a pointer at the last
    # job list — even when the prior turn happened to mention transfers.
    from ..rag.query_analysis import is_cdc_delivery_question

    if is_cdc_delivery_question(text):
        return None
    prior = last_assistant_content(history).lower()
    low = text.lower()
    # ``that`` points at a previous turn in "which of those failed" and heads a
    # relative clause in "connectors that are not postgres". The second restricts
    # a noun this very turn named; read as a pointer it threw the restriction
    # away and re-listed every connector as though nothing had been asked.
    pointer_text = _RELATIVE_PRONOUN.sub(" ", low)
    # Noun *runs* / "the last run", never the verb in "if I run …".
    jobs_cue = bool(
        re.search(
            r"\b(?:jobs?|transfers?|failed|failures|runs)\b"
            r"|(?:last|this|that|my|the)\s+run\b",
            low,
        )
    ) or ("job" in prior or "transfer" in prior or "pipeline" in prior)
    # "is it safe" is a dummy pronoun, not a pointer at the last job list.
    dummy_it = bool(
        re.search(r"\bis\s+it\s+(?:safe|idempotent|lossy|dangerous|ok|okay|fine)\b", low)
    )
    job_pointer = bool(re.search(r"\b(?:those|these|them|that)\b", pointer_text)) or (
        bool(re.search(r"\bit\b", pointer_text)) and not dummy_it
    )
    if jobs_cue and job_pointer:
        return [("list_jobs", {"limit": 10})]
    connectors_cue = bool(re.search(r"\bconnectors?\b", low)) or "connector" in prior
    # "and the other one?" points at the item of the previous list the operator
    # has not asked about yet. It carries no pronoun, so it reached no tool and
    # was refused as undocumented; re-listing is at least the right subject.
    other_one = bool(re.search(r"\bthe\s+other\s+ones?\b", low))
    if connectors_cue and (
        other_one or re.search(r"\b(?:those|these|them|that|it)\b", pointer_text)
    ):
        # A restriction the turn carries is part of the answer, not noise to drop.
        engine, excluded = connector_engine_filter(text)
        args: dict[str, Any] = {}
        if engine:
            args = {"engine": engine, "engine_excluded": excluded}
        return [("list_connectors", args)]
    return None


def resolve_table_coreference_tools(
    message: str,
    focus: PilotFocus | None,
) -> list[tuple[str, dict[str, Any]]] | None:
    """sample/schema that table → tool with focus.table (not only aggregate edits)."""
    if not focus or not (focus.table or "").strip():
        return None
    if not _COREFERENCE_RE.search(message or ""):
        return None
    # "what schema change policies are there" carries the product's own subject
    # and an existential ``there``, not a pointer at the remembered table. This
    # layer runs ahead of ordinary routing, so without the guard it took the
    # turn away from a correct documentation plan and introspected the last
    # table the operator happened to count.
    if asks_its_own_question(message or ""):
        return None
    low = (message or "").lower()
    args: dict[str, Any] = {"table": focus.table}
    if focus.connector_name:
        args["connector_name"] = focus.connector_name
    if re.search(r"\b(?:sample|preview|show\s+(?:me\s+)?(?:some\s+)?rows|fetch)\b", low):
        return [("sample_connector_object", args)]
    if re.search(r"\b(?:schema|columns|describe|structure|introspect)\b", low):
        return [("introspect_connector_schema", args)]
    return None


#: Every list the pilot prints puts one item per bullet and names it in bold
#: (connectors) or backticks (tables). Reading the *bullets* rather than every
#: bold span is what keeps the heading — "You have **2 saved connector(s)**" —
#: out of the candidate set.
_BULLET_LINE = re.compile(r"^\s*[•\-\*]\s+(.+)$")
_BULLET_NAME = re.compile(r"^(?:\*\*([^*]{1,60})\*\*|`([^`]{1,60})`)")

#: Ordinal pointers into that list. ``job`` and ``run`` are deliberately absent:
#: "my last job" is a query the job tools answer, not a pointer at a printed list.
_ORDINALS: tuple[tuple[str, int], ...] = (
    (r"first|1st", 0),
    (r"second|2nd", 1),
    (r"third|3rd", 2),
    (r"fourth|4th", 3),
    (r"fifth|5th", 4),
    (r"last|final", -1),
)
_ORDINAL_NOUN = (
    r"(?:one|item|entry|connector|connection|table|collection|dataset|"
    r"schedule|pipeline)s?"
)


def offered_names(assistant_text: str) -> list[str]:
    """The list items the pilot last printed, in the order it printed them."""
    names: list[str] = []
    seen: set[str] = set()
    for raw_line in (assistant_text or "").splitlines():
        bullet = _BULLET_LINE.match(raw_line)
        if not bullet:
            continue
        named = _BULLET_NAME.match(bullet.group(1).strip())
        if not named:
            continue
        name = (named.group(1) or named.group(2) or "").strip()
        if not name or name.lower() in seen:
            continue
        seen.add(name.lower())
        names.append(name)
    return names[:12]


def resolve_ordinal_reference(
    message: str,
    history: list[dict] | None,
) -> str | None:
    """Rewrite "the first one" as the name the previous list gave it.

    "how many connectors do i have" then "how many tables on the first one" used
    to reach the connector lookup with the literal words *the first one*, which
    matched nothing and re-asked which connector was meant — while the list it
    was pointing at was still on screen. Returning a rewritten *message* keeps
    ordinary routing in charge of what the turn then means.
    """
    text = _clean(message)
    if not text:
        return None
    for pattern, index in _ORDINALS:
        hit = re.search(
            rf"\b(?:the\s+)?(?:{pattern})\s+{_ORDINAL_NOUN}\b",
            text,
            re.I,
        )
        if not hit:
            continue
        names = offered_names(last_assistant_content(history))
        if not names:
            return None
        if index >= len(names):
            return None
        return f"{text[: hit.start()]}{names[index]}{text[hit.end() :]}".strip()
    return None


def names_pending_candidate(message: str, pending: PendingSlot | None) -> str:
    """The offered candidate this turn names outright inside a fuller sentence.

    An open "which connector did you mean?" used to swallow the next turn whole,
    so "what tables are on Demo Orders" — which answers the question and asks a
    new one — was met with "I didn't match that reply" and the same list again.
    A turn that names a candidate is self-sufficient: clear the slot and route it
    normally rather than filling the old tool with a new question's subject.
    """
    if not pending:
        return ""
    text = _clean(message).lower()
    if not text:
        return ""
    for cand in pending.candidates or []:
        name = (cand or "").strip()
        if not name or name.lower() == text:
            # A bare reply *is* the slot answer; that path handles it.
            continue
        if re.search(rf"(?<![A-Za-z0-9]){re.escape(name.lower())}(?![A-Za-z0-9])", text):
            return name
    return ""


def pending_from_assistant_clarification(
    history: list[dict] | None,
) -> PendingSlot | None:
    """Rebuild a soft pending when memory TTL cleared but last ask is still in transcript."""
    q = last_assistant_content(history)
    if not q:
        return None
    low = q.lower()
    if "which connector" in low:
        missing = "connector_name"
        tool = "sample_connector_object"
    elif "which table" in low or "which collection" in low:
        missing = "table"
        tool = "sample_connector_object"
    else:
        return None
    # Bold candidates from the assistant message: **Local Postgres**
    raw = re.findall(r"\*\*([^*]{1,60})\*\*", q)
    cleaned: list[str] = []
    for c in raw:
        c = c.strip()
        if not c or len(c) > 48:
            continue
        if re.search(r"\b(?:confirm|jobs?|connectors?|settings|transfer|studio)\b", c, re.I):
            continue
        cleaned.append(c)
    return PendingSlot(
        tool=tool,
        missing=missing,
        args={},
        candidates=cleaned[:8],
        question=q.split("\n")[0][:240],
    )


# --------------------------------------------------------------------------
# 1. Clarification answers
# --------------------------------------------------------------------------


_FRESH_INTENT_RE = re.compile(
    r"\b(?:how\s+many|count|sum|avg|average|total|top|bottom|select|show|list|"
    r"sample|analyze|filter|group(?:ed)?\s+by|take\s+me|go\s+to|navigate|"
    r"open|explain|what(?:'s|\s+is)|how\s+does|how\s+do|can\s+you|could\s+you|"
    r"please|transfer|move|copy|sync|delete|export|create|schedule|"
    r"fix|repair|heal|quarantine)\b",
    re.I,
)

# Elliptical edits must never fill a connector/table clarification slot.
# Avoid bare metric verbs ("count of orders…") — those are fresh asks; "… instead"
# is covered by _INSTEAD_RE below.
_ELLIPTICAL_EDIT_RE = re.compile(
    r"^(?:only|just|and|also|now|then|same(?:\s+(?:for|but))?|use\s+\w+\s+instead|"
    r"make\s+it|switch\s+to|filter\s+to|do\s+that\s+again|by|per|"
    r"group(?:ed)?\s+by|drop\s+(?:the\s+)?group(?:ing)?|no\s+grouping|"
    r"top\s+\d|bottom\s+\d|"
    r"again|once\s+more|re-?run|repeat|sort|order\s+by|"
    r"where|filter|instead)\b",
    re.I,
)


# A turn carrying its own interrogative frame *and* its own subject is a
# question, not a fragment of the previous one. Without this, "where do I find
# the log for a run" was parsed as the SQL predicate of a remembered
# aggregation — ``^where\b`` is genuinely how an elliptical filter starts — so
# every knowledge question asked after a data question came back as a count of
# the wrong table. Turns containing a coreference ("what about that one") are
# excluded: those really do lean on the remembered subject.
_QUESTION_FRAME = re.compile(
    r"\bhow\s+(?:do|can|does|did|would|should)\s+(?:i|we|you|it)\b"
    r"|\bhow\s+many\s+\w+(?:\s+\w+){0,2}\s+(?:are|is|do|does)\b"
    # An auxiliary verb straight after ``where`` is enough on its own: a SQL
    # predicate puts a column there, so "where do rejected rows go" is asking a
    # question however its noun phrase is spelled. Requiring a pronoun or
    # determiner next left that turn looking elliptical, and with a sampled
    # table in focus it was answered as a filter over those rows —
    # "I couldn't complete that lookup: Provide a column to filter on."
    r"|\bwhere\s+(?:do|does|did|can|could|should|would|will|is|are|was|were)\b"
    r"|\bwhat\s+happens\b"
    r"|\bwhat\s+(?:is|are|does|do)\s+(?:a|an|the|this|it|each|my|your)\b"
    r"|\bwhat\s+\w+(?:\s+\w+){0,2}\s+(?:are|is)\s+there\b"
    r"|\bwhy\s+(?:do|does|did|is|are|was|were|can'?t|cannot)\b"
    r"|\bwho\s+can\b"
    r"|\bdo\s+(?:you|i|we)\s+(?:support|have|need|ever)\b"
    r"|\bis\s+it\s+(?:safe|idempotent|lossy|dangerous)\b"
    r"|\bif\s+(?:i|we|you)\s+(?:run|replay|redeliver)\b",
    re.I,
)

# "how many gates are there" is existential ``there``, not the anaphoric
# ``there`` that points at a remembered subject, so it must not count as a
# coreference.
_EXISTENTIAL_THERE = re.compile(r"\b(?:are|is|was|were)\s+there\b", re.I)

# ``that``/``which`` immediately before a verb heads a relative clause that
# restricts a noun in this same turn — "connectors that are not postgres" — so it
# is not a pointer at anything the previous turn said.
_RELATIVE_PRONOUN = re.compile(
    r"\b(?:that|which|who)\s+(?:are|is|was|were|do|does|did|have|has|had|can|"
    r"could|will|would|aren'?t|isn'?t|don'?t|doesn'?t|failed|fail|passed|pass|"
    r"ran|run|use|uses|point|points|match|matches)\b",
    re.I,
)


def has_own_question_frame(text: str) -> bool:
    """Whether the text is an interrogative about a subject the product documents.

    Both halves are required. The frame alone would swallow data questions that
    legitimately inherit the remembered table; the subject alone would swallow
    "by region", which names a corpus heading word and is still elliptical.
    """
    if not text or not _QUESTION_FRAME.search(text):
        return False
    try:
        from ..rag.product_docs import names_product_subject
    except Exception:
        return False
    return names_product_subject(text)


def asks_its_own_question(message: str) -> bool:
    """``has_own_question_frame``, unless the turn points at a remembered subject.

    A pronoun only points *outside* the turn when nothing inside it came first.
    "Where do rejected rows go and can I replay them" opens its own question and
    then refers back to the rows it just named; read as a coreference it looked
    elliptical, and with a sampled table in focus it was answered as a query
    over those rows instead of from the quarantine documentation.
    """
    text = _clean(message)
    if not text:
        return False
    frame = _QUESTION_FRAME.search(text)
    coref = _COREFERENCE_RE.search(_EXISTENTIAL_THERE.sub(" ", text))
    if coref and not (frame and frame.start() <= coref.start()):
        return False
    return has_own_question_frame(text)


# A leading ``where`` is a SQL predicate in "where region = east" and an English
# interrogative in "where do rejected rows go and can I replay them". The opener
# cannot tell them apart, and reading it as a predicate routed the second one to
# ``filter_result`` over whatever table was last sampled, so a documented
# question about quarantine was answered "I couldn't complete that lookup:
# Provide a column to filter on."
_PREDICATE_OPENER = re.compile(r"^(?:filter|where)\b", re.I)

# A predicate puts a column straight after ``where``; an auxiliary verb there
# means a question. ``filter`` is an imperative and is never interrogative.
_INTERROGATIVE_PREDICATE = re.compile(
    r"^where\s+(?:do|does|did|is|are|was|were|can|could|should|would|will|shall"
    r"|may|might|must|am|have|has|had)\b",
    re.I,
)

# What a predicate looks like once the opener is stripped: a comparison, a SQL
# predicate keyword, or the "column is value" shape of "filter where status is
# paid".
_PREDICATE_SHAPE = re.compile(
    r"[<>]=?|!=|<>|="
    r"|\bis\s*n[o']?t\b|\bisn'?t\b|\bnot\b"
    r"|\b(?:is|are)\s+\S+"
    r"|\b(?:like|between|in|null|empty|blank)\b"
    r"|\b(?:contains?|starts?\s+with|ends?\s+with|equals?|matches?)\b"
    r"|\b(?:greater|less|more|fewer|higher|lower)\s+than\b"
    r"|\bat\s+(?:least|most)\b",
    re.I,
)

#: A bare imperative is this long at most. "Filter" or "filter this result" is
#: an under-specified command, and the tool asking which column is the right
#: answer to it — unlike a sentence, which has to look like a predicate.
_BARE_PREDICATE_WORDS = 3


def opens_a_row_predicate(message: str, columns: Sequence[str] = ()) -> bool:
    """Whether a leading ``where``/``filter`` filters stored rows or asks a question.

    ``columns`` are the stored result's own column names when they are known,
    which settles the cases no general shape can: "where region east" is a
    predicate precisely because the sampled rows have a ``region``.
    """
    text = _clean(message)
    if not _PREDICATE_OPENER.match(text):
        return False
    if _INTERROGATIVE_PREDICATE.match(text):
        return False
    if len(_words(text)) <= _BARE_PREDICATE_WORDS:
        return True
    if _PREDICATE_SHAPE.search(_PREDICATE_OPENER.sub("", text, count=1)):
        return True
    named = {c.strip().lower() for c in columns if c and c.strip()}
    return bool(named & set(_words(text)))


# Acts that are self-contained turns by definition, so they can never be the
# answer to "which connector did you mean?". Reusing the dialogue-act classifier
# keeps this in step with what the router already recognises instead of growing a
# second keyword list beside ``_FRESH_INTENT_RE``.
_SELF_CONTAINED_ACTS = frozenset({"briefing", "greeting", "next_action", "thanks"})


def looks_like_fresh_intent(message: str) -> bool:
    """True when the user clearly started a new request (not a slot fill / typo)."""
    reply = _clean(message)
    if not reply:
        return False
    if bool(_FRESH_INTENT_RE.search(reply)) or asks_its_own_question(reply):
        return True
    # An open clarification used to swallow the next unrelated question: asked
    # right after a failed connector match, "is anything waiting on me" replayed
    # "No connector matched “quarantined”" and added "I didn't match that reply".
    from .dialogue_acts import classify_dialogue_act

    return classify_dialogue_act(reply) in _SELF_CONTAINED_ACTS


def looks_like_elliptical_edit(message: str) -> bool:
    """True for follow-up edits like \"only paid ones\" / \"and by region?\"."""
    reply = _clean(message)
    if not reply or len(_words(reply)) > _MAX_FOLLOWUP_WORDS:
        return False
    if asks_its_own_question(reply):
        return False
    if _ELLIPTICAL_EDIT_RE.search(reply):
        return True
    if _INSTEAD_RE.search(reply) and len(_words(reply)) <= 6:
        return True
    if _COREFERENCE_RE.search(reply) and len(_words(reply)) <= 8:
        return True
    return False


def resolve_pending_answer(
    message: str,
    pending: PendingSlot | None,
) -> tuple[str, dict[str, Any]] | None:
    """Fill the slot the pilot asked about and return the re-runnable tool call.

    Only bare, short replies are treated as answers. Anything that parses as a
    fresh request must not be swallowed by a stale question.
    """
    if not pending or not pending.tool or not pending.missing:
        return None
    reply = _clean(message)
    words = _words(reply)
    if not reply or len(words) > 6:
        return None
    # A fresh analytics / imperative / navigate / explain sentence is never a
    # slot answer — even when short ("take me to jobs", "fix bad data").
    if looks_like_fresh_intent(reply):
        return None
    # Elliptical edits ("only paid ones", "and by region?") are follow-ups, not
    # connector/table names — never swallow them into a clarification slot.
    if looks_like_elliptical_edit(reply):
        return None

    lower = reply.lower()
    candidates = [c for c in (pending.candidates or []) if c]

    value = ""
    if candidates:
        # Exact / case-insensitive / unique-substring match against the offered set.
        for cand in candidates:
            if cand.lower() == lower:
                value = cand
                break
        if not value:
            hits = [c for c in candidates if lower and lower in c.lower()]
            if len(hits) == 1:
                value = hits[0]
        if not value and lower in {"the first", "first", "first one", "1"}:
            value = candidates[0]
        if not value and lower in {"the second", "second", "second one", "2"} and len(candidates) > 1:
            value = candidates[1]
        # With a candidate list, refuse free-form text that doesn't match —
        # guessing a connector name the operator never offered is how silent
        # wrong-warehouse queries happen.
        if not value:
            return None

    if not value:
        # No candidate list (e.g. "which table?") — accept a bare identifier only.
        # Never accept a multi-word sentence as a connector/table name.
        if lower in _AFFIRMATIVE:
            return None
        if len(words) > 3:
            return None
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.\-]{0,60}", reply):
            if lower in {"i don't know", "not sure", "no", "nope", "none", "jobs", "contracts", "settings"}:
                return None
            value = reply
        elif re.fullmatch(r"[A-Za-z_][A-Za-z0-9_. \-]{0,40}", reply) and len(words) <= 3:
            # "Local Postgres" / "Prod Mongo" style names
            if any(w in lower for w in ("take", "me", "to", "go", "open", "show", "the")):
                return None
            value = reply
    if not value:
        return None

    args = dict(pending.args or {})
    args[pending.missing] = value
    # A freshly named connector invalidates a previously guessed id.
    if pending.missing == "connector_name":
        args.pop("connector_id", None)
    return pending.tool, args


# --------------------------------------------------------------------------
# 2 + 3. Edits and slot inheritance
# --------------------------------------------------------------------------


def _extract_edit_group_by(message: str) -> tuple[str, bool]:
    """Return (dimension, found). "and by region" / "per country" / "no grouping"."""
    if re.search(r"\b(?:no|without|drop|remove)\s+(?:the\s+)?group(?:ing|\s+by)?\b", message, re.I):
        return "", True
    grp = re.search(
        r"\b(?:group(?:ed)?\s+by|broken\s+down\s+by|by|per)\s+"
        r"([A-Za-z_][A-Za-z0-9_ ]{0,40}?)"
        r"(?=\s+(?:instead|now|too|also|please|from|in|on|for)\b|[.,;?!]|$)",
        message,
        re.I,
    )
    if not grp:
        return "", False
    dim = _clean(grp.group(1))
    dim = re.sub(r"^(?:the|a|an)\s+", "", dim, flags=re.I).strip()
    return dim, bool(dim)


def _extract_edit_metric(message: str) -> tuple[str, str]:
    """Return (metric, measure_column) for "average amount instead" style edits."""
    for pattern, metric in _METRIC_EDIT_PHRASES:
        found = re.search(rf"\b{pattern}\b", message, re.I)
        if not found:
            continue
        tail = message[found.end() :]
        col = re.match(
            r"\s*(?:of\s+|for\s+|the\s+)*([A-Za-z_][A-Za-z0-9_ ]{0,40}?)"
            r"(?=\s+(?:instead|now|too|also|please|by|per|from|in|on)\b|[.,;?!]|$)",
            tail,
            re.I,
        )
        column = ""
        if col:
            candidate = _clean(col.group(1))
            if candidate.lower() not in {
                "instead", "rather", "value", "values", "one", "it", "that", "this",
                "rows", "row", "records",
            }:
                column = candidate
        return metric, column
    return "", ""


# Where a named slot value ends: the next edit clause, punctuation, or the end
# of the turn. A table switch states the table *last* ("same for products",
# "what about the invoices table"), so anything else trailing the candidate
# means the candidate was a modifier and not the subject: the noun in "what
# about generated columns" was being dropped and its adjective introspected as
# a table. ``_extract_edit_group_by`` deliberately keeps a shorter tail — ``by``
# and ``per`` introduce *its* value rather than ending one.
_VALUE_TAIL = (
    r"(?=\s+(?:instead|rather|now|too|also|please|and|but|"
    r"by|per|group(?:ed)?\s+by|from|in|on|for|with|"
    r"only|just|where|filter|top|bottom|limit)\b"
    r"|[.,;?!]|$)"
)


#: "and customers" after counting ``orders`` is the same question about another
#: table — the shortest form of "same for customers". Only a plural noun is read
#: this way: "and pending" is far more likely to be a filter value, and reading
#: it as a table would answer a question the operator did not ask.
_BARE_TABLE_SWAP = re.compile(
    r"^(?:and|also|now|then|plus)\s+(?:the\s+)?"
    r"([A-Za-z_][A-Za-z0-9_.]{2,48}s)"
    r"(?:\s+(?:table|collection))?[\s.?!]*$",
    re.I,
)


def _extract_edit_table(message: str) -> str:
    """"same for products", "now do orders", "what about the invoices table"."""
    m = re.search(
        r"\b(?:same\s+(?:for|on|with)|now\s+(?:do|try|for)|what\s+about|how\s+about|switch\s+to)\s+"
        r"(?:the\s+)?([A-Za-z_][A-Za-z0-9_.]{1,48})"
        r"(?:\s+(?:table|collection))?" + _VALUE_TAIL,
        message,
        re.I,
    )
    if not m:
        m = _BARE_TABLE_SWAP.match(_clean(message))
    if not m:
        return ""
    table = _clean(m.group(1))
    # "what about it" / "same for that" point at the remembered table; they do
    # not name a new one. Left to the parser they became a lookup of a table
    # literally called ``it``.
    if _COREFERENCE_RE.fullmatch(table):
        return ""
    banned = _PLATFORM_NOUNS | set(_TEMPORAL_GRAINS) | {
        "average", "avg", "mean", "sum", "total", "count", "min", "max",
        "minimum", "maximum", "distinct", "unique", "amount", "price",
        "revenue", "qty", "quantity", "value", "values",
    }
    if table.lower() in banned:
        return ""
    # "what about average amount" is a metric edit, not a table switch.
    if re.search(
        rf"\b(?:what|how)\s+about\s+{re.escape(table)}\b.{{0,20}}\b"
        rf"(?:amount|price|revenue|qty|quantity|value|of|for)\b",
        message,
        re.I,
    ):
        return ""
    return table


def _extract_edit_limit(message: str) -> int:
    m = re.search(r"\b(?:top|first|bottom|last|show|only|just)\s+(\d{1,3})\b", message, re.I)
    if m:
        return max(1, min(int(m.group(1)), 200))
    return 0


def looks_like_followup(message: str, focus: PilotFocus | None) -> bool:
    """Cheap gate: does this turn lean on remembered state at all?"""
    if not focus or not focus.has_target():
        return False
    text = _clean(message)
    if not text:
        return False
    words = _words(text)
    if len(words) > _MAX_FOLLOWUP_WORDS:
        return False
    if asks_its_own_question(text):
        return False
    # Self-contained asks name their own table/connector — not elliptical.
    if re.search(
        r"\b(?:from|in)\s+[A-Za-z_][A-Za-z0-9_]*\b",
        text,
        re.I,
    ) and not _COREFERENCE_RE.search(text):
        return False
    if (
        re.search(r"\bon\s+[A-Za-z_][A-Za-z0-9_\- ]{1,40}\s*$", text, re.I)
        and len(words) >= 5
        and not _COREFERENCE_RE.search(text)
    ):
        return False
    # "again" after a count is the same query re-run. It reached no tool at all
    # and came back as "outside what the Datawrap documentation covers".
    if _REPEAT_RE.match(text):
        return True
    if _COREFERENCE_RE.search(text):
        return True
    if re.match(r"^(?:and|also|now|then|what\s+about|how\s+about|ok\s+)", text, re.I):
        return True
    if re.match(r"^(?:sort|order)\b", text, re.I):
        return True
    if _INSTEAD_RE.search(text):
        return True
    # A bare "by region" / "top 5" / "average amount" is elliptical by itself.
    if re.match(r"^(?:group(?:ed)?\s+by|by|per)\b", text, re.I):
        return True
    if re.match(r"^(?:top|bottom)\s+\d", text, re.I):
        return True
    # "only paid ones" / "just pending" — filter the remembered subject.
    if re.match(r"^(?:only|just)\s+\S+", text, re.I):
        return True
    if opens_a_row_predicate(text, focus.columns or ()):
        return True
    # "no grouping" / "drop the group by" removes a slot without naming a subject.
    if _extract_edit_group_by(text)[1]:
        return True
    metric, _ = _extract_edit_metric(text)
    return bool(metric) and len(words) <= 6


def resolve_followup(
    message: str,
    focus: PilotFocus | None,
) -> AggregationRequest | None:
    """Build the next aggregation by editing the remembered one.

    Returns None when the turn does not reference prior state, so the caller
    falls through to ordinary routing.
    """
    if not focus or not focus.has_target():
        return None
    if not looks_like_followup(message, focus):
        return None

    text = _clean(message)
    base_metric = focus.metric if focus.metric in _METRICS else "count"
    req = AggregationRequest(
        metric=base_metric,
        column=focus.column,
        table=focus.table,
        group_by=focus.grain or focus.group_by,
        connector_name=focus.connector_name,
        limit=focus.limit or 20,
        descending=focus.descending,
        where=getattr(focus, "where", "") or "",
    )

    # "again" changes nothing, which is exactly the edit: re-run what we ran.
    if _REPEAT_RE.match(text):
        return req

    edited = False

    where_clause = _extract_edit_where(text, focus)
    if where_clause:
        req.where = where_clause
        edited = True

    table = _extract_edit_table(text)
    if table:
        req.table = table
        # A different table cannot keep the previous table's columns.
        req.column = ""
        req.group_by = ""
        edited = True

    metric, measure = _extract_edit_metric(text)
    if metric:
        req.metric = metric
        if measure:
            req.column = measure
            edited = True
        elif metric == "count":
            req.column = ""
        edited = True

    dim, found = _extract_edit_group_by(text)
    if found:
        req.group_by = dim
        edited = True

    limit = _extract_edit_limit(text)
    if limit:
        req.limit = limit
        edited = True

    if _ASC_RE.search(text):
        req.descending = False
        edited = True
    elif _DESC_RE.search(text) and not re.match(r"^(?:top|bottom)\s+\d", text, re.I):
        req.descending = True
        # A direction on its own is a whole edit. Without this, "sort it
        # descending" made no change the caller could see, so it fell through to
        # retrieval and was refused as undocumented.
        edited = True

    if not edited:
        return None

    # A metric that needs a measure but inherited none is unanswerable — let the
    # tool ask for the column against the real schema instead of guessing.
    if _METRICS[req.metric][1] and not req.column:
        req.missing.append("column")
    return req


def inherit_focus_slots(
    planned: list[tuple[str, dict[str, Any]]],
    focus: PilotFocus | None,
) -> list[tuple[str, dict[str, Any]]]:
    """Fill omitted connector/table on freshly parsed calls from working memory.

    "count of orders by status" then "average price in products" — the second
    turn names no connector, and re-asking for it every time is what made the
    pilot feel amnesiac. Only *absent* slots are filled; anything the user said
    explicitly always wins.
    """
    if not focus or not planned:
        return planned
    out: list[tuple[str, dict[str, Any]]] = []
    for name, args in planned:
        merged = dict(args or {})
        if name in _CONNECTOR_SCOPED_TOOLS:
            if not merged.get("connector_id") and not merged.get("connector_name"):
                if focus.connector_id:
                    merged["connector_id"] = focus.connector_id
                elif focus.connector_name:
                    merged["connector_name"] = focus.connector_name
        if name in _TABLE_SCOPED_TOOLS and not merged.get("table") and focus.table:
            merged["table"] = focus.table
        if name == "aggregate_data" and not merged.get("where") and getattr(focus, "where", ""):
            merged["where"] = focus.where
        out.append((name, merged))
    return out


# Tools whose subject is the platform itself — its jobs, connectors, schedules,
# contracts and datasets. The remembered warehouse table is never a candidate
# reading of a turn that resolved one of these, so the ellipsis layer must leave
# such a plan alone. Documentation tools are deliberately absent: "by region"
# and "how many rows" both fall back to ``explain_product`` and genuinely do
# need the remembered subject.
_OWN_SUBJECT_TOOLS = frozenset({
    "list_jobs",
    "get_job",
    "open_job",
    "list_connectors",
    "search_connectors",
    "list_schedules",
    "get_schedule",
    "open_schedule",
    "run_schedule_now",
    "list_contracts",
    "list_datasets",
    "get_preflight_run",
    "brief_workspace",
})


def names_its_own_subject(planned: list[tuple[str, dict[str, Any]]]) -> bool:
    """Whether routing already found a subject the working memory cannot supply.

    "How many jobs ran today" is a bare metric phrase, so the elliptical gate
    fires — but it resolves ``list_jobs``, which means the turn said what it was
    about. Rewriting it as an edit of the remembered aggregation answered it
    with a row count of an unrelated table.
    """
    return any(name in _OWN_SUBJECT_TOOLS for name, _ in planned or ())


_CONNECTOR_SCOPED_TOOLS = frozenset({
    "aggregate_data",
    "sample_connector_object",
    "introspect_connector_schema",
    "list_connector_objects",
    "run_query",
})

_TABLE_SCOPED_TOOLS = frozenset({
    "aggregate_data",
    "sample_connector_object",
    "introspect_connector_schema",
})


def focus_from_tool_output(name: str, output: dict[str, Any]) -> dict[str, Any]:
    """Extract the working-memory update from a successful tool result."""
    if not isinstance(output, dict):
        return {}
    update: dict[str, Any] = {
        "connector_id": str(output.get("connector_id") or ""),
        "connector_name": str(output.get("connector_name") or ""),
        "connector_type": str(output.get("type") or ""),
        "table": str(output.get("table") or ""),
        "result_id": str(output.get("result_id") or ""),
        "tool": name,
    }
    if name == "aggregate_data":
        # The aggregate owns these slots outright — keep the blanks so an
        # emptied GROUP BY is remembered as emptied.
        authoritative = {
            "metric": str(output.get("metric") or ""),
            "column": str(output.get("column") or ""),
            "group_by": str(output.get("group_by") or ""),
            "grain": str(output.get("grain") or ""),
            "where": str(output.get("where") or ""),
        }
        update = {k: v for k, v in update.items() if v not in ("", None, [])}
        update.update(authoritative)
        return update
    if name in ("sample_connector_object", "introspect_connector_schema"):
        cols = output.get("columns") or []
        names: list[str] = []
        for col in cols:
            if isinstance(col, dict) and col.get("name"):
                names.append(str(col["name"]))
            elif isinstance(col, str):
                names.append(col)
        if names:
            update["columns"] = names
    return {k: v for k, v in update.items() if v not in ("", None, [])}


def clarification_slot(name: str, args: dict[str, Any], error: str) -> PendingSlot | None:
    """Turn a tool's "which one did you mean?" error into a resumable slot."""
    text = error or ""
    if not text:
        return None
    lowered = text.lower()

    if "which connector" in lowered or "no connector matched" in lowered:
        # Candidate names are emitted as **bold** by schema_tools.
        candidates = re.findall(r"\*\*([^*]+)\*\*", text)
        return PendingSlot(
            tool=name,
            args={k: v for k, v in (args or {}).items() if k != "connector_name"},
            missing="connector_name",
            question=text,
            candidates=[c.strip() for c in candidates if c.strip()],
        )
    if "which table" in lowered or "did you mean" in lowered and "table" in lowered:
        return PendingSlot(
            tool=name,
            args=dict(args or {}),
            missing="table",
            question=text,
        )
    if "no table" in lowered and ("which table" in lowered or "did you mean" in lowered or "list tables" in lowered):
        return PendingSlot(
            tool=name,
            args=dict(args or {}),
            missing="table",
            question=text,
        )
    if re.search(r"which column|which date column", lowered):
        candidates = re.findall(r"[:]\s*(.+)$", text.strip())
        listed: list[str] = []
        if candidates:
            listed = [c.strip(" .") for c in candidates[0].split(",") if c.strip(" .")]
        return PendingSlot(
            tool=name,
            args=dict(args or {}),
            missing="column",
            question=text,
            candidates=listed,
        )
    return None
