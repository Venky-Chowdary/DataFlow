"""Row-count intent: "how many rows / count the rows / row count" → COUNT(*).

QA asked Pilot to count rows and got a 25-row sample ("show me the count of rows
in X" matched the sample route), or a table called ``of`` / ``are``, or the
documentation. A row count is one intent with two slots — the table and the
connector — so it is recognised once, before the sample and free-form
aggregation parsers, and always runs ``aggregate_data`` with ``metric=count``
(an exact ``SELECT COUNT(*)`` whose answer says it is not a sample).

The connector slot is filled by :mod:`connector_resolver` (name, display name or
id, case/spacing-blind). Filters, groupings and anything that is not plainly a
row count are left to the existing parsers.
"""

from __future__ import annotations

import logging
import re
from typing import Any

_ROW = r"(?:rows?|records?|entries|lines)"
_COUNT_ASK = re.compile(
    rf"\bhow\s+many\s+{_ROW}\b"
    rf"|\b(?:row|record)\s*-?\s*counts?\b"
    rf"|\bcount\s+(?:of\s+|up\s+)?(?:all\s+)?(?:the\s+)?(?:number\s+of\s+)?{_ROW}\b"
    rf"|\bnumber\s+of\s+{_ROW}\b"
    rf"|\btotal\s+(?:number\s+of\s+)?{_ROW}\b",
    re.I,
)
# "count orders" / "count the orders rows" — a bare count verb on one object.
_COUNT_OBJECT = re.compile(
    rf"^\s*(?:please\s+)?(?:can\s+you\s+|could\s+you\s+)?count\s+(?:the\s+)?"
    rf"([A-Za-z_][\w.$]*)(?:\s+(?:table|{_ROW}))?\s*[?.!]*\s*$",
    re.I,
)
# Anything beyond a whole-table count belongs to the aggregation parser.
_NOT_PLAIN = re.compile(
    r"\b(?:by|per|group(?:ed)?|where|distinct|unique|each|average|avg|mean|sum|"
    r"max(?:imum)?|min(?:imum)?|between|since|today|yesterday|last|after|before|"
    r"nulls?|empty|blank|missing|duplicates?|greater|less|more\s+than|fewer|"
    r"equals?|contains?|like|not)\b|[<>=]",
    re.I,
)
# Platform objects have their own reads (jobs, quarantine, connectors …).
_PLATFORM = re.compile(
    r"\b(?:jobs?|transfers?|runs?|pipelines?|schedules?|quarantin\w*|staging|"
    r"written|wrote|loaded|copied|moved|synced|inserted|rejected|failed|skipped|"
    r"connectors?|connections?|datasets?|uploads?|tables|collections|schemas|"
    r"contracts?|gates?|preflight)\b",
    re.I,
)
_IDENT = r"[A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*){0,2}"
_STOP = frozenset(
    """
    a an the in on of from for inside within into at to is are was were be there here
    does do did have has had how many much count counts counting row rows record
    records entries entry line lines number total table please can could would you
    me tell give show get what whats what's it its this that these those all exactly
    currently now i we my our up and find out let know like return query check just
    quick quickly actual real again so far right big size sql select star
    """.split()
)
_COREFERENT = frozenset({"it", "that", "this", "them", "those", "these"})


def _saved_pool() -> list[dict[str, Any]]:
    try:
        from services.connector_store import list_connectors

        return [c.to_dict() if hasattr(c, "to_dict") else dict(c.__dict__) for c in list_connectors()]
    except Exception as exc:  # noqa: BLE001 - no pool means no connector slot
        logging.getLogger(__name__).warning("Connector list unavailable: %s", exc, exc_info=exc)
        return []


def _candidates(text: str) -> list[str]:
    return [
        tok
        for tok in re.findall(_IDENT, text)
        if tok.lower() not in _STOP and not tok.isdigit()
    ]


def _table_slot(text: str) -> str:
    for pattern in (
        rf"\btable\s+({_IDENT})",
        rf"\b({_IDENT})\s+table\b",
        rf"\b(?:in|from|of|for|inside|within|on)\s+(?:the\s+)?({_IDENT})",
        rf"\bdoes\s+(?:the\s+)?({_IDENT})\s+have\b",
    ):
        for found in re.finditer(pattern, text, re.I):
            name = found.group(1)
            if name.lower() not in _STOP:
                return name
    left = _candidates(text)
    return left[0] if len(left) == 1 else ""


def plan_row_count(message: str) -> list[tuple[str, dict]] | None:
    """``[("aggregate_data", {...count...})]`` for a plain row-count ask, else None."""
    text = (message or "").strip()
    if not text or len(text) > 300 or "\n" in text:
        return None
    if re.match(r"^\s*(?:select|with)\b", text, re.I):
        return None

    from .connector_resolver import find_connector_mention, strip_mention

    mention = find_connector_mention(text, _saved_pool())
    rest = strip_mention(text, mention) if mention else text
    bare = _COUNT_OBJECT.match(rest)
    if not (_COUNT_ASK.search(rest) or bare):
        return None
    if _NOT_PLAIN.search(rest) or _PLATFORM.search(rest):
        return None

    table = bare.group(1) if bare and bare.group(1).lower() not in _STOP else _table_slot(rest)
    if table.lower() in _COREFERENT:
        return None
    if not mention:
        # Without a resolved connector, every other word must be accounted for:
        # an unknown connector name left in the sentence is the not-found
        # recovery's job, never a silent read of some other connector.
        left = [t for t in _candidates(rest) if t != table]
        if not table or left:
            return None
    args: dict[str, Any] = {"metric": "count", "table": table}
    if mention:
        unique = mention.unique
        # Ambiguous: pass what was typed; the shared resolver asks which one.
        if unique:
            args["connector_id"] = str(unique.get("id") or unique.get("_id") or "")
            args["connector_name"] = str(unique.get("name") or "")
        else:
            args["connector_name"] = mention.text
    return [("aggregate_data", args)]
