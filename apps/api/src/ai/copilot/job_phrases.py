"""Read the time window and qualifiers out of a jobs question — and what is left.

The window reader used to take the first phrase it knew and drop the rest, so
"the day before yesterday" was answered for yesterday and "in 2025" for the
whole history, stated as fact (review C2, M1, M4). Here every phrase that is
applied is *consumed*; whatever time-like word or qualifier is left over is
returned so the caller can refuse it by name. There is one rule — anything
time-like or qualifying that was not applied is refused — not a list of
unsupported phrases to keep in step with the supported ones.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

_NUMBER_WORDS = {
    "a": 1, "an": 1, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
    "seven": 7, "eight": 8, "nine": 9, "ten": 10, "eleven": 11, "twelve": 12,
    "thirteen": 13, "fourteen": 14, "fifteen": 15, "sixteen": 16, "seventeen": 17,
    "eighteen": 18, "nineteen": 19, "twenty": 20, "thirty": 30, "forty": 40,
    "fifty": 50, "sixty": 60, "ninety": 90, "couple of": 2, "a couple of": 2,
}
_NUM = r"\d{1,4}|a\s+couple\s+of|couple\s+of|" + "|".join(
    sorted((w for w in _NUMBER_WORDS if " " not in w), key=len, reverse=True)
)
_UNITS = {
    "minute": "minutes", "minutes": "minutes", "min": "minutes", "mins": "minutes", "m": "minutes",
    "hour": "hours", "hours": "hours", "hr": "hours", "hrs": "hours", "h": "hours",
    "day": "days", "days": "days", "d": "days",
    "week": "weeks", "weeks": "weeks", "wk": "weeks", "wks": "weeks", "w": "weeks",
}
_ISO = r"\d{4}-\d{2}-\d{2}"

#: Phrases that are applied, in the order they are tried. Each match is removed
#: from the text before the next is tried.
_RANGE = re.compile(rf"\b(?:from|between)\s+({_ISO})\s+(?:to|and|until|through|-)\s+({_ISO})\b", re.I)
_SINCE_ISO = re.compile(rf"\b(?:since|from)\s+({_ISO})\b", re.I)
_ON_ISO = re.compile(rf"\b(?:on\s+)?({_ISO})\b", re.I)
_ROLLING = re.compile(
    r"\b(?:(?:in|over|during|for|within)\s+)?(?P<the>the\s+)?(?P<edge>last|past|previous)\s+"
    rf"(?:(?P<n>{_NUM})\s*)?(?P<unit>minutes?|mins?|hours?|hrs?|days?|weeks?|wks?|(?<=\d)[hdwm](?![a-z]))\b",
    re.I,
)
_NAMED = (
    ("since_yesterday", re.compile(r"\bsince\s+yesterday\b", re.I)),
    ("today", re.compile(r"\b(?:so\s+far\s+)?today\b(?:\s+so\s+far)?", re.I)),
    ("yesterday", re.compile(r"\byesterday\b", re.I)),
    ("this_week", re.compile(r"\bthis\s+week\b", re.I)),
    ("this_month", re.compile(r"\bthis\s+month\b", re.I)),
    # "the last month" / "the past month" is a rolling month: not applied.
    ("last_month", re.compile(r"(?<!the\s)\b(?:last|previous)\s+month\b", re.I)),
)
#: "Recently" is answered for a stated window, never for the whole history.
_RECENTLY = re.compile(r"\b(?:recently|lately|of\s+late)\b", re.I)
RECENT_WINDOW = "last_7_days"
#: Words that read as time but filter nothing: "running now", "recent jobs".
_NO_OP = re.compile(
    r"\b(?:right\s+now|just\s+now|now|currently|at\s+the\s+moment|at\s+present|so\s+far|recent|latest|"
    r"most\s+recent|newest)\b",
    re.I,
)

_MONTHS = (
    r"january|february|march|april|june|july|august|september|october|november|december|"
    r"jan|feb|mar|apr|jun|jul|aug|sept?|oct|nov|dec"
)
_WEEKDAYS = r"monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon|tues?|wed|thur?s?|fri|sat|sun"
#: A time-like word left over after the applied phrases were removed.
_TIME_TOKEN = re.compile(
    r"(?:\b(?:in|over|during|for|within|since|before|after|until|till|on|by|from|at|between|of)\s+)?"
    r"(?:\b(?:the|this|that|last|past|previous|next|coming|few|several|couple\s+of|a)\s+)*"
    r"(?:\b\d+\s+)?"
    r"(?:\b(?:today|tonight|tomorrow|yesterday|hours?|hrs?|minutes?|mins?|days?|weeks?|"
    r"weekends?|months?|years?|quarters?|fortnights?|mornings?|afternoons?|evenings?|nights?|"
    r"overnight|midnight|noon|ago|since|before|after|until|till|between|during|earlier|later|"
    r"wtd|mtd|ytd|to[\s-]date|eod|eow|eom|q[1-4]|o'?clock|" + _MONTHS + "|" + _WEEKDAYS + r")\b"
    r"|\bmay\s+\d{1,2}\b|\b(?:in|on|since)\s+may\b"
    r"|\b\d{1,2}(?::\d{2})?\s*(?:am|pm)\b|\b\d{1,2}(?:st|nd|rd|th)\b"
    r"|\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b|\b(?:19|20)\d{2}\b|\b\d+\s*[hdwm]\b)"
    r"(?:\s+(?:ago|before|after|earlier|later))?",
    re.I,
)

#: Gate-8 verification verdicts the ledger filters by (``verification``).
_VERIFY_OBJECT = r"(?:the\s+|their\s+|its\s+)?(?:gate[\s-]*8|g8|verification|reconciliation|parity(?:\s+check)?)"
_VERIFICATION = (
    ("failed", re.compile(
        rf"\b(?:fail(?:ed|ing|s)?)\s+{_VERIFY_OBJECT}\b"
        r"|\b(?:gate[\s-]*8|g8|verification|reconciliation|parity(?:\s+check)?)\s+(?:has\s+|have\s+|was\s+)?failed\b",
        re.I,
    )),
    ("passed", re.compile(
        rf"\b(?:pass(?:ed|es)?)\s+{_VERIFY_OBJECT}\b"
        r"|\b(?:gate[\s-]*8|g8|verification|reconciliation|parity(?:\s+check)?)\s+(?:has\s+|have\s+|was\s+)?passed\b",
        re.I,
    )),
)

#: Qualifiers the job ledger read does not apply: connector types, sync modes,
#: verification, negation. Each is refused by name until a filter applies it.
_QUALIFIERS = (
    re.compile(
        r"\b(?:mysql|mariadb|postgres(?:ql)?|pg|snowflake|big\s*query|redshift|oracle|sql\s*server|"
        r"mssql|sqlite|mongo(?:db)?|cassandra|dynamo(?:db)?|elastic(?:search)?|kafka|s3|gcs|"
        r"azure\s+blob|adls|csv|parquet|excel|xlsx|databricks|synapse|teradata|db2|salesforce|"
        r"hubspot|stripe|duckdb|clickhouse|neo4j|redis|rest\s+api)\b",
        re.I,
    ),
    re.compile(
        r"\b(?:cdc|change\s+data\s+capture|incremental|full[\s_-]*refresh\w*|upserts?|"
        r"append(?:[\s_-]only)?|overwrite|snapshots?)\b",
        re.I,
    ),
    # States that are not a stored status: refused, never read as a near one.
    re.compile(
        r"\b(?:stuck|hung|hanging|killed|timed[\s-]*out|time[\s-]*outs?|aborted|crashed|"
        r"unsuccessful|skipped)\b",
        re.I,
    ),
    re.compile(r"\b(?:verif\w*|gate[\s-]*8|g8|parity|reconcil\w*|trust\s+report)\b", re.I),
)
_NEGATION = re.compile(
    r"\b(?:not|never|no|without|except|excluding|other\s+than|non[\s-]\w+)\b|\w+n['’]t\b", re.I
)
#: A saved connector named after a preposition: "on Local Postgres".
_NAMED_TARGET = re.compile(
    r"\b(?:on|from|to|into|via|using|in)\s+"
    r"(?!(?:I|Datawrap|DataFlow|Pilot|Jobs?|Transfer|Studio|UTC)\b)((?:[A-Z][\w-]*)(?:\s+[A-Z][\w-]*){0,3})"
)


@dataclass(frozen=True)
class FilterReading:
    """What a question asks the ledger to filter by, and what it could not apply."""

    window: dict[str, str] = field(default_factory=dict)
    #: The phrase to refuse by name ("" when everything was applied).
    unapplied: str = ""
    #: ``time`` for a time phrase, ``filter`` for any other qualifier.
    kind: str = ""
    #: The Gate-8 verdict asked for (``failed`` / ``passed``), applied.
    verification: str = ""
    #: The question with every applied phrase removed.
    rest: str = ""
    #: The words of the applied window, as the operator wrote them ("yesterday").
    window_text: str = ""


def _blank(text: str, start: int, end: int) -> str:
    return text[:start] + " " * (end - start) + text[end:]


def _rolling_key(match: re.Match[str]) -> str | None:
    """``last_<n>_<unit>`` for a rolling phrase, or None when it is a calendar one."""
    unit = _UNITS[match.group("unit").lower()]
    raw = re.sub(r"\s+", " ", (match.group("n") or "").lower()).strip()
    if not raw:
        # "last week" is the calendar week; "the last week", "the past week"
        # and "past week" are the last seven days — one meaning for both.
        if unit == "weeks" and not match.group("the") and match.group("edge").lower() != "past":
            return "last_week"
        n = 1
    else:
        n = int(raw) if raw.isdigit() else _NUMBER_WORDS.get(raw, 0)
    if n < 1:
        return None
    return f"last_{n}_{unit}"


def read_filters(message: str) -> FilterReading:
    """The window a jobs question names and the first phrase it could not apply."""
    original = message or ""
    text = original
    windows: list[dict[str, str]] = []
    spans: list[tuple[int, int]] = []
    unapplied: list[tuple[int, int]] = []

    window_spans: list[tuple[int, int]] = []

    def take(match: re.Match[str], window: dict[str, str] | None) -> None:
        nonlocal text
        spans.append(match.span())
        if window is None:
            unapplied.append(match.span())
        else:
            windows.append(window)
            window_spans.append(match.span())
        text = _blank(text, *match.span())

    verdicts: list[str] = []
    for verdict, pattern in _VERIFICATION:
        for match in list(pattern.finditer(text)):
            verdicts.append(verdict)
            text = _blank(text, *match.span())
    for match in list(_RANGE.finditer(text)):
        take(match, {"date_from": match.group(1), "date_to": match.group(2)})
    for match in list(_SINCE_ISO.finditer(text)):
        take(match, {"date_from": match.group(1)})
    for match in list(_ON_ISO.finditer(text)):
        take(match, {"date_from": match.group(1), "date_to": match.group(1)})
    for key, pattern in _NAMED[:1]:
        for match in list(pattern.finditer(text)):
            take(match, {"window": key})
    for match in list(_ROLLING.finditer(text)):
        key = _rolling_key(match)
        take(match, {"window": key} if key else None)
    for key, pattern in _NAMED[1:]:
        for match in list(pattern.finditer(text)):
            take(match, {"window": key})
    for match in list(_RECENTLY.finditer(text)):
        take(match, {"window": RECENT_WINDOW})
    for match in list(_NO_OP.finditer(text)):
        text = _blank(text, *match.span())

    leftover = [m.span() for m in _TIME_TOKEN.finditer(text) if m.group(0).strip()]
    if unapplied or leftover or len(windows) > 1:
        # Name the whole time expression: what was applied plus what was not.
        every = spans + leftover
        start, end = min(s for s, _ in every), max(e for _, e in every)
        return FilterReading(window={}, unapplied=original[start:end].strip(" ,.?!"), kind="time")
    for pattern in _QUALIFIERS:
        found = pattern.search(text)
        if found:
            return FilterReading(window={}, unapplied=found.group(0), kind="filter")
    if len(set(verdicts)) > 1:
        return FilterReading(window={}, unapplied="passed and failed verification", kind="filter")
    return FilterReading(
        window=windows[0] if windows else {}, verification=verdicts[0] if verdicts else "", rest=text,
        window_text=" ".join(original[a:b].strip() for a, b in sorted(window_spans)),
    )


def narrowing_in(message: str) -> str:
    """A named connector ("on Local Postgres") or a negation ("didn't"), or "".

    Read only for a filtered count or list: there either one would silently
    widen the answer to every connector, or invert it.
    """
    target = _NAMED_TARGET.search(message or "")
    if target:
        return f"{target.group(0).split()[0]} {target.group(1)}"
    found = _NEGATION.search(message or "")
    return found.group(0) if found else ""
