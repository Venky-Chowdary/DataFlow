"""Row-level data rules spoken in chat, parsed into engine-shaped controls.

Operators state a transfer and its rules in one breath — "move users from
Prod to Warehouse, only rows where status = active, upsert on id, skip nulls
in email". Two things then matter more than fluency:

* the rule clauses must not be read as part of the connector's name, or the
  route stops resolving and the request looks like something the Pilot cannot
  do at all;
* a rule the engine cannot apply is **never** dropped. Accepting "only active
  rows" and then copying the whole table is a correctness failure, not a
  wording problem. Anything unresolved comes back as a question, and the
  caller refuses to stage the run.

The parser only emits controls the transfer engine already honours:
``TransferRequest.source_filter`` (see :mod:`services.row_filter`), the upsert
key on the stream contract, and ``limit``. It never invents a schedule, a
transform, or a masking rule.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from .schedule_cadence import cadence_qualifiers


def _utterance_row_limit(raw: str) -> int:
    """English 'first 1,000 rows' uses US grouping — still the write parser.

    This is spoken English, not a data cell. Auto would fail-close ``1,000``.
    """
    from services.transform_engine import (
        decimal_wire_value,
        reset_active_number_locale,
        set_active_number_locale,
    )

    token = set_active_number_locale("US")
    try:
        dec = decimal_wire_value(raw)
    finally:
        reset_active_number_locale(token)
    if dec is None or dec <= 0 or dec != dec.to_integral_value():
        return 0
    return int(dec)

# Engine operator spellings (services.row_filter._FILTER_OPS). The analytics
# filter parser uses "not_null"; the engine wants "is_not_null" — do not mix.
_NEGATE = {
    "eq": "ne",
    "ne": "eq",
    "gt": "lte",
    "gte": "lt",
    "lt": "gte",
    "lte": "gt",
    "in": "not_in",
    "not_in": "in",
    "is_null": "is_not_null",
    "is_not_null": "is_null",
}
_SYMBOL_OPS = {
    "=": "eq",
    "==": "eq",
    "!=": "ne",
    "<>": "ne",
    ">": "gt",
    ">=": "gte",
    "<": "lt",
    "<=": "lte",
}
_WORD_OPS = {
    "is": "eq",
    "equals": "eq",
    "equal to": "eq",
    "not": "ne",
    "is not": "ne",
    "greater than": "gt",
    "more than": "gt",
    "over": "gt",
    "above": "gt",
    "at least": "gte",
    "less than": "lt",
    "under": "lt",
    "below": "lt",
    "at most": "lte",
    "contains": "contains",
    "starts with": "startswith",
    "ends with": "endswith",
}
_COLUMN = r"[A-Za-z_][A-Za-z0-9_.$]*"


@dataclass(frozen=True)
class TransferDataRules:
    """What the operator asked for, split into applied controls and questions."""

    source_filter: dict[str, Any] = field(default_factory=dict)
    upsert_key: str = ""
    dedupe_key: str = ""
    limit: int = 0
    cadence: str = ""
    #: Rules that were understood but cannot be applied from chat. Each entry is
    #: a full sentence for the operator — a request for the missing detail, or a
    #: statement that the control lives in Transfer Studio.
    questions: tuple[str, ...] = ()
    #: Human-readable echo of every rule that *was* applied, for the preview.
    applied: tuple[str, ...] = ()

    @property
    def empty(self) -> bool:
        return not (
            self.source_filter
            or self.upsert_key
            or self.dedupe_key
            or self.limit
            or self.cadence
            or self.questions
        )

    @property
    def blocking(self) -> bool:
        """True when staging a run would ignore something the operator asked for."""
        return bool(self.questions)

    def as_intent_fields(self) -> dict[str, Any]:
        """Slots to merge into a parsed transfer intent (empty values omitted)."""
        out: dict[str, Any] = {}
        if self.source_filter:
            out["source_filter"] = self.source_filter
        if self.upsert_key:
            out["upsert_key"] = self.upsert_key
        if self.dedupe_key:
            out["dedupe_key"] = self.dedupe_key
        if self.limit:
            out["limit"] = self.limit
        if self.cadence:
            out["cadence"] = self.cadence
        if self.questions:
            out["rule_questions"] = list(self.questions)
        if self.applied:
            out["applied_rules"] = list(self.applied)
        return out


def _clean_value(raw: str) -> str:
    return str(raw or "").strip().strip("\"'").strip().rstrip(".")


def _predicate(column: str, op: str, value: Any, *, negate: bool = False) -> dict[str, Any]:
    if negate:
        op = _NEGATE.get(op, op)
    spec: dict[str, Any] = {"column": column, "operator": op}
    if op not in {"is_null", "is_not_null"}:
        spec["value"] = value
    return spec


def _describe(spec: dict[str, Any]) -> str:
    op = spec.get("operator")
    col = spec.get("column")
    if op == "is_null":
        return f"{col} is null"
    if op == "is_not_null":
        return f"{col} is not null"
    value = spec.get("value")
    shown = ", ".join(str(v) for v in value) if isinstance(value, list) else str(value)
    words = {
        "eq": "=", "ne": "!=", "gt": ">", "gte": ">=", "lt": "<", "lte": "<=",
        "in": "in", "not_in": "not in", "contains": "contains",
        "startswith": "starts with", "endswith": "ends with", "regex": "matches",
    }
    return f"{col} {words.get(str(op), str(op))} {shown}"


def _parse_condition(text: str, *, negate: bool = False) -> dict[str, Any] | None:
    """Parse one comparison — the only shapes the row filter can honour."""
    cond = str(text or "").strip().rstrip(".;,")
    if not cond:
        return None

    m = re.match(rf"^({_COLUMN})\s+is\s+not\s+(?:null|empty|blank)$", cond, re.I)
    if m:
        return _predicate(m.group(1), "is_not_null", None, negate=negate)
    m = re.match(rf"^({_COLUMN})\s+is\s+(?:null|empty|blank)$", cond, re.I)
    if m:
        return _predicate(m.group(1), "is_null", None, negate=negate)
    m = re.match(rf"^({_COLUMN})\s+(?:is\s+)?not\s+in\s*\(([^)]*)\)$", cond, re.I)
    if m:
        values = [_clean_value(v) for v in m.group(2).split(",") if _clean_value(v)]
        return _predicate(m.group(1), "not_in", values, negate=negate) if values else None
    m = re.match(rf"^({_COLUMN})\s+in\s*\(([^)]*)\)$", cond, re.I)
    if m:
        values = [_clean_value(v) for v in m.group(2).split(",") if _clean_value(v)]
        return _predicate(m.group(1), "in", values, negate=negate) if values else None
    m = re.match(rf"^({_COLUMN})\s*(=|==|!=|<>|>=|<=|>|<)\s*(.+)$", cond, re.I)
    if m:
        op = _SYMBOL_OPS[m.group(2)]
        return _predicate(m.group(1), op, _clean_value(m.group(3)), negate=negate)
    # Word operators, longest phrase first so "is not" beats "is".
    for phrase in sorted(_WORD_OPS, key=len, reverse=True):
        m = re.match(rf"^({_COLUMN})\s+{re.escape(phrase)}\s+(.+)$", cond, re.I)
        if m:
            return _predicate(
                m.group(1), _WORD_OPS[phrase], _clean_value(m.group(2)), negate=negate
            )
    return None


# A condition clause may carry commas inside a value list — "country in (US, CA)"
# is one condition, not two. Splitting on every comma is how half of it got lost.
_CONDITION = r"(?:[^,;()]|\([^)]*\))+"
_CONJUNCTION_RE = re.compile(r"\s+(and|or)\s+", re.IGNORECASE)


def _split_conjuncts(clause: str) -> tuple[list[str], str]:
    """Split "a = 1 and b > 2" into its parts and the joiner used.

    A clause mixing ``and`` with ``or`` has no unambiguous precedence in plain
    English, so it is returned as a single unparsable part rather than guessed.
    """
    joiners = {j.lower() for j in _CONJUNCTION_RE.findall(clause or "")}
    if not joiners:
        return [clause], "and"
    if len(joiners) > 1:
        return [clause], ""
    joiner = joiners.pop()
    parts = [p for p in _CONJUNCTION_RE.split(clause) if p.lower() not in {"and", "or"}]
    return [p for p in parts if p.strip()], joiner


_CADENCE_RE = re.compile(
    r"\b(nightly|every\s+night|each\s+night|daily|every\s+day|hourly|every\s+hour"
    r"|weekly|every\s+week|monthly"
    r"|every\s+\d+\s+(?:minutes?|mins?|hours?|days?))\b",
    re.IGNORECASE,
)
_LIMIT_RE = re.compile(
    r"\b(?:only\s+the\s+first|first|top|limit(?:\s+to)?|just\s+the\s+first)\s+"
    r"(\d[\d,]*)\s*(?:rows?|records?|documents?)?\b",
    re.IGNORECASE,
)
_UPSERT_KEY_RE = re.compile(
    rf"\b(?:upsert|merge)(?:ing)?\s+(?:on|by|using|with|key(?:ed)?(?:\s+on)?)\s+"
    rf"(?:the\s+)?(?:column\s+)?({_COLUMN})",
    re.IGNORECASE,
)
_DEDUPE_KEY_RE = re.compile(
    rf"\b(?:dedupe|dedup|deduplicate|de-?duplicate|drop\s+duplicates?|"
    rf"remove\s+duplicates?)\s*(?:rows?\s*)?(?:on|by|using)\s+"
    rf"(?:the\s+)?(?:column\s+)?({_COLUMN})",
    re.IGNORECASE,
)
_ROWS_WHERE_RE = re.compile(
    r"\b(?P<neg>exclude|excluding|skip|skipping|ignore|ignoring|drop|without)?\s*"
    r"(?:only\s+|just\s+|keep\s+(?:only\s+)?)?"
    r"(?:the\s+)?(?:rows?|records?|documents?)?\s*"
    rf"(?:where|with|having|that\s+have)\s+(?P<cond>{_CONDITION})",
    re.IGNORECASE,
)
_BARE_WHERE_RE = re.compile(rf"\bwhere\s+(?P<cond>{_CONDITION})", re.IGNORECASE)
_SKIP_NULLS_BARE_RE = re.compile(
    r"\b(?:skip|ignore|drop|exclude|no)\s+(?:the\s+)?(?:rows?\s+with\s+)?"
    r"(?:null|nulls|empty|blank)s?(?:\s+values?)?\b",
    re.IGNORECASE,
)
_SKIP_NULLS_COL_RE = re.compile(
    rf"\b(?:skip|ignore|drop|exclude)\s+(?:rows?\s+)?(?:with\s+|where\s+)?"
    rf"(?:null|nulls|empty|blank)s?\s+(?:values?\s+)?(?:in|on|for)\s+"
    rf"(?:the\s+)?(?:column\s+)?({_COLUMN})",
    re.IGNORECASE,
)
_NOT_NULL_COL_RE = re.compile(
    rf"\b({_COLUMN})\s+(?:must\s+not\s+be|cannot\s+be|is\s+not)\s+(?:null|empty|blank)\b",
    re.IGNORECASE,
)
# Recognised, and honestly out of reach from chat: these are Map-step controls.
_STUDIO_ONLY_RULES: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(rf"\b(?:mask|redact|obfuscate|anonymi[sz]e)\s+(?:the\s+)?({_COLUMN})", re.I),
        'Masking “{0}” is a Map-step transform — chat cannot apply it. '
        "Open Transfer Studio ▸ Map to set it, then run from there.",
    ),
    (
        re.compile(rf"\b(?:hash|sha256|md5)\s+(?:the\s+)?({_COLUMN})", re.I),
        'Hashing “{0}” is a Map-step transform — chat cannot apply it. '
        "Set it in Transfer Studio ▸ Map.",
    ),
    (
        re.compile(rf"\brename\s+({_COLUMN})\s+to\s+({_COLUMN})", re.I),
        'Renaming “{0}” to “{1}” is a mapping edit — set it in Transfer Studio ▸ Map '
        "so the change is reviewed against the destination DDL.",
    ),
    (
        re.compile(rf"\bconvert\s+({_COLUMN})\s+to\s+([A-Za-z][\w()., ]{{1,30}})", re.I),
        'Converting “{0}” to {1} changes the DDL contract — do it in '
        "Transfer Studio ▸ Map, where the lossy-type check can see it.",
    ),
    (
        re.compile(r"\b(?:trim|uppercase|lowercase|normali[sz]e)\s+(?:the\s+)?(\w+)", re.I),
        'Transforming “{0}” is a Map-step control — chat cannot apply it.',
    ),
)
# "keep only active rows" names a business state, not a column. Guessing which
# column carries it (status? is_active? state?) would silently move the wrong
# rows, so ask.
_VAGUE_STATE_RE = re.compile(
    r"\b(?:only|keep|just)\s+(?:the\s+)?(active|inactive|valid|invalid|open|closed|"
    r"current|live|new|recent|latest|clean|good)\s+(?:rows?|records?|ones?|data)\b",
    re.IGNORECASE,
)
_RULES_PREFIX_RE = re.compile(
    r"\b(?:by\s+)?(?:following|follow|apply(?:ing)?|using|with|per|obey(?:ing)?|as\s+per)\s+"
    r"(?:these|the|below|following)?\s*(?:data\s+|migration\s+|business\s+)?rules?\s*[:\-]?\s*",
    re.IGNORECASE,
)


# "with rows where amount > 100" — the clause names its own subject again, and
# the introducer alternation already consumed the first word, so the tail read
# verbatim made a plain predicate unparsable and refused the transfer.
_COND_PREAMBLE_RE = re.compile(
    r"^(?:the\s+)?(?:rows?|records?|documents?)\s+(?:where|having|with|that\s+have)\s+",
    re.IGNORECASE,
)


def _condition_text(cond: str) -> str:
    """The comparison inside a condition clause, without a repeated subject."""
    return _COND_PREAMBLE_RE.sub("", str(cond or "").strip(), count=1)


def _names_sync_mode(cond: str) -> bool:
    """Is this "with …" clause the sync mode rather than a row condition?

    ``_ROWS_WHERE_RE`` accepts ``with`` as a condition introducer, so "…to
    Snowflake with overwrite" read *overwrite* as a predicate: unparsable, so the
    request came back as a question, and the word was cut out of the route text
    before the route parser could read the mode. The operator asked for an
    overwrite and got a refusal. Mode vocabulary has one owner
    (``transfer_tools.sync_mode_from_phrase``); a clause it recognises is left in
    the route text for the route parser and raises no question.
    """
    from .transfer_tools import sync_mode_from_phrase

    text = _condition_text(cond).strip(".;,")
    # A real predicate wins: a column literally named "append" still compares.
    if not text or len(text.split()) > 4 or _parse_condition(text) is not None:
        return False
    return bool(sync_mode_from_phrase(text, default=""))


def _merged(spans: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Overlapping cuts merged, ordered back to front so indexes stay valid.

    Two rules can claim overlapping words ("every monday" and "monday"); cutting
    both separately would splice the route text at stale offsets.
    """
    merged: list[tuple[int, int]] = []
    for start, end in sorted(set(spans)):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return list(reversed(merged))


def parse_transfer_data_rules(message: str) -> tuple[str, TransferDataRules]:
    """Split a transfer request into its route text and its data rules.

    Returns ``(route_text, rules)``. ``route_text`` has every recognised rule
    clause removed so the route parser sees "move users from A to B" and not a
    connector called "B, only rows where status = active".
    """
    text = str(message or "")
    filters: list[dict[str, Any]] = []
    questions: list[str] = []
    applied: list[str] = []
    upsert_key = ""
    dedupe_key = ""
    limit = 0
    cadence = ""
    # Spans removed from the route text, collected then cut back-to-front.
    cuts: list[tuple[int, int]] = []

    def cut(match: re.Match[str]) -> None:
        cuts.append((match.start(), match.end()))

    hit = _RULES_PREFIX_RE.search(text)
    if hit:
        cut(hit)

    for pattern, template in _STUDIO_ONLY_RULES:
        for m in pattern.finditer(text):
            questions.append(template.format(*[g or "" for g in m.groups()]))
            cut(m)

    for m in _CADENCE_RE.finditer(text):
        cadence = cadence or m.group(1).strip().lower()
        cut(m)
    if cadence:
        # The time, weekday and zone that qualify the cadence travel with it —
        # "nightly at 2am IST" is one instruction, and leaving the tail in the
        # route text would name a connector "Warehouse at 2am IST".
        for start, end in cadence_qualifiers(text):
            cadence = f"{cadence} {text[start:end].strip()}".strip()
            cuts.append((start, end))

    for m in _LIMIT_RE.finditer(text):
        limit = _utterance_row_limit(m.group(1))
        if limit:
            applied.append(f"first {limit} rows only")
        cut(m)

    for m in _UPSERT_KEY_RE.finditer(text):
        upsert_key = upsert_key or m.group(1)
        cut(m)
    for m in _DEDUPE_KEY_RE.finditer(text):
        dedupe_key = dedupe_key or m.group(1)
        cut(m)

    for m in _SKIP_NULLS_COL_RE.finditer(text):
        filters.append(_predicate(m.group(1), "is_not_null", None))
        cut(m)
    for m in _NOT_NULL_COL_RE.finditer(text):
        filters.append(_predicate(m.group(1), "is_not_null", None))
        cut(m)

    where_hits = list(_ROWS_WHERE_RE.finditer(text)) or list(_BARE_WHERE_RE.finditer(text))
    for m in where_hits:
        if _names_sync_mode(m.group("cond")):
            continue
        negate = bool(m.groupdict().get("neg"))
        parts, joiner = _split_conjuncts(_condition_text(m.group("cond")))
        specs = [_parse_condition(part, negate=negate) for part in parts]
        if not joiner or not all(specs):
            # Read but not understood. Applying the half we parsed would move
            # rows the operator excluded, so ask instead.
            questions.append(
                f"I could not read the condition “{m.group('cond').strip()}” well "
                "enough to apply it exactly. Restate it as simple comparisons, "
                "e.g. “where status = active and amount > 100”."
            )
            cut(m)
            continue
        if joiner == "or" and len(specs) > 1:
            or_group = {"or": [s for s in specs if s]}
            filters.append(or_group)
        else:
            filters.extend(s for s in specs if s)
        cut(m)

    for m in _VAGUE_STATE_RE.finditer(text):
        state = m.group(1).lower()
        questions.append(
            f"“{state} rows” names a business state, not a column — I will not guess "
            f"which one carries it. Tell me the predicate (for example "
            f"“where status = {state}”) and I will apply it exactly."
        )
        cut(m)

    if not filters and _SKIP_NULLS_BARE_RE.search(text):
        m = _SKIP_NULLS_BARE_RE.search(text)
        questions.append(
            "“Skip nulls” needs a column — dropping every row with a null anywhere "
            "would silently discard good rows. Name the column, e.g. "
            "“skip nulls in email”."
        )
        if m:
            cut(m)

    source_filter: dict[str, Any] = {}
    if len(filters) == 1:
        source_filter = filters[0]
    elif filters:
        source_filter = {"and": filters}
    for spec in filters:
        applied.append(_describe(spec))

    if upsert_key and dedupe_key and upsert_key.lower() != dedupe_key.lower():
        questions.append(
            f"You asked to upsert on {upsert_key} and dedupe on {dedupe_key}. "
            "An upsert has one key — say which column decides identity."
        )
    elif upsert_key:
        applied.append(f"upsert keyed on {upsert_key}")
    elif dedupe_key:
        applied.append(f"upsert (dedupe) keyed on {dedupe_key}")

    route = text
    for start, end in _merged(cuts):
        route = f"{route[:start]} {route[end:]}"
    route = re.sub(r"\s*,\s*(?=,|$)", "", route)
    route = re.sub(r"[,;]\s*$", "", route.strip())
    route = re.sub(r"\s+", " ", route).strip().strip(",;").strip()

    return route, TransferDataRules(
        source_filter=source_filter,
        upsert_key=upsert_key,
        dedupe_key=dedupe_key,
        limit=limit,
        cadence=cadence,
        questions=tuple(questions),
        applied=tuple(applied),
    )


def filter_columns(spec: dict[str, Any] | None) -> list[str]:
    """Every column a filter spec reads, so the caller can ground it in the source."""
    out: list[str] = []
    node = spec or {}
    for key in ("and", "or"):
        if key in node:
            for child in node.get(key) or []:
                out.extend(filter_columns(child))
            return out
    col = str(node.get("column") or node.get("field") or "").strip()
    if col:
        out.append(col)
    return out
