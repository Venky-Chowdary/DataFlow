"""Schema-grounded WHERE predicates for Data Pilot analytics.

"How many **open** orders", "revenue **in 2024**", "customers **where email is
null**" — an aggregate without filters answers a different question than the one
asked, so this module turns filter phrases into typed, bound predicates.

Design rules, taken from query-engine practice rather than convenience:

* **Bind, never interpolate.** Every literal leaves as a ``:p0`` parameter, so a
  value containing a quote is data, not syntax. Column and table names are still
  validated against the live schema and quoted per dialect.
* **Half-open date windows.** "in 2024" becomes ``>= 2024-01-01 AND < 2025-01-01``.
  ``BETWEEN`` is inclusive on both ends and silently pulls in the next day's
  midnight row; the half-open form is also the only one that stays correct across
  DST-length days.
* **No functions on the filtered column.** ``DATE(created_at) = '2024-01-01'``
  cannot use an index; a boundary range can. On a large fact table that is the
  difference between a second and a full scan.
* **Coerce against the real column type, or refuse.** Comparing a NUMERIC column
  to "abc" is a user error worth reporting, not a 500 from the driver — and in
  PostgreSQL ``text = 123`` is a hard type error, so the literal's carrier has to
  match the column's.

MongoDB gets the same predicates as a ``$match`` stage, with dates emitted as
Extended JSON ``{"$date": ...}`` so they arrive as BSON Dates instead of strings.
"""

from __future__ import annotations

import calendar
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

_MAX_PREDICATES = 8
_MAX_IN_VALUES = 50

# Operators we can express in both SQL and Mongo.
_SQL_OPS = {
    "eq": "=",
    "ne": "<>",
    "gt": ">",
    "gte": ">=",
    "lt": "<",
    "lte": "<=",
}

_MONGO_OPS = {
    "eq": "$eq",
    "ne": "$ne",
    "gt": "$gt",
    "gte": "$gte",
    "lt": "$lt",
    "lte": "$lte",
}

_NUMERIC_HINTS = (
    "int", "serial", "numeric", "decimal", "real", "double", "float",
    "money", "number", "bigint", "smallint",
)
_TEMPORAL_HINTS = ("date", "time", "timestamp")
_BOOLEAN_HINTS = ("bool",)

_TRUE_WORDS = frozenset({"true", "t", "yes", "y", "1", "on"})
_FALSE_WORDS = frozenset({"false", "f", "no", "n", "0", "off"})


class PredicateError(ValueError):
    """A filter that cannot be honoured — reported to the operator verbatim."""


@dataclass
class Predicate:
    """One grounded comparison, ready to render in SQL or Mongo."""

    column: str
    op: str
    values: list[Any] = field(default_factory=list)
    # Human phrase, echoed back so the operator can see what was applied.
    text: str = ""


@dataclass
class ParsedFilter:
    """A filter phrase before schema grounding."""

    column: str
    op: str
    raw_values: list[str] = field(default_factory=list)
    text: str = ""


# --------------------------------------------------------------------------
# Natural-language parsing
# --------------------------------------------------------------------------

_IDENT = r"[A-Za-z_][A-Za-z0-9_ ]{0,40}?"

# Where a filter value stops. Without "on"/"from"/"in" a trailing connector
# phrase ("... = open on Local Postgres") gets swallowed into the literal.
_END = (
    r"(?=\s+(?:and|or|group|grouped|by|per|order|ordered|on|from|in|for|with|"
    r"limit|top|bottom)\b|[.,;?!]|$)"
)

# Ordered: the most specific shape must win. Each pattern captures a column and
# whatever the comparison needs.
_FILTER_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(rf"\b({_IDENT})\s+is\s+not\s+(?:null|empty|blank)\b", re.I), "not_null"),
    (re.compile(rf"\b({_IDENT})\s+is\s+(?:null|empty|blank)\b", re.I), "is_null"),
    (re.compile(rf"\b({_IDENT})\s+(?:is\s+)?not\s+in\s*\(([^)]{{1,300}})\)", re.I), "not_in"),
    (re.compile(rf"\b({_IDENT})\s+(?:is\s+)?in\s*\(([^)]{{1,300}})\)", re.I), "in"),
    (
        re.compile(
            rf"\b({_IDENT})\s+(?:contains|like|starts?\s+with|ends?\s+with)\s+"
            rf"['\"]?([^'\"\n,;]{{1,80}}?)['\"]?{_END}",
            re.I,
        ),
        "contains",
    ),
    (
        re.compile(
            rf"\b({_IDENT})\s*(>=|<=|!=|<>|>|=|<)\s*['\"]?([^'\"\n,;]{{1,80}}?)['\"]?{_END}",
            re.I,
        ),
        "cmp",
    ),
    (
        re.compile(
            rf"\b({_IDENT})\s+(?:is\s+)?(?:at\s+least|greater\s+than\s+or\s+equal\s+to|"
            r"no\s+less\s+than)\s+([^\s,;]{1,40})",
            re.I,
        ),
        "gte",
    ),
    (
        re.compile(
            rf"\b({_IDENT})\s+(?:is\s+)?(?:at\s+most|less\s+than\s+or\s+equal\s+to|"
            r"no\s+more\s+than)\s+([^\s,;]{1,40})",
            re.I,
        ),
        "lte",
    ),
    (
        re.compile(
            rf"\b({_IDENT})\s+(?:is\s+)?(?:greater\s+than|more\s+than|over|above|exceeds?)\s+"
            r"([^\s,;]{1,40})",
            re.I,
        ),
        "gt",
    ),
    (
        re.compile(
            rf"\b({_IDENT})\s+(?:is\s+)?(?:less\s+than|under|below|fewer\s+than)\s+"
            r"([^\s,;]{1,40})",
            re.I,
        ),
        "lt",
    ),
    (
        re.compile(
            rf"\b({_IDENT})\s+(?:is|are|equals?|=)\s+['\"]?([A-Za-z0-9_.@\-+]{{1,60}})['\"]?{_END}",
            re.I,
        ),
        "eq",
    ),
)

# Clause words that separate a column name from the words in front of it. The
# capture is deliberately loose, so "orders where status" must collapse to
# "status" before grounding — otherwise a valid filter is rejected as an
# unknown column.
_CLAUSE_SPLIT = re.compile(
    r"\b(?:where|with|having|and|or|only|for|that|which|who|whose|"
    r"has|have|had|filtered\s+(?:by|on|to))\b",
    re.I,
)

# Phrases that introduce a filter clause; text after them is scanned.
_WHERE_LEAD = re.compile(
    r"\b(?:where|with|having|filtered\s+(?:by|on|to)|only\s+(?:for|the)?|"
    r"for\s+(?:which|those)|that\s+(?:have|has|are|is))\b",
    re.I,
)

# Words that can never be a column name in a filter phrase.
_NON_COLUMN_WORDS = frozenset({
    "the", "a", "an", "there", "it", "they", "them", "this", "that", "these",
    "those", "value", "row", "rows", "record", "records", "count", "total",
    "sum", "average", "avg", "group", "order", "and", "or", "not", "me", "my",
    "all", "any", "each", "every", "how", "many", "much", "what", "which",
})


def _clean_column(raw: str) -> str:
    token = (raw or "").strip().strip("\"'`").strip()
    parts = [p for p in _CLAUSE_SPLIT.split(token) if p.strip()]
    if parts:
        token = parts[-1].strip()
    token = re.sub(r"^(?:the|a|an|its|their|my|our)\s+", "", token, flags=re.I)
    token = re.sub(r"\s+(?:is|are|was|were)$", "", token, flags=re.I).strip()
    # Drop leading question/metric nouns ("how many rows in orders email").
    words = token.split()
    while len(words) > 1 and words[0].lower() in _NON_COLUMN_WORDS:
        words = words[1:]
    return " ".join(words)


def _split_list(raw: str) -> list[str]:
    parts = [p.strip().strip("\"'`").strip() for p in re.split(r"[,;]|\bor\b", raw or "", flags=re.I)]
    return [p for p in parts if p][:_MAX_IN_VALUES]


def parse_filters(message: str) -> tuple[list[ParsedFilter], str]:
    """Extract filter phrases; return them plus the message with them removed.

    Removing the matched spans matters: the leftover text is what the metric and
    table parser sees, so "count of orders where status = open" must not leave
    "status = open" behind to be mistaken for a table name.
    """
    text = (message or "").strip()
    if not text:
        return [], message

    found: list[ParsedFilter] = []
    spans: list[tuple[int, int]] = []

    for pattern, kind in _FILTER_PATTERNS:
        for match in pattern.finditer(text):
            column = _clean_column(match.group(1))
            if not column or column.lower() in _NON_COLUMN_WORDS:
                continue
            if len(column.split()) > 3:
                continue

            # The column capture is loose and may have swallowed the metric and
            # table ("how many orders where status"). Only the part from the real
            # column onward is the filter, so only that is removed — otherwise
            # stripping the match would delete the question itself.
            head = match.group(0)
            offset = head.lower().rfind(column.lower())
            start = match.start() + (offset if offset > 0 else 0)
            end = match.end()
            if any(s < end and start < e for s, e in spans):
                continue

            parsed = _build_parsed(kind, column, match, text[start:end].strip())
            if parsed is None:
                continue
            found.append(parsed)
            spans.append((start, end))
            if len(found) >= _MAX_PREDICATES:
                break
        if len(found) >= _MAX_PREDICATES:
            break

    remainder = text
    for start, end in sorted(spans, reverse=True):
        remainder = remainder[:start] + " " + remainder[end:]
    remainder = _WHERE_LEAD.sub(" ", remainder)
    remainder = re.sub(r"\s{2,}", " ", remainder).strip(" ,;")
    return found, remainder


def _build_parsed(
    kind: str,
    column: str,
    match: re.Match[str],
    phrase: str,
) -> ParsedFilter | None:
    if kind in ("is_null", "not_null"):
        return ParsedFilter(column=column, op=kind, text=phrase)
    if kind in ("in", "not_in"):
        values = _split_list(match.group(2))
        if not values:
            return None
        return ParsedFilter(column=column, op=kind, raw_values=values, text=phrase)
    if kind == "contains":
        value = (match.group(2) or "").strip()
        if not value:
            return None
        lowered = phrase.lower()
        op = "starts_with" if "start" in lowered else "ends_with" if "end" in lowered else "contains"
        return ParsedFilter(column=column, op=op, raw_values=[value], text=phrase)
    if kind == "cmp":
        symbol = match.group(2)
        value = (match.group(3) or "").strip()
        op = {
            "=": "eq", "!=": "ne", "<>": "ne",
            ">": "gt", ">=": "gte", "<": "lt", "<=": "lte",
        }.get(symbol, "")
        if not op or not value:
            return None
        return ParsedFilter(column=column, op=op, raw_values=[value], text=phrase)
    value = (match.group(2) or "").strip()
    if not value:
        return None
    return ParsedFilter(column=column, op=kind, raw_values=[value], text=phrase)


# --------------------------------------------------------------------------
# Relative and calendar date windows
# --------------------------------------------------------------------------

_RELATIVE_WINDOW = re.compile(
    r"\b(?:in|during|over)?\s*(?:the\s+)?(?:last|past|previous)\s+(\d{1,4})\s+"
    r"(day|days|week|weeks|month|months|quarter|quarters|year|years)\b",
    re.I,
)
_NAMED_WINDOW = re.compile(
    r"\b(today|yesterday|this\s+week|this\s+month|this\s+quarter|this\s+year|"
    r"last\s+week|last\s+month|last\s+quarter|last\s+year)\b",
    re.I,
)
_YEAR_WINDOW = re.compile(r"\b(?:in|during|for)\s+(19|20)(\d{2})\b", re.I)
_MONTH_NAMES = {
    m.lower(): i
    for i, m in enumerate(calendar.month_name)
    if m
}
_MONTH_ABBR = {
    m.lower(): i
    for i, m in enumerate(calendar.month_abbr)
    if m
}
_MONTH_WINDOW = re.compile(
    r"\b(?:in|during|for)\s+([A-Za-z]{3,9})\s+((?:19|20)\d{2})\b", re.I
)


def _month_start(anchor: date, months_back: int) -> date:
    year = anchor.year
    month = anchor.month - months_back
    while month <= 0:
        month += 12
        year -= 1
    return date(year, month, 1)


def _add_months(start: date, months: int) -> date:
    year = start.year + (start.month - 1 + months) // 12
    month = (start.month - 1 + months) % 12 + 1
    return date(year, month, 1)


def parse_time_window(message: str, now: datetime | None = None) -> tuple[date, date, str] | None:
    """Resolve a relative/calendar phrase into a half-open ``[start, end)`` window.

    The end is always exclusive. "in 2024" is ``[2024-01-01, 2025-01-01)`` — never
    ``BETWEEN '2024-01-01' AND '2024-12-31'``, which drops everything timestamped
    after midnight on the last day.
    """
    text = (message or "").strip()
    if not text:
        return None
    anchor = (now or datetime.now(timezone.utc)).date()

    month_named = _MONTH_WINDOW.search(text)
    if month_named:
        name = month_named.group(1).lower()
        month = _MONTH_NAMES.get(name) or _MONTH_ABBR.get(name)
        if month:
            start = date(int(month_named.group(2)), month, 1)
            return start, _add_months(start, 1), month_named.group(0).strip()

    year = _YEAR_WINDOW.search(text)
    if year:
        y = int(f"{year.group(1)}{year.group(2)}")
        return date(y, 1, 1), date(y + 1, 1, 1), year.group(0).strip()

    rel = _RELATIVE_WINDOW.search(text)
    if rel:
        n = max(1, int(rel.group(1)))
        unit = rel.group(2).lower().rstrip("s")
        end = anchor + timedelta(days=1)  # include everything up to end of today
        if unit == "day":
            return anchor - timedelta(days=n - 1), end, rel.group(0).strip()
        if unit == "week":
            return anchor - timedelta(weeks=n) + timedelta(days=1), end, rel.group(0).strip()
        if unit == "month":
            return _month_start(anchor, n - 1), end, rel.group(0).strip()
        if unit == "quarter":
            return _month_start(anchor, 3 * n - 1), end, rel.group(0).strip()
        if unit == "year":
            return date(anchor.year - n + 1, 1, 1), end, rel.group(0).strip()

    named = _NAMED_WINDOW.search(text)
    if named:
        phrase = re.sub(r"\s+", " ", named.group(1).strip().lower())
        if phrase == "today":
            return anchor, anchor + timedelta(days=1), "today"
        if phrase == "yesterday":
            return anchor - timedelta(days=1), anchor, "yesterday"
        if phrase == "this week":
            start = anchor - timedelta(days=anchor.weekday())
            return start, start + timedelta(days=7), "this week"
        if phrase == "last week":
            start = anchor - timedelta(days=anchor.weekday() + 7)
            return start, start + timedelta(days=7), "last week"
        if phrase == "this month":
            start = date(anchor.year, anchor.month, 1)
            return start, _add_months(start, 1), "this month"
        if phrase == "last month":
            start = _month_start(anchor, 1)
            return start, _add_months(start, 1), "last month"
        if phrase == "this quarter":
            start = date(anchor.year, 3 * ((anchor.month - 1) // 3) + 1, 1)
            return start, _add_months(start, 3), "this quarter"
        if phrase == "last quarter":
            start = _add_months(date(anchor.year, 3 * ((anchor.month - 1) // 3) + 1, 1), -3)
            return start, _add_months(start, 3), "last quarter"
        if phrase == "this year":
            return date(anchor.year, 1, 1), date(anchor.year + 1, 1, 1), "this year"
        if phrase == "last year":
            return date(anchor.year - 1, 1, 1), date(anchor.year, 1, 1), "last year"
    return None


# --------------------------------------------------------------------------
# Type-aware grounding
# --------------------------------------------------------------------------


def _kind_of(col_type: str) -> str:
    low = (col_type or "").lower()
    if any(h in low for h in _BOOLEAN_HINTS):
        return "boolean"
    if any(h in low for h in _TEMPORAL_HINTS):
        return "temporal"
    if any(h in low for h in _NUMERIC_HINTS):
        return "numeric"
    return "text"


def _coerce(value: str, kind: str, column: str) -> Any:
    """Convert a spoken literal to the column's carrier, or refuse honestly."""
    raw = (value or "").strip().strip("\"'`")
    if kind == "numeric":
        cleaned = raw.replace(",", "").replace("$", "").replace("%", "").strip()
        try:
            if re.fullmatch(r"[-+]?\d+", cleaned):
                return int(cleaned)
            return float(cleaned)
        except ValueError:
            raise PredicateError(
                f"`{column}` is numeric, so it can't be compared to “{raw}”."
            ) from None
    if kind == "boolean":
        low = raw.lower()
        if low in _TRUE_WORDS:
            return True
        if low in _FALSE_WORDS:
            return False
        raise PredicateError(
            f"`{column}` is boolean — use true/false, not “{raw}”."
        )
    if kind == "temporal":
        parsed = _parse_date_literal(raw)
        if parsed is None:
            raise PredicateError(
                f"`{column}` is a date column — “{raw}” is not a date I can read. "
                "Try 2024-01-31, “in 2024”, or “last 30 days”."
            )
        return parsed
    return raw


def _parse_date_literal(raw: str) -> date | None:
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%m/%d/%Y", "%Y-%m", "%Y"):
        try:
            parsed = datetime.strptime(raw, fmt)
        except ValueError:
            continue
        return parsed.date()
    return None


def ground_filters(
    parsed: list[ParsedFilter],
    columns: list[dict[str, Any]],
    resolve_column,
    column_type,
) -> list[Predicate]:
    """Bind each filter phrase to a real column and coerce its literals.

    ``resolve_column`` / ``column_type`` are injected so this module stays free of
    the aggregation tool's schema helpers — one resolver, no second copy.
    """
    names = [str(c.get("name") or "") for c in columns if c.get("name")]
    out: list[Predicate] = []
    for pf in parsed:
        real = resolve_column(pf.column, names)
        if not real:
            # A filter we cannot ground is never silently dropped — dropping it
            # would answer a broader question than the operator asked.
            raise PredicateError(
                f"Column '{pf.column}' is not in this table. "
                f"Available columns: {', '.join(names[:20])}."
            )
        kind = _kind_of(column_type(columns, real))

        if pf.op in ("is_null", "not_null"):
            out.append(Predicate(column=real, op=pf.op, text=pf.text))
            continue

        if pf.op in ("contains", "starts_with", "ends_with"):
            if kind != "text":
                raise PredicateError(
                    f"`{real}` is {kind}, so a text match doesn't apply. "
                    "Use =, >, or a range instead."
                )
            out.append(
                Predicate(column=real, op=pf.op, values=[pf.raw_values[0]], text=pf.text)
            )
            continue

        if pf.op in ("in", "not_in"):
            values = [_coerce(v, kind, real) for v in pf.raw_values]
            out.append(Predicate(column=real, op=pf.op, values=values, text=pf.text))
            continue

        value = _coerce(pf.raw_values[0], kind, real)
        if kind == "temporal" and pf.op == "eq":
            # An equality on a date column means "that whole day", which is a
            # half-open range — matching a DATE literal against a TIMESTAMP
            # column would otherwise only hit exact midnight.
            out.append(
                Predicate(
                    column=real,
                    op="range",
                    values=[value, value + timedelta(days=1)],
                    text=pf.text,
                )
            )
            continue
        out.append(Predicate(column=real, op=pf.op, values=[value], text=pf.text))
    return out


def temporal_window_predicate(
    window: tuple[date, date, str],
    columns: list[dict[str, Any]],
    column_type,
    preferred: str = "",
) -> Predicate | None:
    """Attach "in 2024" / "last 30 days" to the table's date column."""
    start, end, phrase = window
    names = [str(c.get("name") or "") for c in columns if c.get("name")]
    temporal = [n for n in names if _kind_of(column_type(columns, n)) == "temporal"]
    if not temporal:
        return None
    chosen = preferred if preferred in temporal else ""
    if not chosen:
        if len(temporal) > 1:
            raise PredicateError(
                f"Which date column should “{phrase}” apply to? "
                f"This table has: {', '.join(temporal[:10])}."
            )
        chosen = temporal[0]
    return Predicate(column=chosen, op="range", values=[start, end], text=phrase)


# --------------------------------------------------------------------------
# Rendering
# --------------------------------------------------------------------------


def to_sql(
    predicates: list[Predicate],
    quote,
    dialect: str,
    prefix: str = "f",
) -> tuple[str, dict[str, Any]]:
    """Render a WHERE body plus its bind parameters. Values are never inlined."""
    if not predicates:
        return "", {}
    clauses: list[str] = []
    params: dict[str, Any] = {}
    counter = 0

    def bind(value: Any) -> str:
        nonlocal counter
        key = f"{prefix}{counter}"
        counter += 1
        params[key] = value
        return f":{key}"

    for pred in predicates:
        col = quote(pred.column, dialect)
        if pred.op == "is_null":
            clauses.append(f"{col} IS NULL")
        elif pred.op == "not_null":
            clauses.append(f"{col} IS NOT NULL")
        elif pred.op == "range":
            # Half-open: inclusive start, exclusive end.
            lo, hi = pred.values[0], pred.values[1]
            clauses.append(f"({col} >= {bind(lo)} AND {col} < {bind(hi)})")
        elif pred.op in ("in", "not_in"):
            placeholders = ", ".join(bind(v) for v in pred.values)
            keyword = "IN" if pred.op == "in" else "NOT IN"
            clauses.append(f"{col} {keyword} ({placeholders})")
        elif pred.op in ("contains", "starts_with", "ends_with"):
            needle = str(pred.values[0]).replace("%", r"\%").replace("_", r"\_")
            pattern = {
                "contains": f"%{needle}%",
                "starts_with": f"{needle}%",
                "ends_with": f"%{needle}",
            }[pred.op]
            # LIKE on the raw column keeps the predicate index-eligible for the
            # prefix case; LOWER() on both sides would forfeit that.
            clauses.append(f"{col} LIKE {bind(pattern)}")
        else:
            clauses.append(f"{col} {_SQL_OPS[pred.op]} {bind(pred.values[0])}")
    return " AND ".join(clauses), params


def _mongo_value(value: Any) -> Any:
    """Emit Extended JSON so dates survive JSON transport as BSON Dates."""
    if isinstance(value, datetime):
        return {"$date": value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")}
    if isinstance(value, date):
        return {"$date": datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")}
    return value


def to_mongo_match(predicates: list[Predicate]) -> dict[str, Any]:
    """Render the same predicates as a ``$match`` filter document."""
    if not predicates:
        return {}
    match: dict[str, Any] = {}

    def merge(column: str, expr: dict[str, Any]) -> None:
        current = match.get(column)
        if isinstance(current, dict):
            current.update(expr)
        else:
            match[column] = expr

    for pred in predicates:
        col = pred.column
        if pred.op == "is_null":
            merge(col, {"$eq": None})
        elif pred.op == "not_null":
            merge(col, {"$ne": None})
        elif pred.op == "range":
            merge(col, {
                "$gte": _mongo_value(pred.values[0]),
                "$lt": _mongo_value(pred.values[1]),
            })
        elif pred.op in ("in", "not_in"):
            key = "$in" if pred.op == "in" else "$nin"
            merge(col, {key: [_mongo_value(v) for v in pred.values]})
        elif pred.op in ("contains", "starts_with", "ends_with"):
            needle = re.escape(str(pred.values[0]))
            pattern = {
                "contains": needle,
                "starts_with": f"^{needle}",
                "ends_with": f"{needle}$",
            }[pred.op]
            merge(col, {"$regex": pattern, "$options": "i"})
        else:
            merge(col, {_MONGO_OPS[pred.op]: _mongo_value(pred.values[0])})
    return match


def describe(predicates: list[Predicate]) -> str:
    """One-line echo of what was actually applied, for the operator's answer."""
    bits: list[str] = []
    for pred in predicates:
        if pred.op == "is_null":
            bits.append(f"`{pred.column}` is null")
        elif pred.op == "not_null":
            bits.append(f"`{pred.column}` is not null")
        elif pred.op == "range":
            bits.append(f"`{pred.column}` in [{pred.values[0]}, {pred.values[1]})")
        elif pred.op in ("in", "not_in"):
            joined = ", ".join(str(v) for v in pred.values[:6])
            more = "…" if len(pred.values) > 6 else ""
            bits.append(f"`{pred.column}` {'in' if pred.op == 'in' else 'not in'} ({joined}{more})")
        elif pred.op in ("contains", "starts_with", "ends_with"):
            bits.append(f"`{pred.column}` {pred.op.replace('_', ' ')} “{pred.values[0]}”")
        else:
            bits.append(f"`{pred.column}` {_SQL_OPS[pred.op]} {pred.values[0]}")
    return " and ".join(bits)
