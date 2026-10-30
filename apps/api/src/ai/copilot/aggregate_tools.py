"""Natural-language aggregation for Datawrap Pilot — real GROUP BY against live tables.

The pilot could sample and profile rows but had no way to answer the most common
analytics questions ("how many orders by status", "average price", "revenue by
month"). Those phrasings previously matched no tool at all, or fell through to a
documentation search, so the pilot looked broken for ordinary data work.

Everything here is grounded in the destination's **real** schema:

* the table and every column are resolved against introspected names, so a
  mis-heard word produces "column not found — here are the real ones" instead of
  invalid SQL or an invented answer;
* the metric comes from a fixed whitelist and identifiers are quoted per
  dialect, so no user text is ever interpolated into SQL as code;
* execution reuses ``query_router._run_query`` behind ``_is_safe_sql``, the same
  read-only path as the rest of the pilot;
* MongoDB gets a real ``$group`` pipeline (``$sum: 1`` for counts, ``$dateTrunc``
  for time buckets) rather than a client-side tally over a sample, so document
  stores return true totals like the SQL engines do;
* results are exact server-side aggregates — never extrapolated from a sample,
  and NULL groups are preserved rather than silently filtered away.
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any

from .query_tools import _quote_ident, _store_result, _tool_result
from .schema_tools import (
    AmbiguousConnectorError,
    _safe_connector,
)

_LOG = logging.getLogger(__name__)

_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$")
_DEFAULT_GROUP_LIMIT = 20
_MAX_GROUP_LIMIT = 200

# Metric → (SQL template, needs a measure column, default result alias)
_METRICS: dict[str, tuple[str, bool, str]] = {
    "count": ("COUNT(*)", False, "row_count"),
    "count_distinct": ("COUNT(DISTINCT {col})", True, "distinct_count"),
    "sum": ("SUM({col})", True, "total"),
    "avg": ("AVG({col})", True, "average"),
    "min": ("MIN({col})", True, "minimum"),
    "max": ("MAX({col})", True, "maximum"),
}

# Words that mean "the rows themselves", not a column to measure.
_ROW_WORDS = frozenset({"row", "rows", "record", "records", "entry", "entries", "item", "items"})

# Column types that cannot be summed or averaged. Numeric-looking text is still
# allowed through — only unambiguously non-numeric carriers are refused.
_NON_NUMERIC_HINTS = (
    "char", "text", "uuid", "json", "bool", "blob", "binary", "bytea",
    "date", "time", "interval", "xml", "enum", "array", "struct", "geography",
)

# SQLite stores exact money as TEXT (NUMERIC affinity would drop scale).
# SUM/AVG of those columns is a real total — the engine coerces numeric text.
_TEXT_MONEY_NAME = re.compile(
    r"(?:^|_)(?:amt|amount|price|total|qty|quantity|balance|fee|cost|rate|score|value)(?:_|$)",
    re.I,
)


def _sqlite_text_money(name: str, col_type: str) -> bool:
    low = (col_type or "").lower()
    if "text" not in low and "char" not in low:
        return False
    return bool(_TEXT_MONEY_NAME.search(name or ""))


def _absorb_bound_literal(where: str, parsed: Any, leftover: str) -> Any | None:
    """Bind a quote/semicolon tail as the comparison value.

    ``status = x'; DROP TABLE t; --`` is one literal. The comparison pattern
    stops at the quote, and refusing the tail never runs the read. Binding the
    whole remainder keeps the statement out of the SQL text. A second predicate
    (``OR '1'='1``) is not absorbed — that tail is still refused.
    """
    from .predicates import ParsedFilter

    tail = (leftover or "").strip()
    if not isinstance(parsed, ParsedFilter) or not tail:
        return None
    if re.search(r"\b(?:and|or)\b", tail, re.I):
        return None
    if not re.match(
        r"""['";]|--|DROP\b|DELETE\b|INSERT\b|UPDATE\b|ALTER\b|TRUNCATE\b""",
        tail,
        re.I,
    ):
        return None
    text = (where or "").strip()
    matched = re.match(
        rf"^{re.escape(parsed.column)}\s*(>=|<=|!=|<>|>|=|<)\s*(.+)$",
        text,
        re.I | re.S,
    )
    if not matched:
        return None
    literal = matched.group(2).strip()
    if not literal:
        return None
    return ParsedFilter(
        column=parsed.column,
        op=parsed.op,
        raw_values=[literal],
        text=text,
    )


def _unparsed_filter_warning(where: str, leftover: str) -> str:
    """Refuse a where-clause tail the predicate parser did not consume.

    ``STS = POSTED' OR '1'='1`` used to run as ``STS = POSTED`` and drop the
    rest (QA RT5-10). A partial predicate is not applied.
    """
    from .predicates import parse_time_window

    text = leftover or ""
    window = parse_time_window(where or "")
    if window is not None and window[2]:
        text = re.sub(re.escape(window[2]), " ", text, count=1, flags=re.I)
    text = re.sub(
        r"\b(?:and|or|where|with|the|a|an|please|thanks|in|on|for|of|by|"
        r"last|past|days?|weeks?|months?|years?)\b",
        " ",
        text,
        flags=re.I,
    )
    text = re.sub(r"[\s,;:.]+", " ", text).strip()
    if not text or not re.search(r"[A-Za-z_'\"=<>]|--", text):
        return ""
    fragment = (leftover or "").strip()
    return (
        "Refused the filter because this part is not a complete "
        f"column = value predicate: {fragment!r}. The query was not run."
    )

_TEMPORAL_GRAINS = {
    "day": "day", "daily": "day", "date": "day",
    "week": "week", "weekly": "week",
    "month": "month", "monthly": "month",
    "quarter": "quarter", "quarterly": "quarter",
    "year": "year", "yearly": "year", "annually": "year",
}

_TEMPORAL_TYPE_HINTS = ("date", "time", "timestamp")

# Datawrap's own objects, not warehouse tables. "how many jobs failed" belongs to
# list_jobs, not to SELECT COUNT(*) FROM jobs — unless the user named a connector,
# which means they really do mean a table with that name.
_PLATFORM_NOUNS = frozenset({
    "connector", "connectors", "connection", "connections",
    "job", "jobs", "transfer", "transfers", "run", "runs",
    "schedule", "schedules", "pipeline", "pipelines",
    "contract", "contracts", "dataset", "datasets",
    "table", "tables", "collection", "collections", "column", "columns",
    "workspace", "workspaces", "preflight", "quarantine",
    # The product's own inventory, which the documentation counts and no
    # warehouse holds a table of. "How many sources can you connect to" and
    # "how many file formats can you read" are questions about the catalog, and
    # the rejection only applies when no connector was named — so "count
    # sources on Demo Orders", where the operator really does mean a table of
    # that name, still reaches the aggregator.
    "source", "sources", "destination", "destinations",
    "format", "formats", "engine", "engines",
    "role", "roles", "gate", "gates", "mode", "modes",
    # Plural only. "How many warehouses do you support" asks what the catalog
    # carries; "how many datasets in the warehouse" is the operator naming
    # their own store as the scope of a live read.
    "warehouses",
})


@dataclass
class AggregationRequest:
    """A parsed analytics question, before it is grounded in a real schema."""

    metric: str = "count"
    column: str = ""
    table: str = ""
    group_by: str = ""
    connector_name: str = ""
    limit: int = _DEFAULT_GROUP_LIMIT
    descending: bool = True
    missing: list[str] = field(default_factory=list)
    # Free-text filter clause, re-parsed and grounded by the tool so the schema
    # stays the authority on column names and literal types.
    where: str = ""

    def as_tool_args(self) -> dict[str, Any]:
        args: dict[str, Any] = {"metric": self.metric, "limit": self.limit}
        for key in ("column", "table", "group_by", "connector_name", "where"):
            value = getattr(self, key)
            if value:
                args[key] = value
        if not self.descending:
            args["order"] = "asc"
        return args


# --------------------------------------------------------------------------
# Natural-language parsing
# --------------------------------------------------------------------------

# Ordered so that longer, more specific phrases win over their substrings
# ("total number of" is a count, "total revenue" is a sum).
_METRIC_PHRASES: tuple[tuple[str, str], ...] = (
    (r"how many (?:different|distinct|unique)", "count_distinct"),
    (r"(?:number|count) of (?:different|distinct|unique)", "count_distinct"),
    (r"(?:distinct|unique)\s+count(?:\s+of)?", "count_distinct"),
    (r"count\s+(?:of\s+)?(?:the\s+)?(?:different|distinct|unique)", "count_distinct"),
    (r"(?:distinct|unique) (?:values|count)? ?of", "count_distinct"),
    (r"(?:distinct|unique)", "count_distinct"),
    (r"(?:how many|how mny|number of|total number of|count of|count)", "count"),
    (r"(?:average|avg|mean)(?: of)?", "avg"),
    (r"(?:sum|total)(?: of)?", "sum"),
    (r"(?:minimum|min|lowest|smallest|earliest)(?: of)?", "min"),
    (r"(?:maximum|max|highest|largest|biggest|latest)(?: of)?", "max"),
    # A breakdown that names no measure is a count per group: "break down orders
    # by region" asks how many orders each region has. Without this the parser
    # returned None, the turn was still recognised as a read of the operator's
    # table, and it reached no tool at all — so the reply was a refusal to a
    # question the engine can answer exactly. Last in the list so a stated
    # measure keeps its metric: "sum amount by region" is still a sum.
    # The verb needs something to group. Followed directly by ``by`` it is not a
    # verb at all but the name of the SQL clause, and "what does group by do" is
    # a question about the clause — it parsed as an aggregation and answered a
    # documentation question with a connector error, the same way "what does
    # quarantine mean" once did.
    (
        r"(?:break(?:\s+\w+)?\s+down|breakdown|group(?:ed)?|"
        r"bucket(?:ed)?|segment(?:ed)?|split|roll(?:ed)?\s*up|tally)"
        r"(?!\s+by\b)",
        "count",
    ),
)

_ASCENDING_HINTS = ("ascending", "lowest", "smallest", "bottom", "least", "fewest")

# "mean" is also the English verb. "what does quarantine mean in Datawrap?" parsed
# as AVG(in Datawrap) FROM Datawrap and answered a documentation question with a
# connector error, so a definitional question is never an aggregation.
_DEFINITIONAL_RE = re.compile(
    r"\b(?:what|which|who|how)\b.*\b(?:mean|means|meaning)\b|"
    r"\b(?:does|do|did)\b[^?]*\bmean\b",
    re.I,
)
# "the mean", "mean of amount", "mean amount" keep their statistical reading.
_STATISTICAL_MEAN_RE = re.compile(
    r"\b(?:average|avg)\b|\b(?:the|a)\s+mean\b|\bmean\s+(?:of|value|amount|price)\b",
    re.I,
)


def _asks_for_a_definition(text: str) -> bool:
    """True when "mean" is the verb "to signify", not the statistical measure."""
    return bool(_DEFINITIONAL_RE.search(text)) and not _STATISTICAL_MEAN_RE.search(text)


# The operation named rather than commanded. A determiner in front of the
# measure makes it the noun the question is *about* — "does **a breakdown**
# include null values", "is **the count** exact or a sample" — where a request
# uses it as a verb ("break down orders by region") or applies it to data ("the
# count of orders by status").
#
# Measured over HTTP: both of those routed to the aggregator, which had nothing
# to aggregate and answered "Connector not found" over the top of the
# documentation that states the rule they asked about. Worse than a bare miss,
# because the measure tail is read as a column name and the rest of the English
# became one: "does a breakdown include null values" planned a count over a
# table called `include null values`.
#
# Decided before any slot filling, for that reason — once the tail has been
# mined for a column there is an invented subject to mistake for a real one.
# Paired with the question naming no data at all, so "does the count include
# nulls on Demo Orders" and "is the total revenue in orders" still reach the
# aggregator, and "show me the count", which reads the same way and is a real
# request, is untouched.
_ABOUT_THE_OPERATION_RE = re.compile(
    r"\b(?:is|are|does|do|can|could|will|would|should)\s+(?:a|an|the)\s+"
    r"(?:count|sum|total|average|avg|mean|min|minimum|max|maximum|"
    r"breakdown|break\s*down|aggregate|aggregation|group(?:ing)?)\b"
    r"|\bwhat\s+does\s+(?:a|an|the)\s+"
    r"(?:count|sum|total|average|avg|breakdown|break\s*down|aggregate|"
    r"aggregation|group(?:ing)?)\b",
    re.I,
)


#: A scope clause is the operator pointing at data — "in orders", "on Demo
#: Orders". Its absence is what makes a measure word a topic rather than a
#: request.
_NAMES_A_SCOPE_RE = re.compile(
    r"\b(?:on|from|in|inside|within)\s+(?:the\s+)?[A-Za-z_]",
    re.I,
)


def _asks_about_the_operation(text: str) -> bool:
    """True when the measure is the subject of the question, not the work to do."""
    return bool(_ABOUT_THE_OPERATION_RE.search(text)) and not _NAMES_A_SCOPE_RE.search(
        text
    )


# "top 5 customers by revenue" carries no metric word but is a ranking request:
# group by the dimension, rank by the measure.
_RANKING_RE = re.compile(
    r"\b(top|bottom)\s+(\d{1,3})\s+(?:the\s+)?"
    r"([A-Za-z_][A-Za-z0-9_ ]{0,40}?)\s+by\s+"
    r"([A-Za-z_][A-Za-z0-9_ ]{0,40}?)"
    r"(?=\s+(?:from|in|on|where|for)\b|[.,;?]|$)",
    re.I,
)

# Measures that mean "count the rows" rather than a numeric column.
_COUNT_MEASURE_WORDS = _ROW_WORDS | {"count", "counts", "volume", "frequency", "total"}


# Conversational filler that can trail a captured name ("connectors do I have").
# Only trimmed from measure/table candidates — a connector may legitimately be
# called "My Warehouse".
# A comparison word occupies the slot where the identifier goes, but it is never
# the name of the thing being compared. "biggest table on Demo Orders" parsed as
# MAX over a column called `table` in a table called `biggest`, and "which table
# has the most rows" sampled one called `most` — invented names sent at a live
# database. Canonical here because the planner in ``tools`` fails closed on the
# same set rather than keeping its own copy.
COMPARATIVE_WORDS = frozenset(
    """
    most fewest least largest biggest smallest widest narrowest
    highest lowest bigger smaller larger fewer more less
    top bottom first last latest newest oldest best worst
    """.split()
)

_STOP_TOKENS = frozenset({
    "do", "does", "did", "i", "we", "you", "your", "my", "our", "is", "are",
    "was", "were", "have", "has", "had", "there", "here", "please", "thanks",
    "that", "this", "to", "and", "or", "be", "been", "get", "got", "show",
    "me", "tell", "currently", "right", "now", "again", "still",
    # Modals. Without them "how many file formats can you read" and "how many
    # sources can you connect to" produced tables called `file formats can`
    # and `sources can` — no error reached the operator, because the
    # documentation answer led, but every such turn ran a doomed lookup first.
    # A modal is a fragment of the question, never part of a name.
    "can", "could", "will", "would", "should", "may", "might", "must", "shall",
})

# Adjectives that mean a status/state filter, not part of the table name.
# "how many paid orders" → table=orders, where status='paid'.
_STATUS_ADJECTIVES = frozenset({
    "paid", "pending", "cancelled", "canceled", "active", "inactive",
    "open", "closed", "failed", "success", "successful", "complete",
    "completed", "draft", "approved", "rejected", "shipped", "refunded",
})


# Pronouns and definite descriptions stand in for the previous subject. They are
# never real table or column names, so leaving them in produced a doomed lookup
# for a table literally called "it" instead of reusing the remembered table.
_COREFERENT_WORDS = frozenset({
    "it", "that", "this", "these", "those", "them", "there",
    "same", "the same", "that one", "the result", "that result",
    "the table", "that table", "this table", "the collection",
})


def is_coreferent(candidate: str) -> bool:
    return (candidate or "").strip().lower() in _COREFERENT_WORDS


def _trim_filler(candidate: str) -> str:
    """Cut a captured phrase at the first conversational filler token."""
    kept: list[str] = []
    for token in (candidate or "").split():
        if token.lower() in _STOP_TOKENS:
            break
        kept.append(token)
    return " ".join(kept)


def _leading_row_word(candidate: str) -> bool:
    """True when a captured measure phrase starts with a row noun ("rows are")."""
    first = (candidate or "").strip().lower().split(" ")[0] if candidate else ""
    return first in _ROW_WORDS


def _strip_span(text: str, span: tuple[int, int]) -> str:
    return f"{text[: span[0]]} {text[span[1] :]}"


def _clean_identifier(raw: str) -> str:
    """Reduce a captured phrase to a bare identifier candidate."""
    token = (raw or "").strip().strip("\"'`?.,;:!").strip()
    token = re.sub(r"^(?:the|a|an|my|our|all|each|every)\s+", "", token, flags=re.I)
    # Only structural nouns are filler. "value"/"amount"/"count" are real column
    # names ("order value"), so stripping them would silently measure the wrong
    # column.
    token = re.sub(r"\s+(?:table|collection|dataset)$", "", token, flags=re.I)
    # Edit words from follow-ups ("average amount instead") must not stick to
    # the identifier; they are never part of a column or table name.
    token = re.sub(r"\s+(?:instead|rather|now|too|also|please)$", "", token, flags=re.I)
    token = token.strip()
    if " " in token:
        # "order status" → "order status"; keep the phrase so schema grounding
        # can match order_status, but drop trailing filler.
        token = re.sub(r"\s+(?:in|on|from|of|for|by)$", "", token, flags=re.I).strip()
    return token


def _split_filter_clause(text: str) -> tuple[str, str]:
    """Separate the analytics question from its filter phrases.

    Returns ``(question_without_filters, filter_text)``. The filter text is kept
    verbatim and re-parsed by the tool once the real schema is known, so a column
    that only exists on the destination can never be silently accepted here.
    """
    from .predicates import parse_filters, parse_time_window

    parsed, remainder = parse_filters(text)
    window = parse_time_window(text)

    spans: list[str] = [pf.text for pf in parsed]
    if window:
        phrase = window[2]
        spans.append(phrase)
        # Remove the window phrase so "in 2024" is not read as a table named 2024.
        idx = remainder.lower().find(phrase.lower())
        if idx >= 0:
            remainder = remainder[:idx] + " " + remainder[idx + len(phrase) :]
            remainder = re.sub(r"\s{2,}", " ", remainder).strip(" ,;")
    if not spans:
        return text, ""
    return (remainder or text), " and ".join(spans)


def parse_aggregation_request(message: str) -> AggregationRequest | None:
    """Extract an aggregation intent from a natural prompt, or None.

    Returns None when the message is not an analytics question so the caller can
    fall through to other routing. A recognised question with an unknown table
    still returns a request with ``missing`` populated, so the pilot can ask a
    specific follow-up instead of silently doing nothing.
    """
    text = (message or "").strip()
    if not text or len(text) > 500:
        return None

    # An explicit SQL statement is not a natural-language aggregation.
    if re.match(r"^\s*(?:select|with|show|describe|explain)\b", text.lower()):
        return None

    if _asks_for_a_definition(text) or _asks_about_the_operation(text):
        return None

    # "orders where amount > 10 on PilotSQLite" — table-first filtered count.
    table_where = re.match(
        r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s+where\s+(.+?)\s+on\s+(?:the\s+)?"
        r"([A-Za-z0-9_][A-Za-z0-9_\- ]{0,48}?)\s*$",
        text,
        re.I,
    )
    if table_where:
        return AggregationRequest(
            metric="count",
            table=_clean_identifier(table_where.group(1)),
            where=table_where.group(2).strip(),
            connector_name=_clean_identifier(table_where.group(3)),
        )

    # Pull filter phrases out first. Left in place, "where status = open" would
    # be scanned for a table name and produce a lookup for a table called
    # "status = open".
    working, where_text = _split_filter_clause(text)
    lower = working.lower()

    ranking = _RANKING_RE.search(working)
    if ranking:
        dimension = _clean_identifier(ranking.group(3))
        measure = _clean_identifier(ranking.group(4))
        if dimension:
            req = AggregationRequest(
                metric="count" if measure.lower() in _COUNT_MEASURE_WORDS else "sum",
                column="" if measure.lower() in _COUNT_MEASURE_WORDS else measure,
                group_by=dimension,
                limit=max(1, min(int(ranking.group(2)), _MAX_GROUP_LIMIT)),
                descending=ranking.group(1).lower() == "top",
                where=where_text,
            )
            return _finish_request(req, _strip_span(working, ranking.span()))

    metric: str = ""
    metric_span: tuple[int, int] | None = None
    for pattern, name in _METRIC_PHRASES:
        found = re.search(rf"\b{pattern}\b", working, re.I)
        if found:
            metric, metric_span = name, found.span()
            break
    if not metric:
        return None

    req = AggregationRequest(metric=metric, where=where_text)
    # Capture the measure tail before any span stripping shifts the offsets.
    measure_tail = working[metric_span[1] :] if metric_span else ""

    # "top 5 …" / "first 10 …" set the group limit.
    top = re.search(r"\b(?:top|first|bottom|last)\s+(\d{1,3})\b", working, re.I)
    if top:
        req.limit = max(1, min(int(top.group(1)), _MAX_GROUP_LIMIT))
        working = _strip_span(working, top.span())
    if any(hint in lower for hint in _ASCENDING_HINTS) and metric != "min":
        req.descending = False
    return _finish_request(req, working, measure_tail=measure_tail)


def _finish_request(
    req: AggregationRequest,
    working: str,
    *,
    measure_tail: str = "",
) -> AggregationRequest | None:
    """Pull connector, grouping, table and measure out of the remaining text."""
    # Strip trailing sentence punctuation so "on Local Postgres?" still binds.
    working = re.sub(r"[?\s.!;,:]+$", "", (working or "").strip())
    measure_tail = re.sub(r"[?\s.!;,:]+$", "", (measure_tail or "").strip())
    # Connector: trailing "on <connector>" mirrors the pilot's existing
    # "sample <table> on <connector>" convention.
    conn = re.search(
        r"\bon\s+(?:the\s+)?([A-Za-z0-9_][A-Za-z0-9_\- ]{0,48}?)"
        r"(?:\s+(?:connector|database|db|warehouse))?\s*$",
        working,
        re.I,
    )
    if conn:
        req.connector_name = _clean_identifier(conn.group(1))
        working = _strip_span(working, conn.span())
        # "on orders PilotSQLite" (missing second "on") → table + connector.
        tokens = (req.connector_name or "").split()
        if len(tokens) == 2 and not req.table:
            left, right = tokens[0], tokens[1]
            if left.islower() and left not in _PLATFORM_NOUNS and (
                right[:1].isupper() or "sql" in right.lower() or "ware" in right.lower()
            ):
                req.table = left
                req.connector_name = right

    # Grouping dimension: "by status", "per region", "grouped by month".
    grp = re.search(
        r"\b(?:group(?:ed)?\s+by|broken\s+down\s+by|by|per)\s+"
        r"([A-Za-z_][A-Za-z0-9_ ]{0,48}?)"
        r"(?=\s+(?:from|in|on|for|of|where|and|with)\b|[.,;?]|$)",
        working,
        re.I,
    )
    if grp:
        candidate = _clean_identifier(grp.group(1))
        if candidate and candidate.lower() not in _ROW_WORDS:
            req.group_by = candidate
            working = _strip_span(working, grp.span())

    # Table: "from orders", "in the products table", "orders table".
    tbl = re.search(
        r"\b(?:from|in|inside|within)\s+(?:the\s+)?"
        r"([A-Za-z_][A-Za-z0-9_. ]{0,48}?)"
        r"(?:\s+(?:table|collection|dataset))?"
        r"(?=\s+(?:where|group|by|per|on|and|with)\b|[.,;?]|$)",
        working,
        re.I,
    )
    if tbl:
        req.table = _trim_filler(_clean_identifier(tbl.group(1)))
        working = _strip_span(working, tbl.span())
    else:
        named = re.search(
            r"\b([A-Za-z_][A-Za-z0-9_.]{1,48})\s+(?:table|collection)\b", working, re.I
        )
        if named:
            req.table = _trim_filler(_clean_identifier(named.group(1)))
            working = _strip_span(working, named.span())

    # Measure column: whatever follows the metric phrase.
    if measure_tail and not req.column:
        measure = re.match(
            r"\s*(?:of\s+|for\s+|the\s+)*([A-Za-z_][A-Za-z0-9_ ]{0,48}?)"
            r"(?=\s+(?:from|in|on|by|per|where|and|with|group)\b|[.,;?]|$)",
            measure_tail,
            re.I,
        )
        if measure:
            candidate = _trim_filler(_clean_identifier(measure.group(1)))
            if _leading_row_word(candidate):
                # "how many rows are in X" measures the rows, not a column.
                if req.metric == "count_distinct":
                    req.metric = "count"
            elif candidate and candidate.lower() != (req.table or "").lower():
                req.column = candidate

    # "count of orders" names the table, not a column, when nothing else did.
    if req.metric == "count" and not req.table and req.column:
        req.table, req.column = req.column, ""

    # "how many paid orders" → table=orders + status filter (not table "paid orders").
    if req.table and " " in req.table.strip():
        parts = req.table.strip().split()
        if len(parts) >= 2 and parts[0].lower() in _STATUS_ADJECTIVES:
            status = parts[0].lower()
            req.table = parts[-1]
            clause = f"status = '{status}'"
            req.where = f"({req.where}) AND {clause}" if req.where else clause

    # "distinct count of region" leaves column=region with metric count_distinct.
    if req.metric == "count" and req.column and req.table:
        # count + column without "distinct" stays as filtered? No — COUNT(col)
        # is unusual; prefer count_distinct when the NL said distinct earlier.
        pass
    if req.metric == "count_distinct" and req.column and not req.table:
        # measure was the dimension; table may still be missing
        pass

    coreferent_subject = False
    if is_coreferent(req.table):
        # "how many rows in it" — the subject comes from working memory.
        req.table = ""
        coreferent_subject = True
    if is_coreferent(req.column):
        req.column = ""
    if is_coreferent(req.group_by):
        req.group_by = ""

    table_tokens = req.table.lower().split()
    if (
        table_tokens
        and (
            req.table.lower() in _PLATFORM_NOUNS
            # First token and last. The head of an English noun phrase is its
            # final word, so "file formats" is a question about formats; only
            # the first was checked, and "file" is not a platform noun.
            or table_tokens[0] in _PLATFORM_NOUNS
            or table_tokens[-1] in _PLATFORM_NOUNS
        )
        and not req.connector_name
    ):
        # Platform inventory question — let the jobs/connectors routes answer it.
        return None

    # "connector count" / bare "how many" with only a platform noun as measure.
    if (
        not req.table
        and not req.connector_name
        and (req.column or "").lower() in _PLATFORM_NOUNS
    ):
        return None
    # Bare metric with no subject at all ("how many", "count") — leave to
    # inventory / unmapped, unless the utterance was coreferent ("… in it").
    if (
        not req.table
        and not req.column
        and not req.group_by
        and not req.connector_name
        and not coreferent_subject
        and req.metric in {"count", "sum", "avg"}
    ):
        return None

    # A superlative in the identifier slot means the sentence never named a
    # subject — "biggest table on Demo Orders" measured a column called `table`
    # in a table called `biggest`. Hand it back so the table-ranking route,
    # which really does answer that question, can claim it.
    if (req.table or "").strip().lower() in COMPARATIVE_WORDS:
        return None
    if (req.column or "").strip().lower() in COMPARATIVE_WORDS:
        return None

    needs_column = _METRICS[req.metric][1]
    if needs_column and not req.column:
        req.missing.append("column")
    if not req.table:
        req.missing.append("table")
    # Spoken plurals ("by regions") → singular stem for schema resolve.
    # Status-like stems (status/basis) are preserved inside `_singular`.
    if req.group_by and " " not in req.group_by.strip():
        req.group_by = _singular(req.group_by.strip())
    return req


def looks_like_aggregation(message: str) -> bool:
    """True when the prompt is an analytics question the aggregate tool can take."""
    parsed = parse_aggregation_request(message)
    return parsed is not None and "column" not in parsed.missing


# --------------------------------------------------------------------------
# Schema grounding
# --------------------------------------------------------------------------


def _normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def _singular(token: str) -> str:
    if token.endswith("ies") and len(token) > 4:
        return token[:-3] + "y"
    if token.endswith(("ses", "xes", "zes", "ches", "shes")) and len(token) > 4:
        return token[:-2]
    # Don't turn status/genus/basis into broken stems.
    if token.endswith(("ss", "us", "is", "os")):
        return token
    if token.endswith("s") and len(token) > 3:
        return token[:-1]
    return token


# Words an operator says *around* a column name rather than as one: schema
# vocabulary ("the amount column"), metric vocabulary ("distinct region") and
# bare articles. They are only ever dropped after the whole phrase has already
# failed to resolve, so a table that really owns a column called `total` still
# answers `total` exactly.
_COLUMN_PHRASE_NOISE = frozenset(
    {
        "a", "all", "an", "any", "by", "col", "cols", "column", "columns",
        "count", "distinct", "each", "entries", "entry", "field", "fields",
        "for", "in", "many", "much", "number", "of", "on", "record", "records",
        "row", "rows", "the", "total", "unique", "value", "values",
    }
)


def resolve_name(needle: str, available: list[str]) -> str:
    """Match a spoken name to a real schema name, or "" when nothing fits."""
    want = (needle or "").strip()
    if not want or not available:
        return ""
    for name in available:  # exact
        if name == want:
            return name
    lowered = want.lower()
    for name in available:  # case-insensitive
        if name.lower() == lowered:
            return name
    target = _normalize_name(want)
    if not target:
        return ""
    for name in available:  # ignoring separators: "order status" → order_status
        if _normalize_name(name) == target:
            return name
    singular = _singular(target)
    for name in available:  # plural tolerance both directions
        norm = _normalize_name(name)
        if norm == singular or _singular(norm) == target or _singular(norm) == singular:
            return name
    # Unique prefix / containment, but only when exactly one column qualifies,
    # so an ambiguous word never silently picks the wrong column. Both the
    # spoken form and its singular stem are tried ("statuses" → order_status).
    for probe in (target, singular):
        if not probe or len(probe) < 3:
            continue
        partial = [
            name
            for name in available
            if _normalize_name(name).startswith(probe) or probe in _normalize_name(name)
        ]
        if len(partial) == 1:
            return partial[0]
    # Spoken phrases carry words the schema does not: "the amount column",
    # "order amount", "distinct region". Resolve the content words one at a
    # time and accept the answer only when they agree on a single column, so an
    # ambiguous phrase still fails closed instead of guessing a column.
    words = [w for w in re.split(r"[^a-z0-9]+", want.lower()) if w]
    if len(words) > 1:
        # A noise word that is itself a real column keeps its vote, so "total
        # amount" stays ambiguous on a table owning both `total` and `amount`.
        content = [
            w
            for w in words
            if w not in _COLUMN_PHRASE_NOISE or resolve_name(w, available)
        ]
        probes = content or words
        if len(probes) < len(words) and len(content) > 1:
            joined = resolve_name(" ".join(content), available)
            if joined:
                return joined
        hits = {resolve_name(w, available) for w in probes}
        hits.discard("")
        if len(hits) == 1:
            return hits.pop()
    return ""


def _is_temporal(col_type: str) -> bool:
    low = (col_type or "").lower()
    return any(hint in low for hint in _TEMPORAL_TYPE_HINTS)


def _temporal_group_expr(quoted: str, grain: str, dialect: str) -> str | None:
    """Dialect-correct date truncation for "by month" style grouping."""
    d = (dialect or "").lower()
    if d in {"postgresql", "postgres", "redshift", "cockroachdb", "timescaledb", "snowflake", "duckdb"}:
        return f"DATE_TRUNC('{grain}', {quoted})"
    if d in {"mysql", "mariadb", "tidb"}:
        fmt = {
            "day": "%Y-%m-%d",
            "month": "%Y-%m-01",
            "year": "%Y-01-01",
        }.get(grain)
        if fmt:
            return f"DATE_FORMAT({quoted}, '{fmt}')"
        if grain == "week":
            return f"DATE(DATE_SUB({quoted}, INTERVAL WEEKDAY({quoted}) DAY))"
        if grain == "quarter":
            return f"CONCAT(YEAR({quoted}), '-Q', QUARTER({quoted}))"
        return None
    if d == "sqlite":
        fmt = {"day": "%Y-%m-%d", "month": "%Y-%m", "year": "%Y"}.get(grain)
        return f"strftime('{fmt}', {quoted})" if fmt else None
    if d == "bigquery":
        return f"DATE_TRUNC({quoted}, {grain.upper()})"
    if d in {"mssql", "sqlserver"}:
        return f"DATETRUNC({grain}, {quoted})"
    return None


def _describe_predicates(predicates: list[Any]) -> str:
    if not predicates:
        return ""
    from .predicates import describe

    return describe(predicates)


_MONGO_FIELD = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")

# MongoDB accumulator per metric. $sum: 1 is the documented way to count
# documents in a group; $sum/$avg silently ignore non-numeric values.
_MONGO_ACCUMULATORS = {
    "sum": "$sum",
    "avg": "$avg",
    "min": "$min",
    "max": "$max",
}


def _mongo_group_id(dim_col: str, grain: str) -> Any:
    """The ``_id`` expression for a $group stage."""
    if not dim_col:
        return None
    if grain:
        # $dateTrunc (MongoDB 5.0+) returns a Date, so buckets sort
        # chronologically and stay usable for further date math — unlike
        # $dateToString, which yields strings.
        return {"$dateTrunc": {"date": f"${dim_col}", "unit": grain}}
    return f"${dim_col}"


def _mongo_pipeline(
    *,
    metric: str,
    measure_col: str,
    dim_col: str,
    grain: str,
    dim_alias: str,
    metric_alias: str,
    limit: int,
    descending: bool,
    match: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Build a read-only aggregation pipeline with SQL-equivalent semantics."""
    pipeline: list[dict[str, Any]] = []
    if match:
        # $match first: it is the only stage that can use an index, and it
        # shrinks what $group has to hold in memory.
        pipeline.append({"$match": match})

    if metric == "count_distinct":
        # SQL COUNT(DISTINCT c) ignores NULLs; {$ne: null} also drops missing
        # fields, which is the Mongo equivalent of "not null".
        pipeline.append({"$match": {measure_col: {"$ne": None}}})
        if dim_col:
            pipeline.extend([
                {"$group": {"_id": {
                    "dim": _mongo_group_id(dim_col, grain),
                    "value": f"${measure_col}",
                }}},
                {"$group": {"_id": "$_id.dim", metric_alias: {"$sum": 1}}},
                {"$sort": {metric_alias: -1 if descending else 1}},
                {"$limit": limit},
                {"$project": {"_id": 0, dim_alias: "$_id", metric_alias: 1}},
            ])
        else:
            pipeline.extend([
                {"$group": {"_id": f"${measure_col}"}},
                {"$count": metric_alias},
            ])
        return pipeline

    if metric == "count":
        accumulator: dict[str, Any] = {"$sum": 1}
    else:
        accumulator = {_MONGO_ACCUMULATORS[metric]: f"${measure_col}"}

    group_stage: dict[str, Any] = {
        "_id": _mongo_group_id(dim_col, grain),
        metric_alias: accumulator,
    }
    pipeline.append({"$group": group_stage})
    if dim_col:
        pipeline.extend([
            {"$sort": {metric_alias: -1 if descending else 1}},
            {"$limit": limit},
            {"$project": {"_id": 0, dim_alias: "$_id", metric_alias: 1}},
        ])
    else:
        pipeline.append({"$project": {"_id": 0, metric_alias: 1}})
    return pipeline


def _limit_clause(sql: str, dialect: str, limit: int) -> str:
    from services.dialect_profiles import append_result_limit

    return append_result_limit(dialect, sql, limit)


def _column_names(columns: list[dict[str, Any]]) -> list[str]:
    return [str(c.get("name") or "") for c in columns if c.get("name")]


def _column_type(columns: list[dict[str, Any]], name: str) -> str:
    for c in columns:
        if str(c.get("name")) == name:
            return str(c.get("inferred_type") or c.get("data_type") or "")
    return ""


def _introspect_columns(conn: dict[str, Any], table: str) -> list[dict[str, Any]]:
    """Ground names in the one canonical schema path (see ``schema_tools``)."""
    from .schema_tools import introspect_connector_table

    return introspect_connector_table(conn, table, purpose="source")["columns"]


def _sibling_table(conn: dict[str, Any], wanted: str, table: str) -> str:
    """The connector's own table this unresolved column name actually matches.

    "how many distinct customers in orders" asks for a column that does not
    exist, and `customers` is a *table* on the same connector. Naming that is the
    difference between a dead end and the operator's next correct question.
    """
    probe = _normalize_name(wanted)
    if not probe or probe == _normalize_name(table):
        return ""
    try:
        from .schema_tools import list_connector_objects

        result = list_connector_objects(
            connector_id=str(conn.get("id") or conn.get("_id") or ""),
            connector_name=str(conn.get("name") or ""),
        )
        objects = list((getattr(result, "output", None) or {}).get("objects") or [])
    except Exception:
        return ""
    for name in objects:
        norm = _normalize_name(str(name))
        if norm == probe or _singular(norm) == _singular(probe):
            return str(name)
    return ""


# --------------------------------------------------------------------------
# The tool
# --------------------------------------------------------------------------


def _group_total(
    *,
    saved: Any,
    dialect: str,
    table: str,
    collection: str,
    dim_col: str,
    grain: str,
    dim_expr: str,
    quoted_table: str,
    where_sql: str,
    binds: dict[str, Any],
    match_stage: dict[str, Any],
) -> int | None:
    """True number of groups under the same filters (D105)."""
    from src.routers.query_router import QueryExecuteRequest, _run_query

    if dialect == "mongodb":
        group_id = _mongo_group_id(dim_col, grain)
        pipeline = []
        if match_stage:
            pipeline.append({"$match": match_stage})
        pipeline += [
            {"$group": {"_id": group_id}},
            {"$count": "n"},
        ]
        body = QueryExecuteRequest(
            connector_id=str(getattr(saved, "id", "") or getattr(saved, "_id", "") or ""),
            query=json.dumps(pipeline, default=str),
            collection=collection,
            limit=10,
        )
        rows, _cols, _schema, _tr = _run_query(saved, body)
        if rows:
            val = rows[0].get("n")
            return int(val) if val is not None else None
        return 0

    count_sql = (
        f"SELECT COUNT(*) AS n_groups FROM ("
        f"SELECT {dim_expr} FROM {quoted_table}{where_sql} "
        f"GROUP BY {dim_expr}) _df_grp"
    )
    body = QueryExecuteRequest(
        connector_id=str(getattr(saved, "id", "") or getattr(saved, "_id", "") or ""),
        query=count_sql,
        limit=10,
        params=dict(binds or {}),
    )
    rows, _cols, _schema, _tr = _run_query(saved, body)
    if rows:
        val = next(iter(rows[0].values()), None)
        return int(val) if val is not None else None
    return 0


def _groups_at_metric(
    *,
    saved: Any,
    dialect: str,
    table: str,
    dim_col: str,
    grain: str,
    dim_expr: str,
    agg_expr: str,
    metric_alias: str,
    quoted_table: str,
    where_sql: str,
    binds: dict[str, Any],
    match_stage: dict[str, Any],
    metric: str,
    measure_col: str,
    cut_value: Any,
) -> int:
    """How many groups share the metric value shown at the LIMIT cut (D105)."""
    from src.routers.query_router import QueryExecuteRequest, _run_query

    if dialect == "mongodb":
        group_id = _mongo_group_id(dim_col, grain)
        acc = _MONGO_ACCUMULATORS.get(metric)
        if metric == "count_distinct":
            accum = {"$addToSet": f"${measure_col}"}
            group_stage = {"_id": group_id, "m": accum}
            project = {"$project": {"m": {"$size": "$m"}}}
        elif acc:
            group_stage = {"_id": group_id, "m": {acc: f"${measure_col}" if acc != "$sum" or measure_col else 1}}
            project = None
        else:
            return 0
        pipeline = []
        if match_stage:
            pipeline.append({"$match": match_stage})
        pipeline.append({"$group": group_stage})
        if project:
            pipeline.append(project)
        pipeline += [{"$match": {"m": cut_value}}, {"$count": "n"}]
        body = QueryExecuteRequest(
            connector_id=str(getattr(saved, "id", "") or getattr(saved, "_id", "") or ""),
            query=json.dumps(pipeline, default=str),
            collection=table,
            limit=10,
        )
        rows, _cols, _schema, _tr = _run_query(saved, body)
        if rows:
            val = rows[0].get("n")
            return int(val or 0)
        return 0

    cut_binds = dict(binds or {})
    cut_binds["_df_cut"] = cut_value
    count_sql = (
        f"SELECT COUNT(*) AS n FROM ("
        f"SELECT {agg_expr} AS m FROM {quoted_table}{where_sql} "
        f"GROUP BY {dim_expr}) _df_cut_t WHERE _df_cut_t.m = :_df_cut"
    )
    body = QueryExecuteRequest(
        connector_id=str(getattr(saved, "id", "") or getattr(saved, "_id", "") or ""),
        query=count_sql,
        limit=10,
        params=cut_binds,
    )
    rows, _cols, _schema, _tr = _run_query(saved, body)
    if rows:
        val = next(iter(rows[0].values()), None)
        return int(val or 0)
    return 0


def aggregate_connector_data(
    connector_id: str = "",
    connector_name: str = "",
    table: str = "",
    metric: str = "count",
    column: str = "",
    group_by: str = "",
    order: str = "desc",
    limit: int = _DEFAULT_GROUP_LIMIT,
    session_id: str = "",
    where: str = "",
    date_column: str = "",
):
    """Run an exact server-side aggregate on a saved connector's table."""
    tool = "aggregate_data"
    metric_key = (metric or "count").strip().lower().replace(" ", "_")
    aliases = {
        "counts": "count",
        "total": "sum",
        "average": "avg",
        "mean": "avg",
        "distinct": "count_distinct",
        "unique": "count_distinct",
        "nunique": "count_distinct",
        "minimum": "min",
        "maximum": "max",
    }
    metric_key = aliases.get(metric_key, metric_key)
    if metric_key not in _METRICS:
        return _tool_result(
            tool,
            success=False,
            error=(
                f"Unsupported metric '{metric}'. Use one of: "
                f"{', '.join(sorted(_METRICS))}."
            ),
        )

    table = (table or "").strip()
    # Unsafe identifiers never reach the store — fail closed before connector lookup.
    if table and not _SAFE_IDENT.match(table):
        return _tool_result(
            tool,
            success=False,
            error=(
                "Provide a simple table/collection name "
                "(letters, numbers, underscore, optional schema.table)."
            ),
        )
    # Empty table with no connector hint → ask for the table first (tests + UX).
    if not table and not (connector_id or connector_name):
        from .example_phrases import example_connector_name

        ex = example_connector_name()
        return _tool_result(
            tool,
            success=False,
            error=f'Which table? Example: "count of orders by status on {ex}".',
        )

    conn, err = _safe_connector(connector_id, connector_name, tool)
    if err:
        return err

    if not table:
        # Railway-class: don't dead-end — list real tables and auto-pick if unique.
        try:
            from .schema_tools import list_connector_objects

            listed = list_connector_objects(
                connector_id=str(conn.get("id") or conn.get("_id") or ""),
                connector_name=str(conn.get("name") or connector_name or ""),
            )
            objs = []
            if listed.success and isinstance(listed.output, dict):
                objs = listed.output.get("objects") or listed.output.get("tables") or []
            names = []
            for o in objs:
                if isinstance(o, dict):
                    n = o.get("name") or o.get("table") or o.get("id")
                    if n:
                        names.append(str(n))
                elif isinstance(o, str):
                    names.append(o)
            if len(names) == 1:
                table = names[0]
            elif names:
                shown = ", ".join(f"`{n}`" for n in names[:12])
                more = f" (+{len(names) - 12} more)" if len(names) > 12 else ""
                return _tool_result(
                    tool,
                    success=False,
                    error=(
                        f"Which table on **{conn.get('name') or 'this connector'}**? "
                        f"{shown}{more}. "
                        'Example: "sum amount by region from orders on '
                        f'{conn.get("name") or "PilotSQLite"}".'
                    ),
                )
            else:
                return _tool_result(
                    tool,
                    success=False,
                    error=(
                        'Which table? Example: "count of orders by status on '
                        f'{conn.get("name") or "your connector"}".'
                    ),
                )
        except Exception:
            from .example_phrases import example_connector_name

            ex = example_connector_name()
            return _tool_result(
                tool,
                success=False,
                error=f'Which table? Example: "count of orders by status on {ex}".',
            )

    ctype = str(conn.get("type") or conn.get("format") or "").lower()
    cid = str(conn.get("id") or conn.get("_id") or "")

    try:
        columns = _introspect_columns(conn, table)
    except AmbiguousConnectorError as exc:
        return _tool_result(tool, success=False, error=exc.message)
    except Exception as exc:
        _LOG.warning("aggregate introspect failed: %s", exc, exc_info=True)
        columns = []

    names = _column_names(columns)
    if not names:
        return _tool_result(
            tool,
            success=False,
            error=(
                f"Could not read the schema of '{table}' on "
                f"{conn.get('name') or 'this connector'}. Check the table name — "
                'you can ask "list tables on that connector".'
            ),
        )

    needs_column = _METRICS[metric_key][1]
    measure_col = ""
    if needs_column:
        wanted = (column or "").strip()
        if not wanted:
            return _tool_result(
                tool,
                success=False,
                error=(
                    f"Which column should I {metric_key.replace('_', ' ')}? "
                    f"Columns in {table}: {', '.join(names[:20])}."
                ),
            )
        measure_col = resolve_name(wanted, names)
        if not measure_col:
            sibling = _sibling_table(conn, wanted, table)
            hint = (
                f" `{sibling}` is a table on {conn.get('name') or 'this connector'}, "
                f"not a column in {table} — ask me to {metric_key.replace('_', ' ')} "
                f"over `{sibling}` instead."
                if sibling
                else ""
            )
            return _tool_result(
                tool,
                success=False,
                error=(
                    f"Column '{wanted}' is not in {table}. "
                    f"Available columns: {', '.join(names[:20])}." + hint
                ),
            )
        if metric_key in {"sum", "avg"}:
            col_type = _column_type(columns, measure_col).lower()
            money_text = ctype == "sqlite" and _sqlite_text_money(measure_col, col_type)
            if col_type and any(hint in col_type for hint in _NON_NUMERIC_HINTS) and not money_text:
                numeric = [
                    n
                    for n in names
                    if not any(h in _column_type(columns, n).lower() for h in _NON_NUMERIC_HINTS)
                ]
                return _tool_result(
                    tool,
                    success=False,
                    error=(
                        f"'{measure_col}' is {col_type.upper()} — {metric_key.upper()} "
                        "needs a numeric column."
                        + (f" Try: {', '.join(numeric[:12])}." if numeric else "")
                    ),
                )

    dialect = ctype
    dim_col = ""
    grain = ""
    if group_by:
        wanted = group_by.strip()
        # "year of Hire_Date" / "monthly by created_at" — grain + column.
        # Without this, resolve_name fuzzy-matched the tail to Hire_Date and
        # grouped by raw day values, returning 20 single-row buckets as an
        # exact year answer (QA R13).
        grain_hint = re.match(
            r"(?i)^(year|years|yearly|annual|annually|quarter|quarterly|"
            r"month|months|monthly|week|weeks|weekly|day|days|daily|date|"
            r"hour|hours|hourly)\b[\s._-]*(?:of|by|in|per|each)?\s+(.+)$",
            wanted,
        )
        if grain_hint:
            candidate = grain_hint.group(2).strip()
            resolved = resolve_name(candidate, names)
            if resolved and _is_temporal(_column_type(columns, resolved)):
                grain = _TEMPORAL_GRAINS.get(grain_hint.group(1).lower(), "")
                dim_col = resolved
            elif resolved:
                # "year of status" — a real column but not a date. Refusing
                # here matters: letting the whole phrase fuzzy-resolve to
                # `status` grouped by the wrong thing while claiming a grain
                # (same silent-wrong-answer class as R13).
                return _tool_result(
                    tool,
                    success=False,
                    error=(
                        f"'{resolved}' is not a date column — "
                        f"'{grain_hint.group(1).lower()} of {candidate}' "
                        "needs a date/timestamp column to bucket."
                    ),
                )
        if not grain_hint and not dim_col and wanted not in names:
            # "<col>:<grain>" / "<col> by <grain>" / "<col>_<grain>" — the
            # answer path for "which date column?" (D105). The head must be a
            # real column; a literal column named like "created_at_day" was
            # already claimed by the `wanted not in names` guard above.
            col_grain = re.match(
                r"(?i)^(.+?)(?::|\s+(?:by|per)\s+|_)"
                r"(day|daily|week|weekly|month|monthly|quarter|quarterly|"
                r"year|yearly|annually)$",
                wanted,
            )
            if col_grain:
                candidate = col_grain.group(1).strip()
                resolved = resolve_name(candidate, names)
                if resolved:
                    if not _is_temporal(_column_type(columns, resolved)):
                        return _tool_result(
                            tool,
                            success=False,
                            error=(
                                f"'{resolved}' is not a date column — "
                                f"'{wanted}' needs a date/timestamp column to bucket."
                            ),
                        )
                    grain = _TEMPORAL_GRAINS.get(col_grain.group(2).lower(), "")
                    dim_col = resolved
        if not grain_hint and not dim_col:
            dim_col = resolve_name(wanted, names)
        if not dim_col:
            # "by month" is a time grain, not a column — find the date column.
            grain = _TEMPORAL_GRAINS.get(wanted.lower(), "")
            if grain:
                temporal = [n for n in names if _is_temporal(_column_type(columns, n))]
                if (date_column or "").strip():
                    # date_column= answers the "which date column" question (D105).
                    resolved_dc = resolve_name(date_column.strip(), names)
                    if not resolved_dc:
                        return _tool_result(
                            tool,
                            success=False,
                            error=(
                                f"date_column '{date_column.strip()}' is not in {table}. "
                                f"Available columns: {', '.join(names[:20])}."
                            ),
                        )
                    if not _is_temporal(_column_type(columns, resolved_dc)):
                        return _tool_result(
                            tool,
                            success=False,
                            error=(
                                f"date_column '{resolved_dc}' is not a date column — "
                                f"'{wanted}' needs a date/timestamp column to bucket."
                            ),
                        )
                    dim_col = resolved_dc
                elif len(temporal) == 1:
                    dim_col = temporal[0]
                elif len(temporal) > 1:
                    return _tool_result(
                        tool,
                        success=False,
                        error=(
                            f"Which date column should I group by {grain}? "
                            f"{table} has: {', '.join(temporal[:10])}."
                        ),
                    )
            if not dim_col:
                sibling = _sibling_table(conn, wanted, table)
                hint = (
                    f" `{sibling}` is a table on "
                    f"{conn.get('name') or 'this connector'}, not a column in "
                    f"{table} — name a column of {table} to group by, or ask "
                    f"about `{sibling}` instead."
                    if sibling
                    else ""
                )
                return _tool_result(
                    tool,
                    success=False,
                    error=(
                        f"Column '{wanted}' is not in {table}. "
                        f"Available columns: {', '.join(names[:20])}." + hint
                    ),
                )

    predicates: list[Any] = []
    if (where or "").strip():
        from .predicates import (
            PredicateError,
            ground_filters,
            parse_filters,
            parse_time_window,
            temporal_window_predicate,
        )

        try:
            parsed_filters, leftover = parse_filters(where)
            if len(parsed_filters) == 1 and (leftover or "").strip():
                absorbed = _absorb_bound_literal(where, parsed_filters[0], leftover)
                if absorbed is not None:
                    parsed_filters = [absorbed]
                    leftover = ""
            predicates = ground_filters(
                parsed_filters, columns, resolve_name, _column_type
            )
            window = parse_time_window(where)
            if window:
                already = {p.column for p in predicates}
                win_pred = temporal_window_predicate(
                    window,
                    columns,
                    _column_type,
                    preferred=dim_col if dim_col not in already else "",
                )
                if win_pred and win_pred.column not in already:
                    predicates.append(win_pred)
            if not predicates:
                return _tool_result(
                    tool,
                    success=False,
                    error=(
                        f"I couldn't turn “{where.strip()}” into a filter on {table}. "
                        f"Columns: {', '.join(names[:20])}."
                    ),
                )
            filter_warning = _unparsed_filter_warning(where, leftover)
            if filter_warning:
                return _tool_result(tool, success=False, error=filter_warning)
        except PredicateError as exc:
            return _tool_result(tool, success=False, error=str(exc))

    dim_alias = f"{dim_col}_{grain}" if (dim_col and grain) else dim_col
    _template, _needs, metric_alias = _METRICS[metric_key]
    if needs_column:
        metric_alias = (
            f"distinct_{measure_col}"
            if metric_key == "count_distinct"
            else f"{metric_alias}_{measure_col}"
        )
    row_limit = max(1, min(int(limit or _DEFAULT_GROUP_LIMIT), _MAX_GROUP_LIMIT))
    descending = str(order or "desc").lower().startswith("d")
    binds: dict[str, Any] = {}
    # SQL-side query fragments — mongo builds pipelines instead; the D105
    # group-count probes read whichever shape applies.
    dim_expr = ""
    agg_expr = ""
    quoted_table = ""
    where_sql = ""
    match_stage: dict[str, Any] = {}

    if dialect == "mongodb":
        for field_name in (measure_col, dim_col):
            if field_name and not _MONGO_FIELD.match(field_name):
                return _tool_result(
                    tool,
                    success=False,
                    error=f"Field '{field_name}' cannot be aggregated safely.",
                )
        match_stage: dict[str, Any] = {}
        if predicates:
            from .predicates import to_mongo_match

            match_stage = to_mongo_match(predicates)
        pipeline = _mongo_pipeline(
            metric=metric_key,
            measure_col=measure_col,
            dim_col=dim_col,
            grain=grain,
            dim_alias=dim_alias,
            metric_alias=metric_alias,
            limit=row_limit,
            descending=descending,
            match=match_stage,
        )
        query_text = json.dumps(pipeline, default=str)
    else:
        quoted_dim = _quote_ident(dim_col, dialect) if dim_col else ""
        dim_expr = quoted_dim
        if grain and quoted_dim:
            dim_expr = _temporal_group_expr(quoted_dim, grain, dialect) or ""
            if not dim_expr:
                return _tool_result(
                    tool,
                    success=False,
                    error=(
                        f"Grouping by {grain} is not supported on {dialect or 'this engine'} yet — "
                        f'try "by {dim_col}".'
                    ),
                )
        agg_expr = (
            _template.format(col=_quote_ident(measure_col, dialect))
            if needs_column
            else _template
        )
        quoted_table = _quote_ident(table, dialect)
        where_sql = ""
        if predicates:
            from .predicates import to_sql

            where_sql, binds = to_sql(predicates, _quote_ident, dialect)
            where_sql = f" WHERE {where_sql}" if where_sql else ""
        if dim_expr:
            sql = (
                f"SELECT {dim_expr} AS {_quote_ident(dim_alias, dialect)}, "
                f"{agg_expr} AS {_quote_ident(metric_alias, dialect)} "
                f"FROM {quoted_table}{where_sql} GROUP BY {dim_expr} "
                f"ORDER BY {_quote_ident(metric_alias, dialect)} "
                f"{'DESC' if descending else 'ASC'}"
            )
            sql = _limit_clause(sql, dialect, row_limit)
        else:
            sql = (
                f"SELECT {agg_expr} AS {_quote_ident(metric_alias, dialect)} "
                f"FROM {quoted_table}{where_sql}"
            )
        query_text = sql

    try:
        from services.connector_store import get_connector
        from src.routers.query_router import QueryExecuteRequest, _is_safe_sql, _run_query

        saved = get_connector(cid)
        if not saved:
            return _tool_result(tool, success=False, error="Connector not found in store.")
        if dialect != "mongodb" and not _is_safe_sql(query_text):
            # Defensive: generated SQL is read-only by construction.
            return _tool_result(tool, success=False, error="Generated query was not read-only.")

        body = QueryExecuteRequest(
            connector_id=cid,
            query=query_text,
            collection=table if dialect == "mongodb" else "",
            limit=max(row_limit, 1),
            params=binds,
        )
        rows, result_cols, schema, truncated = _run_query(saved, body)
    except AmbiguousConnectorError as exc:
        return _tool_result(tool, success=False, error=exc.message)
    except Exception as exc:
        detail = str(getattr(exc, "detail", exc))
        _LOG.warning("aggregate_data failed: %s", detail, exc_info=True)
        return _tool_result(tool, success=False, error=f"Aggregation failed: {detail}")

    # D105: when LIMIT may have cut groups, the answer must name the true
    # group count and truncated=true — not present a partial top-N as exact.
    total_groups: int | None = None
    ties_at_cut = False
    if dim_col and rows:
        try:
            total_groups = _group_total(
                saved=saved,
                dialect=dialect,
                table=table,
                collection=table if dialect == "mongodb" else "",
                dim_col=dim_col,
                grain=grain,
                dim_expr=dim_expr if dialect != "mongodb" else "",
                quoted_table=quoted_table if dialect != "mongodb" else "",
                where_sql=where_sql if dialect != "mongodb" else "",
                binds=binds,
                match_stage=match_stage if dialect == "mongodb" else {},
            )
        except Exception:
            _LOG.debug("aggregate group-count probe failed", exc_info=True)
        if total_groups is not None and len(rows) < total_groups:
            truncated = True
            try:
                cut_value = rows[-1].get(metric_alias)
                if cut_value is not None:
                    ties_at_cut = bool(
                        _groups_at_metric(
                            saved=saved,
                            dialect=dialect,
                            table=table,
                            dim_col=dim_col,
                            grain=grain,
                            dim_expr=dim_expr if dialect != "mongodb" else "",
                            agg_expr=agg_expr if dialect != "mongodb" else "",
                            metric_alias=metric_alias,
                            quoted_table=quoted_table if dialect != "mongodb" else "",
                            where_sql=where_sql if dialect != "mongodb" else "",
                            binds=binds,
                            match_stage=match_stage if dialect == "mongodb" else {},
                            metric=metric_key,
                            measure_col=measure_col,
                            cut_value=cut_value,
                        )
                        > sum(
                            1 for r in rows if r.get(metric_alias) == cut_value
                        )
                    )
            except Exception:
                _LOG.debug("aggregate tie-at-cut probe failed", exc_info=True)
        elif total_groups is not None:
            truncated = False

    meta = {
        "connector_id": cid,
        "connector_name": conn.get("name"),
        "type": ctype,
        "table": table,
        "metric": metric_key,
        "column": measure_col or None,
        "group_by": dim_alias or None,
        "grain": grain or None,
        "where": (where or "").strip() or None,
        "query": query_text,
        "truncated": truncated,
        "filters": _describe_predicates(predicates),
        "filtered": bool(predicates),
    }
    result_id = _store_result(
        rows=rows,
        columns=result_cols,
        column_schema=schema,
        meta=meta,
        session_id=session_id,
        source=tool,
    )

    scalar: Any = None
    if not dim_col and rows:
        first = rows[0]
        scalar = first.get(metric_alias)
        if scalar is None and first:
            scalar = next(iter(first.values()))

    return _tool_result(
        tool,
        success=True,
        output={
            **meta,
            "result_id": result_id,
            "columns": result_cols,
            "metric_alias": metric_alias,
            # The true group count under the filters — not the LIMIT (D105).
            "group_count": (
                total_groups if total_groups is not None else (len(rows) if dim_col else 0)
            ),
            "shown_groups": len(rows) if dim_col else 0,
            "ties_at_cut": ties_at_cut,
            "value": scalar,
            "rows": rows[: min(row_limit, 50)],
            "exact": True,
            "read_only": True,
        },
    )


#: Counting every table on a connector is one round trip per table, so the fan-out
#: is bounded. A wider schema is reported as truncated rather than counted
#: partially and presented as the whole inventory.
_MAX_RANKED_TABLES = 40

#: Exact counts beyond the displayed ``limit`` — the catalog estimate can
#: mis-rank near the cut, so a few boundary candidates are counted too and the
#: real counts re-sort them.
_RANK_MARGIN = 5

#: Time a ranking may spend refining the estimate shortlist with exact
#: COUNT(*) (D201: QA's bar is a PostgreSQL ranking under 10s).
_RANK_COUNT_BUDGET_SECONDS = 6.0

#: Exact counts sent at once to one source.
_RANK_COUNT_PARALLEL = 4


def _estimate_table_row_counts(
    conn: dict[str, Any], names: list[str], *, whole_schema: bool = False
) -> dict[str, int]:
    """Bulk catalog row estimates — one query, never per-table COUNT(*).

    ``pg_class.reltuples`` / ``information_schema.tables.table_rows`` are
    planner estimates, not counts; they exist to shortlist which tables get
    the exact COUNT so ``limit`` bounds the work instead of only trimming
    the output (QA C12/C14 — 70 exact counts on million-row tables timed
    out the request). Engines without cheap stats return ``{}`` and the
    caller falls back to the bounded exact-count loop.
    """
    ctype = str(conn.get("type") or conn.get("format") or "").lower()
    try:
        from .schema_tools import _endpoint_from_connector
        from src.transfer.adapters import resolve_connector_config

        endpoint = _endpoint_from_connector(conn)
        cfg = resolve_connector_config(endpoint)
    except Exception:
        return {}
    wanted = {str(n).strip() for n in names if str(n).strip()}
    if not wanted:
        return {}
    sql = ""
    family = ""
    if ctype in {"postgresql", "postgres", "pg", "redshift", "greenplum", "cockroachdb", "timescaledb", "yugabytedb"}:
        family = "postgresql"
        # A partitioned table (relkind 'p') and a TimescaleDB hypertable keep
        # no rows of their own: their volume is the sum of their partitions /
        # chunks (pg_inherits, any depth). A table never ANALYZEd has
        # reltuples -1; the stats collector's n_live_tup still counts it
        # (D201: a hypertable ranked as empty and every parent fell to exact
        # COUNT(*) over all its chunks).
        sql = (
            "WITH RECURSIVE est AS ("
            " SELECT c.oid, CASE WHEN c.relkind = 'p' THEN NULL"
            "  WHEN c.reltuples >= 0 THEN c.reltuples::bigint"
            "  ELSE COALESCE(NULLIF(s.n_live_tup, 0), -1) END AS n"
            " FROM pg_class c LEFT JOIN pg_stat_all_tables s ON s.relid = c.oid"
            " WHERE c.relkind IN ('r','p')),"
            " tree AS ("
            " SELECT c.oid AS root, c.oid AS member FROM pg_class c WHERE c.relkind IN ('r','p')"
            " UNION ALL SELECT t.root, i.inhrelid FROM tree t JOIN pg_inherits i ON i.inhparent = t.member)"
            " SELECT c.relname, n.nspname,"
            "  CASE WHEN bool_and(e.n IS NULL OR e.n >= 0) AND bool_or(e.n >= 0)"
            "  THEN sum(GREATEST(e.n, 0))::bigint ELSE -1 END"
            " FROM tree t JOIN pg_class c ON c.oid = t.root"
            " JOIN pg_namespace n ON n.oid = c.relnamespace"
            " JOIN est e ON e.oid = t.member"
            " WHERE n.nspname NOT IN ('pg_catalog','information_schema')"
            " AND n.nspname NOT LIKE '\\_timescaledb%' ESCAPE '\\'"
            " GROUP BY c.relname, n.nspname"
        )
    elif ctype in {"mysql", "mariadb", "singlestore", "tidb"}:
        family = "mysql"
        sql = (
            "SELECT table_name, table_schema, table_rows "
            "FROM information_schema.tables WHERE table_type = 'BASE TABLE'"
        )
    if not sql:
        return {}
    try:
        from services.column_profile import _connect

        db = _connect(family, cfg)
        try:
            cur = db.cursor()
            cur.execute(sql)
            raw = cur.fetchall() or []
        finally:
            try:
                db.close()
            except Exception:
                pass
    except Exception:
        return {}
    from connectors.sql_identifiers import split_qualified_table

    stored: dict[tuple[str | None, str], int] = {}
    for relname, nsp, est in raw:
        try:
            n = int(est)
        except (TypeError, ValueError):
            continue
        # reltuples -1 means never analyzed — that is "no estimate", not 0.
        if n < 0:
            continue
        stored[(str(nsp or ""), str(relname or ""))] = n
    out: dict[str, int] = {}
    for name in wanted:
        sch, tbl = split_qualified_table(name, str(cfg.get("schema") or "") or None)
        hit = stored.get((sch or "", tbl))
        if hit is None and sch:
            hit = stored.get(("", tbl))
        if hit is None:
            hits = [v for (s, t), v in stored.items() if t == tbl]
            if len(hits) == 1:
                hit = hits[0]
        if hit is not None:
            out[name] = hit
    if whole_schema:
        # The object listing is capped; the catalog is not. Every table in the
        # connector's schema is ranked, listed or not (D24).
        home = str(cfg.get("schema") or "").strip()
        if home:
            for (sch, tbl), value in stored.items():
                if sch.lower() == home.lower() and tbl not in out:
                    out[tbl] = value
    return out


def _direct_row_count(conn: dict[str, Any], name: str) -> int | None:
    """One ``SELECT COUNT(*)`` on the endpoint's session (PostgreSQL / MySQL).

    The aggregation path grounds every count (introspection, a sample, the
    schema allow-list): eight catalog queries and a connect per table before
    the COUNT itself. A ranking names tables the catalog just listed, so it
    counts them directly; other engines keep the aggregation path (``None``).
    A count that fails or is cancelled by its time budget raises.
    """
    ctype = str(conn.get("type") or conn.get("format") or "").lower()
    if ctype in {"postgresql", "postgres", "pg", "timescaledb", "greenplum", "cockroachdb", "yugabytedb"}:
        family = "postgresql"
    elif ctype in {"mysql", "mariadb", "tidb"}:
        family = "mysql"
    else:
        return None
    try:
        from connectors.sql_identifiers import split_qualified_table
        from services.column_profile import _connect
        from src.transfer.adapters import resolve_connector_config

        from .schema_tools import _endpoint_from_connector

        cfg = resolve_connector_config(_endpoint_from_connector(conn))
        schema, table = split_qualified_table(name, str(cfg.get("schema") or "") or None)
        db = _connect(family, cfg)
    except Exception as exc:  # noqa: BLE001 - no session: the aggregation path decides
        _LOG.info("direct count of %s unavailable: %s", name, exc)
        return None
    if family == "postgresql":
        q = lambda part: '"' + str(part).replace('"', '""') + '"'  # noqa: E731
    else:
        q = lambda part: "`" + str(part).replace("`", "``") + "`"  # noqa: E731
    target = f"{q(schema)}.{q(table)}" if schema else q(table)
    try:
        cur = db.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {target}")  # nosec B608 - quoted identifiers
        row = cur.fetchone()
        try:
            db.rollback()
        except Exception:  # noqa: BLE001 - read-only
            pass
    finally:
        db.close()
    return int(row[0]) if row else None


def rank_connector_tables(
    connector_id: str = "",
    connector_name: str = "",
    order: str = "desc",
    limit: int = 10,
):
    """Exact row counts for the tables on one connector, ranked by size.

    "which table has the most rows on Demo Orders" had no tool behind it, so the
    planner read the superlative as an identifier and sampled a table literally
    named ``biggest`` — an invented name against a live database. This is the
    real answer: list the objects, count each one server-side through the same
    aggregate path as any other count, and rank what came back.

    Tables that cannot be counted are returned in ``skipped`` with their reason.
    A view that refuses a count is ordinary; dropping it from the ranking without
    saying so would present a partial inventory as the whole one.
    """
    tool = "rank_connector_tables"
    conn, err = _safe_connector(connector_id, connector_name, tool)
    if err:
        return err

    cid = str(conn.get("id") or conn.get("_id") or "")
    cname = str(conn.get("name") or connector_name or "")
    from .schema_tools import list_connector_objects

    listed = list_connector_objects(connector_id=cid, connector_name=cname)
    if not listed.success:
        return _tool_result(tool, success=False, error=listed.error)
    info = listed.output or {}
    names = [str(n).strip() for n in (info.get("objects") or []) if str(n).strip()]
    if not names:
        return _tool_result(
            tool,
            success=False,
            error=(
                f"**{cname}** reported no tables to rank"
                + (f" — {info.get('message')}" if info.get("message") else ".")
            ),
        )

    ascending = str(order or "desc").strip().lower() in {"asc", "ascending", "up"}
    shown = max(1, int(limit or 10))

    # ``limit`` must bound the *work*, not just the output (QA C12/C14):
    # exact-counting every object is one COUNT(*) per table and timed out on
    # warehouses with million-row tables. A bulk catalog estimate shortlists
    # limit+margin candidates; those alone get the exact COUNT and the real
    # counts re-sort. Engines without cheap stats keep the bounded loop.
    estimates = _estimate_table_row_counts(
        conn, names, **({"whole_schema": True} if info.get("truncated") else {})
    )
    if estimates:
        names = names + [n for n in estimates if n not in set(names)]
        # The estimate query covers every table in one round trip, so it ranks
        # the whole inventory; only the exact COUNTs are bounded, to the
        # shortlist. Cutting to the first names before ranking dropped a big
        # table that sorted late (D24).
        est_ranked = sorted(
            (n for n in names if n in estimates),
            key=lambda n: estimates[n],
            reverse=not ascending,
        )
        # Objects with no estimate (views, never-analyzed tables) cannot be
        # ranked by catalog — they join the count set within the same bound.
        unestimated = [n for n in names if n not in estimates]
        considered = (
            est_ranked[: shown + _RANK_MARGIN]
            + unestimated[: shown + _RANK_MARGIN]
        )
    else:
        considered = names[:_MAX_RANKED_TABLES]

    # The answer is a ranking: the catalog estimate already ranks every table.
    # Exact COUNT(*) refines the shortlist only as far as a step budget allows,
    # a few at a time; a count that does not fit keeps its estimate, labelled
    # (D201/D24: fifteen serial COUNTs on big PostgreSQL / TimescaleDB tables
    # ran past the 45s deadline and the call returned nothing).
    from .tool_deadline import Step, parallel_steps, step_seconds

    counts: dict[str, int] = {}
    skipped: list[dict[str, str]] = []
    count_deadline = time.monotonic() + step_seconds(_RANK_COUNT_BUDGET_SECONDS)

    def _count(name: str) -> Any:
        try:
            direct = _direct_row_count(conn, name)
        except Exception as exc:  # noqa: BLE001 - reported as not counted
            return _tool_result(tool, success=False, error=str(exc)[:200])
        if direct is not None:
            return _tool_result(tool, success=True, output={"value": direct})
        return aggregate_connector_data(
            connector_id=cid, connector_name=cname, table=name, metric="count"
        )

    # Tables the catalog cannot estimate are counted next, as far as the
    # budget goes: they cannot be ranked any other way.
    import collections
    import threading

    extra_unestimated = [n for n in names if n not in estimates and n not in set(considered)]
    queue = collections.deque(list(considered) + extra_unestimated)
    attempted: list[str] = []
    failures: dict[str, str] = {}
    qlock = threading.Lock()

    def _worker() -> None:
        # A few counters pull from one queue, so a slow table does not hold
        # the small ones behind it.
        while time.monotonic() < count_deadline - 0.05:
            with qlock:
                if not queue:
                    return
                name = queue.popleft()
                attempted.append(name)
            counted = _count(name)
            value = (counted.output or {}).get("value") if counted.success else None
            try:
                if value is None:
                    raise ValueError(str(counted.error or "count returned no value"))
                counts[name] = int(value)
            except (TypeError, ValueError) as exc:
                failures[name] = str(exc)

    parallel_steps(
        [
            Step(f"counting rows on {cname} (worker {i + 1})", _worker, conn, "source")
            for i in range(_RANK_COUNT_PARALLEL)
        ],
        seconds=max(0.0, count_deadline - time.monotonic()),
        bound_each=True,
    )
    with qlock:
        considered = list(dict.fromkeys(list(considered) + attempted))
        not_reached = list(queue)
    for name in considered + not_reached:
        if name in counts or name in estimates:
            continue
        skipped.append({
            "table": name,
            "error": failures.get(name)
            or "not counted within the ranking's time budget and the catalog has no estimate",
        })

    ranked: list[dict[str, Any]] = []
    for name in considered:
        if name in counts:
            ranked.append({"table": name, "rows": counts[name], "basis": "count"})
        elif name in estimates:
            ranked.append({"table": name, "rows": int(estimates[name]), "basis": "estimate"})
    # Tables outside the shortlist are ranked by estimate already; they are
    # not listed but they are part of the ranking (they sort below the cut).
    ranked.sort(key=lambda r: r["rows"], reverse=not ascending)
    estimated = [r["table"] for r in ranked[:shown] if r["basis"] == "estimate"]
    # ``exact`` means every shown row is an exact COUNT(*) over the full
    # requested population; a catalog-estimate shortlist can omit a table whose
    # stats are stale (D24), and an estimate shown is never exact.
    full_population = (
        len(considered) == len(names)
        and not bool(info.get("truncated"))
        and not estimated
        and not skipped
    )
    return _tool_result(
        tool,
        success=True,
        output={
            "connector_id": cid,
            "connector_name": cname,
            "type": info.get("type") or "",
            "order": "asc" if ascending else "desc",
            "ranked": ranked[:shown],
            # Tables given an exact COUNT(*); the rest of ``ranked`` is estimates.
            "counted": len(counts),
            "total_objects": int(info.get("total") or len(names)),
            "considered": len(considered),
            "total_rows": sum(r["rows"] for r in ranked),
            "skipped": skipped,
            # Rows shown from the catalog estimate because their exact count
            # did not fit the time budget. Never presented as exact.
            "estimated_tables": estimated,
            "row_count_basis": (
                "count" if not estimated else ("estimate" if not counts else "mixed")
            ),
            "truncated": len(names) > len(considered) or bool(info.get("truncated")),
            # The shortlist came from catalog estimates; every reported row
            # count is still an exact COUNT(*).
            "shortlisted_by": "catalog_estimate" if estimates else "all_tables",
            "exact": full_population,
            "read_only": True,
        },
    )
