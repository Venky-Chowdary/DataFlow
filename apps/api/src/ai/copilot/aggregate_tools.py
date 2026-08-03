"""Natural-language aggregation for Data Pilot — real GROUP BY against live tables.

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
from dataclasses import dataclass, field
from typing import Any

from .query_tools import _MAX_QUERY_ROWS, _quote_ident, _store_result, _tool_result
from .schema_tools import (
    AmbiguousConnectorError,
    _endpoint_from_connector,
    _normalize_columns,
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

_TEMPORAL_GRAINS = {
    "day": "day", "daily": "day", "date": "day",
    "week": "week", "weekly": "week",
    "month": "month", "monthly": "month",
    "quarter": "quarter", "quarterly": "quarter",
    "year": "year", "yearly": "year", "annually": "year",
}

_TEMPORAL_TYPE_HINTS = ("date", "time", "timestamp")

# DataFlow's own objects, not warehouse tables. "how many jobs failed" belongs to
# list_jobs, not to SELECT COUNT(*) FROM jobs — unless the user named a connector,
# which means they really do mean a table with that name.
_PLATFORM_NOUNS = frozenset({
    "connector", "connectors", "connection", "connections",
    "job", "jobs", "transfer", "transfers", "run", "runs",
    "schedule", "schedules", "pipeline", "pipelines",
    "contract", "contracts", "dataset", "datasets",
    "table", "tables", "collection", "collections", "column", "columns",
    "workspace", "workspaces", "preflight", "quarantine",
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
)

_ASCENDING_HINTS = ("ascending", "lowest", "smallest", "bottom", "least", "fewest")

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
_STOP_TOKENS = frozenset({
    "do", "does", "did", "i", "we", "you", "your", "my", "our", "is", "are",
    "was", "were", "have", "has", "had", "there", "here", "please", "thanks",
    "that", "this", "to", "and", "or", "be", "been", "get", "got", "show",
    "me", "tell", "currently", "right", "now", "again", "still",
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
        and (req.table.lower() in _PLATFORM_NOUNS or table_tokens[0] in _PLATFORM_NOUNS)
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
    if (dialect or "").lower() in {"mssql", "sqlserver"}:
        return sql.replace("SELECT ", f"SELECT TOP {int(limit)} ", 1)
    return f"{sql} LIMIT {int(limit)}"


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


# --------------------------------------------------------------------------
# The tool
# --------------------------------------------------------------------------


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
            return _tool_result(
                tool,
                success=False,
                error=(
                    f"Column '{wanted}' is not in {table}. "
                    f"Available columns: {', '.join(names[:20])}."
                ),
            )
        if metric_key in {"sum", "avg"}:
            col_type = _column_type(columns, measure_col).lower()
            if col_type and any(hint in col_type for hint in _NON_NUMERIC_HINTS):
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
        dim_col = resolve_name(wanted, names)
        if not dim_col:
            # "by month" is a time grain, not a column — find the date column.
            grain = _TEMPORAL_GRAINS.get(wanted.lower(), "")
            if grain:
                temporal = [n for n in names if _is_temporal(_column_type(columns, n))]
                if len(temporal) == 1:
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
                return _tool_result(
                    tool,
                    success=False,
                    error=(
                        f"Column '{wanted}' is not in {table}. "
                        f"Available columns: {', '.join(names[:20])}."
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
            parsed_filters, _ = parse_filters(where)
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
            "group_count": len(rows) if dim_col else 0,
            "value": scalar,
            "rows": rows[: min(row_limit, 50)],
            "exact": True,
            "read_only": True,
        },
    )
