"""Context resolution for Data Pilot follow-up turns.

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
    r"same|the same|that one|the table|that table|this table|"
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
    if not val or val.lower() in _PLATFORM_NOUNS:
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


# --------------------------------------------------------------------------
# 1. Clarification answers
# --------------------------------------------------------------------------


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
    # A fresh analytics / imperative sentence is never a slot answer, even when
    # it is short ("count orders now"). Let ordinary routing take it.
    if re.search(
        r"\b(?:how\s+many|count|sum|avg|average|total|top|bottom|select|show|list|"
        r"sample|analyze|filter|group(?:ed)?\s+by)\b",
        reply,
        re.I,
    ):
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
        # No candidate list (e.g. "which table?") — accept a bare identifier.
        if lower in _AFFIRMATIVE:
            return None
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_. \-]{0,60}", reply):
            if lower in {"i don't know", "not sure", "no", "nope", "none"}:
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


def _extract_edit_table(message: str) -> str:
    """"same for products", "now do orders", "what about the invoices table"."""
    m = re.search(
        r"\b(?:same\s+(?:for|on|with)|now\s+(?:do|try|for)|what\s+about|how\s+about|switch\s+to)\s+"
        r"(?:the\s+)?([A-Za-z_][A-Za-z0-9_.]{1,48})"
        r"(?:\s+(?:table|collection))?\b",
        message,
        re.I,
    )
    if not m:
        return ""
    table = _clean(m.group(1))
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
    if _COREFERENCE_RE.search(text):
        return True
    if re.match(r"^(?:and|also|now|then|what\s+about|how\s+about|ok\s+)", text, re.I):
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
    if re.match(r"^(?:where|filter)\b", text, re.I):
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
    if "which table" in lowered:
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
