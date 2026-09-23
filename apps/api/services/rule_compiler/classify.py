"""Deterministic classification of one rule cell.

This is the algorithm, not a model. The same text always yields the same kind.
Anything that is not a closed form becomes ``unknown`` and the compiler puts
it on the review queue — that is how we refuse silent invention.

Closed forms map onto engines that already exist (Map write transform,
``code_crosswalk`` / G20, or a named shape step). No fourth runtime.
"""

from __future__ import annotations

import re
from typing import Any

# Closed vocabulary operators actually type in mapping workbooks.
_DIRECT = re.compile(
    r"^(?:direct|1\s*[:\-–]\s*1|1\s+to\s+1|map|mapped|passthrough|"
    r"pass[\s-]?through|as[\s-]?is|preserve|identity|copy|none|n/?a|-)?$",
    re.I,
)
_OMIT = re.compile(
    r"\b(?:omit|exclude|skip|ignore|drop|do\s+not\s+(?:map|load|transfer|send))\b",
    re.I,
)
_LOWER = re.compile(r"\b(?:lower(?:case)?|lcase|tolower)\b", re.I)
_UPPER = re.compile(r"\b(?:upper(?:case)?|ucase|toupper)\b", re.I)
_TRIM = re.compile(r"\b(?:trim|strip(?:\s+space)?)\b", re.I)
_DATE = re.compile(
    r"\b(?:date|datetime|timestamp|to\s+iso|iso-?8601|"
    r"mm\s*/\s*dd\s*/\s*yyyy|dd\s*/\s*mm\s*/\s*yyyy|yyyy-mm-dd|"
    r"parse\s+date|convert.{0,20}date)\b",
    re.I,
)
_EMAIL = re.compile(r"\bemail\b", re.I)
_PHONE = re.compile(r"\bphone\b", re.I)
_DEFAULT = re.compile(
    r"\b(?:default(?:\s+to)?|if\s+null|nvl|coalesce)\b\s*[:\s]+(?P<value>.+)$",
    re.I,
)
_DERIVE = re.compile(
    r"(?P<expr>[A-Za-z_][\w.]*\s*[*+/x×]\s*[\d.]+"
    r"|[\d.]+\s*[*+/x×]\s*[A-Za-z_][\w.]*)",
    re.I,
)
_CONCAT_HINT = re.compile(
    r"\b(?:concat(?:enate)?|combine|join\s+(?:columns?|fields?))\b",
    re.I,
)
_CONCAT_EXPR = re.compile(
    r"([A-Za-z_][\w.]*)\s*(?:\+|\&|\|\|)\s*([A-Za-z_][\w.]*)",
)
_REPLACE = re.compile(
    r"\breplace\s+['\"]?(?P<search>.+?)['\"]?\s+(?:with|by)\s+['\"]?(?P<repl>.*)$",
    re.I,
)
_NULL_IF = re.compile(
    r"\b(?:null\s+if|treat\s+as\s+null|sentinel)\b\s*[:\s]+(?P<values>.+)$",
    re.I,
)
_CONSTANT = re.compile(
    r"\b(?:constant|literal|always|set\s+to)\b\s*[:\s]+(?P<value>.+)$",
    re.I,
)
_PAD = re.compile(
    r"\bpad(?:\s+(?P<side>left|right))?\s+(?:to\s+)?(?P<width>\d+)",
    re.I,
)
_SPLIT = re.compile(
    r"\bsplit\b.*?(?:on|by|at)\s+['\"]?(?P<sep>[^'\"]+?)['\"]?(?:\s|$)",
    re.I,
)
_ROUND = re.compile(r"\bround(?:\s+to)?\s+(?P<places>\d+)", re.I)
_HASH = re.compile(
    r"\b(?:hash(?:\s+pii)?|mask|anonymi[sz]e|redact|one[\s-]?way)\b",
    re.I,
)
_INT = re.compile(
    r"\b(?:cast|parse|convert)\s+(?:to\s+)?int(?:eger)?\b|\binteger\b",
    re.I,
)
_NUM = re.compile(
    r"\b(?:cast|parse|convert)\s+(?:to\s+)?(?:decimal|numeric|number|float)\b"
    r"|\b(?:decimal|numeric|float)\b",
    re.I,
)
_BOOL = re.compile(r"\b(?:boolean|bool|true\s*/\s*false)\b", re.I)
_NOT_COLUMN = frozenset({
    "lowercase", "uppercase", "lower", "upper", "validate", "trim", "strip",
    "parse", "cast", "direct", "omit", "email", "phone", "hash", "replace",
    "default", "null", "concat", "concatenate", "combine", "and", "or",
    "convert", "normalize", "format",
})
_CURRENCY = re.compile(r"\b(?:currency|money|dollar|gbp|eur)\b", re.I)
_PERCENT = re.compile(r"\bpercent(?:age)?\b", re.I)
# Table-level joins / VLOOKUP — not a pre-load shape op.
_JOIN = re.compile(
    r"\b(?:left\s+join|inner\s+join|right\s+join|full\s+join|"
    r"vlookup|xlookup|lookup\s+from|join\s+(?:to|with|on))\b",
    re.I,
)
# A → ACTIVE  |  A=ACTIVE  |  A:ACTIVE
_LOOKUP_PAIR = re.compile(
    r"([A-Za-z0-9_.-]+)\s*(?:→|->|=>|=|:)\s*([A-Za-z0-9_./ -]+)",
)
_LOOKUP_HINT = re.compile(
    r"\b(?:lookup|crosswalk|decode|code\s+map|map(?:ping)?\s+to)\b",
    re.I,
)
_DATE_TOKEN = re.compile(r"^(?:Y{2,4}|M{1,2}|D{1,2}|H{1,2}|S{1,2})$", re.I)


def parse_lookup(text: str) -> dict[str, str]:
    """Closed code table from a cell.

    Two or more pairs always win. A single pair is accepted only when it looks
    like a code (``A → ACTIVE``) — never a date format (``YYYY → YYYY-MM-DD``).
    """
    pairs: dict[str, str] = {}
    for key, value in _LOOKUP_PAIR.findall(text or ""):
        k = key.strip()
        v = value.strip().rstrip(",")
        if k and v and k.lower() not in {"if", "when", "default"}:
            pairs[k] = v
    if len(pairs) >= 2:
        return pairs
    if len(pairs) == 1:
        key, value = next(iter(pairs.items()))
        if _DATE.search(text or ""):
            return {}
        if _LOOKUP_HINT.search(text or "") or (
            _looks_like_code(key) and _looks_like_code(value)
        ):
            return pairs
    return {}


def _looks_like_code(token: str) -> bool:
    text = (token or "").strip()
    if not text or len(text) > 32:
        return False
    if _DATE_TOKEN.fullmatch(text):
        return False
    return True


def _concat_columns(text: str) -> tuple[list[str], str]:
    """Named columns and an optional separator from a concat cell."""
    columns: list[str] = []
    for left, right in _CONCAT_EXPR.findall(text or ""):
        for part in (left, right):
            name = part.strip()
            if name and name not in columns:
                columns.append(name)
    if not columns:
        # ``concat first_name and last_name`` / ``concatenate a, b, c``
        tail = re.sub(r"^(?:concat(?:enate)?|combine)\s+", "", text or "", flags=re.I)
        tail = re.sub(r"\b(?:and|with|into)\b", ",", tail, flags=re.I)
        for part in re.split(r"[,\s]+", tail):
            name = part.strip(" \"'")
            if re.fullmatch(r"[A-Za-z_][\w.]*", name or "") and name.lower() not in {
                "concat", "concatenate", "combine", "columns", "fields", "join",
            }:
                if name not in columns:
                    columns.append(name)
    separator = ""
    sep_match = re.search(r"(?:sep(?:arator)?|delimited?\s+by)\s+['\"](.+?)['\"]", text or "", re.I)
    if sep_match:
        separator = sep_match.group(1)
    elif "space" in (text or "").lower() and "concat" in (text or "").lower():
        separator = " "
    return columns, separator


def classify_rule(text: str) -> dict[str, Any]:
    """One rule cell → kind, plane hint, and any structured payload.

    Order is specific-to-general: a lookup that also says "map" is a lookup,
    not Direct. Unknown is a status, not a guess.
    """
    raw = (text or "").strip()
    if _DIRECT.match(raw):
        return {"kind": "direct", "plane": "map", "confidence": 0.99}

    lookup = parse_lookup(raw)
    if lookup:
        return {
            "kind": "lookup",
            "plane": "map",
            "confidence": 0.98,
            "mapping": lookup,
        }
    if _LOOKUP_HINT.search(raw) and not lookup:
        return {
            "kind": "unknown",
            "plane": "review",
            "confidence": 0.4,
            "reason": "lookup named but no A → B pairs were found",
        }

    if _JOIN.search(raw):
        return {
            "kind": "join",
            "plane": "review",
            "confidence": 0.4,
            "reason": (
                "Join / VLOOKUP is not a pre-load rule. Name join keys on a "
                "Joins sheet or write the projection as Source → query; "
                "this compiler will not invent a grain."
            ),
        }

    if _OMIT.search(raw):
        return {"kind": "omit", "plane": "map", "confidence": 0.97}

    concat_expr = _CONCAT_EXPR.search(raw)
    concat_hint = bool(_CONCAT_HINT.search(raw))
    concat_tokens_ok = False
    if concat_expr:
        left, right = concat_expr.group(1), concat_expr.group(2)
        concat_tokens_ok = (
            left.lower() not in _NOT_COLUMN and right.lower() not in _NOT_COLUMN
        )
    if concat_hint or concat_tokens_ok:
        columns, separator = _concat_columns(raw)
        columns = [c for c in columns if c.lower() not in _NOT_COLUMN]
        return {
            "kind": "concat",
            "plane": "shape",
            "confidence": 0.94 if len(columns) >= 2 else 0.55,
            "columns": columns,
            "separator": separator,
            **({} if len(columns) >= 2 else {
                "reason": "concat named but fewer than two columns were identified",
            }),
        }

    derive = _DERIVE.search(raw)
    if derive:
        expr = derive.group("expr").replace("×", "*").replace("x", "*").replace("X", "*")
        expr = re.sub(r"\s+", " ", expr)
        return {
            "kind": "derive",
            "plane": "shape",
            "confidence": 0.95,
            "expression": expr,
        }

    replace = _REPLACE.search(raw)
    if replace:
        return {
            "kind": "replace",
            "plane": "shape",
            "confidence": 0.93,
            "search": replace.group("search").strip().strip("\"'"),
            "replacement": (replace.group("repl") or "").strip().strip("\"'"),
        }

    default = _DEFAULT.search(raw)
    if default:
        return {
            "kind": "default",
            "plane": "shape",
            "confidence": 0.93,
            "value": default.group("value").strip().strip("\"'"),
        }

    null_if = _NULL_IF.search(raw)
    if null_if:
        values = [
            part.strip().strip("\"'")
            for part in re.split(r"[|,;/]", null_if.group("values") or "")
            if part.strip()
        ]
        return {
            "kind": "null_if",
            "plane": "shape",
            "confidence": 0.92,
            "values": values,
        }

    constant = _CONSTANT.search(raw)
    if constant:
        return {
            "kind": "constant",
            "plane": "shape",
            "confidence": 0.93,
            "value": constant.group("value").strip().strip("\"'"),
        }

    pad = _PAD.search(raw)
    if pad:
        return {
            "kind": "pad",
            "plane": "shape",
            "confidence": 0.92,
            "width": int(pad.group("width")),
            "side": (pad.group("side") or "left").lower(),
        }

    split = _SPLIT.search(raw)
    if split:
        return {
            "kind": "split",
            "plane": "shape",
            "confidence": 0.9,
            "separator": split.group("sep").strip().strip("\"'"),
        }

    rounded = _ROUND.search(raw)
    if rounded:
        return {
            "kind": "round",
            "plane": "shape",
            "confidence": 0.93,
            "places": int(rounded.group("places")),
        }

    if _HASH.search(raw):
        return {"kind": "hash", "plane": "map", "confidence": 0.95}
    if _EMAIL.search(raw):
        return {"kind": "email", "plane": "map", "confidence": 0.96}
    if _PHONE.search(raw):
        return {"kind": "phone", "plane": "map", "confidence": 0.96}
    if _DATE.search(raw):
        return {"kind": "date", "plane": "map", "confidence": 0.96}
    if _CURRENCY.search(raw):
        return {"kind": "currency", "plane": "map", "confidence": 0.95}
    if _PERCENT.search(raw):
        return {"kind": "percentage", "plane": "map", "confidence": 0.95}
    if _INT.search(raw):
        return {"kind": "cast_integer", "plane": "map", "confidence": 0.95}
    if _NUM.search(raw):
        return {"kind": "cast_number", "plane": "map", "confidence": 0.95}
    if _BOOL.search(raw):
        return {"kind": "cast_boolean", "plane": "map", "confidence": 0.95}
    if _LOWER.search(raw):
        return {"kind": "case_lower", "plane": "map", "confidence": 0.97}
    if _UPPER.search(raw):
        return {"kind": "case_upper", "plane": "map", "confidence": 0.97}
    if _TRIM.search(raw):
        return {"kind": "trim", "plane": "map", "confidence": 0.97}

    if not raw:
        return {"kind": "direct", "plane": "map", "confidence": 0.99}
    return {
        "kind": "unknown",
        "plane": "review",
        "confidence": 0.35,
        "reason": "rule text is not a closed form this compiler can execute",
    }
