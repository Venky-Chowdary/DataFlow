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
    r"\b(?:omit|do\s+not\s+(?:map|load|transfer|send)|"
    r"(?:skip|drop|exclude|ignore)(?!\s+(?:rows?|columns?|fields?)\b))\b",
    re.I,
)
_LOWER = re.compile(r"\b(?:lower(?:case)?|lcase|tolower)\b", re.I)
_UPPER = re.compile(r"\b(?:upper(?:case)?|ucase|toupper)\b", re.I)
_TITLE = re.compile(r"\b(?:title[\s-]?case|proper(?:\s+case)?|initcap)\b", re.I)
_TRIM = re.compile(r"\b(?:trim|strip(?:\s+space)?|ltrim|rtrim)\b", re.I)
_COLLAPSE = re.compile(r"\b(?:collapse\s+whitespace|squeeze\s+spaces?|normalize\s+spaces?)\b", re.I)
_STRIP_CTRL = re.compile(
    r"\b(?:strip\s+controls?|non[\s-]?printable|zero[\s-]?width|control\s+char)\b",
    re.I,
)
_DATE = re.compile(
    r"\b(?:date|datetime|timestamp|to\s+iso|iso-?8601|"
    r"mm\s*/\s*dd\s*/\s*yyyy|dd\s*/\s*mm\s*/\s*yyyy|yyyy-mm-dd|"
    r"parse\s+date|convert.{0,20}date)\b",
    re.I,
)
_TIME = re.compile(r"\b(?:time(?:[\s-]?of[\s-]?day)?|hh:mm(?::ss)?)\b", re.I)
_EMAIL = re.compile(r"\bemail\b", re.I)
_PHONE = re.compile(r"\bphone\b", re.I)
_DEFAULT = re.compile(
    r"\b(?:default(?:\s+to)?|if\s+null(?:\s+then)?|nvl|coalesce)\b\s*[:\s]+(?P<value>.+)$",
    re.I,
)
_DEFAULT_BARE = re.compile(
    r"\b(?:default(?:\s+to)?|if\s+null(?:\s+then)?)\s+(?P<value>\S+)$",
    re.I,
)
_DERIVE = re.compile(
    r"(?P<expr>[A-Za-z_][\w.]*\s*[*+/x×-]\s*[\d.]+"
    r"|[\d.]+\s*[*+/x×-]\s*[A-Za-z_][\w.]*)",
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
    r"\b(?:replace|substitute)\s+['\"]?(?P<search>.+?)['\"]?\s+(?:with|by)\s+['\"]?(?P<repl>.*)$",
    re.I,
)
_NULL_IF = re.compile(
    r"\b(?:null\s+if|treat\s+as\s+null|sentinel|nullif)\b\s*[:\s]+(?P<values>.+)$",
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
_TRUNCATE = re.compile(r"\btruncat(?:e|ion)\s+(?:to\s+)?(?P<places>\d+)", re.I)
_ABS = re.compile(r"\b(?:abs\s*\(|absolute\s+value)\b", re.I)
_CLAMP = re.compile(
    r"\bclamp\b.*?(\d+(?:\.\d+)?)\s*(?:to|-|,)\s*(\d+(?:\.\d+)?)",
    re.I,
)
_SUBSTR = re.compile(
    r"\b(?:substr(?:ing)?|mid)\s*\(\s*(?P<col>[A-Za-z_][\w.]*)\s*,\s*(?P<start>\d+)\s*(?:,\s*(?P<length>\d+))?\s*\)",
    re.I,
)
_LEFT = re.compile(
    r"\bleft\s*\(\s*(?P<col>[A-Za-z_][\w.]*)\s*,\s*(?P<length>\d+)\s*\)",
    re.I,
)
_RIGHT = re.compile(
    r"\bright\s*\(\s*(?P<col>[A-Za-z_][\w.]*)\s*,\s*(?P<length>\d+)\s*\)",
    re.I,
)
_PREFIX = re.compile(
    r"\b(?:prefix|prepend)\s+['\"](?P<value>.+?)['\"]",
    re.I,
)
_SUFFIX = re.compile(
    r"\b(?:suffix|append)\s+['\"](?P<value>.+?)['\"]",
    re.I,
)
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
_JSON = re.compile(r"\b(?:parse\s+json|json(?:b)?|struct)\b", re.I)
_BINARY = re.compile(r"\b(?:binary|base64|bytes)\b", re.I)
_UNICODE = re.compile(r"\b(?:unicode|nfc|nfd|normalize\s+unicode)\b", re.I)
_ZONE = re.compile(r"\b(?:timezone|time\s+zone|iana|assume\s+zone)\b", re.I)
_NOT_COLUMN = frozenset({
    "lowercase", "uppercase", "lower", "upper", "validate", "trim", "strip",
    "parse", "cast", "direct", "omit", "email", "phone", "hash", "replace",
    "default", "null", "concat", "concatenate", "combine", "and", "or",
    "convert", "normalize", "format", "title", "proper",
})
_CURRENCY = re.compile(r"\b(?:currency|money|dollar)\b", re.I)
_PERCENT = re.compile(r"\bpercent(?:age)?\b", re.I)
_JOIN = re.compile(
    r"\b(?:left\s+join|inner\s+join|right\s+join|full\s+join|"
    r"vlookup|xlookup|lookup\s+from|join\s+(?:to|with|on))\b",
    re.I,
)
_FILTER = re.compile(
    r"\b(?:keep|only|where|filter)\s+(?:rows?\s+)?(?:if|where)?\s*"
    r"(?P<col>[A-Za-z_][\w.]*)\s*(?P<op>=|!=|<>|>=|<=|>|<)\s*(?P<val>.+)$",
    re.I,
)
_EXCLUDE_ROWS = re.compile(
    r"\b(?:exclude|drop|omit)\s+rows?\s+(?:where|if)\s+"
    r"(?P<col>[A-Za-z_][\w.]*)\s*(?P<op>=|!=|<>|>=|<=|>|<)\s*(?P<val>.+)$",
    re.I,
)
_DIVERT = re.compile(
    r"\b(?:divert|quarantine)\s+(?:rows?\s+)?(?:where|if)\s+"
    r"(?P<col>[A-Za-z_][\w.]*)\s*(?P<op>=|!=|<>|>=|<=|>|<)\s*(?P<val>.+)$",
    re.I,
)
_IF_FN = re.compile(
    r"\bif\s*\(\s*(?P<cond>.+?)\s*,\s*(?P<then>.+?)\s*(?:,\s*(?P<else>.+?))?\s*\)\s*$",
    re.I,
)
_REQUIRED = re.compile(r"\b(?:required|mandatory|not\s+null|non[\s-]?null)\b", re.I)
_UNIQUE = re.compile(r"\b(?:unique|primary\s+key|\bpk\b)\b", re.I)
_LOOKUP_PAIR = re.compile(
    r"([A-Za-z0-9_.-]+)\s*(?:→|->|=>|=|:)\s*([A-Za-z0-9_./ -]+)",
)
_LOOKUP_CORR = re.compile(
    r"(?:^|[,;]\s*)"
    r"(?P<keys>blank|empty|null|missing|"
    r"(?:[A-Za-z0-9_.-]+(?:\s*(?:[,|/]|or|and)\s*[A-Za-z0-9_.-]+)*))"
    r"\s*(?:→|->|=>|=|:)\s*"
    r"(?P<val>[A-Za-z0-9_./ -]+?)"
    r"(?=\s*[,;]\s*(?:blank|empty|null|missing|"
    r"[A-Za-z0-9_.-]+(?:\s*(?:[,|/]|or|and)\s*[A-Za-z0-9_.-]+)*)"
    r"\s*(?:→|->|=>|=|:)|$)",
    re.I,
)
_LOOKUP_HINT = re.compile(
    r"\b(?:lookup|crosswalk|decode|code\s+map|map(?:ping)?\s+to)\b",
    re.I,
)
_DATE_TOKEN = re.compile(r"^(?:Y{2,4}|M{1,2}|D{1,2}|H{1,2}|S{1,2})$", re.I)
_EXCEL_FN = re.compile(
    r"^=?\s*(?P<fn>UPPER|LOWER|TRIM|PROPER|CONCATENATE|CONCAT|TEXTJOIN|"
    r"SUBSTITUTE|REPLACE|LEFT|RIGHT|MID|ABS|ROUND|IF|IFERROR|VLOOKUP|XLOOKUP|"
    r"LEN|LENGTH|VALUE|TEXT|DATEVALUE|TO_DATE|TO_NUMBER|TO_CHAR|"
    r"CAST|COALESCE|IFNULL|NVL|NULLIF)\s*\(",
    re.I,
)
_CAST_SQL = re.compile(
    r"\bcast\s*\(\s*(?P<col>[A-Za-z_][\w.]*)\s+as\s+"
    r"(?P<type>int(?:eger)?|decimal|numeric|number|float|bool(?:ean)?|"
    r"date|timestamp|text|varchar)\s*\)",
    re.I,
)
_PG_CAST = re.compile(
    r"\b(?P<col>[A-Za-z_][\w.]*)\s*::\s*"
    r"(?P<type>int(?:eger)?|decimal|numeric|number|float|bool(?:ean)?|date|timestamp|text|varchar)\b",
    re.I,
)
_COALESCE = re.compile(
    r"\b(?:coalesce|ifnull|nvl)\s*\(\s*(?P<col>[A-Za-z_][\w.]*)\s*,\s*(?P<value>.+?)\s*\)",
    re.I,
)
_NULLIF_FN = re.compile(
    r"\bnullif\s*\(\s*(?P<col>[A-Za-z_][\w.]*)\s*,\s*(?P<value>.+?)\s*\)",
    re.I,
)
_LEN = re.compile(r"\b(?:len|length)\s*\(\s*(?P<col>[A-Za-z_][\w.]*)\s*\)", re.I)
_TO_DATE = re.compile(r"\bto_date\b", re.I)
_TO_NUMBER = re.compile(r"\bto_number\b", re.I)
_TO_CHAR = re.compile(r"\bto_char\b", re.I)
_CHAIN_SPLIT = re.compile(r"\s*(?:;|\bthen\b)\s*", re.I)
_CLEANSE_KINDS = frozenset({
    "trim", "case_lower", "case_upper", "title", "collapse", "strip_controls",
    "email", "phone", "hash", "unicode",
})
_DATE_MASK = re.compile(
    r"(?P<mask>Y{2,4}M{2}D{2}|D{2}M{2}Y{4}|M{2}D{2}Y{4}|"
    r"(?:Y{2,4}|M{1,4}|D{1,2}|MON)(?:[-/.](?:Y{2,4}|M{1,4}|D{1,2}|MON)){1,2})",
    re.I,
)
_AMBIGUOUS_DATE = re.compile(
    r"\b(?:mm\s*/\s*dd|dd\s*/\s*mm)\b.+\b(?:dd\s*/\s*mm|mm\s*/\s*dd)\b",
    re.I,
)
_DATE_WORDS = re.compile(
    r"\b(?:date|datetime|timestamp|convert|parse|to|iso|iso-?8601|format)\b",
    re.I,
)
_NAMED_REF = re.compile(
    r"^(?:%(?P<pct>[A-Za-z_][\w]*)%|"
    r"(?:use|apply|rule)\s+(?P<use>[A-Za-z_][\w]*)|"
    r"(?P<call>[A-Za-z_][\w]*)\s*\(\s*\))$",
    re.I,
)
_APPLY_TO = re.compile(
    r"^(?:apply|use)\s+(?P<name>[A-Za-z_][\w]*)\s+to\s+(?P<cols>.+)$",
    re.I,
)
_UNMAPPED_DEFAULT = re.compile(
    r"(?:(?:unmapped|unknown|unmatched|else)(?:\s+codes?)?|\*)\s*"
    r"(?:→|->|=>|:|=|to)\s*(?P<value>.+)$",
    re.I,
)
_UNMAPPED_DEFAULT_PHRASE = re.compile(
    r"\bdefault\s+(?:unmapped|unknown|unmatched)(?:\s+codes?)?\s+(?:to\s+)?(?P<value>.+)$",
    re.I,
)
_UNMAPPED_DIVERT = re.compile(
    r"\b(?:quarantine|divert)\s+(?:unknown|unmapped|unmatched)(?:\s+codes?)?\b",
    re.I,
)
_DECODE = re.compile(
    r"^=?\s*decode\s*\(\s*(?P<col>[A-Za-z_][\w.]*)\s*,\s*(?P<args>.+?)\s*\)\s*;?\s*$",
    re.I | re.S,
)
_CASE_BLOCK = re.compile(
    r"^=?\s*case\b(?P<body>.+)\bend\s*;?\s*$",
    re.I | re.S,
)
_SIMPLE_CASE_COL = re.compile(
    r"^\s*(?P<col>[A-Za-z_][\w.]*)\s+(?P<rest>when\b.+)$",
    re.I | re.S,
)
_WHEN_THEN = re.compile(
    r"when\s+(?P<cond>.+?)\s+then\s+(?P<then>.+?)(?=\s+when\b|\s+else\b|\s*$)",
    re.I | re.S,
)
_CASE_ELSE = re.compile(r"\belse\s+(?P<else>.+?)\s*$", re.I | re.S)
_EQ_ATOM = re.compile(
    r"^(?P<col>[A-Za-z_][\w.]*)\s*=\s*(?P<val>.+)$",
    re.I,
)
_IN_FILTER = re.compile(
    r"\b(?:keep|only|where|filter)\s+(?:rows?\s+)?(?:if|where)?\s*"
    r"(?P<col>[A-Za-z_][\w.]*)\s+in\s*\((?P<vals>.+?)\)",
    re.I,
)
_EXCLUDE_IN = re.compile(
    r"\b(?:exclude|drop|omit)\s+rows?\s+(?:where|if)\s+"
    r"(?P<col>[A-Za-z_][\w.]*)\s+in\s*\((?P<vals>.+?)\)",
    re.I,
)
_BETWEEN = re.compile(
    r"\b(?:keep|only|where|filter)\s+(?:rows?\s+)?(?:if|where)?\s*"
    r"(?P<col>[A-Za-z_][\w.]*)\s+between\s+(?P<lo>\S+)\s+and\s+(?P<hi>\S+)",
    re.I,
)
_EXCLUDE_BETWEEN = re.compile(
    r"\b(?:exclude|drop|omit)\s+rows?\s+(?:where|if)\s+"
    r"(?P<col>[A-Za-z_][\w.]*)\s+between\s+(?P<lo>\S+)\s+and\s+(?P<hi>\S+)",
    re.I,
)
_LIKE = re.compile(
    r"\b(?:keep|only|where|filter)\s+(?:rows?\s+)?(?:if|where)?\s*"
    r"(?P<col>[A-Za-z_][\w.]*)\s+(?P<not>not\s+)?like\s+['\"](?P<pat>.+?)['\"]",
    re.I,
)
_EXCLUDE_LIKE = re.compile(
    r"\b(?:exclude|drop|omit)\s+rows?\s+(?:where|if)\s+"
    r"(?P<col>[A-Za-z_][\w.]*)\s+(?P<not>not\s+)?like\s+['\"](?P<pat>.+?)['\"]",
    re.I,
)
_ISNULL_FILTER = re.compile(
    r"\b(?:keep|only|where|filter)\s+(?:rows?\s+)?(?:if|where)?\s*"
    r"(?P<col>[A-Za-z_][\w.]*)\s+is\s+(?P<not>not\s+)?null\b",
    re.I,
)
_EXCLUDE_NULL = re.compile(
    r"\b(?:exclude|drop|omit)\s+rows?\s+(?:where|if)\s+"
    r"(?P<col>[A-Za-z_][\w.]*)\s+is\s+(?P<not>not\s+)?null\b",
    re.I,
)
_NOT_IN = re.compile(
    r"\b(?:keep|only|where|filter)\s+(?:rows?\s+)?(?:if|where)?\s*"
    r"(?P<col>[A-Za-z_][\w.]*)\s+not\s+in\s*\((?P<vals>.+?)\)",
    re.I,
)
_EXCLUDE_NOT_IN = re.compile(
    r"\b(?:exclude|drop|omit)\s+rows?\s+(?:where|if)\s+"
    r"(?P<col>[A-Za-z_][\w.]*)\s+not\s+in\s*\((?P<vals>.+?)\)",
    re.I,
)
_KEEP_COLS = re.compile(
    r"\bkeep\s+(?:only\s+)?(?:columns?|fields?)\s+(?P<cols>.+)$",
    re.I,
)
_DROP_COL = re.compile(
    r"\b(?:drop|remove)\s+(?:column|field)s?\s+(?P<col>[A-Za-z_][\w.]*)",
    re.I,
)
_FLATTEN = re.compile(r"\bflatten\s+json\b", re.I)
_CASE_INSENSITIVE = re.compile(r"\bcase[\s-]?insensitive\b", re.I)
_IIF = re.compile(r"^=?\s*iif\s*\(", re.I)
_NVL2 = re.compile(
    r"^=?\s*nvl2\s*\(\s*(?P<col>[A-Za-z_][\w.]*)\s*,\s*(?P<present>.+?)\s*,\s*(?P<missing>.+?)\s*\)\s*$",
    re.I | re.S,
)
_REG_EXTRACT = re.compile(
    r"\b(?:reg(?:ex)?_extract|regexp_substr|regexp_extract)\s*\(\s*"
    r"(?P<col>[A-Za-z_][\w.]*)\s*,\s*['\"](?P<pat>.+?)['\"]"
    r"(?:\s*,\s*(?P<grp>\d+))?",
    re.I,
)
_REG_REPLACE = re.compile(
    r"\b(?:reg(?:ex)?_replace|regexp_replace)\s*\(\s*"
    r"(?P<col>[A-Za-z_][\w.]*)\s*,\s*['\"](?P<pat>.+?)['\"]"
    r"\s*,\s*['\"](?P<repl>.*)['\"]",
    re.I,
)
_HASH_ID = re.compile(
    r"\bhash\s+identity\b(?:\s+(?:of|on|from|over))?\s+(?P<cols>.+)$",
    re.I,
)
_UNNEST = re.compile(r"\b(?:unnest|explode)\s+json\b", re.I)
_FILL_DOWN = re.compile(
    r"\b(?:fill[\s-]?down|previous\s+row|lag\s*\(|variable\s+port)\b",
    re.I,
)
_TERNARY = re.compile(
    r"^(?P<cond>.+?)\s*\?\s*(?P<then>.+?)\s*:\s*(?P<else>.+)$",
)
_SKIP_LOOKUP_KEYS = frozenset({
    "if", "when", "default", "unmapped", "unknown", "unmatched", "else", "*",
})
_BLANK_LOOKUP_KEYS = frozenset({"blank", "empty", "null", "missing"})


def named_rule_ref(text: str) -> str:
    """Informatica ``%RuleName%`` / ``use RuleName`` / ``RuleName()``."""
    match = _NAMED_REF.match((text or "").strip())
    if not match:
        return ""
    return (match.group("pct") or match.group("use") or match.group("call") or "").strip()


def named_rule_targets(text: str) -> tuple[str, list[str]]:
    """Informatica mapplet reuse: ``apply EmailClean to email, alt_email``."""
    match = _APPLY_TO.match((text or "").strip())
    if not match:
        return "", []
    columns: list[str] = []
    for part in re.split(r"\s*(?:,|\band\b)\s*", match.group("cols") or ""):
        name = part.strip().strip("\"'")
        if re.fullmatch(r"[A-Za-z_][\w.]*", name or ""):
            if name not in columns:
                columns.append(name)
    return match.group("name").strip(), columns


def unknown_code_policy(text: str) -> dict[str, str]:
    """G20 / OMOP: unmapped codes are refuse, never identity.

    A declared catch-all or quarantine is recorded. It does not pass the
    source code through unchanged, and it is never written as a ``*``
    identity pair on ``code_crosswalk``.
    """
    raw = (text or "").strip()
    if _UNMAPPED_DIVERT.search(raw):
        return {"action": "divert", "value": ""}
    default = _UNMAPPED_DEFAULT_PHRASE.search(raw) or _UNMAPPED_DEFAULT.search(raw)
    if default:
        value = default.group("value").strip().strip("\"'").rstrip(".")
        if value and value.lower() not in {"end", "null"}:
            return {"action": "default", "value": value}
    return {"action": "refuse", "value": ""}


def _split_sql_args(text: str, *, strip_quotes: bool = True) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    quote = ""
    depth = 0
    for ch in text or "":
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = ""
            continue
        if ch in "'\"":
            quote = ch
            buf.append(ch)
            continue
        if ch == "(":
            depth += 1
            buf.append(ch)
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            buf.append(ch)
            continue
        if ch == "," and depth == 0:
            token = "".join(buf).strip()
            parts.append(token.strip("\"'") if strip_quotes else token)
            buf = []
            continue
        buf.append(ch)
    if buf:
        token = "".join(buf).strip()
        parts.append(token.strip("\"'") if strip_quotes else token)
    return [part for part in parts if part]


def _is_column_token(value: str) -> bool:
    text = (value or "").strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return False
    return bool(re.fullmatch(r"[A-Za-z_][\w.]*", text or ""))


def _case_atom(value: str) -> str:
    raw = (value or "").strip()
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in {"'", '"'}:
        return json_escape(raw[1:-1])
    text = raw.strip("\"'")
    if re.fullmatch(r"-?[\d.]+", text) or re.fullmatch(r"[A-Za-z_][\w.]*", text or ""):
        return text
    return json_escape(text)


def parse_decode(text: str) -> dict[str, Any] | None:
    """Oracle DECODE(col, k, v, … [, default]) → closed lookup, never identity."""
    match = _DECODE.match((text or "").strip())
    if not match:
        return None
    args = _split_sql_args(match.group("args"))
    if len(args) < 2:
        return None
    pairs: dict[str, str] = {}
    unmapped = ""
    index = 0
    while index + 1 < len(args):
        key, value = args[index], args[index + 1]
        if key.lower() not in {"unmapped", "unknown", "unmatched", "else", "*"}:
            pairs[key] = value
        index += 2
    if index < len(args):
        unmapped = args[index]
    if not pairs:
        return None
    out: dict[str, Any] = {
        "kind": "lookup",
        "plane": "map",
        "confidence": 0.96,
        "mapping": pairs,
    }
    if unmapped:
        out["unmapped"] = unmapped
    return out


def parse_sql_case(text: str) -> dict[str, Any] | None:
    """ANSI CASE: equality branches become G20 pairs; the rest become ``if()``."""
    match = _CASE_BLOCK.match((text or "").strip())
    if not match:
        return None
    body = match.group("body").strip()
    simple = _SIMPLE_CASE_COL.match(body)
    subject = ""
    rest = body
    if simple:
        subject = simple.group("col")
        rest = simple.group("rest")
    else_val = ""
    else_match = _CASE_ELSE.search(rest)
    work = rest
    if else_match:
        else_val = else_match.group("else").strip().strip("\"'")
        work = rest[: else_match.start()].strip()
    branches = list(_WHEN_THEN.finditer(work))
    if not branches:
        return None
    pairs: dict[str, str] = {}
    same_col = subject
    all_eq = True
    derived: list[tuple[str, str]] = []
    for branch in branches:
        cond = branch.group("cond").strip()
        then = branch.group("then").strip()
        if subject:
            pairs[cond.strip("\"'")] = then.strip("\"'")
            derived.append((f"{subject} = {_case_atom(cond)}", then))
            continue
        eq = _EQ_ATOM.match(cond)
        if eq:
            col = eq.group("col")
            val = eq.group("val").strip().strip("\"'")
            if not same_col:
                same_col = col
            if col.lower() != same_col.lower():
                all_eq = False
            pairs[val] = then.strip("\"'")
            derived.append((cond, then))
        else:
            all_eq = False
            derived.append((cond, then))
    if (subject or all_eq) and pairs:
        out: dict[str, Any] = {
            "kind": "lookup",
            "plane": "map",
            "confidence": 0.96,
            "mapping": pairs,
        }
        if else_val:
            out["unmapped"] = else_val
        return out
    expr = _case_atom(else_val) if else_val else "null"
    for cond, then in reversed(derived):
        expr = f"if({cond}, {_case_atom(then)}, {expr})"
    return {
        "kind": "derive",
        "plane": "shape",
        "confidence": 0.9,
        "expression": expr,
    }


def _split_lookup_keys(raw: str) -> list[str]:
    parts = re.split(r"\s*(?:[,|/]|or|and)\s*", raw or "", flags=re.I)
    return [part.strip().strip("\"'") for part in parts if part.strip()]


def parse_lookup_spec(text: str) -> tuple[dict[str, str], str]:
    """Clio correspondences: one or many source codes → one target, plus blank."""
    pairs: dict[str, str] = {}
    blank = ""
    raw = text or ""
    matches = list(_LOOKUP_CORR.finditer(raw))
    clauses: list[tuple[str, str]] = []
    if matches:
        clauses = [(m.group("keys"), m.group("val")) for m in matches]
    clauses.extend(_LOOKUP_PAIR.findall(raw))
    for keys_raw, value_raw in clauses:
        value = value_raw.strip().rstrip(",")
        for key in _split_lookup_keys(keys_raw):
            folded = key.lower()
            if folded in _SKIP_LOOKUP_KEYS:
                continue
            if folded in _BLANK_LOOKUP_KEYS:
                if value:
                    blank = value
                continue
            if _DATE_TOKEN.fullmatch(key):
                continue
            if key and value:
                pairs[key] = value
    return pairs, blank


def parse_lookup(text: str) -> dict[str, str]:
    """Closed code table from a cell.

    Two or more pairs always win. A single pair is accepted only when it looks
    like a code (``A → ACTIVE``) — never a date format (``YYYY → YYYY-MM-DD``).
    Clio set-valued correspondences (``A, I, P → ACTIVE``) expand to one
    pair per source code. Blank/empty/null is not a G20 code.
    """
    pairs, _blank = parse_lookup_spec(text)
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


def _rewrite_informatica_pred(text: str) -> str:
    """ISNULL / IS_NULL → shape ``is_null`` so IIF can execute."""
    out = re.sub(r"\bis_?null\s*\(", "is_null(", text or "", flags=re.I)
    out = re.sub(r"\bis_?not_?null\s*\(", "is_not_null(", out, flags=re.I)
    return out


def _like_condition(
    col: str,
    pattern: str,
    *,
    negated: bool = False,
    insensitive: bool = False,
) -> str:
    """SQL LIKE with a single ``%`` prefix/suffix → starts_with / ends_with / contains.

    ``_`` or multiple ``%`` stay unbound — a guessed regex is silent loss.
    ILIKE is ``lower(col)`` against a lowercased literal — shape compare is
    otherwise case-sensitive (G20 / COMA honesty).
    """
    pat = (pattern or "").strip()
    if not col or not pat or "_" in pat:
        return ""
    if pat.count("%") > 2:
        return ""
    subject = f"lower({col})" if insensitive else col
    needle = pat.lower() if insensitive else pat
    if needle.startswith("%") and needle.endswith("%") and "%" not in needle[1:-1]:
        pred = f"contains({subject}, {json_escape(needle[1:-1])})"
    elif needle.endswith("%") and "%" not in needle[:-1]:
        pred = f"starts_with({subject}, {json_escape(needle[:-1])})"
    elif needle.startswith("%") and "%" not in needle[1:]:
        pred = f"ends_with({subject}, {json_escape(needle[1:])})"
    elif "%" not in needle:
        pred = f"{subject} = {json_escape(needle)}"
    else:
        return ""
    return f"not {pred}" if negated else pred


def _in_condition(col: str, values: str) -> str:
    atoms = []
    for part in _split_sql_args(values):
        token = part.strip()
        if not token:
            continue
        atoms.append(token if re.fullmatch(r"-?[\d.]+", token) else json_escape(token))
    if not atoms:
        return ""
    return "(" + " or ".join(f"{col} = {atom}" for atom in atoms) + ")"


def _like_pattern_token(raw: str) -> str:
    """Quoted LIKE pattern, or a single unquoted token. ``_`` stays unbound."""
    text = (raw or "").strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1]
    if re.fullmatch(r"[A-Za-z0-9.%@+\-]+", text or ""):
        return text
    return ""


_ROW_HEAD = re.compile(
    r"^(?P<head>"
    r"(?:divert|quarantine)(?:\s+rows?)?|"
    r"(?:exclude|drop|omit)\s+rows?|"
    r"(?:keep|only|filter)(?:\s+rows?)?|"
    r"where"
    r")\s+(?:(?:if|where)\s+)?(?P<pred>.+)$",
    re.I,
)
_KEEP_COLS_GUARD = re.compile(r"\bkeep\s+(?:only\s+)?(?:columns?|fields?)\b", re.I)
_DROP_COL_GUARD = re.compile(r"\b(?:drop|remove)\s+(?:column|field)s?\b", re.I)
_BETWEEN_ATOM = re.compile(
    r"^(?P<col>[A-Za-z_][\w.]*)\s+between\s+(?P<lo>\S+)\s+and\s+(?P<hi>\S+)$",
    re.I,
)
_IN_ATOM = re.compile(
    r"^(?P<col>[A-Za-z_][\w.]*)\s+(?P<not>not\s+)?in\s*\((?P<vals>.+)\)$",
    re.I,
)
_LIKE_ATOM = re.compile(
    r"^(?P<col>[A-Za-z_][\w.]*)\s+(?P<not>not\s+)?(?P<op>i?like)\s+(?P<pat>.+)$",
    re.I,
)
_IS_ATOM = re.compile(
    r"^(?P<col>[A-Za-z_][\w.]*)\s+is\s+(?P<not>not\s+)?(?P<kind>null|empty|blank|missing)$",
    re.I,
)
_ENGLISH_STR = re.compile(
    r"^(?P<col>[A-Za-z_][\w.]*)\s+(?P<not>does\s+not\s+|not\s+)?"
    r"(?P<op>contains?|starts?\s+with|ends?\s+with)\s+(?P<val>.+)$",
    re.I,
)
_ENGLISH_CMP = re.compile(
    r"^(?P<col>[A-Za-z_][\w.]*)\s+"
    r"(?P<op>equals?|equal\s+to|is\s+equal\s+to|not\s+equal(?:\s+to)?|"
    r"does\s+not\s+equal|greater\s+than(?:\s+or\s+equal(?:\s+to)?)?|"
    r"less\s+than(?:\s+or\s+equal(?:\s+to)?)?|at\s+least|at\s+most)\s+(?P<val>.+)$",
    re.I,
)
_SYM_ATOM = re.compile(
    r"^(?P<col>[A-Za-z_][\w.]*)\s*(?P<op>=|!=|<>|>=|<=|>|<)\s*(?P<val>.+)$",
    re.I,
)
_SET_OP = re.compile(
    r"\b(?:group\s+by|select\s+distinct|distinct\s+rows|keep\s+distinct|"
    r"pivot\b|unpivot\b|order\s+by|union(?:\s+all)?\s+select|"
    r"over\s*\(|merge\s+into|partition\s+by|window\s+function|"
    r"deduplicate|dedupe\b)\b",
    re.I,
)
_EMPTY_NULL = re.compile(
    r"\b(?:treat\s+(?:empty|blank)(?:\s+strings?)?\s+as\s+null|"
    r"null\s+if\s+(?:empty|blank)|(?:empty|blank)\s+(?:string\s+)?(?:is|as)\s+null|"
    r"blank\s+is\s+null)\b",
    re.I,
)
_NAMED_ZONE = re.compile(
    r"\b(?:assume\s+(?:time\s*)?zone|time\s*zone|at\s+time\s+zone|tz)\s+"
    r"['\"]?(?P<zone>UTC|GMT|[A-Za-z]+(?:/[A-Za-z0-9_+\-]+)+)['\"]?",
    re.I,
)
_YN_FLAG = re.compile(
    r"^(?:y\s*/\s*n|yes\s*/\s*no|t\s*/\s*f)(?:\s*(?:flag|boolean|bool))?$",
    re.I,
)
_COALESCE_FN = re.compile(r"^=?\s*(?P<fn>coalesce|ifnull|nvl)\s*\(", re.I)


def _split_bool_expr(text: str) -> tuple[list[str], list[str]] | None:
    """Split ``and`` / ``or`` at depth 0. BETWEEN's AND is not a connector."""
    parts: list[str] = []
    ops: list[str] = []
    buf: list[str] = []
    i = 0
    depth = 0
    quote = ""
    raw = text or ""
    n = len(raw)
    while i < n:
        ch = raw[i]
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = ""
            i += 1
            continue
        if ch in "'\"":
            quote = ch
            buf.append(ch)
            i += 1
            continue
        if ch == "(":
            depth += 1
            buf.append(ch)
            i += 1
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            buf.append(ch)
            i += 1
            continue
        if depth == 0:
            rest = raw[i:]
            between_pending = bool(re.search(r"\bbetween\s+\S+\s*$", "".join(buf), re.I))
            if rest.startswith("&&"):
                part = "".join(buf).strip()
                if not part:
                    return None
                parts.append(part)
                ops.append("and")
                buf = []
                i += 2
                continue
            if rest.startswith("||"):
                part = "".join(buf).strip()
                if not part:
                    return None
                parts.append(part)
                ops.append("or")
                buf = []
                i += 2
                continue
            and_m = re.match(r"\band\b", rest, re.I)
            or_m = re.match(r"\bor\b", rest, re.I)
            if and_m and not between_pending:
                part = "".join(buf).strip()
                if not part:
                    return None
                parts.append(part)
                ops.append("and")
                buf = []
                i += and_m.end()
                continue
            if or_m:
                part = "".join(buf).strip()
                if not part:
                    return None
                parts.append(part)
                ops.append("or")
                buf = []
                i += or_m.end()
                continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    if not parts:
        return None
    return parts, ops


def _wrapped_parens(text: str) -> bool:
    raw = (text or "").strip()
    if not (raw.startswith("(") and raw.endswith(")")):
        return False
    depth = 0
    quote = ""
    for i, ch in enumerate(raw):
        if quote:
            if ch == quote:
                quote = ""
            continue
        if ch in "'\"":
            quote = ch
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i == len(raw) - 1
            if depth < 0:
                return False
    return False


def _english_cmp_op(token: str) -> str:
    key = re.sub(r"\s+", " ", (token or "").strip().lower())
    return {
        "equal": "=",
        "equals": "=",
        "equal to": "=",
        "is equal to": "=",
        "not equal": "<>",
        "not equal to": "<>",
        "does not equal": "<>",
        "greater than": ">",
        "less than": "<",
        "greater than or equal": ">=",
        "greater than or equal to": ">=",
        "less than or equal": "<=",
        "less than or equal to": "<=",
        "at least": ">=",
        "at most": "<=",
    }.get(key, "")


def parse_predicate(text: str) -> str:
    """Compile a row predicate. Leftover or mixed and/or without parens → ``""``."""
    raw = (text or "").strip()
    if not raw:
        return ""
    if _wrapped_parens(raw):
        return parse_predicate(raw[1:-1].strip())
    split = _split_bool_expr(raw)
    if not split:
        return ""
    parts, ops = split
    if ops and len(set(ops)) > 1:
        return ""
    compiled: list[str] = []
    for part in parts:
        chunk = part.strip()
        if _wrapped_parens(chunk):
            atom = parse_predicate(chunk[1:-1].strip())
        elif ops:
            atom = parse_predicate_atom(chunk)
        else:
            atom = parse_predicate_atom(chunk)
        if not atom:
            return ""
        compiled.append(atom)
    if not compiled:
        return ""
    if not ops:
        return compiled[0]
    joiner = f" {ops[0]} "
    if len(compiled) == 1:
        return compiled[0]
    return "(" + joiner.join(compiled) + ")"


def parse_predicate_atom(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        return ""
    between = _BETWEEN_ATOM.match(raw)
    if between:
        return (
            f"{between.group('col')} >= {_case_atom(between.group('lo'))} and "
            f"{between.group('col')} <= {_case_atom(between.group('hi'))}"
        )
    in_m = _IN_ATOM.match(raw)
    if in_m:
        cond = _in_condition(in_m.group("col"), in_m.group("vals"))
        if not cond:
            return ""
        return f"not {cond}" if in_m.group("not") else cond
    like = _LIKE_ATOM.match(raw)
    if like:
        pat = _like_pattern_token(like.group("pat"))
        cond = _like_condition(
            like.group("col"),
            pat,
            negated=bool(like.group("not")),
            insensitive=like.group("op").lower() == "ilike",
        )
        return cond
    is_m = _IS_ATOM.match(raw)
    if is_m:
        fn = "is_not_null" if is_m.group("not") else "is_null"
        return f"{fn}({is_m.group('col')})"
    english = _ENGLISH_STR.match(raw)
    if english:
        val = english.group("val").strip()
        if not val:
            return ""
        op = re.sub(r"\s+", " ", english.group("op").lower())
        if op.startswith("start"):
            pred = f"starts_with({english.group('col')}, {_quote_lit(val)})"
        elif op.startswith("end"):
            pred = f"ends_with({english.group('col')}, {_quote_lit(val)})"
        else:
            pred = f"contains({english.group('col')}, {_quote_lit(val)})"
        return f"not {pred}" if english.group("not") else pred
    cmp_m = _ENGLISH_CMP.match(raw)
    if cmp_m:
        oper = _english_cmp_op(cmp_m.group("op"))
        if not oper:
            return ""
        return _condition(cmp_m.group("col"), oper, cmp_m.group("val"))
    sym = _SYM_ATOM.match(raw)
    if sym:
        return _condition(sym.group("col"), sym.group("op"), sym.group("val"))
    return ""


def parse_row_policy(text: str) -> dict[str, Any] | None:
    """keep / exclude / divert + a fully-consumed predicate.

    iMAP complex matches: every atom must compile. A leftover ``and …`` is
    review, not a silent half-filter (that is how rows vanish).
    """
    raw = (text or "").strip()
    if not raw or _KEEP_COLS_GUARD.search(raw) or _DROP_COL_GUARD.search(raw):
        return None
    head = _ROW_HEAD.match(raw)
    if not head:
        return None
    pred = parse_predicate(head.group("pred"))
    kind_word = re.sub(r"\s+", " ", head.group("head").strip().lower())
    if kind_word.startswith("divert") or kind_word.startswith("quarantine"):
        kind = "divert"
        keep = True
    elif kind_word.startswith("exclude") or kind_word.startswith("drop") or kind_word.startswith("omit"):
        kind = "filter"
        keep = False
    else:
        kind = "filter"
        keep = True
    if not pred:
        return {
            "kind": "unknown",
            "plane": "review",
            "confidence": 0.4,
            "reason": (
                "Row predicate is not a closed form (mixed and/or without "
                "parentheses, LIKE _ / multiple %, or leftover text). "
                "It was not applied — a guessed filter is silent loss."
            ),
        }
    if kind == "divert":
        return {"kind": "divert", "plane": "shape", "confidence": 0.9, "condition": pred}
    return {"kind": "filter", "plane": "shape", "confidence": 0.9, "condition": pred, "keep": keep}


def parse_coalesce(text: str) -> dict[str, Any] | None:
    """COALESCE/NVL/IFNULL. Two-arg literal → default; otherwise shape coalesce()."""
    raw = (text or "").strip()
    match = _COALESCE_FN.match(raw)
    if not match:
        return None
    start = raw.find("(", match.start())
    if start < 0:
        return None
    depth = 0
    quote = ""
    end = -1
    for i, ch in enumerate(raw[start:], start):
        if quote:
            if ch == quote:
                quote = ""
            continue
        if ch in "'\"":
            quote = ch
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end < 0:
        return None
    leftover = raw[end + 1:].strip().rstrip(";").strip()
    if leftover:
        return None
    args = _split_sql_args(raw[start + 1:end], strip_quotes=False)
    if len(args) < 2:
        return None
    if len(args) == 2 and not _is_column_token(args[1]):
        return {
            "kind": "default",
            "plane": "shape",
            "confidence": 0.93,
            "value": args[1].strip().strip("\"'"),
        }
    expr = "coalesce(" + ", ".join(_case_atom(arg) for arg in args) + ")"
    return {"kind": "derive", "plane": "shape", "confidence": 0.93, "expression": expr}


def parse_iif(text: str) -> dict[str, Any] | None:
    """Informatica / SQL Server IIF(cond, then [, else]) → shape ``if()``."""
    raw = (text or "").strip()
    if re.search(r"\bdd_(?:reject|insert|update|delete)\b", raw, re.I):
        return {
            "kind": "unknown",
            "plane": "review",
            "confidence": 0.4,
            "reason": (
                "IIF update strategy (DD_REJECT / DD_INSERT) is not a pre-load "
                "rule. Confirm on Map write mode — this compiler will not invent it."
            ),
        }
    if not _IIF.match(raw):
        return None
    inner = raw[raw.find("(") + 1: raw.rfind(")")] if "(" in raw else ""
    args = _split_sql_args(inner)
    if len(args) < 2:
        return None
    cond = _rewrite_informatica_pred(args[0])
    then = args[1]
    else_ = args[2] if len(args) > 2 else "null"
    return {
        "kind": "derive",
        "plane": "shape",
        "confidence": 0.92,
        "expression": f"if({cond}, {_case_atom(then)}, {_case_atom(else_)})",
    }


def _concat_columns(text: str) -> tuple[list[str], str]:
    """Named columns and an optional separator from a concat cell."""
    columns: list[str] = []
    for left, right in _CONCAT_EXPR.findall(text or ""):
        for part in (left, right):
            name = part.strip()
            if name and name not in columns:
                columns.append(name)
    if not columns:
        tail = re.sub(r"^(?:concat(?:enate)?|combine|textjoin)\s+", "", text or "", flags=re.I)
        tail = re.sub(r"\b(?:and|with|into)\b", ",", tail, flags=re.I)
        for part in re.split(r"[,\s]+", tail):
            name = part.strip(" \"'")
            if re.fullmatch(r"[A-Za-z_][\w.]*", name or "") and name.lower() not in {
                "concat", "concatenate", "combine", "columns", "fields", "join", "textjoin",
            }:
                if name not in columns:
                    columns.append(name)
    separator = ""
    sep_match = re.search(r"(?:sep(?:arator)?|delimited?\s+by)\s+['\"](.+?)['\"]", text or "", re.I)
    if sep_match:
        separator = sep_match.group(1)
    elif "space" in (text or "").lower() and re.search(r"concat|textjoin", text or "", re.I):
        separator = " "
    return columns, separator


def _quote_lit(value: str) -> str:
    text = (value or "").strip().strip("\"'")
    if re.fullmatch(r"-?[\d.]+", text):
        return text
    return json_escape(text)


def json_escape(value: str) -> str:
    return '"' + (value or "").replace("\\", "\\\\").replace('"', '\\"') + '"'


def _condition(col: str, op: str, val: str) -> str:
    oper = "<>" if op == "!=" else op
    return f"{col} {oper} {_quote_lit(val)}"


def _excel_formula(raw: str) -> dict[str, Any] | None:
    """Map a leading Excel formula onto a closed form when the function is known."""
    text = (raw or "").strip()
    if text.startswith("="):
        text = text[1:].strip()
    match = _EXCEL_FN.match("=" + text if not text.startswith("=") else text)
    if not match:
        # ``=A2*12`` / ``=salary*12``
        derive = _DERIVE.search(text)
        if derive and re.match(r"^[A-Za-z_]", text):
            expr = derive.group("expr").replace("×", "*").replace("x", "*").replace("X", "*")
            return {"kind": "derive", "plane": "shape", "confidence": 0.94, "expression": re.sub(r"\s+", " ", expr)}
        return None
    fn = match.group("fn").upper()
    inner = text[text.find("(") + 1: text.rfind(")")] if "(" in text else ""
    inner_u = inner.upper()
    nested_extras: list[dict[str, Any]] = []
    if "TRIM(" in inner_u:
        nested_extras.append({"kind": "trim", "op": "trim"})
    if fn in {"UPPER"}:
        return {"kind": "case_upper", "plane": "map", "confidence": 0.97, "extras": nested_extras}
    if fn in {"LOWER"}:
        return {"kind": "case_lower", "plane": "map", "confidence": 0.97, "extras": nested_extras}
    if fn in {"TRIM"}:
        if "UPPER(" in inner_u:
            return {"kind": "case_upper", "plane": "map", "confidence": 0.96, "extras": [{"kind": "trim", "op": "trim"}]}
        if "LOWER(" in inner_u:
            return {"kind": "case_lower", "plane": "map", "confidence": 0.96, "extras": [{"kind": "trim", "op": "trim"}]}
        return {"kind": "trim", "plane": "map", "confidence": 0.97}
    if fn in {"PROPER"}:
        return {"kind": "title", "plane": "shape", "confidence": 0.93}
    if fn in {"CONCATENATE", "CONCAT", "TEXTJOIN"}:
        columns, separator = _concat_columns(inner.replace("&", "+"))
        return {
            "kind": "concat",
            "plane": "shape",
            "confidence": 0.93 if len(columns) >= 2 else 0.55,
            "columns": columns,
            "separator": separator,
        }
    if fn in {"SUBSTITUTE", "REPLACE"}:
        parts = [p.strip().strip("\"'") for p in inner.split(",") if p.strip()]
        if len(parts) >= 3:
            return {
                "kind": "replace",
                "plane": "shape",
                "confidence": 0.92,
                "search": parts[1],
                "replacement": parts[2],
            }
    if fn == "LEFT":
        left = _LEFT.search(f"left({inner})")
        if left:
            return {
                "kind": "substr",
                "plane": "shape",
                "confidence": 0.92,
                "expression": f"substr({left.group('col')}, 1, {left.group('length')})",
            }
    if fn == "RIGHT":
        right = _RIGHT.search(f"right({inner})")
        if right:
            return {
                "kind": "substr",
                "plane": "shape",
                "confidence": 0.92,
                "expression": (
                    f"substr({right.group('col')}, "
                    f"length({right.group('col')}) - {int(right.group('length')) - 1})"
                ),
            }
    if fn == "MID":
        mid = _SUBSTR.search(f"mid({inner})")
        if mid:
            length = mid.group("length") or "1"
            return {
                "kind": "substr",
                "plane": "shape",
                "confidence": 0.92,
                "expression": f"substr({mid.group('col')}, {mid.group('start')}, {length})",
            }
    if fn == "ABS":
        return {"kind": "absolute", "plane": "shape", "confidence": 0.93}
    if fn == "ROUND":
        rounded = _ROUND.search("round " + (inner.split(",")[-1] if "," in inner else "0"))
        return {
            "kind": "round",
            "plane": "shape",
            "confidence": 0.93,
            "places": int(rounded.group("places")) if rounded else 0,
        }
    if fn in {"VLOOKUP", "XLOOKUP"}:
        return {
            "kind": "join",
            "plane": "review",
            "confidence": 0.4,
            "reason": "Excel VLOOKUP/XLOOKUP is not a pre-load rule. Name join keys or write Source → query.",
        }
    if fn in {"IF", "IFERROR"}:
        closed = _IF_FN.search(text)
        if closed:
            return {
                "kind": "derive",
                "plane": "shape",
                "confidence": 0.9,
                "expression": (
                    f"if({closed.group('cond')}, {closed.group('then')}"
                    + (f", {closed.group('else')}" if closed.group("else") else "")
                    + ")"
                ),
            }
    if fn in {"VALUE", "TO_NUMBER"}:
        return {"kind": "cast_number", "plane": "map", "confidence": 0.94}
    if fn in {"DATEVALUE", "TEXT", "TO_DATE", "TO_CHAR"}:
        return {"kind": "date", "plane": "map", "confidence": 0.94}
    if fn in {"LEN", "LENGTH"}:
        col = inner.split(",")[0].strip() or "value"
        return {
            "kind": "derive",
            "plane": "shape",
            "confidence": 0.92,
            "expression": f"length({col})",
        }
    if fn in {"CAST"}:
        casted = _CAST_SQL.search(f"cast({inner})")
        if casted:
            return _cast_kind(casted.group("type"))
    if fn in {"COALESCE", "IFNULL", "NVL"}:
        coal = parse_coalesce(f"{fn}({inner})")
        if coal:
            return coal
    if fn in {"NULLIF"}:
        nf = _NULLIF_FN.search(f"nullif({inner})")
        if nf:
            return {
                "kind": "null_if",
                "plane": "shape",
                "confidence": 0.92,
                "values": [nf.group("value").strip().strip("\"'")],
            }
    return None


def _cast_kind(type_name: str) -> dict[str, Any]:
    token = (type_name or "").strip().lower()
    if token.startswith("int"):
        return {"kind": "cast_integer", "plane": "map", "confidence": 0.95}
    if token.startswith("bool"):
        return {"kind": "cast_boolean", "plane": "map", "confidence": 0.95}
    if token in {"date", "timestamp"}:
        return {"kind": "date", "plane": "map", "confidence": 0.94}
    return {"kind": "cast_number", "plane": "map", "confidence": 0.95}


def _compound_extras(raw: str, kind: str) -> list[dict[str, Any]]:
    extras: list[dict[str, Any]] = []
    if kind not in {"trim"} and _TRIM.search(raw):
        extras.append({"kind": "trim", "op": "trim"})
    if kind not in {"collapse"} and _COLLAPSE.search(raw):
        extras.append({"kind": "collapse", "op": "collapse_whitespace"})
    return extras


def parse_date_spec(text: str) -> dict[str, Any] | None:
    """Informatica-style date mask. MM/DD vs DD/MM must be named, not assumed."""
    raw = (text or "").strip()
    if not raw:
        return None
    if _AMBIGUOUS_DATE.search(raw):
        return {
            "kind": "date",
            "plane": "review",
            "confidence": 0.4,
            "reason": (
                "Date mask names both MM/DD and DD/MM. Pick one — "
                "a swapped day is silent loss."
            ),
        }
    parts = re.split(r"\s*(?:→|->|=>)\s*", raw, maxsplit=1)
    source_match = _DATE_MASK.search(parts[0])
    dest_match = _DATE_MASK.search(parts[1]) if len(parts) > 1 else None
    if not source_match:
        return None
    leftover = _DATE_MASK.sub("", parts[0])
    leftover = _DATE_WORDS.sub("", leftover)
    leftover = re.sub(r"[^A-Za-z0-9]", "", leftover)
    if leftover:
        return None
    return {
        "kind": "date",
        "plane": "shape",
        "confidence": 0.96,
        "format": source_match.group("mask").upper(),
        "output_format": dest_match.group("mask").upper() if dest_match else "YYYY-MM-DD",
    }


def _classify_chain(raw: str, flags: dict[str, Any]) -> dict[str, Any] | None:
    """iMAP / Informatica expression chain: trim then lowercase then email."""
    parts = [part.strip() for part in _CHAIN_SPLIT.split(raw) if part.strip()]
    if len(parts) < 2:
        return None
    atoms = [classify_rule(part, atomic=True) for part in parts]
    if any(str(atom.get("kind") or "") not in _CLEANSE_KINDS for atom in atoms):
        return None
    primary = dict(atoms[-1])
    extras = list(primary.get("extras") or [])
    for atom in atoms[:-1]:
        kind = str(atom.get("kind") or "")
        if kind == "trim":
            extras.append({"kind": "trim", "op": "trim"})
        elif kind == "collapse":
            extras.append({"kind": "collapse", "op": "collapse_whitespace"})
        elif kind == "case_lower":
            extras.append({"kind": "case_lower", "op": "case", "mode": "lower"})
        elif kind == "case_upper":
            extras.append({"kind": "case_upper", "op": "case", "mode": "upper"})
        elif kind == "title":
            extras.append({"kind": "title", "op": "case", "mode": "title"})
    primary["extras"] = extras
    primary.update(flags)
    return primary


def classify_rule(text: str, *, atomic: bool = False) -> dict[str, Any]:
    """One rule cell → kind, plane hint, and any structured payload.

    Order is specific-to-general: a lookup that also says "map" is a lookup,
    not Direct. Unknown is a status, not a guess.
    """
    raw = (text or "").strip()
    flags: dict[str, Any] = {}
    if _REQUIRED.search(raw):
        flags["required"] = True
    if _UNIQUE.search(raw):
        flags["unique"] = True

    if raw.startswith("=") or _EXCEL_FN.match(raw):
        excel = _excel_formula(raw)
        if excel:
            extras = list(excel.get("extras") or [])
            seen = {(item.get("op") or item.get("kind")) for item in extras}
            for extra in _compound_extras(raw, excel["kind"]):
                key = extra.get("op") or extra.get("kind")
                if key not in seen:
                    extras.append(extra)
                    seen.add(key)
            excel["extras"] = extras
            excel.update(flags)
            return excel

    if _DIRECT.match(raw):
        return {"kind": "direct", "plane": "map", "confidence": 0.99, **flags}

    casted = _CAST_SQL.search(raw) or _PG_CAST.search(raw)
    if casted:
        got = _cast_kind(casted.group("type"))
        got.update(flags)
        return got
    coal = parse_coalesce(raw)
    if coal:
        coal.update(flags)
        return coal
    nf = _NULLIF_FN.search(raw)
    if nf:
        return {
            "kind": "null_if",
            "plane": "shape",
            "confidence": 0.92,
            "values": [nf.group("value").strip().strip("\"'")],
            **flags,
        }
    length = _LEN.search(raw)
    if length:
        return {
            "kind": "derive",
            "plane": "shape",
            "confidence": 0.92,
            "expression": f"length({length.group('col')})",
            **flags,
        }
    if _TO_DATE.search(raw) or _TO_CHAR.search(raw):
        return {"kind": "date", "plane": "map", "confidence": 0.94, **flags}
    if _TO_NUMBER.search(raw):
        return {"kind": "cast_number", "plane": "map", "confidence": 0.95, **flags}

    decoded = parse_decode(raw)
    if decoded:
        decoded.update(flags)
        return decoded
    cased = parse_sql_case(raw)
    if cased:
        cased.update(flags)
        return cased
    iif = parse_iif(raw)
    if iif:
        iif.update(flags)
        return iif
    ternary_early = _TERNARY.match(raw)
    if ternary_early and "?" in raw and ":" in raw and not _DATE_MASK.search(raw):
        return {
            "kind": "derive",
            "plane": "shape",
            "confidence": 0.9,
            "expression": (
                f"if({_rewrite_informatica_pred(ternary_early.group('cond'))}, "
                f"{_case_atom(ternary_early.group('then'))}, {_case_atom(ternary_early.group('else'))})"
            ),
            **flags,
        }
    nvl2 = _NVL2.match(raw)
    if nvl2:
        return {
            "kind": "derive",
            "plane": "shape",
            "confidence": 0.92,
            "expression": (
                f"if(is_not_null({nvl2.group('col')}), "
                f"{_case_atom(nvl2.group('present'))}, "
                f"{_case_atom(nvl2.group('missing'))})"
            ),
            **flags,
        }
    extracted = _REG_EXTRACT.search(raw)
    if extracted:
        group = extracted.group("grp") or "0"
        return {
            "kind": "derive",
            "plane": "shape",
            "confidence": 0.92,
            "expression": (
                f"regex_extract({extracted.group('col')}, "
                f"{json_escape(extracted.group('pat'))}, {group})"
            ),
            **flags,
        }
    replaced = _REG_REPLACE.search(raw)
    if replaced:
        return {
            "kind": "replace",
            "plane": "shape",
            "confidence": 0.92,
            "search": replaced.group("pat"),
            "replacement": replaced.group("repl"),
            "regex": True,
            **flags,
        }

    policy = parse_row_policy(raw)
    if policy:
        policy.update(flags)
        return policy

    pairs, blank = parse_lookup_spec(raw)
    lookup = parse_lookup(raw)
    if lookup:
        out = {
            "kind": "lookup",
            "plane": "map",
            "confidence": 0.98,
            "mapping": lookup,
            "extras": _compound_extras(raw, "lookup"),
            **flags,
        }
        if blank:
            out["blank_default"] = blank
        if _CASE_INSENSITIVE.search(raw):
            out["case_insensitive"] = True
            out["confidence"] = 0.5
            out["reason"] = (
                "Lookup is marked case-insensitive. G20 matches codes exactly — "
                "confirm both casings or this is silent remap."
            )
        return out
    if blank and not lookup:
        return {
            "kind": "default",
            "plane": "shape",
            "confidence": 0.9,
            "value": blank,
            **flags,
        }
    if _LOOKUP_HINT.search(raw) and not lookup:
        return {
            "kind": "unknown",
            "plane": "review",
            "confidence": 0.4,
            "reason": "lookup named but no A → B pairs were found",
            **flags,
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
            **flags,
        }

    keep_cols = _KEEP_COLS.search(raw)
    if keep_cols:
        columns = [
            part.strip().strip("\"'")
            for part in re.split(r"\s*(?:,|\band\b)\s*", keep_cols.group("cols") or "")
            if re.fullmatch(r"[A-Za-z_][\w.]*", part.strip().strip("\"'") or "")
            and part.strip().lower() not in {"columns", "fields", "only", "keep"}
        ]
        return {
            "kind": "keep_columns",
            "plane": "shape",
            "confidence": 0.92 if columns else 0.5,
            "columns": columns,
            **({} if columns else {"reason": "keep columns named but none were identified"}),
            **flags,
        }
    drop_col = _DROP_COL.search(raw)
    if drop_col:
        return {
            "kind": "drop_column",
            "plane": "shape",
            "confidence": 0.92,
            "column": drop_col.group("col"),
            **flags,
        }

    if _OMIT.search(raw):
        return {"kind": "omit", "plane": "map", "confidence": 0.97, **flags}

    if not atomic:
        chained = _classify_chain(raw, flags)
        if chained:
            return chained

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
            "extras": _compound_extras(raw, "concat"),
            **({} if len(columns) >= 2 else {
                "reason": "concat named but fewer than two columns were identified",
            }),
            **flags,
        }

    closed_if = re.search(
        r"(?<![A-Za-z_])if\s*\(\s*(?P<cond>.+?)\s*,\s*(?P<then>.+?)\s*(?:,\s*(?P<else>.+?))?\s*\)\s*$",
        raw,
        re.I,
    )
    if closed_if:
        return {
            "kind": "derive",
            "plane": "shape",
            "confidence": 0.9,
            "expression": (
                f"if({_rewrite_informatica_pred(closed_if.group('cond'))}, "
                f"{closed_if.group('then')}"
                + (f", {closed_if.group('else')}" if closed_if.group("else") else "")
                + ")"
            ),
            **flags,
        }
    ternary = _TERNARY.match(raw)
    if ternary and "?" in raw and ":" in raw and not _DATE_MASK.search(raw):
        return {
            "kind": "derive",
            "plane": "shape",
            "confidence": 0.9,
            "expression": (
                f"if({_rewrite_informatica_pred(ternary.group('cond'))}, "
                f"{_case_atom(ternary.group('then'))}, {_case_atom(ternary.group('else'))})"
            ),
            **flags,
        }

    left = _LEFT.search(raw)
    if left:
        return {
            "kind": "substr",
            "plane": "shape",
            "confidence": 0.92,
            "expression": f"substr({left.group('col')}, 1, {left.group('length')})",
            **flags,
        }
    right = _RIGHT.search(raw)
    if right:
        return {
            "kind": "substr",
            "plane": "shape",
            "confidence": 0.92,
            "expression": (
                f"substr({right.group('col')}, "
                f"length({right.group('col')}) - {int(right.group('length')) - 1})"
            ),
            **flags,
        }
    substr = _SUBSTR.search(raw)
    if substr:
        length = substr.group("length") or "1"
        return {
            "kind": "substr",
            "plane": "shape",
            "confidence": 0.92,
            "expression": f"substr({substr.group('col')}, {substr.group('start')}, {length})",
            **flags,
        }

    prefix = _PREFIX.search(raw)
    if prefix:
        return {
            "kind": "prefix",
            "plane": "shape",
            "confidence": 0.91,
            "value": prefix.group("value"),
            **flags,
        }
    suffix = _SUFFIX.search(raw)
    if suffix:
        return {
            "kind": "suffix",
            "plane": "shape",
            "confidence": 0.91,
            "value": suffix.group("value"),
            **flags,
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
            **flags,
        }

    replace = _REPLACE.search(raw)
    if replace:
        return {
            "kind": "replace",
            "plane": "shape",
            "confidence": 0.93,
            "search": replace.group("search").strip().strip("\"'"),
            "replacement": (replace.group("repl") or "").strip().strip("\"'"),
            **flags,
        }

    default = _DEFAULT.search(raw) or _DEFAULT_BARE.search(raw)
    if default:
        return {
            "kind": "default",
            "plane": "shape",
            "confidence": 0.93,
            "value": default.group("value").strip().strip("\"'"),
            **flags,
        }

    if _EMPTY_NULL.search(raw):
        return {
            "kind": "null_if",
            "plane": "shape",
            "confidence": 0.93,
            "values": [""],
            **flags,
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
            **flags,
        }

    constant = _CONSTANT.search(raw)
    if constant:
        return {
            "kind": "constant",
            "plane": "shape",
            "confidence": 0.93,
            "value": constant.group("value").strip().strip("\"'"),
            **flags,
        }

    pad = _PAD.search(raw)
    if pad:
        return {
            "kind": "pad",
            "plane": "shape",
            "confidence": 0.92,
            "width": int(pad.group("width")),
            "side": (pad.group("side") or "left").lower(),
            **flags,
        }

    split = _SPLIT.search(raw)
    if split:
        return {
            "kind": "split",
            "plane": "shape",
            "confidence": 0.9,
            "separator": split.group("sep").strip().strip("\"'"),
            **flags,
        }

    rounded = _ROUND.search(raw)
    if rounded:
        return {
            "kind": "round",
            "plane": "shape",
            "confidence": 0.93,
            "places": int(rounded.group("places")),
            **flags,
        }
    truncated = _TRUNCATE.search(raw)
    if truncated:
        return {
            "kind": "truncate",
            "plane": "shape",
            "confidence": 0.92,
            "places": int(truncated.group("places")),
            **flags,
        }
    clamp = _CLAMP.search(raw)
    if clamp:
        return {
            "kind": "clamp",
            "plane": "shape",
            "confidence": 0.91,
            "min": clamp.group(1),
            "max": clamp.group(2),
            **flags,
        }
    if _ABS.search(raw):
        return {"kind": "absolute", "plane": "shape", "confidence": 0.93, **flags}

    if _FILL_DOWN.search(raw):
        return {
            "kind": "unknown",
            "plane": "review",
            "confidence": 0.4,
            "reason": (
                "Fill-down / previous-row / Informatica variable port is not "
                "row-local. This compiler will not invent a window."
            ),
            **flags,
        }
    if _SET_OP.search(raw):
        return {
            "kind": "unknown",
            "plane": "review",
            "confidence": 0.4,
            "reason": (
                "GROUP BY / DISTINCT / PIVOT / UNION / OVER / MERGE is not "
                "row-local. Confirm on Operations Transforms — this compiler "
                "will not invent a grain."
            ),
            **flags,
        }
    hashed_id = _HASH_ID.search(raw)
    if hashed_id:
        stop = frozenset({"of", "on", "from", "over", "columns", "fields", "and", "the", "hash", "identity"})
        columns = [
            part.strip().strip("\"'")
            for part in re.split(r"\s*(?:,|\band\b)\s*", hashed_id.group("cols") or "")
            if re.fullmatch(r"[A-Za-z_][\w.]*", part.strip().strip("\"'") or "")
            and part.strip().lower() not in stop
        ]
        return {
            "kind": "hash_identity",
            "plane": "shape",
            "confidence": 0.93 if columns else 0.5,
            "columns": columns,
            **({} if columns else {"reason": "hash identity named but no columns were identified"}),
            **flags,
        }
    if _UNNEST.search(raw):
        return {"kind": "unnest", "plane": "shape", "confidence": 0.9, **flags}
    if _FLATTEN.search(raw):
        return {"kind": "flatten", "plane": "shape", "confidence": 0.9, **flags}
    if _HASH.search(raw):
        return {"kind": "hash", "plane": "map", "confidence": 0.95, "extras": _compound_extras(raw, "hash"), **flags}
    if _EMAIL.search(raw):
        return {"kind": "email", "plane": "map", "confidence": 0.96, "extras": _compound_extras(raw, "email"), **flags}
    if _PHONE.search(raw):
        return {"kind": "phone", "plane": "map", "confidence": 0.96, "extras": _compound_extras(raw, "phone"), **flags}
    date_spec = parse_date_spec(raw)
    if date_spec:
        date_spec.update(flags)
        return date_spec
    if _DATE.search(raw):
        return {"kind": "date", "plane": "map", "confidence": 0.96, **flags}
    if _TIME.search(raw) and not _DATE.search(raw):
        return {"kind": "time", "plane": "map", "confidence": 0.94, **flags}
    if _CURRENCY.search(raw):
        return {"kind": "currency", "plane": "map", "confidence": 0.95, **flags}
    if _PERCENT.search(raw):
        return {"kind": "percentage", "plane": "map", "confidence": 0.95, **flags}
    if _INT.search(raw):
        return {"kind": "cast_integer", "plane": "map", "confidence": 0.95, **flags}
    if _NUM.search(raw):
        return {"kind": "cast_number", "plane": "map", "confidence": 0.95, **flags}
    if _BOOL.search(raw):
        return {"kind": "cast_boolean", "plane": "map", "confidence": 0.95, **flags}
    if _JSON.search(raw):
        return {"kind": "json", "plane": "map", "confidence": 0.93, **flags}
    if _BINARY.search(raw):
        return {"kind": "binary", "plane": "map", "confidence": 0.93, **flags}
    named_zone = _NAMED_ZONE.search(raw)
    if named_zone:
        return {
            "kind": "timezone",
            "plane": "map",
            "confidence": 0.96,
            "zone": named_zone.group("zone"),
            **flags,
        }
    if _ZONE.search(raw):
        return {
            "kind": "timezone",
            "plane": "review",
            "confidence": 0.5,
            "reason": "A source timezone must be named (IANA). This compiler will not assume UTC.",
            **flags,
        }
    if _YN_FLAG.match(raw):
        return {
            "kind": "unknown",
            "plane": "review",
            "confidence": 0.4,
            "reason": (
                "parse_boolean refuses informal Y/N / yes/no. Write "
                "Y → true, N → false — a guessed flag is silent remap."
            ),
            **flags,
        }
    if _UNICODE.search(raw):
        return {"kind": "unicode", "plane": "shape", "confidence": 0.9, **flags}
    if _STRIP_CTRL.search(raw):
        return {"kind": "strip_controls", "plane": "map", "confidence": 0.95, **flags}
    if _COLLAPSE.search(raw):
        return {"kind": "collapse", "plane": "shape", "confidence": 0.94, **flags}
    if _TITLE.search(raw):
        return {"kind": "title", "plane": "shape", "confidence": 0.94, **flags}
    if _LOWER.search(raw):
        return {"kind": "case_lower", "plane": "map", "confidence": 0.97, "extras": _compound_extras(raw, "case_lower"), **flags}
    if _UPPER.search(raw):
        return {"kind": "case_upper", "plane": "map", "confidence": 0.97, "extras": _compound_extras(raw, "case_upper"), **flags}
    if _TRIM.search(raw):
        return {"kind": "trim", "plane": "map", "confidence": 0.97, **flags}

    if flags.get("required") or flags.get("unique"):
        return {
            "kind": "contract",
            "plane": "review",
            "confidence": 0.6,
            "reason": (
                "Required / unique is a Validate contract, not a write transform. "
                "Confirm the dest column on Map; Validate already fail-closes nulls and duplicate keys."
            ),
            **flags,
        }

    if not raw:
        return {"kind": "direct", "plane": "map", "confidence": 0.99}
    return {
        "kind": "unknown",
        "plane": "review",
        "confidence": 0.35,
        "reason": "rule text is not a closed form this compiler can execute",
        **flags,
    }
