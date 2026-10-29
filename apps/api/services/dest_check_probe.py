"""D112 — probe the source population against the destination's live CHECK constraints.

``inspect_destination_for_preflight`` now surfaces ``check_constraints`` from the
destination catalog. This probe evaluates those predicates against the source
rows Validate already walks (full bounded population when the source re-reads,
the posted sample otherwise) and names the offending rows before Confirm —
instead of the first violation surfacing as a raw driver error post-write.

Scope is deliberately honest: the evaluator covers the portable predicate
subset (comparisons, AND/OR/NOT, IS [NOT] NULL, [NOT] IN, [NOT] BETWEEN,
``TRUE``/``FALSE``/``NULL``/number/string literals, optional ``::cast``
suffixes). Anything outside — functions, subqueries, LIKE, arithmetic — is
reported as ``not_evaluated``; a predicate we cannot parse is never treated as
satisfied. SQL three-valued logic applies: a CHECK fails a row only when the
predicate is FALSE; NULL making it UNKNOWN is legal.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable, Mapping
from typing import Any

logger = logging.getLogger(__name__)

_UNKNOWN = None  # SQL UNKNOWN marker for the 3VL evaluator

_MAX_NAMED = 25


# --------------------------------------------------------------------------- #
# Tokenizer + parser (safe predicate subset)
# --------------------------------------------------------------------------- #

_TOKEN_RE = re.compile(
    r"""(?ix)
    (?:>=|<=|<>|!=|=|<|>)           # comparison operators
    |\(|\)|,                         # punctuation
    |'(?:[^']|'')*'                  # 'string' literal with '' escape
    |"(?:[^"]|"")*"                  # "quoted identifier"
    |\[[^\]]+\]                      # [bracketed identifier]
    |`(?:[^`]|``)*`                  # `backtick identifier` (MySQL)
    |-?\d+(?:\.\d+)?                 # number literal
    |[a-z_][a-z0-9_.$#]*             # bare identifier / keyword
    |\s+|[^\s]                       # whitespace (skip) / other token
    """
)


def _tokenize(text: str) -> list[str]:
    tokens = []
    for m in _TOKEN_RE.finditer(text):
        tok = m.group(0)
        if tok.isspace():
            continue
        tokens.append(tok)
    return tokens


class _ParseFail(Exception):
    pass


class _Parser:
    """Recursive-descent over the portable CHECK subset. Raises _ParseFail."""

    def __init__(self, tokens: list[str]):
        self.tokens = tokens
        self.pos = 0

    def peek(self) -> str | None:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def next(self) -> str:
        tok = self.peek()
        if tok is None:
            raise _ParseFail("unexpected end of predicate")
        self.pos += 1
        return tok

    def accept(self, word: str) -> bool:
        tok = self.peek()
        if tok is not None and tok.upper() == word:
            self.pos += 1
            return True
        return False

    def parse(self) -> Any:
        node = self.parse_or()
        if self.peek() is not None:
            raise _ParseFail(f"trailing token {self.peek()!r}")
        return node

    def parse_or(self) -> Any:
        node = self.parse_and()
        while self.accept("OR"):
            node = ("or", node, self.parse_and())
        return node

    def parse_and(self) -> Any:
        node = self.parse_not()
        while self.accept("AND"):
            node = ("and", node, self.parse_not())
        return node

    def parse_not(self) -> Any:
        if self.accept("NOT"):
            return ("not", self.parse_not())
        tok = self.peek()
        if tok == "(":
            self.next()
            inner = self.parse_or()
            if self.next() != ")":
                raise _ParseFail("missing )")
            return inner
        return self.parse_predicate()

    def parse_predicate(self) -> Any:
        left = self.parse_operand()
        tok = self.peek()
        if tok is None or tok.upper() == "AND" or tok.upper() == "OR" or tok == ")":
            # bare boolean column (e.g. CHECK (flag)) — Postgres allows it
            return ("truthy", left)
        if tok.upper() == "IS":
            self.next()
            neg = self.accept("NOT")
            word = self.next().upper()
            if word in {"NULL", "TRUE", "FALSE", "DISTINCT"}:
                if word == "DISTINCT":
                    if not self.accept("FROM"):
                        raise _ParseFail("IS DISTINCT without FROM")
                    return ("isnotdistinct" if not neg else "isdistinct",
                            left, self.parse_operand())
                return ("is", word.lower(), neg, left)
            raise _ParseFail(f"unsupported IS operand {word!r}")
        neg = self.accept("NOT")
        if (self.peek() or "").upper() == "BETWEEN":
            self.next()
            lo = self.parse_operand()
            if not self.accept("AND"):
                raise _ParseFail("BETWEEN without AND")
            hi = self.parse_operand()
            return ("between", neg, left, lo, hi)
        if (self.peek() or "").upper() == "IN":
            self.next()
            if self.next() != "(":
                raise _ParseFail("IN without (")
            items = [self.parse_operand()]
            while self.accept(","):
                items.append(self.parse_operand())
            if self.next() != ")":
                raise _ParseFail("IN list unclosed")
            return ("in", neg, left, items)
        if neg:
            # "col NOT >= 2" is not SQL; NOT before non-IN/BETWEEN is a parse fail
            raise _ParseFail("NOT before comparison")
        op = self.next()
        if op not in {"=", "<>", "!=", ">", "<", ">=", "<="}:
            raise _ParseFail(f"unsupported operator {op!r}")
        return ("cmp", op, left, self.parse_operand())

    def parse_operand(self) -> Any:
        tok = self.next()
        if tok == "-":
            inner = self.parse_operand()
            return ("neg", inner)
        if tok == "(":
            inner = self.parse_or()
            if self.next() != ")":
                raise _ParseFail("missing )")
            # SQL Server stores ``[amount]>=(0)``: a parenthesised operand is
            # its value, not a boolean test of it.
            if isinstance(inner, tuple) and inner and inner[0] == "truthy":
                return inner[1]
            return inner
        if tok.startswith("'"):
            val = tok[1:-1].replace("''", "'")
            return ("lit", val)
        if re.fullmatch(r"-?\d+(?:\.\d+)?", tok):
            return ("lit", float(tok) if "." in tok else int(tok))
        upper = tok.upper()
        if upper in {"NULL", "TRUE", "FALSE"}:
            return ("lit", {"NULL": None, "TRUE": True, "FALSE": False}[upper])
        if upper in {"AND", "OR", "NOT", "IN", "IS", "BETWEEN", "LIKE", "CASE",
                     "SELECT", "WHEN", "THEN", "ELSE", "END", "CAST"}:
            raise _ParseFail(f"keyword {upper!r} where value expected")
        if tok.startswith('"'):
            ident = tok[1:-1].replace('""', '"')
        elif tok.startswith("["):
            ident = tok[1:-1]
        elif tok.startswith("`"):
            ident = tok[1:-1].replace("``", "`")
        else:
            ident = tok
        # schema.table.column → column
        if "." in ident:
            ident = ident.rsplit(".", 1)[-1]
        return ("col", ident)


def _compile(predicate: str) -> Any:
    """Parse predicate text; returns the AST or raises _ParseFail.

    ``expr::type`` casts are declarative noise for evaluation: the tokenizer
    emits ``:``, ``:``, ``ident`` triples, which this fold removes (an embedded
    ``'lit'::ident`` inside a quoted literal is safe — the literal regex
    swallows the whole quoted span first).
    """
    tokens = _tokenize(predicate)
    # Catalog text may carry the leading keyword (MySQL 'CHECK ((expr))')
    # or a named form ('CONSTRAINT "name" CHECK ((expr))').
    if tokens and tokens[0].upper() == "CONSTRAINT":
        tokens = tokens[2:]
    if tokens and tokens[0].upper() == "CHECK":
        tokens = tokens[1:]
    folded: list[str] = []
    i = 0
    while i < len(tokens):
        # MySQL prints string literals with a charset introducer
        # (``_utf8mb4'paid'``); the introducer carries no value.
        if (
            i + 1 < len(tokens)
            and re.fullmatch(r"_[a-zA-Z0-9]+", tokens[i])
            and tokens[i + 1].startswith("'")
        ):
            i += 1
            continue
        if (
            i + 2 < len(tokens)
            and tokens[i] == ":"
            and tokens[i + 1] == ":"
            and re.fullmatch(r"[a-zA-Z_][\w$]*(\[\])?", tokens[i + 2] or "")
        ):
            i += 3
            continue
        folded.append(tokens[i])
        i += 1
    return _Parser(folded).parse()


# --------------------------------------------------------------------------- #
# Evaluator (SQL three-valued logic)
# --------------------------------------------------------------------------- #

def _cmp(op: str, a: Any, b: Any) -> Any:
    if a is None or b is None:
        return _UNKNOWN
    try:
        if isinstance(a, bool) != isinstance(b, bool):
            # bool vs number compares as Python does; keep it strict-ish
            pass
        if op == "=":
            return a == b
        if op in {"<>", "!="}:
            return a != b
        # ordered comparisons: allow number↔number and str↔str; coerce numeric strings
        try:
            fa, fb = float(a), float(b)
        except (TypeError, ValueError):
            fa, fb = None, None
        if fa is not None:
            a2, b2 = fa, fb
        else:
            a2, b2 = str(a), str(b)
        if op == ">":
            return a2 > b2
        if op == "<":
            return a2 < b2
        if op == ">=":
            return a2 >= b2
        if op == "<=":
            return a2 <= b2
    except (TypeError, ValueError):
        return _UNKNOWN
    return _UNKNOWN


def _eval(node: Any, row: Mapping[str, Any], cols: dict[str, str]) -> Any:
    kind = node[0]
    if kind == "lit":
        return node[1]
    if kind == "neg":
        v = _eval(node[1], row, cols)
        return -v if isinstance(v, (int, float)) else _UNKNOWN
    if kind == "col":
        field = cols.get(str(node[1]).lower())
        if field is None:
            return _UNKNOWN
        return row.get(field)
    if kind == "truthy":
        v = _eval(node[1], row, cols)
        return v if isinstance(v, bool) else (bool(v) if v is not None else _UNKNOWN)
    if kind == "or":
        a = _eval(node[1], row, cols)
        if a is True:
            return True
        b = _eval(node[2], row, cols)
        if b is True:
            return True
        return _UNKNOWN if a is _UNKNOWN or b is _UNKNOWN else False
    if kind == "and":
        a = _eval(node[1], row, cols)
        if a is False:
            return False
        b = _eval(node[2], row, cols)
        if b is False:
            return False
        return _UNKNOWN if a is _UNKNOWN or b is _UNKNOWN else True
    if kind == "not":
        v = _eval(node[1], row, cols)
        return _UNKNOWN if v is _UNKNOWN else not v
    if kind == "cmp":
        return _cmp(node[1], _eval(node[2], row, cols), _eval(node[3], row, cols))
    if kind == "is":
        word, neg, operand = node[1], node[2], node[3]
        v = _eval(operand, row, cols)
        result = {
            "null": v is None,
            "true": v is True,
            "false": v is False,
        }[word]
        return (not result) if neg else result
    if kind in {"isdistinct", "isnotdistinct"}:
        a = _eval(node[1], row, cols)
        b = _eval(node[2], row, cols)
        distinct = (a is None) != (b is None) or (a is not None and a != b)
        return distinct if kind == "isdistinct" else not distinct
    if kind == "between":
        _, neg, left, lo, hi = node
        ge_lo = _cmp(">=", _eval(left, row, cols), _eval(lo, row, cols))
        le_hi = _cmp("<=", _eval(left, row, cols), _eval(hi, row, cols))
        inside = _eval(("and", ("lit", ge_lo), ("lit", le_hi)), row, cols)
        return _UNKNOWN if inside is _UNKNOWN else (not inside if neg else inside)
    if kind == "in":
        _, neg, left, items = node
        lv = _eval(left, row, cols)
        saw_unknown = lv is None
        hit = False
        for item in items:
            iv = _eval(item, row, cols)
            if iv is None:
                saw_unknown = True
            elif lv == iv:
                hit = True
        result = True if hit else (_UNKNOWN if saw_unknown else False)
        if result is _UNKNOWN:
            return _UNKNOWN
        return (not result) if neg else result
    raise _ParseFail(f"unknown node {kind!r}")


# --------------------------------------------------------------------------- #
# Constraint shape + probe
# --------------------------------------------------------------------------- #

def _constraint_predicate(item: Any) -> tuple[str, str]:
    """(name, predicate) from a dest catalog entry; '' when unreadable."""
    if isinstance(item, str):
        return "", item
    if not isinstance(item, Mapping):
        return "", ""
    pred = (
        item.get("sqltext")
        or item.get("predicate")
        or item.get("expression")
        or item.get("definition")
        or item.get("constraintdef")
        or ""
    )
    name = str(item.get("name") or item.get("constraint_name") or "")
    return str(name), str(pred)


def _dest_to_source_fields(mappings: list[dict[str, Any]]) -> dict[str, str]:
    """dest column (lower) -> source field name, from the posted mappings."""
    out: dict[str, str] = {}
    for m in mappings or []:
        src = str(m.get("source") or "").strip()
        for key in ("target", "destination", "dest", "name"):
            dst = str(m.get(key) or "").strip()
            if src and dst:
                out[dst.lower()] = src
    return out


def check_constraint_source_fields(
    check_constraints: list[Any], mappings: list[dict[str, Any]]
) -> list[str]:
    """Source fields the evaluable CHECK predicates read, via the mappings.

    The population walk has to read exactly these: the bounded-carrier walk
    g3f uses skips a BIGINT -> BIGINT column, and an empty walk read as "every
    CHECK holds" (D112-CHECK).
    """
    field_map = _dest_to_source_fields(mappings)
    fields: set[str] = set()
    for item in check_constraints or []:
        _name, pred = _constraint_predicate(item)
        if not pred:
            continue
        try:
            ast = _compile(pred)
        except _ParseFail:
            continue
        for node in _walk_cols(ast):
            src = field_map.get(str(node[1]).lower())
            if src:
                fields.add(src)
    return sorted(fields)


def probe_dest_check_constraints(
    *,
    rows: Iterable[Mapping[str, Any]],
    mappings: list[dict[str, Any]],
    check_constraints: list[Any],
    coverage: str = "sample_check_probe",
    max_named: int = _MAX_NAMED,
) -> dict[str, Any]:
    """Evaluate dest CHECK predicates over source-keyed rows.

    Returns a report with ``violating_count``, the named offending rows
    (row_index + their mapped key values), and per-constraint disposition:
    ``evaluated`` / ``not_evaluated`` / ``no_rows``. Never raises — a probe
    that cannot answer says so.
    """
    out: dict[str, Any] = {
        "ran": False,
        "coverage": coverage,
        "population_proof": coverage == "population_check_probe",
        "evaluated": 0,
        "not_evaluated": 0,
        "violating_count": 0,
        "violations": [],
        "checks": [],
        "note": "",
    }
    if not check_constraints:
        out["note"] = "No destination CHECK constraints reported."
        return out

    field_map = _dest_to_source_fields(mappings)
    compiled: list[tuple[str, str, Any, dict[str, str]]] = []
    for item in check_constraints:
        name, pred = _constraint_predicate(item)
        if not pred:
            out["checks"].append({"name": name, "status": "not_evaluated",
                                  "reason": "no predicate text in catalog entry"})
            out["not_evaluated"] += 1
            continue
        try:
            ast = _compile(pred)
        except _ParseFail as exc:
            out["checks"].append({"name": name, "predicate": pred,
                                  "status": "not_evaluated", "reason": str(exc)})
            out["not_evaluated"] += 1
            continue
        # predicate columns must resolve to mapped source fields (col -> src)
        needed = {n[1].lower() for n in _walk_cols(ast)}
        resolved = {c: field_map.get(c) for c in needed}
        if any(v is None for v in resolved.values()):
            out["checks"].append({
                "name": name, "predicate": pred, "status": "not_evaluated",
                "reason": "predicate column not covered by a mapping: "
                          + ", ".join(sorted(c for c, v in resolved.items() if v is None)),
            })
            out["not_evaluated"] += 1
            continue
        compiled.append((name, pred, ast, {c: v for c, v in resolved.items() if v}))
        out["checks"].append({"name": name, "predicate": pred, "status": "evaluated"})
        out["evaluated"] += 1

    out["ran"] = True
    if not compiled:
        out["note"] = (
            "Destination CHECK constraint(s) present but none evaluable by the "
            "portable predicate subset — population CHECK fit not proven."
        )
        return out

    named: list[dict[str, Any]] = []
    violating = 0
    for idx, row in enumerate(rows):
        for name, pred, ast, cols in compiled:
            try:
                verdict = _eval(ast, row, cols)
            except Exception:
                verdict = _UNKNOWN
            if verdict is False:  # CHECK fails only on FALSE, not UNKNOWN
                violating += 1
                if len(named) < max_named:
                    named.append({
                        "row_index": idx,
                        "constraint": name or pred,
                        "predicate": pred,
                        "values": {c: row.get(f) for c, f in cols.items()},
                    })
    out["violating_count"] = violating
    out["violations"] = named
    if violating:
        out["note"] = (
            f"{violating} source row(s) violate destination CHECK constraint(s) "
            f"({coverage}); named rows are quarantine candidates."
        )
    elif out["not_evaluated"]:
        out["note"] = (
            "No violating rows found in the evaluated constraints; "
            f"{out['not_evaluated']} constraint(s) were not evaluable."
        )
    else:
        out["note"] = "All evaluated CHECK constraints hold on the probed rows."
    return out


def _walk_cols(node: Any) -> Iterable[tuple]:
    kind = node[0]
    if kind == "col":
        yield node
    for child in node[1:]:
        if isinstance(child, tuple):
            yield from _walk_cols(child)
        elif isinstance(child, list):
            for c in child:
                if isinstance(c, tuple):
                    yield from _walk_cols(c)
