"""The expression language a shaping recipe is written in.

A `derive` or `filter` step needs an expression, and there were three ways to
provide one: raw SQL (no engine exists over a spreadsheet, and every dialect
spells things differently), Python ``eval`` (arbitrary code execution in a
multi-tenant product), or a small closed grammar parsed to an AST this product
owns. This module is the third choice.

The properties the rest of the engine relies on:

* **Deterministic.** No clock, no randomness, no I/O, no host access. The same
  row yields the same value in preview, in Validate and in Execute, and across
  chunk boundaries, resumes and CDC replays.
* **Row-local.** An expression can read the columns of its own row and nothing
  else, so it survives streaming without buffering the population.
* **Statically checked.** Unknown column names, unknown functions and wrong
  arity are refused at design time with a message naming the fix, rather than
  at row 431 of a million.
* **Exact.** Numbers are `Decimal`, never `float`, so a shaping step cannot be
  the thing that loses the last digit of a client's money column.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, DecimalException, InvalidOperation, ROUND_HALF_UP
from typing import Any, Callable, Mapping, Sequence

__all__ = [
    "ExpressionError",
    "EvalError",
    "FUNCTIONS",
    "Expression",
    "compile_expression",
    "describe_functions",
]


class ExpressionError(ValueError):
    """A malformed expression. The message names the position and the fix."""


class EvalError(ValueError):
    """An expression that cannot be evaluated for one row's values."""


# A regex is compiled from operator input, so it must be bounded: a pattern
# longer than this, or a subject longer than this, is refused rather than given
# to the engine to backtrack over a million rows.
_MAX_PATTERN = 200
_MAX_SUBJECT = 100_000
_MAX_REPEAT = 10_000

# ---------------------------------------------------------------------------
# Tokenizer
# ---------------------------------------------------------------------------

_PUNCT = (
    "<>", "<=", ">=", "!=", "==",
    "(", ")", ",", "+", "-", "*", "/", "%", "<", ">", "=",
)

_KEYWORDS = {"and", "or", "not", "null", "true", "false"}


@dataclass(frozen=True, slots=True)
class _Token:
    kind: str  # number | string | name | punct | keyword | end
    text: str
    pos: int


def _tokenize(source: str) -> list[_Token]:
    tokens: list[_Token] = []
    i = 0
    n = len(source)
    while i < n:
        ch = source[i]
        if ch.isspace():
            i += 1
            continue
        if ch == "'":
            j = i + 1
            buf: list[str] = []
            while True:
                if j >= n:
                    raise ExpressionError(
                        f"unterminated text literal starting at position {i + 1}"
                    )
                if source[j] == "'":
                    if j + 1 < n and source[j + 1] == "'":
                        buf.append("'")
                        j += 2
                        continue
                    j += 1
                    break
                buf.append(source[j])
                j += 1
            tokens.append(_Token("string", "".join(buf), i))
            i = j
            continue
        if ch == "[":
            j = source.find("]", i + 1)
            if j < 0:
                raise ExpressionError(
                    f"unterminated column reference starting at position {i + 1}"
                )
            tokens.append(_Token("name", source[i + 1 : j].strip(), i))
            i = j + 1
            continue
        if ch.isdigit() or (ch == "." and i + 1 < n and source[i + 1].isdigit()):
            j = i
            seen_dot = False
            while j < n and (source[j].isdigit() or (source[j] == "." and not seen_dot)):
                seen_dot = seen_dot or source[j] == "."
                j += 1
            tokens.append(_Token("number", source[i:j], i))
            i = j
            continue
        if ch.isalpha() or ch == "_":
            j = i
            while j < n and (source[j].isalnum() or source[j] in "_"):
                j += 1
            word = source[i:j]
            kind = "keyword" if word.casefold() in _KEYWORDS else "name"
            tokens.append(_Token(kind, word, i))
            i = j
            continue
        for punct in _PUNCT:
            if source.startswith(punct, i):
                tokens.append(_Token("punct", punct, i))
                i += len(punct)
                break
        else:
            raise ExpressionError(
                f"unexpected character {ch!r} at position {i + 1}"
            )
    tokens.append(_Token("end", "", len(source)))
    return tokens


# ---------------------------------------------------------------------------
# AST
# ---------------------------------------------------------------------------


class _Node:
    """Base for AST nodes. ``kind`` is what the canonical form records."""

    kind = "node"

    def evaluate(self, row: Mapping[str, Any]) -> Any:  # pragma: no cover - abstract
        raise NotImplementedError

    def columns(self) -> set[str]:
        return set()

    def canonical(self) -> Any:  # pragma: no cover - abstract
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class _Literal(_Node):
    value: Any

    def evaluate(self, row: Mapping[str, Any]) -> Any:
        return self.value

    def canonical(self) -> Any:
        if isinstance(self.value, Decimal):
            return ["num", str(self.value)]
        return ["lit", self.value]


@dataclass(frozen=True, slots=True)
class _Column(_Node):
    name: str

    def evaluate(self, row: Mapping[str, Any]) -> Any:
        if self.name not in row:
            raise EvalError(f"column '{self.name}' is not in this row")
        return row[self.name]

    def columns(self) -> set[str]:
        return {self.name}

    def canonical(self) -> Any:
        return ["col", self.name]


@dataclass(frozen=True, slots=True)
class _Unary(_Node):
    op: str
    operand: _Node

    def evaluate(self, row: Mapping[str, Any]) -> Any:
        value = self.operand.evaluate(row)
        if self.op == "not":
            return not _as_bool(value)
        if value is None:
            return None
        return -_as_number(value)

    def columns(self) -> set[str]:
        return self.operand.columns()

    def canonical(self) -> Any:
        return ["unary", self.op, self.operand.canonical()]


_COMPARISONS = {"=", "==", "<>", "!=", "<", "<=", ">", ">="}


@dataclass(frozen=True, slots=True)
class _Binary(_Node):
    op: str
    left: _Node
    right: _Node

    def evaluate(self, row: Mapping[str, Any]) -> Any:
        if self.op in ("and", "or"):
            left = _as_bool(self.left.evaluate(row))
            # Short-circuit: `is_null(x) or length(x) > 2` must not evaluate the
            # right side for a null x, or a guard clause could not be written.
            if self.op == "and" and not left:
                return False
            if self.op == "or" and left:
                return True
            return _as_bool(self.right.evaluate(row))

        left = self.left.evaluate(row)
        right = self.right.evaluate(row)
        if self.op in _COMPARISONS:
            return _compare(self.op, left, right)
        if left is None or right is None:
            return None
        return _arithmetic(self.op, left, right)

    def columns(self) -> set[str]:
        return self.left.columns() | self.right.columns()

    def canonical(self) -> Any:
        return ["bin", self.op, self.left.canonical(), self.right.canonical()]


@dataclass(frozen=True, slots=True)
class _Call(_Node):
    name: str
    args: tuple[_Node, ...]

    def evaluate(self, row: Mapping[str, Any]) -> Any:
        spec = FUNCTIONS[self.name]
        if spec.lazy:
            return spec.impl(row, self.args)
        return spec.impl(*[a.evaluate(row) for a in self.args])

    def columns(self) -> set[str]:
        found: set[str] = set()
        for arg in self.args:
            found |= arg.columns()
        return found

    def canonical(self) -> Any:
        return ["call", self.name, [a.canonical() for a in self.args]]


# ---------------------------------------------------------------------------
# Value coercion — one definition, shared by every operator and function
# ---------------------------------------------------------------------------


def is_blank(value: Any) -> bool:
    """Null-ish for shaping purposes: ``None``, an empty/whitespace string, NaN.

    A spreadsheet's empty cell arrives as ``""`` from one reader and ``None``
    from another; treating them differently would make the same recipe behave
    differently per source.
    """
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, float):
        return value != value
    return False


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        # "f" never uses exponent notation, so a scaled decimal renders as the
        # digits the source held rather than as 1E+3.
        return format(value, "f")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def _as_number(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return Decimal(1) if value else Decimal(0)
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        # str() of a float is its shortest round-trip form, which is the closest
        # thing to what the operator typed; Decimal(float) would expose binary
        # noise like 0.1000000000000000055511151231257827.
        return Decimal(str(value))
    text = _as_text(value)
    if text is None or text.strip() == "":
        raise EvalError("expected a number, got an empty value")
    try:
        return Decimal(text.strip())
    except (InvalidOperation, DecimalException) as exc:
        raise EvalError(f"'{text}' is not a number") from exc


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float, Decimal)):
        return value != 0
    text = str(value).strip().casefold()
    if text in ("true", "t", "yes", "y", "1"):
        return True
    if text in ("false", "f", "no", "n", "0", ""):
        return False
    raise EvalError(f"'{value}' is not a truth value")


def _both_numeric(left: Any, right: Any) -> bool:
    for value in (left, right):
        if isinstance(value, bool):
            return False
        if isinstance(value, (int, float, Decimal)):
            continue
        if isinstance(value, str):
            try:
                Decimal(value.strip())
            except (InvalidOperation, DecimalException, ValueError):
                return False
            continue
        return False
    return True


def _compare(op: str, left: Any, right: Any) -> bool | None:
    if left is None or right is None:
        # Three-valued logic: a comparison against an unknown is unknown, and
        # `filter` treats unknown as "does not match" rather than guessing.
        return None
    if isinstance(left, bool) or isinstance(right, bool):
        pair: tuple[Any, Any] = (_as_bool(left), _as_bool(right))
    elif _both_numeric(left, right):
        pair = (_as_number(left), _as_number(right))
    elif isinstance(left, (datetime, date)) and isinstance(right, (datetime, date)):
        pair = (_comparable_moment(left), _comparable_moment(right))
    else:
        pair = (_as_text(left) or "", _as_text(right) or "")
    a, b = pair
    if op in ("=", "=="):
        return a == b
    if op in ("<>", "!="):
        return a != b
    if op == "<":
        return a < b
    if op == "<=":
        return a <= b
    if op == ">":
        return a > b
    return a >= b


def _comparable_moment(value: datetime | date) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    # A calendar date carries no zone. Stamping UTC here would invent an offset
    # the source never declared, which is exactly the temporal-carrier mistake
    # the ltz/tz/ntz split exists to prevent.
    return datetime(value.year, value.month, value.day)  # noqa: DTZ001


def _arithmetic(op: str, left: Any, right: Any) -> Any:
    if op == "+" and (isinstance(left, str) or isinstance(right, str)) and not _both_numeric(left, right):
        raise EvalError(
            "'+' adds numbers; use concat() to join text so a numeric column "
            "cannot be concatenated by accident"
        )
    a = _as_number(left)
    b = _as_number(right)
    if op == "+":
        return a + b
    if op == "-":
        return a - b
    if op == "*":
        return a * b
    if b == 0:
        raise EvalError("division by zero")
    if op == "%":
        return a % b
    return a / b


# ---------------------------------------------------------------------------
# Function library
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Function:
    name: str
    min_args: int
    max_args: int  # -1 for variadic
    impl: Callable[..., Any]
    summary: str
    lazy: bool = False

    def accepts(self, count: int) -> bool:
        if count < self.min_args:
            return False
        return self.max_args < 0 or count <= self.max_args


def _fn_coalesce(*values: Any) -> Any:
    for value in values:
        if not is_blank(value):
            return value
    return None


def _fn_if(row: Mapping[str, Any], args: Sequence[_Node]) -> Any:
    """Lazy so an unchosen branch cannot fail: ``if(is_null(x), 0, 1/x)``."""
    condition = _as_bool(args[0].evaluate(row))
    if condition:
        return args[1].evaluate(row)
    return args[2].evaluate(row) if len(args) > 2 else None


def _fn_substr(value: Any, start: Any, length: Any = None) -> Any:
    text = _as_text(value)
    if text is None:
        return None
    begin = int(_as_number(start))
    if begin == 0:
        raise EvalError("substr positions are 1-based; 0 is not a position")
    index = begin - 1 if begin > 0 else max(len(text) + begin, 0)
    if length is None:
        return text[index:]
    count = int(_as_number(length))
    if count < 0:
        raise EvalError("substr length cannot be negative")
    return text[index : index + count]


def _fn_replace(value: Any, needle: Any, replacement: Any = "") -> Any:
    text = _as_text(value)
    if text is None:
        return None
    return text.replace(_as_text(needle) or "", _as_text(replacement) or "")


def _compiled_pattern(pattern: Any) -> "re.Pattern[str]":
    raw = _as_text(pattern) or ""
    if len(raw) > _MAX_PATTERN:
        raise EvalError(
            f"pattern is {len(raw)} characters; the limit is {_MAX_PATTERN} so a "
            "million rows cannot be spent backtracking"
        )
    try:
        return re.compile(raw)
    except re.error as exc:
        raise EvalError(f"invalid pattern: {exc}") from exc


def _guard_subject(text: str) -> str:
    if len(text) > _MAX_SUBJECT:
        raise EvalError(
            f"value is {len(text)} characters; regex functions refuse subjects "
            f"longer than {_MAX_SUBJECT}"
        )
    return text


def _fn_regex_replace(value: Any, pattern: Any, replacement: Any = "") -> Any:
    text = _as_text(value)
    if text is None:
        return None
    return _compiled_pattern(pattern).sub(
        _as_text(replacement) or "", _guard_subject(text)
    )


def _fn_regex_extract(value: Any, pattern: Any, group: Any = 0) -> Any:
    text = _as_text(value)
    if text is None:
        return None
    match = _compiled_pattern(pattern).search(_guard_subject(text))
    if match is None:
        return None
    index = int(_as_number(group))
    try:
        return match.group(index)
    except (IndexError, re.error) as exc:
        raise EvalError(f"pattern has no group {index}") from exc


def _fn_regex_matches(value: Any, pattern: Any) -> Any:
    text = _as_text(value)
    if text is None:
        return None
    return _compiled_pattern(pattern).search(_guard_subject(text)) is not None


def _fn_split_part(value: Any, separator: Any, index: Any) -> Any:
    text = _as_text(value)
    if text is None:
        return None
    sep = _as_text(separator) or ""
    if sep == "":
        raise EvalError("split_part needs a non-empty separator")
    position = int(_as_number(index))
    if position == 0:
        raise EvalError("split_part positions are 1-based; 0 is not a position")
    parts = text.split(sep)
    try:
        return parts[position - 1] if position > 0 else parts[position]
    except IndexError:
        return None


def _fn_pad(value: Any, width: Any, fill: Any, *, left: bool) -> Any:
    text = _as_text(value)
    if text is None:
        return None
    target = int(_as_number(width))
    if target < 0:
        raise EvalError("pad width cannot be negative")
    if target > _MAX_REPEAT:
        raise EvalError(f"pad width {target} exceeds the {_MAX_REPEAT} limit")
    filler = _as_text(fill) or " "
    if filler == "":
        filler = " "
    if len(text) >= target:
        # Truncation keeps the significant end: a left-pad is building a fixed
        # width code, so the tail is what identifies it.
        return text[len(text) - target :] if left else text[:target]
    shortfall = target - len(text)
    grown = (filler * (shortfall // len(filler) + 1))[:shortfall]
    return grown + text if left else text + grown


def _round_half_up(value: Any, places: Any = 0) -> Any:
    if value is None:
        return None
    number = _as_number(value)
    digits = int(_as_number(places))
    if abs(digits) > 40:
        raise EvalError("round places must be within 40 digits")
    quantum = Decimal(1).scaleb(-digits)
    # Bankers' rounding would surprise a finance user reading the preview, and
    # ROUND_HALF_UP is what a spreadsheet does.
    return number.quantize(quantum, rounding=ROUND_HALF_UP)


def _fn_truncate(value: Any, places: Any = 0) -> Any:
    if value is None:
        return None
    number = _as_number(value)
    digits = int(_as_number(places))
    quantum = Decimal(1).scaleb(-digits)
    return number.quantize(quantum, rounding="ROUND_DOWN")


def _fn_to_number(value: Any, *, allow_grouping: bool = True) -> Any:
    """Parse a number a human typed: grouping, currency, parenthesised negative.

    Grouping uses the write-path locale contract. Auto refuses a lone ``1,234``.
    """
    if is_blank(value):
        return None
    if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        return _as_number(value)
    from services.transform_engine import decimal_wire_value

    parsed = decimal_wire_value(value)
    if parsed is not None:
        return parsed
    if not allow_grouping:
        text = (_as_text(value) or "").strip()
        if text in ("", "-", "+", "."):
            raise EvalError(f"'{value}' is not a number")
        try:
            number = Decimal(text)
        except (InvalidOperation, DecimalException) as exc:
            raise EvalError(f"'{value}' is not a number") from exc
        if not number.is_finite():
            raise EvalError(f"'{value}' is not a number")
        return number
    raise EvalError(f"'{value}' is not a number")


def _fn_to_date(value: Any, fmt: Any = None) -> Any:
    """Parse a date with an *explicit* format, or ISO when none is given.

    There is deliberately no ambiguous auto-detection: `03/04/2026` is two
    different dates in two countries, and a migration that guesses is a
    migration that silently corrupts a quarter of the rows.
    """
    if is_blank(value):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        # Midnight in no zone: the wall-clock value the source stated (see
        # _comparable_moment).
        return datetime(value.year, value.month, value.day)  # noqa: DTZ001
    text = (_as_text(value) or "").strip()
    pattern = _as_text(fmt) if fmt is not None else None
    if pattern:
        try:
            return datetime.strptime(text, pattern)
        except ValueError as exc:
            raise EvalError(f"'{text}' does not match format '{pattern}'") from exc
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EvalError(
            f"'{text}' is not an ISO date; pass an explicit format as the second "
            "argument (day/month order is never guessed)"
        ) from exc


def _fn_format_date(value: Any, fmt: Any) -> Any:
    moment = _fn_to_date(value)
    if moment is None:
        return None
    pattern = _as_text(fmt)
    if not pattern:
        raise EvalError("format_date needs a format")
    return moment.strftime(pattern)


def _fn_to_boolean(value: Any) -> Any:
    if is_blank(value):
        return None
    return _as_bool(value)


def _fn_normalize_unicode(value: Any, form: Any = "NFC") -> Any:
    text = _as_text(value)
    if text is None:
        return None
    name = (_as_text(form) or "NFC").upper()
    if name not in ("NFC", "NFD", "NFKC", "NFKD"):
        raise EvalError(f"'{name}' is not a Unicode normal form")
    return unicodedata.normalize(name, text)  # type: ignore[arg-type]


def _fn_strip_characters(value: Any, kind: Any) -> Any:
    """Remove a named character class — the Data Cleansing equivalent."""
    text = _as_text(value)
    if text is None:
        return None
    name = (_as_text(kind) or "").strip().casefold()
    if name == "punctuation":
        return re.sub(r"[^\w\s]", "", text, flags=re.UNICODE)
    if name == "digits":
        return re.sub(r"\d", "", text)
    if name == "letters":
        return re.sub(r"[^\W\d_]", "", text, flags=re.UNICODE)
    if name == "non_numeric":
        return re.sub(r"[^0-9.\-]", "", text)
    if name == "non_printable":
        return "".join(c for c in text if c.isprintable() or c in "\t\n")
    if name == "whitespace":
        return re.sub(r"\s+", "", text)
    raise EvalError(
        f"'{name}' is not a character class; use punctuation, digits, letters, "
        "non_numeric, non_printable or whitespace"
    )


def _fn_collapse_whitespace(value: Any) -> Any:
    text = _as_text(value)
    if text is None:
        return None
    return re.sub(r"\s+", " ", text).strip()


def _fn_length(value: Any) -> Any:
    text = _as_text(value)
    return None if text is None else Decimal(len(text))


def _fn_concat(*values: Any) -> Any:
    return "".join(_as_text(v) or "" for v in values)


def _fn_least(*values: Any) -> Any:
    present = [v for v in values if v is not None]
    if not present:
        return None
    return min(present, key=_sort_key)


def _fn_greatest(*values: Any) -> Any:
    present = [v for v in values if v is not None]
    if not present:
        return None
    return max(present, key=_sort_key)


def _sort_key(value: Any) -> tuple[int, Any]:
    try:
        return (0, _as_number(value))
    except EvalError:
        return (1, _as_text(value) or "")


def _fn_clamp(value: Any, low: Any, high: Any) -> Any:
    if is_blank(value):
        return None
    number = _as_number(value)
    floor_v = _as_number(low)
    ceil_v = _as_number(high)
    if floor_v > ceil_v:
        raise EvalError("clamp lower bound is above its upper bound")
    return max(floor_v, min(ceil_v, number))


def _fn_nullif(value: Any, sentinel: Any) -> Any:
    if _compare("=", value, sentinel) is True:
        return None
    return value


def _fn_title_case(value: Any) -> Any:
    text = _as_text(value)
    if text is None:
        return None
    # str.title() mangles "O'Brien" into "O'Brien" -> "O'Brien"; capitalising
    # each word's first letter and leaving the rest alone is what an operator
    # means by Title Case for names.
    return re.sub(r"\b(\w)", lambda m: m.group(1).upper(), text.lower())


def _text_fn(name: str, fn: Callable[[str], Any], summary: str) -> _Function:
    def impl(value: Any) -> Any:
        text = _as_text(value)
        return None if text is None else fn(text)

    return _Function(name, 1, 1, impl, summary)


def _number_fn(name: str, fn: Callable[[Decimal], Any], summary: str) -> _Function:
    def impl(value: Any) -> Any:
        if is_blank(value):
            return None
        return fn(_as_number(value))

    return _Function(name, 1, 1, impl, summary)


_FUNCTION_LIST: tuple[_Function, ...] = (
    # text
    _text_fn("lower", str.lower, "Lower-case the text"),
    _text_fn("upper", str.upper, "Upper-case the text"),
    _text_fn("trim", str.strip, "Remove leading and trailing whitespace"),
    _text_fn("ltrim", lambda s: s.lstrip(), "Remove leading whitespace"),
    _text_fn("rtrim", lambda s: s.rstrip(), "Remove trailing whitespace"),
    _Function("title_case", 1, 1, _fn_title_case, "Capitalise each word"),
    _Function("length", 1, 1, _fn_length, "Number of characters"),
    _Function("concat", 1, -1, _fn_concat, "Join values as text"),
    _Function("substr", 2, 3, _fn_substr, "Substring from a 1-based position"),
    _Function("replace", 2, 3, _fn_replace, "Replace every literal occurrence"),
    _Function("regex_replace", 2, 3, _fn_regex_replace, "Replace by pattern"),
    _Function("regex_extract", 2, 3, _fn_regex_extract, "First match, or a group"),
    _Function("regex_matches", 2, 2, _fn_regex_matches, "Whether the pattern matches"),
    _Function("split_part", 3, 3, _fn_split_part, "Nth field of a delimited value"),
    _Function(
        "lpad", 2, 3,
        lambda v, w, f=" ": _fn_pad(v, w, f, left=True),
        "Pad on the left to a width",
    ),
    _Function(
        "rpad", 2, 3,
        lambda v, w, f=" ": _fn_pad(v, w, f, left=False),
        "Pad on the right to a width",
    ),
    _Function("collapse_whitespace", 1, 1, _fn_collapse_whitespace, "Squeeze runs of whitespace"),
    _Function("strip_characters", 2, 2, _fn_strip_characters, "Remove a character class"),
    _Function("normalize_unicode", 1, 2, _fn_normalize_unicode, "Apply a Unicode normal form"),
    _Function("starts_with", 2, 2, lambda v, p: None if _as_text(v) is None else (_as_text(v) or "").startswith(_as_text(p) or ""), "Prefix test"),
    _Function("ends_with", 2, 2, lambda v, p: None if _as_text(v) is None else (_as_text(v) or "").endswith(_as_text(p) or ""), "Suffix test"),
    _Function("contains", 2, 2, lambda v, p: None if _as_text(v) is None else (_as_text(p) or "") in (_as_text(v) or ""), "Substring test"),
    # numeric
    _Function("round", 1, 2, _round_half_up, "Round half-up to N decimal places"),
    _Function("truncate", 1, 2, _fn_truncate, "Drop digits beyond N places"),
    _number_fn("abs", abs, "Absolute value"),
    _number_fn("ceil", lambda d: d.to_integral_value(rounding="ROUND_CEILING"), "Round up"),
    _number_fn("floor", lambda d: d.to_integral_value(rounding="ROUND_FLOOR"), "Round down"),
    _number_fn("sign", lambda d: Decimal(0) if d == 0 else Decimal(1 if d > 0 else -1), "-1, 0 or 1"),
    _Function("clamp", 3, 3, _fn_clamp, "Hold a value between two bounds"),
    _Function("least", 1, -1, _fn_least, "Smallest non-null value"),
    _Function("greatest", 1, -1, _fn_greatest, "Largest non-null value"),
    # typing
    _Function("to_number", 1, 1, _fn_to_number, "Parse a human-written number"),
    _Function("to_text", 1, 1, lambda v: _as_text(v), "Render as text"),
    _Function("to_date", 1, 2, _fn_to_date, "Parse a date with an explicit format"),
    _Function("format_date", 2, 2, _fn_format_date, "Render a date with a format"),
    _Function("to_boolean", 1, 1, _fn_to_boolean, "Parse Y/N, 1/0, true/false"),
    # null and conditional
    _Function("coalesce", 1, -1, _fn_coalesce, "First value that is not blank"),
    _Function("nullif", 2, 2, _fn_nullif, "Null when the value equals the sentinel"),
    _Function("is_null", 1, 1, lambda v: is_blank(v), "Whether the value is blank"),
    _Function("is_not_null", 1, 1, lambda v: not is_blank(v), "Whether a value is present"),
    _Function("if", 2, 3, _fn_if, "Conditional value", True),
)

FUNCTIONS: dict[str, _Function] = {f.name: f for f in _FUNCTION_LIST}


def describe_functions() -> list[dict[str, Any]]:
    """The function catalog, for the Shape editor's help panel."""
    return [
        {
            "name": f.name,
            "min_args": f.min_args,
            "max_args": f.max_args,
            "summary": f.summary,
        }
        for f in sorted(_FUNCTION_LIST, key=lambda f: f.name)
    ]


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

# Binding power per operator; higher binds tighter.
_PRECEDENCE = {
    "or": 1,
    "and": 2,
    "=": 3, "==": 3, "<>": 3, "!=": 3, "<": 3, "<=": 3, ">": 3, ">=": 3,
    "+": 4, "-": 4,
    "*": 5, "/": 5, "%": 5,
}


class _Parser:
    def __init__(self, source: str):
        self.source = source
        self.tokens = _tokenize(source)
        self.index = 0

    @property
    def current(self) -> _Token:
        return self.tokens[self.index]

    def advance(self) -> _Token:
        token = self.tokens[self.index]
        self.index += 1
        return token

    def expect(self, text: str) -> None:
        token = self.current
        if token.text != text:
            raise ExpressionError(
                f"expected {text!r} at position {token.pos + 1}, got "
                f"{token.text or 'end of expression'!r}"
            )
        self.advance()

    def parse(self) -> _Node:
        node = self.parse_expression(0)
        if self.current.kind != "end":
            raise ExpressionError(
                f"unexpected {self.current.text!r} at position {self.current.pos + 1}"
            )
        return node

    def parse_expression(self, min_power: int) -> _Node:
        node = self.parse_prefix()
        while True:
            token = self.current
            op = token.text.casefold() if token.kind == "keyword" else token.text
            power = _PRECEDENCE.get(op) if token.kind in ("punct", "keyword") else None
            if power is None or power < min_power:
                return node
            self.advance()
            right = self.parse_expression(power + 1)
            node = _Binary(op, node, right)

    def parse_prefix(self) -> _Node:
        token = self.advance()
        if token.kind == "number":
            return _Literal(Decimal(token.text))
        if token.kind == "string":
            return _Literal(token.text)
        if token.kind == "keyword":
            word = token.text.casefold()
            if word == "null":
                return _Literal(None)
            if word == "true":
                return _Literal(True)
            if word == "false":
                return _Literal(False)
            if word == "not":
                return _Unary("not", self.parse_expression(_PRECEDENCE["and"] + 1))
            raise ExpressionError(
                f"'{token.text}' cannot start an expression (position {token.pos + 1})"
            )
        if token.kind == "punct":
            if token.text == "(":
                node = self.parse_expression(0)
                self.expect(")")
                return node
            if token.text == "-":
                return _Unary("-", self.parse_expression(_PRECEDENCE["*"]))
            if token.text == "+":
                return self.parse_expression(_PRECEDENCE["*"])
            raise ExpressionError(
                f"{token.text!r} cannot start an expression (position {token.pos + 1})"
            )
        if token.kind == "end":
            # Only genuinely empty at position 0. Reaching the end mid-parse means
            # an operator is missing its right-hand value ("[status] <>"), and
            # calling that "empty" sends the operator looking for the wrong fault.
            if token.pos == 0:
                raise ExpressionError("expression is empty")
            raise ExpressionError(
                f"expression ends after position {token.pos}: a value is missing"
            )

        # A name: either a function call or a column reference.
        if self.current.kind == "punct" and self.current.text == "(":
            name = token.text.casefold()
            self.advance()
            args: list[_Node] = []
            if not (self.current.kind == "punct" and self.current.text == ")"):
                while True:
                    args.append(self.parse_expression(0))
                    if self.current.kind == "punct" and self.current.text == ",":
                        self.advance()
                        continue
                    break
            self.expect(")")
            spec = FUNCTIONS.get(name)
            if spec is None:
                raise ExpressionError(
                    f"unknown function '{token.text}' — the catalog is "
                    f"{', '.join(sorted(FUNCTIONS))}"
                )
            if not spec.accepts(len(args)):
                allowed = (
                    f"{spec.min_args}+"
                    if spec.max_args < 0
                    else (
                        str(spec.min_args)
                        if spec.min_args == spec.max_args
                        else f"{spec.min_args}-{spec.max_args}"
                    )
                )
                raise ExpressionError(
                    f"{name}() takes {allowed} argument(s), got {len(args)}"
                )
            return _Call(name, tuple(args))
        return _Column(token.text)


@dataclass(frozen=True, slots=True)
class Expression:
    """A parsed, checked expression that can be evaluated per row."""

    source: str
    root: _Node

    @property
    def columns(self) -> frozenset[str]:
        """Source columns this expression reads — used for design-time checks."""
        return frozenset(self.root.columns())

    def evaluate(self, row: Mapping[str, Any]) -> Any:
        return self.root.evaluate(row)

    def matches(self, row: Mapping[str, Any]) -> bool:
        """Truth of a predicate, with unknown treated as *not* matching."""
        value = self.root.evaluate(row)
        if value is None:
            return False
        return _as_bool(value)

    def canonical(self) -> Any:
        """Shape-independent form, so the recipe hash ignores whitespace."""
        return self.root.canonical()


def compile_expression(
    source: str,
    *,
    known_columns: Sequence[str] | None = None,
    label: str = "expression",
) -> Expression:
    """Parse and check an expression, refusing anything it cannot prove safe.

    ``known_columns`` turns a typo into a design-time refusal naming the closest
    real column instead of a per-row failure at scale.
    """
    text = (source or "").strip()
    if not text:
        raise ExpressionError(f"{label} is empty")
    if len(text) > 4000:
        raise ExpressionError(f"{label} is longer than 4000 characters")
    root = _Parser(text).parse()
    expression = Expression(text, root)
    if known_columns is not None:
        available = list(known_columns)
        lookup = {c.casefold(): c for c in available}
        for name in sorted(expression.columns):
            if name in available:
                continue
            near = lookup.get(name.casefold())
            if near:
                raise ExpressionError(
                    f"{label} refers to column '{name}'; the source spells it "
                    f"'{near}' — column names are case-sensitive here"
                )
            raise ExpressionError(
                f"{label} refers to column '{name}', which the source does not "
                f"have. Available: {', '.join(available[:20])}"
                + (" …" if len(available) > 20 else "")
            )
    return expression
