"""The declared type of a derived column, propagated through its expression.

A recipe ``derive amount2 = [amount] * 2`` used to be typed by re-inferring the
shaped *sample*: 1,000 rows of ``amount = id x 1.37`` sampled as values below
1,000, so ``amount2`` was declared ``DECIMAL(5,2)`` and then treated as a
declared type by the population scan — 636 rows overflowed (QA round 11, T5).

The type of ``DECIMAL(10,2) * 2`` is not a sample question. SQL's own result
rules give it from the operands:

* ``a + b`` / ``a - b``: scale ``max(s1, s2)``, integer digits
  ``max(p1 - s1, p2 - s2) + 1``;
* ``a * b``: precision ``p1 + p2``, scale ``s1 + s2``;
* ``a / b`` and ``a % b``: no fixed scale — an open ``DECIMAL``;
* unary minus keeps the operand's type; a FLOAT operand makes the result FLOAT.

Anything else (a function, CASE, text) returns ``None`` and the caller keeps its
own rule. A result wider than 38 digits is an open ``DECIMAL``.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

_MAX_PRECISION = 38
_INTEGER_DIGITS = {"smallint": 5, "int2": 5, "integer": 10, "int": 10, "int4": 10,
                   "bigint": 19, "int8": 19, "tinyint": 3}


class _Open(Exception):
    """The result has no fixed (p, s): an open DECIMAL."""


def _decimal_of(type_text: str) -> tuple[int, int] | str | None:
    """(precision, scale) for an exact numeric, ``"float"``, or None (not numeric)."""
    from services.type_system import normalize_logical_type, parse_numeric_precision_scale

    raw = str(type_text or "").strip()
    logical = normalize_logical_type(raw)
    if logical == "float":
        return "float"
    if logical == "decimal":
        p, s = parse_numeric_precision_scale(raw)
        if p is None:
            raise _Open
        return int(p), int(s or 0)
    head = raw.lower().split("(", 1)[0].strip().split()[0] if raw else ""
    if head in _INTEGER_DIGITS:
        return _INTEGER_DIGITS[head], 0
    return None


def _literal(value: Any) -> tuple[int, int] | None:
    if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
        return None
    number = Decimal(value).normalize() if not isinstance(value, int) else Decimal(value)
    sign, digits, exponent = number.as_tuple()
    scale = max(-int(exponent), 0)
    precision = max(len(digits), scale) if digits else 1
    if int(exponent) > 0:
        precision += int(exponent)
    return max(precision, 1), scale


def _type_of(node: Any, prior: Mapping[str, str]) -> tuple[int, int] | str | None:
    from services import shape_expr as ex

    if isinstance(node, ex._Literal):
        return _literal(node.value)
    if isinstance(node, ex._Column):
        if node.name not in prior:
            return None
        return _decimal_of(prior[node.name])
    if isinstance(node, ex._Unary) and node.op == "-":
        return _type_of(node.operand, prior)
    if isinstance(node, ex._Binary) and node.op in {"+", "-", "*", "/", "%"}:
        left = _type_of(node.left, prior)
        right = _type_of(node.right, prior)
        if left is None or right is None:
            return None
        if left == "float" or right == "float":
            return "float"
        if node.op in {"/", "%"}:
            raise _Open
        (p1, s1), (p2, s2) = left, right  # type: ignore[misc]
        if node.op == "*":
            return p1 + p2, s1 + s2
        scale = max(s1, s2)
        return max(p1 - s1, p2 - s2) + 1 + scale, scale
    return None


def derived_column_type(expression: Any, prior: Mapping[str, str]) -> str | None:
    """Declared type of ``expression`` over columns typed by ``prior``, or None."""
    root = getattr(expression, "root", None)
    if root is None:
        return None
    try:
        result = _type_of(root, prior)
    except _Open:
        return "DECIMAL"
    if result is None:
        return None
    if result == "float":
        return "DOUBLE PRECISION"
    precision, scale = result  # type: ignore[misc]
    if precision > _MAX_PRECISION:
        return "DECIMAL"
    return f"DECIMAL({max(precision, scale, 1)},{scale})"


def propagated_derive_types(recipe: Any, prior: Mapping[str, str]) -> dict[str, str]:
    """Types of derive targets no later step rewrites, from untouched operands.

    A derive that reads a column an earlier step rewrote, or whose target a
    later step rewrites, keeps the caller's rule (the shaped values describe it).
    """
    steps = list(getattr(recipe, "enabled_steps", []) or [])
    out: dict[str, str] = {}
    rewritten: set[str] = set()
    for index, step in enumerate(steps):
        writes = set(getattr(step, "writes", ()) or ())
        if step.op == "derive_column" and step.expression is not None:
            target = str(step.options.get("to") or "")
            reads = set(step.expression.columns)
            later = set()
            for after in steps[index + 1:]:
                later |= set(getattr(after, "writes", ()) or ())
            if target and not (reads & rewritten) and target not in later:
                typed = derived_column_type(step.expression, prior)
                if typed:
                    out[target] = typed
        rewritten |= writes
    return out
