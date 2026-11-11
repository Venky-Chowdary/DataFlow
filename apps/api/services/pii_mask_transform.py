"""The ``mask_pii`` transform's cell semantics, and the clear-value check (D355).

``mask_pii`` on a scalar cell has always masked the whole value: a recognised
PII pattern keeps its shape (``a***@****.com``), anything else keeps a short
head and tail (``Al*****on``). On a JSON document it masked only the pattern
matches inside the text, so every name, address, date of birth, short or
foreign phone number, unformatted SSN and luhn-invalid card number landed in
clear — on every destination, and a document holding none of the patterns
landed whole (QA: Timescale -> MongoDB, 6 of 6 rows). A JSON number the PAN
pattern matched was replaced by unquoted text, which is not JSON.

A document is now masked value by value: every string and number in it is
masked as the same value would be in a scalar column (keys, nesting, booleans
and nulls keep the document's shape). The pattern mask is kept for each value,
and a value no pattern changed falls back to the head/tail mask — a
luhn-invalid 16-digit "card" is still a value the operator asked to mask.

``clear_tokens`` lists the values a cell carries (the cell, and each value of a
document) so the post-write protection proof can tell whether a destination
cell still holds a value the source held in clear.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

from services.pii_guard import mask as pattern_mask


class _JsonNumber(str):
    """A JSON number kept as the literal the document spelled."""


#: Characters a mask writes. A value made only of them is already masked.
_MASK_CHARS = frozenset("*…")


def _head_tail_mask(text: str) -> str:
    """The scalar mask for a value no PII pattern recognises (pii_guard.mask)."""
    if len(text) <= 4:
        return "*" * len(text)
    if len(text) <= 12:
        return text[:2] + "*" * (len(text) - 4) + text[-2:]
    return text[:6] + "…" + text[-4:]


def mask_text(text: str) -> str:
    """Mask one scalar value; never return a non-empty value unchanged."""
    out = pattern_mask(text)
    if out == text and text and not set(text) <= _MASK_CHARS:
        # A pattern matched nothing it would change (e.g. a luhn-invalid
        # 16-digit number): the operator still asked for the value masked.
        out = _head_tail_mask(text)
    return out


def _parse_document(raw: Any, text: str) -> Any:
    """The JSON object / array a cell holds, or ``None`` for a scalar cell."""
    if isinstance(raw, (dict, list, tuple)):
        return raw
    if text[:1] not in ("{", "["):
        return None
    try:
        return json.loads(
            text,
            parse_int=_JsonNumber,
            parse_float=_JsonNumber,
            parse_constant=_JsonNumber,
        )
    except (ValueError, RecursionError):
        return None


def _mask_leaves(node: Any) -> Any:
    if isinstance(node, dict):
        return {k: _mask_leaves(v) for k, v in node.items()}
    if isinstance(node, (list, tuple)):
        return [_mask_leaves(v) for v in node]
    if node is None or isinstance(node, bool):
        return node
    return mask_text(str(node))


def mask_pii_cell(raw: Any, text: str) -> str:
    """``mask_pii`` of one non-empty cell. ``text`` is its decoded wire text."""
    doc = _parse_document(raw, text)
    if doc is None:
        return mask_text(text)
    return json.dumps(_mask_leaves(doc), ensure_ascii=False, separators=(",", ":"))


def _leaves(node: Any) -> Iterator[str]:
    if isinstance(node, dict):
        for v in node.values():
            yield from _leaves(v)
    elif isinstance(node, (list, tuple)):
        for v in node:
            yield from _leaves(v)
    elif node is not None and not isinstance(node, bool):
        yield str(node)


def clear_tokens(value: Any) -> set[str]:
    """Every value a cell carries: the cell's text and each value of a document.

    Empty text and text made only of mask characters are left out — they are
    never a clear value a protection could have leaked.
    """
    if value is None or isinstance(value, bool):
        return set()
    text = value if isinstance(value, str) else ""
    doc = _parse_document(value, text.strip())
    out = set(_leaves(doc)) if doc is not None else set()
    if not isinstance(value, (dict, list, tuple)):
        out.add(str(value).strip())
    return {t for t in out if t and not set(t) <= _MASK_CHARS}
