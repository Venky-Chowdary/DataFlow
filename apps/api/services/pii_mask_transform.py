"""The ``mask_pii`` transform's cell semantics, and the clear-value check (D355).

``mask_pii`` on a scalar cell masks the whole value: a recognised PII pattern
keeps its shape (``a***@****.com``); anything else keeps its length class only
(``Alice Johnson`` -> ``********``). It used to keep a short head and tail
(``Al*****on``, ``Alice …nson``): ten of fourteen characters of a name in
clear, and 35 of 40 common first names with a unique mask (algorithm audit
H7). No source character of an unrecognised value survives now; a mask is
never longer than its value, so it fits the same column. Operators who need
joinable pseudonyms use ``hash_pii``.

On a JSON document it masked only the pattern
matches inside the text, so every name, address, date of birth, short or
foreign phone number, unformatted SSN and luhn-invalid card number landed in
clear — on every destination, and a document holding none of the patterns
landed whole (QA: Timescale -> MongoDB, 6 of 6 rows). A JSON number the PAN
pattern matched was replaced by unquoted text, which is not JSON.

A document is now masked value by value: every string and number in it is
masked as the same value would be in a scalar column (keys, nesting, booleans
and nulls keep the document's shape). The pattern mask is kept for each value,
and a value no pattern changed falls back to the length-class mask — a
luhn-invalid 16-digit "card" is still a value the operator asked to mask.

``clear_tokens`` lists the values a cell carries (the cell, and each value of a
document) so the post-write protection proof can tell whether a destination
cell still holds a value the source held in clear.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

from services.pii_guard import PII_PATTERNS, _looks_structured
from services.pii_guard import mask as pattern_mask


class _JsonNumber(str):
    """A JSON number kept as the literal the document spelled."""


#: Characters a mask writes. A value made only of them is already masked.
_MASK_CHARS = frozenset("*…")


#: Longest mask written for a value no pattern recognises.
_MAX_CLASS_MASK = 32


def _length_class_mask(text: str) -> str:
    """The mask of a value no PII pattern recognises: its length class only.

    Stars, as many as the largest power of two that fits the value (the value's
    own length below four, at most 32). No character of the value survives, the
    exact length is not disclosed, and the mask is never longer than the value.
    """
    size = len(text)
    if size < 4:
        return "*" * size
    return "*" * min(1 << (size.bit_length() - 1), _MAX_CLASS_MASK)


def _pattern_recognised(text: str) -> bool:
    """A value ``pii_guard.mask`` masks by pattern, not by its head/tail fallback."""
    return bool(
        PII_PATTERNS["email"].fullmatch(text.strip())
        or _looks_structured(text)
        or any(p.search(text) for p in PII_PATTERNS.values())
    )


def mask_text(text: str) -> str:
    """Mask one scalar value; never return a non-empty value unchanged."""
    if not text or set(text) <= _MASK_CHARS:
        return text
    if _pattern_recognised(text):
        out = pattern_mask(text)
        if out != text:
            return out
        # A pattern matched nothing it would change (e.g. a luhn-invalid
        # 16-digit number): the operator still asked for the value masked.
    return _length_class_mask(text)


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
