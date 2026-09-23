"""Cupid / COMA-style linguistic matching for spoken column names.

Cupid (Madhavan, Bernstein, Rahm, VLDB 2001) combines token and n-gram
similarity, then a human checks leftovers. Similarity Flooding (Melnik,
García-Molina, Rahm, ICDE 2002) is the same honesty: unique winner +
threshold + gap; ambiguous pairs stay unbound.

This is the matcher, not a synonym dictionary. ``fname`` does not invent
``first_name``. ``cust_id`` may bind ``customer_id`` when that winner is
unique on the selected schema.
"""

from __future__ import annotations

import re

from .normalize import fold, split_qualified

_TOKEN = re.compile(r"[a-z0-9]+")
_CAMEL = re.compile(r"[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z]+|[A-Z]+|\d+")

# Short warehouse tokens collide (id / no / num / pk). Cupid filters these too.
_STOP = frozenset({"id", "no", "num", "pk", "fk", "key", "cd", "code", "val", "value"})


def name_tokens(value: str) -> list[str]:
    text = (value or "").strip()
    if not text:
        return []
    parts = _CAMEL.findall(text) or _TOKEN.findall(text.lower())
    out: list[str] = []
    for part in parts:
        token = part.lower()
        if token and token not in out:
            out.append(token)
    if not out:
        folded = fold(text)
        if folded:
            out.append(folded)
    return out


def trigram_dice(left: str, right: str) -> float:
    """Dice coefficient on padded character trigrams (COMA name matcher)."""
    a = fold(left)
    b = fold(right)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    pad_a = f"  {a} "
    pad_b = f"  {b} "
    grams_a = {pad_a[i:i + 3] for i in range(len(pad_a) - 2)}
    grams_b = {pad_b[i:i + 3] for i in range(len(pad_b) - 2)}
    if not grams_a or not grams_b:
        return 0.0
    return 2.0 * len(grams_a & grams_b) / (len(grams_a) + len(grams_b))


def _edit_le1(left: str, right: str) -> bool:
    if left == right:
        return True
    if abs(len(left) - len(right)) > 1:
        return False
    if len(left) > len(right):
        left, right = right, left
    if len(right) - len(left) == 1:
        i = j = 0
        skipped = False
        while i < len(left) and j < len(right):
            if left[i] == right[j]:
                i += 1
                j += 1
                continue
            if skipped:
                return False
            skipped = True
            j += 1
        return True
    mismatch = 0
    for a, b in zip(left, right):
        if a != b:
            mismatch += 1
            if mismatch > 1:
                return False
    return True


def token_align(left: list[str], right: list[str]) -> float:
    if not left or not right:
        return 0.0
    scored = 0.0
    for token in left:
        best = 0.0
        for other in right:
            if token == other:
                best = 1.0
                break
            if token in _STOP or other in _STOP:
                if token == other:
                    best = 1.0
                continue
            if len(token) >= 4 and len(other) >= 4:
                if other.startswith(token) or token.startswith(other):
                    best = max(best, 0.86)
                elif min(len(token), len(other)) >= 5 and _edit_le1(token, other):
                    best = max(best, 0.8)
        scored += best
    return scored / max(len(left), len(right))


def name_similarity(left: str, right: str) -> float:
    """Hybrid linguistic score in ``[0, 1]``. Exact fold is 1.0."""
    a = fold(left)
    b = fold(right)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    dice = trigram_dice(a, b)
    aligned = token_align(name_tokens(left), name_tokens(right))
    return max(dice, 0.6 * aligned + 0.4 * dice)


def unique_linguistic_match(
    needle: str,
    candidates: list[str],
    *,
    threshold: float = 0.72,
    gap: float = 0.12,
) -> tuple[str, float]:
    """The one candidate that uniquely wins, or ``('', 0)``."""
    table, column = split_qualified(needle)
    spoken = column or needle
    want = fold(spoken)
    _ = table
    if len(want) < 4 or not candidates:
        return "", 0.0
    ranked = sorted(
        ((candidate, name_similarity(spoken, split_qualified(candidate)[1] or candidate))
         for candidate in candidates),
        key=lambda item: item[1],
        reverse=True,
    )
    if not ranked or ranked[0][1] < threshold:
        return "", 0.0
    if len(ranked) > 1 and ranked[0][1] - ranked[1][1] < gap:
        return "", 0.0
    return ranked[0][0], ranked[0][1]
