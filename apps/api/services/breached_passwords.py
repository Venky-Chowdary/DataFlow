"""Offline breached-password backstop (D340) — no network, always on.

The HIBP range lookup (``services.hibp``) is the broad check, but it is
fail-open and needs outbound HTTPS. This small bundled list is the floor that
holds without it: the head of the public breach-corpus top lists in the forms
the 12-character minimum still lets through (``password1234``,
``qwertyuiop123``…), plus the shapes those lists are made of — a common
password (``services.common_password_digests.COMMON_PASSWORDS``) with only
digits/symbols appended, one repeated, a run of the digit row, or one or two
distinct characters. Everything is compared lower-case.
"""

from __future__ import annotations

from services.common_password_digests import COMMON_PASSWORDS

_COMMON = frozenset(w.lower() for w in COMMON_PASSWORDS)

#: Breach-corpus favourites of 12+ characters that the shape rules below miss.
_BREACHED_LONG = frozenset({
    "1q2w3e4r5t6y", "1q2w3e4r5t6y7u", "1qaz2wsx3edc", "1qaz2wsx3edc4rfv", "zaq12wsxcde3",
    "zaq1zaq1zaq1", "qazwsxedcrfv", "qazwsxedcrfvtgb", "123qweasdzxc", "1234qwerasdf",
    "qwer1234asdf", "qwerty123456", "qwertyuiopasdf", "asdfghjklqwerty", "abcdefghijkl",
    "abcdefghijklmn", "abc123abc123", "abcd1234abcd", "abc123456789", "a1b2c3d4e5f6",
    "iloveyouiloveyou", "administrator", "administrator1", "letmeinletmein",
    "welcomewelcome", "changemechangeme", "q1w2e3r4t5y6", "1a2b3c4d5e6f",
})

_SUFFIX = "0123456789!@#$%^&*()-_=+.,?~ "
_DIGIT_RUNS = "1234567890" * 4 + "0987654321" * 4


def is_breached_password(password: str) -> bool:
    """True when ``password`` is on the bundled list or has one of its shapes."""
    low = (password or "").strip().lower()
    if not low:
        return False
    if low in _BREACHED_LONG or low in _COMMON:
        return True
    if len(set(low)) <= 2:
        return True
    if low.isdigit() and low in _DIGIT_RUNS:
        return True
    base = low.rstrip(_SUFFIX)
    if base in _COMMON or base in _BREACHED_LONG:
        return True
    for word in _COMMON:
        if len(word) >= 4 and len(base) >= 2 * len(word) and base == word * (len(base) // len(word)):
            return True
    return False


__all__ = ["is_breached_password"]
