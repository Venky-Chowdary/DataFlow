"""Digests of common passwords, so a hashed password is not accepted as a hashing key.

An admin who pastes ``sha256("password")`` as the workspace hash_pii key has
a key anyone can guess, and every hashed column under it is reversible by a
dictionary. ``services.pii_hash_keys.key_strength_problem`` refuses the raw
MD5 / SHA-1 / SHA-256 digests of this compact list (common-password lists'
heads, plus their capitalised and digit-suffixed forms).
"""

from __future__ import annotations

import hashlib
from functools import lru_cache

COMMON_PASSWORDS = (
    "password", "123456", "12345678", "123456789", "1234567890", "qwerty", "abc123",
    "111111", "123123", "admin", "letmein", "welcome", "monkey", "dragon", "master",
    "sunshine", "princess", "football", "baseball", "iloveyou", "trustno1", "shadow",
    "superman", "batman", "michael", "jennifer", "hunter", "hunter2", "secret",
    "passw0rd", "p@ssw0rd", "password1", "password123", "qwerty123", "qwertyuiop",
    "1q2w3e4r", "zaq12wsx", "asdfgh", "asdfghjkl", "000000", "654321", "666666",
    "121212", "7777777", "987654321", "freedom", "whatever", "charlie", "donald",
    "login", "starwars", "solo", "access", "flower", "hello", "loveme", "mustang",
    "pass", "test", "test123", "changeme", "default", "root", "toor", "guest",
    "correcthorsebatterystaple", "correct horse battery staple", "datawrap", "dataflow",
    "pii", "hashkey", "secretkey", "mysecret", "supersecret", "s3cr3t",
)


#: Common words of six or more letters. An "encoded" key whose text spells one
#: (case-insensitive) is a phrase that happens to be valid base64
#: ("correcthorsebatterystaple…") — random base64 contains one with negligible
#: probability (about (1/32)^6 per position and word, case-insensitive).
COMMON_WORDS = tuple(sorted({
    *(w for w in COMMON_PASSWORDS if len(w) >= 6 and w.isalpha()),
    "about", "above", "after", "again", "alpha", "apple", "battery", "beach", "black",
    "bravo", "brown", "build", "change", "charlie", "chicken", "close", "cloud", "correct",
    "dance", "delta", "dream", "earth", "eight", "every", "first", "forest", "fox", "garden",
    "green", "happy", "heart", "horse", "house", "jumps", "kitten", "large", "laugh", "lazy",
    "light", "lucky", "magic", "money", "morning", "mother", "music", "night", "ocean",
    "orange", "other", "paper", "party", "phone", "pizza", "planet", "plant", "power",
    "purple", "quick", "river", "rocket", "secret", "seven", "silver", "smile", "snake",
    "space", "staple", "storm", "summer", "table", "thing", "three", "tiger", "today",
    "train", "water", "white", "winter", "world", "yellow", "zebra", "workspace", "digest",
    "datawrap", "dataflow", "company", "customer", "private", "public", "random",
}))


def spells_common_word(text: str) -> bool:
    low = text.lower()
    return any(len(w) >= 6 and w in low for w in COMMON_WORDS)


@lru_cache(maxsize=1)
def common_password_digests() -> frozenset[bytes]:
    words: set[str] = set()
    for word in COMMON_PASSWORDS:
        for form in (word, word.capitalize(), word.upper(), f"{word}1", f"{word}123", f"{word}!"):
            words.add(form)
    out: set[bytes] = set()
    for form in words:
        raw = form.encode("utf-8")
        for algo in (hashlib.md5, hashlib.sha1, hashlib.sha256):
            out.add(algo(raw).digest())  # nosec B324 - recognising digests, not hashing
    return frozenset(out)
