"""Load vertical abbreviation packs into the semantic mapper SSOT.

Healthcare/finance gazetteers extend ABBREVIATIONS without inventing a second
mapping engine. Fail-closed gates still never trust embeddings.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_DIR = Path(__file__).resolve().parent
_PACKS = ("healthcare.json", "finance.json")


def load_domain_abbreviations() -> dict[str, str]:
    out: dict[str, str] = {}
    for name in _PACKS:
        path = _DIR / name
        if not path.exists():
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(raw, dict):
            continue
        for key, value in raw.items():
            k = str(key or "").strip().lower()
            v = str(value or "").strip().lower()
            if k and v:
                out[k] = v
    return out


def merge_abbreviations(base: dict[str, str]) -> dict[str, str]:
    """Return a new dict: base first, then domain packs (packs fill gaps only)."""
    merged = dict(base)
    for key, value in load_domain_abbreviations().items():
        merged.setdefault(key, value)
    return merged


def gazetteer_stats() -> dict[str, Any]:
    packs = load_domain_abbreviations()
    return {
        "domain_entries": len(packs),
        "packs": list(_PACKS),
        "honesty": "Abbreviation boosts only - never override DDL/identity gates.",
    }
