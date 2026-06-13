"""Training lexicon from synthetic schema pairs — boosts BM25 mapper."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=1)
def load_training_lexicon() -> dict[str, list[tuple[str, float]]]:
    """Map normalized source token → list of (target, weight) from training data."""
    lexicon: dict[str, list[tuple[str, float]]] = {}
    data_path = Path(__file__).resolve().parents[3] / "packages" / "ml" / "src" / "ml" / "data" / "synthetic_v1.jsonl"
    if not data_path.exists():
        return lexicon

    try:
        with data_path.open(encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                record = json.loads(line)
                for m in record.get("output", {}).get("mappings", []):
                    src = _norm(m.get("source", ""))
                    tgt = m.get("target", "")
                    conf = float(m.get("confidence", 0.9))
                    if not src or not tgt:
                        continue
                    lexicon.setdefault(src, [])
                    if not any(t == tgt for t, _ in lexicon[src]):
                        lexicon[src].append((tgt, conf))
    except Exception:
        return {}

    return lexicon


def lexicon_boost(source: str, target: str) -> float | None:
    lex = load_training_lexicon()
    src = _norm(source)
    tgt = _norm(target)
    for candidate, weight in lex.get(src, []):
        if _norm(candidate) == tgt:
            return min(weight, 0.98)
    for entries in lex.values():
        for candidate, weight in entries:
            if _norm(candidate) == tgt and src in _norm(candidate):
                return weight * 0.85
    return None


def _norm(name: str) -> str:
    import re

    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")
