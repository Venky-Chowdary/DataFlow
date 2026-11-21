"""Leak check for the held-out mapping benchmark's test split.

The test split is only honest while its answers stay out of everything the
mapper learns from. This module lists every place a test-split answer
(source column -> gold destination column) shows up in the mapper's knowledge:

* ``dict``: the abbreviation dictionary (with domain gazetteers), the synonym
  dictionary and its token abbreviations, the synthetic training lexicon and
  the ML baseline vocabulary, matched on the whole name and on the tokens that
  differ between source and gold (``ovdft`` -> ``overdraft``).
* ``line``: any line of a knowledge file (dictionaries, gazetteers, training
  data, mapping fixtures and mapping tests, see ``_SCAN_GLOBS``) that quotes
  both the source name and the gold name, e.g. ``"ovdft_lmt": "overdraft_limit"``.

Overlaps that existed when the set was frozen are recorded in the fixture
(``known_overlap_at_freeze``); the CI test fails on any new one, and on a
recorded one that no longer exists (so the list only shrinks).
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

API_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = API_ROOT.parents[1]
# Where mapping knowledge lives: dictionaries, synonym lists, gazetteers,
# training data, mapping fixtures and the tests that pin mapping answers.
_SCAN_GLOBS = (
    "apps/api/services/semantic_*.py",
    "apps/api/services/schematic_index.py",
    "apps/api/services/mapping_*.py",
    "apps/api/services/training_lexicon.py",
    "apps/api/services/domain_gazetteers/**/*",
    "apps/api/src/ai/knowledge/**/*.py",
    "apps/api/src/ai/semantic_engine.py",
    "apps/api/scripts/*golden*.py",
    "apps/api/tests/fixtures/**/*",
    "apps/api/tests/test_*mapp*.py",
    "apps/api/tests/test_*golden*.py",
    "apps/api/tests/test_*abbrev*.py",
    "apps/api/tests/test_*accuracy*.py",
    "apps/api/tests/test_*semantic*.py",
    "packages/ml/**/*",
)
_SCAN_SUFFIXES = {".py", ".json", ".jsonl", ".yaml", ".yml", ".csv", ".txt"}
# The benchmark itself, its evaluator and this checker necessarily name the answers.
_EXCLUDED_NAMES = {
    "mapping_holdout_v1.json",
    "mapping_holdout_eval.py",
    "mapping_holdout_leak.py",
    "test_mapping_holdout_leak.py",
    "test_mapping_holdout_gate.py",
}
_QUOTED = re.compile(r"""["']([A-Za-z0-9_.\[\]\- ]{1,64})["']""")
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")


def norm(name: str) -> str:
    s = _CAMEL.sub("_", str(name or "").strip())
    s = re.sub(r"[^a-z0-9]+", "_", s.lower())
    return re.sub(r"_+", "_", s).strip("_")


def tokens(name: str) -> set[str]:
    return {t for t in norm(name).split("_") if t}


def heldout_test_pairs(doc: dict[str, Any]) -> list[tuple[str, str, str]]:
    """``(table, source, gold)`` for every non-trivial answer in the test split."""
    out = []
    for t in doc["tables"]:
        if t["split"] != "test":
            continue
        for src, gold in t["gold"].items():
            if gold and norm(src) != norm(gold):
                out.append((t["id"], src, gold))
    return out


def _knowledge() -> dict[str, Any]:
    from services.semantic_abbreviations import ABBREVIATIONS

    try:
        from src.ai.knowledge.synonyms import SYNONYM_DICTIONARY, TOKEN_ABBREVIATIONS
    except ImportError:  # pragma: no cover - alternate sys.path layout
        from ai.knowledge.synonyms import SYNONYM_DICTIONARY, TOKEN_ABBREVIATIONS
    from services.training_lexicon import load_training_lexicon

    groups = [{norm(k), *(norm(v) for v in vs)} for k, vs in SYNONYM_DICTIONARY.items()]
    vocab: set[str] = set()
    base = REPO_ROOT / "packages" / "ml" / "models" / "baseline.json"
    if base.is_file():
        vocab = {norm(t) for t in json.loads(base.read_text(encoding="utf-8")).get("targets", [])}
    return {
        "abbreviations": {norm(k): norm(v) for k, v in ABBREVIATIONS.items()},
        "token_abbreviations": {norm(k): norm(v) for k, v in TOKEN_ABBREVIATIONS.items()},
        "synonym_groups": groups,
        "training_lexicon": {k: {norm(t) for t, _w in v} for k, v in load_training_lexicon().items()},
        "ml_vocab": vocab,
    }


def dictionary_leaks(pairs: Iterable[tuple[str, str, str]]) -> set[tuple[str, str, str]]:
    k = _knowledge()
    leaks: set[tuple[str, str, str]] = set()
    for _table, src, gold in pairs:
        s, g = norm(src), norm(gold)
        st, gt = tokens(src) - tokens(gold), tokens(gold) - tokens(src)
        for name in ("abbreviations", "token_abbreviations"):
            d = k[name]
            if d.get(s) == g:
                leaks.add((f"dict:{name}", src, gold))
            for tok in st:
                if tok in d and tokens(d[tok]) & gt:
                    leaks.add((f"dict:{name}:{tok}", src, gold))
        for grp in k["synonym_groups"]:
            if s in grp and g in grp:
                leaks.add(("dict:synonyms", src, gold))
            for tok in st:
                if tok in grp and grp & gt:
                    leaks.add((f"dict:synonyms:{tok}", src, gold))
        if g in k["training_lexicon"].get(s, set()):
            leaks.add(("dict:training_lexicon", src, gold))
        if s in k["ml_vocab"] and g in k["ml_vocab"]:
            leaks.add(("dict:ml_vocab", src, gold))
    return leaks


def _scan_files() -> Iterable[Path]:
    seen: set[Path] = set()
    for pattern in _SCAN_GLOBS:
        for path in sorted(REPO_ROOT.glob(pattern)):
            if (
                path in seen
                or path.suffix not in _SCAN_SUFFIXES
                or path.name in _EXCLUDED_NAMES
                or "__pycache__" in path.parts
                or not path.is_file()
            ):
                continue
            seen.add(path)
            yield path


@lru_cache(maxsize=1)
def _quoted_lines() -> tuple[tuple[str, int, frozenset[str]], ...]:
    rows = []
    for path in _scan_files():
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        rel = str(path.relative_to(REPO_ROOT))
        for i, line in enumerate(text.splitlines(), 1):
            found = _QUOTED.findall(line)
            if len(found) >= 2:
                rows.append((rel, i, frozenset(norm(q) for q in found)))
    return tuple(rows)


def line_leaks(pairs: Iterable[tuple[str, str, str]]) -> set[tuple[str, str, str]]:
    wanted = {(norm(s), norm(g)): (s, g) for _t, s, g in pairs}
    by_src: dict[str, list[str]] = {}
    for s, g in wanted:
        by_src.setdefault(s, []).append(g)
    leaks: set[tuple[str, str, str]] = set()
    for rel, _i, quoted in _quoted_lines():
        for s in quoted & by_src.keys():
            for g in by_src[s]:
                if g in quoted:
                    src, gold = wanted[(s, g)]
                    leaks.add((f"line:{rel}", src, gold))
    return leaks


def find_leaks(doc: dict[str, Any]) -> list[tuple[str, str, str]]:
    pairs = heldout_test_pairs(doc)
    return sorted(dictionary_leaks(pairs) | line_leaks(pairs))
