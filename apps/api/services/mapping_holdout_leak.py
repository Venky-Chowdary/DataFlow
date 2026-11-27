"""Leak check: a policed fixture's answers stay out of the mapper's knowledge.

Policed: the ``dev2`` split of ``tests/fixtures/mapping_holdout_v1.json`` and
any external fixture given by path (a sealed blind set is checked by the
lead with ``scripts/check_mapping_leaks.py --fixture PATH``, which prints
counts only). An answer is ``source -> gold destination``. Evidence is
recorded as two kinds of association, each keyed globally (once the mapper
knows it, it knows it wherever it is written):

* ``name``: the whole source name and the whole gold name (``ovdft_lmt`` and
  ``overdraft_limit``);
* ``token``: a token only the source has and a token only the gold has
  (``ovdft`` and ``overdraft``), singular and plural alike.

Sources of evidence: the abbreviation dictionary with its gazetteers, the
synonym dictionary and its token abbreviations, the synthetic training
lexicon, the ML vocabulary, the schematic index (does it resolve source to
gold?), and every knowledge file in ``_SCAN_GLOBS`` (dictionaries, the mapping
policy's word lists, fixtures, mapping tests), read as words in any quoting or
none, within a window of three lines so a multi-line dict entry counts.

Associations that existed in the base code before this benchmark was used
are recorded in the fixture (``known_overlap_at_freeze``); any other fails.
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
    # Type-system tests list type names ("int", "year"), not mapping answers.
    "test_accuracy_type_pairs.py",
    "test_accuracy_type_pairs_live.py",
    "test_type_fidelity_truth_table.py",
    "type_fidelity_truth_table.json",
}
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")


def norm(name: str) -> str:
    s = _CAMEL.sub("_", str(name or "").strip())
    s = re.sub(r"[^a-z0-9]+", "_", s.lower())
    return re.sub(r"_+", "_", s).strip("_")


def tokens(name: str) -> set[str]:
    return {t for t in norm(name).split("_") if t}


_WORD = re.compile(r"[A-Za-z][A-Za-z0-9]*(?:_[A-Za-z0-9]+)*")
WINDOW = 3


def stem(token: str) -> str:
    """Singular form for comparison (plurals, ``-ies``, ``-es``)."""
    t = token.lower()
    if len(t) > 4 and t.endswith("ies"):
        return t[:-3] + "y"
    if len(t) > 4 and t.endswith(("ses", "xes", "zes", "ches", "shes")):
        return t[:-2]
    if len(t) > 3 and t.endswith("s") and not t.endswith("ss"):
        return t[:-1]
    return t


def stems(name: str) -> set[str]:
    return {stem(t) for t in tokens(name)}


def policed_pairs(doc: dict[str, Any], split: str | None = "dev2") -> list[tuple[str, str, str]]:
    """``(table, source, gold)`` for every non-trivial answer of the policed tables.

    ``split=None`` polices every table (an external fixture)."""
    out = []
    for t in doc["tables"]:
        if split is not None and t.get("split") != split:
            continue
        for src, gold in t["gold"].items():
            if gold and norm(src) != norm(gold):
                out.append((t["id"], src, gold))
    return out


def _wanted(pairs: Iterable[tuple[str, str, str]]) -> tuple[dict, dict]:
    """name index ``norm(src) -> {norm(gold)}`` and token index ``stem -> {stem}``."""
    names: dict[str, set[str]] = {}
    toks: dict[str, set[str]] = {}
    for _t, src, gold in pairs:
        names.setdefault(norm(src), set()).add(norm(gold))
        st, gt = stems(src) - stems(gold), stems(gold) - stems(src)
        for a in st:
            if len(a) >= 2 and not a.isdigit():
                toks.setdefault(a, set()).update(g for g in gt if len(g) >= 2)
    return names, toks


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
    pairs = [(norm(k), norm(v)) for k, v in ABBREVIATIONS.items()]
    pairs += [(norm(k), norm(v)) for k, v in TOKEN_ABBREVIATIONS.items()]
    for k, vs in load_training_lexicon().items():
        pairs += [(norm(k), norm(t)) for t, _w in vs]
    return {"pairs": pairs, "groups": groups, "vocab": vocab}


def dictionary_leaks(pairs: Iterable[tuple[str, str, str]]) -> set[tuple[str, str, str]]:
    pairs = list(pairs)
    names, toks = _wanted(pairs)
    k = _knowledge()
    found: set[tuple[str, str, str]] = set()

    def assoc(a: str, b: str) -> None:
        if b in names.get(a, ()):
            found.add(("name", a, b))
        for x in stems(a):
            for y in stems(b) & toks.get(x, set()):
                found.add(("token", x, y))

    for a, b in k["pairs"]:
        assoc(a, b)
    for grp in k["groups"]:
        for a in grp:
            for b in grp:
                if a != b:
                    assoc(a, b)
    for a in k["vocab"]:
        for b in k["vocab"]:
            if a != b and b in names.get(a, ()):
                found.add(("name", a, b))
    try:
        from services.schematic_index import schematic_match_boost

        for _t, src, gold in pairs:
            if schematic_match_boost(src, gold) is not None:
                found.add(("name", norm(src), norm(gold)))
    except ImportError:  # pragma: no cover
        pass
    return found


def scan_files(base: Path = REPO_ROOT, globs: Iterable[str] = _SCAN_GLOBS) -> list[Path]:
    seen: set[Path] = set()
    out: list[Path] = []
    for pattern in globs:
        for path in sorted(base.glob(pattern)):
            if (
                path in seen
                or path.suffix not in _SCAN_SUFFIXES
                or path.name in _EXCLUDED_NAMES
                or "__pycache__" in path.parts
                or not path.is_file()
            ):
                continue
            seen.add(path)
            out.append(path)
    return out


@lru_cache(maxsize=8)
def _windows(paths: tuple[Path, ...]) -> tuple[tuple[frozenset[str], frozenset[str]], ...]:
    """Per window of lines: (normalised identifiers, token stems)."""
    rows = []
    for path in paths:
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        per_line = []
        for line in lines:
            idents = {norm(w) for w in _WORD.findall(line)}
            stems_ = {stem(t) for w in idents for t in w.split("_") if t}
            per_line.append((idents, stems_))
        for i in range(len(per_line)):
            chunk = per_line[i:i + WINDOW]
            rows.append((frozenset().union(*(c[0] for c in chunk)), frozenset().union(*(c[1] for c in chunk))))
    return tuple(rows)


def file_leaks(pairs: Iterable[tuple[str, str, str]], files: Iterable[Path] | None = None) -> set[tuple[str, str, str]]:
    names, toks = _wanted(pairs)
    paths = tuple(files) if files is not None else tuple(scan_files())
    found: set[tuple[str, str, str]] = set()
    for idents, st in _windows(paths):
        for a in idents & names.keys():
            for b in names[a] & idents:
                found.add(("name", a, b))
        for a in st & toks.keys():
            for b in toks[a] & st:
                found.add(("token", a, b))
    return found


def find_leaks(
    doc: dict[str, Any], split: str | None = "dev2", files: Iterable[Path] | None = None
) -> list[tuple[str, str, str]]:
    pairs = policed_pairs(doc, split)
    return sorted(dictionary_leaks(pairs) | file_leaks(pairs, files))


def grandfathered(doc: dict[str, Any]) -> set[tuple[str, str, str]]:
    return {tuple(e) for e in (doc.get("known_overlap_at_freeze") or {}).get("entries", [])}


def leak_summary(doc: dict[str, Any], split: str | None = "dev2") -> dict[str, Any]:
    """Counts only: how many answers are touched by known and by new associations."""
    pairs = policed_pairs(doc, split)
    found = set(find_leaks(doc, split))
    known = grandfathered(doc)

    def touched(assocs: set) -> int:
        n = 0
        for _t, src, gold in pairs:
            s_n, g_n = norm(src), norm(gold)
            st, gt = stems(src) - stems(gold), stems(gold) - stems(src)
            if any((k == "name" and a == s_n and b == g_n) or (k == "token" and a in st and b in gt)
                   for k, a, b in assocs):
                n += 1
        return n

    return {
        "answers": len(pairs),
        "associations_found": len(found),
        "new_associations": len(found - known),
        "answers_touched_by_known": touched(found & known),
        "answers_touched_by_new": touched(found - known),
        "answers_with_whole_name_known": sum(
            1 for _t, s, g in pairs if ("name", norm(s), norm(g)) in found),
    }
