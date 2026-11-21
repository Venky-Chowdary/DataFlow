"""Leak check: the held-out test split's answers stay out of mapper knowledge.

A test-split answer (source -> gold destination column) must not be added to
the abbreviation dictionary, domain gazetteers, synonym lists, training
lexicon, ML vocabulary, mapping fixtures or mapping tests. Overlaps that
already existed when the set was frozen are listed in the fixture under
``known_overlap_at_freeze``; anything new fails here. Tune on ``dev`` only.
"""

from __future__ import annotations

import logging

from services.mapping_holdout_eval import load_holdout
from services.mapping_holdout_leak import dictionary_leaks, find_leaks, line_leaks, heldout_test_pairs


def test_no_new_test_split_answer_in_mapper_knowledge() -> None:
    doc = load_holdout()
    known = {tuple(e) for e in doc["known_overlap_at_freeze"]["entries"]}
    logging.disable(logging.WARNING)
    try:
        found = set(find_leaks(doc))
    finally:
        logging.disable(logging.NOTSET)
    new = sorted(found - known)
    assert not new, (
        "Held-out TEST answers leaked into mapper knowledge (dictionaries, synonyms, "
        f"training data or mapping fixtures/tests): {new}. Remove them; tune on the dev split only."
    )


def test_leak_detector_catches_dictionary_and_fixture_leaks(monkeypatch) -> None:
    doc = load_holdout()
    pairs = heldout_test_pairs(doc)
    assert pairs, "test split has non-trivial answers"
    # Pick a test answer whose distinctive source token is unknown today.
    table, src, gold = next(p for p in pairs if p[1] == "ovdft_lmt")
    assert not dictionary_leaks([(table, src, gold)])

    import services.semantic_abbreviations as sa

    monkeypatch.setitem(sa.ABBREVIATIONS, "ovdft", "overdraft")
    assert any(k.startswith("dict:abbreviations") for k, _s, _g in dictionary_leaks([(table, src, gold)]))

    import services.mapping_holdout_leak as leak

    rows = (("apps/api/tests/fixtures/x.json", 1, frozenset({"ovdft_lmt", "overdraft_limit"})),)
    monkeypatch.setattr(leak, "_quoted_lines", lambda: rows)
    assert line_leaks([(table, src, gold)]) == {("line:apps/api/tests/fixtures/x.json", src, gold)}


def test_known_overlap_entries_are_well_formed() -> None:
    doc = load_holdout()
    entries = doc["known_overlap_at_freeze"]["entries"]
    test_answers = {(s, g) for _t, s, g in heldout_test_pairs(doc)}
    for kind, src, gold in entries:
        assert kind.startswith(("dict:", "line:"))
        assert (src, gold) in test_answers
