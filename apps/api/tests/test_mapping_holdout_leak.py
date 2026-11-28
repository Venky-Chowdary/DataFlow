"""Leak check: the policed fixture's answers stay out of the mapper's knowledge.

A ``dev2`` answer (source -> gold column) may not be associated, by whole name
or by a token only one side has (plurals included), in the abbreviation
dictionary, gazetteers, synonym lists, schematic index, training lexicon, ML
vocabulary, or any knowledge file (quoted or not, backticks, a multi-line
dict). Associations the base code already held are listed in the fixture.
"""

from __future__ import annotations

import logging
from pathlib import Path

from services import mapping_holdout_leak as leak
from services.mapping_holdout_eval import load_holdout

PAIR = [("t", "ovdft_lmt", "overdraft_limit")]


def test_no_new_dev2_association_in_mapper_knowledge() -> None:
    doc = load_holdout()
    logging.disable(logging.WARNING)
    try:
        found = set(leak.find_leaks(doc))
    finally:
        logging.disable(logging.NOTSET)
    new = sorted(found - leak.grandfathered(doc))
    assert not new, f"dev2 answers leaked into mapper knowledge: {new}. Remove them; tune on dev only."


def test_grandfathered_entries_are_well_formed() -> None:
    for kind, a, b in leak.grandfathered(load_holdout()):
        assert kind in ("name", "token") and a and b


def test_file_scanner_finds_every_written_form(tmp_path: Path) -> None:
    forms = {
        "plain.csv": "src,dst\novdft_lmt,overdraft_limit\n",
        "backticks.txt": "maps `ovdft_lmt` onto `overdraft_limit`\n",
        "yaml.yaml": "ovdft_lmt:\n  - overdraft_limit\n",
        "multiline.py": 'X = {\n    "ovdft":\n        "overdraft",\n}\n',
        "plural.json": '{"ovdfts": "overdrafts"}\n',
    }
    for name, text in forms.items():
        path = tmp_path / name
        path.write_text(text, encoding="utf-8")
        assert leak.file_leaks(PAIR, files=[path]), name
    clean = tmp_path / "clean.txt"
    clean.write_text("overdraft appears here\n\n\n\nand ovdft far below\n", encoding="utf-8")
    assert leak.file_leaks(PAIR, files=[clean]) == set()
    assert ("name", "ovdft_lmt", "overdraft_limit") in leak.file_leaks(PAIR, files=[tmp_path / "plain.csv"])
    assert ("token", "ovdft", "overdraft") in leak.file_leaks(PAIR, files=[tmp_path / "multiline.py"])


def test_scan_covers_the_policy_word_lists_and_the_real_tree() -> None:
    files = {p.name for p in leak.scan_files()}
    for name in ("mapping_assignment_policy.py", "schematic_index.py", "semantic_abbreviations.py",
                 "synonyms.py", "mapping_golden.json", "test_mapping_assignment_policy.py"):
        assert name in files, name


def test_dictionary_half_detects_a_new_entry(monkeypatch) -> None:
    import services.semantic_abbreviations as sa

    assert not leak.dictionary_leaks(PAIR)
    monkeypatch.setitem(sa.ABBREVIATIONS, "ovdft", "overdraft")
    assert ("token", "ovdft", "overdraft") in leak.dictionary_leaks(PAIR)


def test_summary_is_counts_only() -> None:
    summary = leak.leak_summary(load_holdout())
    assert set(summary) == {"answers", "associations_found", "new_associations", "answers_touched_by_known",
                            "answers_touched_by_new", "answers_with_whole_name_known"}
    assert all(isinstance(v, int) for v in summary.values())
    assert summary["new_associations"] == 0
