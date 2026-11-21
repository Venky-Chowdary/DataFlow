"""Accuracy Track A items 2-3: assignment and suggestion policy around map_columns.

Names here are invented for the tests (none come from the held-out test split,
which tests/test_mapping_holdout_leak.py polices).
"""

from __future__ import annotations

import pytest

from services.mapping_assignment_policy import (
    inferred_abbreviation,
    name_kind,
    pin_decisive_pairs,
    rescore_pairs,
    sample_kind,
)
from services.semantic_mapper import map_columns


def _typed(cols):
    return [{"name": n, "inferred_type": t, "samples": list(s)} for n, t, s in cols]


def _by_source(rows):
    return {r["source"]: r for r in rows}


# ---------------- item 2: decisive pairs are not traded away ----------------


def test_mutual_best_pair_is_pinned_before_the_solver() -> None:
    scores = {
        ("a", "x"): (0.99, ""), ("a", "y"): (0.70, ""),
        ("b", "x"): (0.70, ""), ("b", "y"): (0.20, ""),
    }
    # Max-sum alone picks a->y + b->x (1.40) over a->x + b->y (1.19).
    assert pin_decisive_pairs({}, ["a", "b"], ["x", "y"], scores) == {"a": "x"}


def test_near_tie_is_left_to_the_solver() -> None:
    scores = {("a", "x"): (0.90, ""), ("a", "y"): (0.88, ""), ("b", "x"): (0.5, ""), ("b", "y"): (0.5, "")}
    assert pin_decisive_pairs({}, ["a", "b"], ["x", "y"], scores) == {}


def test_clear_abbreviation_is_not_traded_for_two_weak_pairs() -> None:
    # Max-sum alone gave div_cd -> first_name (0.70) + misc_txt -> division_code
    # (0.69) over div_cd -> division_code (0.95) + a leftover (0.11).
    src = _typed([("div_cd", "VARCHAR", ["OPS"]), ("misc_txt", "VARCHAR", ["foo"])])
    tgt = [{"name": "division_code", "inferred_type": "VARCHAR"}, {"name": "first_name", "inferred_type": "VARCHAR"}]
    out = _by_source(map_columns(
        [c["name"] for c in src], [t["name"] for t in tgt], source_schemas=src, target_schemas=tgt,
        destination_db_type="postgresql", destination_table_exists=True, source_db_type="oracle",
    ))
    assert out["div_cd"]["target"] == "division_code"
    assert out["misc_txt"]["target"] not in {"division_code", "first_name"}


# ---------------- item 3: candidates the dictionary lacks ----------------


@pytest.mark.parametrize(
    "source,target,prefixes,expected",
    [
        ("dsch_dt", "discharge_date", (), True),        # skeleton + dictionary token
        ("los", "length_of_stay", (), True),            # acronym across a stopword
        ("attd_phys", "attending_physician", (), True),  # skeleton + truncation
        ("wh_fnm", "first_name", ("wh",), True),         # entity prefix dropped
        ("bldg", "building_code", (), True),             # trailing generic leaf
        ("rate", "registration_date", (), False),        # a word is not a skeleton
        ("paid", "payment_id", (), False),               # inner vowels: not an abbreviation
        ("amount", "amount", (), False),                 # identical: lexical scorer's job
        ("st", "street", (), False),                     # two letters are too short
    ],
)
def test_inferred_abbreviation(source, target, prefixes, expected) -> None:
    expand = {"dt": ["date"], "cd": ["code"]}
    assert inferred_abbreviation(
        source, target, entity_prefixes=frozenset(prefixes), expand=lambda t: expand.get(t, [t])
    ) is expected


def test_inferred_candidate_is_proposed_for_review_never_auto() -> None:
    src = _typed([("dschg_dt", "TIMESTAMP", ["2024-01-02 10:00"]), ("wrd", "VARCHAR", ["W2"])])
    tgt = [{"name": "discharge_date", "inferred_type": "TIMESTAMP"}, {"name": "ward", "inferred_type": "VARCHAR"},
           {"name": "room", "inferred_type": "VARCHAR"}]
    out = _by_source(map_columns(
        [c["name"] for c in src], [t["name"] for t in tgt], source_schemas=src, target_schemas=tgt,
        destination_db_type="postgresql", destination_table_exists=True, source_db_type="postgresql",
    ))
    row = out["dschg_dt"]
    assert row["target"] == "discharge_date"
    assert row["requires_review"] is True and row["confidence"] < 0.85


def test_sample_and_name_kinds() -> None:
    assert sample_kind(["a@b.io", "c@d.com"]) == "email"
    assert sample_kind(["https://x.io"]) == "url"
    assert sample_kind(["ven_8Kx2", "ven_77"]) == "prefixed_id"
    assert sample_kind(["card_declined"]) == "other"  # snake_case word, not an id
    assert sample_kind([]) == ""
    assert name_kind("billing_email") == "email" and name_kind("homepage") == "url"
    assert name_kind("vendor_id") == ""


def test_email_samples_find_the_only_email_column() -> None:
    scores = {("notify_addr", "contact_email"): (0.3, "weak"), ("notify_addr", "vendor_id"): (0.3, "weak")}
    rescore_pairs(scores, ["notify_addr"], ["contact_email", "vendor_id"],
                  source_samples={"notify_addr": ["a@b.io", "c@d.org"]})
    assert scores[("notify_addr", "contact_email")][0] >= 0.80
    assert "review required" in scores[("notify_addr", "contact_email")][1]
    assert scores[("notify_addr", "vendor_id")][0] == 0.3
