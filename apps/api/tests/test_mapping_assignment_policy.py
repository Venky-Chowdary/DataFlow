"""Accuracy Track A items 2-5: assignment and suggestion policy around map_columns.

Most names here are invented. Some come from the held-out ``dev`` split and are
dev knowledge, said openly: ``dsch_dt``, ``los``, ``attd_phys`` (abbreviation
cases) and the Stripe ``customer`` id case. None come from ``dev2``, which
tests/test_mapping_holdout_leak.py polices.
"""

from __future__ import annotations

import pytest

from services.mapping_assignment_policy import (
    SUGGESTION_FLOOR,
    _backfill_freed_alternatives,
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


def test_freed_column_backfills_a_no_match_row_for_review() -> None:
    rows = [
        {"source": "a", "target": "a", "create_new": True, "source_type": "VARCHAR", "confidence": 0.7,
         "assignment_strategy": "create_compatible_new", "requires_review": True,
         "alternatives": [{"target": "x", "confidence": 0.82, "reasoning": "r"}, {"target": "y", "confidence": 0.3}]},
        {"source": "b", "target": "y", "source_type": "VARCHAR", "confidence": 0.9, "requires_review": False},
    ]
    out = _backfill_freed_alternatives(rows, {"x", "y"}, {"dest_db": "postgresql", "target_types": {"x": "VARCHAR"}})
    assert out[0]["target"] == "x" and out[0]["requires_review"] is True
    assert "create_new" not in out[0]
    assert out[0]["confidence"] < 0.85
    # Below the floor or already used: stays a new column.
    rows[0]["alternatives"][0]["confidence"] = SUGGESTION_FLOOR - 0.01
    rows[0]["target"], rows[0]["create_new"] = "a", True
    assert _backfill_freed_alternatives([dict(rows[0])], {"x"}, {})[0]["create_new"] is True


# ---------------- item 3: candidates the dictionary lacks ----------------


@pytest.mark.parametrize(
    "source,target,prefixes,expected",
    [
        ("dsch_dt", "discharge_date", (), True),        # skeleton + dictionary token
        ("los", "length_of_stay", (), True),            # acronym across a stopword
        ("attd_phys", "attending_physician", (), True),  # skeleton + truncation
        ("wh_fnm", "first_name", ("wh",), True),         # entity prefix dropped
        ("prgm", "program_code", (), True),              # trailing generic leaf
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


# ---------------- item 4: suggestion floor ----------------


def test_low_confidence_leftover_is_not_proposed_onto_an_existing_column() -> None:
    src = _typed([("given_nm", "VARCHAR", ["Ann"]), ("legacy_ref", "VARCHAR", ["Q-9"])])
    tgt = [{"name": "given_name", "inferred_type": "VARCHAR"}, {"name": "account_id", "inferred_type": "VARCHAR"}]
    out = _by_source(map_columns(
        [c["name"] for c in src], [t["name"] for t in tgt], source_schemas=src, target_schemas=tgt,
        destination_db_type="postgresql", destination_table_exists=True, source_db_type="postgresql",
    ))
    leftover = out["legacy_ref"]
    assert leftover["target"] != "account_id"
    assert leftover.get("create_new") is True and leftover["requires_review"] is True
    # The candidate is still offered.
    assert (leftover.get("alternatives") or [{}])[0].get("target") == "account_id"


def test_every_proposal_onto_an_existing_column_meets_the_floor() -> None:
    from services.mapping_holdout_eval import run_holdout

    rows = run_holdout("dev")
    low = [(r["source"], r["pred"], r["conf"]) for r in rows if r["pred"] and r["conf"] < SUGGESTION_FLOOR]
    assert low == []


def test_auto_approve_floor_is_at_or_above_the_dev_calibrated_safe_threshold() -> None:
    from preflight.gates import g4_confidence_floor
    from services.mapping_holdout_eval import AUTO_APPROVE_FLOOR, calibrate_auto_threshold, run_holdout

    cal = calibrate_auto_threshold(run_holdout("dev"))
    assert g4_confidence_floor(None) == AUTO_APPROVE_FLOOR == 0.85
    # The dev data never supports lowering the floor (n is too small for a
    # 0.95 Wilson bound), so the gate stays where it is.
    assert cal["recommended"] is None or cal["recommended"] >= AUTO_APPROVE_FLOOR, cal
    # No threshold at or above the product floor falls short of 0.98 on dev.
    assert cal["highest_unsafe"] is None or cal["highest_unsafe"] < AUTO_APPROVE_FLOOR, cal
    assert cal["lowest_meeting_target"] is not None and cal["lowest_meeting_target"] <= AUTO_APPROVE_FLOOR


# ---------------- item 5: entity vs attribute ----------------


def test_id_shaped_entity_column_is_not_auto_mapped_to_an_email_attribute() -> None:
    src = _typed([("vendor", "VARCHAR", ["ven_8Kx2", "ven_77Qa"]), ("total", "DECIMAL", ["10.00"])])
    tgt = [{"name": "vendor_email", "inferred_type": "VARCHAR"}, {"name": "vendor_id", "inferred_type": "VARCHAR"},
           {"name": "total", "inferred_type": "DECIMAL"}]
    out = _by_source(map_columns(
        [c["name"] for c in src], [t["name"] for t in tgt], source_schemas=src, target_schemas=tgt,
        destination_db_type="postgresql", destination_table_exists=True, source_db_type="postgresql",
    ))
    row = out["vendor"]
    assert not (row["target"] == "vendor_email" and not row["requires_review"] and row["confidence"] >= 0.85), row
    assert row["target"] == "vendor_id"


def test_email_samples_still_map_onto_the_email_attribute() -> None:
    src = _typed([("vendor", "VARCHAR", ["a@b.io", "c@d.org"])])
    tgt = [{"name": "vendor_email", "inferred_type": "VARCHAR"}, {"name": "vendor_id", "inferred_type": "VARCHAR"}]
    out = _by_source(map_columns(
        ["vendor"], ["vendor_email", "vendor_id"], source_schemas=src, target_schemas=tgt,
        destination_db_type="postgresql", destination_table_exists=True, source_db_type="postgresql",
    ))
    assert out["vendor"]["target"] == "vendor_email"


def test_masked_samples_do_not_contradict_an_exact_email_name() -> None:
    src = _typed([("email", "VARCHAR", ["***", "***"])])
    tgt = [{"name": "email", "inferred_type": "VARCHAR"}]
    out = map_columns(["email"], ["email"], source_schemas=src, target_schemas=tgt,
                      destination_db_type="postgresql", destination_table_exists=True)
    assert out[0]["target"] == "email" and out[0]["confidence"] >= 0.85


# ---------------- review findings 1, 9, 10 ----------------


def _map(src, tgt):
    return _by_source(map_columns(
        [c["name"] for c in src], [t["name"] for t in tgt], source_schemas=src, target_schemas=tgt,
        destination_db_type="postgresql", destination_table_exists=True, source_db_type="postgresql",
    ))


@pytest.mark.parametrize(
    "name,samples,targets",
    [
        ("contact", ["a@b.io", "c@d.org"], ["contact_website", "contact_phone"]),
        ("customer", ["cus_9s6", "cus_1Ab"], ["customer_email", "customer_name"]),
        ("profile", ["usr_81a", "usr_2B"], ["profile_email", "profile_name"]),
    ],
)
def test_a_cap_never_hands_the_sibling_column_an_auto_approval(name, samples, targets) -> None:
    out = _map(_typed([(name, "VARCHAR", samples)]), [{"name": t, "inferred_type": "VARCHAR"} for t in targets])
    row = out[name]
    assert row.get("create_new") or row["requires_review"] is True, row
    assert row["confidence"] < 0.85


@pytest.mark.parametrize(
    "name,samples,target,other",
    [
        ("unsubscribe_url", ["https://x.io/u/1", "https://x.io/u/2"], "email_unsubscribe_url", "email_subject"),
        ("locale", ["en_US", "de_DE"], "email_locale", "email_subject"),
    ],
)
def test_head_word_decides_the_name_kind(name, samples, target, other) -> None:
    out = _map(_typed([(name, "VARCHAR", samples)]),
               [{"name": target, "inferred_type": "VARCHAR"}, {"name": other, "inferred_type": "VARCHAR"}])
    assert out[name]["target"] == target and not out[name].get("create_new")
    assert sample_kind(["en_US", "de_DE"]) == "other"
    assert name_kind("email_unsubscribe_url") == "url" and name_kind("email_locale") == ""


def test_a_real_word_is_not_an_initial_plus_stopword() -> None:
    assert inferred_abbreviation("cat", "created_at") is False
    assert inferred_abbreviation("los", "length_of_stay") is True
    out = _map(_typed([("cat", "VARCHAR", ["Shoes"])]),
               [{"name": "category", "inferred_type": "VARCHAR"}, {"name": "created_at", "inferred_type": "TIMESTAMP"}])
    assert out["cat"]["target"] == "category"
