"""Parity Studio classifier: every cause rule, with cases that match and cases that must not.

Each rule is a pure function of (source, destination, catalog context). The
tables below pin the cause code and whether it counts as explained; the
negative rows pin what each rule must leave to another rule (or to
"unexplained").
"""

from __future__ import annotations

import pytest

from services import parity_classifier as pc
from services.parity_classifier import CellContext

PLAIN = CellContext()
CHAR10 = CellContext(dest_type="CHAR(10)")
REAL = CellContext(dest_type="REAL")
DOUBLE = CellContext(dest_type="DOUBLE PRECISION")
NUMERIC = CellContext(dest_type="NUMERIC(12,2)")
ORACLE = CellContext(dest_engine="oracle")


@pytest.mark.parametrize(
    ("rule", "source", "dest", "ctx", "explained"),
    [
        (pc.null_vs_empty, "", None, PLAIN, False),
        (pc.null_vs_empty, None, "", PLAIN, False),
        (pc.null_vs_empty, "", None, ORACLE, True),
        (pc.whitespace, "n5  ", "n5", PLAIN, False),
        (pc.whitespace, "  n5", "n5", PLAIN, False),
        (pc.whitespace, "a  b", "a b", PLAIN, False),
        (pc.whitespace, "ab", "ab        ", CHAR10, True),
        (pc.letter_case, "ABC", "abc", PLAIN, False),
        (pc.letter_case, "Straße", "STRASSE", PLAIN, False),
        (pc.unicode_normalization, "é", "é", PLAIN, True),
        (pc.unicode_normalization, "ﬁle", "file", PLAIN, False),
        (pc.encoding, "héllo", "hÃ©llo", PLAIN, False),
        (pc.encoding, "naïve café", "na?ve caf?", PLAIN, False),
        (pc.encoding, "zürich", "z�rich", PLAIN, False),
        (pc.truncation, "abcdef", "abc", PLAIN, False),
        (pc.truncation, "日本語テキスト", "日本語", PLAIN, False),
        (pc.timezone_shift, "2024-01-01T10:00:00+02:00", "2024-01-01T08:00:00Z", PLAIN, True),
        (pc.timezone_shift, "2024-01-01T10:00:00+02:00", "2024-01-01T10:00:00+00:00", PLAIN, False),
        (pc.timezone_shift, "2024-01-01T10:00:00+02:00", "2024-01-01 08:00:00", PLAIN, True),
        (pc.timezone_shift, "2024-01-01T10:00:00+02:00", "2024-01-01 10:00:00", PLAIN, False),
        (pc.timezone_shift, "2024-01-01 10:00:00", "2024-01-01 05:30:00", PLAIN, False),
        (pc.numeric_scale, "10.1049", "10.10", PLAIN, False),
        (pc.numeric_scale, "7.1249", "7.12", PLAIN, False),
        (pc.numeric_scale, "7.5", "8", PLAIN, False),
        (pc.numeric_scale, "10.19", "10.1", PLAIN, False),  # cut, not rounded
        (pc.numeric_scale, "10.10", "10.1", PLAIN, True),
        (pc.numeric_scale, "1.25", "1.2500", PLAIN, True),
        (pc.float_precision, "0.1", "0.10000000000000001", DOUBLE, True),
        # The same binary64 on a column that is not a float is still lost digits (review finding 2).
        (pc.float_precision, "0.1", "0.10000000000000001", PLAIN, False),
        (pc.float_precision, "3.14159274101257", "3.1415927", REAL, True),
        (pc.float_precision, "1.0000000001", "1.0000000002", DOUBLE, False),
        (pc.float_precision, "123456.789012", "123456.7890625", NUMERIC, False),
    ],
)
def test_rule_names_its_cause(rule, source, dest, ctx, explained):
    found = rule(source, dest, ctx)
    assert found is not None, (rule.__name__, source, dest)
    assert found["code"] == rule.__name__
    assert found["explained"] is explained
    assert found["label"] == pc.CAUSE_LABELS[found["code"]]
    assert not source or source not in found["detail"], "a detail never carries a value"


@pytest.mark.parametrize(
    ("rule", "source", "dest", "ctx"),
    [
        (pc.null_vs_empty, None, None, PLAIN),
        (pc.null_vs_empty, "", "", PLAIN),
        (pc.null_vs_empty, "a", None, PLAIN),
        (pc.whitespace, "ab", "ab", PLAIN),
        (pc.whitespace, "ab ", "abc", PLAIN),
        (pc.whitespace, None, "x", PLAIN),
        (pc.letter_case, "abc", "abd", PLAIN),
        (pc.letter_case, "abc", "abc", PLAIN),
        (pc.unicode_normalization, "cafe", "café", PLAIN),
        (pc.encoding, "abc", "abd", PLAIN),
        (pc.encoding, "abc", "a?c", PLAIN),  # '?' replacing ASCII is not a code-page loss
        (pc.truncation, "abc", "abcdef", PLAIN),
        (pc.truncation, "abcdef", "abd", PLAIN),
        (pc.truncation, "abcdef", "", PLAIN),
        (pc.truncation, "7.1249", "7.12", PLAIN),  # numbers are a scale question
        (pc.timezone_shift, "2024-01-01T10:00:00Z", "2024-01-01T10:00:00Z", PLAIN),
        (pc.timezone_shift, "2024-01-01 10:00:00", "2024-01-01 10:07:00", PLAIN),
        (pc.timezone_shift, "2024-01-01", "2024-01-02", PLAIN),
        (pc.timezone_shift, "not a date", "2024-01-01T10:00:00Z", PLAIN),
        (pc.numeric_scale, "10.12", "10.13", PLAIN),
        (pc.numeric_scale, "10.1", "10.2", PLAIN),
        (pc.numeric_scale, "1.5", "abc", PLAIN),
        (pc.numeric_scale, "1.2", "1.25", PLAIN),  # the destination holds more digits: no cut
        (pc.float_precision, "1.0", "1.5", DOUBLE),
        (pc.float_precision, "2", "2", DOUBLE),
        (pc.float_precision, "x", "1", DOUBLE),
    ],
)
def test_rule_leaves_other_pairs_alone(rule, source, dest, ctx):
    assert rule(source, dest, ctx) is None, (rule.__name__, source, dest)


def test_transform_applied_replays_the_runs_own_transform():
    ctx = CellContext(dest_type="TEXT", transforms=("trim",))
    found = pc.transform_applied("n5  ", "n5", "n5", ctx)
    assert found["code"] == "transform_applied" and found["explained"] is True
    assert "trim" in found["detail"]
    assert pc.transform_applied("n5", "n5", "n5", ctx) is None  # nothing changed
    assert pc.transform_applied("n5  ", "n6", "n5", ctx) is None  # replay does not give dest


def test_held_out_and_scoped_rows():
    assert pc.quarantined("7", {"7"})["explained"] is True
    assert pc.quarantined("8", {"7"}) is None
    assert pc.quarantined(None, {"7"}) is None
    assert pc.concurrent_source_update("7", ["7"])["code"] == "concurrent_source_update"
    assert pc.concurrent_source_update("7", []) is None
    assert pc.out_of_scope_population("extra_in_destination", outside_this_run=True)["explained"]
    assert pc.out_of_scope_population("extra_in_destination", outside_this_run=False) is None
    assert pc.out_of_scope_population("missing_in_destination", outside_this_run=True) is None
    accepted = pc.accepted_by_operator({"by": "a@x.com", "note": "known"})
    assert accepted["explained"] is True and "a@x.com" in accepted["detail"]
    assert pc.accepted_by_operator(None) is None


def test_classify_value_order_and_equality():
    text = CellContext(dest_type="TEXT")
    # Equal cells are no difference; Gate-8's own fingerprint decides with a type.
    assert pc.classify_value(expected="5.1249", dest="5.1249", ctx=PLAIN) is None
    assert pc.classify_value(expected="1", dest="1.0", ctx=PLAIN) is None
    assert pc.classify_value(expected="10.10", dest="10.1", ctx=NUMERIC) is None
    assert pc.classify_value(expected="n5  ", dest="n5", ctx=text)["code"] == "whitespace"
    assert pc.classify_value(expected="7.1249", dest="7.12", ctx=PLAIN)["code"] == "numeric_scale"
    assert pc.classify_value(expected="abc ", dest="ABC", ctx=PLAIN)["code"] == "unexplained"
    # Held-out keys win over any value rule.
    assert pc.classify_value(expected="a", dest="b", ctx=PLAIN, key="7", quarantined_keys={"7"})["code"] == (
        "quarantined"
    )
    # A transform that produced the destination value explains it.
    trim = CellContext(dest_type="TEXT", transforms=("trim",))
    found = pc.classify_value(expected="n5", dest="n5", source_raw="n5  ", raw_known=True, ctx=trim)
    assert found["code"] == "transform_applied"


def test_classify_row():
    assert pc.classify_row("missing_in_destination", key="4", quarantined_keys={"4"})["code"] == "quarantined"
    assert pc.classify_row("missing_in_destination", key="4")["code"] == "unexplained"
    assert pc.classify_row("extra_in_destination", key="9", outside_this_run=True)["code"] == (
        "out_of_scope_population"
    )
    assert pc.classify_row("extra_in_destination", key="9", drifted_keys={"9"})["code"] == (
        "concurrent_source_update"
    )
    assert pc.classify_row("duplicate_key", key=None)["explained"] is False


def test_every_cause_code_is_labelled_and_stable():
    codes = {
        "whitespace", "letter_case", "unicode_normalization", "numeric_scale", "float_precision",
        "timezone_shift", "null_vs_empty", "truncation", "encoding", "transform_applied",
        "quarantined", "accepted_by_operator", "concurrent_source_update",
        "out_of_scope_population", "unexplained",
    }
    assert set(pc.CAUSE_LABELS) == codes
    assert {r.__name__ for r in pc.VALUE_RULES} <= codes
