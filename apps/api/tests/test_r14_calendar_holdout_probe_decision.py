"""QA browser 3 (review follow-up): the invalid-calendar holdout is decided once, from the refusal.

services.coercion_probe decides it per column next to the numeric-cast file
holdout, by matching each failure's REASON (connectors.sql_temporal.
is_invalid_calendar_refusal), counted on every failure and not only on the
five listed examples. G3 and G8 read ``holdout_kind``; they no longer
re-decide on the cell's value. The bound is explicit: the impossible days
must stay below INVALID_CALENDAR_MAX_SHARE (one in five) of the sampled
values; Execute holds the same cells out beyond the sample either way.
"""

from __future__ import annotations

import pytest

import services.coercion_probe as cp

GOOD = [{"d": f"2024-01-{i:02d}"} for i in range(1, 20)]


def _probe(rows, *, mode="balanced", file_rows=True):
    report = cp.analyze_coercion(
        sample_rows=rows, mappings=[{"source": "d", "target": "d"}],
        source_types={"d": "DATE"}, dest_types={"d": "DATE"}, dest_db_type="postgresql",
        validation_mode=mode, file_row_quarantine=file_rows,
    )
    return report["by_source"]["d"]


def test_a_minority_of_impossible_days_in_a_file_is_a_balanced_holdout():
    col = _probe([*GOOD, {"d": "2024-02-30"}])
    assert (col["severity"], col["holdout_kind"]) == ("warn", "invalid_calendar")


@pytest.mark.parametrize("kwargs", [{"mode": "strict"}, {"file_rows": False}])
def test_strict_and_a_database_extract_are_not_this_holdout(kwargs):
    col = _probe([*GOOD, {"d": "2024-02-30"}], **kwargs)
    assert (col["severity"], col["holdout_kind"]) == ("block", "")


def test_lenient_holds_out_every_cast_refusal_under_the_same_policy():
    col = _probe([*GOOD, {"d": "2024-02-30"}, {"d": "abc"}], mode="lenient")
    assert col["holdout_kind"] == "cast_failure"


@pytest.mark.parametrize("mode,reason,record_batch,held", [
    ("lenient", "Invalid integer: 'abc'", True, True),
    ("lenient", None, False, True),
    ("balanced", "Invalid date: '2024-02-30' is not a real calendar date (2024-02 has 29 days)", True, True),
    ("balanced", "Invalid date: '2024-02-30' is not a real calendar date (2024-02 has 29 days)", False, False),
    ("balanced", "Invalid integer: 'abc'", True, False),
    ("balanced", None, True, False),
    ("strict", "Invalid date: '2024-02-30' is not a real calendar date (2024-02 has 29 days)", True, False),
    ("maximum", "Invalid integer: 'abc'", True, False),
])
def test_one_policy_answers_for_every_mode(mode, reason, record_batch, held):
    from services.validation_mode_contract import mode_holds_out_refusal

    assert mode_holds_out_refusal(mode, reason, record_batch=record_batch) is held


def test_beyond_the_share_the_column_blocks_even_past_the_listed_examples():
    # 6 impossible days of 25 = 24%: more than the five listed failures, and
    # above one in five — blocked, not silently held out.
    bad = [{"d": f"2024-02-{30 + (i % 2)}"} for i in range(6)]
    col = _probe([*GOOD, *bad])
    assert col["failed"] == 6 and len(col["sample_failures"]) == 5
    assert (col["severity"], col["holdout_kind"]) == ("block", "")


def test_a_calendar_looking_cell_refused_for_another_reason_still_blocks(monkeypatch):
    real = cp.apply_transform

    def other_rule(raw, transform):
        if str(raw) == "2024-02-30":
            return None, "Invalid boolean: '2024-02-30' is not a recognised true/false token"
        return real(raw, transform)

    monkeypatch.setattr(cp, "apply_transform", other_rule)
    col = _probe([*GOOD, {"d": "2024-02-30"}])
    assert (col["severity"], col["holdout_kind"]) == ("block", "")


def test_gates_no_longer_decide_on_the_value():
    from pathlib import Path

    gates = (Path(__file__).resolve().parents[3] / "packages/preflight/src/preflight/gates.py").read_text()
    assert "invalid_calendar_reason" not in gates
    assert 'get("holdout_kind") == "invalid_calendar"' in gates
    # Cell holdouts ask the mode policy; the only literal "lenient" left is the
    # object-store byte copy (no cells are graded there).
    assert gates.count('mode == "lenient"') == 1, gates.count('mode == "lenient"')
    probe = (Path(__file__).resolve().parents[1] / "services/coercion_probe.py").read_text()
    assert "mode_holds_out_refusal(mode, err" in probe
