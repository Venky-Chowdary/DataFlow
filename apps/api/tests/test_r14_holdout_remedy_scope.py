"""QA review #3: explain offers "hold out these rows" only where that is the remedy.

``_row_value_holdout_actions`` (QA browser 3) offered to hold out every G3f
finding: a width overflow ("Hold out 900 rows of 'name' … the column's type is
right" — the fix is to widen), a finding already held out under a contract,
and an all-bad column. It also dropped every other quarantine action. Now:
transform failures only (``population_fit_scan.is_transform_failure_finding``),
still blocking, fewer unfit rows than scanned, merged with the other actions.
"""

from __future__ import annotations

from services.validation_assistant import explain_validation


def _finding(source, *, transform, reason, unfit, aborts=True):
    return {
        "source": source, "target": source, "target_type": "DATE" if transform else "VARCHAR(10)",
        "transform": transform, "unfit_rows": unfit, "example_rows": [642], "example_values": ["31/02/2025"],
        "unfit_reason": reason, "aborts_job": aborts,
    }


def _preflight(findings, scanned=1000, extra_blockers=(), record_batch=True):
    return {
        "passed": False,
        "gates": [{"id": "g3f_population_fit", "status": "block", "message": "values fail", "details": {}}],
        "blockers": [
            {"id": "g3f_population_fit", "message": "1 value(s) fail the declared transform",
             "details": {"rows_scanned": scanned, "findings": findings}},
            *extra_blockers,
        ],
        "population_fit": {"rows_scanned": scanned, "findings": findings},
        "source_record_batch": record_batch,
    }


def _holds(result):
    return [a for a in result["suggested_actions"] if a.get("kind") == "quarantine_and_rerun"]


BAD_DATE = dict(transform="date_iso", reason="date_iso: Invalid date: '31/02/2025' is not a real calendar date")


def test_a_bad_value_in_some_rows_is_offered_hold_out():
    holds = _holds(explain_validation(_preflight([_finding("order_date", unfit=1, **BAD_DATE)]), use_llm=False))
    assert [h["column"] for h in holds] == ["order_date"]
    assert holds[0]["rows"] == [642] and holds[0]["row_count"] == 1 and holds[0]["rows_scanned"] == 1000


def test_a_width_overflow_is_not_offered_hold_out():
    overflow = _finding("name", transform="", reason="value is 412 characters; VARCHAR(10) holds 10", unfit=900)
    result = explain_validation(_preflight([overflow]), use_llm=False)
    assert not [h for h in _holds(result) if h.get("column") == "name"], result["suggested_actions"]


def test_an_all_bad_column_is_not_offered_hold_out():
    result = explain_validation(_preflight([_finding("order_date", unfit=1000, **BAD_DATE)]), use_llm=False)
    assert not [h for h in _holds(result) if h.get("column") == "order_date"], result["suggested_actions"]


def test_a_finding_already_held_out_is_not_offered_again():
    held = _finding("order_date", unfit=3, aborts=False, **BAD_DATE)
    result = explain_validation(_preflight([held]), use_llm=False)
    assert not [h for h in _holds(result) if h.get("column") == "order_date"]


def test_example_rows_are_labelled_and_the_count_is_the_findings():
    from services.quarantine_from_preflight import preflight_quarantine_row_count, quarantine_rows_from_preflight

    finding = _finding("order_date", unfit=5000, **BAD_DATE)
    finding["example_rows"] = [642, 700, 701]
    finding["example_values"] = ["31/02/2025", "30/02/2025", "31/04/2025"]
    pf = _preflight([finding])
    pf["gates"][0]["details"] = {"findings": [finding]}
    rows = quarantine_rows_from_preflight(pf)
    shown = [r for r in rows if r.get("row") in (642, 700, 701)]
    assert len(shown) == 3
    assert "example 1 of 5000" in shown[0]["reason"]
    assert preflight_quarantine_row_count(rows) == 5000


def test_the_remedy_is_balanced_validation_not_a_second_mechanism():
    holds = _holds(explain_validation(_preflight([_finding("order_date", unfit=1, **BAD_DATE)]), use_llm=False))
    assert holds[0]["validation_mode"] == "balanced"
    assert "risk_acceptance" not in holds[0]
    assert "switches the whole run to Balanced validation" in holds[0]["label"]
    assert holds[0]["balanced_holds_out"] is True


# ---------------------------------------------- review B4 / S5 / nits

BAD_INT = dict(transform="cast_integer", reason="cast_integer: Invalid integer: 'abc'")


def test_a_bad_number_in_a_file_is_offered_balanced():
    """Balanced holds a numeric cast refusal out on a file (R4-D02, the one policy)."""
    holds = _holds(explain_validation(_preflight([_finding("qty", unfit=1, **BAD_INT)]), use_llm=False))
    assert holds and holds[0]["validation_mode"] == "balanced", holds
    assert holds[0]["balanced_holds_out"] is True


def test_a_bad_number_on_a_database_source_is_offered_lenient_not_balanced():
    pf = _preflight([_finding("qty", unfit=1, **BAD_INT)], record_batch=False)
    holds = _holds(explain_validation(pf, use_llm=False))
    assert holds and holds[0]["validation_mode"] == "lenient", holds
    assert holds[0]["balanced_holds_out"] is False
    assert "switches the whole run to Lenient validation" in holds[0]["label"]


def test_an_impossible_date_on_a_database_source_is_not_a_balanced_holdout():
    pf = _preflight([_finding("order_date", unfit=1, **BAD_DATE)], record_batch=False)
    holds = _holds(explain_validation(pf, use_llm=False))
    assert holds and holds[0]["validation_mode"] == "lenient", holds


def test_an_unknown_scan_is_never_offered():
    pf = _preflight([_finding("order_date", unfit=1, **BAD_DATE)], scanned=0)
    pf["population_fit"]["rows_scanned"] = 0
    assert not _holds(explain_validation(pf, use_llm=False))


def test_not_offered_beside_a_blocker_the_switch_would_not_clear():
    other = {"id": "g15_dest_exists_shape", "message": "destination column type differs", "details": {}}
    pf = _preflight([_finding("order_date", unfit=1, **BAD_DATE)], extra_blockers=[other])
    assert not [h for h in _holds(explain_validation(pf, use_llm=False)) if h.get("column") == "order_date"]


def test_the_csv_report_says_which_mode_holds_every_issue_out():
    from services.csv_validator import validate_csv_content

    dates = b"id,d\n1,2024-01-01\n2,2024-02-30\n3,2024-01-03\n"
    assert validate_csv_content(dates, ["id", "d"], {"id": "INTEGER", "d": "DATE"})["holdout_modes"] == {
        "balanced": True, "lenient": True,
    }
    nums = b"id,q\n1,5\n2,abc\n"
    assert validate_csv_content(nums, ["id", "q"], {"id": "INTEGER", "q": "INTEGER"})["holdout_modes"] == {
        "balanced": True, "lenient": True,
    }
    flags = b"id,b\n1,true\n2,maybe\n"  # not a numeric or calendar refusal
    assert validate_csv_content(flags, ["id", "b"], {"id": "INTEGER", "b": "BOOLEAN"})["holdout_modes"] == {
        "balanced": False, "lenient": True,
    }
