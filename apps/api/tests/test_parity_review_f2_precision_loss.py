"""Review finding 2: float_precision must not explain real digit loss on a non-float column.

``float(a) == float(b)`` is true for two different integers above 2**53 and for
a NUMERIC whose cents were cut, so "the same binary64" said nothing about a
BIGINT or a NUMERIC(30,2) destination: the rows lost data and the report called
it explained. Only a float-family column on either side makes binary rounding
an expected representation.
"""

from __future__ import annotations

import pytest

from services.parity_classifier import CellContext, classify_value
from services.parity_report import build_parity_report


@pytest.mark.parametrize(
    "source, dest, dest_type, code",
    [
        ("9007199254740993", "9007199254740992", "bigint", "float_precision"),
        ("12345678901234567.12", "12345678901234567", "numeric(30,2)", "numeric_scale"),
        ("12345678901234567.12", "12345678901234567", "", "numeric_scale"),
        ("9007199254740993", "9007199254740992", "", "float_precision"),
    ],
)
def test_digit_loss_on_a_non_float_column_is_unexplained(source, dest, dest_type, code):
    found = classify_value(expected=source, dest=dest, ctx=CellContext(dest_type=dest_type))
    assert found is not None
    assert found["code"] == code
    assert found["explained"] is False, found


@pytest.mark.parametrize(
    "source, dest, ctx",
    [
        # The same binary64 written with more digits: a float source column (re-review 14:
        # a float destination alone is no excuse, see test_parity_review2_f14_*).
        ("0.1000000000000000055511151231257827", "0.1",
         CellContext(source_type="double precision", dest_type="double precision")),
        ("0.1000000000000000055511151231257827", "0.1", CellContext(source_type="float8", dest_type="numeric")),
    ],
)
def test_binary_rounding_on_a_float_column_stays_explained(source, dest, ctx):
    found = classify_value(expected=source, dest=dest, ctx=ctx)
    assert found is not None and found["code"] == "float_precision"
    assert found["explained"] is True, found


def test_the_reviewers_bigint_job_is_unexplained():
    job = {
        "_id": "job-1", "status": "completed",
        "reconciliation": {
            "passed": False, "source_rows": 10, "target_rows": 10,
            "verification_ladder": {
                "layers": {"L5": {"details": {"pk_column": "id", "mismatch_count": 1}}},
                "localization": {"rows": [{"pk": "7", "column": "big", "source_value": "9007199254740993",
                                           "target_value": "9007199254740992"}]},
            },
        },
        "destination_summary": {"table": "orders", "primary_key_columns": ["id"],
                                "sync_mode": "full_refresh_overwrite"},
        "transfer_request": {"mappings": [{"source": "big", "target": "big", "target_type": "bigint"},
                                          {"source": "id", "target": "id"}]},
    }
    report = build_parity_report(job, reread=False)
    assert report["verdict"] == "unexplained", report["verdict_reason"]
    assert report["parity"]["unexplained"] == 1
    assert report["signoff"]["status"] == "blocked"
