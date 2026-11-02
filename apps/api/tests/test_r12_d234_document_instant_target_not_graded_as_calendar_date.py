"""D234: a BSON/ES ``date`` target is a UTC instant, not a calendar DATE.

QA saw ``ts TIMESTAMPTZ -> Mongo DATE`` lossy beside the job keeping the time of
day: the grader read the document store's single temporal carrier as a
calendar day, so the coercion issue named the wrong logical type and risk.
"""
from __future__ import annotations

import pytest

from services.type_coercion_validator import validate_mapping_coercions


@pytest.mark.parametrize("engine", ["mongodb", "elasticsearch"])
def test_document_date_target_is_graded_as_an_instant(engine):
    issues = validate_mapping_coercions(
        [{"source": "ts", "target": "ts", "target_type": "date", "create_new": True}],
        source_types={"ts": "TIMESTAMPTZ(6)"},
        target_types={"ts": "date"},
        dest_db_type=engine,
        dest_table_exists=False,
    )
    ts = [i for i in issues if i["source"] == "ts"]
    assert ts, "microseconds into a millisecond carrier must still be graded"
    assert ts[0]["target_logical"] == "datetime"
    assert "UTC instant, 3 fractional digits" in ts[0]["message"]
    assert "calendar" not in str(ts[0].get("suggested_fix") or "")


def test_relational_date_target_stays_a_calendar_date():
    issues = validate_mapping_coercions(
        [{"source": "ts", "target": "ts", "target_type": "DATE", "create_new": True}],
        source_types={"ts": "TIMESTAMPTZ(6)"},
        target_types={"ts": "DATE"},
        dest_db_type="postgresql",
        dest_table_exists=False,
    )
    assert [i["target_logical"] for i in issues if i["source"] == "ts"] == ["date"]
