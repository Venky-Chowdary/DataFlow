"""QA browser 3: an ISO-shaped value naming no real calendar day votes for its temporal family.

``2024-02-30`` inside the plan sample used to classify as text, demoting the
whole column to VARCHAR (then refused as VARCHAR -> DATE). It now votes DATE
(``... 25:61:00`` votes TIMESTAMP); the cell is still refused at write by the
same predicate (``connectors.sql_temporal.invalid_calendar_reason``). Nothing
else may change classification: every token that predicate does not flag
classifies exactly as before.
"""

from __future__ import annotations

import pytest

from services.schema_inference import _invalid_calendar_family, classify_value, infer_column

#: Tokens across every family the classifier knows, none an invalid calendar date.
UNCHANGED = [
    "", "  ", "null", "abc", "hello world", "2024-02-28", "2024-02-29", "2023-02-29x",
    "2024-01-01 10:00:00", "2024-01-01T00:00:00", "2024-01-01T10:00:00Z", "10:30:00",
    "25:61", "31/02/2025", "01/02/2024", "20240230", "20240101", "42", "0", "007",
    "3.14", "1e5", "-17", "$1,000.00", "45%", "true", "f", "yes", "550e8400-e29b-41d4-a716-446655440000",
    "{\"a\": 1}", "[1,2]", "POINT(1 2)", "P1D", "a@b.co", "+1 555 010 0001", "2024-13", "2024-1-1",
    "x" * 300,
]


@pytest.mark.parametrize("token", UNCHANGED)
def test_tokens_the_rule_does_not_flag_classify_as_before(token):
    assert _invalid_calendar_family(token) == ""


@pytest.mark.parametrize("token,family", [
    ("2024-02-30", "DATE"), ("2025-02-31", "DATE"), ("2024-13-01", "DATE"), ("0000-00-00", "DATE"),
    ("2024-02-30 10:00:00", "TIMESTAMP"), ("2024-02-10 25:61:00", "TIMESTAMP"),
])
def test_invalid_calendar_values_vote_their_temporal_family(token, family):
    assert classify_value(token) == family
    assert classify_value(token, field_name="d") == family


def test_one_bad_day_in_the_sample_keeps_the_column_a_date():
    samples = ["2024-01-01", "2024-02-30", "2024-03-01", "", "2024-04-01"]
    assert infer_column(samples, field_name="d")["logical_type"] == "DATE"
    # A word is still a word: the column stays text.
    assert infer_column(["2024-01-01", "next tuesday"], field_name="d")["logical_type"] != "DATE"
