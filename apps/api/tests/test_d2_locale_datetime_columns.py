"""D2: the numeric-locale check must not run on declared non-numeric columns.

orders.jsonl ``created_at`` is declared a timestamp; its rendered instants
("1,234.56"-shaped groupings in the heuristic's eyes) must not raise a
``number_locale_assumed`` warning. A genuinely ambiguous numeric column in the
same sample must still be flagged.
"""

from services.transform_engine import ambiguous_number_columns


def test_datetime_column_not_flagged_for_grouped_samples():
    rows = [
        {"created_at": "2024-01-01T00:00:00.000", "amt": "1,234.56"},
        {"created_at": "2024-01-02T00:00:00.000", "amt": "2,345.67"},
    ]
    columns = ["created_at", "amt"]
    column_types = {"created_at": "TIMESTAMP", "amt": "DECIMAL(10,2)"}
    findings = ambiguous_number_columns(
        rows, columns, column_types=column_types
    )
    assert all(f["column"] != "created_at" for f in findings), (
        f"declared TIMESTAMP column flagged for number locale: {findings}"
    )


def test_date_and_time_declarations_also_skipped():
    rows = [{"d": "2024,01,01", "t": "01,23,45"}]
    column_types = {"d": "DATE", "t": "TIME"}
    assert ambiguous_number_columns(
        rows, ["d", "t"], column_types=column_types
    ) == []


def test_ambiguous_numeric_column_still_flagged():
    rows = [{"amt": "10.129"}, {"amt": "1,234"}]
    findings = ambiguous_number_columns(
        rows, ["amt"], column_types={"amt": "DECIMAL(10,3)"}
    )
    # DECIMAL declares a number — the locale check applies and must flag the
    # ambiguous grouping samples (unchanged behavior).
    assert findings and findings[0]["column"] == "amt"


def test_undeclared_column_still_flagged():
    rows = [{"x": "1,234"}, {"x": "5,678"}]
    findings = ambiguous_number_columns(rows, ["x"], column_types={})
    assert findings and findings[0]["column"] == "x"
