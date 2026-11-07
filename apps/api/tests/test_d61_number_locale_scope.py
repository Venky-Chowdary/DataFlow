"""D61 — number_locale_assumed warning fired on timestamp/JSON/typed columns.

Repro (pf_3ec3fb8084d3, pf_34f4d7e1b20c, M16 meta column): S3 CSV/Parquet
staging raised a false "set number locale US or EU" assumption on
`2024-01-15T10:30:00.000`-style values — the grouped-number pre-filter only
checked "has a digit + a , or .". The warning's contract is numeric-text
columns in delimited files: typed cells, temporal text, and JSON documents
can never be grouping-ambiguous.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal


def test_timestamp_text_not_flagged():
    from services.transform_engine import ambiguous_number_columns

    rows = [
        {"ts": "2024-01-15T10:30:00.000"},
        {"ts": "2024-02-29 23:59:59.999999"},
        {"ts": "2024-01-15"},
    ]
    assert ambiguous_number_columns(rows) == []


def test_typed_cells_not_flagged():
    """Parquet/typed staging carries datetime/Decimal objects — a typed cell
    can never be grouping-ambiguous (its value is already bound)."""
    from services.transform_engine import ambiguous_number_columns

    rows = [
        {"ts": datetime(2024, 1, 15, 10, 30, 0, 123456)},
        {"amt": Decimal("1234.56")},
        {"n": 10500},
    ]
    assert ambiguous_number_columns(rows) == []


def test_json_document_not_flagged():
    """The Kafka/S3 meta column family — {'a': 1.5} is not a number."""
    from services.transform_engine import ambiguous_number_columns

    rows = [{"meta": '{"a": 1.5, "b": [1, 2]}'}, {"meta": '["x", 2.5]'}]
    assert ambiguous_number_columns(rows) == []


def test_declared_non_numeric_column_not_flagged():
    from services.transform_engine import ambiguous_number_columns

    rows = [{"ts": "15.01.2024 10:30"}]
    assert ambiguous_number_columns(
        rows, column_types={"ts": "TIMESTAMPTZ(6)"}
    ) == []
    assert ambiguous_number_columns(
        rows, column_types={"ts": "BOOLEAN"}
    ) == []
    # A declared DECIMAL column stays in scope — that's the delimited-file
    # contract the warning exists for.
    assert ambiguous_number_columns(
        [{"amt": "10.129"}], column_types={"amt": "DECIMAL(15,3)"}
    )


def test_genuine_ambiguity_still_flagged():
    from services.transform_engine import ambiguous_number_columns

    findings = ambiguous_number_columns([{"amt": "10.129"}])
    assert [f["column"] for f in findings] == ["amt"]
    assert "10.129" in findings[0]["samples"]


def test_day_first_dotted_timestamp_not_flagged():
    """01.02.2024 10:30 is not an ISO form, so it used to fall through to the
    grouped-number pre-filter and raise the US/EU locale warning on a
    timestamp column (D61)."""
    from services.transform_engine import ambiguous_number_columns

    rows = [{"ts": "01.02.2024 10:30"}, {"ts": "15.01.2024 23:59:59"}]
    assert ambiguous_number_columns(rows) == []
