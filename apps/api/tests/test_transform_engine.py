"""Transform engine unit tests."""

import pytest

from services.transform_engine import apply_transform, dry_run_sample, infer_transform, preview_quarantine_cells


def test_infer_decimal_for_amount():
    assert infer_transform("AMT", "payment_amount", "VARCHAR") == "decimal"


def test_apply_decimal():
    val, err = apply_transform("1,234.50", "decimal")
    assert err is None
    assert val == "1234.50"


def test_apply_date():
    val, err = apply_transform("2024-01-15", "date")
    assert err is None
    assert val == "2024-01-15"


def test_apply_date_resolves_unambiguous_day_month_order():
    # 31 cannot be a month, so it must be day-first.
    val, err = apply_transform("31/12/2024", "date")
    assert err is None
    assert val == "2024-12-31"
    # 31 cannot be a month, so month-first is the only valid reading.
    val2, err2 = apply_transform("12/31/2024", "date")
    assert err2 is None
    assert val2 == "2024-12-31"


def test_apply_date_fails_closed_on_ambiguous_mdy_dmy():
    """05/06/2024 is May 6 (US) or June 5 (EU) — never silently pick MDY."""
    val, err = apply_transform("05/06/2024", "date")
    assert val is None
    assert err and "Invalid date" in err
    val2, err2 = apply_transform("05/06/2024", "datetime")
    assert val2 is None
    assert err2
    # Equal day/month is unambiguous either locale.
    val3, err3 = apply_transform("05/05/2024", "date")
    assert err3 is None
    assert val3 == "2024-05-05"

def test_apply_json_and_boolean():
    val, err = apply_transform('{"b": 2, "a": 1}', "json")
    assert err is None
    assert val == '{"b":2,"a":1}'

    bool_val, bool_err = apply_transform("yes", "boolean")
    assert bool_err is None
    assert bool_val is True


def test_apply_integer_rejects_fractional_value():
    val, err = apply_transform("10.5", "integer")
    assert val is None
    assert "Invalid integer" in err


def test_preview_quarantine_cells_flags_bad_decimal():
    result = preview_quarantine_cells(
        headers=["amt"],
        sample_rows=[["x"], ["1.25"]],
        mappings=[{"source": "amt", "target": "amount", "transform": "decimal"}],
        column_types={"amt": "VARCHAR"},
    )
    assert result["quarantine_count"] == 1
    assert result["cells"][0]["status"] == "quarantine"
    assert result["ok_count"] >= 1


def test_dry_run_catches_bad_decimal():
    ok, errors = dry_run_sample(
        headers=["AMT"],
        sample_rows=[["not-a-number"]],
        mappings=[{"source": "AMT", "target": "payment_amount", "transform": "decimal"}],
        column_types={"AMT": "DECIMAL"},
    )
    assert not ok
    assert errors


def test_apply_hash_pii_is_deterministic():
    val, err = apply_transform("secret@email.com", "hash_pii")
    assert err is None
    assert val
    assert len(val) == 32
    again, _ = apply_transform("secret@email.com", "hash_pii")
    assert again == val


def test_apply_uuid_validates():
    val, err = apply_transform("550e8400-e29b-41d4-a716-446655440000", "uuid")
    assert err is None
    assert val == "550e8400-e29b-41d4-a716-446655440000"

    bad, err2 = apply_transform("not-a-uuid", "uuid")
    assert bad is None
    assert "Invalid UUID" in err2


def test_apply_decimal_accounting_and_scientific():
    val, err = apply_transform("(1,234.56)", "decimal")
    assert err is None
    assert val == "-1234.56"

    val2, err2 = apply_transform("1.5e3", "decimal")
    assert err2 is None
    assert val2 == "1500"

    val3, err3 = apply_transform("$10,000.00", "decimal")
    assert err3 is None
    assert val3 == "10000.00"


def test_unknown_transform_fails_closed():
    val, err = apply_transform("hello", "bogus_transform")
    assert val is None
    assert "Unknown transform" in err


@pytest.mark.parametrize(
    "text,expected",
    [
        ("12/31/2024", "2024-12-31"),
        ("12-31-2024", "2024-12-31"),
        ("12.31.2024", "2024-12-31"),
        ("12/31/24", "2024-12-31"),
        ("12-31-24", "2024-12-31"),
        ("31/12/2024", "2024-12-31"),
        ("31-12-2024", "2024-12-31"),
        ("31.12.2024", "2024-12-31"),
        ("31/12/24", "2024-12-31"),
        ("31-12-24", "2024-12-31"),
        ("2024/12/31", "2024-12-31"),
        ("2024-12-31", "2024-12-31"),
        ("2024.12.31", "2024-12-31"),
    ],
)
def test_apply_date_handles_various_separators_and_two_digit_years(text, expected):
    val, err = apply_transform(text, "date")
    assert err is None, f"{text!r} failed: {err}"
    assert val == expected, f"{text!r} -> {val!r}, expected {expected!r}"


def test_apply_datetime_with_mixed_format_and_two_digit_year():
    val, err = apply_transform("12-31-24 14:30:00", "datetime")
    assert err is None
    assert val == "2024-12-31T14:30:00Z"


def test_apply_date_reconciles_same_value_across_formats():
    """Different source string formats for the same day must normalize identically."""
    values = []
    for text in ["12/31/2024", "12-31-2024", "31-12-2024", "2024-12-31", "2024/12/31", "12.31.2024", "31.12.2024", "12-31-24", "31-12-24"]:
        val, err = apply_transform(text, "date")
        assert err is None
        values.append(val)
    assert len(set(values)) == 1
    assert values[0] == "2024-12-31"

