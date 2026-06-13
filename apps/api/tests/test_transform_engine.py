"""Transform engine unit tests."""

from services.transform_engine import apply_transform, dry_run_sample, infer_transform


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


def test_dry_run_catches_bad_decimal():
    ok, errors = dry_run_sample(
        headers=["AMT"],
        sample_rows=[["not-a-number"]],
        mappings=[{"source": "AMT", "target": "payment_amount", "transform": "decimal"}],
        column_types={"AMT": "DECIMAL"},
    )
    assert not ok
    assert errors
