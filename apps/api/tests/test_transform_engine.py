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


def test_unknown_transform_fails_closed():
    val, err = apply_transform("hello", "bogus_transform")
    assert val is None
    assert "Unknown transform" in err

