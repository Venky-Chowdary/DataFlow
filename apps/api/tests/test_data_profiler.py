"""Data profiler statistical inference tests."""

from services.data_profiler import merge_profiler_schema, profile_column, profile_dataset


def test_profile_column_detects_integer():
    prof = profile_column("id", ["1", "2", "3", "4"])
    assert prof["inferred_type"] == "INTEGER"
    assert prof["confidence"] >= 0.5


def test_profile_column_detects_email_as_varchar():
    prof = profile_column("email", ["a@b.com", "c@d.org"])
    assert prof["inferred_type"] == "VARCHAR"


def test_profile_dataset_builds_schema():
    rows = [
        {"id": "1", "amount": "10.5"},
        {"id": "2", "amount": "20.0"},
    ]
    result = profile_dataset(["id", "amount"], rows)
    assert result["schema"]["id"] == "INTEGER"
    assert "amount" in result["schema"]
    assert result["quality_score"] > 0


def test_merge_profiler_schema_overrides_naive():
    merged = merge_profiler_schema({"id": "VARCHAR"}, {"id": "INTEGER"})
    assert merged["id"] == "INTEGER"
