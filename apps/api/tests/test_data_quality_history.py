"""Tests for the history-aware data quality profiler."""


import pytest

from services.data_quality_history import (
    ColumnProfile,
    detect_anomalies,
    load_historical_profile,
    profile_batch,
    profile_column,
    validate_batch_against_history,
)


@pytest.fixture
def source_dest():
    return (
        {"kind": "database", "format": "postgresql", "table": "orders"},
        {"kind": "database", "format": "snowflake", "table": "orders"},
    )


def test_profile_column_basic() -> None:
    p = profile_column([1, 2, 3, None, 4], "id", "integer")
    assert p.count == 5
    assert p.null_count == 1
    assert p.mean == 2.5
    assert p.min_value == "1"
    assert p.max_value == "4"
    assert p.std is not None


def test_profile_column_string_lengths() -> None:
    p = profile_column(["a", "bb", "ccc", None], "name", "string")
    assert p.min_length == 1
    assert p.max_length == 3
    assert p.avg_length == 2.0


def test_profile_batch() -> None:
    rows = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": None, "name": None},
    ]
    profiles = profile_batch(rows, {"id": "integer", "name": "string"})
    assert "id" in profiles
    assert "name" in profiles
    assert profiles["id"].null_count == 1
    assert profiles["name"].null_count == 1


def test_anomaly_detection_empty_history() -> None:
    current = {"id": ColumnProfile(column="id", count=10, null_count=0)}
    assert detect_anomalies(current, None) == []


def test_anomaly_null_rate_shift() -> None:
    historical = {"email": ColumnProfile(column="email", count=100, null_count=0)}
    current = {"email": ColumnProfile(column="email", count=100, null_count=15)}
    issues = detect_anomalies({"email": current["email"]}, historical)
    assert any("null-rate" in issue for issue in issues)


def test_anomaly_mean_drift() -> None:
    historical = {
        "amount": ColumnProfile(
            column="amount",
            count=100,
            null_count=0,
            dtype="float",
            mean=100.0,
            std=10.0,
            min_value="90",
            max_value="110",
        )
    }
    current = {
        "amount": ColumnProfile(
            column="amount",
            count=100,
            null_count=0,
            dtype="float",
            mean=140.0,
            std=10.0,
            min_value="90",
            max_value="110",
        )
    }
    issues = detect_anomalies(current, historical)
    assert any("standard deviations" in issue for issue in issues)


def test_validate_and_save(source_dest, tmp_path, monkeypatch) -> None:
    source, dest = source_dest
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))

    rows = [{"id": 1, "amount": 100.0}, {"id": 2, "amount": 200.0}]
    passed, issues, profile = validate_batch_against_history(
        rows, source, dest, schema={"id": "integer", "amount": "float"}, save_baseline=True
    )
    assert passed is True
    assert issues == []
    assert "id" in profile

    # Reload and check anomaly detection
    historical = load_historical_profile(source, dest)
    assert historical is not None
    assert historical["amount"].mean == 150.0
