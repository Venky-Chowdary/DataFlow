"""Tests for the history-aware data quality profiler (last-N multi-load)."""

import pytest

from services.data_quality_history import (
    ColumnProfile,
    compare_route_to_history,
    detect_anomalies,
    load_historical_profile,
    load_run_history,
    profile_batch,
    profile_column,
    quarantine_histogram,
    save_profile,
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


def test_ring_buffer_keeps_multiple_loads(source_dest, tmp_path, monkeypatch) -> None:
    source, dest = source_dest
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))

    for i in range(5):
        rows = [{"id": j, "amount": 100.0 + i} for j in range(10)]
        save_profile(
            source,
            dest,
            profile_batch(rows, {"id": "integer", "amount": "float"}),
            job_id=f"job-{i}",
            rejected_details=(
                [{"row": 0, "column": "ts", "value": "bad", "reason": "Incorrect datetime"}]
                if i == 4
                else []
            ),
            rejected_rows=1 if i == 4 else 0,
            row_count=10,
        )

    runs = load_run_history(source, dest)
    assert len(runs) == 5
    assert runs[-1]["job_id"] == "job-4"
    assert runs[-1]["quarantine_histogram"]

    # Latest load introduces a quarantine pattern absent from prior loads.
    report = compare_route_to_history(
        [{"id": 1, "amount": 104.0}],
        source,
        dest,
        schema={"id": "integer", "amount": "float"},
        rejected_details=[
            {"row": 0, "column": "ts", "value": "bad", "reason": "Incorrect datetime value"}
        ],
    )
    # History already includes job-4; comparing a new sample with same pattern may
    # show spike or new depending on whether prior hist keys match. Either way
    # prior_load_count must be > 0.
    assert report["prior_load_count"] == 5


def test_novel_quarantine_pattern_detected(source_dest, tmp_path, monkeypatch) -> None:
    source, dest = source_dest
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))

    clean = [{"id": 1, "amount": 10.0}]
    for i in range(3):
        save_profile(
            source,
            dest,
            profile_batch(clean, {"id": "integer", "amount": "float"}),
            job_id=f"clean-{i}",
            row_count=1,
        )

    report = compare_route_to_history(
        clean,
        source,
        dest,
        schema={"id": "integer", "amount": "float"},
        rejected_details=[
            {
                "row": 0,
                "column": "column_5",
                "value": "2024-08-09T01:58:42Z",
                "reason": "Incorrect datetime value",
            }
        ],
    )
    assert report["novel_quarantine_patterns"]
    assert report["novel_quarantine_patterns"][0]["column"] == "column_5"


def test_quarantine_histogram_stable_keys() -> None:
    h = quarantine_histogram(
        [
            {"column": "a", "reason": "Incorrect datetime value: 'x' for column 'a'"},
            {"column": "a", "reason": "Incorrect datetime value: 'y' for column 'a'"},
        ]
    )
    assert len(h) == 1
    assert next(iter(h.values())) == 2
