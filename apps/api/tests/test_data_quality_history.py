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


@pytest.fixture(autouse=True)
def _isolate_quality_profile_store(tmp_path, monkeypatch):
    """Do not read or write ``apps/api/data/quality_profiles`` from this module.

    Host leftovers (1M bench, prior pytest) inflate ``rows_written_total`` and
    invent measured success for the wrong run. Each test gets its own data dir.
    """
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))


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


def test_history_identity_distinguishes_host_from_table_only() -> None:
    """Validate must use the same host/port key Execute persist writes.

    Table-only lookup used to miss a 1M load and invent unmeasured.
    """
    from services.data_quality_history import history_endpoint_from_config
    from services.historical_success_contract import measure_route_historical_success

    full_src = history_endpoint_from_config(
        {"host": "127.0.0.1", "port": 5432, "database": "dataflow", "schema": "public"},
        kind="database",
        format="postgresql",
        table="bench_hist_src",
    )
    full_dst = history_endpoint_from_config(
        {"host": "127.0.0.1", "port": 3306, "database": "dataflow"},
        kind="database",
        format="mysql",
        table="bench_hist_dst",
    )
    save_profile(
        full_src,
        full_dst,
        profile_batch([], {"employee_id": "VARCHAR(32)"}),
        job_id="hist-identity",
        rejected_rows=0,
        row_count=1000,
    )
    measured = measure_route_historical_success(full_src, full_dst)
    assert measured["measured"] is True
    assert measured["success_rate"] == 1.0
    assert measured["rows_written_total"] == 1000
    assert measured["runs_observed"] == 1

    table_only_src = {"kind": "database", "format": "postgresql", "table": "bench_hist_src"}
    table_only_dst = {"kind": "database", "format": "mysql", "table": "bench_hist_dst"}
    missed = measure_route_historical_success(table_only_src, table_only_dst)
    assert missed["measured"] is False
    assert missed["success_rate"] is None


def test_save_profile_same_job_id_does_not_double_count() -> None:
    """Execute retry / re-persist of the same job must not invent a second load."""
    from services.data_quality_history import history_endpoint_from_config
    from services.historical_success_contract import measure_route_historical_success

    src = history_endpoint_from_config(
        {"host": "127.0.0.1", "port": 5432, "database": "dataflow"},
        kind="database",
        format="postgresql",
        table="hist_idempotent_src",
    )
    dst = history_endpoint_from_config(
        {"host": "127.0.0.1", "port": 3306, "database": "dataflow"},
        kind="database",
        format="mysql",
        table="hist_idempotent_dst",
    )
    cols = profile_batch([], {"id": "INTEGER"})
    save_profile(src, dst, cols, job_id="job-retry", row_count=1000)
    save_profile(src, dst, cols, job_id="job-retry", row_count=1000)
    first = measure_route_historical_success(src, dst)
    assert first["runs_observed"] == 1
    assert first["rows_written_total"] == 1000

    save_profile(src, dst, cols, job_id="job-next", row_count=500)
    second = measure_route_historical_success(src, dst)
    assert second["runs_observed"] == 2
    assert second["rows_written_total"] == 1500


def test_isolated_data_dir_does_not_see_other_store(tmp_path, monkeypatch) -> None:
    """A leftover host (or other-worker) profile must not inflate this measure."""
    from services.data_quality_history import history_endpoint_from_config
    from services.historical_success_contract import measure_route_historical_success

    src = history_endpoint_from_config(
        {"host": "10.0.0.8", "port": 5432, "database": "dataflow"},
        kind="database",
        format="postgresql",
        table="hist_isolate_src",
    )
    dst = history_endpoint_from_config(
        {"host": "10.0.0.8", "port": 3306, "database": "dataflow"},
        kind="database",
        format="mysql",
        table="hist_isolate_dst",
    )
    leftover = tmp_path / "leftover"
    isolated = tmp_path / "isolated"
    leftover.mkdir()
    isolated.mkdir()
    cols = profile_batch([], {"id": "INTEGER"})

    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(leftover))
    save_profile(src, dst, cols, job_id="other-worker", row_count=9999)
    assert measure_route_historical_success(src, dst)["rows_written_total"] == 9999

    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(isolated))
    unseen = measure_route_historical_success(src, dst)
    assert unseen["measured"] is False
    assert unseen["success_rate"] is None
    save_profile(src, dst, cols, job_id="this-worker", row_count=1000)
    seen = measure_route_historical_success(src, dst)
    assert seen["rows_written_total"] == 1000
    assert seen["runs_observed"] == 1


def test_quality_profiles_dir_override_does_not_rewrite_data_dir(tmp_path, monkeypatch) -> None:
    """Workers isolate load history without moving connectors/tenants."""
    from services.data_quality_history import (
        history_endpoint_from_config,
        quality_profiles_dir,
    )
    from services.historical_success_contract import measure_route_historical_success
    from services.platform_config import data_dir

    data = tmp_path / "shared-data"
    override = tmp_path / "profiles_gw0"
    data.mkdir()
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(data))
    monkeypatch.setenv("DATAFLOW_QUALITY_PROFILES_DIR", str(override))

    src = history_endpoint_from_config(
        {"host": "10.0.0.9", "port": 5432, "database": "dataflow"},
        kind="database",
        format="postgresql",
        table="hist_override_src",
    )
    dst = history_endpoint_from_config(
        {"host": "10.0.0.9", "port": 3306, "database": "dataflow"},
        kind="database",
        format="mysql",
        table="hist_override_dst",
    )
    cols = profile_batch([], {"id": "INTEGER"})
    save_profile(src, dst, cols, job_id="override-1", row_count=42)

    assert quality_profiles_dir() == override
    assert data_dir() == data
    assert list(override.glob("*.json"))
    default = data / "quality_profiles"
    assert not default.exists() or not list(default.glob("*.json"))
    measured = measure_route_historical_success(src, dst)
    assert measured["rows_written_total"] == 42
    assert measured["runs_observed"] == 1


def test_quarantine_histogram_stable_keys() -> None:
    h = quarantine_histogram(
        [
            {"column": "a", "reason": "Incorrect datetime value: 'x' for column 'a'"},
            {"column": "a", "reason": "Incorrect datetime value: 'y' for column 'a'"},
        ]
    )
    assert len(h) == 1
    assert next(iter(h.values())) == 2
