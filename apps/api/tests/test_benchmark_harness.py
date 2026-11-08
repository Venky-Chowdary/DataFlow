"""Verify the cloud scale harness can drive a local SQLite transfer end-to-end."""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from benchmarks.cloud_scale import generate_csv
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


@pytest.mark.parametrize("key_width", [3, 8])
@pytest.mark.parametrize("page_size, pages", [(7, 16), (10, 11)])
def test_indexed_keyset_benchmark_reports_verified_local_evidence(key_width, page_size, pages):
    import statistics
    from benchmarks.cloud_scale import run_indexed_keyset_benchmark

    before = {name: os.environ.get(name) for name in ["DATAFLOW_JOB_STORE", "DATAFLOW_DISABLE_OBJECT_STORE"]}
    report = run_indexed_keyset_benchmark(rows=200, key_width=key_width, page_size=page_size, repetitions=2)

    assert report["success"] is True
    assert report["evidence_level"] == "sqlite_local_query_benchmark"
    assert report["database_engine"] == "sqlite"
    assert report["database_version"]
    assert report["verified_successors"] == 100
    assert report["warmup_runs"] == 1
    assert report["fixture_rows"] == 200
    assert report["timing_scope"]["measured"] == "sqlalchemy_execute_and_fetch"
    assert "correctness_verification" in report["timing_scope"]["excluded"]
    assert report["migration_throughput_evidence"] is False
    assert report["plan_scope"] == "initial_midpoint_bookmark"
    assert "PRIMARY KEY" in report["fixture_ddl"]
    for name, bindings in [("native_tuple", key_width), ("portable_or", key_width * (key_width + 1) // 2)]:
        result = report["algorithms"][name]
        assert result["bind_count"] == bindings
        assert result["plan"]
        assert result["sql"]
        assert len(result["query_seconds_samples"]) == 2
        assert result["verified_rows"] == 100
        assert result["pages"] == pages
        assert result["query_bind_count"] == bindings + 2
        assert result["median_query_seconds"] == statistics.median(result["query_seconds_samples"])
        assert result["uses_index_search"] == any(row["detail"].startswith("SEARCH ") for row in result["plan"])
    assert report["algorithms"]["native_tuple"]["uses_index_search"] is True
    assert before == {name: os.environ.get(name) for name in before}


def test_indexed_keyset_benchmark_odd_fixture_scope_and_warmup_exclusion():
    from benchmarks.cloud_scale import run_indexed_keyset_benchmark

    report = run_indexed_keyset_benchmark(rows=21, key_width=2, page_size=3, repetitions=2)

    assert report["scope"] == {"first_ordinal": 10, "last_ordinal": 20}
    assert report["verified_successors"] == 11
    for result in report["algorithms"].values():
        assert result["pages"] == 5
        assert result["verified_rows"] == 11
        assert len(result["query_seconds_samples"]) == 2


@pytest.mark.parametrize("options", [
    {"rows": 1}, {"rows": 1_000_001}, {"key_width": 1}, {"key_width": 17},
    {"page_size": 0}, {"repetitions": 0}, {"timeout_seconds": 0},
    {"timeout_seconds": float("inf")},
    {"database_url": "mysql://localhost/unused"},
])
def test_indexed_keyset_benchmark_refuses_unsafe_parameters(options):
    from benchmarks.cloud_scale import run_indexed_keyset_benchmark

    with pytest.raises(ValueError):
        run_indexed_keyset_benchmark(**options)


def test_indexed_keyset_benchmark_stops_at_deadline(monkeypatch):
    import benchmarks.cloud_scale as harness

    readings = iter([0.0, 2.0])
    monkeypatch.setattr(harness.time, "monotonic", lambda: next(readings, 2.0))
    with pytest.raises(TimeoutError, match="deadline"):
        harness.run_indexed_keyset_benchmark(rows=20, timeout_seconds=1)


def test_indexed_keyset_benchmark_refuses_incorrect_successors(monkeypatch):
    import sqlalchemy as sa
    import services.keyset_pagination as keyset
    from benchmarks.cloud_scale import run_indexed_keyset_benchmark

    monkeypatch.setattr(keyset, "sqlalchemy_keyset_clause", lambda *args, **kwargs: sa.true())
    with pytest.raises(ValueError, match="correctness"):
        run_indexed_keyset_benchmark(rows=20, page_size=5, repetitions=1)


def test_indexed_keyset_benchmark_interrupts_sqlite_and_disposes_engine(monkeypatch):
    import sqlalchemy as sa
    import benchmarks.cloud_scale as harness

    calls = 0
    disposed = []
    rolled_back = []
    real_create = sa.create_engine

    def tracked_engine(*args, **kwargs):
        engine = real_create(*args, **kwargs)
        sa.event.listen(engine, "engine_disposed", lambda current: disposed.append(True))
        sa.event.listen(engine, "rollback", lambda connection: rolled_back.append(True))
        return engine

    def clock():
        nonlocal calls
        calls += 1
        return 0.0 if calls <= 3 else 2.0

    monkeypatch.setattr(sa, "create_engine", tracked_engine)
    monkeypatch.setattr(harness.time, "monotonic", clock)
    with pytest.raises(TimeoutError, match="deadline") as caught:
        harness.run_indexed_keyset_benchmark(rows=2_000, timeout_seconds=1)

    assert isinstance(caught.value.__cause__, sa.exc.DBAPIError)
    assert disposed == [True]
    assert rolled_back == [True]


@pytest.mark.parametrize("key_width", [3, 8])
def test_indexed_keyset_postgresql_when_explicitly_configured(key_width):
    from benchmarks.cloud_scale import run_indexed_keyset_benchmark

    database_url = os.environ.get("DATAFLOW_KEYSET_BENCHMARK_URL")
    if not database_url:
        pytest.skip("PostgreSQL benchmark requires explicit DATAFLOW_KEYSET_BENCHMARK_URL")
    report = run_indexed_keyset_benchmark(
        rows=2_000, key_width=key_width, page_size=64, repetitions=2,
        database_url=database_url, timeout_seconds=30,
    )

    assert report["database_engine"] == "postgresql"
    assert report["evidence_level"] == "postgresql_query_benchmark"
    assert report["fixture_storage"] == "temporary_postgresql_table"
    assert report["verified_successors"] == 1_000
    assert report["migration_throughput_evidence"] is False
    for result in report["algorithms"].values():
        assert result["plan"][0]["Plan"]
        assert result["verified_rows"] == 1_000
    assert report["algorithms"]["native_tuple"]["uses_index_search"] is True


def test_indexed_keyset_postgresql_cancellation_rolls_back_and_disposes(monkeypatch):
    import sqlalchemy as sa
    from benchmarks.cloud_scale import run_indexed_keyset_benchmark

    database_url = os.environ.get("DATAFLOW_KEYSET_BENCHMARK_URL")
    if not database_url:
        pytest.skip("PostgreSQL benchmark requires explicit DATAFLOW_KEYSET_BENCHMARK_URL")
    disposed = []
    rolled_back = []
    real_create = sa.create_engine

    def delay_plan(connection, cursor, statement, parameters, context, executemany):
        return ("SELECT pg_sleep(3)", {}) if statement.startswith("EXPLAIN") else (statement, parameters)

    def tracked_engine(*args, **kwargs):
        engine = real_create(*args, **kwargs)
        sa.event.listen(engine, "before_cursor_execute", delay_plan, retval=True)
        sa.event.listen(engine, "engine_disposed", lambda current: disposed.append(True))
        sa.event.listen(engine, "rollback", lambda connection: rolled_back.append(True))
        return engine

    monkeypatch.setattr(sa, "create_engine", tracked_engine)
    with pytest.raises(TimeoutError, match="deadline") as caught:
        run_indexed_keyset_benchmark(rows=20, database_url=database_url, timeout_seconds=1)

    assert isinstance(caught.value.__cause__, sa.exc.DBAPIError)
    assert caught.value.__cause__.orig.pgcode == "57014"
    assert disposed == [True]
    assert rolled_back == [True]


@pytest.mark.parametrize("rows, return_code", [(21, 0), (1, 1)])
@pytest.mark.parametrize("module", ["benchmarks.cloud_scale", "apps.api.benchmarks.cloud_scale"])
def test_indexed_keyset_cli_emits_json_and_exit_status(rows, return_code, module):
    import json
    import subprocess

    environment = dict(os.environ)
    if module == "apps.api.benchmarks.cloud_scale":
        environment.pop("PYTHONPATH", None)
    else:
        environment["PYTHONPATH"] = str(_API_ROOT)
    environment.pop("DATAFLOW_KEYSET_BENCHMARK_URL", None)
    result = subprocess.run(
        [sys.executable, "-m", module, "--indexed-keyset", "--rows", str(rows),
         "--key-width", "2", "--page-size", "3", "--repetitions", "1"],
        env=environment, cwd=_API_ROOT.parents[1], capture_output=True, text=True, timeout=30,
    )
    report = json.loads(result.stdout)

    assert result.returncode == return_code, result.stderr
    assert report["success"] is (return_code == 0)
    if return_code == 0:
        assert report["database_engine"] == "sqlite"
        assert report["verified_successors"] == 11
        assert report["migration_throughput_evidence"] is False
    else:
        assert report["error_type"] == "ValueError"


def test_generate_csv_is_detinistic_and_parsable():
    content = generate_csv(1000, seed=7)
    assert b"id,amount,status,created_at" in content
    rows = content.decode("utf-8").strip().splitlines()
    assert len(rows) == 1001
    assert rows[1].startswith("7,")


def test_local_sqlite_scale_transfer(tmp_path: Path):
    rows = 10_000
    db_path = tmp_path / "scale.db"
    content = generate_csv(rows, seed=1)

    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_content=content,
        source_filename="scale.csv",
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            connection_string=str(db_path),
            table="scale_payments",
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
    )

    job_id = f"bench_sqlite_{os.getpid():06d}"
    result = UniversalTransferEngine().execute_tracked(request, job_id)

    assert result.success, result.error
    assert result.records_transferred == rows
    assert result.records_per_second > 0
    assert result.reconciliation.get("passed") is True

    conn = sqlite3.connect(str(db_path))
    try:
        count = conn.execute("SELECT COUNT(*) FROM scale_payments").fetchone()[0]
        assert count == rows
    finally:
        conn.close()


def test_dataflow_exceeds_competitive_baseline():
    """Local SQLite 100k transfer must clear a *measured* floor (not invent 5k).

    ``data/proofs/throughput_microbench.json`` records ~3.6k rows/s on a 5k
    sqlite→sqlite microbench. Full 100k file→sqlite with reconcile is slower
    on the same hardware (~1k–2k). Override with ``DATAFLOW_BENCH_MIN_RPS``.
    """
    from benchmarks.cloud_scale import run_local_benchmark

    # Measured floor with CI/host variance headroom — never claim unmeasured 5k.
    # Contended Windows hosts often land ~650–900 rps on the 100k path.
    min_rps = float(os.environ.get("DATAFLOW_BENCH_MIN_RPS", "600"))
    report = run_local_benchmark(100_000)
    assert report["success"], f"Benchmark failed: {report.get('error', '')}"
    assert report["records_per_second"] > min_rps, (
        f"Throughput too low: {report['records_per_second']} rows/sec "
        f"(floor={min_rps}; see data/proofs/throughput_microbench.json)"
    )
    assert report["destination_summary"].get("verified") is True, "Row count verification failed"
    assert report["peak_memory_mb"] < 500, f"Memory too high: {report['peak_memory_mb']} MB"
