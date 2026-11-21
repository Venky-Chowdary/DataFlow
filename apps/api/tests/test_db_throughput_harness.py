"""The throughput harness itself: deterministic data, a real run, an honest report.

Small live runs of ``benchmarks.db_throughput`` on every route: the source
fill matches the Python reference row for row on both engines, the transfer
succeeds, the independent re-read finds every cell equal, and the report
carries the fields the CI smoke and the docs read. Also checks the floors
file the CI smoke gates on.
"""

from __future__ import annotations

import json

import pytest

from benchmarks import db_throughput, throughput_smoke
from tests.helpers.live_env import mysql_creds, mysql_up, pg_creds, pg_up


@pytest.fixture()
def bench_env(monkeypatch, tmp_path):
    p, m = pg_creds(), mysql_creds()
    for key, value in {
        "PGHOST": p["host"], "PGPORT": str(p["port"]), "PGUSER": p["username"],
        "PGPASSWORD": p["password"], "PGDATABASE": p["database"],
        "MYSQL_HOST": m["host"], "MYSQL_PORT": str(m["port"]), "MYSQL_USER": m["username"],
        "MYSQL_PASSWORD": m["password"], "MYSQL_DATABASE": m["database"],
    }.items():
        monkeypatch.setenv(key, value)
    return tmp_path


def test_reference_row_is_deterministic_and_mixed():
    rows = [db_throughput.expected_row(g) for g in range(1, 400)]
    assert rows == [db_throughput.expected_row(g) for g in range(1, 400)]
    notes = [r[8] for r in rows]
    assert None in notes and "" in notes and any(n and "\t" in n for n in notes)
    for col in range(1, 11):
        if col in (7, 10):  # label, city: NOT NULL
            continue
        assert any(r[col] is None for r in rows), db_throughput.COLUMNS[col]


@pytest.mark.parametrize("route", ["pg-pg", "pg-mysql", "mysql-pg", "mysql-mysql"])
def test_small_live_run_reports_and_verifies(route, bench_env):
    for engine in set(route.split("-")):
        if not (pg_up() if engine == "pg" else mysql_up()):
            pytest.skip(f"{'PostgreSQL' if engine == 'pg' else 'MySQL'} not reachable")
    out = bench_env / "r.json"
    rc = db_throughput.main(["--route", route, "--rows", "1500", "--out", str(out)])
    report = json.loads(out.read_text(encoding="utf-8"))
    assert rc == 0, report
    assert report["success"] and report["records_transferred"] == 1500
    check = report["independent_check"]
    assert check["passed"] and check["rows_compared"] == 1500
    assert check["source_rows_not_matching_reference"] == 0
    for key in ("rows_per_second", "wall_seconds", "verification_seconds", "peak_rss_mb",
                "load_methods", "phase_profile"):
        assert key in report, key
    assert report["verification_seconds"] > 0


def test_floor_breach_fails_the_run(bench_env):
    if not pg_up():
        pytest.skip("PostgreSQL not reachable")
    out = bench_env / "r.json"
    rc = db_throughput.main(["--route", "pg-pg", "--rows", "200", "--out", str(out),
                             "--min-rows-per-second", "100000000"])
    report = json.loads(out.read_text(encoding="utf-8"))
    assert rc == 1 and any("below the floor" in f for f in report["failures"])


def test_smoke_floors_cover_every_route_with_a_floor():
    floors = throughput_smoke.load_floors()
    routes = floors["routes"]
    assert {"pg-pg", "pg-pg-row", "pg-mysql", "mysql-pg"} <= set(routes)
    for name, cfg in routes.items():
        assert cfg["route"] in {f"{a}-{b}" for a in db_throughput.ENGINES for b in db_throughput.ENGINES}
        assert float(cfg["min_rows_per_second"]) > 0, name


def test_floors_scale_down_only_for_a_slower_runner_and_never_below_three_quarters():
    assert throughput_smoke.effective_floor(400, 0.4, 0.5) == 320
    assert throughput_smoke.effective_floor(400, 0.4, 0.8) == 300  # clamped, not 200
    assert throughput_smoke.effective_floor(400, 0.4, 4.0) == 300
    assert throughput_smoke.effective_floor(300, 0.4, 0.2) == 300
    assert throughput_smoke.effective_floor(300, 0, 0.8) == 300


def test_every_row_path_floor_stays_above_the_pre_fast_path_rate():
    """At the most a runner can scale a floor down, the row-path routes still
    fail at the rate the code ran before the fast paths (271 rows/s max)."""
    floors = throughput_smoke.load_floors()
    for name, cfg in floors["routes"].items():
        lowest = float(cfg["min_rows_per_second"]) * throughput_smoke.MIN_SCALE
        assert lowest > 271 * 1.05, (name, lowest)
    assert throughput_smoke.load_floors()["reference_calibration_seconds"] > 0
    assert 0 < throughput_smoke.calibrate(rounds=1) < 60
