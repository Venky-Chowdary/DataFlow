"""Proofs → Scale throughput, end to end through the HTTP routes the page uses.

QA (production 557f556c):
* the 100k benchmark failed with "Request timed out" after ~3 minutes — the
  page held one HTTP request open for the whole run;
* peak memory showed "0 MB" — the engine reports the growth of the API
  process's lifetime ``ru_maxrss`` high-water mark, which a long-lived process
  has already passed;
* "VS DATAWRAP" said competitors were "5.6x slower" while listing 4,000 rows/s
  against our 717 — the label named no subject and read inverted.

The run is now a background job (a child process) with progress; memory is
that process's peak RSS; the comparison is computed from the two numbers.
"""

from __future__ import annotations

import os
import time

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(monkeypatch):
    from src.main import app

    monkeypatch.setenv("DATAFLOW_ENV", "development")
    monkeypatch.delenv("DATAFLOW_DISABLE_OBJECT_STORE", raising=False)
    with TestClient(app) as c:
        yield c


def _wait(client: TestClient, run_id: str, timeout: float = 240.0) -> tuple[dict, list[dict]]:
    seen: list[dict] = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        res = client.get(f"/api/v1/workspace/benchmark/runs/{run_id}")
        assert res.status_code == 200, res.text
        run = res.json()["run"]
        seen.append(run)
        if run["status"] in {"succeeded", "failed"}:
            return run, seen
        time.sleep(0.3)
    raise AssertionError(f"benchmark run did not finish: {seen[-1] if seen else None}")


def test_benchmark_runs_in_the_background_with_progress_and_real_memory(client):
    started = time.monotonic()
    res = client.post("/api/v1/workspace/benchmark/runs", json={"rows": 10_000})
    # The request returns immediately; the run happens after it.
    assert res.status_code == 202, res.text
    assert time.monotonic() - started < 5
    run = res.json()["run"]
    assert run["status"] in {"queued", "running"}

    # A second start while one is in flight joins it instead of competing for CPU.
    again = client.post("/api/v1/workspace/benchmark/runs", json={"rows": 10_000})
    assert again.status_code == 200 and again.json()["run"]["id"] == run["id"]

    final, seen = _wait(client, run["id"])
    assert final["status"] == "succeeded", final
    assert any(s["status"] == "running" for s in seen)
    assert final["progress_pct"] == 100.0

    report = final["report"]
    assert report["success"] is True
    assert report["rows"] == 10_000
    assert report["destination_summary"]["verified"] is True
    assert report["records_per_second"] > 0
    assert report["peak_memory_mb"] > 10, report["peak_memory_mb"]
    assert report["peak_memory_basis"]

    # The API process's environment is not switched to the benchmark's store.
    assert "DATAFLOW_DISABLE_OBJECT_STORE" not in os.environ

    # Comparison labels follow the numbers.
    ours = report["records_per_second"]
    for c in report["competitors"]:
        label = c["comparison"]["label"]
        if c["typical_rps"] > ours * 1.1:
            assert label.startswith("Datawrap") and "slower" in label, (ours, c)
        elif c["typical_rps"] < ours / 1.1:
            assert label.startswith("Datawrap") and "faster" in label, (ours, c)

    md = client.get(f"/api/v1/workspace/benchmark/runs/{run['id']}?format=md")
    assert md.status_code == 200
    assert "Datawrap vs this" in md.text
    assert "0 MB" not in md.text


def test_comparison_label_names_datawrap_and_follows_the_ratio():
    from services.benchmark_runs import comparison

    qa = comparison(717.6, 4000)
    assert qa["verdict"] == "slower"
    assert qa["label"] == "Datawrap 5.6× slower"
    assert comparison(5000, 2500)["label"] == "Datawrap 2.0× faster"
    assert comparison(1000, 1050)["label"] == "About the same"
    assert comparison(0, 4000)["verdict"] == "unknown"


def test_unknown_run_is_404(client):
    res = client.get("/api/v1/workspace/benchmark/runs/nope")
    assert res.status_code == 404
    assert res.json()["detail"] == "Benchmark run not found"
