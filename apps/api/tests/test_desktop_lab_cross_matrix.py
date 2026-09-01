"""Unique-engine source × dest cartesian — live desktop backends.

100% is not claimed. Skip when a port is closed. Never invent Salesforce green.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from services.desktop_lab_cross import (
    CORE_UNIQUE_ENGINES,
    LIVE_UNIQUE_ENGINES,
    bind_live_engine,
    engines_for_run,
    run_live_engine_cross_matrix,
)


def test_cross_matrix_lists_unique_engines_not_saas_twins():
    assert "postgresql" in LIVE_UNIQUE_ENGINES
    assert "mysql" in LIVE_UNIQUE_ENGINES
    assert "mongodb" in LIVE_UNIQUE_ENGINES
    assert "s3" in LIVE_UNIQUE_ENGINES
    assert "elasticsearch" in LIVE_UNIQUE_ENGINES
    assert "elasticsearch" not in CORE_UNIQUE_ENGINES
    assert "salesforce" not in LIVE_UNIQUE_ENGINES
    assert "hubspot" not in LIVE_UNIQUE_ENGINES
    assert "postgresql_rds" not in LIVE_UNIQUE_ENGINES
    assert len(LIVE_UNIQUE_ENGINES) == len(set(LIVE_UNIQUE_ENGINES))
    assert len(LIVE_UNIQUE_ENGINES) >= 10
    assert engines_for_run() == CORE_UNIQUE_ENGINES


def test_bind_skips_closed_port(tmp_path, monkeypatch):
    import services.desktop_lab_cross as mod

    monkeypatch.setattr(mod, "_reachable", lambda *a, **k: False)
    bound = bind_live_engine("postgresql", "t", tmp_path)
    assert isinstance(bound, str)
    assert "not reachable" in bound


def test_pg_mysql_minio_cross_smoke_when_reachable(tmp_path):
    """Four OLTP/object pairs — the route the UI smoke missed."""
    from services.desktop_lab_cross import _seed, _transfer_pair, _sid

    wanted = ("postgresql", "mysql", "s3")
    seeds = {}
    for engine in wanted:
        bound, row = _seed(engine, tmp_path)
        if row["status"] == "skipped":
            continue
        assert row["status"] == "passed", row
        seeds[engine] = bound
    if len(seeds) < 2:
        return
    for src_id, src in seeds.items():
        for dst_id in seeds:
            dst = bind_live_engine(dst_id, _sid("d"), tmp_path)
            assert not isinstance(dst, str), dst
            outcome = _transfer_pair(src, dst)
            assert outcome["status"] == "passed", (src_id, dst_id, outcome)


@pytest.mark.skipif(
    os.environ.get("DATAFLOW_CROSS_MATRIX", "").strip() != "1",
    reason="Full unique-engine cartesian is opted in via DATAFLOW_CROSS_MATRIX=1",
)
def test_live_engine_cross_matrix_writes_artifact():
    report = run_live_engine_cross_matrix(persist=True)
    assert report["pairs"] == len(engines_for_run()) ** 2
    assert report["passed"] + report["failed"] + report["skipped"] == report["pairs"]
    assert report["honesty"]["catalog_tiles_are_not_transfer_live"] is True
    assert report["honesty"]["not_catalog_alias_cartesian"] is True
    for row in report["routes"]:
        if row["status"] == "passed":
            assert row.get("silent_loss") in (False, None)
            assert row.get("integrity") in {
                "passed",
                "dest_count_pair_payload_sampled_on_engine",
            }
    artifact = Path("/opt/cursor/artifacts/desktop_lab_cross.json")
    if artifact.is_file():
        saved = json.loads(artifact.read_text())
        assert saved["pairs"] == report["pairs"]
        assert saved["passed"] == report["passed"]


def test_elasticsearch_bind_skips_closed_port(tmp_path, monkeypatch):
    import services.desktop_lab_cross as mod

    monkeypatch.setattr(mod, "_reachable", lambda *a, **k: False)
    bound = bind_live_engine("elasticsearch", "t", tmp_path)
    assert isinstance(bound, str)
    assert "9200" in bound


def test_pair_timeout_is_skip_never_pass(monkeypatch):
    import services.desktop_lab_cross as mod

    monkeypatch.setenv("DATAFLOW_CROSS_EXTENDED", "")
    monkeypatch.setattr(mod, "engines_for_run", lambda: ("sqlite",))

    def boom(fn, timeout_sec, *args, **kwargs):
        raise TimeoutError("exceeded 15s")

    monkeypatch.setattr(mod, "_call_with_timeout", boom)
    report = mod.run_live_engine_cross_matrix(persist=False)
    assert report["passed"] == 0
    assert report["failed"] == 0
    assert report["skipped"] == 1
    assert report["routes"][0]["status"] == "skipped"
    assert "source sqlite was not seeded" in report["routes"][0]["error"]
    assert report["unique_engines_seed_skipped"]
    assert "exceeded" in report["unique_engines_seed_skipped"][0]["error"]
