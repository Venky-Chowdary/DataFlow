"""Connector "live" claims come from CI results, never from CI configuration (A4).

An engine is live-verified only when a CI job whose manifest entry *proves*
it ran green (scripts/check_live_skips.py results, merged by
services/connector_ci_evidence.py). These tests pin that rule, and fail when
repository documentation calls an engine live-verified (or derives "live"
from what a workflow starts) without such evidence.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.connector_ci_evidence import (  # noqa: E402
    engine_evidence,
    evidence_document,
    load_manifest,
    load_results,
)

# Committed evidence a docs page may cite (written from a green CI run's
# connector-ci-evidence artifact). Absent: no page may claim live-verified.
EVIDENCE_SNAPSHOT = REPO / "docs" / "reference" / "connector_ci_evidence.json"


def _result(job: str, status: str, executed: int = 120) -> dict:
    return {
        "schema_version": 1,
        "job": job,
        "status": status,
        "counts": {"executed": executed, "unexpected_skips": 0 if status == "green" else 3},
        "run": {"sha": "abc", "run_id": "1"},
        "not_run_reason": "opt-in" if status == "not_run" else "",
    }


@pytest.fixture(scope="module")
def manifest() -> dict:
    return load_manifest()


def test_without_ci_results_nothing_is_live_verified(manifest) -> None:
    doc = evidence_document(manifest, [])
    assert doc["live_verified"] == []
    assert doc["engines"]["sqlserver"]["status"] == "no_ci_result"
    assert doc["engines"]["kafka"]["status"] == "no_ci_result"


def test_sqlserver_is_live_only_when_its_job_runs_green(manifest) -> None:
    skipped_everything = engine_evidence(manifest, [_result("cdc-sqlserver", "red", executed=0)])
    assert skipped_everything["sqlserver"]["status"] == "ci_red"
    assert not skipped_everything["sqlserver"]["live_verified"]
    green = engine_evidence(manifest, [_result("cdc-sqlserver", "green")])
    assert green["sqlserver"]["live_verified"]


def test_a_red_proving_job_outweighs_a_green_one(manifest) -> None:
    ev = engine_evidence(
        manifest, [_result("cdc-sqlserver", "green"), _result("engine-suite", "red")]
    )
    assert ev["sqlserver"]["status"] == "ci_red"


def test_peers_do_not_speak_for_an_engine(manifest) -> None:
    # cdc-sqlserver starts PostgreSQL and MySQL as peers; it does not prove them.
    ev = engine_evidence(manifest, [_result("cdc-sqlserver", "green")])
    assert ev["postgresql"]["status"] == "no_ci_result"
    assert ev["mysql"]["status"] == "no_ci_result"


def test_kafka_is_proved_only_by_the_nightly_engine_suite(manifest) -> None:
    pr_jobs = [
        _result(name, "green")
        for name, spec in manifest["jobs"].items()
        if spec["workflow"].endswith("ci.yml") and spec["mode"] != "report"
    ]
    assert not engine_evidence(manifest, pr_jobs)["kafka"]["live_verified"]
    nightly = engine_evidence(manifest, [_result("engine-suite", "green")])
    assert nightly["kafka"]["live_verified"]


def test_an_opt_in_job_that_did_not_run_is_not_live(manifest) -> None:
    ev = engine_evidence(manifest, [_result("cdc-oracle", "not_run", executed=0)])
    assert ev["oracle"]["status"] == "not_run"
    assert not ev["oracle"]["live_verified"]


def test_report_mode_jobs_never_prove_anything(manifest) -> None:
    for name, spec in manifest["jobs"].items():
        if spec["mode"] == "report":
            assert not spec.get("proves"), name


def test_emulated_engines_are_marked(manifest) -> None:
    ev = engine_evidence(manifest, [_result("warehouse-sku", "green")])
    assert ev["bigquery"]["live_verified"] and ev["bigquery"]["emulated"]


def test_load_results_ignores_other_json(tmp_path: Path) -> None:
    (tmp_path / "a.json").write_text(json.dumps(_result("cdc-sqlserver", "green")), encoding="utf-8")
    (tmp_path / "other.json").write_text(json.dumps({"unique_driver_count": 3}), encoding="utf-8")
    (tmp_path / "broken.json").write_text("{", encoding="utf-8")
    assert [r["job"] for r in load_results([tmp_path])] == ["cdc-sqlserver"]


def test_every_proving_job_starts_its_engine_in_the_workflow(manifest) -> None:
    """A job cannot prove an engine its workflow never starts."""
    markers = {
        "postgresql": r"start_ci_databases\.sh[^\n]*\bpostgres(-cdc)?\b|postgres:16",
        "mysql": r"start_ci_databases\.sh[^\n]*\bmysql\b|mysql:8",
        "mongodb": r"start_ci_databases\.sh[^\n]*\bmongo\b|mongo:7",
        "redis": r"start_ci_databases\.sh[^\n]*\bredis\b|image: redis",
        "kafka": r"apache/kafka",
        "sqlserver": r"start_ci_databases\.sh[^\n]*\bsqlserver\b",
        "oracle": r"DATAFLOW_ORACLE_HOST",
        "bigquery": r"start_ci_databases\.sh[^\n]*\bbigquery\b",
        "snowflake": r"fakesnow",
    }
    shared = (REPO / ".github" / "actions" / "ci-services" / "action.yml").read_text(encoding="utf-8")
    for name, spec in manifest["jobs"].items():
        text = (REPO / spec["workflow"]).read_text(encoding="utf-8") + shared
        for engine in spec.get("proves") or []:
            assert re.search(markers[engine], text), f"{name} proves {engine} but never starts it"


_LIVE_CLAIM = re.compile(r"live[- ]verified|verified (?:live )?in CI|CI[- ]verified", re.IGNORECASE)
# PR 404's first draft derived this column from the workflow YAML.
_CONFIG_CLAIM = re.compile(r"\blive container\b", re.IGNORECASE)


def _doc_files() -> list[Path]:
    docs = REPO / "docs"
    return sorted(docs.rglob("*.md")) if docs.is_dir() else []


def test_docs_never_call_an_engine_live_without_ci_evidence(manifest) -> None:
    verified: set[str] = set()
    if EVIDENCE_SNAPSHOT.is_file():
        snap = json.loads(EVIDENCE_SNAPSHOT.read_text(encoding="utf-8"))
        verified = set(snap.get("live_verified") or [])
    engines = set(engine_evidence(manifest, []))
    problems = []
    for path in _doc_files():
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            named = {e for e in engines if re.search(rf"`{re.escape(e)}`|\b{re.escape(e)}\b", line)}
            if not named:
                continue
            if _CONFIG_CLAIM.search(line):
                problems.append(f"{path.relative_to(REPO)}:{n}: CI configuration shown as a live claim")
            if _LIVE_CLAIM.search(line):
                for engine in sorted(named - verified):
                    problems.append(f"{path.relative_to(REPO)}:{n}: {engine} called live-verified without CI evidence")
    assert not problems, "\n".join(problems)


def test_capability_matrix_report_claims_nothing_without_ci_evidence() -> None:
    from scripts.connector_capability_matrix_report import _ci_evidence

    doc = _ci_evidence(None)
    assert doc["live_verified"] == []
    assert not any(ev["live_verified"] for ev in doc["engines"].values())
