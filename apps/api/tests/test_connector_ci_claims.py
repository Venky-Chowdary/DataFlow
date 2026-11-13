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
    green = engine_evidence(
        manifest,
        [
            {**_result("cdc-sqlserver", "green"), "labels": {"driver": d}}
            for d in ("pyodbc", "pymssql")
        ],
    )
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
    shards = [
        {**_result("engine-suite", "green"), "labels": {"shard": str(n)}} for n in range(1, 7)
    ]
    assert engine_evidence(manifest, shards)["kafka"]["live_verified"]
    # A shard that never reported (cancelled, timed out) is not a green nightly.
    partial = engine_evidence(manifest, shards[:5])["kafka"]
    assert partial["status"] == "no_ci_result"
    assert "engine-suite[shard=6]" in partial["missing_results"]


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


def _claim_files() -> list[Path]:
    """Where a reader sees connector status: docs, every README, and the web UI's strings."""
    docs = REPO / "docs"
    files = sorted(docs.rglob("*.md")) if docs.is_dir() else []
    files += sorted(
        p for p in REPO.rglob("README*")
        if p.is_file() and not {"node_modules", ".git"} & set(p.relative_to(REPO).parts)
    )
    web = REPO / "apps" / "web" / "src"
    files += sorted(
        p for p in web.rglob("*") if p.suffix in {".ts", ".tsx"} and ".test." not in p.name
    ) if web.is_dir() else []
    return files


def _engine_patterns(manifest) -> dict[str, re.Pattern[str]]:
    """Engine id plus its display aliases (manifest ``display_aliases``), case-insensitive."""
    aliases = manifest.get("display_aliases") or {}
    out = {}
    for engine in engine_evidence(manifest, []):
        names = [engine, *aliases.get(engine, [])]
        alt = "|".join(re.escape(n) for n in sorted(names, key=len, reverse=True))
        out[engine] = re.compile(rf"(?<![A-Za-z0-9_])(?:{alt})(?![A-Za-z0-9_])", re.IGNORECASE)
    return out


def _claims(manifest, files: list[Path], verified: set[str]) -> list[str]:
    patterns = _engine_patterns(manifest)
    problems = []
    for path in files:
        rel = path.relative_to(REPO) if path.is_relative_to(REPO) else path
        for n, line in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
            named = {e for e, pat in patterns.items() if pat.search(line)}
            if not named:
                continue
            if _CONFIG_CLAIM.search(line):
                problems.append(f"{rel}:{n}: CI configuration shown as a live claim")
            if _LIVE_CLAIM.search(line):
                for engine in sorted(named - verified):
                    problems.append(f"{rel}:{n}: {engine} called live-verified without CI evidence")
    return problems


def test_docs_never_call_an_engine_live_without_ci_evidence(manifest) -> None:
    verified: set[str] = set()
    if EVIDENCE_SNAPSHOT.is_file():
        snap = json.loads(EVIDENCE_SNAPSHOT.read_text(encoding="utf-8"))
        verified = set(snap.get("live_verified") or [])
    files = _claim_files()
    assert any(p.name.startswith("README") for p in files)
    assert any(p.suffix == ".tsx" for p in files), "the web UI strings are scanned"
    problems = _claims(manifest, files, verified)
    assert not problems, "\n".join(problems)


@pytest.mark.parametrize(
    ("line", "engine"),
    [
        ("SQL Server: live-verified in CI", "sqlserver"),
        ("**MSSQL** is CI-verified", "sqlserver"),
        ("Microsoft SQL Server — verified in CI", "sqlserver"),
        ("Postgres is live verified", "postgresql"),
        ("POSTGRESQL: verified live in CI", "postgresql"),
        ("Mongo (live-verified)", "mongodb"),
        ("MongoDB: CI-verified", "mongodb"),
        ("mysql / MariaDB live-verified", "mysql"),
        ("Apache Kafka: live-verified", "kafka"),
        ("Oracle Database: CI verified", "oracle"),
        ("Google BigQuery is live verified", "bigquery"),
        ('label: "Live-verified", engine: "SqlServer"', "sqlserver"),
    ],
)
def test_a_claim_is_caught_whatever_the_engine_is_called(manifest, tmp_path: Path, line: str, engine: str) -> None:
    page = tmp_path / "README.md"
    page.write_text(f"# Connectors\n\n{line}\n", encoding="utf-8")
    problems = _claims(manifest, [page], verified=set())
    assert any(f"{engine} called live-verified" in p for p in problems), problems
    # Once CI evidence verifies the engine, the same line is a fair claim.
    assert not [p for p in _claims(manifest, [page], verified={engine}) if f"{engine} called" in p]


def test_every_started_engine_has_display_aliases(manifest) -> None:
    aliases = manifest.get("display_aliases") or {}
    for engine in engine_evidence(manifest, []):
        assert aliases.get(engine), f"live_engine_jobs.json display_aliases lacks {engine}"
    assert {"SQL Server", "MSSQL"} <= set(aliases["sqlserver"])
    assert "Postgres" in aliases["postgresql"] and "Mongo" in aliases["mongodb"]


def test_capability_matrix_report_claims_nothing_without_ci_evidence() -> None:
    from scripts.connector_capability_matrix_report import _ci_evidence

    doc = _ci_evidence(None)
    assert doc["live_verified"] == []
    assert not any(ev["live_verified"] for ev in doc["engines"].values())


def _driver_result(status: str, driver: str) -> dict:
    return {**_result("cdc-sqlserver", status), "labels": {"driver": driver}}


def test_sqlserver_evidence_names_the_drivers_that_proved_it(manifest) -> None:
    ev = engine_evidence(
        manifest, [_driver_result("green", "pyodbc"), _driver_result("green", "pymssql")]
    )["sqlserver"]
    assert ev["live_verified"]
    assert ev["drivers"] == ["pymssql", "pyodbc"]


def test_sqlserver_needs_the_production_driver_green(manifest) -> None:
    # Production has no ODBC driver and dials SQL Server through pymssql, so a
    # pyodbc-only green is not live-verified, and a red pymssql leg is red.
    only_odbc = engine_evidence(manifest, [_driver_result("green", "pyodbc")])["sqlserver"]
    assert only_odbc["status"] == "no_ci_result"
    assert "cdc-sqlserver[driver=pymssql]" in only_odbc["missing_results"]
    red = engine_evidence(
        manifest, [_driver_result("green", "pyodbc"), _driver_result("red", "pymssql")]
    )["sqlserver"]
    assert red["status"] == "ci_red"


def test_a_green_job_whose_engine_tests_skipped_does_not_prove_that_engine(manifest) -> None:
    shards = [
        {**_result("engine-suite", "green"), "labels": {"shard": str(n)},
         "engine_skips": {"kafka": 3} if n == 2 else {}}
        for n in range(1, 7)
    ]
    ev = engine_evidence(manifest, shards)
    assert not ev["kafka"]["live_verified"]
    assert ev["kafka"]["status"] == "ci_red"
    assert ev["redis"]["live_verified"]
