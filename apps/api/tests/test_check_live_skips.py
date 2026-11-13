"""The live-engine skip checker (A3): a skip in a live-engine job fails CI.

Covers junit parsing, the per-job allowlist, the three modes, the executed
floor, opt-in jobs that did not run, and that every live-engine job in the
manifest is wired to the checker in its workflow (and nothing else is).
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

from scripts.check_live_skips import (  # noqa: E402
    MANIFEST,
    check,
    load_manifest,
    main,
    names_engine,
    node_id,
    read_junit,
)

JUNIT = """<?xml version="1.0" encoding="utf-8"?>
<testsuites><testsuite name="pytest">
  <testcase classname="tests.test_a" name="test_ok" time="0.1"/>
  <testcase classname="tests.test_a.TestK" name="test_m[p1]" time="0.1"/>
  <testcase classname="tests.test_b" name="test_fail"><failure message="boom">x</failure></testcase>
  <testcase classname="tests.test_b" name="test_err"><error message="teardown">x</error></testcase>
  <testcase classname="tests.test_c" name="test_xf"><skipped type="pytest.xfail" message="known"/></testcase>
  <testcase classname="tests.test_live" name="test_pg"><skipped type="pytest.skip" message="DATAFLOW_PG_TEST_DSN unset"/></testcase>
  <testcase classname="tests.test_live" name="test_saas"><skipped type="pytest.skip" message="No endpoint template for driver 'zendesk'"/></testcase>
</testsuite></testsuites>
"""


def _manifest(mode: str = "strict", **job) -> dict:
    spec = {"mode": mode, "engines": ["postgresql", "sqlserver"], "min_executed": 1, "expected_skips": []}
    spec.update(job)
    return {
        "schema_version": 1,
        "engine_tokens": {
            "postgresql": ["postgres", "pg", "pgvector"],
            "sqlserver": ["sqlserver", "sql server", "mssql"],
            "oracle": ["oracle"],
        },
        "jobs": {"j": spec},
    }


@pytest.fixture()
def junit(tmp_path: Path) -> Path:
    path = tmp_path / "j.xml"
    path.write_text(JUNIT, encoding="utf-8")
    return path


def test_node_id_rebuilds_module_class_and_param() -> None:
    assert node_id("tests.test_a", "test_ok") == "tests/test_a.py::test_ok"
    assert node_id("tests.test_a.TestK", "test_m[p1]") == "tests/test_a.py::TestK::test_m[p1]"
    assert node_id("", "tests/test_mod.py") == "tests/test_mod.py"


def test_read_junit_classifies_every_outcome(junit: Path) -> None:
    outcomes = {c.test: c.outcome for c in read_junit([junit])}
    assert outcomes == {
        "tests/test_a.py::test_ok": "passed",
        "tests/test_a.py::TestK::test_m[p1]": "passed",
        "tests/test_b.py::test_fail": "failed",
        "tests/test_b.py::test_err": "failed",
        "tests/test_c.py::test_xf": "xfailed",
        "tests/test_live.py::test_pg": "skipped",
        "tests/test_live.py::test_saas": "skipped",
    }


def test_strict_mode_fails_on_every_unlisted_skip(junit: Path) -> None:
    result = check("j", [c for c in read_junit([junit]) if c.outcome != "failed"], _manifest())
    assert result.status == "red"
    assert result.exit_code() == 1
    assert {r["test"] for r in result.unexpected} == {
        "tests/test_live.py::test_pg",
        "tests/test_live.py::test_saas",
    }
    # An xfail is not a skip.
    assert result.to_json()["counts"]["xfailed"] == 1


def test_a_failed_test_is_red_even_with_no_skips(junit: Path) -> None:
    cases = [c for c in read_junit([junit]) if c.outcome != "skipped"]
    result = check("j", cases, _manifest())
    assert not result.unexpected
    assert result.status == "red" and result.exit_code() == 1


def test_allowlist_needs_both_the_test_and_the_reason(junit: Path) -> None:
    allow = [
        {"test": "tests/test_live.py::test_saas", "reason": "No endpoint template", "why": "SaaS"},
        # Right test, wrong reason: the PG skip must still count.
        {"test": "tests/test_live.py::test_pg", "reason": "not reachable", "why": "PG down"},
        {"test": "tests/test_gone.py::*", "reason": ".*", "why": "removed module"},
    ]
    cases = [c for c in read_junit([junit]) if c.outcome != "failed"]
    result = check("j", cases, _manifest(expected_skips=allow))
    assert [r["test"] for r in result.expected] == ["tests/test_live.py::test_saas"]
    assert result.expected[0]["why"] == "SaaS"
    assert [r["test"] for r in result.unexpected] == ["tests/test_live.py::test_pg"]
    assert {r["test"] for r in result.stale_allowlist} == {
        "tests/test_live.py::test_pg",
        "tests/test_gone.py::*",
    }


def test_engine_scoped_counts_only_skips_that_name_a_job_engine(junit: Path) -> None:
    cases = [c for c in read_junit([junit]) if c.outcome != "failed"]
    result = check("j", cases, _manifest(mode="engine_scoped"))
    assert [r["test"] for r in result.unexpected] == ["tests/test_live.py::test_pg"]
    assert result.unexpected[0]["engines"] == "postgresql"
    assert [r["test"] for r in result.ignored] == ["tests/test_live.py::test_saas"]
    assert result.exit_code() == 1


def test_engine_tokens_match_whole_words_only() -> None:
    tokens = {"postgresql": ["pg"], "sqlserver": ["sql server", "mssql"], "oracle": ["oracle"]}
    engines = list(tokens)
    assert names_engine("DATAFLOW_PG_TEST_DSN unset", engines, tokens) == ["postgresql"]
    assert names_engine("pgvector extension missing", engines, tokens) == []
    assert names_engine("live SQL Server not reachable", engines, tokens) == ["sqlserver"]
    assert names_engine("source database/oracle not reachable", ["postgresql"], tokens) == []


def test_report_mode_publishes_but_never_fails(junit: Path) -> None:
    result = check("j", read_junit([junit]), _manifest(mode="report", min_executed=999))
    assert result.exit_code() == 0
    assert not result.enforced
    assert [r["test"] for r in result.unexpected] == ["tests/test_live.py::test_pg"]


def test_a_collapsed_selection_is_not_green(junit: Path) -> None:
    cases = [c for c in read_junit([junit]) if c.outcome == "passed"]
    result = check("j", cases, _manifest(min_executed=5))
    assert result.status == "red"
    assert any("floor is 5" in p for p in result.problems)


def test_missing_junit_is_red(tmp_path: Path) -> None:
    result = check("j", [], _manifest(min_executed=0), missing_junit=[str(tmp_path / "nope.xml")])
    assert result.status == "red" and result.exit_code() == 1


def test_opt_in_job_that_did_not_run_is_not_run_not_green() -> None:
    opt = check("j", [], _manifest(opt_in="needs a secret"), not_run="secret unset")
    assert opt.status == "not_run" and opt.exit_code() == 0
    plain = check("j", [], _manifest(), not_run="nobody started it")
    assert plain.status == "not_run" and plain.exit_code() == 1


def test_cli_writes_the_result_json(junit: Path, tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    manifest = tmp_path / "m.json"
    manifest.write_text(json.dumps(_manifest(mode="engine_scoped")), encoding="utf-8")
    out = tmp_path / "out" / "j.json"
    code = main(["--job", "j", "--junit", str(junit), "--manifest", str(manifest), "--out", str(out)])
    data = json.loads(out.read_text(encoding="utf-8"))
    assert code == 1
    assert data["status"] == "red"
    assert data["counts"]["unexpected_skips"] == 1
    assert data["counts"]["failed"] == 2


def test_manifest_entries_carry_a_reason_and_a_why(tmp_path: Path) -> None:
    bad = _manifest(expected_skips=[{"test": "tests/x.py::*", "reason": "x", "why": ""}])
    path = tmp_path / "m.json"
    path.write_text(json.dumps(bad), encoding="utf-8")
    with pytest.raises(ValueError, match="lacks"):
        load_manifest(path)


def _checker_jobs(workflow: Path) -> set[str]:
    text = workflow.read_text(encoding="utf-8")
    return set(re.findall(r"check_live_skips\.py[^\n]*?--job[ =]\"?([a-z0-9-]+)", text))


def test_every_live_engine_job_is_wired_to_the_checker() -> None:
    manifest = load_manifest(MANIFEST)
    by_workflow: dict[str, set[str]] = {}
    for name, spec in manifest["jobs"].items():
        by_workflow.setdefault(spec["workflow"], set()).add(name)
    wired_anywhere: set[str] = set()
    for workflow, jobs in by_workflow.items():
        wired = _checker_jobs(REPO / workflow)
        wired_anywhere |= wired
        assert jobs <= wired, f"{workflow} never runs the checker for {sorted(jobs - wired)}"
    for path in (REPO / ".github" / "workflows").glob("*.yml"):
        unknown = _checker_jobs(path) - set(manifest["jobs"])
        assert not unknown, f"{path.name} checks jobs missing from the manifest: {sorted(unknown)}"


def test_labels_record_which_variant_of_a_job_ran(junit: Path, tmp_path: Path, monkeypatch) -> None:
    """cdc-sqlserver runs once per SQL Server driver; the result names the driver."""
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    manifest = tmp_path / "m.json"
    manifest.write_text(json.dumps(_manifest(mode="report")), encoding="utf-8")
    out = tmp_path / "j.json"
    main(["--job", "j", "--junit", str(junit), "--manifest", str(manifest), "--out", str(out),
          "--label", "driver=pymssql"])
    assert json.loads(out.read_text(encoding="utf-8"))["labels"] == {"driver": "pymssql"}
    with pytest.raises(SystemExit):
        main(["--job", "j", "--junit", str(junit), "--manifest", str(manifest), "--label", "pymssql"])


def test_a_skip_that_also_names_an_engine_the_job_lacks_is_not_this_jobs(tmp_path: Path) -> None:
    """"MySQL 3306 or Oracle 1521 not reachable" skips because Oracle is absent."""
    from scripts.check_live_skips import Case

    manifest = _manifest(mode="engine_scoped", engines=["postgresql"])
    manifest["engine_tokens"]["mysql"] = ["mysql"]
    manifest["jobs"]["j"]["engines"] = ["postgresql", "mysql"]
    cases = [
        Case("tests/a.py::t1", "skipped", "MySQL 3306 or Oracle 1521 not reachable"),
        Case("tests/a.py::t2", "skipped", "live MySQL / PostgreSQL not reachable"),
        Case("tests/a.py::t3", "skipped", "PostgreSQL 5432 or SQL Server 1433 not reachable"),
        Case("tests/a.py::t4", "passed"),
    ]
    result = check("j", cases, manifest)
    assert [r["test"] for r in result.unexpected] == ["tests/a.py::t2"]
    assert {r["test"] for r in result.ignored} == {"tests/a.py::t1", "tests/a.py::t3"}


def test_mariadb_is_its_own_engine_not_mysql() -> None:
    manifest = load_manifest(MANIFEST)
    assert "mariadb" not in manifest["engine_tokens"]["mysql"]
    assert "mariadb" in manifest["engine_tokens"]


def test_report_engines_are_counted_but_do_not_fail_the_job() -> None:
    """The nightly cannot yet prove Kafka / SQL Server skip-free: their skips are
    published per engine (so evidence stays honest) without failing the run."""
    from scripts.check_live_skips import Case

    manifest = _manifest(mode="engine_scoped", report_engines=["sqlserver"])
    cases = [
        Case("tests/a.py::t1", "skipped", "live SQL Server not reachable"),
        Case("tests/a.py::t2", "passed"),
    ]
    result = check("j", cases, manifest)
    assert result.status == "green" and result.exit_code() == 0
    assert result.to_json()["engine_skips"] == {"sqlserver": 1}
    assert [r["test"] for r in result.reported] == ["tests/a.py::t1"]


def test_a_job_can_inherit_another_jobs_expected_skips() -> None:
    """The nightly runs the same suite as api-tests: one allowlist, not two copies."""
    from scripts.check_live_skips import Case

    manifest = _manifest(mode="engine_scoped", inherit_expected_skips=["base"])
    manifest["jobs"]["base"] = {
        "mode": "report",
        "engines": ["postgresql"],
        "expected_skips": [{"test": "tests/a.py::*", "reason": "wal_level=replica", "why": "second PG"}],
    }
    cases = [Case("tests/a.py::t", "skipped", "needs a PostgreSQL with wal_level=replica"), Case("tests/a.py::u", "passed")]
    result = check("j", cases, manifest)
    assert not result.unexpected and result.expected[0]["why"] == "second PG"
