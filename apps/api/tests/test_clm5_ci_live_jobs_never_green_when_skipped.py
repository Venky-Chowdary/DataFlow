"""CLM-5: a live-engine CI job may not show green while its live steps skip.

cdc-oracle finished "success" with every step skipped (opt-in off), and
cdc-sqlserver once skipped its six CDC tests and its SKU matrix while green.
PR 424's strict checker (scripts/check_live_skips.py) now fails a skip inside
a job that runs; these tests pin the workflow wiring around it:

* cdc-oracle opts in at the job, so "off" is a skipped (grey) job, never a
  green one, and live-engine-evidence records not_run for it;
* every junit file a cdc-sqlserver pytest step writes is fed to the strict
  checker, which runs even when an earlier step failed.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[3]
CI = REPO / ".github" / "workflows" / "ci.yml"
MANIFEST = REPO / "apps" / "api" / "scripts" / "live_engine_jobs.json"


def _jobs() -> dict:
    return yaml.safe_load(CI.read_text(encoding="utf-8"))["jobs"]


def test_oracle_opt_in_is_a_skipped_job_not_a_green_one() -> None:
    job = _jobs()["cdc-oracle"]
    assert "vars.ENABLE_ORACLE_CDC == 'true'" in str(job.get("if") or "")
    for step in job["steps"]:
        cond = str(step.get("if") or "")
        # No step may be skipped on an opt-out flag: that is how the job
        # used to end green having run nothing.
        assert "skip" not in cond, step.get("name")
        assert "--not-run" not in str(step.get("run") or ""), step.get("name")
    run = "\n".join(str(s.get("run") or "") for s in job["steps"])
    assert re.search(r"DATAFLOW_ORACLE_PASSWORD[^\n]*\n[^\n]*\n\s*exit 1", run), "missing secret must fail"
    assert "check_live_skips.py --job cdc-oracle --junit" in run


def test_the_evidence_job_records_oracle_not_run_and_waits_for_api_tests() -> None:
    job = _jobs()["live-engine-evidence"]
    assert {"cdc-oracle", "api-tests"} <= set(job["needs"])
    steps = [s for s in job["steps"] if "--not-run" in str(s.get("run") or "")]
    assert len(steps) == 1
    assert "needs.cdc-oracle.result == 'skipped'" in str(steps[0].get("if"))


def test_the_required_gate_accepts_a_skipped_oracle_only_when_it_is_off() -> None:
    gate = _jobs()["required-checks"]
    run = "\n".join(str(s.get("run") or "") for s in gate["steps"])
    assert '[ "$name" = "cdc-oracle" ] && [ "$result" = "skipped" ] && [ "$ENABLE_ORACLE_CDC" != "true" ]' in run
    env = {k: v for s in gate["steps"] for k, v in (s.get("env") or {}).items()}
    assert env.get("ENABLE_ORACLE_CDC") == "${{ vars.ENABLE_ORACLE_CDC }}"


def test_every_sqlserver_junit_reaches_the_strict_checker() -> None:
    job = _jobs()["cdc-sqlserver"]
    assert not job.get("continue-on-error")
    written: set[str] = set()
    checker = None
    for step in job["steps"]:
        run = str(step.get("run") or "")
        assert not step.get("continue-on-error"), step.get("name")
        written |= set(re.findall(r'--junitxml="\$RUNNER_TEMP/live-junit/([\w.-]+)"', run))
        if "check_live_skips.py --job cdc-sqlserver" in run:
            checker = step
    assert written, "the SQL Server job writes no junit"
    assert checker is not None
    fed = set(re.findall(r'--junit "\$RUNNER_TEMP/live-junit/([\w.-]+)"', str(checker["run"])))
    assert written <= fed, sorted(written - fed)
    # It runs after a failed step too: a missing junit is red, not skipped.
    assert str(checker.get("if")).replace(" ", "") == "${{!cancelled()}}"
    spec = json.loads(MANIFEST.read_text(encoding="utf-8"))["jobs"]["cdc-sqlserver"]
    assert spec["mode"] == "strict" and spec["expected_skips"] == []


#: In the Required PR gate (three green legs in a row). kafka-live is not
#: yet: its three legs on 81f5ec525 ran out of time (see ci.yml).
_REQUIRED_LIVE_JOBS = {"oracle-live"}


@pytest.mark.parametrize(("job_name", "engine", "starts"), [
    ("oracle-live", "oracle", "start_ci_databases.sh oracle"),
    ("kafka-live", "kafka", "start_ci_databases.sh kafka"),
])
def test_the_container_live_jobs_run_on_every_backend_change_and_feed_every_junit(
    job_name: str, engine: str, starts: str
) -> None:
    """oracle-live / kafka-live need no secret: they start the engine and run.

    They follow the changes filter (skipped only when the backend checks are
    reused), start their engine, write junit only where the checker reads
    it, run the checker after a failed step, and the evidence job waits for
    them. Their manifest entry proves exactly their engine.
    """
    jobs = _jobs()
    job = jobs[job_name]
    assert str(job.get("if")).strip() == "${{ needs.changes.outputs.reuse != 'true' }}"
    assert "secrets." not in json.dumps(job), "the container job must not need a secret"
    assert not job.get("continue-on-error")
    runs = "\n".join(str(s.get("run") or "") for s in job["steps"])
    assert starts in runs
    written: set[str] = set()
    checker = None
    for step in job["steps"]:
        run = str(step.get("run") or "")
        assert not step.get("continue-on-error"), step.get("name")
        written |= set(re.findall(r'--junitxml="\$RUNNER_TEMP/live-junit/([\w.-]+)"', run))
        if f"check_live_skips.py --job {job_name}" in run:
            checker = step
    assert written and checker is not None
    fed = set(re.findall(r'--junit "\$RUNNER_TEMP/live-junit/([\w.-]+)"', str(checker["run"])))
    assert written == fed, (sorted(written), sorted(fed))
    assert str(checker.get("if")).replace(" ", "") == "${{!cancelled()}}"
    assert job_name in jobs["live-engine-evidence"]["needs"]
    # Never an opt-in exception in the gate: either required (fails on
    # anything but success, a web-only reuse skip aside) or not in it at all.
    gate = jobs["required-checks"]
    gate_run = "\n".join(str(st.get("run") or "") for st in gate["steps"])
    assert f'[ "$name" = "{job_name}" ]' not in gate_run
    if job_name in _REQUIRED_LIVE_JOBS:
        assert job_name in gate["needs"]
        assert f'"{job_name}:$' in gate_run
    spec = json.loads(MANIFEST.read_text(encoding="utf-8"))["jobs"][job_name]
    assert spec["proves"] == [engine] and spec["mode"] in {"strict", "engine_scoped"}
    assert not spec.get("emulated"), "a container of the real engine is not an emulator"
