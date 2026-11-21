"""CI security scanning: dependency audits, static analysis and a DAST baseline.

Pins what the ``security`` job gates on (pip-audit, npm audit of production
dependencies, bandit High) and that the OWASP ZAP baseline runs report-only:
it must not be a required check until its findings are triaged.
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[3]


def _jobs() -> dict:
    return yaml.safe_load((REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))["jobs"]


def _runs(job: dict) -> str:
    return "\n".join(str(step.get("run") or "") for step in job["steps"])


def test_security_job_gates_on_dependency_audits_and_bandit():
    runs = _runs(_jobs()["security"])
    assert "pip-audit --local" in runs
    assert "npm audit --omit=dev --audit-level=high" in runs
    assert "-lll" in runs and "bandit -c bandit.yaml" in runs


def test_npm_audit_step_is_gating():
    step = next(s for s in _jobs()["security"]["steps"] if "npm audit" in str(s.get("run") or ""))
    assert "|| true" not in step["run"]
    assert not step.get("continue-on-error")


def test_bandit_medium_findings_are_reported_not_gated():
    step = next(s for s in _jobs()["security"]["steps"] if "-ll -f json" in str(s.get("run") or ""))
    assert "|| true" in step["run"]


def test_zap_baseline_is_report_only_and_not_required():
    jobs = _jobs()
    dast = jobs["dast-baseline"]
    assert dast["continue-on-error"] is True
    runs = _runs(dast)
    assert "zap-baseline.py" in runs and " -I " in runs
    assert "DATAFLOW_REQUIRE_AUTH" in str(dast["steps"])
    assert "dast-baseline" not in jobs["required-checks"]["needs"]
