"""CI reuses backend checks only for a web-only change on top of a green backend.

.github/scripts/ci_backend_reuse.py decides; ci.yml's gate accepts a skipped
backend job only when that decision said reuse=true.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[3]
SCRIPT = REPO / ".github" / "scripts" / "ci_backend_reuse.py"


@pytest.fixture()
def reuse(monkeypatch):
    spec = importlib.util.spec_from_file_location("ci_backend_reuse", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "pull_request")
    return mod


def _world(mod, monkeypatch, *, changed, base_moved=(), green=("a1",)):
    history = ["a1", "a0"]

    def git(*args):
        if args[:2] == ("rev-list", "--parents"):
            return "M base head"
        if args == ("rev-parse", "HEAD^2"):
            return "head"
        if args == ("rev-parse", "HEAD^1"):
            return "base"
        if args[0] == "rev-list":
            return "\n".join(history)
        if args[0] == "merge-base":
            return "fork"
        if args[:2] == ("diff", "--name-only") and args[2] == "fork":
            return "\n".join(base_moved)
        if args[:2] == ("diff", "--name-only"):
            return "\n".join(changed)
        raise AssertionError(args)

    monkeypatch.setattr(mod, "_git", git)
    ok = {name: "success" for name in mod.BACKEND_CHECKS}
    monkeypatch.setattr(mod, "_check_runs", lambda _r, sha, _t: ok if sha in green else {})


def test_web_only_change_on_a_green_backend_reuses(reuse, monkeypatch):
    _world(reuse, monkeypatch, changed=["apps/web/src/pages/QueryPage.tsx", "apps/web/e2e/x.spec.ts-snapshots/a.png"])
    assert reuse.decide("o/r", "t")[:2] == (True, "a1")


@pytest.mark.parametrize("path", [
    "apps/api/services/x.py", ".github/workflows/ci.yml", "apps/api/data/connector_ci_evidence.json",
    "Dockerfile.api", "deploy/nginx.conf.template", "apps/api/README.md",
])
def test_any_backend_path_runs_everything(reuse, monkeypatch, path):
    _world(reuse, monkeypatch, changed=["apps/web/src/a.ts", path])
    assert reuse.decide("o/r", "t")[0] is False


def test_no_green_ancestor_runs_everything(reuse, monkeypatch):
    _world(reuse, monkeypatch, changed=["apps/web/src/a.ts"], green=())
    assert reuse.decide("o/r", "t")[0] is False


def test_backend_changes_on_the_base_branch_run_everything(reuse, monkeypatch):
    _world(reuse, monkeypatch, changed=["apps/web/src/a.ts"], base_moved=["apps/api/services/y.py"])
    assert reuse.decide("o/r", "t")[0] is False


def test_a_push_run_never_reuses(reuse, monkeypatch):
    monkeypatch.setenv("GITHUB_EVENT_NAME", "push")
    assert reuse.decide("o/r", "t")[0] is False


def test_the_gate_only_forgives_the_jobs_the_script_proves(reuse):
    wf = yaml.safe_load((REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))
    gate = wf["jobs"]["required-checks"]["steps"][0]["env"]["REUSABLE"].split()
    skippable = {name for name, job in wf["jobs"].items()
                 if "changes.outputs.reuse != 'true'" in str(job.get("if", "")) and name != "live-engine-evidence"}
    assert set(gate) == skippable
    # Every skippable job's check runs are among what the script requires green.
    names = " ".join(reuse.BACKEND_CHECKS)
    for job in skippable:
        label = {"api-image": "API image"}.get(job, job)
        assert label in names, job
    # Studio and plan-conflict UI jobs always run.
    for job in ("studio-ui", "studio-ui-next", "plan-conflict-e2e"):
        assert "changes" not in str(wf["jobs"][job].get("needs", "")), job
