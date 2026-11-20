"""The CI BigQuery emulator gets a fresh process per BigQuery test module.

goccy/go-googlesql#10: the emulator's wasm SQL parser fails every query once its
memory passes 2 GiB, so a long shard failed whichever BigQuery test ran last
(PR #433, shard 2). See tests/helpers/bigquery_emulator_lifecycle.py.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from tests.helpers import bigquery_emulator_lifecycle as life


@pytest.fixture
def docker(monkeypatch):
    calls: list[list[str]] = []

    def run(argv, **_kw):
        calls.append(list(argv))
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr(life, "_last_module", None)
    monkeypatch.setattr(life.shutil, "which", lambda _name: "/usr/bin/docker")
    monkeypatch.setattr(life, "_port_open", lambda: True)
    monkeypatch.setattr(life, "_serving", lambda _t: True)
    monkeypatch.setattr(life.subprocess, "run", run)
    return calls


def test_nothing_restarts_unless_ci_names_the_container(docker, monkeypatch):
    monkeypatch.delenv("DATAFLOW_CI_BIGQUERY_CONTAINER", raising=False)
    life.before_test("tests/test_x.py::test_bigquery_case")
    assert docker == []


def test_one_restart_per_bigquery_module_and_none_for_other_tests(docker, monkeypatch):
    monkeypatch.setenv("DATAFLOW_CI_BIGQUERY_CONTAINER", "dataflow-bigquery-emulator")
    life.before_test("tests/test_a.py::test_postgres_only")
    life.before_test("tests/test_a.py::test_to_bigquery[1]")
    life.before_test("tests/test_a.py::test_to_bigquery[2]")
    life.before_test("tests/test_b.py::test_bigquery_emulator_case")
    assert docker == [
        ["docker", "restart", "--time", "5", "dataflow-bigquery-emulator"],
        ["docker", "restart", "--time", "5", "dataflow-bigquery-emulator"],
    ]


def test_no_emulator_running_means_no_restart(docker, monkeypatch):
    monkeypatch.setenv("DATAFLOW_CI_BIGQUERY_CONTAINER", "dataflow-bigquery-emulator")
    monkeypatch.setattr(life, "_port_open", lambda: False)
    life.before_test("tests/test_a.py::test_bigquery")
    assert docker == []


def test_a_failed_restart_or_an_emulator_that_never_serves_fails_loudly(docker, monkeypatch):
    monkeypatch.setenv("DATAFLOW_CI_BIGQUERY_CONTAINER", "dataflow-bigquery-emulator")
    monkeypatch.setattr(
        life.subprocess, "run",
        lambda *_a, **_k: SimpleNamespace(returncode=1, stderr="No such container"),
    )
    with pytest.raises(RuntimeError, match="could not restart dataflow-bigquery-emulator: No such container"):
        life.before_test("tests/test_a.py::test_bigquery")

    monkeypatch.setattr(life.subprocess, "run", lambda *_a, **_k: SimpleNamespace(returncode=0, stderr=""))
    monkeypatch.setattr(life, "_serving", lambda _t: False)
    with pytest.raises(RuntimeError, match="did not serve the tests' dataset"):
        life.before_test("tests/test_b.py::test_bigquery")


def test_the_ci_settings_name_the_container_the_start_script_runs():
    from pathlib import Path

    root = Path(__file__).resolve().parents[3]
    assert "DATAFLOW_CI_BIGQUERY_CONTAINER=dataflow-bigquery-emulator" in (
        root / ".github" / "ci-live-engines.vars"
    ).read_text(encoding="utf-8")
    script = (root / ".github" / "scripts" / "start_ci_databases.sh").read_text(encoding="utf-8")
    assert "--name dataflow-bigquery-emulator --restart unless-stopped" in script
    assert "--database=" in script
