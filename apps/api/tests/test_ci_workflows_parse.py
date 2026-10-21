"""Every GitHub Actions workflow must be valid YAML.

An unquoted ``- name: !important …`` parses as a YAML tag; GitHub then
rejects the whole file and every push run fails in 0s with no jobs.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

WORKFLOWS = sorted(
    (Path(__file__).resolve().parents[3] / ".github" / "workflows").glob("*.y*ml")
)


def test_workflows_found():
    assert WORKFLOWS, "no workflow files found"


@pytest.mark.parametrize("path", WORKFLOWS, ids=lambda p: p.name)
def test_workflow_is_valid_yaml_with_jobs(path: Path):
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(doc, dict) and isinstance(doc.get("jobs"), dict) and doc["jobs"]
    for job_id, job in doc["jobs"].items():
        for step in job.get("steps") or []:
            assert isinstance(step.get("name", ""), str), f"{job_id}: non-string step name"
