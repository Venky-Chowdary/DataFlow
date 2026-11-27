"""Every CI step that depends on another step, or reads another step's log, finds it in its job.

REVIEW_ORACLE finding 10: two steps were pasted into the secrets-based
cdc-oracle job. The deny probe was gated on ``steps.python_ready``, which
that job does not define, so it never ran (a skipped step reads as fine), and
"Failing Oracle test ids" grepped ``$RUNNER_TEMP/oracle-*.log`` that no step
of that job writes.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[3] / ".github" / "workflows" / "ci.yml"
_STEP_REF = re.compile(r"steps\.([A-Za-z0-9_-]+)\.")
_LOG_READ = re.compile(r'"?\$RUNNER_TEMP"?/([A-Za-z0-9_.-]*)\*\.log')
_LOG_WRITE = re.compile(r'(?:tee|>)\s+(?:-a\s+)?"?\$RUNNER_TEMP"?/([A-Za-z0-9_.${}-]+\.log)')


def _jobs() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"]


@pytest.mark.parametrize("job_name", sorted(_jobs()))
def test_step_conditions_name_steps_of_their_own_job(job_name):
    steps = _jobs()[job_name].get("steps") or []
    defined: set[str] = set()
    for step in steps:
        for ref in _STEP_REF.findall(str(step.get("if") or "")):
            assert ref in defined, f"{job_name}: step {step.get('name')!r} depends on undefined step {ref!r}"
        if step.get("id"):
            defined.add(str(step["id"]))


@pytest.mark.parametrize("job_name", sorted(_jobs()))
def test_log_globs_read_logs_their_job_writes(job_name):
    steps = _jobs()[job_name].get("steps") or []
    written = [w for s in steps for w in _LOG_WRITE.findall(str(s.get("run") or ""))]
    for step in steps:
        for prefix in _LOG_READ.findall(str(step.get("run") or "")):
            assert any(w.startswith(prefix) for w in written), (
                f"{job_name}: step {step.get('name')!r} reads $RUNNER_TEMP/{prefix}*.log, "
                f"which no step of the job writes ({written})"
            )


def test_cdc_oracle_runs_no_container_only_step():
    names = [str(s.get("name") or "") for s in _jobs()["cdc-oracle"]["steps"]]
    assert not any("SELECT-only login" in n for n in names), names
