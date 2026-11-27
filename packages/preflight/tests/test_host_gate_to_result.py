"""_host_gate_to_result: how a hosted gate's answer becomes a package GateResult.

Review round 3 #9 / round 4 #3: no status or an unknown one proved nothing
(an unmeasured skip that says why); a host that reports error / fail refused
the check (a block, never a skip); a host skip that names no reason gains one.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

_PREFLIGHT_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(_PREFLIGHT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PREFLIGHT_ROOT))

import pytest  # noqa: E402

from preflight.gates import _host_gate_to_result  # noqa: E402
from preflight.models import GateId, GateStatus  # noqa: E402

GATE = GateId.G13_SOURCE_COVERAGE


def _run(payload):
    return _host_gate_to_result(GATE, payload, time.perf_counter())


def test_this_is_the_package_under_test():
    import preflight

    assert Path(preflight.__file__).resolve().is_relative_to(_PREFLIGHT_ROOT.resolve())


@pytest.mark.parametrize(("payload", "reason"), [
    ({}, "host_status_missing"),
    ({"status": "", "details": {"applicable": False}}, "host_status_missing"),
    ({"status": "maybe", "details": {"applicable": False}}, "host_status_unknown"),
])
def test_no_known_status_is_an_unmeasured_skip_without_the_hosts_details(payload, reason):
    result = _run(payload)
    assert result.status == GateStatus.SKIP
    assert result.details["unmeasured"] is True
    assert result.details["remediation_kind"] == "retry_validate"
    assert result.details["skip_reason"] == reason
    assert "applicable" not in result.details
    assert result.message.startswith("This check could not run")


@pytest.mark.parametrize("raw", ["error", "ERROR", "fail", "Failed"])
def test_a_host_error_or_failure_is_a_block(raw):
    result = _run({"status": raw, "message": "coverage probe crashed", "details": {"x": 1}})
    assert result.status == GateStatus.BLOCK
    assert result.message == "coverage probe crashed"
    assert result.details == {"x": 1, "host_status": raw.lower()}
    assert _run({"status": raw}).message == "This check failed in the service that runs it."


@pytest.mark.parametrize("details", [None, {}, {"note": "nothing to say"}])
def test_a_host_skip_without_a_reason_gains_one(details):
    result = _run({"status": "skip", "message": "", "details": details})
    assert result.status == GateStatus.SKIP
    assert result.details["unmeasured"] is True
    assert result.details["skip_reason"] == "host_skip_unexplained"
    assert result.message


def test_a_host_skip_that_says_why_and_known_statuses_pass_through():
    declared = _run({"status": "skip", "message": "No foreign keys", "details": {"declared": False}})
    assert declared.status == GateStatus.SKIP and declared.details == {"declared": False}
    assert declared.message == "No foreign keys"
    for raw, want in (("pass", GateStatus.PASS), ("PASS", GateStatus.PASS), ("warn", GateStatus.WARN), ("block", GateStatus.BLOCK)):
        result = _run({"status": raw, "message": "m", "details": {"k": "v"}})
        assert result.status == want and result.message == "m" and result.details == {"k": "v"}
