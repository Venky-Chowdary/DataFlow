"""A cadence must stop after a failure that cannot change by itself.

From the field: 48 of a customer's 50 Jobs rows were the same schedule, the same
route, failing every hour with the same verdict. Two paths let that happen — a
schedule whose connector no longer resolves recorded a failed run and never
parked, and the park rule only fired when the *wording* of two failures matched,
so anything that varied its text ground on forever.

Parking never hides the failure: the run is still recorded failed, and the
schedule holds one finding with the corrective action until a human answers it.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import services.schedule_runner as runner  # noqa: E402
from services.failure_retry_policy import DETERMINISTIC, TRANSIENT  # noqa: E402


class _Sched:
    def __init__(self, history: list[dict]) -> None:
        self.run_history = history
        self.id = "sched-1"
        self.enabled = True


def _failed(error: str) -> dict:
    return {"status": "failed", "error": error}


def _transient(retry: bool = False) -> dict:
    return {"retry": retry, "reason": "", "failure_class": {"kind": TRANSIENT}}


def test_identical_verdict_twice_parks() -> None:
    sched = _Sched([_failed("connection refused on host db-1")])
    assert (
        runner._park_reason(
            sched, _transient(), _failed("connection refused on host db-1")
        )
        == "identical_failure_repeated"
    )


def test_third_failure_parks_even_when_the_wording_changes() -> None:
    """The ceiling must not depend on two error strings matching."""
    sched = _Sched(
        [_failed("connection refused on host db-1"), _failed("read timeout on db-2")]
    )
    assert (
        runner._park_reason(sched, _transient(), _failed("TLS handshake failed on db-3"))
        == "consecutive_failures"
    )


def test_a_success_resets_the_ceiling() -> None:
    sched = _Sched(
        [
            _failed("read timeout on db-2"),
            {"status": "completed"},
            _failed("TLS handshake failed on db-3"),
        ]
    )
    assert runner._park_reason(sched, _transient(), _failed("quota exceeded")) == ""


def test_a_gate_refusal_parks_on_its_first_beat() -> None:
    sched = _Sched([])
    decision = {"retry": False, "reason": "", "failure_class": {"kind": DETERMINISTIC}}
    assert (
        runner._park_reason(sched, decision, _failed("Gate G6 refused the run"))
        == "deterministic_refusal"
    )


def test_a_missing_connector_parks_on_one_finding(monkeypatch) -> None:
    """Previously this recorded a failed run per beat and never parked."""
    parked: dict[str, object] = {}

    monkeypatch.setattr(
        runner, "_resolve_connector", lambda _cid: None, raising=True
    )
    monkeypatch.setattr(
        runner,
        "_park_on_decision",
        lambda sid, exc, **kw: parked.update(schedule_id=sid, message=str(exc)),
        raising=True,
    )

    class _S:
        id = "sched-1"
        enabled = True
        source_connector_id = "gone"
        dest_connector_id = "gone"

    monkeypatch.setattr(
        "services.schedule_store.get_schedule", lambda _sid: _S(), raising=True
    )

    assert runner._dispatch_transfer("sched-1") is None
    assert parked["schedule_id"] == "sched-1"
    # The operator is told which side to fix, not just that a beat failed.
    assert "connector is missing" in str(parked["message"])
    assert "Re-select" in str(parked["message"])
