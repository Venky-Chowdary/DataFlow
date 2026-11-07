"""``preflight.run.completed`` is emitted once per persisted preflight run.

It carries the verdict, gate counts and blocking gate ids — not table names,
blocker messages, or connection details.
"""

from __future__ import annotations

import logging

import pytest

from services import preflight_run_store
from services.logging_config import ContextFilter, JsonFormatter, TextFormatter

TABLE = "acct_secret_tbl_z9"


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []
        self.addFilter(ContextFilter())

    def emit(self, record: logging.LogRecord) -> None:
        if getattr(record, "event", "") == "preflight.run.completed":
            self.records.append(record)


@pytest.fixture
def capture(tmp_path, monkeypatch):
    monkeypatch.setattr(preflight_run_store, "STORE_PATH", tmp_path / "runs.jsonl")
    log = logging.getLogger("services.preflight_run_store")
    handler = _Capture()
    level = log.level
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)
    try:
        yield handler
    finally:
        log.removeHandler(handler)
        log.setLevel(level)


def _route() -> dict:
    return {"source_type": "mysql", "dest_type": "postgresql", "dest_table": TABLE}


def _rendered(record) -> str:
    return JsonFormatter().format(record) + TextFormatter().format(record)


def test_blocked_run_is_held_info_with_gate_ids(capture):
    result = {
        "passed": False, "passed_count": 7, "total_gates": 9,
        "blockers": [
            {"id": "g6_capacity", "message": f"{TABLE}.amount overflows"},
            {"id": "g9_identity", "message": f"duplicate key in {TABLE}"},
            {"id": "g6_capacity", "message": "second column"},
        ],
    }
    saved = preflight_run_store.save_preflight_run(result, validation_mode="strict", route=_route())
    (record,) = capture.records
    assert record.getMessage() == "preflight.run.completed"
    assert (record.outcome, record.levelno, record.verdict) == ("held", logging.INFO, "blocked")
    assert record.run_id == saved["run_id"]
    assert record.blocking_gate_ids == "g6_capacity,g9_identity"
    assert (record.blocker_count, record.passed_count, record.total_gates) == (3, 7, 9)
    assert (record.source_type, record.destination_type) == ("mysql", "postgresql")
    assert TABLE not in _rendered(record)


def test_passed_run_is_succeeded(capture):
    preflight_run_store.save_preflight_run(
        {"passed": True, "passed_count": 9, "total_gates": 9}, route=_route()
    )
    (record,) = capture.records
    assert (record.outcome, record.verdict, record.blocking_gate_ids, record.blocker_count) == (
        "succeeded", "passed", None, 0,
    )


def test_pilot_string_route_is_accepted(capture):
    """Pilot passes ``route="src->dst"``; the event must not break the save."""
    saved = preflight_run_store.save_preflight_run(
        {"passed": False, "blockers": ["not-a-dict", {"id": "g1"}]},
        source_label=f"conn.{TABLE}", route="mysql->postgresql",
    )
    assert saved["run_id"]
    (record,) = capture.records
    assert (record.source_type, record.destination_type, record.blocking_gate_ids) == (
        "mysql", "postgresql", "g1",
    )
    assert TABLE not in _rendered(record)
