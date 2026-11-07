"""D252 (privileged login): a lagging capture job is capture pending, not drift.

With ``sa`` the reader's own sp_cdc_scan is still refused while SQL Agent's
capture job owns the log reader (error 22903). The reader then took the
harvested max LSN as the capture head, read 0 changes, and the job either
blamed the login or reported plain drift. It must wait for a fresh agent scan,
or fail with an explicit capture-pending error.
"""
from __future__ import annotations

import contextlib
from datetime import datetime, timedelta, timezone

import pytest

from tests.test_r12_d252_cdc_harvest_refused import _run
from tests.test_cdc_drift_repair import _seed_image_repair_scenario

_AGENT_BUSY = (
    "Another connection with session ID 61 is already running 'sp_replcmds' "
    "for Change Data Capture in the current database. (22903)"
)


class _Cursor:
    def __init__(self, state):
        self.state = state
        self._row = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, *args):
        if "sp_cdc_scan" in sql:
            raise RuntimeError(_AGENT_BUSY)
        if "GETDATE" in sql:
            self._row = (self.state["now"],)
        elif "dm_cdc_log_scan_sessions" in sql:
            ends = self.state["scan_ends"]
            self._row = (ends.pop(0) if len(ends) > 1 else ends[0],)
        elif "fn_cdc_get_max_lsn" in sql or "max_lsn" in sql.lower():
            self._row = (b"\x00" * 9 + b"\x01",)

    def fetchone(self):
        return self._row


class _Conn:
    def __init__(self, state):
        self.state = state

    def cursor(self):
        return _Cursor(self.state)

    def commit(self):
        pass


def _reader(state, monkeypatch):
    from connectors import sqlserver_cdc_native as mod

    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    cdc = mod.SqlServerNativeCdc.__new__(mod.SqlServerNativeCdc)
    cdc.capture_instance = ""
    cdc._captures = {}
    cdc.cursor_key = "k"
    cdc._conn = lambda: contextlib.nullcontext(_Conn(state))
    cdc._max_lsn = lambda cur: "0x0000002F000021980003"
    return mod, cdc


def test_agent_owned_log_reader_is_not_a_login_refusal(monkeypatch):
    t0 = datetime(2026, 11, 3, 0, 40, tzinfo=timezone.utc)
    _mod, cdc = _reader({"now": t0, "scan_ends": [t0]}, monkeypatch)
    cdc.force_cdc_scan()
    assert cdc.capture_harvest_refused() == "capture_agent_owns_log_reader"


def test_head_waits_for_a_scan_that_began_after_the_check(monkeypatch):
    t0 = datetime(2026, 11, 3, 0, 40, tzinfo=timezone.utc)
    state = {
        "now": t0,
        # stale end, then the in-flight scan ending past t0, then a fresh one
        "scan_ends": [t0 - timedelta(seconds=30), t0 + timedelta(seconds=2),
                      t0 + timedelta(seconds=7)],
    }
    mod, cdc = _reader(state, monkeypatch)
    monkeypatch.setattr(mod, "_capture_wait_budget_sec", lambda: 30.0)
    assert cdc.capture_head_position() == "0000002f000021980003.c"
    assert cdc.capture_harvest_refused() is None


def test_head_reports_lagging_agent_when_no_fresh_scan_in_budget(monkeypatch):
    t0 = datetime(2026, 11, 3, 0, 40, tzinfo=timezone.utc)
    state = {"now": t0, "scan_ends": [t0 - timedelta(seconds=600)]}
    mod, cdc = _reader(state, monkeypatch)
    monkeypatch.setattr(mod, "_capture_wait_budget_sec", lambda: 0.0)
    cdc.capture_head_position()
    assert cdc.capture_harvest_refused() == "capture_agent_lagging"


@pytest.mark.parametrize("reader_read_nothing", [False, True])
def test_lagging_agent_fails_as_capture_pending_not_drift(
    tmp_path, monkeypatch, reader_read_nothing
):
    from services.cdc_drift_repair import CdcDriftRequiresFencedRepair

    source, destination, _cfg, _key, state = _seed_image_repair_scenario(
        tmp_path, monkeypatch, row_count=3, pending_count=2
    )
    fake = state["fake_cdc_class"]
    fake.capture_harvest_refused = lambda self: "capture_agent_lagging"
    if reader_read_nothing:
        # Live repro: 0 changes read, so the reader has no position to compare.
        fake.capture_reader_position = lambda self: None
    with pytest.raises(CdcDriftRequiresFencedRepair) as caught:
        _run(source, destination)
    evidence = caught.value.evidence
    assert evidence["capture_pending"] is True
    assert evidence["capture_pending_reason"] == "capture_agent_lagging"
    message = str(caught.value)
    assert message.startswith("Capture pending:")
    assert "not drift" in message
    assert "sp_cdc_scan" not in message  # a privileged login is not blamed
    assert "db_owner" not in message
    assert "sys.sp_cdc_help_jobs" in message
