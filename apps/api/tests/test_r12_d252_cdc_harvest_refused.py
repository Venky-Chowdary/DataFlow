"""D252: a reader that cannot harvest the SQL Server log must not call lag drift.

A read-only CDC login cannot run sys.sp_cdc_scan. With the capture job
behind, a committed change is invisible to the reader, the capture head
equals the reader position, and the check used to report plain drift with
no hint that capture had not harvested the change.
"""
from __future__ import annotations

import contextlib

import pytest

from tests.test_cdc_drift_repair import _seed_image_repair_scenario


def _run(source, destination):
    from src.transfer.cdc_transfer import run_cdc_database_transfer

    run_cdc_database_transfer(
        source,
        destination,
        mappings=[
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "v", "target": "v", "confidence": 1.0},
        ],
        schema={"id": "string", "v": "string"},
        sync_mode="cdc",
        stream_contracts=[
            {
                "name": "orders",
                "selected": True,
                "primary_key": "id",
                "cursor_field": "id",
                "sync_mode": "cdc",
            }
        ],
        delivery_guarantee="exactly_once",
        delivery_pinned=True,
    )


def test_refused_harvest_reports_capture_pending_not_plain_drift(
    tmp_path, monkeypatch
):
    from services.cdc_drift_repair import CdcDriftRequiresFencedRepair

    source, destination, _cfg, _key, state = _seed_image_repair_scenario(
        tmp_path, monkeypatch, row_count=3, pending_count=2
    )
    state["fake_cdc_class"].capture_harvest_refused = (
        lambda self: "OperationalError"
    )
    with pytest.raises(CdcDriftRequiresFencedRepair) as caught:
        _run(source, destination)

    evidence = caught.value.evidence
    assert evidence["capture_pending"] is True
    assert evidence["capture_pending_reason"] == "harvest_unverified"
    message = str(caught.value)
    assert "sys.sp_cdc_scan" in message
    assert "sys.sp_cdc_help_jobs" in message
    assert "repair_cdc_drift" in message


def test_harvesting_reader_still_reports_real_drift(tmp_path, monkeypatch):
    from services.cdc_drift_repair import CdcDriftRequiresFencedRepair

    source, destination, _cfg, _key, state = _seed_image_repair_scenario(
        tmp_path, monkeypatch, row_count=3, pending_count=2
    )
    state["fake_cdc_class"].capture_harvest_refused = lambda self: None
    with pytest.raises(CdcDriftRequiresFencedRepair) as caught:
        _run(source, destination)
    assert caught.value.evidence["capture_pending"] is False
    assert "sp_cdc_help_jobs" not in str(caught.value)


class _Cursor:
    def __init__(self, refuse):
        self.refuse = refuse

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, *args):
        if self.refuse and "sp_cdc_scan" in sql:
            raise PermissionError("EXECUTE permission denied on sp_cdc_scan")


class _Conn:
    def __init__(self, refuse):
        self.refuse = refuse

    def cursor(self):
        return _Cursor(self.refuse)

    def commit(self):
        pass


def test_sqlserver_reader_records_a_refused_scan_and_clears_it():
    from connectors.sqlserver_cdc_native import SqlServerNativeCdc

    cdc = SqlServerNativeCdc.__new__(SqlServerNativeCdc)
    cdc.capture_instance = ""
    cdc._captures = {}
    cdc.cursor_key = "k"
    refuse = {"on": True}
    cdc._conn = lambda: contextlib.nullcontext(_Conn(refuse["on"]))

    cdc.force_cdc_scan()
    assert cdc.capture_harvest_refused() == "PermissionError"
    refuse["on"] = False
    cdc.force_cdc_scan()
    assert cdc.capture_harvest_refused() is None
