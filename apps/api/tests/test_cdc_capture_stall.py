"""SQL Server CDC capture-stall honesty — frozen max_lsn ≠ catch-up."""

from __future__ import annotations

from services.cdc_capture_stall import classify_mssql_capture_stall
from src.transfer.cdc_transfer import _cdc_lag_fields
from src.transfer.engine import _CDC_JOB_FIELDS


def test_classify_errors_critical():
    out = classify_mssql_capture_stall(error_count=1, dmv_available=True)
    assert out["capture_stall"] is True
    assert out["capture_stall_severity"] == "critical"


def test_classify_latency_warn_and_freeze_critical():
    warn = classify_mssql_capture_stall(
        scan_latency_sec=90.0,
        dmv_available=True,
        max_lsn="abc",
        frozen_for_sec=10.0,
    )
    assert warn["capture_stall"] is True
    assert warn["capture_stall_severity"] == "warn"

    crit = classify_mssql_capture_stall(
        scan_latency_sec=90.0,
        dmv_available=True,
        max_lsn="abc",
        frozen_for_sec=200.0,
    )
    assert crit["capture_stall_severity"] == "critical"


def test_classify_idle_frozen_tip_not_stall():
    # Idle source: max_lsn frozen, latency low/zero — not a stall.
    out = classify_mssql_capture_stall(
        max_lsn="abc",
        frozen_for_sec=600.0,
        scan_latency_sec=0.0,
        error_count=0,
        dmv_available=True,
    )
    assert out["capture_stall"] is False


def test_classify_dmv_unavailable_unknown():
    out = classify_mssql_capture_stall(dmv_available=False)
    assert out["capture_stall"] is False
    assert out["capture_stall_severity"] == "unknown"


def test_catalog_dmv_errors_set_stall(monkeypatch):
    from connectors.sqlserver_cdc_native import SqlServerNativeCdc

    cdc = object.__new__(SqlServerNativeCdc)
    cdc.capture_instance = "dbo_orders"
    cdc.start_lsn = "0000002a00000001000000ff"
    cdc.cursor_key = "ck"
    cdc.table = "orders"
    cdc.tables = ["orders"]
    cdc._shared = False
    cdc._captures = {"orders": "dbo_orders"}
    cdc._capture_catalog_cache = None
    cdc._capture_catalog_cache_at = 0.0
    cdc._stall_max_lsn = "0000002a00000001000000ff"
    cdc._stall_max_lsn_at = 0.0  # frozen a long time once monotonic advances

    class _Lease:
        acquired = True

    cdc._lease = _Lease()

    class _Cur:
        def execute(self, sql, params=None):
            self._sql = " ".join(str(sql).split())

        def fetchone(self):
            s = self._sql.upper()
            if "FN_CDC_GET_MIN_LSN" in s:
                return (bytes.fromhex("0000002a0000000100000001"),)
            if "FN_CDC_GET_MAX_LSN" in s:
                return (bytes.fromhex("0000002a00000001000000ff"),)
            if "DM_CDC_LOG_SCAN" in s:
                # latency, empty_scan_count, error_count, failed_sessions, last_commit
                return (120.0, 5, 1, 0, bytes.fromhex("0000002a00000001000000ff"))
            if "MAP_LSN_TO_TIME" in s:
                from datetime import datetime, timezone

                return (datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc),)
            return None

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class _Conn:
        def cursor(self):
            return _Cur()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(cdc, "_conn", lambda: _Conn())
    monkeypatch.setattr(cdc, "_resolve_all_captures", lambda cur: {"orders": "dbo_orders"})
    status = cdc._capture_catalog_status(max_age_sec=0)
    assert status["capture_stall"] is True
    assert status["capture_stall_severity"] == "critical"
    assert status["max_lsn_time"]


def test_lag_fields_clear_false_catchup_on_stall():
    class _Fake:
        consistent_point_lsn = None
        _last_event_commit_at = None
        _last_heartbeat_at = None

        def cdc_metadata(self):
            return {
                "plugin": "sqlserver_native_cdc",
                "slot_name": "dbo_orders",
                "active": True,
                "slot_exists": True,
                "min_lsn": "0a",
                "max_lsn": "0f",
                "restart_lsn": "0a",
                "confirmed_flush_lsn": "0f",
                "wal_status": "reserved",
                "capture_stall": True,
                "capture_stall_severity": "critical",
                "capture_stall_reason": "CDC log scan errors",
                "capture_latency_seconds": 120.0,
                "max_lsn_time": "2026-08-07T12:00:00+00:00",
                "replication_lag_bytes": 0,
                "replication_lag_seconds": 0.0,
                "cdc_lag_basis": "wal_bytes",
                "freshness_severity": "critical",
                "delivery": "at-least-once",
            }

        def replication_lag_bytes(self):
            return 0

        def replication_lag_seconds(self):
            return 0.0

    fields = _cdc_lag_fields(_Fake())
    assert fields["cdc_capture_stall"] is True
    assert fields["cdc_freshness_severity"] == "critical"
    assert fields["cdc_lag_seconds"] is None  # cleared false catch-up
    assert fields["cdc_lag_basis"] == "capture_scan"
    assert fields["cdc_max_lsn"] == "0f"
    assert fields["cdc_capture_latency_seconds"] == 120.0
    for key in (
        "cdc_capture_stall",
        "cdc_capture_stall_reason",
        "cdc_capture_latency_seconds",
        "cdc_max_lsn_time",
    ):
        assert key in _CDC_JOB_FIELDS
