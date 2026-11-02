"""SQL Server capture catalog proof — min_lsn / max_lsn for Theater."""

from __future__ import annotations

from src.transfer.cdc_transfer import _cdc_lag_fields
from src.transfer.engine import _CDC_JOB_FIELDS


def test_capture_catalog_status_parses_min_max(monkeypatch):
    from connectors.sqlserver_cdc_native import SqlServerNativeCdc

    cdc = object.__new__(SqlServerNativeCdc)
    cdc.capture_instance = "dbo_orders"
    cdc.start_lsn = "0000002a0000000100000001"
    cdc.cursor_key = "mssql-cdc:db:dbo.orders"
    cdc.table = "orders"
    cdc.tables = ["orders"]
    cdc._shared = False
    cdc._captures = {"orders": "dbo_orders"}
    cdc._capture_catalog_cache = None
    cdc._capture_catalog_cache_at = 0.0

    class _Lease:
        acquired = True

    cdc._lease = _Lease()

    class _Cur:
        def execute(self, sql, params=None):
            self._sql = " ".join(str(sql).split())
            self._params = params

        def fetchone(self):
            s = self._sql.upper()
            if "FN_CDC_GET_MIN_LSN" in s:
                return (bytes.fromhex("0000002a0000000100000001"),)
            if "FN_CDC_GET_MAX_LSN" in s:
                return (bytes.fromhex("0000002a00000001000000ff"),)
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
    assert status["capture_exists"] is True
    assert status["min_lsn"]
    assert status["max_lsn"]
    assert status["restart_lsn"] == status["min_lsn"]
    assert status["confirmed_flush_lsn"]
    assert status["wal_status"] == "unreserved"  # resume == min → at_risk
    assert status["retention_status"] == "at_risk"
    assert status["capture_instance"] == "dbo_orders"

    cdc._conn = lambda: (_ for _ in ()).throw(RuntimeError("should use cache"))
    cached = cdc._capture_catalog_status(max_age_sec=30)
    assert cached["max_lsn"] == status["max_lsn"]


def test_capture_catalog_gap_wal_lost(monkeypatch):
    from connectors.sqlserver_cdc_native import SqlServerNativeCdc

    cdc = object.__new__(SqlServerNativeCdc)
    cdc.capture_instance = "dbo_orders"
    cdc.start_lsn = "000000100000000100000001"  # before min
    cdc.cursor_key = "ck"
    cdc.table = "orders"
    cdc.tables = ["orders"]
    cdc._shared = False
    cdc._captures = {"orders": "dbo_orders"}
    cdc._capture_catalog_cache = None
    cdc._capture_catalog_cache_at = 0.0

    class _Lease:
        acquired = False

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
    assert status["wal_status"] == "lost"
    assert status["retention_status"] == "gap"


def test_cdc_metadata_and_lag_fields_promote_capture(monkeypatch):
    from connectors.sqlserver_cdc_native import SqlServerNativeCdc

    cdc = object.__new__(SqlServerNativeCdc)
    cdc.phase = "streaming"
    cdc.capture_instance = "dbo_orders"
    cdc.start_lsn = "0000002a0000000100000010"
    cdc.tables = ["orders"]
    cdc.table = "orders"
    cdc._shared = False
    cdc._captures = {"orders": "dbo_orders"}
    cdc.row_filter = "all"

    class _Lease:
        acquired = True

        def theater_fields(self):
            return {"cdc_lease_holder": "worker-1"}

    cdc._lease = _Lease()
    monkeypatch.setattr(
        cdc,
        "_capture_catalog_status",
        lambda **kwargs: {
            "plugin": "sqlserver_native_cdc",
            "slot_exists": True,
            "capture_exists": True,
            "active": True,
            "capture_instance": "dbo_orders",
            "min_lsn": "0000002a0000000100000001",
            "max_lsn": "0000002a00000001000000ff",
            "restart_lsn": "0000002a0000000100000001",
            "confirmed_flush_lsn": "0000002a0000000100000010",
            "wal_status": "lost",
            "retention_status": "gap",
            "captures": {"orders": "dbo_orders"},
        },
    )

    meta = cdc.cdc_metadata()
    assert meta["min_lsn"] == "0000002a0000000100000001"
    assert meta["max_lsn"] == "0000002a00000001000000ff"
    assert meta["wal_status"] == "lost"
    assert meta["slot_name"] == "dbo_orders"

    fields = _cdc_lag_fields(cdc)
    assert fields["cdc_min_lsn"] == "0000002a0000000100000001"
    assert fields["cdc_max_lsn"] == "0000002a00000001000000ff"
    assert fields["cdc_capture_instance"] == "dbo_orders"
    assert fields["cdc_restart_lsn"] == "0000002a0000000100000001"
    assert fields["cdc_confirmed_flush_lsn"] == "0000002a0000000100000010"
    assert fields["cdc_wal_status"] == "lost"
    assert fields["cdc_freshness_severity"] == "critical"
    assert "sqlserver_cdc" in str(fields.get("cdc_lag_unknown_reason") or "")
    for key in (
        "cdc_min_lsn",
        "cdc_max_lsn",
        "cdc_capture_instance",
        "cdc_wal_status",
        "cdc_restart_lsn",
    ):
        assert key in _CDC_JOB_FIELDS


def test_capture_ok_reserved(monkeypatch):
    from connectors.sqlserver_cdc_native import SqlServerNativeCdc

    cdc = object.__new__(SqlServerNativeCdc)
    cdc.capture_instance = "dbo_orders"
    cdc.start_lsn = "0000002a0000000100000010"  # after min
    cdc.cursor_key = "ck"
    cdc.table = "orders"
    cdc.tables = ["orders"]
    cdc._shared = False
    cdc._captures = {"orders": "dbo_orders"}
    cdc._capture_catalog_cache = None
    cdc._capture_catalog_cache_at = 0.0

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
    assert status["wal_status"] == "reserved"
    assert status["retention_status"] == "ok"
