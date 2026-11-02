"""MySQL binlog catalog proof — retention gap fail-closed (PG slot parity)."""

from __future__ import annotations

from datetime import datetime, timezone

from services.cdc_cursor_gap import CdcBinlogGapError
from services.cdc_retention_probe import classify_binlog_retention
from src.transfer.cdc_transfer import _cdc_lag_fields
from src.transfer.engine import _CDC_JOB_FIELDS


def test_classify_binlog_retention_gap_missing_file():
    result = classify_binlog_retention(
        "mysql-bin.000001",
        4,
        ["mysql-bin.000003", "mysql-bin.000004"],
        cursor_key="mysql:db:t",
    )
    assert result.status == "gap"
    assert "000001" in result.resume


def test_classify_binlog_retention_at_risk_on_oldest():
    result = classify_binlog_retention(
        "mysql-bin.000003",
        100,
        ["mysql-bin.000003", "mysql-bin.000004"],
    )
    assert result.status == "at_risk"


def test_classify_binlog_retention_ok_and_gtid_purged():
    ok = classify_binlog_retention(
        "mysql-bin.000004",
        50,
        ["mysql-bin.000003", "mysql-bin.000004"],
    )
    assert ok.status == "ok"
    purged = classify_binlog_retention(
        "",
        None,
        ["mysql-bin.000004"],
        resume_gtid="uuid:1-10",
        gtid_purged="uuid:1-5",
        gtid_in_purged=True,
    )
    assert purged.status == "gap"


def test_binlog_catalog_status_parses_logs(monkeypatch):
    from connectors.mysql_change_stream import MySqlChangeStreamCdc

    cdc = object.__new__(MySqlChangeStreamCdc)
    cdc.cfg = {}
    cdc.database = "db"
    cdc.tables = ["orders"]
    cdc.table = "orders"
    cdc.cursor_key = "mysql:db:orders"
    cdc.resume_token = {"file": "mysql-bin.000002", "pos": 120}
    cdc._binlog_catalog_cache = None
    cdc._binlog_catalog_cache_at = 0.0
    cdc._mysql_server_id = lambda: 12345

    class _Lease:
        acquired = True

    cdc._lease = _Lease()

    class _Cur:
        def __init__(self):
            self._sql = ""

        def execute(self, sql, params=None):
            self._sql = " ".join(str(sql).split())

        def fetchone(self):
            s = self._sql.upper()
            if "LIKE 'LOG_BIN'" in s:
                return ("log_bin", "ON")
            if "LIKE 'BINLOG_FORMAT'" in s:
                return ("binlog_format", "ROW")
            if "LIKE 'BINLOG_ROW_IMAGE'" in s:
                return ("binlog_row_image", "FULL")
            if "LIKE 'BINLOG_EXPIRE" in s:
                return ("binlog_expire_logs_seconds", "259200")
            if "LIKE 'EXPIRE_LOGS_DAYS'" in s:
                return ("expire_logs_days", "0")
            if "SHOW MASTER STATUS" in s or "SHOW BINARY LOG STATUS" in s:
                return ("mysql-bin.000003", 999)
            if "GTID_PURGED" in s and "GTID_SUBSET" not in s:
                return ("",)
            if "GTID_EXECUTED" in s:
                return ("uuid:1-100",)
            if "GTID_SUBSET" in s:
                return (0,)
            return None

        def fetchall(self):
            if "SHOW BINARY LOGS" in self._sql.upper():
                return [("mysql-bin.000002", 1024), ("mysql-bin.000003", 2048)]
            return []

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class _Conn:
        def cursor(self):
            return _Cur()

        def close(self):
            return None

    monkeypatch.setattr(cdc, "_conn", lambda: _Conn())
    status = cdc._binlog_catalog_status(max_age_sec=0)
    assert status["slot_exists"] is True
    assert status["oldest_file"] == "mysql-bin.000002"
    assert status["restart_lsn"] == "mysql-bin.000002:4"
    assert status["confirmed_flush_lsn"] == "mysql-bin.000002:120"
    assert status["wal_status"] == "unreserved"  # on oldest → at_risk
    assert status["retention_status"] == "at_risk"

    cdc._conn = lambda: (_ for _ in ()).throw(RuntimeError("should use cache"))
    cached = cdc._binlog_catalog_status(max_age_sec=30)
    assert cached["confirmed_flush_lsn"] == "mysql-bin.000002:120"


def test_assert_resume_raises_binlog_gap(monkeypatch):
    from connectors.mysql_change_stream import MySqlChangeStreamCdc

    cdc = object.__new__(MySqlChangeStreamCdc)
    cdc.cursor_key = "mysql:db:orders"
    cdc.resume_token = {"file": "mysql-bin.000001", "pos": 4}
    monkeypatch.setattr(
        cdc,
        "_binlog_catalog_status",
        lambda **kwargs: {
            "retention_status": "gap",
            "wal_status": "lost",
            "confirmed_flush_lsn": "mysql-bin.000001:4",
            "oldest_file": "mysql-bin.000009",
            "gtid_purged": "",
        },
    )
    try:
        cdc._assert_resume_within_retention()
        assert False, "expected CdcBinlogGapError"
    except CdcBinlogGapError as exc:
        assert exc.code == "cdc_binlog_gap"
        assert "000001" in exc.resume


def test_cdc_metadata_and_lag_fields_promote_binlog(monkeypatch):
    from connectors.mysql_change_stream import MySqlChangeStreamCdc

    cdc = object.__new__(MySqlChangeStreamCdc)
    cdc.resume_token = {"file": "mysql-bin.000010", "pos": 1}
    cdc._last_event_commit_at = None
    cdc._last_heartbeat_at = datetime.now(timezone.utc)
    cdc._lag_observation = {
        "cdc_lag_basis": "wal_bytes",
        "cdc_lag_seconds": 0.0,
        "cdc_heartbeat_age_sec": 0.1,
        "freshness_severity": "ok",
        "replication_lag_bytes": 512,
    }

    class _Lease:
        acquired = True

        def theater_fields(self):
            return {"cdc_lease_holder": "h1"}

    cdc._lease = _Lease()
    monkeypatch.setattr(
        cdc,
        "_binlog_catalog_status",
        lambda **kwargs: {
            "plugin": "mysql-binlog",
            "slot_exists": True,
            "active": True,
            "restart_lsn": "mysql-bin.000009:4",
            "confirmed_flush_lsn": "mysql-bin.000010:1",
            "wal_status": "lost",
            "server_id": 42,
            "gtid_purged": "",
            "retention_status": "gap",
            "binlog_expire_logs_seconds": 86400,
        },
    )
    monkeypatch.setattr(cdc, "replication_lag_seconds", lambda: 0.0)
    monkeypatch.setattr(cdc, "replication_lag_bytes", lambda: 512)
    monkeypatch.setattr(cdc, "_mysql_server_id", lambda: 42)

    meta = cdc.cdc_metadata()
    assert meta["plugin"] == "mysql-binlog"
    assert meta["wal_status"] == "lost"
    assert meta["slot_name"] == "server_id:42"

    fields = _cdc_lag_fields(cdc)
    assert fields["cdc_wal_status"] == "lost"
    assert fields["cdc_freshness_severity"] == "critical"
    assert "mysql_binlog" in str(fields.get("cdc_lag_unknown_reason") or "")
    for key in (
        "cdc_slot_active",
        "cdc_slot_exists",
        "cdc_restart_lsn",
        "cdc_wal_status",
        "cdc_confirmed_flush_lsn",
    ):
        assert key in _CDC_JOB_FIELDS
