"""CDC slot catalog proof — active / restart_lsn / wal_status for Theater."""

from __future__ import annotations

from datetime import datetime, timezone

from src.transfer.cdc_transfer import _cdc_lag_fields
from src.transfer.engine import _CDC_JOB_FIELDS


def test_slot_catalog_status_parses_pg13_row(monkeypatch):
    from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc

    cdc = object.__new__(PostgreSqlChangeStreamCdc)
    cdc.slot_name = "df_slot_test"
    cdc.output_plugin = "pgoutput"
    cdc._slot_catalog_cache = None
    cdc._slot_catalog_cache_at = 0.0

    class _Cur:
        def __init__(self):
            self._n = 0

        def execute(self, sql, params=None):
            self._n += 1
            self._sql = sql

        def fetchone(self):
            return (True, "0/100", "0/200", "pgoutput", "reserved")

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class _Conn:
        def cursor(self):
            return _Cur()

        def commit(self):
            return None

        def rollback(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(cdc, "_conn", lambda: _Conn())
    status = cdc._slot_catalog_status(max_age_sec=0)
    assert status["slot_exists"] is True
    assert status["active"] is True
    assert status["restart_lsn"] == "0/100"
    assert status["confirmed_flush_lsn"] == "0/200"
    assert status["wal_status"] == "reserved"
    assert status["plugin"] == "pgoutput"

    # Cache hit — no second connection.
    cdc._conn = lambda: (_ for _ in ()).throw(RuntimeError("should use cache"))
    cached = cdc._slot_catalog_status(max_age_sec=30)
    assert cached["confirmed_flush_lsn"] == "0/200"


def test_cdc_metadata_includes_slot_catalog(monkeypatch):
    from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc

    cdc = object.__new__(PostgreSqlChangeStreamCdc)
    cdc.slot_name = "df_slot"
    cdc.output_plugin = "pgoutput"
    cdc.publication_name = "df_pub"
    cdc.phase = "streaming"
    cdc.consistent_point_lsn = "0/ABC"
    cdc._last_event_commit_at = None
    cdc._last_heartbeat_at = datetime.now(timezone.utc)
    cdc._lag_observation = {
        "cdc_lag_basis": "wal_bytes",
        "cdc_lag_seconds": 0.0,
        "cdc_heartbeat_age_sec": 0.1,
        "freshness_severity": "ok",
        "replication_lag_bytes": 1024,
    }

    class _Lease:
        def theater_fields(self):
            return {}

    cdc._lease = _Lease()
    monkeypatch.setattr(
        cdc,
        "_slot_catalog_status",
        lambda **kwargs: {
            "slot_exists": True,
            "active": True,
            "restart_lsn": "0/10",
            "confirmed_flush_lsn": "0/20",
            "wal_status": "reserved",
            "plugin": "pgoutput",
        },
    )
    monkeypatch.setattr(cdc, "replication_lag_seconds", lambda: 0.0)
    monkeypatch.setattr(cdc, "replication_lag_bytes", lambda: 1024)
    meta = cdc.cdc_metadata()
    assert meta["active"] is True
    assert meta["restart_lsn"] == "0/10"
    assert meta["confirmed_flush_lsn"] == "0/20"
    assert meta["wal_status"] == "reserved"


def test_cdc_lag_fields_promotes_slot_catalog():
    class _Fake:
        consistent_point_lsn = "0/MEM"
        _last_event_commit_at = None
        _last_heartbeat_at = None

        def cdc_metadata(self):
            return {
                "plugin": "pgoutput",
                "slot_name": "df_orders",
                "active": False,
                "slot_exists": True,
                "restart_lsn": "0/111",
                "confirmed_flush_lsn": "0/222",
                "wal_status": "lost",
                "replication_lag_bytes": 0,
                "replication_lag_seconds": 0.0,
                "cdc_lag_basis": "wal_bytes",
                "delivery": "at-least-once",
            }

        def replication_lag_bytes(self):
            return 0

        def replication_lag_seconds(self):
            return 0.0

    fields = _cdc_lag_fields(_Fake())
    assert fields["cdc_slot_active"] is False
    assert fields["cdc_slot_exists"] is True
    assert fields["cdc_restart_lsn"] == "0/111"
    assert fields["cdc_confirmed_flush_lsn"] == "0/222"  # live catalog wins over memory
    assert fields["cdc_wal_status"] == "lost"
    assert fields["cdc_freshness_severity"] == "critical"
    for key in (
        "cdc_slot_active",
        "cdc_slot_exists",
        "cdc_restart_lsn",
        "cdc_wal_status",
        "cdc_confirmed_flush_lsn",
    ):
        assert key in _CDC_JOB_FIELDS


def test_inactive_slot_warns_without_inventing_seconds():
    class _Fake:
        consistent_point_lsn = None
        _last_event_commit_at = None
        _last_heartbeat_at = datetime.now(timezone.utc)

        def cdc_metadata(self):
            return {
                "plugin": "pgoutput",
                "slot_name": "df_idle",
                "active": False,
                "slot_exists": True,
                "restart_lsn": "0/1",
                "confirmed_flush_lsn": "0/1",
                "wal_status": "reserved",
                "replication_lag_bytes": 4096,
                "delivery": "at-least-once",
            }

        def replication_lag_bytes(self):
            return 4096

        def replication_lag_seconds(self):
            return 0.0

    fields = _cdc_lag_fields(_Fake())
    assert fields["cdc_slot_active"] is False
    assert fields["cdc_lag_seconds"] == 0.0  # catch-up bytes
    assert fields.get("cdc_freshness_severity") == "warn"
