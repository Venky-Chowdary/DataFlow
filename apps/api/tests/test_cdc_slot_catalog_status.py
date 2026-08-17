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


def test_ensure_slot_refuses_to_recreate_lost_slot_on_resume():
    """Poll must not create a slot at current WAL — that skips the lost window."""
    import pytest

    from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc
    from services.cdc_cursor_gap import CdcSlotGapError

    cdc = object.__new__(PostgreSqlChangeStreamCdc)
    cdc.slot_name = "df_lost"
    cdc.output_plugin = "pgoutput"
    cdc.cursor_key = "pg:test:orders"
    cdc.consistent_point_lsn = "0/200"
    cdc._resume_expected = True
    cdc._slot_catalog_cache = {
        "slot_exists": True,
        "wal_status": "lost",
        "restart_lsn": "0/100",
        "confirmed_flush_lsn": "0/200",
    }
    cdc._slot_catalog_cache_at = 10**9
    cdc._acquire_cdc_lease = lambda: None

    with pytest.raises(CdcSlotGapError) as exc:
        cdc._ensure_slot(allow_create=False, recreate_if_lost=False)
    assert exc.value.code == "cdc_slot_gap"
    assert "lost" in str(exc.value).lower()


def test_ensure_slot_recreates_lost_slot_only_during_snapshot(monkeypatch):
    from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc

    cdc = object.__new__(PostgreSqlChangeStreamCdc)
    cdc.slot_name = "df_lost"
    cdc.output_plugin = "pgoutput"
    cdc.cursor_key = "pg:test:orders"
    cdc.consistent_point_lsn = "0/200"
    cdc._resume_expected = True
    cdc._slot_catalog_cache = {
        "slot_exists": True,
        "wal_status": "lost",
        "restart_lsn": "0/100",
        "confirmed_flush_lsn": "0/200",
    }
    cdc._slot_catalog_cache_at = 10**9
    cdc._acquire_cdc_lease = lambda: None
    calls = {"dropped": 0, "created": 0}

    def _drop():
        calls["dropped"] += 1
        cdc._slot_catalog_cache = None

    class _Cur:
        def execute(self, sql, params=None):
            if "pg_create_logical_replication_slot" in str(sql):
                calls["created"] += 1

        def fetchone(self):
            if calls["created"]:
                return ("0/999",)
            return None

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class _Conn:
        def cursor(self):
            return _Cur()

        def commit(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    cdc._drop_replication_slot = _drop
    monkeypatch.setattr(cdc, "_conn", lambda: _Conn())
    lsn = cdc._ensure_slot(allow_create=True, recreate_if_lost=True)
    assert calls["dropped"] == 1
    assert calls["created"] == 1
    assert cdc.consistent_point_lsn == "0/999"
