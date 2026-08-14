"""CDC retention probe classification proofs (no network mocks)."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_classify_lsn_ok_at_risk_gap():
    from services.cdc_retention_probe import classify_lsn_retention

    ok = classify_lsn_retention("0c", "0a")
    assert ok.status == "ok"
    assert ok.resume == "0c"
    assert ok.retained == "0a"

    edge = classify_lsn_retention("0a", "0a")
    assert edge.status == "at_risk"

    gap = classify_lsn_retention("0a", "0b", cursor_key="ck1")
    assert gap.status == "gap"
    assert gap.cursor_key == "ck1"
    assert "re-snapshot" in gap.message.lower() or "reset" in gap.message.lower()


def test_classify_lsn_no_watermark():
    from services.cdc_retention_probe import classify_lsn_retention

    r = classify_lsn_retention("", "0a")
    assert r.status == "no_watermark"


def test_classify_scn_ok_at_risk_gap():
    from services.cdc_retention_probe import classify_scn_retention

    ok = classify_scn_retention(50_000, 1_000, at_risk_headroom=10_000)
    assert ok.status == "ok"

    risk = classify_scn_retention(5_000, 1_000, at_risk_headroom=10_000)
    assert risk.status == "at_risk"
    assert risk.details.get("headroom") == 4_000

    gap = classify_scn_retention(50, 100)
    assert gap.status == "gap"


def test_resume_lsn_from_token_json():
    from connectors.sqlserver_cdc_native import encode_mssql_cdc_token
    from services.cdc_retention_probe import _resume_lsn_from_watermark

    token = encode_mssql_cdc_token("0abc", table="orders", phase="streaming")
    assert _resume_lsn_from_watermark(token) == "0abc"
    assert _resume_lsn_from_watermark("0def") == "0def"


def test_job_fields_shape():
    from services.cdc_retention_probe import classify_lsn_retention

    fields = classify_lsn_retention("0a", "0b").job_fields()
    assert fields["cdc_retention_status"] == "gap"
    assert fields["cdc_retention_resume"] == "0a"
    assert fields["cdc_retention_retained"] == "0b"


def test_synthetic_gap_then_clear_roundtrip(tmp_path, monkeypatch):
    """Prove probe sees gap on a fabricated watermark, then clears to no_watermark."""
    from services import sync_cursor as sc

    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")

    from connectors.sqlserver_cdc_native import encode_mssql_cdc_token
    from services.cdc_retention_probe import classify_lsn_retention, _resume_lsn_from_watermark
    from services.sync_cursor import clear_watermark, get_watermark, set_watermark

    ck = "test:retention:gap"
    token = encode_mssql_cdc_token("0a", table="t", phase="streaming")
    set_watermark(ck, token)
    resume = _resume_lsn_from_watermark(get_watermark(ck))
    probe = classify_lsn_retention(resume, "0b", cursor_key=ck)
    assert probe.status == "gap"

    clear_watermark(ck)
    assert get_watermark(ck) is None
    probe2 = classify_lsn_retention(_resume_lsn_from_watermark(None), "0b", cursor_key=ck)
    assert probe2.status == "no_watermark"


def test_classify_pg_slot_lost_and_missing_are_gaps():
    from services.cdc_retention_probe import classify_pg_slot_retention

    lost = classify_pg_slot_retention(
        slot_exists=True,
        wal_status="lost",
        restart_lsn="0/100",
        confirmed_flush_lsn="0/200",
        watermark="slot=df_x|phase=streaming|lsn=0/200",
        slot_name="df_x",
        cursor_key="ck",
    )
    assert lost.status == "gap"
    assert lost.dialect == "postgresql"
    assert "lost window" in lost.message.lower() or "wal_status=lost" in lost.message

    missing = classify_pg_slot_retention(
        slot_exists=False,
        watermark="slot=df_x|phase=streaming|lsn=0/200",
        slot_name="df_x",
    )
    assert missing.status == "gap"
    assert missing.retained == "slot_missing"

    fresh = classify_pg_slot_retention(slot_exists=False, watermark=None, slot_name="df_x")
    assert fresh.status == "no_watermark"

    reserved = classify_pg_slot_retention(
        slot_exists=True, wal_status="reserved", restart_lsn="0/1", watermark="0/1"
    )
    assert reserved.status == "ok"

    risk = classify_pg_slot_retention(
        slot_exists=True, wal_status="unreserved", restart_lsn="0/1", watermark="0/1"
    )
    assert risk.status == "at_risk"

    pg12 = classify_pg_slot_retention(
        slot_exists=True, wal_status="", restart_lsn="0/1", watermark="0/1"
    )
    assert pg12.status == "ok"


def test_when_needed_pg_slot_lost_is_blocking_snapshot():
    from services.cdc_retention_probe import classify_pg_slot_retention
    from services.cdc_snapshot_mode import KIND_BLOCKING, classify_snapshot_plan, SnapshotMode

    probe = classify_pg_slot_retention(
        slot_exists=True,
        wal_status="lost",
        confirmed_flush_lsn="0/200",
        watermark="0/200",
        slot_name="df_x",
    )
    plan = classify_snapshot_plan(
        SnapshotMode.WHEN_NEEDED, watermark="0/200", retention_status=probe.status
    )
    assert plan["kind"] == KIND_BLOCKING
    assert plan["lost_window"] is True
    assert plan["run_snapshot"] is True


def test_attach_cdc_retention_uses_pg_slot_catalog():
    from types import SimpleNamespace

    from services.cdc_retention_probe import attach_cdc_retention

    cdc = SimpleNamespace(
        table="orders",
        cursor_key="pg:db:orders",
        slot_name="df_orders",
        consistent_point_lsn="0/200",
        _resume_expected=True,
        _slot_catalog_status=lambda max_age_sec=0: {
            "slot_exists": True,
            "wal_status": "lost",
            "restart_lsn": "0/100",
            "confirmed_flush_lsn": "0/200",
        },
    )
    probe = attach_cdc_retention(cdc, {"type": "postgresql", "database": "db"}, table="orders")
    assert probe is not None
    assert probe.status == "gap"
    assert cdc._cdc_retention.status == "gap"


def test_classify_ct_version_ok_at_risk_gap():
    from services.cdc_retention_probe import classify_ct_version_retention

    ok = classify_ct_version_retention(20, 5, current_version=21)
    assert ok.status == "ok"
    assert ok.resume == "20"
    assert ok.retained == "5"
    assert ok.details.get("plugin") == "sqlserver_change_tracking"

    edge = classify_ct_version_retention(5, 5)
    assert edge.status == "at_risk"

    gap = classify_ct_version_retention(4, 5, cursor_key="ck-ct")
    assert gap.status == "gap"
    assert gap.cursor_key == "ck-ct"
    assert "changetable" in gap.message.lower()
    assert "reinitialize" in gap.message.lower() or "snapshot" in gap.message.lower()

    fresh = classify_ct_version_retention(None, 5)
    assert fresh.status == "no_watermark"

    disabled = classify_ct_version_retention(9, None, ct_enabled=False)
    assert disabled.status == "gap"
    assert disabled.retained == "ct_disabled"

    unknown = classify_ct_version_retention(9, None, ct_enabled=True)
    assert unknown.status == "unknown"


def test_when_needed_ct_gap_is_blocking_snapshot():
    from services.cdc_retention_probe import classify_ct_version_retention
    from services.cdc_snapshot_mode import KIND_BLOCKING, classify_snapshot_plan, SnapshotMode

    probe = classify_ct_version_retention(3, 10)
    plan = classify_snapshot_plan(
        SnapshotMode.WHEN_NEEDED, watermark="3", retention_status=probe.status
    )
    assert plan["kind"] == KIND_BLOCKING
    assert plan["lost_window"] is True
    assert plan["run_snapshot"] is True


def test_attach_cdc_retention_uses_ct_catalog():
    from types import SimpleNamespace

    from services.cdc_retention_probe import attach_cdc_retention

    cdc = SimpleNamespace(
        table="orders",
        schema="dbo",
        cursor_key="mssql-ct:db:dbo.orders",
        version=4,
        phase="streaming",
        _ct_catalog_status=lambda max_age_sec=0: {
            "plugin": "sqlserver_change_tracking",
            "ct_enabled": True,
            "min_valid_version": 10,
            "current_version": 12,
            "resume_version": 4,
        },
    )
    probe = attach_cdc_retention(cdc, {"type": "sqlserver", "database": "db"}, table="orders")
    assert probe is not None
    assert probe.status == "gap"
    assert probe.resume == "4"
    assert probe.retained == "10"
    assert cdc._cdc_retention.status == "gap"


def test_attach_ct_catalog_unreachable_is_unknown_not_gap():
    """A down SQL Server is not a CHANGE_RETENTION gap — do not snapshot-skip WAL."""
    from types import SimpleNamespace

    from services.cdc_retention_probe import attach_cdc_retention

    cdc = SimpleNamespace(
        table="orders",
        schema="dbo",
        cursor_key="mssql-ct:db:dbo.orders",
        version=4,
        phase="streaming",
        _ct_catalog_status=lambda max_age_sec=0: {
            "plugin": "sqlserver_change_tracking",
            "ct_enabled": None,
            "min_valid_version": None,
            "current_version": None,
            "resume_version": 4,
            "error": "connection refused",
        },
    )
    probe = attach_cdc_retention(cdc, {"type": "sqlserver", "database": "db"}, table="orders")
    assert probe is not None
    assert probe.status == "unknown"


def test_resume_ct_version_from_streaming_token():
    from connectors.sqlserver_change_stream import encode_sqlserver_resume_token
    from services.cdc_retention_probe import _resume_ct_version_from_watermark

    token = encode_sqlserver_resume_token(42, table="orders", phase="streaming")
    assert _resume_ct_version_from_watermark(token) == 42
    snap = encode_sqlserver_resume_token(42, table="orders", phase="snapshot", offset=10)
    assert _resume_ct_version_from_watermark(snap) is None
    assert _resume_ct_version_from_watermark(None) is None
    assert _resume_ct_version_from_watermark("mssql-ct:orders:7") == 7


def test_resume_token_unix_seconds_decodes_v1_timestamp():
    from services.cdc_retention_probe import resume_token_unix_seconds

    seconds = 1_700_000_000
    token = {"_data": f"82{seconds:08x}00000001"}
    assert resume_token_unix_seconds(token) == seconds
    assert resume_token_unix_seconds(f"mongo:82{seconds:08x}00000001") == seconds
    assert resume_token_unix_seconds({"phase": "snapshot", "token": token}) == seconds
    assert resume_token_unix_seconds({"phase": "streaming", "offset": 0, "collection": "orders"}) is None
    assert resume_token_unix_seconds({"_data": "not-hex"}) is None
    assert resume_token_unix_seconds(None) is None


def test_classify_mongo_oplog_ok_at_risk_gap():
    from services.cdc_retention_probe import classify_mongo_oplog_retention

    ok = classify_mongo_oplog_retention(1_700_100_000, 1_700_000_000, newest_oplog_unix=1_700_200_000)
    assert ok.status == "ok"
    assert ok.details.get("plugin") == "mongodb_change_stream"

    edge = classify_mongo_oplog_retention(1_700_000_000, 1_700_000_000)
    assert edge.status == "at_risk"

    gap = classify_mongo_oplog_retention(1_699_000_000, 1_700_000_000, cursor_key="ck-mongo")
    assert gap.status == "gap"
    assert gap.cursor_key == "ck-mongo"
    assert "clusterTime" in gap.message or "oplog" in gap.message.lower()

    lost = classify_mongo_oplog_retention(1_700_000_000, None, history_lost=True)
    assert lost.status == "gap"
    assert lost.retained == "oplog_purged"

    inv = classify_mongo_oplog_retention(None, None, invalidated=True)
    assert inv.status == "gap"
    assert inv.retained == "invalidate"

    fresh = classify_mongo_oplog_retention(None, 1_700_000_000)
    assert fresh.status == "no_watermark"

    unknown = classify_mongo_oplog_retention(1_700_000_000, None)
    assert unknown.status == "unknown"


def test_when_needed_mongo_oplog_gap_is_blocking_snapshot():
    from services.cdc_retention_probe import classify_mongo_oplog_retention
    from services.cdc_snapshot_mode import KIND_BLOCKING, classify_snapshot_plan, SnapshotMode

    probe = classify_mongo_oplog_retention(1_699_000_000, 1_700_000_000)
    plan = classify_snapshot_plan(
        SnapshotMode.WHEN_NEEDED, watermark="82", retention_status=probe.status
    )
    assert plan["kind"] == KIND_BLOCKING
    assert plan["lost_window"] is True
    assert plan["run_snapshot"] is True


def test_attach_cdc_retention_uses_mongo_oplog_catalog():
    from types import SimpleNamespace

    from services.cdc_retention_probe import attach_cdc_retention

    cdc = SimpleNamespace(
        collection="orders",
        cursor_key="mongodb:db:orders",
        _oplog_catalog_status=lambda max_age_sec=0: {
            "plugin": "mongodb_change_stream",
            "resume_unix": 1_699_000_000,
            "oldest_oplog_unix": 1_700_000_000,
            "newest_oplog_unix": 1_700_100_000,
        },
    )
    probe = attach_cdc_retention(cdc, {"type": "mongodb", "database": "db"}, table="orders")
    assert probe is not None
    assert probe.status == "gap"
    assert probe.resume == "1699000000"
    assert cdc._cdc_retention.status == "gap"


def test_attach_mongo_catalog_unreachable_is_unknown_not_gap():
    from types import SimpleNamespace

    from services.cdc_retention_probe import attach_cdc_retention

    cdc = SimpleNamespace(
        collection="orders",
        cursor_key="mongodb:db:orders",
        _oplog_catalog_status=lambda max_age_sec=0: {
            "plugin": "mongodb_change_stream",
            "resume_unix": 1_700_000_000,
            "oldest_oplog_unix": None,
            "newest_oplog_unix": None,
            "error": "not authorized on local",
        },
    )
    probe = attach_cdc_retention(cdc, {"type": "mongodb"}, table="orders")
    assert probe is not None
    assert probe.status == "unknown"
