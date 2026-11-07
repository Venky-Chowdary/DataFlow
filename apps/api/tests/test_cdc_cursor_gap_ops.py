"""CDC cursor gap + watermark clear — operator recovery proofs."""

from __future__ import annotations

import sys
from importlib import reload
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_humanize_cdc_lsn_gap():
    from services.cdc_cursor_gap import CdcLsnGapError
    from services.error_handling import humanize_transfer_failure

    exc = CdcLsnGapError(
        "resume before min_lsn",
        resume_lsn="0a",
        min_lsn="0b",
        cursor_key="mssql-cdc:db:dbo.orders",
    )
    h = humanize_transfer_failure(exc)
    assert h["code"] == "cdc_lsn_gap"
    assert h["confidence"] == "high"
    assert h["cursor_key"] == "mssql-cdc:db:dbo.orders"
    assert "watermark" in h["fix"].lower() or "snapshot" in h["fix"].lower()


def test_humanize_cdc_scn_gap():
    from services.cdc_cursor_gap import CdcScnGapError
    from services.error_handling import humanize_transfer_failure

    exc = CdcScnGapError(
        "resume before redo",
        resume_scn=50,
        oldest_scn=100,
        cursor_key="oracle-logminer:ORCL:APP.ORDERS",
    )
    h = humanize_transfer_failure(exc)
    assert h["code"] == "cdc_scn_gap"
    assert h["resume"] == "50"
    assert h["retained"] == "100"


def test_humanize_cdc_binlog_gap():
    from services.cdc_cursor_gap import CdcBinlogGapError
    from services.error_handling import humanize_transfer_failure

    exc = CdcBinlogGapError(
        "resume binlog purged",
        resume_file="mysql-bin.000001",
        resume_pos=4,
        oldest_file="mysql-bin.000009",
        cursor_key="mysql:db:orders",
    )
    h = humanize_transfer_failure(exc)
    assert h["code"] == "cdc_binlog_gap"
    assert h["dialect"] == "mysql"
    assert "000001" in h["resume"]
    assert "watermark" in h["fix"].lower() or "snapshot" in h["fix"].lower()


def test_humanize_cdc_slot_gap():
    from services.cdc_cursor_gap import CdcSlotGapError
    from services.error_handling import humanize_transfer_failure

    exc = CdcSlotGapError(
        "slot wal_status=lost",
        slot_name="df_orders",
        wal_status="lost",
        restart_lsn="0/100",
        confirmed_flush_lsn="0/200",
        cursor_key="pg:db:orders",
    )
    h = humanize_transfer_failure(exc)
    assert h["code"] == "cdc_slot_gap"
    assert h["dialect"] == "postgresql"
    assert h["cursor_key"] == "pg:db:orders"
    assert "snapshot" in h["fix"].lower()


def test_job_has_cursor_gap_includes_ct_code():
    from services.cdc_cursor_gap import GAP_ERROR_CODES, job_has_cursor_gap

    assert "cdc_ct_gap" in GAP_ERROR_CODES
    assert "cdc_oplog_gap" in GAP_ERROR_CODES
    assert job_has_cursor_gap({"error_code": "cdc_ct_gap"}) is True
    assert job_has_cursor_gap({"error_code": "cdc_oplog_gap"}) is True
    assert job_has_cursor_gap({"error_code": "cdc_slot_gap"}) is True
    assert job_has_cursor_gap({"error_code": "other"}) is False


def test_humanize_cdc_ct_gap():
    from services.cdc_cursor_gap import CdcCtGapError
    from services.error_handling import humanize_transfer_failure

    exc = CdcCtGapError(
        "last_sync_version before min_valid_version",
        resume_version=4,
        min_valid_version=10,
        cursor_key="mssql-ct:db:dbo.orders",
    )
    h = humanize_transfer_failure(exc)
    assert h["code"] == "cdc_ct_gap"
    assert h["dialect"] == "sqlserver"
    assert h["resume"] == "4"
    assert h["retained"] == "10"
    assert "snapshot" in h["fix"].lower() or "watermark" in h["fix"].lower()


def test_humanize_cdc_oplog_gap():
    from services.cdc_cursor_gap import CdcOplogGapError
    from services.error_handling import humanize_transfer_failure

    exc = CdcOplogGapError(
        "resume point may no longer be in the oplog",
        resume_unix=1_699_000_000,
        oldest_oplog_unix=1_700_000_000,
        cursor_key="mongodb:db:orders",
    )
    h = humanize_transfer_failure(exc)
    assert h["code"] == "cdc_oplog_gap"
    assert h["dialect"] == "mongodb"
    assert h["resume"] == "1699000000"
    assert "snapshot" in h["fix"].lower() or "watermark" in h["fix"].lower()


def test_job_failure_fields_stamp_cursor_gap():
    from services.cdc_cursor_gap import CdcLsnGapError
    from src.transfer.engine import _job_failure_fields

    details, extras = _job_failure_fields(
        CdcLsnGapError("gap", resume_lsn="0a", min_lsn="0c", cursor_key="ck1")
    )
    assert extras.get("cdc_cursor_gap") is True
    assert extras.get("cdc_lease_cursor_key") == "ck1"
    assert details.get("code") == "cdc_lsn_gap"


def test_job_failure_fields_stamp_snapshot_plan_on_refuse():
    from services.cdc_cursor_gap import CdcCursorGapError
    from src.transfer.engine import _job_failure_fields

    details, extras = _job_failure_fields(
        CdcCursorGapError(
            "gap refuse",
            dialect="mysql",
            resume="a",
            retained="b",
            cursor_key="ck2",
            snapshot_plan={
                "kind": "refuse",
                "snapshot_mode": "initial",
                "next_action": "set_when_needed",
                "lost_window": True,
            },
        )
    )
    assert extras.get("cdc_cursor_gap") is True
    assert extras.get("snapshot_plan", {}).get("kind") == "refuse"
    assert extras.get("snapshot_mode") == "initial"


def test_evaluate_resume_safety_allows_cursor_gap_without_checkpoint():
    from services.checkpoint_service import evaluate_resume_safety

    out = evaluate_resume_safety(
        None,
        job={"cdc_cursor_gap": True, "snapshot_mode": "when_needed", "status": "failed"},
    )
    assert out["ok"] is True
    assert out["gap_restart"] is True
    assert "not a checkpoint continuation" in " ".join(out["warnings"]).lower() or "gap" in out["honesty"].lower()


def test_clear_watermark_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    import services.platform_config as pc
    import services.sync_cursor as sc

    reload(pc)
    reload(sc)

    sc.set_watermark("ck-gap", "0a")
    assert sc.get_watermark("ck-gap") == "0a"
    out = sc.clear_watermark("ck-gap")
    assert out["cleared"] is True
    assert out["prior_watermark"] == "0a"
    assert sc.get_watermark("ck-gap") is None
    missing = sc.clear_watermark("ck-gap")
    assert missing["reason"] == "not_found"


def test_humanize_append_only_sink():
    from services.cdc_effectively_once import CdcAppendOnlySinkError
    from services.error_handling import humanize_transfer_failure

    h = humanize_transfer_failure(CdcAppendOnlySinkError("append blocked"))
    assert h["code"] == "cdc_append_only_sink"
    assert "Allow append-only" in h["fix"] or "upsert" in h["fix"].lower()


def test_change_stream_poll_raises_oplog_gap_before_watch():
    """Expired resume token must not open watch() at current clusterTime."""
    from unittest.mock import MagicMock, patch

    import pytest

    from connectors.mongodb_change_stream import MongodbChangeStreamCdc
    from services.cdc_cursor_gap import CdcOplogGapError
    from services.cdc_lease import configure_store, reset_store

    configure_store(backend="memory")
    seconds = 1_699_000_000
    oldest = 1_700_000_000
    token = {"_data": f"82{seconds:08x}00000001"}

    class _Ts:
        time = oldest

    oplog = MagicMock()
    oplog.find_one.return_value = {"ts": _Ts()}
    local = MagicMock()
    local.__getitem__.return_value = oplog
    coll = MagicMock()
    db = MagicMock()
    db.__getitem__ = MagicMock(return_value=coll)
    client = MagicMock()
    client.__getitem__ = MagicMock(return_value=db)
    client.local = local
    try:
        with patch("connectors.mongodb_change_stream._new_mongo_client", return_value=client):
            reader = MongodbChangeStreamCdc(
                {
                    "host": "localhost",
                    "database": "test",
                    "cursor_key": "ck-oplog-gap",
                    "lease_holder_id": "unit",
                },
                collection="orders",
                primary_key="_id",
                resume_token=token,
                max_wait_seconds=0.1,
            )
            with pytest.raises(CdcOplogGapError) as exc:
                list(reader.poll())
        assert exc.value.code == "cdc_oplog_gap"
        coll.watch.assert_not_called()
    finally:
        reset_store()


def test_change_stream_invalidate_raises_gap():
    from unittest.mock import MagicMock, patch

    import pytest

    from connectors.mongodb_change_stream import MongodbChangeStreamCdc
    from services.cdc_cursor_gap import CdcOplogGapError
    from services.cdc_lease import configure_store, reset_store

    configure_store(backend="memory")
    stream = MagicMock()
    stream.resume_token = {"_data": "invalidate-token"}
    stream.try_next.side_effect = [
        {"operationType": "invalidate", "ns": {"db": "test", "coll": "orders"}},
        None,
    ]
    coll = MagicMock()
    coll.watch.return_value.__enter__ = MagicMock(return_value=stream)
    coll.watch.return_value.__exit__ = MagicMock(return_value=False)
    db = MagicMock()
    db.__getitem__ = MagicMock(return_value=coll)
    client = MagicMock()
    client.__getitem__ = MagicMock(return_value=db)
    try:
        with patch("connectors.mongodb_change_stream._new_mongo_client", return_value=client):
            reader = MongodbChangeStreamCdc(
                {
                    "host": "localhost",
                    "database": "test",
                    "cursor_key": "ck-oplog-inv",
                    "lease_holder_id": "unit",
                },
                collection="orders",
                primary_key="_id",
                max_wait_seconds=0.5,
            )
            with pytest.raises(CdcOplogGapError) as exc:
                list(reader.poll())
        assert exc.value.retained == "invalidate"
    finally:
        reset_store()


def test_change_stream_history_lost_raises_gap():
    from unittest.mock import MagicMock, patch

    import pytest

    from connectors.mongodb_change_stream import MongodbChangeStreamCdc
    from services.cdc_cursor_gap import CdcOplogGapError
    from services.cdc_lease import configure_store, reset_store

    configure_store(backend="memory")

    class _Lost(Exception):
        code = 286
        codeName = "ChangeStreamHistoryLost"

    coll = MagicMock()
    coll.watch.side_effect = _Lost(
        "Resume of change stream was not possible, as the resume point may no longer be in the oplog"
    )
    db = MagicMock()
    db.__getitem__ = MagicMock(return_value=coll)
    client = MagicMock()
    client.__getitem__ = MagicMock(return_value=db)
    seconds = 1_700_000_000
    try:
        with patch("connectors.mongodb_change_stream._new_mongo_client", return_value=client):
            reader = MongodbChangeStreamCdc(
                {
                    "host": "localhost",
                    "database": "test",
                    "cursor_key": "ck-oplog-286",
                    "lease_holder_id": "unit",
                },
                collection="orders",
                primary_key="_id",
                resume_token={"_data": f"82{seconds:08x}00000001"},
                max_wait_seconds=0.1,
            )
            with pytest.raises(CdcOplogGapError) as exc:
                list(reader.poll())
        assert exc.value.code == "cdc_oplog_gap"
    finally:
        reset_store()
