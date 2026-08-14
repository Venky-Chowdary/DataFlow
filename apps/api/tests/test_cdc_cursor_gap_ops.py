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
    assert job_has_cursor_gap({"error_code": "cdc_ct_gap"}) is True
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
