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


def test_job_failure_fields_stamp_cursor_gap():
    from services.cdc_cursor_gap import CdcLsnGapError
    from src.transfer.engine import _job_failure_fields

    details, extras = _job_failure_fields(
        CdcLsnGapError("gap", resume_lsn="0a", min_lsn="0c", cursor_key="ck1")
    )
    assert extras.get("cdc_cursor_gap") is True
    assert extras.get("cdc_lease_cursor_key") == "ck1"
    assert details.get("code") == "cdc_lsn_gap"


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
