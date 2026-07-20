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
