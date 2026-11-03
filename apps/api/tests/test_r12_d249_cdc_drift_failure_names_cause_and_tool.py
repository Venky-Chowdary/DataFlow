"""D249/D252: a CDC drift failure names the mismatch, the table and repair_cdc_drift.

The job said only "Source capture is pending; Re-copy the evidenced keys with
the confirmed repair action" when 30 rows had landed with truncated
microseconds: the headline was a side condition, and neither the tool nor the
table was named.
"""

from __future__ import annotations

import pytest

from services.cdc_drift_repair import CdcDriftRequiresFencedRepair
from src.transfer.job_failure import _job_failure_fields

EVIDENCE = {
    "code": "cdc_drift_requires_fenced_repair",
    "cursor_key": "pg:source:qaenv_seed",
    "source_table": "qaenv_seed",
    "dest_table": "mx_pg_ms_cdc",
    "pk_fields": ["id"],
    "drift_count": 30,
    "keys": [{"key_id": f"{i:064x}", "display": {"id": "••••"}} for i in range(30)],
}


@pytest.mark.parametrize("pending", [False, True])
def test_drift_failure_names_count_table_and_tool(pending):
    evidence = {**EVIDENCE, "capture_pending": pending, "capture_pending_reason": "head_not_reached"}
    details, extras = _job_failure_fields(CdcDriftRequiresFencedRepair(evidence))
    for message in (details["message"], extras["operator_error"]):
        assert message.startswith("CDC verification found 30 rows in mx_pg_ms_cdc"), message
        assert "repair_cdc_drift" in message
        assert "(evidenced by id)" in message
        assert ("Capture is pending" in message) is pending


def test_exception_text_matches_operator_message():
    exc = CdcDriftRequiresFencedRepair({**EVIDENCE, "drift_count": 1, "keys": EVIDENCE["keys"][:1]})
    assert str(exc).startswith("CDC verification found 1 row in mx_pg_ms_cdc")
    assert "repair_cdc_drift" in str(exc)
