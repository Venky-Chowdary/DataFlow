"""Re-review 4, finding 7 (Low-Medium): a multi-table run is never evaluated as one table.

Extra streams were read only when ``destination_summary.multi_stream`` was true, but the
multi-table CDC paths (``cdc_transfer.py``, the shared reader and the per-table loop) write
``streams`` without that flag: a 100-row gap on ``customers`` was ignored and the run read
``proven``. Two or more ``streams`` now make a run multi-table, flag or not.
"""

from __future__ import annotations

import pytest

from services.parity_report import build_parity_report

STREAMS = [{"name": "orders", "status": "completed", "row_accounting": {"source_count": 500, "dest_count": 500}},
           {"name": "customers", "status": "completed", "row_accounting": {"source_count": 1000, "dest_count": 900}}]


def _job(**dest) -> dict:
    return {"_id": "j", "status": "completed",
            "reconciliation": {"passed": True, "source_rows": 500, "target_rows": 500, "culprit_census_clean": True},
            "destination_summary": {"table": "orders", "primary_key_columns": ["id"], "sync_mode": "cdc", **dest}}


@pytest.mark.parametrize("flag", [{"multi_stream": True}, {}], ids=["flagged", "cdc_unflagged"])
def test_every_stream_of_a_multi_table_run_is_a_table(flag):
    report = build_parity_report(_job(streams=STREAMS, **flag), reread=False)
    assert [t["name"] for t in report["tables"]] == ["orders", "customers"]
    assert report["verdict"] == "unexplained", report["verdict_reason"]


def test_a_single_stream_entry_stays_one_table():
    report = build_parity_report(_job(streams=STREAMS[:1]), reread=False)
    assert [t["name"] for t in report["tables"]] == ["orders"]
    assert report["verdict"] == "proven"
