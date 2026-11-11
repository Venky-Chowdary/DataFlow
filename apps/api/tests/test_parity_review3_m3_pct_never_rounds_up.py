"""Re-review 3, M3: a share is rounded down and never reads 100% while a row differs.

``round(..., 4)`` in the API and ``:.2f`` in the export turned 1 missing row in
25,000 into "100.00%" (and 1 in 10,000,000 into 100.0 in the API).
"""

from __future__ import annotations

from services.parity_report import build_parity_report, floor_pct, public_report, share_pct
from services.parity_signoff import render_markdown


def _job(rows: int) -> dict:
    return {"_id": "m3", "status": "completed",
            "reconciliation": {"passed": False, "source_rows": rows, "target_rows": rows - 1,
                               "culprit_keys": {"missing_in_dest": ["42"]}},
            "destination_summary": {"table": "t", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"}}


def test_one_bad_row_in_ten_million_is_below_100_in_the_api():
    report = build_parity_report(_job(10_000_000), reread=False)
    assert report["parity"]["unexplained"] == 1
    assert report["tables"][0]["proven_equal_pct"] < 100
    assert report["parity"]["proven_equal_pct"] < 100


def test_the_export_never_reads_100_percent_with_a_bad_row():
    md = render_markdown(public_report(build_parity_report(_job(25_000), reread=False)), None)
    assert "100.00%" not in md
    assert "proven equal: 99.99%" in md


def test_shares_round_down():
    assert floor_pct(99.99996, short=False) == 99.9999
    assert share_pct(2, 3) == 66.6666
    assert share_pct(3, 3) == 100.0
