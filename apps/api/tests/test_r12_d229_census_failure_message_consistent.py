"""D229: a failed key census never also claims no value compare ran.

Production job 6ae8f8a14fb8a205cd02ecfe said "6 changed — value mismatch"
and, in the same message, "the only proof the route produced is
destination COUNT(*) … There is no source value digest". The census did
compare values; the count-only scope note belongs to verdicts where none ran.
"""

from pathlib import Path

import src.transfer.reconcile_step as rs
from tests.test_x41b_count_only_verdict import _run


def test_census_mismatch_message_names_the_compare_it_ran(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        rs, "_generic_key_census", lambda **_: ([2], [], [], None)
    )
    result = _run(tmp_path / "s.db", tmp_path / "d.db", digest=False, monkeypatch=monkeypatch)
    report = result.reconciliation or {}
    message = str(report.get("message") or result.error or "")
    assert not result.success
    assert report.get("verification_verdict") == "failed"
    assert "1 changed — value mismatch" in message
    assert "key census" in message
    assert "no source value digest" not in message
    assert "only proof the route produced is destination COUNT(*)" not in message
