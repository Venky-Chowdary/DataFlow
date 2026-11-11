"""Re-review 3, H2: a named cell is re-checked by that cell, not by any difference on its row.

The "cannot be re-checked" rule (contract 29) was per key: an explained
difference on another column of the key (CHAR padding) hid the cell the
evidence named, so the report read ``explained`` with sign-off open. Each
(key, kind, column) the evidence named is now re-checked on its own.
"""

from __future__ import annotations

import pytest

from services import parity_cache
from services import parity_report as pr


@pytest.fixture(autouse=True)
def _clean_cache():
    parity_cache.clear()
    yield
    parity_cache.clear()


def _job(code_type: str, recon_extra: dict | None = None, **extra) -> dict:
    job = {
        "_id": "h2", "status": "completed",
        "reconciliation": {"passed": False, "source_rows": 100, "target_rows": 100,
                           "culprit_keys": {"changed": ["7"]}, **(recon_extra or {})},
        "destination_summary": {"table": "t", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"},
        "transfer_request": {"mappings": [
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "note", "target": "note", "target_type": "TEXT"},
            {"source": "code", "target": "code", "target_type": code_type}]},
    }
    job.update(extra)
    return job


def _reread(note_dest: str = "hello"):
    def fake(job, ev, keys):
        return {"7": {"raw": {"id": "7", "note": "hello", "code": "ab"},
                      "expected": {"id": "7", "note": "hello", "code": "ab"},
                      "dest": {"id": "7", "note": note_dest, "code": "ab        "}, "held_out": False}}
    return fake


def test_explained_padding_does_not_hide_a_named_row(monkeypatch):
    monkeypatch.setattr(pr, "_bounded_reread", _reread())
    report = pr.build_parity_report(_job("CHAR(10)"), reread=True)
    assert report["verdict"] == "unexplained", report["verdict_reason"]
    assert any("cannot be re-checked" in d["cause"]["detail"] for d in report["differences"])
    assert report["signoff"]["status"] == "blocked"


def test_explained_padding_does_not_hide_a_named_cell(monkeypatch):
    monkeypatch.setattr(pr, "_bounded_reread", _reread())
    report = pr.build_parity_report(_job("CHAR(10)", {
        "culprit_keys": {},
        "sample_compare": {"rows_compared": 100, "mismatches": [
            {"key": "7", "source": "note", "target": "note", "source_value": "hello", "target_value": "HELLO"}]},
    }), reread=True)
    note = [d for d in report["differences"] if d["column"] == "note"]
    assert note and "cannot be re-checked" in note[0]["cause"]["detail"]
    assert report["verdict"] == "unexplained"


def test_a_named_cell_that_rereads_different_is_rechecked(monkeypatch):
    monkeypatch.setattr(pr, "_bounded_reread", _reread("HELLO"))
    report = pr.build_parity_report(_job("CHAR(10)"), reread=True)
    assert not any("cannot be re-checked" in d["cause"]["detail"] for d in report["differences"])
    assert {d["column"] for d in report["differences"] if not d["cause"]["explained"]} == {"note"}


def test_a_fix_rechecked_key_is_resolved_despite_padding(monkeypatch):
    monkeypatch.setattr(pr, "_bounded_reread", _reread())
    report = pr.build_parity_report(_job("CHAR(10)", parity={"version": 2, "resolved": {"t": ["7"]}}), reread=True)
    assert report["parity"]["unexplained"] == 0
