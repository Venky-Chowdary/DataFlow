"""Re-review finding 2 (HIGH): a re-read must not clear keys Gate-8 named.

``same_value`` compared every typed cell with ``fingerprint_for_reconcile``,
whose date and boolean coercions apply to TEXT/VARCHAR too: ``TRUE`` -> ``true``
in VARCHAR(10) and ``2024-01-02`` -> ``2024-01-02 00:00:00`` in TEXT read equal,
so the keys Gate-8 named were cleared and the report read ``proven``. On a
character column the text is the value; and an evidence-named key whose
re-read shows no difference can no longer be re-checked — it is never
"resolved" by that alone.
"""

from __future__ import annotations

import pytest

from services import parity_report as pr
from services.parity_classifier import CellContext, classify_value, same_value


@pytest.mark.parametrize("left,right,ddl", [
    ("TRUE", "true", "VARCHAR(10)"),
    ("2024-01-02", "2024-01-02 00:00:00", "TEXT"),
    ("0001/1/1 ", "0001/1/1", "TEXT"),
    ("2024-01-02T10:00:00+02:00", "2024-01-02T08:00:00+00:00", "TEXT"),
    ("1.50", "1.5", "VARCHAR(10)"),
])
def test_character_columns_compare_exact_text(left, right, ddl):
    assert same_value(left, right, CellContext(dest_type=ddl)) is False
    assert classify_value(expected=left, dest=right, ctx=CellContext(dest_type=ddl)) is not None


def test_typed_columns_keep_the_gate8_rule():
    assert same_value("TRUE", "1", CellContext(dest_type="BOOLEAN")) is True
    assert same_value("2024-01-02", "2024-01-02 00:00:00", CellContext(dest_type="DATE")) is True


def test_padding_on_text_is_whitespace():
    found = classify_value(expected="0001/1/1 ", dest="0001/1/1", ctx=CellContext(dest_type="TEXT"))
    assert found is not None and found["code"] == "whitespace" and found["explained"] is False


def _job(**extra) -> dict:
    job = {
        "_id": "j", "status": "completed",
        "reconciliation": {"passed": False, "source_rows": 100, "target_rows": 100, "culprit_keys": {"changed": ["7"]}},
        "destination_summary": {"table": "flags", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"},
        "transfer_request": {"mappings": [
            {"source": "id", "target": "id"},
            {"source": "flag", "target": "flag", "target_type": "VARCHAR(10)"},
            {"source": "d", "target": "d", "target_type": "TEXT"},
        ]},
    }
    job.update(extra)
    return job


def _reread_returning(dest: dict):
    def fake(job, ev, keys):
        return {"7": {"raw": {"id": "7", "flag": "TRUE", "d": "2024-01-02"},
                      "expected": {"id": "7", "flag": "TRUE", "d": "2024-01-02"},
                      "dest": dest, "held_out": False}}
    return fake


def test_reread_of_coerced_text_keeps_the_differences(monkeypatch):
    monkeypatch.setattr(pr, "_bounded_reread", _reread_returning({"id": "7", "flag": "true", "d": "2024-01-02 00:00:00"}))
    report = pr.build_parity_report(_job(), reread=True)
    assert report["verdict"] == "unexplained", report["verdict_reason"]
    assert {d["column"] for d in report["differences"] if not d["cause"]["explained"]} == {"flag", "d"}


def test_evidence_named_key_that_reads_equal_cannot_be_rechecked(monkeypatch):
    monkeypatch.setattr(pr, "_bounded_reread", _reread_returning({"id": "7", "flag": "TRUE", "d": "2024-01-02"}))
    report = pr.build_parity_report(_job(), reread=True)
    assert report["verdict"] == "unexplained", report["verdict_reason"]
    [diff] = [d for d in report["differences"] if "7" in d["keys_sample"]]
    assert diff["cause"]["explained"] is False and "cannot be re-checked" in diff["cause"]["detail"]
    assert report["signoff"]["status"] == "blocked"


def test_a_key_a_fix_recheck_resolved_reads_proven(monkeypatch):
    monkeypatch.setattr(pr, "_bounded_reread", _reread_returning({"id": "7", "flag": "TRUE", "d": "2024-01-02"}))
    report = pr.build_parity_report(_job(parity={"version": 3, "resolved": {"flags": ["7"]}}), reread=True)
    assert report["verdict"] == "proven", report["verdict_reason"]
    # The same holds for the keys a fix is re-checking right now (its settle round).
    report = pr.build_parity_report(_job(), reread=True, recheck={"flags": ["7"]})
    assert report["verdict"] == "proven", report["verdict_reason"]
