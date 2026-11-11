"""Re-review finding 14 (LOW): float rule, bound rounding, the tool's link and basis.

* ``float_precision`` was explained when only the destination was a float:
  BIGINT 9007199254740993 written to a DOUBLE as ...992 read "explained", and
  a NUMERIC(12,3) written to a REAL compared equal under the REAL fingerprint.
  Only a float *source* explains binary rounding.
* ``format_rate_pct`` rounded half-up, so "at most 0.14%" was stated for a
  0.1444% bound; an upper bound is rounded up.
* The Pilot tool linked ``/jobs/{id}?tab=trust``, which the web app does not
  route, and answered from the stored evidence while the page re-read the
  keys: it now links the real deep link and says which basis it used.
"""

from __future__ import annotations

import pytest

from services.parity_classifier import CellContext, classify_value
from services.parity_stats import format_rate_pct


def test_an_integer_source_rounded_by_a_double_column_is_unexplained():
    found = classify_value(expected="9007199254740993", dest="9007199254740992",
                           ctx=CellContext(source_type="bigint", dest_type="double precision"))
    assert found is not None and found["code"] == "float_precision" and found["explained"] is False


def test_a_decimal_source_rounded_by_a_real_column_is_a_difference():
    found = classify_value(expected="123456789.123", dest="123456792",
                           ctx=CellContext(source_type="numeric(12,3)", dest_type="real"))
    assert found is not None and found["explained"] is False


def test_a_float_source_keeps_its_explained_rounding():
    found = classify_value(expected="0.1000000000000000055511151231257827", dest="0.1",
                           ctx=CellContext(source_type="float8", dest_type="double precision"))
    assert found is None or found["explained"] is True


@pytest.mark.parametrize("rate,text", [
    (0.001444, "0.15%"), (0.0014967, "0.15%"), (0.0015, "0.15%"), (0.0299949, "3.00%"),
    (0.00009994, "0.01%"), (0.0000012, "0.00012%"), (0.0, "0.00%"), (1.0, "100.00%"),
])
def test_an_upper_bound_is_rounded_up(rate, text):
    assert format_rate_pct(rate) == text


def test_the_tool_links_the_routed_trust_report(monkeypatch):
    from src.ai.copilot import parity_tools

    assert parity_tools.trust_report_link("job 1") == "/#/jobs?jobId=job+1&panel=trust-report"


def test_the_tool_uses_the_pages_recent_reread_and_says_so(monkeypatch):
    from services import parity_cache
    from services import parity_report as pr
    from src.ai.copilot import lifecycle_tools, parity_tools

    parity_cache.clear()
    job = {
        "_id": "tool-basis", "status": "completed", "updated_at": "2026-11-11T00:00:00+00:00",
        "reconciliation": {"passed": False, "source_rows": 100, "target_rows": 100, "culprit_keys": {"changed": ["7"]}},
        "destination_summary": {"table": "flags", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"},
        "transfer_request": {"mappings": [{"source": "id", "target": "id"},
                                          {"source": "flag", "target": "flag", "target_type": "VARCHAR(10)"}]},
    }
    monkeypatch.setattr(lifecycle_tools, "resolve_job", lambda job_id, selector: (job, None))
    reads: list[str] = []

    def fake(job_, ev, keys):
        reads.append("read")
        return {"7": {"raw": {"id": "7", "flag": "TRUE"}, "expected": {"id": "7", "flag": "TRUE"},
                      "dest": {"id": "7", "flag": "true"}, "held_out": False}}

    monkeypatch.setattr(pr, "_bounded_reread", fake)
    stored = parity_tools.explain_parity(job_id="tool-basis").output
    assert reads == [] and "stored evidence only" in stored["basis"]  # the tool never reads a database itself
    page = pr.build_parity_report(job, cache="use")  # the Trust Report page reads
    assert reads == ["read"]
    tool = parity_tools.explain_parity(job_id="tool-basis").output
    assert reads == ["read"], "the tool re-used the page's read"
    assert "live re-read" in tool["basis"]
    assert tool["summary"]["verdict"] == page["verdict"]
    assert tool["open"] == "/#/jobs?jobId=tool-basis&panel=trust-report"
    parity_cache.clear()
