"""Finding fingerprints use present_cell_text, not str(value).

``str(True)`` invented ``True`` so dest ``true`` missed the sample token.
``if source_value`` dropped integer ``0``. Reader-null is not a fingerprint.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.decision_kernel.findings import (  # noqa: E402
    build_finding,
    fingerprint_value,
    findings_from_coercion_report,
)
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402


def test_fingerprint_true_shares_dest_true():
    assert fingerprint_value(True) == fingerprint_value("true")
    assert fingerprint_value(True) != fingerprint_value("True")
    assert fingerprint_value(0) == fingerprint_value("0")
    assert fingerprint_value(0) != fingerprint_value("")


def test_fingerprint_reader_null_is_empty():
    empty = fingerprint_value(None)
    assert fingerprint_value(SQL_NULL_SENTINEL) == empty
    assert fingerprint_value("") == empty


def test_build_finding_fingerprints_zero():
    finding = build_finding(
        source_column="n",
        target_column="n",
        failure_message="Invalid integer",
        source_value=0,  # type: ignore[arg-type]
    )
    assert finding.source_value_fingerprint == fingerprint_value(0)
    assert finding.source_value_fingerprint != ""


def test_findings_report_omits_sentinel_sample():
    report = {
        "columns": [
            {
                "source": "note",
                "target": "note",
                "severity": "block",
                "failed": 1,
                "sample_failures": [{"row": 1, "value": SQL_NULL_SENTINEL, "reason": "x"}],
            }
        ]
    }
    out = findings_from_coercion_report(report)
    assert out
    assert not out[0].get("source_value_fingerprint")
