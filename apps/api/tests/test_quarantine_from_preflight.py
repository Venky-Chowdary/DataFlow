"""Preflight findings must be inspectable as quarantine rows."""

from __future__ import annotations

from services.quarantine_from_preflight import merge_job_quarantine, quarantine_rows_from_preflight


def test_encoding_findings_become_quarantine_rows():
    pf = {
        "passed": False,
        "gates": [
            {
                "id": "g5_dry_run",
                "status": "block",
                "message": "Dry-run / integrity failed",
                "details": {
                    "encoding_issues": [
                        {
                            "column": "description",
                            "row": 3,
                            "message": "format-control character detected (U+200B)",
                            "sample": "hello\u200bworld",
                            "chars": ["U+200B"],
                            "suggested_transform": "strip_controls",
                        },
                        {
                            "column": "description",
                            "row": 7,
                            "message": "format-control character detected (U+200B)",
                            "sample": "foo\u200bbar",
                            "chars": ["U+200B"],
                        },
                    ],
                },
            }
        ],
        "blockers": [
            {
                "id": "g5_dry_run",
                "message": "Dry-run / integrity failed: description: format-control",
                "guidance": {"fix": "Apply strip_controls"},
            }
        ],
    }
    rows = quarantine_rows_from_preflight(pf)
    assert len(rows) == 2
    assert rows[0]["column"] == "description"
    assert rows[0]["row"] == 3
    assert "200B" in rows[0]["reason"] or "format-control" in rows[0]["reason"]
    assert rows[0]["policy"] == "preflight_quarantine"
    assert "\u200b" in rows[0]["value"]


def test_merge_prefers_write_details():
    job = {
        "rejected_details": [{"row": 1, "column": "age", "value": "x", "reason": "bad int"}],
        "preflight": {
            "gates": [{"details": {"encoding_issues": [{"column": "t", "row": 2, "message": "zwsp"}]}}],
        },
    }
    merged = merge_job_quarantine(job)
    assert len(merged) == 1
    assert merged[0]["column"] == "age"


def test_merge_falls_back_to_preflight():
    job = {
        "rejected_details": [],
        "preflight": {
            "gates": [
                {
                    "details": {
                        "encoding_issues": [
                            {"column": "description", "row": 1, "message": "format-control", "sample": "a\u200bb"},
                        ]
                    }
                }
            ]
        },
    }
    merged = merge_job_quarantine(job)
    assert len(merged) == 1
    assert merged[0]["column"] == "description"


def test_schema_policy_finding_does_not_suggest_strip_controls():
    pf = {
        "passed": False,
        "gates": [{
            "id": "g10_schema_policy",
            "status": "block",
            "message": "Schema change policy incomplete",
            "details": {
                "issues": ["Backfill new fields requires automatic column propagation"],
            },
        }],
        "blockers": [{
            "id": "g10_schema_policy",
            "message": "Schema change policy incomplete",
            "details": {"issues": ["Backfill new fields requires automatic column propagation"]},
        }],
    }
    rows = quarantine_rows_from_preflight(pf)
    assert rows
    assert rows[0]["suggested_transform"] is None
    assert "Backfill" in rows[0]["reason"]
