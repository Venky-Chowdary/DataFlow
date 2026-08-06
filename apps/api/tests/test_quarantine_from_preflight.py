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
                    "status": "block",
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


def test_signed_risk_contract_warn_not_quarantined():
    """Continue-policy fidelity notes must not look like write rejects."""
    pf = {
        "passed": False,
        "gates": [
            {
                "id": "g3_schema_contract",
                "status": "pass",
                "details": {
                    "issues_detail": [
                        {
                            "source": "country_auto_detected",
                            "target": "country_auto_detected",
                            "severity": "warn",
                            "message": (
                                "Column 'country_auto_detected' → INTEGER: declared "
                                "fidelity collapse (TEXT → INTEGER) — continue-policy "
                                "Risk Contract signed."
                            ),
                        }
                    ],
                },
            },
            {
                "id": "g9_data_integrity",
                "status": "block",
                "details": {
                    "issues": [
                        "id: duplicate key values from source probe (a×2)",
                    ],
                },
            },
        ],
        "blockers": [
            {
                "id": "g9_data_integrity",
                "message": "Duplicate identity keys",
                "details": {
                    "issues": ["id: duplicate key values from source probe (a×2)"],
                },
            }
        ],
    }
    rows = quarantine_rows_from_preflight(pf)
    assert len(rows) == 1
    assert "duplicate" in rows[0]["reason"].lower()
    assert "country_auto_detected" not in (rows[0].get("column") or "")


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


def test_objectid_lossy_string_fills_column_and_dedupes_integrity():
    pf = {
        "passed": False,
        "gates": [
            {
                "id": "g3_schema_contract",
                "status": "block",
                "details": {
                    "issues": [
                        "Lossy coercion: userId (OBJECTID) → user_id (TEXT) — OBJECTID specialty polarity collapse"
                    ],
                    "issues_detail": [
                        {
                            "source": "userId",
                            "target": "user_id",
                            "source_type": "OBJECTID",
                            "target_type": "TEXT",
                            "reason": "Lossy coercion: userId (OBJECTID) → user_id (TEXT) — OBJECTID specialty polarity collapse",
                            "suggested_fix": "Remap target type to VARCHAR(24)",
                        }
                    ],
                },
            },
            {
                "id": "g9_data_integrity",
                "status": "block",
                "details": {
                    "issues": ["userId (OBJECTID) → user_id (TEXT)"],
                },
            },
        ],
        "blockers": [
            {
                "id": "g3_schema_contract",
                "message": "1 type coercion issue(s); Data integrity failed: userId (OBJECTID) → user_id (TEXT)",
            }
        ],
    }
    rows = quarantine_rows_from_preflight(pf)
    assert len(rows) == 1
    assert rows[0]["column"] == "userId"
    assert rows[0]["target"] == "user_id"
    assert "specialty polarity" in rows[0]["reason"].lower()
    assert rows[0]["suggested_transform"] is None


def test_preflight_quarantine_preserves_sql_null_not_empty():
    from services.value_serializer import SQL_NULL_SENTINEL

    pf = {
        "passed": False,
        "gates": [
            {
                "id": "g5_dry_run",
                "status": "block",
                "details": {
                    "encoding_issues": [
                        {
                            "column": "note",
                            "row": 1,
                            "message": "null sample integrity",
                            "sample": None,
                        }
                    ],
                },
            }
        ],
        "blockers": [],
    }
    rows = quarantine_rows_from_preflight(pf)
    assert rows
    assert rows[0]["value"] == SQL_NULL_SENTINEL
    assert rows[0]["values"]["note"] == SQL_NULL_SENTINEL
