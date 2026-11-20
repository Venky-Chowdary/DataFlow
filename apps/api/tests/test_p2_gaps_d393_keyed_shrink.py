"""D393 gap: an in-place merge never hides rows a keyed destination lost.

``merged_in_place`` explains a destination that did not grow by the changed
keys (incremental_append onto an enforced key, SQL Server). A keyed
destination that *shrank* is not that case: the rows it lost stay an
unexplained missing difference, and no merge is claimed (R9).
"""

from __future__ import annotations


def _job(*, source_rows: int, target_rows: int, before: int) -> dict:
    return {
        "status": "completed",
        "sync_mode": "incremental_append",
        "reconciliation": {
            "source_rows": source_rows,
            "target_rows": target_rows,
            "target_rows_before": before,
            "passed": True,
            "sample_compare": {"rows_compared": target_rows, "_sample": None},
        },
        "destination_summary": {
            "sync_mode": "incremental_append",
            "primary_key_columns": ["id"],
            "table": "t",
            "type": "sqlserver",
        },
        "transfer_request": {
            "sync_mode": "incremental_append",
            "mappings": [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
            "destination": {"format": "sqlserver", "table": "t"},
        },
    }


def test_a_keyed_destination_that_shrank_reports_missing_rows_not_a_merge():
    from services.parity_evidence import extract_evidence

    evidence = extract_evidence(_job(source_rows=2, target_rows=29, before=31))
    observations = evidence[0].observations
    assert not any((o.cause or {}).get("code") == "merged_in_place" for o in observations)
    missing = [
        o for o in observations
        if o.kind == "missing_in_destination" and not (o.cause or {}).get("explained")
    ]
    # 31 before + 2 written (both merges at best) can never leave 29.
    assert missing and sum(o.rows for o in missing) >= 2, observations


def test_merged_rows_are_never_more_than_the_run_wrote():
    from services.parity_evidence import extract_evidence

    evidence = extract_evidence(_job(source_rows=3, target_rows=32, before=31))
    merged = [
        o for o in evidence[0].observations if (o.cause or {}).get("code") == "merged_in_place"
    ]
    # One new key grew the table; the other two were merged in place.
    assert merged and merged[0].rows == 2, evidence[0].observations
