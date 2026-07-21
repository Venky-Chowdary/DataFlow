from services.job_list_view import slim_job_for_list


def test_slim_job_for_list_drops_heavy_arrays_keeps_counts():
    job = {
        "_id": "abc",
        "status": "completed",
        "records_processed": 1000,
        "rejected_rows": 3,
        "rejected_details": [{"row": 1, "error": "x"}] * 500,
        "logs": ["a"] * 200,
        "mapping_proof": {"pairs": [{"a": 1}] * 100},
        "destination_summary": {
            "rejected_rows": 3,
            "rejected_details": [{"row": 2}],
            "written": 997,
        },
        "reconciliation": {
            "matched": 997,
            "mismatches": [{"id": 1}] * 50,
        },
    }
    slim = slim_job_for_list(job)
    assert slim["_id"] == "abc"
    assert slim["status"] == "completed"
    assert slim["rejected_rows"] == 3
    assert "rejected_details" not in slim
    assert "logs" not in slim
    assert "mapping_proof" not in slim
    assert slim["destination_summary"]["written"] == 997
    assert "rejected_details" not in slim["destination_summary"]
    assert slim["reconciliation"]["matched"] == 997
    assert "mismatches" not in slim["reconciliation"]
