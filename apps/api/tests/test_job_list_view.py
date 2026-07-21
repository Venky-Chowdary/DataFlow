from services.job_list_view import slim_job_for_list


def test_slim_job_for_list_whitelist_drops_heavy_payload():
    job = {
        "_id": "abc",
        "status": "completed",
        "records_processed": 1000,
        "rejected_rows": 3,
        "rejected_details": [{"row": 1, "error": "x"}] * 500,
        "logs": ["a"] * 200,
        "mapping_proof": {"pairs": [{"a": 1}] * 100},
        "preflight": {"gates": [{"id": "g1"}] * 20},
        "transfer_request": {"mappings": [{"source": "a"}] * 50},
        "event_log": [{"m": 1}] * 100,
        "destination_summary": {
            "rejected_rows": 3,
            "rejected_details": [{"row": 2}],
            "written": 997,
        },
        "reconciliation": {"matched": 997, "mismatches": [{"id": 1}] * 50},
        "checkpoint": {
            "chunk_index": 2,
            "rows_processed": 500,
            "phase": "writing",
            "huge": "x" * 1000,
        },
    }
    slim = slim_job_for_list(job)
    assert slim["_id"] == "abc"
    assert slim["status"] == "completed"
    assert slim["rejected_rows"] == 3
    assert "rejected_details" not in slim
    assert "logs" not in slim
    assert "mapping_proof" not in slim
    assert "preflight" not in slim
    assert "transfer_request" not in slim
    assert "event_log" not in slim
    assert "destination_summary" not in slim
    assert "reconciliation" not in slim
    assert slim["checkpoint"]["chunk_index"] == 2
    assert "huge" not in slim["checkpoint"]
