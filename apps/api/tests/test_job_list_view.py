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
        "row_accounting": {
            "dest_count": 4,
            "writer_ack": 1000,
            "balanced": True,
            "conservation_kind": "overwrite",
            "rows_written_source": "gate8_dest_readback",
            "note": "Dest COUNT(*) closes the identity.",
        },
        "trust_score": 91,
    }
    slim = slim_job_for_list(job)
    assert slim["_id"] == "abc"
    assert slim["status"] == "completed"
    assert slim["rejected_rows"] == 3
    assert slim["row_accounting"]["dest_count"] == 4
    assert slim["row_accounting"]["writer_ack"] == 1000
    assert slim["trust_score"] == 91
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


def test_slim_job_keeps_connector_ids_and_drops_transfer_request():
    from services.job_list_view import slim_job_for_list

    slim = slim_job_for_list(
        {
            "_id": "job-1",
            "status": "completed",
            "source_name": "airports",
            "source_connector_id": "mysql-venky",
            "dest_connector_id": "sf-dest",
            "transfer_request": {
                "source": {"connector_id": "mysql-venky", "password": "secret"},
                "destination": {"connector_id": "sf-dest"},
                "mappings": [{"source": "id"}] * 80,
            },
        }
    )
    assert slim["source_connector_id"] == "mysql-venky"
    assert slim["dest_connector_id"] == "sf-dest"
    assert "transfer_request" not in slim


def test_slim_job_recovers_connector_ids_from_legacy_transfer_request():
    from services.job_list_view import connector_ids_from_job, slim_job_for_list

    job = {
        "_id": "legacy",
        "status": "completed",
        "source_name": "airports",
        "transfer_request": {
            "source": {"connector_id": "mysql-venky", "table": "airports"},
            "destination": {"connector_id": "sf-dest", "table": "AUDIT"},
        },
    }
    assert connector_ids_from_job(job) == ("mysql-venky", "sf-dest")
    slim = slim_job_for_list(job)
    assert slim["source_connector_id"] == "mysql-venky"
    assert slim["dest_connector_id"] == "sf-dest"
    assert "transfer_request" not in slim


def test_slim_job_lifts_multi_table_names_and_drops_stream_ledgers():
    job = {
        "_id": "multi",
        "status": "completed",
        "source_name": "orders",
        "destination_collection": "orders",
        "streams": [{"name": "stale", "row_accounting": {"dest_count": 99}}],
        "destination_summary": {
            "table": "orders",
            "multi_stream": True,
            "checksum": "abc",
            "streams": [
                {"name": "customers", "row_accounting": {"dest_count": 2, "note": "x" * 500}},
                {"name": "orders", "row_accounting": {"dest_count": 2}},
                {"status": "completed"},
                {"name": "customers"},
            ],
        },
    }
    slim = slim_job_for_list(job)
    assert slim["stream_names"] == ["customers", "orders"]
    assert "destination_summary" not in slim
    assert "streams" not in slim
    assert slim["source_name"] == "orders"


def test_slim_job_names_selected_contracts_before_stream_health_exists():
    slim = slim_job_for_list(
        {
            "_id": "starting",
            "source_name": "orders",
            "destination_collection": "orders",
            "transfer_request": {
                "stream_contracts": [
                    {"name": "customers", "selected": True},
                    {"name": "orders"},
                    {"name": "archive", "selected": False},
                    {"stream": "orders"},
                ]
            },
        }
    )
    assert slim["stream_names"] == ["customers", "orders"]
    assert "transfer_request" not in slim


def test_slim_job_does_not_let_the_first_written_table_hide_the_selection():
    slim = slim_job_for_list(
        {
            "_id": "partial",
            "destination_summary": {"streams": [{"name": "customers", "row_accounting": {"dest_count": 2}}]},
            "transfer_request": {
                "stream_contracts": [{"name": "customers"}, {"name": "orders"}],
            },
        }
    )
    assert slim["stream_names"] == ["customers", "orders"]
    assert "destination_summary" not in slim


def test_slim_job_keeps_measured_streams_ahead_of_contracts():
    slim = slim_job_for_list(
        {
            "_id": "measured",
            "destination_summary": {
                "streams": [{"name": "customers"}, {"name": "orders"}],
            },
            "transfer_request": {
                "stream_contracts": [{"name": "alpha"}, {"name": "beta"}],
            },
        }
    )
    assert slim["stream_names"] == ["customers", "orders"]


def test_slim_job_omits_stream_names_for_a_single_table():
    slim = slim_job_for_list(
        {
            "_id": "one",
            "source_name": "orders",
            "destination_summary": {"streams": [{"name": "orders", "row_accounting": {"dest_count": 2}}]},
        }
    )
    assert "stream_names" not in slim


def test_slim_job_uses_top_level_streams_when_the_summary_list_is_empty():
    slim = slim_job_for_list(
        {
            "_id": "cdc",
            "streams": [{"stream": "customers"}, {"name": "orders"}],
            "destination_summary": {"streams": []},
        }
    )
    assert slim["stream_names"] == ["customers", "orders"]
