"""Terminal job updates must carry the stream list the summary already has."""

from services.row_conservation import attach_conservation_to_updates
from src.transfer.job_failure import summary_streams


def test_summary_streams_ignores_empty_and_missing():
    assert summary_streams(None) is None
    assert summary_streams({}) is None
    assert summary_streams({"streams": []}) is None
    assert summary_streams({"streams": [{"name": "orders"}]})[0]["name"] == "orders"


def test_terminal_update_copies_summary_streams_onto_the_job():
    updates = {
        "records_processed": 3,
        "destination_summary": {
            "streams": [
                {"name": "orders", "status": "completed", "records_processed": 2},
                {"name": "users", "status": "completed", "records_processed": 1},
            ],
        },
    }
    attach_conservation_to_updates("completed", updates)
    assert [row["name"] for row in updates["streams"]] == ["orders", "users"]
    assert "row_accounting" in updates


def test_failed_update_still_copies_summary_streams():
    updates = {
        "destination_summary": {
            "streams": [{"name": "orders", "status": "failed", "error": "count refused"}],
        },
    }
    attach_conservation_to_updates("failed", updates)
    assert updates["streams"][0]["error"] == "count refused"


def test_running_update_does_not_promote_streams():
    updates = {
        "destination_summary": {"streams": [{"name": "orders"}]},
    }
    attach_conservation_to_updates("running", updates)
    assert "streams" not in updates
    assert "row_accounting" not in updates


def test_summary_streams_replace_a_stale_top_level_list():
    updates = {
        "streams": [{"name": "stale"}],
        "destination_summary": {"streams": [{"name": "orders"}]},
    }
    attach_conservation_to_updates("completed", updates)
    assert updates["streams"][0]["name"] == "orders"


def test_previous_summary_streams_survive_a_status_only_terminal_update():
    updates = {"records_processed": 2}
    previous = {
        "destination_summary": {"streams": [{"name": "orders", "status": "completed"}]},
    }
    attach_conservation_to_updates("completed", updates, previous=previous)
    assert updates["streams"][0]["name"] == "orders"
