from __future__ import annotations

from datetime import date, datetime, timezone

from services.job_list_filters import memory_job_matches, mongo_job_list_query, status_values


def test_mongo_job_query_combines_scope_status_search_and_inclusive_dates():
    query = mongo_job_list_query(
        {"workspace_id": "workspace-a"},
        status="completed",
        search="orders [east]",
        date_from=date(2026, 11, 1),
        date_to=date(2026, 11, 8),
    )
    clauses = query["$and"]
    assert {"workspace_id": "workspace-a"} in clauses
    completed = next(part["status"]["$in"] for part in clauses if "status" in part)
    assert set(completed) == {
        "completed", "completed_with_quarantine", "completed_with_warnings", "success", "succeeded",
    }
    search = next(part["$or"] for part in clauses if "$or" in part)
    expected_search = {"$regex": r"orders\ \[east\]", "$options": "i"}
    assert next(part["name"] for part in search if "name" in part) == expected_search
    assert {"_id": expected_search} in search
    created = next(part["created_at"] for part in clauses if "created_at" in part)
    assert created == {
        "$gte": datetime(2026, 11, 1, tzinfo=timezone.utc),
        "$lt": datetime(2026, 11, 9, tzinfo=timezone.utc),
    }

def test_mongo_job_id_prefix_search_uses_object_id_range():
    query = mongo_job_list_query(search="abc123")
    search = query.get("$or") or next(part["$or"] for part in query.get("$and", []) if "$or" in part)
    object_id_range = next(part["_id"] for part in search if "_id" in part and "$gte" in part["_id"])

    assert str(object_id_range["$gte"]).startswith("abc123")
    assert str(object_id_range["$lte"]).startswith("abc123")


def test_in_memory_job_filters_match_search_status_and_calendar_days():
    job = {
        "_id": "job-42",
        "name": "Orders [East] import",
        "status": "completed_with_quarantine",
        "created_at": "2026-11-08T23:59:59-05:00",
    }
    assert memory_job_matches(
        job,
        status="completed",
        search="orders [east]",
        date_from=date(2026, 11, 9),
        date_to=date(2026, 11, 9),
    )
    assert not memory_job_matches(job, status="failed")
    assert not memory_job_matches(job, search="not present")
    assert memory_job_matches({"_id": "abcdef123456", "status": "failed"}, search="abcdef")


def test_unknown_or_all_status_does_not_add_a_status_filter():
    assert status_values("all") is None
    assert status_values("unknown") is None
    assert status_values("running") == ("running", "pending", "queued", "retrying")
    assert {"success", "succeeded", "completed_with_warnings"}.issubset(set(status_values("completed") or ()))