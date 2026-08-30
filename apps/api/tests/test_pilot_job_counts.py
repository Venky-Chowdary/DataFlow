"""A "how many jobs?" answer must be the counted history, not the page we could show.

The browser run asked Pilot for a job count with 50 jobs stored; it listed five recent
ids and stated no total, which contradicted the Jobs page and read as a wrong answer.
"""

from src.ai.copilot.job_narration import narrate_jobs


def _jobs(n: int, status: str = "completed") -> list[dict]:
    return [
        {
            "id": f"job{i}",
            "source": "pg",
            "destination": "mysql",
            "status": status,
            "records": 1000 + i,
        }
        for i in range(n)
    ]


def test_count_question_reports_the_counted_total_not_the_window():
    answer = narrate_jobs(
        {"jobs": _jobs(10), "count": 10, "total": 50, "status_counts": {"completed": 50}},
        "how many jobs do I have?",
    )
    assert "**50**" in answer
    # The bullets are an excerpt and say so, instead of implying the total.
    assert "5 most recent" in answer
    assert answer.count("• `") == 5


def test_failure_question_counts_failures_across_the_whole_history():
    answer = narrate_jobs(
        {
            "jobs": _jobs(3, "failed") + _jobs(2),
            "count": 5,
            "total": 50,
            "status_counts": {"failed": 12, "completed": 37, "cancelled": 1},
        },
        "which jobs failed?",
    )
    assert "**13** of your **50** job(s) failed" in answer


def test_no_failures_states_the_real_total():
    answer = narrate_jobs(
        {"jobs": _jobs(4), "count": 4, "total": 4, "status_counts": {"completed": 4}},
        "any failed jobs?",
    )
    assert "None of your **4** job(s) failed" in answer


def test_window_note_is_omitted_when_the_bullets_are_the_whole_history():
    answer = narrate_jobs(
        {"jobs": _jobs(3), "count": 3, "total": 3, "status_counts": {"completed": 3}},
        "list my jobs",
    )
    assert "You have **3** transfer job(s)" in answer
    assert "most recent — open" not in answer


def test_empty_history_asks_for_the_first_transfer():
    answer = narrate_jobs({"jobs": [], "count": 0, "total": 0, "status_counts": {}}, "my jobs")
    assert "No transfer jobs yet" in answer


def test_missing_total_falls_back_to_the_window_without_inventing_a_number():
    answer = narrate_jobs({"jobs": _jobs(2), "count": 2}, "how many jobs")
    assert "**2** transfer job(s)" in answer


def test_memory_store_counts_by_status_over_every_job():
    from services.mongodb_service import MemoryMongoDBService

    store = MemoryMongoDBService()
    for i in range(7):
        store._jobs[f"j{i}"] = {
            "_id": f"j{i}",
            "status": "failed" if i % 3 == 0 else "completed",
            "workspace_id": "w1" if i < 4 else "w2",
        }

    assert store.count_jobs() == {"total": 7, "by_status": {"failed": 3, "completed": 4}}
    scoped = store.count_jobs(workspace_id="w1")
    assert scoped["total"] == 4
