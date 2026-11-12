"""Pilot ground truth — every word of a counting question is accounted for (review 2, C2c).

Round 1 refused the qualifiers it had a list for; round 2 found the everyday
ones it had no list for ("orders jobs", "nightly jobs", "failed twice",
"with a duplicate key error") answered with the whole-history failure count,
grounded. The rule is now inverted: once the status, window, verification
and subject words are read, a word Pilot cannot account for makes the
question refused by name. A word the store can filter by exactly — a source
or destination name (the Jobs page search), a connector type, a sync mode —
is applied, and the count equals the store's for that same filter.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tests.helpers import pilot_truth as pt

NOW = datetime(2026, 11, 11, 15, 0, tzinfo=timezone.utc)


def _seed(store=None) -> None:
    """6 failed of 13: orders 2, invoices 3, customers 1; types and sync modes vary."""
    def job(status, minutes, **extra):
        extra.setdefault("destination_collection", "dw_main")
        pt.seed_job(status, at=NOW - timedelta(minutes=minutes), **extra)

    job("failed", 10, source_name="orders", records_processed=5_000_000,
        error="duplicate key value violates unique constraint", sync_mode="cdc")
    job("failed", 20, source_name="orders", records_processed=10, error="permission denied for table orders",
        source_type="mysql")
    for i in range(3):
        job("failed", 30 + i, source_name="invoices", error="connection reset by peer",
            destination_type="bigquery", sync_mode="full_refresh_append")
    job("failed", 40, source_name="customers", error="cast failure", reconciliation={"passed": False})
    for i in range(4):
        job("completed", 50 + i, source_name="invoices", source_type="mysql", sync_mode="cdc")
    job("completed", 60, source_name="orders", reconciliation={"passed": False})
    job("completed", 61, source_name="orders", reconciliation={"passed": False})
    job("running", 5, source_name="orders")


@pytest.fixture
def ledger(monkeypatch):
    pt.reset_workspace(monkeypatch)
    pt.freeze_clock(monkeypatch, NOW, "UTC")
    _seed()
    return pt.agent()


def _refused(response, *words: str) -> None:
    answer = response.answer
    assert not pt.grounded(response), f"a refusal is not grounded: {answer!r}"
    assert not pt.bold_ints(answer), f"a refused question states no count: {answer!r}"
    low = answer.lower()
    assert "can't" in low or "cannot" in low or "won't" in low, answer
    for word in words:
        assert word.lower() in low, f"the refusal must name “{word}”: {answer!r}"


# ---- words the store filters by exactly: applied, equal to the store ---------

APPLIED = [
    ("how many orders jobs failed", {"status": "failed", "search": "orders"}, "orders"),
    ("how many jobs failed for orders", {"status": "failed", "search": "orders"}, "orders"),
    ("how many jobs from orders failed", {"status": "failed", "search": "orders"}, "orders"),
    ("how many of the invoices jobs failed", {"status": "failed", "search": "invoices"}, "invoices"),
    ("how many mysql jobs failed", {"status": "failed", "source_type": "mysql"}, "mysql"),
    ("how many jobs from postgres failed", {"status": "failed", "source_type": "postgresql"}, "postgresql"),
    ("how many jobs to bigquery failed", {"status": "failed", "destination_type": "bigquery"}, "bigquery"),
    ("how many cdc jobs failed", {"status": "failed", "sync_mode": "cdc"}, "cdc"),
    ("how many cdc jobs completed", {"status": "completed", "sync_mode": "cdc"}, "cdc"),
    ("how many invoices jobs are there", {"search": "invoices"}, "invoices"),
]


@pytest.mark.parametrize("question,filters,named", APPLIED)
def test_a_filter_the_store_applies_is_applied_and_counted_exactly(ledger, question, filters, named):
    response = pt.ask(ledger, question)
    truth = int(pt.mongo().count_job_matches(**filters) or 0)
    broader = int(pt.mongo().count_job_matches(**{k: v for k, v in filters.items() if k == "status"}) or 0)
    assert truth != broader, "fixture must tell the filtered count from the broader one"
    assert pt.bold_ints(response.answer)[:1] == [truth], response.answer
    assert named in response.answer.lower(), f"the answer must say what it filtered by: {response.answer!r}"
    assert pt.grounded(response)


# ---- words nothing applies: refused by name, never the broader count ----------

REFUSED = [
    ("how many jobs failed for Acme", "Acme"),
    ("how many jobs failed in production", "production"),
    ("how many nightly jobs failed", "nightly"),
    ("how many scheduled jobs failed", "scheduled"),
    ("how many manual jobs failed", "manual"),
    ("how many jobs failed twice", "twice"),
    ("how many jobs failed more than once", "more than once"),
    ("how many jobs failed repeatedly", "repeatedly"),
    ("how many jobs failed with a duplicate key error", "duplicate key"),
    ("how many jobs failed because of permissions", "because of permissions"),
    ("how many jobs failed due to connection resets", "due to connection resets"),
    ("how many big jobs failed", "big"),
    ("how many jobs with more than 1000 rows failed", "more than 1000 rows"),
    ("how many jobs to the warehouse failed", "warehouse"),
    ("how many jobs did alice run", "alice"),
    ("how many retried jobs failed", "retried"),
    ("how many incremental jobs failed", "incremental"),
    ("how many slow jobs completed", "slow"),
    ("how many jobs failed from orders to invoices", "orders"),
]


@pytest.mark.parametrize("question,word", REFUSED)
def test_a_word_nothing_applies_is_refused_by_name(ledger, question, word):
    _refused(pt.ask(ledger, question), word)


# ---- the everyday questions are still answered --------------------------------

ANSWERED = [
    ("how many jobs failed", {"status": "failed"}),
    ("how many of my jobs failed today", {"status": "failed"}),
    ("show me my failed jobs", {"status": "failed"}),
    ("which jobs failed?", {"status": "failed"}),
    ("how many jobs are running right now", {"status": "running"}),
    ("how many jobs need my attention", {"status": "attention"}),
    ("how many transfers have completed so far", {"status": "completed"}),
    ("can you tell me how many failed jobs there are", {"status": "failed"}),
    ("hey, how many jobs failed in total?", {"status": "failed"}),
    ("number of failed transfers", {"status": "failed"}),
]


@pytest.mark.parametrize("question,filters", ANSWERED)
def test_everyday_counting_questions_are_still_answered(ledger, question, filters):
    response = pt.ask(ledger, question)
    truth = int(pt.mongo().count_job_matches(**filters) or 0)
    assert pt.bold_ints(response.answer)[:1] == [truth], response.answer
    assert pt.grounded(response)


# ---- the same counts on the Mongo store (throwaway database, dropped) ---------

PARITY = [(q, f) for q, f, _ in APPLIED] + [(q, f) for q, f in ANSWERED[:3]]


def _mongo_uri() -> str:
    import os

    return os.environ.get("P2_MONGO_URI", "mongodb://127.0.0.1:27017")


def _mongo_live() -> bool:
    from tests.helpers.live_env import mongo_up

    return mongo_up(_mongo_uri())


@pytest.mark.skipif(not _mongo_live(), reason="live MongoDB not reachable")
def test_counts_equal_the_store_on_mongo_as_on_memory(monkeypatch):
    import uuid

    from services import mongodb_service

    pt.reset_workspace(monkeypatch)
    pt.freeze_clock(monkeypatch, NOW, "UTC")
    memory = pt.mongo()
    _seed()
    memory_answers = {q: pt.bold_ints(pt.ask(pt.agent(), q).answer)[:1] for q, _ in PARITY}

    name = f"pilot_truth_parity_{uuid.uuid4().hex[:10]}"
    live = mongodb_service.MongoDBService(_mongo_uri())
    live.db_name = name
    assert live.connect()
    try:
        monkeypatch.setenv("DATAFLOW_JOB_STORE", "mongo")
        monkeypatch.setattr(mongodb_service, "_mongodb_service", live)
        from bson import ObjectId

        coll = live.client[name]["transfer_jobs"]
        for row in memory._jobs.values():
            doc = {k: v for k, v in row.items() if k not in ("_id", "id")}
            coll.insert_one({"_id": ObjectId(), **doc})
        from src.ai.copilot import context_builder

        context_builder.reset_context_cooldowns()
        for question, filters in PARITY:
            truth = int(live.count_job_matches(**filters) or 0)
            assert truth == int(memory.count_job_matches(**filters) or 0), question
            answer = pt.ask(pt.agent(), question).answer
            assert pt.bold_ints(answer)[:1] == [truth] == memory_answers[question], (question, answer)
    finally:
        live.client.drop_database(name)
        assert name not in live.client.list_database_names()
