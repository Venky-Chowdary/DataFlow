"""Named-fixture QA for the first-party chatbot — Google/Microsoft operator asks.

This is 100% on **this fixture**, not all English. Each case is a wording an
enterprise evaluator actually types. A miss here is a silent-wrong answer
(dbt answered as Studio, SSH answered as wal_level), not a style nits.
"""

from __future__ import annotations

import pytest

from src.ai.rag.product_docs import compose_product_answer, retrieve_product_answer


def _lead(question: str) -> str:
    answer = retrieve_product_answer(question, limit=4)
    if not answer.hits:
        return f"[{answer.verdict.outcome}]"
    body = " ".join((compose_product_answer(answer) or "").split())
    return body.split(". ")[0].lower()


@pytest.mark.parametrize(
    "question,needles,forbidden",
    [
        ("can I use dbt with datawrap?", ("does not run dbt",), ("wal_level", "query playground")),
        ("do you support dbt Cloud?", ("does not run dbt",), ("wal_level",)),
        ("does datawrap run dbt as the transfer engine?", ("does not run dbt",), ("exactly-once",)),
        ("can we run our dbt models after the load?", ("dbt", "complement"), ("wal_level",)),
        ("hey do you guys have dbt cloud?", ("does not run dbt",), ("studio",)),
        ("do you open an SSH tunnel to postgres?", ("does not open ssh",), ("wal_level", "query playground")),
        ("can I connect through a bastion host?", ("does not open ssh", "bastion"), ("wal_level",)),
        ("do you support ssh tunnels?", ("does not open ssh",), ("wal_level",)),
        ("is there a jump host option for mysql?", ("does not open ssh",), ("binlog",)),
        ("is cdc exactly-once?", ("least",), ()),
        ("do you guarantee exactly once delivery?", ("least",), ()),
        ("are you chatgpt?", ("local engine",), ("we are chatgpt",)),
        ("do we have our own llm?", ("own local engine",), ()),
        ("gotta have logical wal for pg cdc right?", ("wal_level",), ("dbt", "ssh")),
        ("where do bad rows go?", ("quarantine",), ("dbt",)),
        ("how is this different from fivetran?", ("semantic mapping", "quarantine"), ("dbt cloud",)),
        ("can a viewer export yaml?", ("viewer", "yaml"), ("dbt",)),
        ("does iceberg use merge on read?", ("merge-on-read",), ("exactly-once",)),
        ("who can start a transfer?", ("job.run",), ("dbt",)),
        ("do you have webhooks?", ("webhook",), ("dbt",)),
    ],
)
def test_enterprise_wording_leads_on_the_asked_fact(
    question: str, needles: tuple[str, ...], forbidden: tuple[str, ...]
) -> None:
    lead = _lead(question)
    missing = [n for n in needles if n.lower() not in lead]
    leaked = [f for f in forbidden if f.lower() in lead]
    assert not missing, f"{missing} not in {lead[:240]}"
    assert not leaked, f"{leaked} leaked into {lead[:240]}"


@pytest.mark.parametrize(
    "question",
    [
        "how do I cook rice tonight",
        "write me a poem about the sea",
        "what is the capital of France",
    ],
)
def test_off_subject_english_is_refused(question: str) -> None:
    answer = retrieve_product_answer(question, limit=4)
    assert answer.verdict.outcome == "refuse"
    assert not answer.hits
