"""QA MX3-21 — "cancelling a PostgreSQL CDC job leaves its logical replication slot".

Keeping the slot is the designed behaviour since e20d503a (E3-006): the next
start of the same route resumes from the log, so a delete committed while the
job was stopped is applied instead of being skipped by a fresh snapshot
(``test_cdc_catchup_honesty::test_cancel_leaves_the_slot_while_the_worker_holds_the_lease``).
The defect is that the Pilot capability card still told operators a cancel
drops the slot, so a kept slot looked like a leak and no one knew to drop it.
"""

from __future__ import annotations

import pytest

from services.cdc_catchup import release_finished_cdc_slot
from src.ai.first_party.capability_contract import pause_cdc_card


def test_capability_card_matches_cancel_slot_behaviour(monkeypatch):
    card = pause_cdc_card()
    if card is None:
        pytest.skip("pause_cdc card not published on this build")
    text = card.text
    assert "when a cancel closes the replication" not in text
    assert "drops its Postgres slot when that job completes" not in text
    assert "cancel" in text.lower() and "keeps its Postgres slot" in text
    assert "pg_drop_replication_slot" in text

    monkeypatch.setattr("services.cdc_catchup._schedule_owns_slot", lambda *a, **k: False)
    monkeypatch.setattr(
        "connectors.postgresql_change_stream.release_pg_capture",
        lambda *_a, **_k: pytest.fail("a cancelled one-shot must keep its slot for resume"),
    )
    out = release_finished_cdc_slot(
        {"cdc_slot_name": "df_orders_slot", "status": "cancelled"},
        reason="cancelled",
        source_cfg={"type": "postgresql", "database": "qa"},
        job_id="job-mx321",
        worker_closed=True,
    )
    assert out == {
        "released": False,
        "reason": "resume_keeps_slot",
        "job_id": "job-mx321",
        "slot_name": "df_orders_slot",
    }
