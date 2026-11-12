"""The enterprise harness isolates a test's database from the mail threads.

The mail outbox sweeper and the send lane are process-wide. A sweeper started
by an earlier test's app lifespan resolves Mongo through the patched
singleton, so its next tick created ``mail_outbox`` in a later test's
throwaway database, and ``test_r12_audit_key_persists_e2e`` saw a write the
read-only check never made.
"""

from __future__ import annotations

import time

import pytest

from services import mail_outbox, signup_service
from tests.r8_ent_harness import ent  # noqa: F401  (fixture)


@pytest.fixture
def leaked_sweeper(monkeypatch):
    """What an earlier test leaves behind, ticking fast enough to be certain."""
    orig = mail_outbox._database

    def fresh():
        mail_outbox._INDEXES_READY = False  # a first pass in this database
        return orig()

    monkeypatch.setattr(mail_outbox, "_database", fresh)
    signup_service.start_mail_outbox_sweeper(interval_sec=0.05)
    yield
    signup_service.stop_mail_outbox_sweeper()


def test_a_sweeper_left_running_never_writes_into_the_tests_database(leaked_sweeper, ent):
    sweeper = signup_service._SWEEPER
    assert sweeper is None or not sweeper.is_alive()
    time.sleep(0.4)  # eight ticks of the leaked period
    assert "mail_outbox" not in ent.db.list_collection_names()
