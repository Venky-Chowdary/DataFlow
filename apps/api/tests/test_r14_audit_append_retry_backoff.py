"""CI api-tests 5/6 (1de54a539): a writer process lost 25 position races in a row.

test_r9_d221_audit_chain_fork_and_key_e2e::test_writer_processes_never_fork_the_chain[fresh]
starts four writer processes on a fresh store; on a busy host one of them
re-read the tip and re-inserted in lockstep with the others until it ran
out of attempts ("audit chain append lost 25 position races") and the
audit write failed. A lost race now waits a short random time before the
next attempt, with a larger budget.
"""

from __future__ import annotations

from pymongo.errors import DuplicateKeyError

from services import audit_chain_link


def test_a_writer_that_keeps_losing_the_position_race_still_lands(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr("time.sleep", lambda s: sleeps.append(s))
    lost = {"n": 0}

    def insert(event):
        if lost["n"] < 30:  # thirty other writers take the position first
            lost["n"] += 1
            raise DuplicateKeyError("E11000 duplicate key", 11000)
        return event

    out = audit_chain_link.append_with_retry(lambda: {"seq": lost["n"]}, insert)
    assert out == {"seq": 30}
    assert len(sleeps) == 30 and all(0.0 <= s <= 0.05 for s in sleeps), sleeps


def test_any_other_insert_error_is_raised_at_once(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    calls = {"n": 0}

    def insert(event):
        calls["n"] += 1
        raise OSError("store down")

    try:
        audit_chain_link.append_with_retry(lambda: {}, insert)
    except OSError:
        pass
    assert calls["n"] == 1
