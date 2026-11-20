"""Pilot state with no local key: kept in memory, said once, and cheap (PR #433 CI).

With no SECRETS_KEY and no deployment AUTH_SECRET, Pilot state cannot be
sealed and stays in memory (audit H5, review 2 M5). ``seal`` still serialized
every value before it found out there was no key, and logged a WARNING for each
one. The result store re-tries every entry it holds on every write (up to 200
row sets of up to 500 rows), so the wave-45 answer corpus serialized and
logged ~1.3 million row sets: the CI log carried millions of identical lines
and the test ran past its 900 s timeout on a slower runner. The fallback is
unchanged; the condition is reported once, with the remedy, and costs nothing.
"""

from __future__ import annotations

import logging
from decimal import Decimal

import pytest

SALARY = Decimal("182000.00")


@pytest.fixture
def no_key(monkeypatch, tmp_path):
    from src.ai.copilot import result_store, sealed_state

    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("ENV", "SECRETS_KEY", "AUTH_SECRET"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    monkeypatch.setenv("DATAFLOW_PILOT_RESULTS_PATH", str(tmp_path / "pilot_results.json"))
    monkeypatch.setattr(result_store, "_store", None)
    # A fresh process as far as the one-time notice is concerned.
    monkeypatch.setattr(sealed_state, "_warned_no_key", False, raising=False)
    return sealed_state


def _sealed_state_warnings(caplog) -> list[logging.LogRecord]:
    return [
        r for r in caplog.records
        if r.name == "src.ai.copilot.sealed_state" and r.levelno >= logging.WARNING
    ]


def test_no_key_is_reported_once_with_the_remedy(no_key, caplog, monkeypatch):
    caplog.set_level(logging.WARNING, logger="src.ai.copilot.sealed_state")
    for _ in range(50):
        assert no_key.seal({"rows": [{"salary": SALARY}]}) is None

    warned = _sealed_state_warnings(caplog)
    assert len(warned) == 1, [r.getMessage() for r in warned]
    text = warned[0].getMessage()
    assert "memory" in text
    assert "SECRETS_KEY" in text and "AUTH_SECRET" in text, text

    # The notice is once; the key check is not cached: a key set later seals.
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "pilot-no-key-test-deployment-key-0123456789")
    token = no_key.seal({"rows": [{"salary": SALARY}]})
    assert token and "182000" not in token
    assert no_key.unseal(token) == {"rows": [{"salary": "182000.00"}]}


def test_a_write_into_a_full_store_does_not_reserialize_what_it_cannot_seal(no_key, monkeypatch):
    """Each put used to serialize every held row set again, only to drop it."""
    from src.ai.copilot import result_store

    store = result_store.get_result_store()
    rows = [{"id": i, "salary": SALARY} for i in range(50)]
    for i in range(result_store._MAX_ENTRIES):
        store.put(rows=rows, columns=["id", "salary"], session_id=f"nokey|{i}")

    calls = {"n": 0}
    real = no_key.json_default

    def counting(value):
        calls["n"] += 1
        return real(value)

    monkeypatch.setattr(no_key, "json_default", counting)
    rid = store.put(rows=rows, columns=["id", "salary"], session_id="nokey|last")
    assert calls["n"] == 0, f"{calls['n']} values serialized for state that stays in memory"

    # The in-memory fallback still serves the follow-up, and nothing is on disk in clear.
    doc = store.get(rid)
    assert doc is not None and doc["rows"][0]["salary"] == SALARY
    spill = result_store._default_path()
    assert not spill.exists() or "182000" not in spill.read_text(encoding="utf-8")
