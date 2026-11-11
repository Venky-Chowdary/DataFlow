"""Re-review 4, 9g (Low): a parity fix error goes through the canonical error path.

``services.parity_errors`` logged its own unstructured line next to the platform's
``services.safe_errors.public_error`` (the canonical error event with a correlation id). The
allowlist stays (the public sentence, sealed in signed packs, keeps only class, code and a fixed
sentence, contract 49), but the server-side record is now ``public_error``'s event, and the full
text logged for operators carries the same correlation id. The public shape is unchanged.
"""

from __future__ import annotations

import logging

from services.parity_errors import safe_error


class UniqueViolation(Exception):
    pass


def test_a_fix_error_emits_the_canonical_event_with_a_correlation_id(caplog):
    exc = UniqueViolation('duplicate key value violates unique constraint "u" DETAIL: Key (e)=(x@y.z)')
    with caplog.at_level(logging.WARNING):
        out = safe_error(exc, what="The re-sync start")
    assert out == "The re-sync start failed (UniqueViolation 23505): a unique key already holds this value (duplicate key)."
    [event] = [r for r in caplog.records if getattr(r, "event", "") == "parity.fix.failed"]
    assert event.outcome == "errored" and event.error_type == "UniqueViolation"
    assert event.correlation_id and getattr(event, "error_code", None) == "23505"
    full = [r for r in caplog.records if "x@y.z" in r.getMessage()]
    assert full and event.correlation_id in full[0].getMessage()


def test_a_child_job_error_text_is_recorded_the_same_way(caplog):
    with caplog.at_level(logging.WARNING):
        out = safe_error("Duplicate entry 'carol@example.com' for key 'email'", what="Re-sync job j1")
    assert "carol@example.com" not in out and "MySQL 1062" in out
    assert any(getattr(r, "event", "") == "parity.fix.failed" for r in caplog.records)


def test_the_exposure_switch_never_reaches_the_sealed_sentence(monkeypatch):
    monkeypatch.setenv("DATAFLOW_EXPOSE_ERROR_DETAIL", "1")
    out = safe_error(UniqueViolation("Key (email)=(bob@example.com) duplicate key value violates unique constraint"))
    assert "bob@example.com" not in out and "Reference" not in out
