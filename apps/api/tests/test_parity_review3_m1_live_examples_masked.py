"""Re-review 3, M1: live example values go through the same pii_guard masking as Gate-8's.

Gate-8 stores its mismatch values through ``pii_guard.redact_reconciliation``
(masked for a sensitive column name or a value that looks like PII). The
re-read put the live, unmasked values into ``differences[].example`` — the
API response, the signed pack and the model's facts — and so did the
ladder's localization values, which Gate-8 never masked.
"""

from __future__ import annotations

import pytest

from services import parity_cache
from services import parity_report as pr

MAPPINGS = [{"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "email", "target": "email", "target_type": "VARCHAR(12)"},
            {"source": "ssn", "target": "ssn", "target_type": "VARCHAR(20)"}]


@pytest.fixture(autouse=True)
def _clean_cache():
    parity_cache.clear()
    yield
    parity_cache.clear()


def _job(recon: dict) -> dict:
    return {"_id": "m1", "status": "completed", "reconciliation": recon,
            "destination_summary": {"table": "t", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"},
            "transfer_request": {"mappings": MAPPINGS}}


def _examples(report: dict) -> str:
    return repr([d["example"] for d in pr.public_report(report)["differences"]])


def test_reread_examples_are_masked(monkeypatch):
    monkeypatch.setattr(pr, "_bounded_reread", lambda job, ev, keys: {"7": {
        "raw": {"id": "7", "email": "alice.smith@example.com", "ssn": "123-45-6789"},
        "expected": {"id": "7", "email": "alice.smith@example.com", "ssn": "123-45-6789"},
        "dest": {"id": "7", "email": "alice.smith@", "ssn": "123-45-678"}, "held_out": False}})
    report = pr.build_parity_report(_job({"passed": False, "source_rows": 10, "target_rows": 10,
                                          "culprit_keys": {"changed": ["7"]}}), reread=True)
    text = _examples(report)
    assert "alice.smith@example.com" not in text and "123-45-6789" not in text and "123-45-678" not in text
    from services.parity_explain import grounded_facts

    assert "alice.smith@example.com" not in repr(grounded_facts(report, include_values=True))


def test_ladder_localization_examples_are_masked():
    report = pr.build_parity_report(_job({
        "passed": False, "source_rows": 10, "target_rows": 10,
        "verification_ladder": {"layers": {"L5": {"passed": False, "details": {"pk_column": "id"}}},
                                "localization": {"rows": [{"pk": "7", "column": "email",
                                                           "source_value": "bob.jones@example.com",
                                                           "target_value": "bob.jones@ex"}]}},
    }), reread=False)
    assert "bob.jones@example.com" not in _examples(report)


def test_non_sensitive_values_still_show():
    report = pr.build_parity_report(_job({
        "passed": False, "source_rows": 10, "target_rows": 10,
        "verification_ladder": {"layers": {"L5": {"passed": False, "details": {"pk_column": "id"}}},
                                "localization": {"rows": [{"pk": "7", "column": "amount",
                                                           "source_value": "10.1049", "target_value": "10.10"}]}},
    }), reread=False)
    assert {"source": "10.1049", "destination": "10.10"} in [d["example"] for d in report["differences"]]
