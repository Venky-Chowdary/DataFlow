"""Re-review 4, 9a (Low): a protected key renamed by its mapping never shows its values.

When the key came only from ``stream_contracts.primary_key`` (a source column name) and the
protected target is renamed (``ssn`` -> ``customer_ssn`` with ``hash_pii``), the protection check
compared a source name against target names: ``keys_sample`` and the re-sync preview showed raw
keys. The contract key is now read in destination naming, like every other key source.
"""

from __future__ import annotations

from services.parity_evidence import key_columns_of
from services.parity_report import build_parity_report, public_report
from services.parity_signoff import render_markdown

MAPS = [{"source": "ssn", "target": "customer_ssn", "transform": "hash_pii"},
        {"source": "amount", "target": "amount", "target_type": "NUMERIC(12,2)"}]
RAW = ("123-45-6789", "987-65-4321")


def _job() -> dict:
    return {"_id": "j", "status": "completed",
            "reconciliation": {"passed": False, "source_rows": 100, "target_rows": 100,
                               "culprit_keys": {"changed": list(RAW)}},
            "destination_summary": {"table": "orders", "primary_key_columns": [], "sync_mode": "full_refresh_overwrite"},
            "transfer_request": {"mappings": MAPS, "stream_contracts": [{"name": "orders", "primary_key": ["ssn"]}],
                                 "source": {"kind": "database"}, "destination": {"kind": "database"}}}


def test_the_contract_key_is_read_in_destination_naming():
    assert key_columns_of(_job()) == ["customer_ssn"]


def test_no_raw_key_reaches_the_report_the_fixes_or_the_export():
    report = public_report(build_parity_report(_job(), reread=False))
    text = repr(report) + render_markdown(report, None)
    assert not any(raw in text for raw in RAW)
    assert all(d["keys_sample"] in ([], ["protected"]) for d in report["differences"])
