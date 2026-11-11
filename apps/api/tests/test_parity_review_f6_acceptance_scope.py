"""Review finding 6: an acceptance covers the rows that were accepted, nothing more.

Acceptances were keyed by table|column|kind|cause only and stored no rows, so
any later difference with the same signature — new keys, ten times the rows —
was auto-accepted. They now store the accepted keys and keyless row count and
apply only to that set; accepting needs a non-empty note (422).
"""

from __future__ import annotations

import uuid

import pytest

from services.mongodb_service import MemoryMongoDBService


@pytest.fixture()
def store(monkeypatch, tmp_path):
    import services.mongodb_service as mongodb_service
    from services import audit_log

    svc = MemoryMongoDBService()
    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: svc)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setenv("DATAFLOW_LLM_ENABLED", "false")
    return svc


def _mismatch(key: int) -> dict:
    return {"key": str(key), "source": "amount", "target": "amount", "source_value": f"{key}.1049",
            "target_value": f"{key}.10"}


def _job(svc, keys: list[int], missing_rows: int = 0) -> str:
    job_id = svc.create_transfer_job({
        "_id": "acc" + uuid.uuid4().hex[:10], "workspace_id": "ws", "status": "completed",
        "reconciliation": {"passed": False, "source_rows": 1000, "target_rows": 1000 - missing_rows,
                           "culprit_census_clean": True,
                           "sample_compare": {"rows_compared": 1000, "mismatches": [_mismatch(k) for k in keys]}},
        "destination_summary": {"table": "payments", "primary_key_columns": ["id"],
                                "sync_mode": "full_refresh_overwrite"},
    })
    svc.update_job_status(job_id, "completed")
    return job_id


def _set_evidence(svc, job_id: str, keys: list[int], missing_rows: int = 0) -> None:
    recon = dict(svc.get_job(job_id)["reconciliation"])
    recon["sample_compare"] = {"rows_compared": 1000, "mismatches": [_mismatch(k) for k in keys]}
    recon["target_rows"] = 1000 - missing_rows
    svc.update_job_fields(job_id, {"reconciliation": recon})


def _accept_all(svc, job_id: str) -> dict:
    from services.parity_fixes import approve_fix
    from services.parity_report import build_parity_report

    report = build_parity_report(svc.get_job(job_id), reread=False, job_id=job_id)
    for fix in [f for f in report["fixes"] if f["action"] == "accept" and f["status"] == "proposed"]:
        report = approve_fix(job_id, fix["id"], actor="owner@example.com", note="known rounding")
    return report


def test_new_keys_with_the_same_signature_are_not_auto_accepted(store):
    from services.parity_report import build_parity_report

    job_id = _job(store, [1, 2])
    assert _accept_all(store, job_id)["parity"]["unexplained"] == 0
    _set_evidence(store, job_id, [1, 2, 3, 4])  # the same cause on two more keys
    report = build_parity_report(store.get_job(job_id), reread=False, job_id=job_id)
    assert report["parity"]["unexplained"] == 2, report["differences"]
    bad = [d for d in report["differences"] if not d["cause"]["explained"]]
    assert sorted(bad[0]["keys_sample"]) == ["3", "4"]
    accepted = [d for d in report["differences"] if d["cause"]["code"] == "accepted_by_operator"]
    assert accepted and accepted[0]["rows"] == 2


def test_more_keyless_rows_than_were_accepted_are_not_auto_accepted(store):
    from services.parity_report import build_parity_report

    job_id = _job(store, [], missing_rows=3)
    _accept_all(store, job_id)
    _set_evidence(store, job_id, [], missing_rows=30)
    report = build_parity_report(store.get_job(job_id), reread=False, job_id=job_id)
    assert report["parity"]["unexplained"] == 27


def test_accepting_needs_a_note(store):
    from services.parity_fixes import approve_fix
    from services.parity_report import build_parity_report
    from services.parity_state import ParityInvalid

    job_id = _job(store, [1])
    report = build_parity_report(store.get_job(job_id), reread=False, job_id=job_id)
    accept = next(f for f in report["fixes"] if f["action"] == "accept")
    with pytest.raises(ParityInvalid):
        approve_fix(job_id, accept["id"], actor="owner@example.com", note="   ")
    assert not store.get_job(job_id).get("parity", {}).get("acceptances")
