"""Re-review 4, finding 5 (Medium, fails closed): accepting a "beyond acceptance" difference works.

Keys 1 and 2 were accepted, then key 3 appeared in the same difference. Approving the proposed
accept for key 3 marked the fix ``applied`` ("Differences accepted"), but only the original id
was looked up in the acceptances, so key 3 stayed unexplained and the accept was proposed again,
forever. The acceptances of a difference and of its ``|beyond_acceptance`` part now count together.
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


def _job(svc) -> str:
    job_id = svc.create_transfer_job({
        "_id": "beyond" + uuid.uuid4().hex[:8], "workspace_id": "w", "status": "completed",
        "reconciliation": {"passed": False, "source_rows": 100, "target_rows": 100,
                           "culprit_keys": {"changed": ["1", "2"]}},
        "destination_summary": {"table": "orders", "primary_key_columns": ["id"],
                                "sync_mode": "full_refresh_overwrite"},
    })
    svc.update_job_status(job_id, "completed")
    return job_id


def _name_keys(svc, job_id: str, keys: list[str]) -> None:
    recon = dict(svc.get_job(job_id)["reconciliation"])
    recon["culprit_keys"] = {"changed": keys}
    svc.update_job_fields(job_id, {"reconciliation": recon})


def _accept_open(svc, job_id: str, note: str) -> dict:
    from services.parity_fixes import approve_fix
    from services.parity_report import build_parity_report

    report = build_parity_report(svc.get_job(job_id), reread=False, job_id=job_id)
    fix = next(f for f in report["fixes"] if f["action"] == "accept" and f["status"] == "proposed")
    return approve_fix(job_id, fix["id"], actor="ops@x", note=note, expected_version=report["version"],
                       preview_sha256=fix["preview_sha256"])


def test_accepting_the_rows_beyond_an_acceptance_explains_them(store):
    from services.parity_report import build_parity_report

    job_id = _job(store)
    _accept_open(store, job_id, "1 and 2 are known")
    _name_keys(store, job_id, ["1", "2", "3"])
    _accept_open(store, job_id, "3 too")
    report = build_parity_report(store.get_job(job_id), reread=False, job_id=job_id)
    assert report["verdict"] == "explained", report["verdict_reason"]
    assert report["parity"]["unexplained"] == 0
    assert not [f for f in report["fixes"] if f["action"] == "accept" and f["status"] == "proposed"]
    [accepted] = report["differences"]
    assert sorted(accepted["keys_sample"]) == ["1", "2", "3"]
    assert "1 and 2 are known" in accepted["cause"]["detail"] and "3 too" in accepted["cause"]["detail"]


def test_a_second_beyond_acceptance_keeps_the_first(store):
    from services.parity_report import build_parity_report

    job_id = _job(store)
    _accept_open(store, job_id, "first")
    _name_keys(store, job_id, ["1", "2", "3"])
    _accept_open(store, job_id, "second")
    _name_keys(store, job_id, ["1", "2", "3", "4"])
    report = build_parity_report(store.get_job(job_id), reread=False, job_id=job_id)
    [open_diff] = [d for d in report["differences"] if not d["cause"]["explained"]]
    assert open_diff["keys_sample"] == ["4"]
    _accept_open(store, job_id, "third")
    report = build_parity_report(store.get_job(job_id), reread=False, job_id=job_id)
    assert report["verdict"] == "explained", report["verdict_reason"]
