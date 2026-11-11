"""Re-review 4, finding 4 (Medium) and 9d (Low): an approve binds the rows a person reviewed.

4: an ``accept`` had no preview, so neither ``expected_version`` nor ``preview_sha256`` bound
its rows. A live re-read changes the rows in a difference without bumping ``version``: the
page showed ``letter_case`` with 1 row (key 1), the destination changed, and the approve
recorded an acceptance of 20 keys. An accept now carries a preview of its rows and keys, and
its digest binds every key it covers.

9d: the re-sync preview names the first 20 keys and the count, and its digest was only of that
text: a different 21st key read as the same preview.
"""

from __future__ import annotations

import pytest

from services.mongodb_service import MemoryMongoDBService


@pytest.fixture()
def store(monkeypatch, tmp_path):
    import services.mongodb_service as mongodb_service
    from services import audit_log, parity_cache

    parity_cache.clear()
    svc = MemoryMongoDBService()
    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: svc)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setenv("DATAFLOW_LLM_ENABLED", "false")
    yield svc
    parity_cache.clear()


def _job(svc, keys: list[str]) -> str:
    job_id = svc.create_transfer_job({
        "name": "t", "workspace_id": "w",
        "reconciliation": {"passed": False, "source_rows": 100, "target_rows": 100,
                           "culprit_keys": {"changed": keys}},
        "transfer_request": {"mappings": [{"source": "id", "target": "id"},
                                          {"source": "name", "target": "name", "target_type": "VARCHAR(40)"}],
                             "source": {"kind": "database"}, "destination": {"kind": "database"}},
        "destination_summary": {"table": "t", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"},
    })
    svc.update_job_status(job_id, "completed")
    return job_id


def test_an_accept_of_rows_that_changed_since_the_review_is_refused(store, monkeypatch):
    from services import parity_cache, parity_report
    from services.parity_fixes import ParityConflict, approve_fix
    from services.parity_report import build_parity_report, public_report

    lowered = {"n": 1}

    def reread(job, ev, keys):  # rows whose destination name is lower-cased now
        return {k: {"raw": {"id": k, "name": "Acme"}, "expected": {"id": k, "name": "Acme"},
                    "dest": {"id": k, "name": "acme" if int(k) <= lowered["n"] else "Acme X"}, "held_out": False}
                for k in keys}

    monkeypatch.setattr(parity_report, "reread_cells", reread)
    job_id = _job(store, [str(i) for i in range(1, 21)])
    seen = public_report(build_parity_report(store.get_job(job_id), job_id=job_id, cache="use"))
    diff = next(d for d in seen["differences"] if d["cause"]["code"] == "letter_case")
    fix = next(f for f in seen["fixes"] if f["action"] == "accept" and diff["id"] in f["difference_ids"])
    assert diff["rows"] == 1
    assert fix["preview"] and "1 row(s)" in fix["preview"] and fix["preview_sha256"]

    lowered["n"] = 20  # the destination changed after the page read; the version did not move
    parity_cache.clear()
    with pytest.raises(ParityConflict):
        approve_fix(job_id, fix["id"], actor="ops@x", note="case only", expected_version=seen["version"],
                    preview_sha256=fix["preview_sha256"])
    assert not (store.get_job(job_id).get("parity") or {}).get("acceptances")


def test_an_accept_of_the_rows_reviewed_records_them(store):
    from services.parity_fixes import approve_fix
    from services.parity_report import build_parity_report

    job_id = _job(store, ["1", "2"])
    report = build_parity_report(store.get_job(job_id), reread=False, job_id=job_id)
    fix = next(f for f in report["fixes"] if f["action"] == "accept")
    approve_fix(job_id, fix["id"], actor="ops@x", note="known", expected_version=report["version"],
                preview_sha256=fix["preview_sha256"])
    [acc] = store.get_job(job_id)["parity"]["acceptances"].values()
    assert sorted(acc["keys"]) == ["1", "2"]


def test_a_protected_key_never_shows_in_the_accept_preview():
    from services.parity_report import build_parity_report

    job = {"_id": "j", "status": "completed",
           "reconciliation": {"passed": False, "source_rows": 10, "target_rows": 10, "culprit_keys": {"changed": ["123-45"]}},
           "transfer_request": {"mappings": [{"source": "ssn", "target": "ssn", "transform": "hash_pii"}]},
           "destination_summary": {"table": "t", "primary_key_columns": ["ssn"], "sync_mode": "full_refresh_overwrite"}}
    fix = next(f for f in build_parity_report(job, reread=False)["fixes"] if f["action"] == "accept")
    assert "123-45" not in fix["preview"] and "protected" in fix["preview"]


def test_the_key_digest_binds_every_key_not_only_the_first_twenty():
    from services.parity_fixes import _keys_preview, preview_digest

    a = [str(i) for i in range(1, 22)]
    b = a[:20] + ["999"]
    assert _keys_preview("t", "id", a) == _keys_preview("t", "id", b)  # the text shows 20 keys and the count
    assert preview_digest(_keys_preview("t", "id", a), a) != preview_digest(_keys_preview("t", "id", b), b)
    assert preview_digest("ALTER TABLE x") == __import__("hashlib").sha256(b"ALTER TABLE x").hexdigest()
