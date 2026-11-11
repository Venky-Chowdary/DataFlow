"""Parity Studio end to end on the real engine route: SQLite -> SQLite via execute_tracked.

The destination corrupts rows in known ways while the run writes them (SQLite
triggers — a real-world cause): it rounds ``amount`` to 2 decimal places on
keys 7, 8, 9 and deletes key 13. The run trims trailing spaces from ``note``
with a recipe step, and the destination already held one row (key 1000).

Gate-8 names the keys; the parity report must explain the trim (your
transform) and the pre-existing row, leave the scale cut and the missing row
unexplained, propose a re-sync of exactly those keys, refuse an in-place widen
on SQLite, repair the rows when the re-sync is approved (round 1: 4 -> 0), then
sign with all three roles and export a pack the existing verify route accepts.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest, transfer_request_to_dict  # noqa: E402


class _Store:
    """The fake job store, with the keyword the real stores take."""

    def __init__(self) -> None:
        from tests.test_property2_golden_path_never_blocked import _FakeMongo

        self._inner = _FakeMongo()
        self.jobs = self._inner.jobs

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def update_job_fields(self, job_id: str, fields: dict, **_kw) -> bool:
        return self._inner.update_job_fields(job_id, fields)

    def get_job(self, job_id: str):
        job = self._inner.get_job(job_id)
        return {**job, "_id": job_id} if job is not None else None


@pytest.fixture()
def store(monkeypatch, tmp_path):
    import services.mongodb_service as mongodb_service
    import services.sync_cursor as sc
    from services import audit_log

    fake = _Store()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setenv("DATAFLOW_LLM_ENABLED", "false")
    return fake


def _exec(path: Path, *sql: str) -> None:
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            for stmt in sql:
                conn.execute(stmt)
    finally:
        conn.close()


def _corrupting_route(tmp_path: Path) -> tuple[Path, Path]:
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _exec(src, "CREATE TABLE pay (id INTEGER PRIMARY KEY, note TEXT, amount NUMERIC(12,4))")
    conn = sqlite3.connect(str(src))
    with conn:
        conn.executemany(
            "INSERT INTO pay VALUES (?, ?, ?)",
            [(i, f"n{i}  " if i % 5 == 0 else f"n{i}", f"{i}.1249") for i in range(1, 41)],
        )
    conn.close()
    _exec(
        dst,
        "CREATE TABLE pay (id INTEGER PRIMARY KEY, note TEXT, amount NUMERIC(12,4))",
        "CREATE TRIGGER cut AFTER INSERT ON pay WHEN NEW.id IN (7, 8, 9) BEGIN "
        "UPDATE pay SET amount = round(NEW.amount, 2) WHERE id = NEW.id; END",
        "CREATE TRIGGER drop13 AFTER INSERT ON pay WHEN NEW.id = 13 BEGIN "
        "DELETE FROM pay WHERE id = 13; END",
        "INSERT INTO pay VALUES (1000, 'kept', 1.5)",
    )
    return src, dst


def _request(src: Path, dst: Path) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="pay"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="pay"),
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "note", "target": "note"},
            {"source": "amount", "target": "amount"},
        ],
        shape_recipe={"steps": [{"op": "trim", "column": "note"}]},
        stream_contracts=[{"name": "pay", "primary_key": ["id"], "sync_mode": "upsert", "selected": True}],
        sync_mode="upsert",
        skip_preflight=True,
        validation_mode="strict",
    )


def _run(store: _Store, req: TransferRequest) -> str:
    job_id = "parity" + uuid.uuid4().hex[:16]
    store.update_job_status(job_id, "pending", transfer_request=transfer_request_to_dict(req),
                            workspace_id="ws-parity")
    UniversalTransferEngine().execute_tracked(req, job_id)
    return job_id


def _by_cause(report: dict) -> dict:
    return {(d["column"], d["kind"], d["cause"]["code"]): d for d in report["differences"]}


def test_parity_loop_on_a_real_engine_run(store, tmp_path):
    from services.audit_log import list_audit_events
    from services.parity_fixes import ParityConflict, approve_fix, public_fix
    from services.parity_report import build_parity_report, public_report
    from services.parity_signoff import export_markdown, sign_off, signed_pack
    from services.signed_proof_pack import verify_signed_proof_pack

    src, dst = _corrupting_route(tmp_path)
    job_id = _run(store, _request(src, dst))
    job = store.get_job(job_id)
    assert job["reconciliation"]["passed"] is False  # Gate-8 caught it; parity explains it

    report = build_parity_report(job, job_id=job_id)
    assert report["verdict"] == "unexplained"
    assert report["parity"]["rows_source"] == 40
    assert report["parity"]["unexplained"] == 4
    assert report["evidence"]["reread"]["pay"] == {"status": "ok", "keys": 4}
    diffs = _by_cause(report)

    trimmed = diffs[("note", "value", "transform_applied")]
    assert trimmed["cause"]["explained"] is True and trimmed["rows"] == 8
    scale = diffs[("amount", "value", "numeric_scale")]
    assert scale["cause"]["explained"] is False
    assert sorted(scale["keys_sample"], key=int) == ["7", "8", "9"]
    assert scale["example"] == {"source": "7.1249", "destination": "7.12"}
    assert scale["cause"]["detail"] == "Scale rounded from 4 to 2 decimal place(s)."
    missing = diffs[(None, "missing_in_destination", "unexplained")]
    assert missing["keys_sample"] == ["13"] and missing["cause"]["explained"] is False
    kept = diffs[(None, "extra_in_destination", "out_of_scope_population")]
    assert kept["rows"] == 1 and kept["cause"]["explained"] is True
    assert len(report["differences"]) == 4
    assert report["tables"][0]["status"] == "unexplained"
    assert report["signoff"]["status"] == "blocked"

    fixes = {f["action"]: f for f in report["fixes"]}
    widen = fixes["widen_column"]
    assert widen["applicable"] is False
    assert "sqlite cannot change a column type in place" in widen["not_applicable_reason"]
    resync = fixes["resync_keys"]
    assert resync["applicable"] is True
    assert sorted(resync["_keys"], key=int) == ["7", "8", "9", "13"]
    assert set(resync["difference_ids"]) == {scale["id"], missing["id"]}
    assert [f["action"] for f in report["fixes"]][:2] == ["widen_column", "resync_keys"]
    with pytest.raises(ParityConflict):
        approve_fix(job_id, widen["id"], actor="ops@example.com")

    # The cause is removed (the operator dropped the triggers), then the fix is approved.
    _exec(dst, "DROP TRIGGER cut", "DROP TRIGGER drop13")
    after = approve_fix(job_id, resync["id"], actor="ops@example.com", note="triggers removed")
    assert after["verdict"] == "explained"
    assert after["parity"]["unexplained"] == 0
    assert after["rounds"] == [
        {**after["rounds"][0], "n": 1, "unexplained_before": 4, "unexplained_after": 0,
         "fix_id": resync["id"]}
    ]
    applied = next(f for f in after["fixes"] if f["id"] == resync["id"])
    assert applied["status"] == "applied" and applied["approved_by"] == "ops@example.com"
    assert "Re-synced 4 key(s)" in applied["result"]
    conn = sqlite3.connect(str(dst))
    try:
        assert conn.execute("SELECT amount FROM pay WHERE id = 7").fetchone()[0] == pytest.approx(7.1249)
        assert conn.execute("SELECT count(*) FROM pay WHERE id = 13").fetchone()[0] == 1
        assert conn.execute("SELECT note FROM pay WHERE id = 5").fetchone()[0] == "n5"
    finally:
        conn.close()
    with pytest.raises(ParityConflict):
        approve_fix(job_id, resync["id"], actor="ops@example.com")  # idempotent: second approve

    # Sign-off: three roles, three people; the third signature seals the report.
    sign_off(job_id, role="engineering", signer_role="editor", actor="eng@example.com", note="checked")
    with pytest.raises(ParityConflict):
        sign_off(job_id, role="data_owner", signer_role="editor", actor="eng@example.com")  # one person, one role
    sign_off(job_id, role="data_owner", signer_role="editor", actor="owner@example.com")
    signed = sign_off(job_id, role="compliance", signer_role="admin", actor="comp@example.com")
    assert signed["signoff"]["status"] == "signed"
    assert signed["signoff"]["signature"].startswith("hmac-sha256:")
    pack = signed_pack(store.get_job(job_id))
    assert verify_signed_proof_pack(pack)["ok"] is True
    assert pack["parity"]["verdict"] == "explained"
    tampered = {**pack, "parity": {**pack["parity"], "verdict": "proven"}}
    assert verify_signed_proof_pack(tampered)["ok"] is False

    md = export_markdown(store.get_job(job_id), job_id)
    assert "**Verdict:** explained" in md
    assert "Every row was compared" in md
    assert "| pay | amount |" not in md  # resolved differences are gone from the signed report
    assert "Re-sync 4 key(s) in pay by upsert — applied by ops@example.com" in md
    assert "- compliance: comp@example.com" in md
    assert f"hmac-sha256:{pack['signature']['value']}" in md
    assert "/api/v1/transfer/proof-pack/verify" in md

    actions = [e.get("action") for e in list_audit_events(limit=200)]
    for name in ("job.parity.fix_approved", "job.parity.fix_applied", "job.parity.signed_off"):
        assert name in actions, name
    assert public_report(signed)["fixes"] and all(
        not k.startswith("_") for f in signed["fixes"] for k in public_fix(f)
    )


def test_a_failed_write_is_recorded_and_not_counted_as_resolved(store, tmp_path, monkeypatch):
    from services import parity_fixes
    from services.audit_log import list_audit_events
    from services.parity_fixes import approve_fix
    from services.parity_report import build_parity_report

    src, dst = _corrupting_route(tmp_path)
    job_id = _run(store, _request(src, dst))
    report = build_parity_report(store.get_job(job_id), job_id=job_id)
    resync = next(f for f in report["fixes"] if f["action"] == "resync_keys")

    def _refuse(*_a, **_k):
        raise RuntimeError("re-sync job x failed: destination locked")

    monkeypatch.setattr(parity_fixes, "_resync_apply", _refuse)
    after = approve_fix(job_id, resync["id"], actor="ops@example.com")
    failed = next(f for f in after["fixes"] if f["id"] == resync["id"])
    assert failed["status"] == "failed" and "destination locked" in failed["result"]
    assert after["rounds"] == []
    assert after["parity"]["unexplained"] == 4
    assert "job.parity.fix_failed" in [e.get("action") for e in list_audit_events(limit=100)]
    retry = [f for f in after["fixes"] if f["action"] == "resync_keys" and f["status"] == "proposed"]
    assert retry and retry[0]["id"] == f"{resync['id']}-2"


def test_accepting_every_unexplained_difference_unblocks_sign_off(store, tmp_path):
    from services.parity_fixes import approve_fix
    from services.parity_report import build_parity_report

    src, dst = _corrupting_route(tmp_path)
    job_id = _run(store, _request(src, dst))
    report = build_parity_report(store.get_job(job_id), job_id=job_id)
    for fix in [f for f in report["fixes"] if f["action"] == "accept"]:
        report = approve_fix(job_id, fix["id"], actor="owner@example.com", note="known rounding")
    assert report["parity"]["unexplained"] == 0
    assert report["verdict"] == "explained"
    accepted = [d for d in report["differences"] if d["cause"]["code"] == "accepted_by_operator"]
    assert len(accepted) == 2
    assert all("owner@example.com" in d["cause"]["detail"] for d in accepted)
    assert report["signoff"]["status"] == "open"
    assert report["rounds"] == []  # accepting writes nothing, so it is not a re-check round
