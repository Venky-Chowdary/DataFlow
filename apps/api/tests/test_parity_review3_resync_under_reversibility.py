"""Parity Studio's key re-sync on a job that must be reversible (reversibility review 3, M5).

The re-sync child inherits ``require_reversibility`` from its parent and adds
a ``source_filter`` naming the keys to restore. The reversibility gate read
that filter as a row change ("pay: source filter") and blocked the repair of
the very job it protects. The child now carries a recorded exemption from the
row-change rule only: its columns are still analysed and enforced.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


from tests.test_parity_e2e_engine import _exec, _run, store  # noqa: F401
from src.transfer.models import EndpointConfig, TransferRequest, transfer_request_to_dict


def _route(tmp_path: Path) -> tuple[Path, Path]:
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _exec(src, "CREATE TABLE pay (id INTEGER PRIMARY KEY, note TEXT, qty INTEGER)")
    conn = sqlite3.connect(str(src))
    with conn:
        conn.executemany("INSERT INTO pay VALUES (?, ?, ?)", [(i, f"n{i}", i * 3) for i in range(1, 41)])
    conn.close()
    _exec(dst, "CREATE TABLE pay (id INTEGER PRIMARY KEY, note TEXT, qty INTEGER)",
          "CREATE TRIGGER drop13 AFTER INSERT ON pay WHEN NEW.id = 13 BEGIN DELETE FROM pay WHERE id = 13; END",
          "INSERT INTO pay VALUES (1000, 'kept', 1)")  # a destination that was not empty keeps the run's rows
    return src, dst


def _request(src: Path, dst: Path, **over) -> TransferRequest:
    def col(name: str, kind: str) -> dict:
        return {"source": name, "target": name, "user_override": True, "source_type": kind, "target_type": kind}

    body = dict(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="pay"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="pay"),
        mappings=[col("id", "INTEGER"), col("note", "TEXT"), col("qty", "INTEGER")],
        stream_contracts=[{"name": "pay", "primary_key": ["id"], "sync_mode": "upsert", "selected": True}],
        sync_mode="upsert", skip_preflight=True, validation_mode="strict", require_reversibility=True,
    )
    body.update(over)
    return TransferRequest(**body)


def test_m5_a_key_resync_of_a_reversible_job_is_not_blocked_by_its_own_key_filter(store, tmp_path):  # noqa: F811
    from services.audit_log import list_audit_events
    from services.parity_fixes import approve_fix
    from services.parity_report import build_parity_report
    from services.reversibility_sources import job_report

    src, dst = _route(tmp_path)
    job_id = _run(store, _request(src, dst))
    report = build_parity_report(store.get_job(job_id), job_id=job_id)
    resync = next(f for f in report["fixes"] if f["action"] == "resync_keys")
    assert resync["applicable"] is True and resync["_keys"] == ["13"]
    _exec(dst, "DROP TRIGGER drop13")  # the cause is removed, then the fix is approved

    after = approve_fix(job_id, resync["id"], actor="ops@example.com", note="trigger removed")
    applied = next(f for f in after["fixes"] if f["id"] == resync["id"])
    assert applied["status"] == "applied", applied["result"]
    assert after["parity"]["unexplained"] == 0
    conn = sqlite3.connect(str(dst))
    try:
        assert conn.execute("SELECT note, qty FROM pay WHERE id = 13").fetchone() == ("n13", 39)
    finally:
        conn.close()

    # The waiver is on the child's stored request and in the audit trail; the flag itself is kept.
    child = store.get_job(applied["child_job_id"])
    stored = child["transfer_request"]
    assert stored["require_reversibility"] is True
    assert stored["reversibility_row_rule_exemption"] == "parity_key_resync" and stored["parent_job_id"] == job_id
    waived = [e for e in list_audit_events(limit=100) if e.get("action") == "job.parity.resync_row_rule_waived"]
    assert len(waived) == 1 and waived[0]["details"]["child_job_id"] == applied["child_job_id"]
    assert waived[0]["details"]["keys"] == 1 and waived[0]["details"]["exemption"] == "parity_key_resync"
    view = job_report(child, applied["child_job_id"])
    assert view["gate"]["status"] == "pass" and view["row_rule_exemption"] == "parity_key_resync"


def test_m5_the_waiver_covers_the_row_rule_only_and_only_a_parity_child(tmp_path):
    from services.parity_apply import _resync_request
    from services.reversibility_sources import execute_gate

    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    types = {"id": "INTEGER", "note": "TEXT", "qty": "INTEGER"}
    fix = {"id": "fix1", "_keys": [3, 7], "_key_column": "id"}

    def gate(request):
        return execute_gate(request, (request.mappings, types, types, [{"id": 3, "note": "n3", "qty": 9}], 1))

    parent = {"transfer_request": transfer_request_to_dict(_request(src, dst)), "workspace_id": "ws1"}
    child = _resync_request(parent, "job1", fix, "op@example.com")
    assert child.source_filter and child.reversibility_row_rule_exemption == "parity_key_resync"
    assert gate(child)["status"] == "pass"
    # A lossy column on the child still blocks: only the key filter is waived.
    lossy = _request(src, dst)
    lossy.mappings[1]["transform"] = "hash_pii"
    blocked = gate(_resync_request({"transfer_request": transfer_request_to_dict(lossy)}, "job1", fix, "op"))
    assert blocked["status"] == "block" and "pay.note" in blocked["message"] and "source filter" not in blocked["message"]
    # An ordinary request cannot claim the waiver (it names no parent job), and a filter still blocks it.
    forged = _request(src, dst, source_filter={"column": "id", "op": "in", "value": [3]},
                      reversibility_row_rule_exemption="parity_key_resync")
    assert gate(forged)["status"] == "block" and "source filter" in gate(forged)["message"]
    # A parent that does not require reversibility gets no waiver stamped on its child.
    plain = _resync_request({"transfer_request": transfer_request_to_dict(
        _request(src, dst, require_reversibility=False))}, "job1", fix, "op")
    assert plain.reversibility_row_rule_exemption == ""
