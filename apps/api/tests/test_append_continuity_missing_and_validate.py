"""X-97 follow-up: a dropped destination is named as dropped, Validate sees the
same continuity verdict Execute enforces, and an audited removal recovers.

Found in the Transfer Studio walk: the destination SQLite file was deleted
between runs, Validate reported execute-ready, then Execute failed with
"could not be re-read" — wrong cause, and no way forward from the UI.
"""

from __future__ import annotations

from preflight.models import GateStatus
from services import append_continuity as ac
from tests.test_x97_append_continuity import (  # noqa: F401 — fixture import
    _BASE,
    _Dest,
    _runner,
    _source,
    fake_store,
)

PASS, BLOCK = GateStatus.PASS.value, GateStatus.BLOCK.value
CONTRACT = [{"name": "t", "sync_mode": "incremental_append", "selected": True, "cursor_field": "seq"}]


def _gate(dest: _Dest, sync: str = "incremental_append"):
    return ac.continuity_preflight_gate(
        dest.endpoint(), sync=sync, contracts=CONTRACT, pass_status=PASS, block_status=BLOCK
    )


def test_compare_names_a_dropped_table_as_missing_not_unreadable():
    v = ac.compare("orders", _BASE, None, dest_missing=True)
    assert v is not None
    assert v.details["mismatch"] == "destination_missing"
    assert "no longer exists" in v.message and "10 rows" in v.message
    assert "accept the removal" in v.details["recovery"]
    assert ac.compare("orders", _BASE, None, dest_missing=None).details["mismatch"] == "unreadable"


def test_dropped_sqlite_file_fails_run2_without_recreating_it(tmp_path, fake_store):
    dest = _Dest("sqlite", tmp_path)
    run = _runner(fake_store, _source(tmp_path, pk=True), dest, pk=True)
    assert run().success
    dest.path.unlink()
    r2 = run()
    assert not r2.success
    assert r2.error_details["mismatch"] == "destination_missing"
    assert "no longer exists" in r2.error
    assert not dest.path.exists(), "a refused run must not create the destination file"


def test_dropped_table_in_existing_sqlite_file_is_missing(tmp_path, fake_store):
    dest = _Dest("sqlite", tmp_path)
    run = _runner(fake_store, _source(tmp_path, pk=True), dest, pk=True)
    assert run().success
    dest._sql(f'DROP TABLE "{dest.table}"')
    r2 = run()
    assert not r2.success and r2.error_details["mismatch"] == "destination_missing"


def _full_append_runner(fake, src, dest: _Dest):
    """Transfer Studio's default route mode: full_refresh_append, no cursor."""
    import uuid

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    def run():
        jid = "x97m-" + uuid.uuid4().hex[:12]
        fake.update_job_status(jid, "pending", transfer_request={})
        req = TransferRequest(
            source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
            destination=dest.endpoint(),
            sync_mode="full_refresh_append",
            skip_preflight=True,
            validation_mode="strict",
        )
        return UniversalTransferEngine().execute_tracked(req, jid)

    return run


def test_audited_removal_clears_baseline_and_next_append_starts_fresh(
    tmp_path, fake_store, monkeypatch
):
    import services.audit_log as audit_log

    events: list[dict] = []
    monkeypatch.setattr(audit_log, "append_audit_event", lambda **kw: events.append(kw) or kw)
    dest = _Dest("sqlite", tmp_path)
    run = _full_append_runner(fake_store, _source(tmp_path, pk=True), dest)
    assert run().success
    dest.path.unlink()
    out = ac.rebaseline(dest.endpoint(), actor="ops@x", reason="dropped on purpose")
    assert out["destination_missing"] is True and out["current"] is None
    rebaselined = [e for e in events if e.get("action") == "append_continuity.rebaselined"]
    assert len(rebaselined) == 1 and rebaselined[0]["details"]["destination_missing"] is True
    assert ac.load_baseline(ac.resolve_target(dest.endpoint()).key) is None
    r3 = run()
    assert r3.success, r3.error
    assert len(dest.snapshot()) == 10


def test_removal_is_refused_while_the_table_still_exists_but_is_unreadable(
    tmp_path, fake_store, monkeypatch
):
    dest = _Dest("sqlite", tmp_path)
    run = _runner(fake_store, _source(tmp_path, pk=True), dest, pk=True)
    assert run().success
    monkeypatch.setattr(ac, "fingerprint", lambda *a, **k: None)
    import pytest

    with pytest.raises(ac.RebaselineError):
        ac.rebaseline(dest.endpoint(), actor="ops@x", reason="r")


def test_validate_gate_matches_execute_verdict(tmp_path, fake_store):
    dest = _Dest("sqlite", tmp_path)
    assert _gate(dest) is None, "no baseline yet: nothing to prove"
    run = _runner(fake_store, _source(tmp_path, pk=True), dest, pk=True)
    assert run().success

    ok = _gate(dest)
    assert ok["id"] == "g21_append_continuity" and ok["status"] == PASS
    assert ok["details"]["found_rows"] == 10 and ok["details"]["scope"] == "row_count_only"
    assert "re-verifies the full value digest" in ok["message"]
    assert _gate(dest, sync="full_refresh_overwrite") is None

    dest.tamper_one("delete")
    drift = _gate(dest)
    assert drift["status"] == BLOCK and drift["details"]["mismatch"] == "row_count"

    dest.path.unlink()
    gone = _gate(dest)
    assert gone["status"] == BLOCK and gone["details"]["mismatch"] == "destination_missing"
    assert not dest.path.exists()


def test_validate_gate_accepts_plan_dict_endpoints(tmp_path, fake_store):
    dest = _Dest("sqlite", tmp_path)
    run = _runner(fake_store, _source(tmp_path, pk=True), dest, pk=True)
    assert run().success
    gate = ac.continuity_preflight_gate(
        {"kind": "database", "format": "sqlite", "database": str(dest.path), "table": dest.table},
        sync="full_refresh_append",
        contracts=[],
        pass_status=PASS,
        block_status=BLOCK,
    )
    assert gate is not None and gate["status"] == PASS


def test_same_count_value_edit_passes_validate_but_execute_still_refuses(tmp_path, fake_store):
    """Validate's count is never sold as proof: the digest check stays at Execute."""
    dest = _Dest("sqlite", tmp_path)
    run = _runner(fake_store, _source(tmp_path, pk=True), dest, pk=True)
    assert run().success
    dest.tamper_one("cell")
    assert _gate(dest)["status"] == PASS
    r2 = run()
    assert not r2.success and r2.error_details["mismatch"] == "checksum"
