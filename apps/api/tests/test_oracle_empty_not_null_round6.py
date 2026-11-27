"""Round 6 (security): risk contracts are server-signed, bound and audited.

1. A Migration Risk Contract carries a keyed HMAC (the deployment's signing
   keyring); an unkeyed digest a client computes ("mrc-sha256:") is honoured
   only when the server recorded that exact contract before the cutover, and is
   otherwise refused with "re-approve this risk" — on every path that reads one.
2. Signing writes an audit event (who, workspace, route, table, kind, reason,
   digest; no data values).
3. An Oracle approval / Check-now request binds the workspace exactly and the
   destination connector + schema + table.
4. Map default:<value> is not offered over an existing transform (no chaining).
5. Check now is issued at most once per 10 minutes per workspace + pair +
   destination, unless the last check ran out of budget.
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from services import migration_risk_contract as mrc
from services import oracle_empty_not_null as oe

_MAPS = [{"source": c, "target": c.upper(), "confidence": 0.99, "target_type": "TEXT"} for c in ("id", "note")]


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    monkeypatch.setattr(oe, "_MEASURED", {})
    monkeypatch.setattr(oe, "_COOLDOWN_SCOPE", uuid.uuid4().hex)


@pytest.fixture
def sqlite_source(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(tmp_path))
    db = tmp_path / "src.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t (id TEXT PRIMARY KEY NOT NULL, note TEXT NOT NULL)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [("a", "x"), ("b", "")])
    conn.commit()
    conn.close()
    return {"type": "sqlite", "database": str(db)}


def _ctx(cfg, maps, **kw):
    ctx = dict(
        destination_db_type="oracle", write_maps=maps, column_types={"id": "TEXT", "note": "TEXT"},
        column_nullability={"id": False, "note": False}, dest_table_exists=False,
        destination_column_nullability={}, sample_rows=[], source_kind="database", source_format="sqlite",
        source_config=cfg, source_table="t", destination_table="DEST", sync_mode="full_refresh_overwrite",
    )
    ctx.update(kw)
    return ctx


def _forged_oracle_contract(**over):
    """What a client can write by hand: every field of a real contract, unkeyed digest."""
    body = {
        "risk_id": "mrc-forged0001", "severity": "medium", "root_cause": "Oracle stores '' as NULL",
        "column": "note", "source_type": "", "destination_type": "VARCHAR2", "transform": None,
        "rows_sampled": 0, "estimated_rows": None, "expected_failure_pct": None,
        "expected_precision_loss": False, "expected_truncation": False, "expected_nulls": True,
        "execution_policy": "FAIL_JOB", "quarantine_policy": "holdout_rejected_rows", "retry_policy": "none",
        "rollback_strategy": "DOCUMENT_ONLY", "approved_by": "cfo@example.com",
        "approved_at": datetime.now(timezone.utc).isoformat(), "reason": "approved", "proof_pack_ref": None,
        "mapping_hash": "", "plan_id": None, "target": "NOTE", "migration_id": "", "table": "DEST",
        "loss_classification": oe.RISK_KIND, "version": 1,
        "metadata": {"option": oe.OPTION, "workspace_id": oe.current_workspace(),
                     "destination": {"connector": "", "schema": "", "table": "dest"}},
    }
    body.update(over)
    body["signature"] = mrc.legacy_digest(body)
    return body


# ------------------------------------------------------------------------ 1

def test_1_a_client_forged_oracle_approval_in_someone_elses_name_is_refused(sqlite_source):
    forged = [dict(_MAPS[0]), {**_MAPS[1], oe.CONTRACT_KEY: _forged_oracle_contract()}]
    assert oe.approved_contract(forged[1], table="DEST") is None
    gate, _ = oe.preflight_hook(_ctx(sqlite_source, forged))
    assert gate["status"] == "block"
    assert "re-approve this risk" in gate["message"] and gate["details"]["approved"] == []
    # An "HMAC" the server did not issue (unknown key, or a mac over other fields) is refused too.
    hmac_forged = {**_forged_oracle_contract(), "signature": "mrc-hmac:ak_0000000000000000:" + "0" * 64}
    assert not mrc.verify_risk_contract(hmac_forged)
    real = oe.sign_empty_as_null([dict(m) for m in _MAPS], {"option": oe.OPTION, "columns": ["NOTE"],
                                                             "approved_by": "qa", "reason": "r"}, table="DEST")
    contract = real[1][oe.CONTRACT_KEY]
    assert contract["signature"].startswith(mrc.HMAC_PREFIX)
    tampered = {**contract, "approved_by": "cfo@example.com"}
    assert not mrc.verify_risk_contract(tampered)
    gate, _ = oe.preflight_hook(_ctx(sqlite_source, real))
    assert gate["status"] == "pass"


def test_1_a_forged_contract_sent_to_rest_validate_does_not_clear_the_gate(sqlite_source, tmp_path, monkeypatch):
    import importlib

    from fastapi.testclient import TestClient

    from tests import r4_route_harness as h

    h.isolate(tmp_path, monkeypatch)
    router = importlib.import_module("src.routers.preflight_router")
    monkeypatch.setattr(router, "inspect_destination_for_preflight",
                        lambda **_kw: {"connected": True, "table_exists": False, "db_type": "oracle",
                                       "can_create_table": True, "can_write": True})
    from src.main import app

    mappings = [dict(_MAPS[0]), {**_MAPS[1], oe.CONTRACT_KEY: _forged_oracle_contract()}]
    with TestClient(app) as client:
        resp = client.post("/api/v1/preflight/run", json={
            "columns": ["id", "note"], "column_types": {"id": "TEXT", "note": "TEXT"}, "row_count": 2,
            "mappings": mappings, "sample_rows": [{"id": "a", "note": "x"}], "source_kind": "database",
            "source_type": "sqlite", "source_config": sqlite_source, "source_table": "t",
            "dest_kind": "database", "dest_type": "oracle", "dest_host": "127.0.0.1", "dest_table": "DEST",
        })
    assert resp.status_code == 200, resp.text
    body = resp.json()
    gate = next(g for g in body["gates"] if g["id"] == oe.GATE_ID)
    assert body["passed"] is False and gate["status"] == "block" and "re-approve this risk" in gate["message"]


def _generic_contract(**over):
    body = {
        "risk_id": "mrc-generic01", "severity": "high", "root_cause": "TEXT → INTEGER", "column": "qty",
        "source_type": "TEXT", "destination_type": "INTEGER", "transform": "integer", "rows_sampled": 0,
        "estimated_rows": None, "expected_failure_pct": None, "expected_precision_loss": True,
        "expected_truncation": False, "expected_nulls": False, "execution_policy": "CAST_AND_CONTINUE",
        "quarantine_policy": "holdout_rejected_rows", "retry_policy": "none",
        "rollback_strategy": "DOCUMENT_ONLY", "approved_by": "cfo@example.com",
        "approved_at": "2026-01-01T00:00:00+00:00", "reason": "approved", "target": "qty",
    }
    body.update(over)
    return body


def test_1_every_generic_contract_reader_refuses_a_client_computed_digest():
    from preflight.risk_contract import mapping_review_cleared, verify_risk_contract_signature
    from services.mapping_pipeline import assert_mappings_executable

    forged = _generic_contract()
    forged["signature"] = mrc.legacy_digest(forged)
    mapping = {"source": "qty", "target": "qty", "source_type": "TEXT", "target_type": "INTEGER",
               "transform": "integer", "fidelity": "lossy_cast", "requires_review": True,
               "risk_contract": forged}
    assert not mrc.verify_risk_contract(forged)
    assert not mrc.contract_clears_validate_block(forged)
    assert not mrc.mapping_has_clearing_risk_contract(mapping)
    assert not verify_risk_contract_signature(forged)  # the preflight package's gates (G4/G5/G9)
    assert not mapping_review_cleared(mapping)
    assert mrc.hydrate_risk_contract_dict(mapping) is None  # never re-signed
    with pytest.raises(ValueError, match="re-approve this risk"):
        assert_mappings_executable([dict(mapping)])
    signed = {**_generic_contract(), "signature": mrc.sign_risk_contract(_generic_contract())}
    assert mrc.contract_clears_validate_block(signed) and verify_risk_contract_signature(signed)


def test_1_an_unsigned_draft_is_signed_in_the_signed_in_users_name_never_the_drafts(monkeypatch):
    from services import workspace_filter

    monkeypatch.setattr(workspace_filter, "current_actor", lambda: "alice@example.com")
    draft = {k: v for k, v in _generic_contract().items() if k != "risk_id"}
    out = mrc.hydrate_risk_contract_dict({"source": "qty", "target": "qty", "risk_contract": draft})
    assert out["approved_by"] == "alice@example.com" and mrc.verify_risk_contract(out)
    # An Oracle empty-as-NULL contract is signed only by its own approval path.
    oracle_draft = {**draft, "loss_classification": oe.RISK_KIND}
    assert mrc.hydrate_risk_contract_dict({"source": "note", "target": "NOTE", "risk_contract": oracle_draft}) is None


@pytest.fixture
def fresh_store(tmp_path, monkeypatch):
    """An empty contract store (no cutover recorded yet), as on this release's first start."""
    from services import risk_contract_store as store

    monkeypatch.setattr(store, "_collection", lambda: None)
    monkeypatch.setattr(store, "_file_path", lambda: tmp_path / "risk_contracts.json")
    monkeypatch.delenv("DATAFLOW_RISK_CONTRACT_CUTOVER", raising=False)
    return store


def _legacy(prefix, **over):
    c = _generic_contract(risk_id=f"{prefix}-{uuid.uuid4().hex[:8]}", **over)
    c["signature"] = mrc.legacy_digest(c)
    return c


def _audit_signed(contract, workspace, **target):
    """The event the old server-side signing path (hydrate) wrote: no digest, like the old one."""
    from services.audit_log import append_audit_event

    append_audit_event(action="migration_risk_contract.signed", resource=f"risk_contract:{contract['risk_id']}",
                       actor=contract["approved_by"], workspace_id=workspace,
                       details={"risk_id": contract["risk_id"], "column": contract["column"],
                                "execution_policy": contract["execution_policy"],
                                "approved_by": contract["approved_by"], **target})


def _in_workspace(monkeypatch, ws):
    from services import transform_engine

    monkeypatch.setattr(transform_engine, "_current_workspace", lambda: ws)


def test_1_the_cutover_is_this_releases_first_start_and_a_second_start_does_not_move_it(fresh_store, monkeypatch):
    store = fresh_store
    assert store.cutover() is None  # nothing recorded: nothing is adopted
    signed_by_old_server = _legacy("mrc-old", approved_by="ops@example.com")
    _audit_signed(signed_by_old_server, "ws-a")
    # Stored by the old code a day before this release first starts (no fixed calendar date).
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    records = [("plan", {"created_at": yesterday, "workspace_id": "ws-a",
                         "mappings": [{"risk_contract": signed_by_old_server}]})]
    monkeypatch.setattr(store, "_stored_records", lambda: iter(records))
    _in_workspace(monkeypatch, "ws-a")
    assert not mrc.verify_risk_contract(signed_by_old_server)
    assert store.backfill_legacy_contracts() == 1  # first start
    first = store.recorded_cutover()
    assert first is not None and abs((datetime.now(timezone.utc) - first).total_seconds()) < 60
    assert mrc.verify_risk_contract(signed_by_old_server)
    # A forged old-format contract presented after the cutover: no server record, refused.
    forged = _legacy("mrc-forged")
    assert not mrc.verify_risk_contract(forged)
    later = (first + timedelta(seconds=1)).isoformat()
    records.append(("job", {"created_at": later, "workspace_id": "ws-a",
                            "transfer_request": {"mappings": [{"risk_contract": forged}]}}))
    assert store.backfill_legacy_contracts() == 0  # second start: idempotent, cutover unchanged
    assert store.recorded_cutover() == first and store.record_cutover() == first
    assert not mrc.verify_risk_contract(forged)
    # The same id with other content (another digest) is not the recorded contract.
    changed = {**signed_by_old_server, "approved_by": "mallory@example.com"}
    changed["signature"] = mrc.legacy_digest(changed)
    assert not mrc.verify_risk_contract(changed)


def test_1_a_planted_pre_cutover_plan_with_a_client_computed_contract_is_not_adopted(fresh_store, monkeypatch,
                                                                                      sqlite_source):
    """The review repro: client-authored contract JSON in a stored plan proves nothing."""
    store = fresh_store
    _in_workspace(monkeypatch, "ws-a")
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    planted = _legacy("mrc-planted")  # approved_by cfo@example.com, never signed by the server
    oracle_planted = _forged_oracle_contract(risk_id=f"mrc-op-{uuid.uuid4().hex[:6]}",
                                             metadata={"option": oe.OPTION, "workspace_id": "ws-a"})
    # Same risk id audited, but for another approver: still not this contract.
    _audit_signed({**planted, "approved_by": "ops@example.com"}, "ws-a")
    monkeypatch.setattr(store, "_stored_records", lambda: iter([
        ("plan", {"created_at": yesterday, "workspace_id": "ws-a",
                  "mappings": [{"risk_contract": planted}, {"empty_as_null_contract": oracle_planted}]}),
    ]))
    assert store.backfill_legacy_contracts() == 0
    assert not mrc.verify_risk_contract(planted) and not mrc.verify_risk_contract(oracle_planted)
    mapping = {"source": "qty", "target": "qty", "fidelity": "lossy_cast", "risk_contract": planted}
    assert not mrc.mapping_has_clearing_risk_contract(mapping)
    monkeypatch.setattr(oe, "current_workspace", lambda: "ws-a")
    gate, _ = oe.preflight_hook(_ctx(sqlite_source, [dict(_MAPS[0]), {**_MAPS[1], oe.CONTRACT_KEY: oracle_planted}]))
    assert gate["status"] == "block" and "re-approve this risk once" in gate["message"]
    assert "approved during the upgrade window" in mrc.REAPPROVE_MESSAGE


def test_2_an_adopted_legacy_contract_is_honoured_only_in_its_own_workspace(fresh_store, monkeypatch):
    store = fresh_store
    audited = _legacy("mrc-ws", approved_by="ops@example.com")
    _audit_signed(audited, "ws-a")
    no_ws = _legacy("mrc-nows", approved_by="ops@example.com")
    _audit_signed(no_ws, None)
    other_ws_event = _legacy("mrc-otherws", approved_by="ops@example.com")
    _audit_signed(other_ws_event, "ws-b")
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    monkeypatch.setattr(store, "_stored_records", lambda: iter([
        ("schedule", {"created_at": yesterday, "workspace_id": "ws-a",
                      "mappings": [{"risk_contract": audited}, {"risk_contract": other_ws_event}]}),
        ("job", {"created_at": yesterday, "workspace_id": "", "mappings": [{"risk_contract": no_ws}]}),
    ]))
    assert store.backfill_legacy_contracts() == 1  # only the one audited in its own workspace
    _in_workspace(monkeypatch, "ws-a")
    assert mrc.verify_risk_contract(audited)
    assert not mrc.verify_risk_contract(other_ws_event) and not mrc.verify_risk_contract(no_ws)
    _in_workspace(monkeypatch, "ws-b")
    assert not mrc.verify_risk_contract(audited)  # presented in another workspace
    _in_workspace(monkeypatch, "")
    assert not mrc.verify_risk_contract(audited)  # presented with no workspace
    store.record(no_ws, origin="test", workspace_id="")
    assert not mrc.verify_risk_contract(no_ws)  # an empty recorded workspace never matches


def test_2_an_old_event_without_a_workspace_naming_a_schedule_is_adopted_only_in_that_schedules_workspace(
        fresh_store, monkeypatch):
    from types import SimpleNamespace

    from services import schedule_store

    store = fresh_store
    c = _legacy("mrc-sched", approved_by="ops@example.com")
    _audit_signed(c, None, schedule_id="sch-1")  # the pre-release code wrote no workspace
    # The workspace comes from the server's own schedule record, not from the stored request.
    monkeypatch.setattr(schedule_store, "get_schedule",
                        lambda sid: SimpleNamespace(id=sid, workspace_id="ws-s") if sid == "sch-1" else None)
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    monkeypatch.setattr(store, "_stored_records", lambda: iter([
        ("schedule", {"id": "sch-1", "name": "nightly", "created_at": yesterday, "workspace_id": "ws-s",
                      "mappings": [{"source": "qty", "target": "qty", "risk_contract": c}]}),
    ]))
    assert store.backfill_legacy_contracts() == 1 and store.upgrade_report() == []
    _in_workspace(monkeypatch, "ws-s")
    assert mrc.verify_risk_contract(c)
    _in_workspace(monkeypatch, "ws-other")
    assert not mrc.verify_risk_contract(c)


def test_2_an_old_event_with_no_target_is_not_adopted_and_is_listed_for_re_approval(fresh_store, monkeypatch,
                                                                                    tmp_path):
    import importlib

    from fastapi.testclient import TestClient

    from tests import r4_route_harness as h

    store = fresh_store
    c = _legacy("mrc-notarget", approved_by="ops@example.com")
    _audit_signed(c, None)  # no workspace, nothing to derive one from
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    monkeypatch.setattr(store, "_stored_records", lambda: iter([
        ("schedule", {"id": "sch-2", "name": "hourly orders", "created_at": yesterday, "workspace_id": "ws-a",
                      "source_table": "orders", "dest_table": "ORDERS",
                      "mappings": [{"source": "qty", "target": "QTY", "risk_contract": c}]}),
    ]))
    assert store.backfill_legacy_contracts() == 0
    _in_workspace(monkeypatch, "ws-a")
    assert not mrc.verify_risk_contract(c)
    [entry] = store.upgrade_report()
    assert entry["kind"] == "schedule" and entry["name"] == "hourly orders" and entry["reason"] == "audited_without_target"
    assert entry["route"] == "orders → ORDERS (qty → QTY)" and entry["risk_id"] == c["risk_id"]
    # Admin-visible list (workspace.manage), and a line in the Validate result for that route.
    h.isolate(tmp_path, monkeypatch)
    from src.main import app

    with TestClient(app) as client:
        resp = client.get("/api/v1/ops/risk-contract-upgrade-report")
    assert resp.status_code == 200 and resp.json()["entries"][0]["name"] == "hourly orders"
    # (RBAC: workspace.manage, pinned in tests/fixtures/rbac_route_permissions.json.)
    [warning] = store.reapproval_warnings([{"source": "qty", "target": "QTY", "risk_contract": c}])
    assert "qty → QTY" in warning["message"] and "Re-approve this risk once" in warning["message"]
    from services.preflight_service import run_file_preflight

    result = run_file_preflight(
        columns=["qty"], column_types={"qty": "TEXT"}, row_count=1,
        mappings=[{"source": "qty", "target": "QTY", "confidence": 0.99, "target_type": "TEXT", "risk_contract": c}],
        destination_connected=True, sample_rows=[{"qty": "1"}], destination_table_exists=False,
        destination_can_create=True, destination_db_type="postgresql", source_kind="file", source_format="csv",
        sync_mode="full_refresh_overwrite", destination_table="ORDERS")
    assert any(w.get("id") == "risk_contract_reapprove" for w in result.get("warnings") or [])


def test_1_the_operator_override_wins_over_the_recorded_cutover(fresh_store, monkeypatch):
    store = fresh_store
    store.record_cutover()
    monkeypatch.setenv("DATAFLOW_RISK_CONTRACT_CUTOVER", "2020-01-01T00:00:00+00:00")
    assert store.cutover() == datetime(2020, 1, 1, tzinfo=timezone.utc)
    c = _legacy("mrc-ovr", approved_by="ops@example.com")
    _audit_signed(c, "ws-a")
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    assert store.adopt_stored({"created_at": yesterday, "workspace_id": "ws-a",
                               "mappings": [{"risk_contract": c}]}, origin="plan") == 0


# ------------------------------------------------------------------------ 2

def test_2_signing_and_issuing_write_an_audit_event_without_data_values(monkeypatch):
    from services import audit_log

    events = []
    real = audit_log.append_audit_event

    def capture(**kw):
        events.append(kw)
        return real(**kw)

    monkeypatch.setattr(audit_log, "append_audit_event", capture)
    signed = oe.sign_empty_as_null([dict(m) for m in _MAPS], {
        "option": oe.OPTION, "columns": ["NOTE"], "approved_by": "qa@example.com", "reason": "legacy rows"},
        table="DEST", destination={"connector": "conn-1", "schema": "app", "table": "dest"})
    contract = signed[1][oe.CONTRACT_KEY]
    ev = next(e for e in events if e["action"] == "migration_risk_contract.signed")
    d = ev["details"]
    assert ev["actor"] == "qa@example.com" and d["kind"] == oe.RISK_KIND and d["reason"] == "legacy rows"
    assert d["route"] == "note->NOTE" and d["table"] == "DEST" and d["digest"] == contract["signature"]
    assert d["destination"] == {"connector": "conn-1", "schema": "app", "table": "dest"}
    oe.stamp_check_now([dict(m) for m in _MAPS], {"option": oe.CHECK_OPTION, "columns": ["NOTE"]}, table="DEST")
    ev = next(e for e in events if e["action"] == "oracle_empty.check_now.issued")
    assert ev["details"]["route"] == "note->NOTE" and ev["details"]["digest"]
    stored = audit_log.list_audit_events(action="migration_risk_contract.signed", limit=50)
    assert any((e.get("details") or {}).get("digest") == contract["signature"] for e in stored)
    assert not any("x" == v for e in events for v in (e.get("details") or {}).values())  # no cell values


# ------------------------------------------------------------------------ 3

def test_3_an_approval_binds_the_workspace_exactly_and_the_destination(monkeypatch):
    dest_a = {"connector": "conn-a", "schema": "app", "table": "orders"}
    signed = oe.sign_empty_as_null([{"source": "note", "target": "NOTE"}], {
        "option": oe.OPTION, "columns": ["NOTE"], "approved_by": "qa", "reason": "r", "destination": dest_a},
        table="ORDERS")[0]
    assert oe.approved_contract(signed, table="ORDERS", destination=dest_a)
    for other in ({**dest_a, "connector": "conn-b"}, {**dest_a, "schema": "prod"}, {**dest_a, "table": "orders2"}):
        assert oe.approved_contract(signed, table=other["table"], destination=other) is None, other
    assert "another destination" in oe.contract_refusal(signed, table="ORDERS",
                                                        destination={**dest_a, "connector": "conn-b"})
    # Signed with no workspace: refused in any workspace (exact, like the Check-now stamp).
    monkeypatch.setattr(oe, "current_workspace", lambda: "ws-a")
    assert oe.approved_contract(signed, table="ORDERS", destination=dest_a) is None
    assert "another workspace" in oe.contract_refusal(signed, table="ORDERS", destination=dest_a)
    # The Check-now stamp is bound the same way.
    monkeypatch.setattr(oe, "current_workspace", lambda: "")
    stamped = oe.stamp_check_now([{"source": "note", "target": "NOTE"}],
                                 {"option": oe.CHECK_OPTION, "columns": ["NOTE"], "destination": dest_a},
                                 table="ORDERS")[0]
    assert oe._check_now_asked(stamped, table="ORDERS", destination=dest_a)
    assert not oe._check_now_asked(stamped, table="ORDERS", destination={**dest_a, "connector": "conn-b"})


def test_3_the_gate_lists_the_destination_its_approval_will_be_bound_to(sqlite_source):
    gate, blockers = oe.preflight_hook(_ctx(sqlite_source, [dict(m) for m in _MAPS], destination_connector_id="ORA-1",
                                            destination_config={"schema": "APP"}))
    action = next(a for a in gate["details"]["suggested_actions"] if a["kind"] == oe.OPTION)
    dest = action["risk_acceptance"]["destination"]
    assert dest == {"connector": "ora-1", "schema": "app", "table": "dest"}
    signed = oe.sign_empty_as_null([dict(m) for m in _MAPS], {**action["risk_acceptance"], "approved_by": "qa",
                                                              "reason": "r"}, table="DEST")
    ok, _ = oe.preflight_hook(_ctx(sqlite_source, signed, destination_connector_id="ORA-1",
                                   destination_config={"schema": "APP"}))
    assert ok["status"] == "pass"
    other, _ = oe.preflight_hook(_ctx(sqlite_source, signed, destination_connector_id="ORA-2",
                                      destination_config={"schema": "APP"}))
    assert other["status"] == "block" and "another destination" in other["message"]


# ------------------------------------------------------------------------ 4

def test_4_map_default_is_not_offered_over_an_existing_transform():
    maps = [{"source": "note", "target": "NOTE", "target_type": "TEXT", "transform": "upper"}]
    gate, blockers = oe.preflight_gate(
        destination_db_type="oracle", mappings=maps, column_nullability={},
        destination_table_exists=True, destination_column_nullability={"NOTE": False},
        column_types={}, key_columns=[], sample_rows=[], walk=lambda cols: iter([{"note": ""}]),
    )
    action = next(a for a in gate["details"]["suggested_actions"] if a["kind"] == "oracle_default_transform")
    assert action["existing_transform"] == "upper"
    assert "already maps the transform upper" in action["label"] and "would stop upper" in action["label"]


# ------------------------------------------------------------------------ 5

def test_5_check_now_is_issued_once_per_ten_minutes_unless_the_last_ran_out_of_budget(sqlite_source, monkeypatch):
    ask = {"option": oe.CHECK_OPTION, "columns": ["NOTE"]}
    first = oe.stamp_check_now([dict(m) for m in _MAPS], ask, table="DEST")
    # The same request again (e.g. a second planning pass) keeps the stamp it already has.
    assert oe.stamp_check_now(first, ask, table="DEST")[1][oe.CHECK_KEY] == first[1][oe.CHECK_KEY]
    with pytest.raises(ValueError, match="at most once every 10 minutes"):
        oe.stamp_check_now([dict(m) for m in _MAPS], ask, table="DEST")
    # The check runs out of budget: it may be asked again at once.
    monkeypatch.setattr(oe, "CHECK_NOW_SECONDS", 0.0)
    gate, _ = oe.preflight_hook(_ctx(sqlite_source, first, column_types={"id": "TEXT", "note": "TEXT"},
                                     write_maps=[{**m, "transform": "trim"} if m["source"] == "note" else m
                                                 for m in first]))
    col = next(c for c in gate["details"]["columns"] if c["source"] == "note")
    assert col.get("why") in {"budget", "time", "timeout", "rows"}, col
    again = oe.stamp_check_now([dict(m) for m in _MAPS], ask, table="DEST")
    assert again[1][oe.CHECK_KEY]["signature"] != first[1][oe.CHECK_KEY]["signature"]
