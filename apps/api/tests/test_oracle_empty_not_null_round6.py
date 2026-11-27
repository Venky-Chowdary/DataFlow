"""Round 6 (security): risk contracts are server-signed, bound and audited.

1. A Migration Risk Contract carries a keyed HMAC (the deployment's signing
   keyring); an unkeyed digest ("mrc-sha256:", what every pre-release contract
   carries and any client can compute) is never honoured: it is refused with
   "re-approve this risk" on every path that reads one, and every stored one is
   listed in the upgrade report (start log, ops endpoint, Validate warning).
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
from datetime import datetime, timezone

import pytest

from services import migration_risk_contract as mrc
from services import oracle_empty_not_null as oe


def _unkeyed(body):
    """The pre-release digest (``mrc-sha256:``): anyone, a client included, can compute it."""
    import hashlib

    return "mrc-sha256:" + hashlib.sha256(mrc._canonical_payload(body).encode("utf-8")).hexdigest()

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
    body["signature"] = _unkeyed(body)
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
    forged["signature"] = _unkeyed(forged)
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
    """An empty report store."""
    from services import risk_contract_store as store

    monkeypatch.setattr(store, "_collection", lambda: None)
    monkeypatch.setattr(store, "_file_path", lambda: tmp_path / "risk_contracts.json")
    return store


def _legacy(prefix, **over):
    c = _generic_contract(risk_id=f"{prefix}-{uuid.uuid4().hex[:8]}", **over)
    c["signature"] = _unkeyed(c)
    return c


def _audit_signed(contract, workspace, **target):
    """The event the pre-release server-side signing path (hydrate) wrote."""
    from services.audit_log import append_audit_event

    append_audit_event(action="migration_risk_contract.signed", resource=f"risk_contract:{contract['risk_id']}",
                       actor=contract["approved_by"], workspace_id=workspace,
                       details={"risk_id": contract["risk_id"], "column": contract["column"],
                                "execution_policy": contract["execution_policy"],
                                "approved_by": contract["approved_by"], **target})


def _in_workspace(monkeypatch, ws):
    from services import transform_engine

    monkeypatch.setattr(transform_engine, "_current_workspace", lambda: ws)


def test_1_a_pre_release_contract_is_never_honoured_whatever_evidence_it_has(monkeypatch, sqlite_source):
    """Even one the old server audited (in its workspace, or naming a schedule) no longer counts."""
    audited = _legacy("mrc-aud", approved_by="ops@example.com")
    _audit_signed(audited, "ws-a")
    named = _legacy("mrc-sch", approved_by="ops@example.com")
    _audit_signed(named, None, schedule_id="sch-1")
    planted = _legacy("mrc-planted")  # what an API caller can write, naming cfo@example.com
    for ws in ("ws-a", "", "ws-b"):
        _in_workspace(monkeypatch, ws)
        for c in (audited, named, planted):
            assert not mrc.verify_risk_contract(c), (ws, c["risk_id"])
            assert not mrc.mapping_has_clearing_risk_contract(
                {"source": "qty", "target": "qty", "fidelity": "lossy_cast", "risk_contract": c})
    oracle_old = _forged_oracle_contract(risk_id=f"mrc-op-{uuid.uuid4().hex[:6]}")
    gate, _ = oe.preflight_hook(_ctx(sqlite_source, [dict(_MAPS[0]), {**_MAPS[1], oe.CONTRACT_KEY: oracle_old}]))
    assert gate["status"] == "block" and "re-approve this risk once" in gate["message"]
    assert "approved before this release" in mrc.REAPPROVE_MESSAGE


def test_1_every_pre_release_contract_is_listed_for_re_approval(fresh_store, monkeypatch, tmp_path):
    from fastapi.testclient import TestClient

    from tests import r4_route_harness as h

    store = fresh_store
    old = _legacy("mrc-old", approved_by="ops@example.com")
    _audit_signed(old, "ws-a")
    old_oracle = _forged_oracle_contract(risk_id=f"mrc-oo-{uuid.uuid4().hex[:6]}")
    keyed = {**_generic_contract(risk_id="mrc-keyed"), "signature": mrc.sign_risk_contract(_generic_contract())}
    records = [
        ("schedule", {"id": "sch-2", "name": "hourly orders", "workspace_id": "ws-a", "source_table": "orders",
                      "dest_table": "ORDERS", "mappings": [{"source": "qty", "target": "QTY", "risk_contract": old},
                                                           {"source": "x", "target": "X", "risk_contract": keyed}]}),
        ("job", {"_id": "job-9", "workspace_id": "ws-b", "transfer_request": {
            "source": {"table": "t"}, "destination": {"table": "DEST"},
            "mappings": [{"source": "note", "target": "NOTE", oe.CONTRACT_KEY: old_oracle}]}}),
    ]
    monkeypatch.setattr(store, "_stored_records", lambda: iter(records))
    first = store.refresh_upgrade_report()
    assert [(e["kind"], e["record_id"], e["route"]) for e in first] == [
        ("schedule", "sch-2", "orders → ORDERS (qty → QTY)"), ("job", "job-9", "t → DEST (note → NOTE)")]
    assert first[0]["name"] == "hourly orders" and first[0]["risk_id"] == old["risk_id"]
    assert store.refresh_upgrade_report() == first == store.upgrade_report()  # every start: rebuilt, same
    # Admin-visible list (workspace.manage, pinned in tests/fixtures/rbac_route_permissions.json).
    h.isolate(tmp_path, monkeypatch)
    from src.main import app

    with TestClient(app) as client:
        resp = client.get("/api/v1/ops/risk-contract-upgrade-report")
    assert resp.status_code == 200 and {e["record_id"] for e in resp.json()["entries"]} >= {"sch-2", "job-9"}
    # And a line in the Validate result for that route.
    from services.preflight_service import run_file_preflight

    result = run_file_preflight(
        columns=["qty"], column_types={"qty": "TEXT"}, row_count=1,
        mappings=[{"source": "qty", "target": "QTY", "confidence": 0.99, "target_type": "TEXT", "risk_contract": old}],
        destination_connected=True, sample_rows=[{"qty": "1"}], destination_table_exists=False,
        destination_can_create=True, destination_db_type="postgresql", source_kind="file", source_format="csv",
        sync_mode="full_refresh_overwrite", destination_table="ORDERS")
    warning = next(w for w in result.get("warnings") or [] if w.get("id") == "risk_contract_reapprove")
    assert "qty → QTY" in warning["message"] and "Re-approve this risk once" in warning["message"]


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
