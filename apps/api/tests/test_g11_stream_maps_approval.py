"""Security re-check F1(c): every stream's Map rows are bound to the approval.

The DDL identity and the Decision Artifact hashed the request's top-level Map
only. A multi-stream route whose stream_contracts[i].mappings changed after
Validate (v_ok -> v_evil) ran green and wrote the unapproved column. Each
selected stream whose own rows differ from the top-level Map is now part of
both hashes, in stream order, at Validate and at Execute. A change after
Validate is "DDL identity mismatch ... Re-validate". An approval stamped
before this (no per-stream component) still holds when the streams' rows
equal the top-level Map, as they did when it was approved.
"""

from __future__ import annotations

import uuid

import pytest

from tests import g11_studio_flow as flow
from tests import r4_route_harness as h
from tests.test_g11_map_target_ingress import _column, _form, drop_world, multi_stream_world

pytestmark = pytest.mark.timeout(600)

TOP = [{"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "BIGINT"},
       {"source": "v", "target": "v_ok", "source_type": "INTEGER", "target_type": "BIGINT"}]
EVIL = [dict(TOP[0]), {**TOP[1], "target": "v_evil"}]


def _contracts(*maps):
    return [{"name": f"s{i}", "selected": True, **({"mappings": m} if m else {})}
            for i, m in enumerate(maps)]


def test_streams_equal_to_the_map_keep_the_pre_upgrade_hash():
    from services.conversion_contract import approved_mapping_ddl_fingerprint, bind_stream_maps

    plain = approved_mapping_ddl_fingerprint(TOP, dest_db="postgresql")
    for contracts in (_contracts(None, None), _contracts(TOP, TOP), _contracts(TOP, None)):
        bound = bind_stream_maps(TOP, contracts, dest_db="postgresql")
        assert approved_mapping_ddl_fingerprint(bound, dest_db="postgresql") == plain


def test_a_streams_own_rows_change_the_hash_in_stream_order():
    from services.conversion_contract import approved_mapping_ddl_fingerprint, bind_stream_maps

    plain = approved_mapping_ddl_fingerprint(TOP, dest_db="postgresql")
    a = approved_mapping_ddl_fingerprint(
        bind_stream_maps(TOP, _contracts(TOP, EVIL), dest_db="postgresql"), dest_db="postgresql")
    b = approved_mapping_ddl_fingerprint(
        bind_stream_maps(TOP, _contracts(EVIL, TOP), dest_db="postgresql"), dest_db="postgresql")
    assert len({plain, a, b}) == 3


def _request(contracts):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", database="d", table="s0"),
        destination=EndpointConfig(kind="database", format="postgresql", table="s0"),
        mappings=[dict(m) for m in TOP], stream_contracts=contracts,
        sync_mode="full_refresh_append", approved_ddl_identity_hash="x" * 64)


def test_execute_refuses_stream_rows_changed_after_validate():
    from services.conversion_contract import bind_stream_maps
    from services.decision_kernel import build_artifact_from_mappings, ddl_identity_report
    from src.transfer.engine_identity import (
        _enforce_ddl_identity,
        _enforce_decision_artifact,
        _operator_contract_maps,
    )

    approved = bind_stream_maps(TOP, _contracts(TOP, TOP), dest_db="postgresql")
    stamp = ddl_identity_report(approved, dest_db="postgresql")["ddl_identity_hash"]
    art = build_artifact_from_mappings(approved, dest_db="postgresql",
                                       route_id="validate:postgresql",
                                       sync_mode="full_refresh_append").to_dict()
    for contracts, ok in ((_contracts(TOP, TOP), True), (_contracts(TOP, EVIL), False)):
        req = _request(contracts)
        maps = _operator_contract_maps(req, list(req.mappings))
        err = _enforce_ddl_identity({}, maps, dest_db="postgresql",
                                    approved_ddl_identity_hash=stamp)
        assert (err is None) is ok, err
        if not ok:
            assert "DDL identity mismatch" in err and "Re-validate" in err
        err, _art = _enforce_decision_artifact(
            {"proof_bundle": {"decision_artifact": art}}, maps, dest_db="postgresql",
            approved_decision_artifact_hash=art["content_hash"], decision_artifact=art,
            sync_mode="full_refresh_append")
        assert (err is None) is ok, err


def test_a_pre_upgrade_schedule_stamp_still_holds_with_equal_streams():
    """Stamped before per-stream binding existed: over the top-level Map only."""
    from services.decision_kernel import ddl_identity_report
    from src.transfer.engine_identity import _enforce_ddl_identity, _operator_contract_maps

    stamp = ddl_identity_report(TOP, dest_db="postgresql")["ddl_identity_hash"]
    req = _request(_contracts(TOP, TOP))
    maps = _operator_contract_maps(req, list(req.mappings))
    assert _enforce_ddl_identity({}, maps, dest_db="postgresql",
                                 approved_ddl_identity_hash=stamp) is None
    req = _request(_contracts(TOP, EVIL))
    maps = _operator_contract_maps(req, list(req.mappings))
    assert _enforce_ddl_identity({}, maps, dest_db="postgresql",
                                 approved_ddl_identity_hash=stamp)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _live(client, change: bool):
    token = uuid.uuid4().hex[:8]
    s1, s2, schema, source, destination, maps = multi_stream_world(token)
    try:
        contracts = [{"name": s1, "selected": True, "sync_mode": "full_refresh_append",
                      "mappings": maps},
                     {"name": s2, "selected": True, "sync_mode": "full_refresh_append",
                      "mappings": maps}]
        plan_id, pf = flow.validate(
            client, source=source, destination=destination, columns=["id", "v"],
            types={"id": "INTEGER", "v": "INTEGER"}, mappings=maps,
            sync_mode="full_refresh_append", stream_contracts=contracts,
            schema_policy="propagate_all")
        assert pf.get("passed") is True, flow.blockers(pf)
        evil = [dict(maps[0]), {**maps[1], "target": "v_evil"}]
        sent = [dict(c, mappings=evil) for c in contracts] if change else contracts
        resp = client.post("/api/v1/transfer/run", data=_form(
            source, destination, plan_id, pf, pf.get("mappings") or maps, sent))
        run = resp.json()
        run = run.get("detail") if isinstance(run.get("detail"), dict) else run
        return run, (_column(schema, (s1, s2), "v_ok"), _column(schema, (s1, s2), "v_evil"))
    finally:
        drop_world(s1, s2, schema)


def test_live_stream_maps_changed_after_validate_are_refused(client):
    run, (ok_rows, evil_rows) = _live(client, change=True)
    assert run.get("success") is not True, run
    assert "identity" in str(run.get("error") or "").lower(), run.get("error")
    assert ok_rows == [] and evil_rows == []


def test_live_validated_stream_maps_run(client):
    run, (ok_rows, evil_rows) = _live(client, change=False)
    assert run.get("success") is True, (run.get("error"), run.get("error_details"))
    assert len(ok_rows) == 4 and evil_rows == []
