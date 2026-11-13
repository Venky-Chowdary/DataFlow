"""Security check B1: Execute uses exactly the stream contracts the approval bound.

bind_stream_maps hashes the selected contracts only, but Execute looked a
stream's contract up by name across all of them (``contract_for_stream``
returned the first match; CDC used ``next(c for c in stream_contracts if
name == ...)``). An unselected twin with the same name ([{s1, selected:
false, v -> v_evil}, {s1, selected: true, approved}, {s2}]) left the hash
unchanged while Execute wrote v_evil (reproduced live through
POST /api/v1/transfer/run, Gate-8 green).

Now the lookups consider selected contracts only, and Execute refuses any
request whose stream contracts repeat a name (after strip and case-fold),
selected or not, before anything is read or written, on every path.
"""

from __future__ import annotations

import uuid

import pytest

from tests import g11_studio_flow as flow
from tests.test_g11_map_target_ingress import (  # noqa: F401  (fixture)
    _column,
    _form,
    client,
    drop_world,
    multi_stream_world,
)

pytestmark = pytest.mark.timeout(600)

TOP = [{"source": "v", "target": "v_ok"}]
EVIL = [{"source": "v", "target": "v_evil"}]


def test_lookups_never_return_an_unselected_twin():
    from services.multi_stream_plan import contract_for_stream, selected_stream_contract

    contracts = [{"name": "s1", "selected": False, "mappings": EVIL},
                 {"name": "s1", "selected": True},
                 {"name": "s2", "selected": True}]
    assert contract_for_stream(contracts, "s1").get("mappings") is None
    assert contract_for_stream(contracts, "S1 ").get("mappings") is None
    assert selected_stream_contract(contracts, "s1").get("selected") is True
    assert contract_for_stream([{"name": "s3", "selected": False}], "s3") == {}


@pytest.mark.parametrize("twin", ["s1", " S1 ", "s1\t"])
def test_execute_refuses_repeated_stream_names(twin):
    from services.source_filter_gate import request_refusal
    from src.transfer.models import EndpointConfig, TransferRequest

    for mode in ("full_refresh_append", "cdc"):
        req = TransferRequest(
            source=EndpointConfig(kind="database", format="postgresql", database="d",
                                  table="s1"),
            destination=EndpointConfig(kind="database", format="postgresql", table="s1"),
            mappings=[dict(m) for m in TOP], sync_mode=mode,
            stream_contracts=[{"name": twin, "selected": False, "mappings": EVIL},
                              {"name": "s1", "selected": True},
                              {"name": "s2", "selected": True}])
        refusal = request_refusal(req)
        assert "more than once" in refusal and "s1" in refusal.lower(), refusal


def _validated(client, token):
    s1, s2, schema, source, destination, maps = multi_stream_world(token)
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
    twin = [{"name": s1, "selected": False, "sync_mode": "full_refresh_append",
             "mappings": evil}] + contracts
    return (s1, s2, schema, source, destination, maps), plan_id, pf, twin


def test_live_unselected_twin_is_refused_on_the_multi_stream_route(client):  # noqa: F811
    """The reviewer's probe, through POST /api/v1/transfer/run."""
    world, plan_id, pf, twin = _validated(client, uuid.uuid4().hex[:8])
    s1, s2, schema, source, destination, maps = world
    try:
        resp = client.post("/api/v1/transfer/run", data=_form(
            source, destination, plan_id, pf, pf.get("mappings") or maps, twin))
        run = resp.json()
        run = run.get("detail") if isinstance(run.get("detail"), dict) else run
        assert run.get("success") is not True, run
        assert "more than once" in str(run.get("error") or run), run
        assert _column(schema, (s1, s2), "v_evil") == []
        assert _column(schema, (s1, s2), "v_ok") == []
    finally:
        drop_world(s1, s2, schema)


def test_live_unselected_twin_is_refused_on_the_cdc_route(client):  # noqa: F811
    """The same contracts on a CDC run, through the engine every CDC route uses."""
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    world, _plan_id, _pf, twin = _validated(client, uuid.uuid4().hex[:8])
    s1, s2, schema, source, destination, maps = world
    try:
        result = UniversalTransferEngine().execute_tracked(TransferRequest(
            source=EndpointConfig.from_dict("database", source),
            destination=EndpointConfig.from_dict("database", destination),
            mappings=[dict(m) for m in maps], sync_mode="cdc",
            stream_contracts=[{**c, "sync_mode": "cdc", "primary_key": ["id"]} for c in twin],
            skip_preflight=True,
        ), "g11twin" + uuid.uuid4().hex[:12])
        assert not result.success and "more than once" in str(result.error), result.error
        assert _column(schema, (s1, s2), "v_evil") == []
        assert _column(schema, (s1, s2), "v_ok") == []
    finally:
        drop_world(s1, s2, schema)
