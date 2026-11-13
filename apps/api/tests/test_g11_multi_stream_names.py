"""G11 review #4: each stream's Map rows get the naming rules, against its own table.

Multi-stream routes carry per-stream mappings (``stream_contracts[i].mappings``)
and land each stream in its own object. The name refusal and the legacy-column
adoption read only the top-level ``request.mappings`` and the request's table,
so:

* a 71-character PostgreSQL target in stream 2 was not refused and was cut
  to 63 characters silently;
* stream 2 into a table whose earlier run created the 63-character column
  failed "physical DDL missing" instead of writing that column.

Validate (``stream_target_gates``) and Execute (``streams_refusal`` before any
write; ``prepare_stream_targets`` in the stream loop and the CDC multi-stream
setup) now judge every stream against its own object.

Studio routes on live PostgreSQL: plan Validate -> approved hashes ->
``/api/v1/transfer/run``; a throwaway destination schema is dropped afterwards.
"""

from __future__ import annotations

import json
import uuid

import pytest

from tests import g11_studio_flow as flow
from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)

LONG = "customer_lifetime_value_after_discounts_and_refunds_in_euro_cents_xyz"  # 70
CUT = LONG[:63]


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


@pytest.fixture
def route():
    token = uuid.uuid4().hex[:8]
    s1, s2, schema = f"g11ms1_{token}", f"g11ms2_{token}", f"g11ms_{token}"
    for t in (s1, s2):
        h.execute("postgresql", f"CREATE TABLE {t} (id int PRIMARY KEY, v int)")  # nosec B608
        h.execute("postgresql", f"INSERT INTO {t} VALUES (1, 10), (2, 20)")  # nosec B608
    h.execute("postgresql", f"CREATE SCHEMA {schema}")
    try:
        yield token, s1, s2, schema
    finally:
        for t in (s1, s2):
            h.drop_table("postgresql", t)
        h.execute("postgresql", f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def _run(client, token, s1, s2, schema, *, stream2_target):
    from services.connector_store import create_connector

    src = create_connector({"name": f"g11ms-src-{token}", "role": "source", "workspace_id": "",
                            "ssl": False, **h.dest_config("postgresql")})
    dst = create_connector({"name": f"g11ms-dst-{token}", "role": "destination",
                            "workspace_id": "", "ssl": False,
                            **{**h.dest_config("postgresql"), "schema": schema}})
    source = {"kind": "database", "format": "postgresql", "connector_id": src.id,
              "database": h.PG["database"], "table": s1}
    destination = {"kind": "database", "format": "postgresql", "connector_id": dst.id,
                   "database": h.PG["database"], "schema": schema, "table": s1}
    m1 = [{"source": "id", "target": "id", "confidence": 1.0, "target_type": "BIGINT"},
          {"source": "v", "target": "v", "confidence": 1.0, "target_type": "BIGINT"}]
    m2 = [{"source": "id", "target": "id", "confidence": 1.0, "target_type": "BIGINT"},
          {"source": "v", "target": stream2_target, "confidence": 1.0, "target_type": "BIGINT"}]
    contracts = [
        {"name": s1, "selected": True, "sync_mode": "full_refresh_append", "mappings": m1},
        {"name": s2, "selected": True, "sync_mode": "full_refresh_append", "mappings": m2},
    ]
    plan_id, pf = flow.validate(
        client, source=source, destination=destination, columns=["id", "v"],
        types={"id": "INTEGER", "v": "INTEGER"}, mappings=m1,
        sync_mode="full_refresh_append", stream_contracts=contracts,
    )
    # A refused plan cannot be executed from Studio; an API caller can still
    # submit the same route without the plan (no plan_id).
    run = flow.execute(
        client, plan_id=plan_id if pf.get("passed") else "", pf=pf, source=source,
        destination=destination, mappings=m1, sync_mode="full_refresh_append",
        stream_contracts=contracts, expect_status=None,
    )
    return pf, run


def _columns(schema: str, table: str) -> list[str]:
    return [r[0] for r in h.query(
        "postgresql",
        "SELECT column_name FROM information_schema.columns "  # nosec B608
        f"WHERE table_schema = '{schema}' AND table_name = '{table}' ORDER BY ordinal_position",
    )]


def test_over_limit_name_in_stream_two_is_refused(client, route):
    token, s1, s2, schema = route
    pf, run = _run(client, token, s1, s2, schema, stream2_target=LONG)
    gate = flow.gates_of(pf).get("g23c_target_names") or {}
    assert gate.get("status") == "block", sorted(flow.gates_of(pf))
    assert f"Stream {s2!r}" in gate["message"] and "at most 63" in gate["message"]
    assert run.get("success") is not True, run
    assert "at most 63" in json.dumps(run), run
    # Nothing was written: no table for either stream.
    assert _columns(schema, s1) == [] and _columns(schema, s2) == []


def test_stream_two_keeps_writing_its_existing_legacy_column(client, route):
    token, s1, s2, schema = route
    h.execute("postgresql", f'CREATE TABLE {schema}.{s2} (id bigint, "{CUT}" bigint)')
    h.execute("postgresql", f"INSERT INTO {schema}.{s2} VALUES (0, 0)")  # nosec B608
    pf, run = _run(client, token, s1, s2, schema, stream2_target=LONG)
    assert pf.get("passed") is True, flow.blockers(pf)
    note = flow.gates_of(pf).get("g23d_existing_target_columns") or {}
    assert f"Stream {s2!r}" in str(note.get("message")) and CUT in str(note.get("message"))
    assert run.get("success") is True, (run.get("error"), run.get("error_details"))
    assert (run.get("reconciliation") or {}).get("passed") is True, run.get("reconciliation")
    assert _columns(schema, s2) == ["id", CUT]
    rows = h.query("postgresql", f'SELECT id, "{CUT}" FROM {schema}.{s2} ORDER BY id')  # nosec B608
    assert [tuple(int(x) for x in r) for r in rows] == [(0, 0), (1, 10), (2, 20)]
    assert _columns(schema, s1) == ["id", "v"]
