"""D55: Datawrap's own column renames map at full confidence on later runs.

QA (production): source columns 'Order ID', 'naive col' (with an accent),
'col-with-dash', 'Updated At' and a 63-character name. After run 1 created the
destination, start_transfer, create_schedule and plan_transfer (append) were
blocked at g4 "4 mapping(s) below floor 0.85" (Order ID -> Order_ID 0.82).

Route here: MCP start_transfer -> Confirm (run 1 creates the table), then MCP
plan_transfer and create_schedule for an append into that table, PostgreSQL
-> PostgreSQL. Every source column must map to the identifier Datawrap
generated for it at the top of the confidence scale (0.99, the score of an
exact same-name pair; Datawrap never reports 1.0), with no blocker, and run 2
must land.

AC-2 ("preserve original names where the engine can quote them") is not
adopted: Datawrap's documented convention is to normalise destination
identifiers (lower snake case, accents transliterated), as Fivetran and
Airbyte do; changing it would rename every existing table's columns. The
requirement that matters, that a rerun recognises its own names, is pinned
here.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(300)

_LONG = "Customer Lifetime Value Including Returns And Exchanges Net Amt"
_COLS = ["Order ID", "naïve col", "col-with-dash", "Updated At", _LONG]


@pytest.fixture
def client(tmp_path, monkeypatch):
    if not h.port_up(5432):
        pytest.skip("live PostgreSQL not configured")
    import services.config  # noqa: F401 - settings load before isolate blanks SEED_DEMO
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _mappings(payload: dict) -> list[dict]:
    plan = payload.get("plan") if isinstance(payload.get("plan"), dict) else payload
    for key in ("mappings", "column_mappings"):
        if isinstance(plan.get(key), list):
            return plan[key]
    for stream in plan.get("streams") or []:
        if isinstance(stream, dict) and isinstance(stream.get("mappings"), list):
            return stream["mappings"]
    raise AssertionError(f"no mappings in {sorted(plan)}")


def test_rerun_maps_its_own_generated_names_at_full_confidence(client):
    from services.connector_store import create_connector

    assert len(_LONG) == 63
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d55q_s_{token}", f"d55q_d_{token}"
    pg = h.pg_conn()
    cur = pg.cursor()
    try:
        ddl = ", ".join(
            f'"{c}" BIGINT PRIMARY KEY' if c == "Order ID"
            else f'"{c}" TIMESTAMP' if c == "Updated At" else f'"{c}" TEXT'
            for c in _COLS
        )
        cur.execute(f'CREATE TABLE "{src_t}" ({ddl})')  # nosec B608
        cur.execute(
            f'INSERT INTO "{src_t}" VALUES (1, \'crème\', \'a-b\', '  # nosec B608
            "TIMESTAMP '2026-10-01 10:00:00', '42.5'), "
            "(2, '', NULL, TIMESTAMP '2026-10-01 11:00:00', NULL)"
        )
        src = create_connector({"name": f"d55q-s-{token}", "type": "postgresql",
                                "role": "source", "workspace_id": "", "ssl": False,
                                **h.PG, "schema": "public"})
        dst = create_connector({"name": f"d55q-d-{token}", "role": "destination",
                                "workspace_id": "", "ssl": False,
                                **h.dest_config("postgresql")})
        args = {"source_connector_id": src.id, "source_table": src_t,
                "dest_connector_id": dst.id, "dest_table": dst_t,
                "validation_mode": "strict", "sync_mode": "full_refresh_append"}
        h.stage_and_run(client, "start_transfer", args)
        created = [str(r[0]) for r in h.query(
            "postgresql",
            "SELECT column_name FROM information_schema.columns "  # nosec B608
            f"WHERE table_name = '{dst_t}' ORDER BY ordinal_position")]
        assert len(created) == len(_COLS), created

        for tool, extra in (("plan_transfer", {}),
                            ("create_schedule", {"cadence": "every 1 hour",
                                                 "name": f"d55q-{token}"})):
            ok, staged = h.mcp_call(client, tool, {**args, **extra})
            assert ok, (tool, staged)
            assert not h.blockers_of(staged), (tool, h.blockers_of(staged))
            maps = {str(m.get("source")): m for m in _mappings(staged)}
            for col, target in zip(_COLS, created):
                m = maps.get(col)
                assert m is not None, (tool, col, sorted(maps))
                assert str(m.get("target")) == target, (tool, col, m)
                # The top of Datawrap's scale: an exact same-name pair scores
                # 0.99 (scores are capped below 1.0 by design), and a rename
                # Datawrap made itself is that same identity, accent or not.
                assert float(m.get("confidence") or 0) >= 0.99, (tool, col, m)

        cur.execute(
            f'INSERT INTO "{src_t}" VALUES (3, \'naïve\', \'x\', '  # nosec B608
            "TIMESTAMP '2026-10-02 10:00:00', 'z')"
        )
        job = h.stage_and_run(client, "start_transfer", args)
        assert str(job.get("status")) == "completed"
        count = h.query("postgresql", f'SELECT COUNT(*) FROM "{dst_t}"')[0][0]  # nosec B608
        assert int(count) == 5, count  # append: 2 + 3
    finally:
        cur.close()
        h.drop_table("postgresql", src_t)
        h.drop_table("postgresql", dst_t)
