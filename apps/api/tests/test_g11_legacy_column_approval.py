"""G11 review #1: Execute's legacy-column rename must not break the Validate approval.

``adopt_for_request`` retargets a long Map target onto the 63-character
column an earlier run created. Validate hashed the operator's Map rows (Decision
Artifact, Map->DDL identity); Execute then hashed the *renamed* rows and refused
the run with "DDL identity mismatch ... Re-validate", which re-validating could
never fix. The approval is checked against the rows the operator approved;
the rename only changes where the writer puts the values.

Driven through the real Studio routes: Validate (plan preflight) -> approved
hashes -> ``/api/v1/transfer/run`` with ``skip_preflight=false``. Live
PostgreSQL and MySQL; every table is dropped afterwards.
"""

from __future__ import annotations

import uuid

import pytest

from tests import g11_studio_flow as flow
from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)

LONG = "customer_lifetime_value_after_discounts_and_refunds_in_euro_cents_x"  # 68
CUT = LONG[:63]


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _source_table(token: str) -> str:
    table = f"g11a_src_{token}"
    h.execute("postgresql", f"CREATE TABLE {table} (id int PRIMARY KEY, v int)")  # nosec B608
    h.execute("postgresql", f"INSERT INTO {table} VALUES (1, 10), (2, 20)")  # nosec B608
    return table


def _columns(kind: str, table: str) -> list[str]:
    schema = "public" if kind == "postgresql" else h.dest_config(kind)["database"]
    return [r[0] for r in h.query(
        kind,
        "SELECT column_name FROM information_schema.columns "  # nosec B608
        f"WHERE table_schema = '{schema}' AND table_name = '{table}' ORDER BY ordinal_position",
    )]


@pytest.mark.parametrize("policy", ["manual_review", "propagate_all"])
@pytest.mark.parametrize("dest_kind", ["postgresql", "mysql"])
def test_validate_then_execute_into_an_existing_legacy_column(client, dest_kind, policy):
    """``manual_review``: Validate refused the existing column as missing (G6).
    ``propagate_all``: Validate planned ADD COLUMN, then Execute's rename broke
    the approved hashes ("DDL identity mismatch ... Re-validate")."""
    token = uuid.uuid4().hex[:8]
    src_table = _source_table(token)
    dest_table = f"g11a_dst_{token}"
    q = h.quote(dest_kind, CUT)
    h.execute(dest_kind, f"CREATE TABLE {h.quote(dest_kind, dest_table)} (id bigint, {q} bigint)")
    h.execute(dest_kind, f"INSERT INTO {h.quote(dest_kind, dest_table)} VALUES (0, 0)")
    try:
        src, dst = flow.connectors(token, "postgresql", dest_kind)
        database = h.dest_config(dest_kind)["database"]
        source = {"kind": "database", "format": "postgresql", "connector_id": src.id,
                  "database": h.PG["database"], "table": src_table}
        destination = {"kind": "database", "format": dest_kind, "connector_id": dst.id,
                       "database": database, "table": dest_table}
        mappings = [
            {"source": "id", "target": "id", "confidence": 1.0, "source_type": "INTEGER",
             "target_type": "BIGINT"},
            {"source": "v", "target": LONG, "confidence": 1.0, "source_type": "INTEGER",
             "target_type": "BIGINT"},
        ]
        plan_id, pf = flow.validate(
            client, source=source, destination=destination, columns=["id", "v"],
            types={"id": "INTEGER", "v": "INTEGER"}, mappings=mappings,
            sync_mode="full_refresh_append", schema_policy=policy,
        )
        assert pf.get("passed") is True, flow.blockers(pf)
        note = flow.gates_of(pf).get("g23d_existing_target_columns") or {}
        assert CUT in str(note.get("message")), sorted(flow.gates_of(pf))

        run = flow.execute(
            client, plan_id=plan_id, pf=pf, source=source, destination=destination,
            mappings=mappings, sync_mode="full_refresh_append", schema_policy=policy,
        )
        assert run.get("success") is True, (run.get("error"), run.get("error_details"))
        assert (run.get("reconciliation") or {}).get("passed") is True, run.get("reconciliation")
        rows = h.query(
            dest_kind,
            f"SELECT id, {q} FROM {h.quote(dest_kind, dest_table)} ORDER BY id",  # nosec B608
        )
        assert [tuple(int(x) for x in r) for r in rows] == [(0, 0), (1, 10), (2, 20)]
        cols = [c for c in _columns(dest_kind, dest_table)]
        assert cols == ["id", CUT], cols  # no second, full-length column
    finally:
        h.drop_table("postgresql", src_table)
        h.drop_table(dest_kind, dest_table)
