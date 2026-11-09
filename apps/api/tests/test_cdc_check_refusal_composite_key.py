"""CDC CHECK refusal on a 2-column key (region, id) commits with quarantine.

Live PG source -> PG / MySQL, balanced, parent status never forced. Run N
inserts (eu,3) and (us,4), which a destination CHECK refuses, plus (us,3) and
(eu,5), which land. (us,3) shares id 3 with a refused key, so any part of the
path that keys by ``id`` alone would hold out or replay the wrong row.
"""

from __future__ import annotations

import importlib
import uuid
from pathlib import Path

import pytest

from tests.test_identity_cdc_matrix import (
    _CHECK,
    _build_db_endpoint,
    _create,
    _drop_slots,
    _endpoint_reachable,
    _pg,
    _population,
    _run,
    fake,  # noqa: F401 — fixture
)

pytestmark = pytest.mark.skipif(
    not __import__("tests.test_identity_cdc_matrix", fromlist=["SOURCE_READY"]).SOURCE_READY[
        "postgresql"
    ][0],
    reason="PostgreSQL with wal_level=logical not reachable",
)

KEY = "region,id"


def _value_proof(table: str, dest, summary: dict):
    from services.cdc_value_digest import prove_cdc_values
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.connector_capabilities import resolve_driver_type
    from src.transfer.models import EndpointConfig
    from src.transfer.reconcile_step import _held_out_key_tuples
    from tests.test_identity_cdc_matrix import COLS, SOURCES

    source = EndpointConfig(kind="database", format="postgresql", table=table,
                            schema="public", **SOURCES["postgresql"])
    keys = list(summary.get("primary_key_columns") or [])
    proof = prove_cdc_values(
        source_type="postgresql", source_cfg=resolve_connector_config(source),
        source_table=table, dest_type=resolve_driver_type(dest.format),
        dest_cfg=resolve_connector_config(dest), dest_table=str(dest.table),
        mappings=[{"source": c, "target": c} for c in COLS],
        held_out_keys=_held_out_key_tuples(summary, keys), held_out_column=keys,
    )
    assert proof is not None
    return proof


@pytest.mark.parametrize("engine", ["postgresql", "mysql"])
def test_composite_key_check_refusal_commits_with_quarantine_and_replays(
    engine, fake, tmp_path: Path, monkeypatch  # noqa: F811
) -> None:
    from fastapi.testclient import TestClient

    from services.quarantine_dlq import list_dlq_events
    from src.main import app
    from src.transfer.models import transfer_request_to_dict
    from tests.route_cert_readback import execute_sql
    import tests.test_identity_cdc_matrix as matrix

    requests: dict[str, dict] = {}

    class _Recording(matrix.UniversalTransferEngine):
        def execute_tracked(self, request, job_id, *a, **k):
            requests[job_id] = transfer_request_to_dict(request)
            return super().execute_tracked(request, job_id, *a, **k)

    monkeypatch.setattr(matrix, "UniversalTransferEngine", _Recording)
    connectors_router = importlib.import_module("src.routers.connectors_router")
    monkeypatch.setattr(connectors_router, "get_mongodb_service", lambda: fake)
    table = "cdckc_" + uuid.uuid4().hex[:8]
    dest = _build_db_endpoint(engine, tmp_path, "dst", table)
    if not _endpoint_reachable(dest):
        pytest.skip(f"{engine} destination not reachable")
    fn = "length" if engine == "postgresql" else "char_length"
    add = f"ALTER TABLE {dest.table} ADD CONSTRAINT ck_{table} CHECK ({fn}(name) <> 7)"
    drop = _CHECK[engine][1].format(t=dest.table, c=f"ck_{table}")
    try:
        _pg(_create("postgresql", table, "PRIMARY KEY (region, id)"))
        assert _run(fake, table, dest, KEY, validation_mode="balanced").success
        execute_sql(dest, [add])
        _pg([f"INSERT INTO {table} VALUES (3,'eu','toolong',2)",
             f"INSERT INTO {table} VALUES (3,'us','fine',2)",
             f"INSERT INTO {table} VALUES (4,'us','toolong',2)",
             f"INSERT INTO {table} VALUES (5,'eu','fine',2)"])
        run_n = _run(fake, table, dest, KEY, validation_mode="balanced")
        parent = fake.jobs[str(run_n.job_id)]
        summary = run_n.destination_summary or {}
        diag = {
            "status": parent.get("status"),
            "error": str(getattr(run_n, "error", ""))[:400],
            "census": summary.get("keyed_census"),
            "ledger": run_n.row_accounting,
            "rejected_rows": summary.get("rejected_rows"),
        }
        assert parent.get("status") == "completed_with_quarantine", diag
        population = _population(dest)
        assert ("3", "us", "fine", "2") in population, population
        assert ("5", "eu", "fine", "2") in population, population

        rejected = summary.get("rejected_details") or []
        named = sorted(
            (str((d.get("values") or {}).get("region")), str((d.get("values") or {}).get("id")))
            for d in rejected
        )
        assert named == [("eu", "3"), ("us", "4")], rejected
        assert summary.get("rejected_rows") == 2, diag
        events = list_dlq_events(job_id=str(run_n.job_id), limit=500)
        assert sum(int(e.get("rows") or 0) for e in events) == 2, events
        assert (summary.get("keyed_census") or {}).get("held_out_keys") == 2, diag
        ledger = run_n.row_accounting or {}
        assert ledger.get("balanced") is True, diag
        assert ledger.get("unaccounted") == 0, diag

        # Gate-8 addresses rows by the whole key: with ``id`` alone the held-out
        # (eu,3) also hid the valid (us,3) from the ladder and the value proof.
        assert [str(c).lower() for c in summary.get("primary_key_columns") or []] == [
            "region", "id",
        ], summary.get("primary_key_columns")
        rec = getattr(run_n, "reconciliation", None) or {}
        assert "column aggregates differ" not in str(rec.get("message") or ""), rec.get("message")

        # Run N+1 lands a newer image of (eu,3), so run N's image of it is stale.
        _pg([f"UPDATE {table} SET name='ok', updated_at=3 WHERE region='eu' AND id=3"])
        _run(fake, table, dest, KEY, validation_mode="balanced")
        assert ("3", "eu", "ok", "3") in _population(dest)

        execute_sql(dest, [drop])
        parent.setdefault("id", str(run_n.job_id))
        parent["transfer_request"] = requests[str(run_n.job_id)]
        resp = TestClient(app).post(
            f"/api/v1/connectors/jobs/{run_n.job_id}/quarantine/replay", json={}
        )
        assert resp.status_code == 200, resp.text
        population = _population(dest)
        assert ("4", "us", "toolong", "2") in population, population
        assert ("3", "eu", "ok", "3") in population, population
        assert ("3", "us", "fine", "2") in population, population
        by_key: dict[tuple[str, str], set[str]] = {}
        for s in resp.json().get("stale_refused") or []:
            key = s["key"]
            by_key.setdefault((str(key.get("region")), str(key.get("id"))), set()).add(s["reason"])
        assert by_key.get(("eu", "3")) == {"cdc_replay_stale_destination_newer"}, by_key
        assert not by_key.get(("us", "4")), by_key

        # The value proof still sees (us,3): losing it on the destination is a
        # miss even though (eu,3) of the same id is held out.
        execute_sql(dest, [f"DELETE FROM {dest.table} WHERE region='us' AND id=3"])
        assert _value_proof(table, dest, summary).missing == 1
    finally:
        _pg([f"DROP TABLE IF EXISTS {table}"])
        _drop_slots(table)
