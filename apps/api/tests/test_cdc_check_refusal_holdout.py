"""CDC run whose destination CHECK refuses two keys commits with quarantine (QA4).

Live PG source -> PG / MySQL, balanced, parent status never forced. Run N
inserts keys 3 and 4 (refused by a destination CHECK) and key 5 (lands). The
census must hold the two refused keys out, the ledger must close, each refused
row is persisted to the DLQ once (image repair used to re-apply and re-persist
the same refused image), and replay of run N lands key 4 while the stale image
of key 3 is still refused.
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


@pytest.mark.parametrize("engine", ["postgresql", "mysql"])
def test_cdc_check_refusal_commits_with_quarantine_and_replays(
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
    table = "cdck_" + uuid.uuid4().hex[:8]
    dest = _build_db_endpoint(engine, tmp_path, "dst", table)
    if not _endpoint_reachable(dest):
        pytest.skip(f"{engine} destination not reachable")
    fn = "length" if engine == "postgresql" else "char_length"
    add = f"ALTER TABLE {dest.table} ADD CONSTRAINT ck_{table} CHECK ({fn}(name) <> 7)"
    drop = _CHECK[engine][1].format(t=dest.table, c=f"ck_{table}")
    try:
        _pg(_create("postgresql", table, "PRIMARY KEY (id)"))
        assert _run(fake, table, dest, "id", validation_mode="balanced").success
        execute_sql(dest, [add])
        _pg([f"INSERT INTO {table} VALUES (3,'eu','toolong',2)",
             f"INSERT INTO {table} VALUES (4,'eu','toolong',2)",
             f"INSERT INTO {table} VALUES (5,'eu','fine',2)"])
        run_n = _run(fake, table, dest, "id", validation_mode="balanced")
        parent = fake.jobs[str(run_n.job_id)]
        summary = run_n.destination_summary or {}
        assert parent.get("status") == "completed_with_quarantine", (
            parent.get("status"), getattr(run_n, "error", ""))
        assert ("5", "eu", "fine", "2") in _population(dest)

        rejected = summary.get("rejected_details") or []
        assert sorted(str((d.get("values") or {}).get("id")) for d in rejected) == ["3", "4"], rejected
        assert summary.get("rejected_rows") == 2, summary.get("rejected_rows")
        events = list_dlq_events(job_id=str(run_n.job_id), limit=500)
        assert sum(int(e.get("rows") or 0) for e in events) == 2, events

        census = summary.get("keyed_census") or {}
        assert census.get("held_out_keys") == 2, census
        ledger = run_n.row_accounting or {}
        assert ledger.get("balanced") is True, ledger
        assert ledger.get("unaccounted") == 0, ledger

        # Run N+1 lands a newer image of key 3, so run N's image of 3 is stale.
        _pg([f"UPDATE {table} SET name='ok', updated_at=3 WHERE id=3"])
        _run(fake, table, dest, "id", validation_mode="balanced")
        assert ("3", "eu", "ok", "3") in _population(dest)

        execute_sql(dest, [drop])
        parent.setdefault("id", str(run_n.job_id))
        parent["transfer_request"] = requests[str(run_n.job_id)]
        resp = TestClient(app).post(
            f"/api/v1/connectors/jobs/{run_n.job_id}/quarantine/replay", json={}
        )
        assert resp.status_code == 200, resp.text
        population = _population(dest)
        assert ("4", "eu", "toolong", "2") in population, population
        assert ("3", "eu", "ok", "3") in population, population
        by_key: dict[str, set[str]] = {}
        for s in resp.json().get("stale_refused") or []:
            by_key.setdefault(s["key"]["id"], set()).add(s["reason"])
        assert by_key.get("3") == {"cdc_replay_stale_destination_newer"}, by_key
        assert not by_key.get("4"), by_key
    finally:
        _pg([f"DROP TABLE IF EXISTS {table}"])
        _drop_slots(table)
