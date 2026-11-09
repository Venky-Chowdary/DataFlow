"""Cross-run stale CDC quarantine replay (live PG source -> PG / MySQL).

Run N quarantines a CDC image of key K (balanced, destination CHECK rejects
it). Run N+1 lands a newer image of K. The operator then relaxes the CHECK and
replays run N's quarantine: the stale image must be refused per row with a
named reason, and an unrelated quarantined key from the same run still lands.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import importlib

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
def test_cross_run_stale_cdc_image_is_refused_on_replay(engine, fake, tmp_path: Path, monkeypatch) -> None:  # noqa: F811
    from fastapi.testclient import TestClient

    from src.main import app
    from tests.route_cert_readback import execute_sql

    import tests.test_identity_cdc_matrix as matrix
    from src.transfer.models import transfer_request_to_dict

    requests: dict[str, dict] = {}

    class _Recording(matrix.UniversalTransferEngine):
        def execute_tracked(self, request, job_id, *a, **k):
            requests[job_id] = transfer_request_to_dict(request)
            return super().execute_tracked(request, job_id, *a, **k)

    monkeypatch.setattr(matrix, "UniversalTransferEngine", _Recording)
    connectors_router = importlib.import_module("src.routers.connectors_router")
    monkeypatch.setattr(connectors_router, "get_mongodb_service", lambda: fake)
    table = "cdcx_" + uuid.uuid4().hex[:8]
    dest = _build_db_endpoint(engine, tmp_path, "dst", table)
    if not _endpoint_reachable(dest):
        pytest.skip(f"{engine} destination not reachable")
    # A function CHECK: the preflight probe reports it not_evaluated, so the
    # balanced run reaches the writer and quarantines the rejected image.
    fn = "length" if engine == "postgresql" else "char_length"
    add = f"ALTER TABLE {dest.table} ADD CONSTRAINT ck_{table} CHECK ({fn}(name) <> 7)"
    drop = _CHECK[engine][1].format(t=dest.table, c=f"ck_{table}")
    try:
        _pg(_create("postgresql", table, "PRIMARY KEY (id)"))
        assert _run(fake, table, dest, "id", validation_mode="balanced").success
        execute_sql(dest, [add])
        # Run N: keys 3 and 4 violate the CHECK and are quarantined; 5 lands.
        _pg([f"INSERT INTO {table} VALUES (3,'eu','toolong',2)",
             f"INSERT INTO {table} VALUES (4,'eu','toolong',2)",
             f"INSERT INTO {table} VALUES (5,'eu','fine',2)"])
        run_n = _run(fake, table, dest, "id", validation_mode="balanced")
        # Run N's Gate-8 reports the quarantined keys missing on this branch
        # (reconcile_step, out of scope here); its quarantine ledger is real.
        rejected = (run_n.destination_summary or {}).get("rejected_details") or []
        assert {str((d.get("values") or {}).get("id")) for d in rejected} >= {"3", "4"}, rejected
        # Run N+1: key 3 gets a newer, valid image that lands.
        _pg([f"UPDATE {table} SET name='ok', updated_at=3 WHERE id=3"])
        # Its Gate-8 still grades key 4 (open in run N) as missing — same
        # reconcile_step gap; the newer image of key 3 does land.
        _run(fake, table, dest, "id", validation_mode="balanced")
        assert ("3", "eu", "ok", "3") in _population(dest)
        # Operator drops the CHECK and replays run N's quarantine.
        execute_sql(dest, [drop])
        parent = fake.jobs[str(run_n.job_id)]
        parent.setdefault("id", str(run_n.job_id))
        parent["transfer_request"] = requests[str(run_n.job_id)]
        parent["status"] = "completed_with_quarantine"
        resp = TestClient(app).post(
            f"/api/v1/connectors/jobs/{run_n.job_id}/quarantine/replay", json={}
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        population = _population(dest)
        assert ("3", "eu", "ok", "3") in population, f"stale replay regressed key 3: {population}"
        assert ("4", "eu", "toolong", "2") in population, f"non-stale key 4 did not replay: {population}"
        stale = body.get("stale_refused") or []
        by_key: dict[str, set[str]] = {}
        for s in stale:
            by_key.setdefault(s["key"]["id"], set()).add(s["reason"])
        assert by_key.get("3") == {"cdc_replay_stale_destination_newer"}, stale
        # Key 4 may carry older images of itself in the ledger; only those are
        # refused (superseded), never because of the destination.
        assert by_key.get("4", set()) <= {"cdc_replay_superseded_in_ledger"}, stale
    finally:
        _pg([f"DROP TABLE IF EXISTS {table}"])
        _drop_slots(table)


def _finding(key: int, lsn: str | None, row: int = 1) -> dict:
    values = {"id": str(key), "name": f"n{key}"}
    if lsn is not None:
        values["_df_lsn"] = lsn
    return {"row": row, "values": values}


def test_partition_refuses_newer_and_equal_destination_lsn_per_row():
    from services.quarantine_dlq import (
        REASON_STALE_DEST_NEWER,
        REASON_SUPERSEDED_IN_LEDGER,
        partition_stale_cdc_replay,
    )

    details = [
        _finding(1, "0/10"),  # dest newer -> refused
        _finding(2, "0/20"),  # dest equal -> refused (already landed)
        _finding(3, "0/30"),  # dest older -> replays
        _finding(4, "0/40"),  # no dest row -> replays
        _finding(5, "0/50"),  # older image of 5 in the same ledger -> superseded
        _finding(5, "0/55"),
        _finding(6, None),    # not a CDC image -> untouched
    ]
    seen: list[list[tuple[str, ...]]] = []

    def dest(keys):
        seen.append(keys)
        return {("1",): "0/99", ("2",): "0/20", ("3",): "0/01"}

    keep, stale = partition_stale_cdc_replay(details, key_columns=["id"], destination_lsns=dest)
    assert seen == [[("1",), ("2",), ("3",), ("4",), ("5",)]]
    assert {(d["values"]["id"], d["values"].get("_df_lsn")) for d in keep} == {
        ("3", "0/30"), ("4", "0/40"), ("5", "0/55"), ("6", None),
    }
    assert {(s["key"]["id"], s["quarantined_lsn"], s["reason"]) for s in stale} == {
        ("1", "0/10", REASON_STALE_DEST_NEWER),
        ("2", "0/20", REASON_STALE_DEST_NEWER),
        ("5", "0/50", REASON_SUPERSEDED_IN_LEDGER),
    }


def test_partition_fails_closed_when_destination_cannot_be_read():
    from services.quarantine_dlq import partition_stale_cdc_replay

    def boom(_keys):
        raise RuntimeError("dest down")

    with pytest.raises(RuntimeError):
        partition_stale_cdc_replay([_finding(1, "0/10")], key_columns=["id"], destination_lsns=boom)


def test_partition_refuses_incomparable_lsn_families():
    from services.quarantine_dlq import REASON_STALE_DEST_NEWER, partition_stale_cdc_replay

    keep, stale = partition_stale_cdc_replay(
        [_finding(1, "0/10")], key_columns=["id"], destination_lsns=lambda _k: {("1",): "mysql-bin.000003:120"}
    )
    assert keep == [] and [s["reason"] for s in stale] == [REASON_STALE_DEST_NEWER]


def test_dual_stamped_finding_keeps_the_lsn_the_stale_guard_matches_on():
    from src.routers.connectors_router import _quarantine_details_to_records

    detail = {"row": 1, "source_values": {"id": 3, "name": "bad"},
              "values": {"id": 3, "name": "bad", "_df_lsn": "0/16B3748"}}
    records, _ = _quarantine_details_to_records([detail])
    assert records and records[0].get("_df_lsn") == "0/16B3748"
