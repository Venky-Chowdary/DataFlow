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


@pytest.mark.parametrize("engine", ["postgresql", "mysql"])
def test_cross_run_deleted_cdc_key_is_not_resurrected_on_replay(engine, fake, monkeypatch, tmp_path: Path) -> None:
    """Run N quarantines keys 3 and 4; run N+1 deletes 3; replay of N keeps 3 deleted."""
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
    table = "cdcd_" + uuid.uuid4().hex[:8]
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
        rejected = (run_n.destination_summary or {}).get("rejected_details") or []
        assert {str((d.get("values") or {}).get("id")) for d in rejected} >= {"3", "4"}, rejected
        # Run N+1 deletes key 3 at the source (Gate-8 grading of open key 4: see above).
        _pg([f"DELETE FROM {table} WHERE id=3"])
        _run(fake, table, dest, "id", validation_mode="balanced")
        assert not [r for r in _population(dest) if r[0] == "3"]
        execute_sql(dest, [drop])
        parent = fake.jobs[str(run_n.job_id)]
        parent.setdefault("id", str(run_n.job_id))
        parent["transfer_request"] = requests[str(run_n.job_id)]
        parent["status"] = "completed_with_quarantine"
        resp = TestClient(app).post(
            f"/api/v1/connectors/jobs/{run_n.job_id}/quarantine/replay", json={}
        )
        assert resp.status_code == 200, resp.text
        population = _population(dest)
        assert not [r for r in population if r[0] == "3"], f"replay resurrected deleted key 3: {population}"
        assert ("4", "eu", "toolong", "2") in population, f"non-stale key 4 did not replay: {population}"
        by_key: dict[str, set[str]] = {}
        for s in resp.json().get("stale_refused") or []:
            by_key.setdefault(s["key"]["id"], set()).add(s["reason"])
        assert by_key.get("3") == {"deleted_after"}, by_key
        assert by_key.get("4", set()) <= {"cdc_replay_superseded_in_ledger"}, by_key
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


def test_partition_refuses_key_tombstoned_at_or_after_the_image():
    from services.quarantine_dlq import REASON_DELETED_AFTER, partition_stale_cdc_replay

    keep, stale = partition_stale_cdc_replay(
        [_finding(1, "0/10"), _finding(2, "0/20"), _finding(3, "0/30")],
        key_columns=["id"],
        destination_lsns=lambda _k: {},
        tombstones={"pk:1": "0/50", "pk:2": "0/20", "pk:3": "0/05"},
        deleted_after_unproven=lambda _lsn: False,
    )
    assert [d["values"]["id"] for d in keep] == ["3"]
    assert {(s["key"]["id"], s["reason"], s.get("deleted_lsn")) for s in stale} == {
        ("1", REASON_DELETED_AFTER, "0/50"), ("2", REASON_DELETED_AFTER, "0/20"),
    }


def _job(jid: str, deletes: int, watermark: str, tracked: bool = False) -> dict:
    cdc = {"deletes": deletes, "watermark": watermark, "cursor_key": "pipe"}
    if tracked:
        cdc["quarantine_tombstones"] = True
    return {"id": jid, "destination_summary": {"cursor_key": "pipe", "cdc": cdc}}


def test_fallback_refuses_absent_key_only_after_untracked_later_deletes():
    from services.quarantine_dlq import (
        REASON_POSSIBLY_DELETED_AFTER,
        later_untracked_deletes,
        partition_stale_cdc_replay,
    )

    def run(jobs, *, tagged=False):
        unproven = later_untracked_deletes(jobs, parent_job_id="n", pipeline_key="pipe", parent_tagged=tagged)
        return partition_stale_cdc_replay(
            [_finding(1, "0/10"), _finding(2, "0/20")],
            key_columns=["id"],
            destination_lsns=lambda _k: {("2",): "0/01"},
            deleted_after_unproven=unproven,
        )

    # Later run applied deletes, no tombstones: absent key 1 is held, present key 2 replays.
    keep, stale = run([_job("n", 5, "0/10"), _job("n1", 1, "0/90")])
    assert [d["values"]["id"] for d in keep] == ["2"]
    assert [(s["key"]["id"], s["reason"]) for s in stale] == [("1", REASON_POSSIBLY_DELETED_AFTER)]
    # No later deletes (or only earlier / other-pipeline ones): absent key still replays.
    other = {"id": "x", "destination_summary": {"cursor_key": "other", "cdc": {"deletes": 3}}}
    keep, stale = run([_job("n", 5, "0/90"), _job("old", 2, "0/05"), other])
    assert stale == [] and len(keep) == 2
    # A tracked later run covers a tagged parent precisely (tombstones decide).
    keep, stale = run([_job("n1", 1, "0/90", tracked=True)], tagged=True)
    assert stale == [] and len(keep) == 2
    # ...but not a legacy untagged parent.
    _keep, stale = run([_job("n1", 1, "0/90", tracked=True)], tagged=False)
    assert [s["reason"] for s in stale] == [REASON_POSSIBLY_DELETED_AFTER]


def _cdc_state():
    from src.transfer.cdc_transfer import CdcState

    return CdcState(cursor_key="pipe")


def test_quarantined_key_delete_is_tombstoned_before_advance(monkeypatch):
    import services.quarantine_dlq as dlq
    from services.cdc_engine import ChangeBatch
    from src.transfer.cdc_transfer import _record_quarantined_key_deletes

    lookups: list[tuple] = []
    recorded: list[dict] = []
    monkeypatch.setattr(dlq, "open_pipeline_quarantine_findings",
                        lambda pk, t: lookups.append((pk, t)) or [_finding(3, "0/10"), _finding(4, "0/11")])
    monkeypatch.setattr(dlq, "record_cdc_key_deletes", lambda **kw: recorded.append(kw))
    dest = type("D", (), {"table": "t"})()
    state = _cdc_state()
    _record_quarantined_key_deletes(state, ChangeBatch(deletes=["9"], resume_token="0/50"), ["id"], "j", dest)
    assert recorded == [] and lookups == [("pipe", "t")]
    _record_quarantined_key_deletes(state, ChangeBatch(deletes=["3"], resume_token="0/60"), ["id"], "j", dest)
    assert lookups == [("pipe", "t")]  # loaded once per run
    assert [(r["identities"], r["dest_table"], r["pipeline_key"]) for r in recorded] == [(["pk:3"], "t", "pipe")]
    # A window without deletes costs nothing.
    _record_quarantined_key_deletes(_cdc_state(), ChangeBatch(inserts=[{"id": "3"}]), ["id"], "j", dest)
    assert lookups == [("pipe", "t")]


def test_tombstone_persistence_failure_refuses_watermark_advance(monkeypatch):
    import services.quarantine_dlq as dlq
    from services.cdc_engine import ChangeBatch
    from src.transfer.cdc_transfer import _record_quarantined_key_deletes

    monkeypatch.setattr(dlq, "open_pipeline_quarantine_findings", lambda pk, t: [_finding(3, "0/10")])

    def boom(**_kw):
        raise OSError("dlq down")

    monkeypatch.setattr(dlq, "record_cdc_key_deletes", boom)
    dest = type("D", (), {"table": "t"})()
    with pytest.raises(RuntimeError, match="refuse watermark advance"):
        _record_quarantined_key_deletes(_cdc_state(), ChangeBatch(deletes=["3"]), ["id"], "j", dest)


def test_delete_of_a_key_the_destination_never_held_still_counts_as_a_later_delete():
    from services.quarantine_dlq import later_untracked_deletes

    job = _job("n1", 0, "0/90")
    job["destination_summary"]["cdc"]["delete_events"] = 1
    unproven = later_untracked_deletes([job], parent_job_id="n", pipeline_key="pipe", parent_tagged=False)
    assert unproven("0/10") is True


def test_tombstone_written_during_a_mongo_outage_survives_mongo_recovery(monkeypatch, tmp_path: Path):
    from services import quarantine_dlq as dlq

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "dlq.jsonl")
    monkeypatch.setattr(dlq, "_dlq_coll", lambda: None)
    dlq.record_cdc_key_deletes(job_id="j2", pipeline_key="pipe", dest_table="t", identities=["k1"], lsn="0/40")

    class _Recovered:
        def find(self, _q):
            return []

    monkeypatch.setattr(dlq, "_dlq_coll", lambda: _Recovered())
    assert dlq.cdc_key_tombstones("pipe", "t") == {"k1": "0/40"}


def test_parent_is_tagged_when_any_quarantine_chunk_carries_the_pipeline(monkeypatch):
    from services import quarantine_dlq as dlq

    events = [
        {"action": "quarantine", "details": {"rejected_details": []}},
        {"action": "quarantine", "details": {"pipeline_key": "pipe", "dest_table": "t"}},
    ]
    monkeypatch.setattr(dlq, "list_dlq_events", lambda **_k: events)
    assert dlq.job_quarantine_pipeline_tagged("n") is True
    monkeypatch.setattr(dlq, "list_dlq_events", lambda **_k: events[:1])
    assert dlq.job_quarantine_pipeline_tagged("n") is False
