"""QA demo (557f556c): Mirror keeps deleted keys as ``_deleted`` tombstones.

Studio's Mirror option promises "Soft-delete dest keys missing from the source
(_deleted). Physical COUNT(*) stays; active population is COUNT(*) WHERE NOT
_deleted." (apps/web/src/lib/transferConstants.ts, DestinationAdvancedDrawer,
ScheduleForm, helpDocs; services/mirror_engine.py's own contract;
docs/ZERO_LOSS_PROPERTIES.md). QA ran a mirror on production: rows were
physically deleted (552 -> 540) and no ``_deleted`` column was created. The
engine defaulted an undeclared stream contract to a hard DELETE.

Contract pinned here, through the UI's REST route (``/api/v1/preflight/run``
then ``/api/v1/transfer/execute``), PG -> PG and PG -> MySQL:

* run 1 lands every source row;
* run 2, after source deletes: the physical row stays, ``_deleted`` (and the
  ``_deleted_at`` stamp) is set on exactly the deleted keys, the survivors are
  byte-identical to the source (NULL, '', whitespace, unicode), and Gate-8
  verifies the active population (``COUNT(*) WHERE NOT _deleted``) against the
  source while reporting the flagged rows;
* run 3, a deleted key re-appearing: it is reactivated, its stamp cleared;
* a hard delete happens only when the stream contract declares it.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)

_TXT = {"postgresql": "TEXT", "mysql": "VARCHAR(60)"}
_ROWS = [
    (1, "alpha"), (2, None), (3, ""), (4, "   "), (5, "Zoë ✓ 中文 🎉"),
    (6, "f"), (7, "g"), (8, "h"), (9, "i"), (10, "j"), (11, "k"), (12, "l"),
]
_DELETED = (2, 4, 9)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _lit(kind: str, v):
    if v is None:
        return "NULL"
    return "'" + str(v).replace("'", "''") + "'"


def _connectors(token: str, dst_kind: str):
    from services.connector_store import create_connector

    src = create_connector({
        "name": f"r7m-src-{token}", "role": "source", "workspace_id": "", "ssl": False,
        **h.dest_config("postgresql"),
    })
    dst = create_connector({
        "name": f"r7m-dst-{token}", "role": "destination", "workspace_id": "", "ssl": False,
        **h.dest_config(dst_kind),
    })
    return src, dst


def _dest_columns(kind: str, table: str) -> set[str]:
    if kind == "mysql":
        rows = h.query(kind, f"SHOW COLUMNS FROM `{table}`")  # nosec B608
    else:
        rows = h.query(
            kind,
            "SELECT column_name FROM information_schema.columns "  # nosec B608
            f"WHERE table_name = '{table}'",
        )
    return {str(r[0]) for r in rows}


def _dest_rows(kind: str, table: str) -> list[tuple]:
    q = h.quote(kind, table)
    rows = h.query(
        kind,
        f"SELECT id, note, {h.quote(kind, '_deleted')}, {h.quote(kind, '_deleted_at')} "  # nosec B608
        f"FROM {q} ORDER BY id",
    )
    return [(int(r[0]), r[1], bool(r[2]), r[3]) for r in rows]


def _job(client, src, dst, src_table: str, dest_table: str, contract_extra=None) -> dict:
    contract = {"name": src_table, "selected": True, "sync_mode": "mirror",
                "primary_key": "id", **(contract_extra or {})}
    mappings = [{"source": c, "target": c, "confidence": 1.0} for c in ("id", "note")]
    pf = client.post("/api/v1/preflight/run", json={
        "dataset_name": src_table, "columns": ["id", "note"],
        "column_types": {"id": "integer", "note": "text"},
        "mappings": mappings, "source_connector_id": src.id,
        "source_kind": "database", "source_type": "postgresql",
        "source_table": src_table, "connector_id": dst.id,
        "dest_kind": "database", "dest_type": dst.type,
        "dest_table": dest_table, "sync_mode": "mirror",
        "stream_contracts": [contract], "validation_mode": "strict",
    })
    assert pf.status_code == 200, pf.text
    resp = client.post("/api/v1/transfer/execute", json={
        "source": {"kind": "database", "format": "postgresql",
                   "connector_id": src.id, "table": src_table},
        "destination": {"kind": "database", "format": dst.type,
                        "connector_id": dst.id, "table": dest_table},
        "mappings": mappings, "sync_mode": "mirror",
        "stream_contracts": [contract], "validation_mode": "strict",
        "async_mode": False, "skip_preflight": True,
    })
    assert resp.status_code == 200, resp.text
    body = resp.json()
    from services.mongodb_service import get_mongodb_service

    job = get_mongodb_service().get_job(str(body.get("job_id") or "")) or {}
    assert str(job.get("status")) == "completed", {
        k: job.get(k) for k in ("status", "error", "message")
    }
    return job


def _summary(job: dict) -> dict:
    summary = dict(job.get("destination_summary") or {})
    summary.update(summary.get("mirror") or {})
    return summary


@pytest.mark.parametrize("dst_kind", ["postgresql", "mysql"])
def test_mirror_soft_deletes_through_studio_rest_route(client, dst_kind):
    token = uuid.uuid4().hex[:8]
    src_table = f"r7m_s_{token}"
    dest_table = f"r7m_d_{token}"
    try:
        h.execute("postgresql", f'CREATE TABLE "{src_table}" (id INT PRIMARY KEY, note TEXT NULL)')
        for rid, note in _ROWS:
            h.execute("postgresql", f'INSERT INTO "{src_table}" VALUES ({rid}, {_lit("postgresql", note)})')  # nosec B608
        src, dst = _connectors(token, dst_kind)

        _job(client, src, dst, src_table, dest_table)  # run 1
        assert len(h.query(dst_kind, f"SELECT id FROM {h.quote(dst_kind, dest_table)}")) == len(_ROWS)  # nosec B608

        h.execute("postgresql", f'DELETE FROM "{src_table}" WHERE id IN {_DELETED}')  # nosec B608
        job2 = _job(client, src, dst, src_table, dest_table)  # run 2
        cols = _dest_columns(dst_kind, dest_table)
        assert {"_deleted", "_deleted_at"} <= cols, cols
        got = _dest_rows(dst_kind, dest_table)
        assert len(got) == len(_ROWS), got  # physical COUNT(*) stays
        assert {r[0] for r in got if r[2]} == set(_DELETED), got
        assert all(r[3] is not None for r in got if r[2]), got  # stamped
        assert all(r[3] is None for r in got if not r[2]), got
        survivors = [(r[0], r[1]) for r in got if not r[2]]
        assert survivors == [r for r in _ROWS if r[0] not in _DELETED], survivors
        s2 = _summary(job2)
        assert s2.get("mirror_delete_mode") == "soft", s2
        assert s2.get("soft_deleted") == len(_DELETED), s2
        assert s2.get("tombstone_rows") == len(_DELETED), s2
        assert s2.get("physical_rows") == len(_ROWS), s2
        assert s2.get("active_rows") == len(_ROWS) - len(_DELETED), s2
        # Gate-8 closes the active population and accounts for the flagged rows.
        ra = job2.get("row_accounting") or {}
        assert ra.get("conservation_kind") == "mirror", ra
        assert ra.get("balanced") is True and ra.get("unaccounted") == 0, ra
        assert ra.get("active_count") == len(_ROWS) - len(_DELETED), ra
        assert ra.get("inferred_deletes") == len(_DELETED), ra
        assert ra.get("dest_count") == len(_ROWS), ra  # physical, tombstones included

        # Run 3: a deleted key comes back -> reactivated, stamp cleared.
        h.execute("postgresql", f"INSERT INTO \"{src_table}\" VALUES (9, 'back')")  # nosec B608
        job3 = _job(client, src, dst, src_table, dest_table)
        got = _dest_rows(dst_kind, dest_table)
        row9 = [r for r in got if r[0] == 9][0]
        assert row9 == (9, "back", False, None), row9
        assert {r[0] for r in got if r[2]} == {2, 4}, got
        s3 = _summary(job3)
        assert s3.get("reactivated") == 1, s3
        assert s3.get("active_rows") == len(_ROWS) - 2, s3

        # A hard delete happens only when the stream contract declares it.
        hard_table = dest_table + "_h"
        hard = {"delete_mode": "hard"}
        try:
            h.execute("postgresql", f"DELETE FROM \"{src_table}\" WHERE id = 9")  # nosec B608
            _job(client, src, dst, src_table, hard_table, hard)
            h.execute("postgresql", f"INSERT INTO \"{src_table}\" VALUES (9, 'x')")  # nosec B608
            _job(client, src, dst, src_table, hard_table, hard)
            h.execute("postgresql", f"DELETE FROM \"{src_table}\" WHERE id = 9")  # nosec B608
            jh = _job(client, src, dst, src_table, hard_table, hard)
            assert "_deleted" not in _dest_columns(dst_kind, hard_table)
            ids = {int(r[0]) for r in h.query(dst_kind, f"SELECT id FROM {h.quote(dst_kind, hard_table)}")}  # nosec B608
            assert 9 not in ids and len(ids) == len(_ROWS) - len(_DELETED), ids
            assert _summary(jh).get("mirror_delete_mode") == "hard"
        finally:
            h.drop_table(dst_kind, hard_table)
    finally:
        h.drop_table("postgresql", src_table)
        h.drop_table(dst_kind, dest_table)


@pytest.mark.parametrize("dst_kind", ["postgresql", "mysql"])
def test_mirror_soft_deletes_through_studio_plan_and_run(client, dst_kind, tmp_path, monkeypatch):
    """Studio's Validate (persisted plan) and Execute (multipart /transfer/run)."""
    import services.transfer_plan_store as plan_store
    from tests.r7_studio_harness import Studio, blockers, preflight_mappings

    monkeypatch.setattr(plan_store, "STORE_PATH", tmp_path / "transfer_plans.json")
    token = uuid.uuid4().hex[:8]
    src_table = f"r7m_s_{token}"
    dest_table = f"r7m_d_{token}"
    contracts = [{"name": src_table, "selected": True, "sync_mode": "mirror", "primary_key": "id"}]
    try:
        h.execute("postgresql", f'CREATE TABLE "{src_table}" (id INT PRIMARY KEY, note TEXT NULL)')
        for rid, note in _ROWS:
            h.execute("postgresql", f'INSERT INTO "{src_table}" VALUES ({rid}, {_lit("postgresql", note)})')  # nosec B608
        src, dst = _connectors(token, dst_kind)
        studio = Studio(
            client,
            source={"kind": "database", "format": "postgresql", "connector_id": src.id, "table": src_table},
            destination={"kind": "database", "format": dst.type, "connector_id": dst.id, "table": dest_table},
            policies={"sync_mode": "mirror", "schema_policy": "manual_review",
                      "validation_mode": "strict", "stream_contracts": contracts},
        )

        def run() -> dict:
            si, di = studio.introspect()
            sc, ss = studio.columns(si)
            dc, ds = studio.columns(di) if di.get("table_exists") else ([], {})
            maps = preflight_mappings(
                studio.map(sc, ss, dc, ds, table_exists=bool(di.get("table_exists"))),
                approve=True, dest_columns=dc if dc else None,
            )
            pf = studio.validate(sc, ss, dc, ds, maps)
            assert pf.get("passed"), blockers(pf)
            job = studio.execute(maps, stream_contracts_json=contracts)
            assert job.get("status") == "completed", job.get("error") or job.get("message")
            return job

        run()
        h.execute("postgresql", f'DELETE FROM "{src_table}" WHERE id IN {_DELETED}')  # nosec B608
        job = run()
        got = _dest_rows(dst_kind, dest_table)
        assert len(got) == len(_ROWS), got
        assert {r[0] for r in got if r[2]} == set(_DELETED), got
        assert [(r[0], r[1]) for r in got if not r[2]] == [r for r in _ROWS if r[0] not in _DELETED]
        ra = job.get("row_accounting") or {}
        assert ra.get("balanced") is True and ra.get("active_count") == len(_ROWS) - len(_DELETED), ra
    finally:
        h.drop_table("postgresql", src_table)
        h.drop_table(dst_kind, dest_table)
