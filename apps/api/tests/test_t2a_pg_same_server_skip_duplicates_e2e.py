"""Review defect (a): same-server PostgreSQL → PostgreSQL skip_duplicates, incremental_deduped.

Reported: when the source changed, Gate-8's key check flagged the rows
skip_duplicates kept as "changed". On this base the run was worse than red:
the same-server route took the engine COPY upsert (``services.copy_upsert``),
whose only merge is ``ON CONFLICT … DO UPDATE``, so the rows skip_duplicates
must keep were overwritten with the changed source values under a green
Gate-8 — the stored rows were lost and nothing said so. A skip_duplicates run
now declines the COPY route; the row path skips stored keys, records them
(``skipped_identity_keys``) and every Gate-8 layer leaves them out, so the
run is green over exactly the rows it wrote, and a wrong write still fails
naming only its own key.

Real engine (``execute_tracked``), live PostgreSQL on both sides of one server.
"""

from __future__ import annotations

import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests import value_fidelity_route as r  # noqa: E402

_MODE = "incremental_deduped"


@pytest.fixture()
def store(tmp_path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


@pytest.fixture()
def pg():
    conn = r.pg_conn()
    table = "t2a_" + uuid.uuid4().hex[:8]
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT, updated_at TIMESTAMP)")
    for i in range(1, 31):
        cur.execute(
            f"INSERT INTO {table} VALUES (%s, %s, timestamp '2026-01-01' + %s * interval '1 minute')",  # nosec B608
            (i, f"n{i}", i),
        )
    try:
        yield cur, table
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        cur.execute(f"DROP TABLE IF EXISTS {table}_out")  # nosec B608
        conn.close()


def _endpoint(table: str, **extra) -> EndpointConfig:
    c = r.PG
    return EndpointConfig(kind="database", format="postgresql", table=table, host=c["host"],
                          port=c["port"], database=c["database"], username=c["username"],
                          password=c["password"], schema="public", extra=extra)


def _run(store, table: str):
    request = TransferRequest(
        source=_endpoint(table),
        destination=_endpoint(table + "_out", duplicate_policy="skip_duplicates"),
        sync_mode=_MODE,
        stream_contracts=[{"name": table, "primary_key": ["id"], "cursor_field": "updated_at",
                           "sync_mode": _MODE, "selected": True}],
        skip_preflight=True,
        validation_mode="strict",
    )
    job_id = "t2a" + uuid.uuid4().hex[:16]
    store.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    return result, store.get_job(job_id) or {}


def _change_source(cur, table: str) -> None:
    cur.execute(
        f"UPDATE {table} SET name = 'changed at source', updated_at = timestamp '2026-02-01' "  # nosec B608
        "WHERE id IN (3, 5)"
    )
    cur.execute(f"INSERT INTO {table} VALUES (31, 'n31', timestamp '2026-02-02')")  # nosec B608


def test_changed_source_keeps_the_stored_rows_and_verifies_green(store, pg):
    cur, table = pg
    first, _job = _run(store, table)
    assert first.success, first.error
    _change_source(cur, table)
    second, job = _run(store, table)
    assert second.success, second.error
    cur.execute(f"SELECT id, name FROM {table}_out ORDER BY id")  # nosec B608
    stored = dict(cur.fetchall())
    assert len(stored) == 31
    assert stored[3] == "n3" and stored[5] == "n5", "skip_duplicates must keep the stored rows"
    assert stored[31] == "n31"
    summary = job.get("destination_summary") or {}
    assert sorted(str(k) for k in summary.get("skipped_identity_keys") or []) == ["3", "5"]
    recon = job.get("reconciliation") or {}
    assert recon.get("passed") is True, recon.get("message")
    assert not (recon.get("culprit_keys") or {}).get("changed"), recon.get("culprit_keys")


def test_a_wrong_write_still_fails_naming_only_its_key(store, pg, monkeypatch):
    cur, table = pg
    first, _job = _run(store, table)
    assert first.success, first.error
    _change_source(cur, table)
    real = engine_mod.run_reconciliation

    def _corrupt_then_verify(**kwargs):
        cur.execute(f"UPDATE {table}_out SET name = 'WRONG' WHERE id = 31")  # nosec B608
        return real(**kwargs)

    monkeypatch.setattr(engine_mod, "run_reconciliation", _corrupt_then_verify)
    second, job = _run(store, table)
    assert not second.success
    changed = [str(k) for k in ((job.get("reconciliation") or {}).get("culprit_keys") or {}).get("changed", [])]
    assert changed == ["31"], job.get("reconciliation", {}).get("message")
