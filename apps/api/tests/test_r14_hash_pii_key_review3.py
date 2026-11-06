"""Review of b20fc3f00: per-job hash_pii key — spellings, replay, one read, pinned id, CDC.

B3  every spelling of the transform resolves through pii_hash_key(), so the
    job snapshots lazily on its first digest — sha256 / HASH_PII / a chain get
    a recorded version like "hash_pii" does.
B2  a quarantine replay digests with the PARENT job's key version.
S1  the secret and its version come from one read of the key document.
S2  a pinned deployment key's version changes when the key does.
S6  a long job adopts a rotated key at a batch boundary and records both.
Live PostgreSQL for B3 / B2 (start_transfer -> confirm -> job, replay_quarantine).
"""

from __future__ import annotations

import hashlib
import hmac
import time
import uuid

import pytest

from tests import value_fidelity_route as r


@pytest.fixture
def keys(tmp_path, monkeypatch):
    for name in ("DATAFLOW_PII_HASH_KEY", "DATAWRAP_PII_HASH_KEY", "DATAFLOW_SECRET", "DATAWRAP_SECRET"):
        monkeypatch.delenv(name, raising=False)
    from services import pii_hash_keys

    monkeypatch.setattr(pii_hash_keys, "_collection", lambda: None)
    monkeypatch.setattr(pii_hash_keys, "_file_path", lambda: tmp_path / "pii_hash_keys.json")
    return pii_hash_keys


@pytest.fixture
def client(tmp_path, monkeypatch, keys):
    with r.make_client(tmp_path, monkeypatch) as c:
        for name in ("DATAFLOW_PII_HASH_KEY", "DATAWRAP_PII_HASH_KEY"):
            monkeypatch.delenv(name, raising=False)
        yield c


def _digest(secret: str, value: str) -> str:
    return hmac.new(secret.encode(), value.encode(), hashlib.sha256).hexdigest()[:32]


@pytest.mark.skipif(not r.up(5432), reason="PostgreSQL not reachable")
@pytest.mark.parametrize("spelling", ["hash_pii", "sha256", "HASH_PII", "Hash_Pii", "trim|hash_pii"])
def test_every_spelling_snapshots_the_key_and_records_its_version(client, keys, spelling):
    pg = r.pg_conn()
    table = f"r14s_{uuid.uuid4().hex[:8]}"
    cur = pg.cursor()
    cur.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, email TEXT)")
    cur.execute(f"INSERT INTO {table} VALUES (1, 'a@example.com'), (2, 'b@example.com')")  # nosec B608
    pg.commit()
    src_id, dst_id = r.connectors(uuid.uuid4().hex[:8], "postgresql")
    try:
        job = r.run(
            client, source_connector_id=src_id, source_table=table, dest_connector_id=dst_id,
            dest_table=table + "_out", sync_mode="full_refresh_overwrite", primary_key="id",
            column_transforms={"email": spelling},
        )
        assert job.get("pii_hash_key_version") == "1", (spelling, job.get("pii_hash_key_version"))
        rows = dict(r.query("postgresql", f'SELECT id, email FROM "{table}_out"'))  # nosec B608
        assert rows[1] == _digest(keys.workspace_key(""), "a@example.com"), (spelling, rows)
    finally:
        r.drop("postgresql", table)
        r.drop("postgresql", table + "_out")
        pg.close()


def test_secret_and_version_come_from_one_read(keys, monkeypatch):
    keys.workspace_key("ws-s1")
    keys.rotate_workspace_key("ws-s1", actor="qa")  # v2 exists
    docs = [keys._load("ws-s1")]
    v1_doc = dict(docs[0], active_version=1)
    reads = iter([v1_doc, docs[0], docs[0]])
    monkeypatch.setattr(keys, "_load", lambda scope: next(reads))
    secret, version = keys.active_key("ws-s1")
    assert version == "1"
    assert secret == keys._active_secret(v1_doc), "the v1 secret with version 1, never v1 secret + '2'"


def test_a_pinned_key_change_changes_its_version_and_warns(keys, monkeypatch):
    monkeypatch.setenv("DATAWRAP_PII_HASH_KEY", "a" * 16 + "deployment-key-one")
    first = keys.current_version("ws")
    monkeypatch.setenv("DATAWRAP_PII_HASH_KEY", "b" * 16 + "deployment-key-two")
    second = keys.current_version("ws")
    assert first.startswith("deployment:") and second.startswith("deployment:") and first != second
    assert keys.key_change_warning("ws", first) is not None
    assert keys.secret_for_version("ws", first) == ""  # the old pinned key is gone: fail closed
    assert keys.secret_for_version("ws", second) == "b" * 16 + "deployment-key-two"


def test_a_long_job_adopts_a_rotated_key_at_a_batch_boundary(keys):
    stamps: list[tuple[str, list[str]]] = []
    with keys.job_key_scope("ws-s6", on_resolve=lambda v, h: stamps.append((v, h))) as snap:
        first = keys.snapshot_secret("ws-s6")
        keys.rotate_workspace_key("ws-s6", actor="qa")
        assert keys.snapshot_secret("ws-s6") == first, "a batch never changes key mid-way"
        keys.renew_job_key_at_boundary(max_age_s=0)
        assert keys.snapshot_secret("ws-s6") != first
        assert keys.snapshot_version() == "2"
    assert [v for v, _ in stamps] == ["1", "2"] and stamps[-1][1] == ["1", "2"]
    assert snap.history == ["1", "2"]


def test_a_job_pinned_to_a_version_digests_with_that_version(keys):
    v1 = keys.workspace_key("ws-b2")
    keys.rotate_workspace_key("ws-b2", actor="qa")
    assert keys.workspace_key("ws-b2") != v1
    with keys.job_key_scope("ws-b2", version="1"):
        assert keys.snapshot_secret("ws-b2") == v1
        assert keys.snapshot_version() == "1"


@pytest.mark.skipif(not r.up(5432), reason="PostgreSQL not reachable")
def test_a_quarantine_replay_hashes_with_the_parent_jobs_key(client, keys):
    from src.ai.copilot.tools import DataPilotTools

    from conftest import spend_pilot_ack
    from services.mongodb_service import get_mongodb_service

    pg = r.pg_conn()
    token = uuid.uuid4().hex[:8]
    src, dst = f"r14b2_s_{token}", f"r14b2_d_{token}"
    cur = pg.cursor()
    cur.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, flt DOUBLE PRECISION, email TEXT)")
    cur.executemany(f"INSERT INTO {src} VALUES (%s,%s,%s)", [  # nosec B608
        (1, 1.0, "a@example.com"), (2, 2.5, "b@example.com"), (3, 3.0, "c@example.com"),
    ])
    cur.execute(f"CREATE TABLE {dst} (id INT PRIMARY KEY, flt INTEGER, email TEXT)")
    pg.commit()
    src_id, _ = r.connectors(token, "postgresql")
    from services.connector_store import get_connector

    name = get_connector(src_id).name
    try:
        staged = DataPilotTools().execute("start_transfer", {
            "source_connector_name": name, "source_table": src,
            "dest_connector_name": name, "dest_table": dst,
            "sync_mode": "full_refresh_append", "validation_mode": "lenient",
            "column_transforms": {"email": "hash_pii"},
            "risk_acceptance": {"execution_policy": "QUARANTINE_ROW", "approved_by": "qa", "reason": "ok"},
        })
        assert staged.success, staged.error
        job_id = spend_pilot_ack(staged.output["ack_id"], "r14-b2")["job_id"]
        job: dict = {}
        for _ in range(240):
            job = get_mongodb_service().get_job(job_id) or {}
            if str(job.get("status") or "") in {"completed", "completed_with_quarantine", "failed", "cancelled"}:
                break
            time.sleep(0.25)
        assert job.get("status") == "completed_with_quarantine", job.get("error")
        assert job.get("pii_hash_key_version") == "1"
        parent_key = keys.workspace_key("")
        keys.rotate_workspace_key("", actor="qa")
        cur.execute(f"ALTER TABLE {dst} ALTER COLUMN flt TYPE DOUBLE PRECISION")
        pg.commit()
        replay = DataPilotTools().execute("replay_quarantine", {"job_id": job_id})
        assert replay.success, replay.error
        out = spend_pilot_ack(replay.output["ack_id"], "r14-b2")
        child = get_mongodb_service().get_job(str(out.get("job_id") or out.get("child_job_id") or "")) or {}
        rows = dict(r.query("postgresql", f"SELECT id, email FROM {dst}"))  # nosec B608
        assert rows[2] == _digest(parent_key, "b@example.com"), "replayed with the parent's key"
        assert rows[1] == _digest(parent_key, "a@example.com")
        assert child.get("pii_hash_key_version") == "1", child.get("pii_hash_key_version")
    finally:
        r.drop("postgresql", src)
        r.drop("postgresql", dst)
        pg.close()
