"""QA review #7: one hash_pii key per job, recorded, and a warning when it changed.

``transform_engine`` resolved the workspace key for every value, so a rotation
while a job ran gave the same value two digests inside one job, the job did not
say which key it used, and the next run silently stopped matching the rows an
upsert keyed on a hashed column had written. Now a job snapshots the key and
its version once (``services.pii_hash_keys.transfer_job_scope``), stamps
``pii_hash_key_version`` on the job and the route's load history, and Validate
warns (``pii_hash_key_changed``) when the key changed since this route's last
load. Live PostgreSQL -> PostgreSQL through start_transfer -> confirm -> job.
"""

from __future__ import annotations

import uuid

import pytest

from tests import value_fidelity_route as r

pytestmark = pytest.mark.skipif(not r.up(5432), reason="PostgreSQL not reachable")

EMAIL = "same.person@example.com"


@pytest.fixture
def client(tmp_path, monkeypatch):
    with r.make_client(tmp_path, monkeypatch) as c:
        for name in ("DATAFLOW_PII_HASH_KEY", "DATAWRAP_PII_HASH_KEY", "DATAFLOW_SECRET", "DATAWRAP_SECRET"):
            monkeypatch.delenv(name, raising=False)
        from services import pii_hash_keys

        monkeypatch.setattr(pii_hash_keys, "_collection", lambda: None)
        monkeypatch.setattr(pii_hash_keys, "_file_path", lambda: tmp_path / "pii_hash_keys.json")
        yield c


@pytest.fixture
def route():
    pg = r.pg_conn()
    token = uuid.uuid4().hex[:8]
    table = f"r14k_{token}"
    cur = pg.cursor()
    cur.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, email TEXT)")
    for i in range(1, 41):
        cur.execute(f"INSERT INTO {table} VALUES (%s, %s)", (i, EMAIL if i in (1, 40) else f"p{i}@example.com"))  # nosec B608
    pg.commit()
    src_id, dst_id = r.connectors(token, "postgresql")
    try:
        yield {"table": table, "dest": table + "_out", "src": src_id, "dst": dst_id}
    finally:
        r.drop("postgresql", table)
        r.drop("postgresql", table + "_out")
        pg.close()


def _kwargs(route):
    return dict(
        source_connector_id=route["src"], source_table=route["table"],
        dest_connector_id=route["dst"], dest_table=route["dest"],
        sync_mode="full_refresh_overwrite", primary_key="id",
        column_transforms={"email": "hash_pii"},
    )


def test_a_rotation_mid_job_leaves_the_job_with_one_digest_and_a_recorded_version(client, route, monkeypatch):
    import services.transform_engine as te
    from services.pii_hash_keys import key_status, rotate_workspace_key, snapshot_version

    real = te._hash_pii
    rotated: list[int] = []

    def rotate_after_first(value):
        digest = real(value)
        # Rotate once the job is running (Validate's sample digests run first).
        if not rotated and snapshot_version():
            rotated.append(rotate_workspace_key("", actor="qa-mid-job")["active_version"])
        return digest

    monkeypatch.setattr(te, "_hash_pii", rotate_after_first)
    job = r.run(client, **_kwargs(route))
    assert rotated == [2], "the key rotated while the job was hashing"
    assert job.get("pii_hash_key_version") == "1", job.get("pii_hash_key_version")
    rows = dict(r.query("postgresql", f'SELECT id, email FROM "{route["dest"]}"'))  # nosec B608
    assert rows[1] == rows[40], "one value, one digest within one job"
    assert key_status("")["active_version"] == 2


def test_the_next_validate_warns_that_the_key_changed_since_the_last_run(client, route):
    from services.pii_hash_keys import rotate_workspace_key

    first = r.run(client, **_kwargs(route))
    assert first.get("pii_hash_key_version") == "1"
    _staged, plan, _ = r.stage(**_kwargs(route))
    ids = [w.get("id") for w in (plan.get("preflight") or {}).get("warnings") or []]
    assert "pii_hash_key_changed" not in ids, ids

    rotate_workspace_key("", actor="qa")
    _staged, plan, _ = r.stage(**_kwargs(route))
    warnings = {w.get("id"): w for w in (plan.get("preflight") or {}).get("warnings") or []}
    assert "pii_hash_key_changed" in warnings, list(warnings)
    text = warnings["pii_hash_key_changed"]["message"]
    assert "version 1" in text and "version 2" in text and "will not match" in text
