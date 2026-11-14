"""Reversibility backstop on live non-SQL writers (review 5, H-2).

The engine hands a writer one batch at a time and most non-SQL writers never
pass a running row number, so each batch starts at row 1. The backstop used to
keep the highest row number reached and failed every honest enforced run larger
than one batch. It now counts the rows that passed the per-value check. An
honest 30,000-row SQLite -> MongoDB run (and a multi-batch run into Redis)
succeeds; a write path that skips the check for some batches still fails.
Skipped when the service is not reachable.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

from tests.test_reversibility_review3 import _UP, _exec, _run, fake  # noqa: F401

N = 25_000


def _src(tmp_path):
    src = tmp_path / "s.sqlite"
    _exec(src, "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT)")
    _exec(src, "INSERT INTO people VALUES (?, ?)", [(i, f"U{i}@EXAMPLE.COM") for i in range(1, N + 1)])
    return src


def _request(src, dst):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="people"),
        destination=dst, sync_mode="full_refresh_overwrite",
        stream_contracts=[{"name": "people", "primary_key": ["id"], "selected": True}],
        validation_mode="strict", skip_preflight=False, require_reversibility=True,
        compliance_acknowledged=True, acknowledgment_actor="owner@example.com",
        acknowledgment_reason="reviewed run of the reversibility gate", mappings=[dict(m) for m in _UP])


@pytest.fixture()
def mongo_dest():
    pymongo = pytest.importorskip("pymongo")
    uri = os.environ.get("DATAFLOW_TEST_MONGO_URI") or os.environ.get("MONGO_URI")
    if not uri:
        pytest.skip("no MongoDB URI configured")
    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000)
    try:
        client.admin.command("ping")
    except Exception as exc:  # noqa: BLE001 - not reachable: skipped
        pytest.skip(f"MongoDB not reachable: {type(exc).__name__}")
    from src.transfer.models import EndpointConfig

    db = f"r5bs_{uuid.uuid4().hex[:8]}"
    yield EndpointConfig(kind="database", format="mongodb", connection_string=uri, database=db,
                         collection="people", table="people"), client[db]
    client.drop_database(db)
    client.close()


def _bypass_after_first_batch(monkeypatch, module):
    """A write path that maps every batch after the first without the per-value check (stamps stripped)."""
    from services.reversibility_enforce import STAMP

    real = module.build_mapped_rows_with_details
    calls = {"n": 0}

    def bypass(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] > 1 and kwargs.get("mappings"):
            kwargs["mappings"] = [{k: v for k, v in m.items() if k != STAMP} if isinstance(m, dict) else m
                                  for m in kwargs["mappings"]]
        return real(*args, **kwargs)

    monkeypatch.setattr(module, "build_mapped_rows_with_details", bypass)
    return calls


def test_h2_an_honest_enforced_run_past_one_batch_into_mongodb_succeeds(tmp_path, fake, mongo_dest):  # noqa: F811
    dst, coll = mongo_dest
    res = _run(fake, _request(_src(tmp_path), dst))
    enforcement = (res.destination_summary or {}).get("reversibility_enforcement") or {}
    assert res.success is True, (res.error, enforcement)
    assert enforcement.get("enforced") is True, enforcement
    email = next(c for c in enforcement["columns"] if c["column"] == "email")
    assert email["rows_checked"] == N and email["rows_written"] == N, email
    assert coll["people"].count_documents({}) == N


def test_h2_a_mongodb_write_path_that_skips_the_check_still_fails(tmp_path, fake, mongo_dest,  # noqa: F811
                                                                    monkeypatch):
    import connectors.mongodb_writer as mongodb_writer

    dst, _coll = mongo_dest
    calls = _bypass_after_first_batch(monkeypatch, mongodb_writer)
    res = _run(fake, _request(_src(tmp_path), dst))
    enforcement = (res.destination_summary or {}).get("reversibility_enforcement") or {}
    assert calls["n"] > 1, "the run must take more than one batch"
    assert res.success is False, enforcement
    assert "email was checked on only" in str(res.error) and f"of {N:,} written row(s)" in str(res.error), res.error
    assert enforcement.get("enforced") is not True
    assert fake.jobs[res.job_id].get("status") == "failed"


def test_h2_an_honest_enforced_run_past_one_batch_into_redis_succeeds(tmp_path, fake):  # noqa: F811
    try:
        with socket.create_connection(("localhost", 6379), timeout=1):
            pass
    except OSError:
        pytest.skip("Redis not reachable on localhost:6379")
    redis = pytest.importorskip("redis")
    from src.transfer.models import EndpointConfig

    prefix = f"r5bs_{uuid.uuid4().hex[:8]}"
    dst = EndpointConfig(kind="database", format="redis", host="localhost", port=6379, database="0", table=prefix)
    client = redis.Redis(host="localhost", port=6379, db=0, socket_timeout=5)
    try:
        res = _run(fake, _request(_src(tmp_path), dst))
        enforcement = (res.destination_summary or {}).get("reversibility_enforcement") or {}
        assert res.success is True, (res.error, enforcement)
        assert enforcement.get("enforced") is True, enforcement
        email = next(c for c in enforcement["columns"] if c["column"] == "email")
        assert email["rows_checked"] == N and email["rows_written"] == N, email
    finally:
        keys = list(client.scan_iter(match=f"{prefix}*", count=5000))
        for start in range(0, len(keys), 5000):
            client.delete(*keys[start:start + 5000])
        client.close()
