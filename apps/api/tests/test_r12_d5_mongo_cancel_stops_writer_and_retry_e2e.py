"""QA r12 MongoDB D5: cancel stops the Mongo writer; retry sees committed rows.

QA (production 8f7d246d): a 100k-row PostgreSQL -> MongoDB job was cancelled
while writing. The job said cancelled, yet the collection kept growing from
6,000 to 20,000 documents (writer_ack=20000, rows_written=null): the Mongo
writer loops over 1,000-document ``insert_many`` batches inside one engine
chunk (20,000 rows) and never polled the cancel flag, which the SQL writers
poll between batches (D102 / D168 ``cancel_checkpoint``). Retry then said
"Zero committed rows" and failed with a DECIMAL(6,4) overflow: it typed the
replay from the partial documents (all small amounts) instead of the source.

Live: PostgreSQL (``PG*`` env) and MongoDB on 127.0.0.1:27017. The cancel is
the real REST cancel, timed between two writer batches so the assertion
"stops within one batch" is exact.
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from decimal import Decimal

import pytest

from tests import r4_route_harness as h

pymongo = pytest.importorskip("pymongo")

pytestmark = pytest.mark.timeout(900)

# 9,000 rows: one engine chunk, three 1,000-document batches before the cancel,
# and the DECIMAL boundary at row 4,000 inside the uncommitted remainder.
ROWS = 9_000
CANCEL_AFTER_BATCHES = 3
# Two engine chunks (20,000-row chunk): a cancel in the second chunk tests the
# partial checkpoint against an already-applied chunk's keyset bookmark.
ROWS_TWO_CHUNKS = 25_000
CANCEL_AFTER_BATCHES_TWO_CHUNKS = 23


def _name(i: int):
    return {1: None, 2: "", 3: "   ", 4: "Zoë 東京 ✓", 5: "O'Brien \"q\""}.get(i % 50, f"n{i}")


def _amount(i: int) -> Decimal:
    # The first rows fit DECIMAL(6,4); later ones need DECIMAL(12,4). Typing
    # the retry from a partial collection (first rows only) overflows.
    if i <= 4_000:
        return Decimal(f"{i % 100}.{i % 10000:04d}")
    return Decimal(f"{i * 7}.{(i * 13) % 10000:04d}")


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _make_route(rows: int, *, with_tags: bool = False):
    if not h.port_up(27017):
        pytest.skip("MongoDB not reachable on :27017")
    h.pg_conn().close()
    from services.connector_store import create_connector

    token = uuid.uuid4().hex[:8]
    table = f"r12d5_src_{token}"
    # ``tags`` exists only for the unnest recipe leg: a JSONB column into a
    # Mongo ARRAY is a fidelity question of its own that the plain legs must
    # not carry.
    tags_col = ", tags JSONB" if with_tags else ""
    h.execute(
        "postgresql",
        f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT, amount NUMERIC(12,4)"
        f"{tags_col})",
    )
    conn = h.pg_conn()
    try:
        with conn.cursor() as cur:
            from psycopg2.extras import execute_values

            if with_tags:
                execute_values(
                    cur,
                    f"INSERT INTO {table} (id, name, amount, tags) VALUES %s",  # nosec B608
                    [(i, _name(i), _amount(i), '["a", "b"]') for i in range(1, rows + 1)],
                    page_size=5000,
                )
            else:
                execute_values(
                    cur,
                    f"INSERT INTO {table} (id, name, amount) VALUES %s",  # nosec B608
                    [(i, _name(i), _amount(i)) for i in range(1, rows + 1)],
                    page_size=5000,
                )
        conn.commit()
    finally:
        conn.close()
    src = create_connector({"name": f"pg-{token}", "type": "postgresql", "schema": "public",
                            **h.dest_config("postgresql")})
    mc = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
    coll = mc["dataflow_test"][f"r12d5_dst_{token}"]
    return {"src": src.id, "table": table, "coll": coll, "rows": rows, "_mc": mc}


def _drop_route(env: dict) -> None:
    env["coll"].drop()
    env["_mc"].close()
    h.drop_table("postgresql", env["table"])


@pytest.fixture
def route():
    env = _make_route(ROWS)
    try:
        yield env
    finally:
        _drop_route(env)


@pytest.fixture
def route_tags():
    env = _make_route(ROWS, with_tags=True)
    try:
        yield env
    finally:
        _drop_route(env)


@pytest.fixture
def route_two_chunks():
    env = _make_route(ROWS_TWO_CHUNKS)
    try:
        yield env
    finally:
        _drop_route(env)


def _form(env: dict, sync_mode: str, recipe: dict | None = None) -> dict:
    form = {
        "source_kind": "database", "source_format": "postgresql",
        "source_connector_id": env["src"], "source_table": env["table"],
        "source_database": h.PG["database"],
        "dest_kind": "database", "dest_format": "mongodb",
        "dest_connection_string": "mongodb://127.0.0.1:27017",
        "dest_database": "dataflow_test",
        "dest_table": env["coll"].name, "dest_collection": env["coll"].name,
        "async_mode": "true", "sync_mode": sync_mode,
    }
    if sync_mode == "incremental_upsert":
        form["stream_contracts_json"] = ""
    if recipe is not None:
        form["shape_recipe_json"] = json.dumps(recipe)
    return form


_TERMINAL = {"completed", "completed_with_quarantine", "failed", "partial", "cancelled"}


def _wait(job_id: str, timeout_s: float = 600.0) -> dict:
    from services.mongodb_service import get_mongodb_service

    deadline = time.monotonic() + timeout_s
    job: dict = {}
    while time.monotonic() < deadline:
        job = get_mongodb_service().get_job(job_id) or {}
        status = str(job.get("status") or "")
        # The cancel route flips the status at once; the worker records what
        # it committed (cancel_outcome) when it stops. Read the job after that
        # final write — never by joining threads: ``df-transfer-*`` are the
        # scheduler pool's long-lived workers.
        if status in _TERMINAL and (status != "cancelled" or job.get("cancel_outcome")):
            return job
        time.sleep(0.25)
    return job


def _cancel_between_batches(monkeypatch, client, coll, after: int = CANCEL_AFTER_BATCHES):
    """Cancel through REST right after the Nth insert_many of the first run."""
    import connectors.mongodb_writer as writer
    from services.mongodb_service import get_mongodb_service

    real = writer._idempotent_insert_many
    state = {"calls": 0, "at_cancel": None, "job_id": None, "armed": True}
    lock = threading.Lock()

    def wrapped(target, docs):
        if not state["armed"]:
            state.setdefault("after", []).append(len(docs))
        n = real(target, docs)
        with lock:
            state["calls"] += 1
            fire = state["armed"] and state["calls"] == after
            if fire:
                state["armed"] = False
        if fire:
            store = get_mongodb_service()
            running = [jid for jid, j in store._jobs.items() if j.get("status") == "running"]
            assert len(running) == 1, running
            state["job_id"] = running[0]
            state["at_cancel"] = coll.count_documents({})
            resp = client.post(f"/api/v1/connectors/jobs/{running[0]}/cancel")
            assert resp.status_code == 200, resp.text
        return n

    monkeypatch.setattr(writer, "_idempotent_insert_many", wrapped)
    return state


def _assert_exact(coll, rows: int = ROWS) -> None:
    # Documents, not distinct ids: a resumed prefix re-written is a duplicate.
    assert coll.count_documents({}) == rows, coll.count_documents({})
    got = {d["id"]: (d.get("name", "<missing>"), d.get("amount")) for d in coll.find({}, {"_id": 0})}
    assert len(got) == rows, len(got)
    bad = []
    for i in range(1, rows + 1):
        name, amount = got[i]
        amount = amount.to_decimal() if hasattr(amount, "to_decimal") else Decimal(str(amount))
        if name != _name(i) or amount != _amount(i):
            bad.append((i, got[i], (_name(i), _amount(i))))
    assert not bad, bad[:5]


@pytest.mark.parametrize("sync_mode", ["full_refresh_overwrite", "full_refresh_append"])
def test_cancel_stops_mongo_writer_and_retry_completes_exact(client, route, monkeypatch, sync_mode):
    coll = route["coll"]
    state = _cancel_between_batches(monkeypatch, client, coll)
    resp = client.post("/api/v1/transfer/run", data=_form(route, sync_mode))
    assert resp.status_code == 200, resp.text
    job_id = resp.json()["job_id"]
    job = _wait(job_id)
    assert state["job_id"] == job_id
    assert job["status"] == "cancelled", (job.get("status"), job.get("error"))

    landed = coll.count_documents({})
    # Stopped within one batch of the cancel (1,000-document batches); the
    # pre-fix writer ran the chunk to its end.
    assert landed - state["at_cancel"] <= 1000, (state["at_cancel"], landed, state.get("after"))
    assert landed < ROWS, landed
    # Row accounting is the collection's own count.
    assert job.get("records_processed") == landed, (job.get("records_processed"), landed)
    outcome = job.get("cancel_outcome") or {}
    assert outcome.get("rows_committed") == landed, outcome

    resp = client.post(f"/api/v1/connectors/jobs/{job_id}/retry")
    if sync_mode == "full_refresh_append":
        # Append has no key: replaying from zero would duplicate the committed
        # rows, and retry must know they exist (not "Zero committed rows").
        assert resp.status_code == 409, resp.text
        detail = resp.json()["detail"]
        assert detail["rows_committed"] == landed, detail
        assert detail["rows_committed_known"] is True, detail
        # Resume picks up after the committed rows: the cancelled chunk's
        # committed prefix is a durable checkpoint, proven by the collection's
        # own count, so the operator is not left with "No durable checkpoint".
        resp = client.post(f"/api/v1/connectors/jobs/{job_id}/resume")
        assert resp.status_code == 200, resp.text
        resumed = _wait(resp.json().get("job_id") or job_id)
        assert resumed["status"] == "completed", (resumed.get("status"), resumed.get("error"))
    else:
        assert resp.status_code == 200, resp.text
        retried = _wait(resp.json()["job_id"])
        # Typed from the source (NUMERIC(12,4)), not from the partial documents
        # the cancelled run left: no DECIMAL(6,4) overflow, nothing quarantined.
        assert retried["status"] == "completed", (
            retried.get("status"), retried.get("error"),
            (retried.get("rejected_details") or [])[:1],
        )
        assert retried.get("records_processed") == ROWS
    _assert_exact(coll)


def _cancelled_run(client, route, monkeypatch, sync_mode, *, recipe=None, after=CANCEL_AFTER_BATCHES):
    """Start, cancel between batches, and return (job, landed, state)."""
    coll = route["coll"]
    state = _cancel_between_batches(monkeypatch, client, coll, after=after)
    resp = client.post("/api/v1/transfer/run", data=_form(route, sync_mode, recipe))
    assert resp.status_code == 200, resp.text
    job_id = resp.json()["job_id"]
    job = _wait(job_id)
    assert state["job_id"] == job_id
    assert job["status"] == "cancelled", (job.get("status"), job.get("error"))
    landed = coll.count_documents({})
    assert landed - state["at_cancel"] <= 1000, (state["at_cancel"], landed, state.get("after"))
    assert job.get("records_processed") == landed, (job.get("records_processed"), landed)
    return job_id, landed


_SHAPED = {
    # Removes a row: the page the writer sees is shorter than the source page.
    "filter": {"version": 1, "steps": [
        {"op": "filter_rows", "options": {"condition": "[id] <> 2"}},
    ]},
    # Expands rows: two documents per source row.
    "unnest": {"version": 1, "steps": [
        {"op": "unnest_json", "column": "tags", "options": {"to": "tag"}},
    ]},
}


@pytest.mark.parametrize("shape", sorted(_SHAPED))
def test_cancel_with_a_shaping_recipe_leaves_no_partial_checkpoint(client, request, monkeypatch, shape):
    """A recipe that removes or expands rows breaks the written-documents ->
    source-rows mapping, so the cancelled chunk's prefix is not checkpointed:
    retry still knows the committed rows, resume is refused (no skipped source
    rows, no duplicated prefix), and the collection is left as it was."""
    route = request.getfixturevalue("route_tags" if shape == "unnest" else "route")
    job_id, landed = _cancelled_run(
        client, route, monkeypatch, "full_refresh_append", recipe=_SHAPED[shape]
    )
    resp = client.post(f"/api/v1/connectors/jobs/{job_id}/retry")
    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"]["rows_committed"] == landed
    resp = client.post(f"/api/v1/connectors/jobs/{job_id}/resume")
    assert resp.status_code == 409, resp.text
    assert "No durable checkpoint" in json.dumps(resp.json()), resp.text
    assert route["coll"].count_documents({}) == landed


def test_cancel_in_the_second_chunk_keeps_the_prefix_and_refuses_a_keyless_resume(
    client, route_two_chunks, monkeypatch
):
    """Chunk 1 (20,000 rows) is applied and checkpointed; the cancel lands three
    batches into chunk 2. A keyless streaming insert is never resumed past a
    chunk boundary (the engine's resume contract: set a primary key or re-run
    with overwrite), so the committed prefix stays exactly as it is — nothing
    is re-read from chunk 1's bookmark (duplicates) and nothing is skipped —
    and retry still knows the committed count."""
    route = route_two_chunks
    job_id, landed = _cancelled_run(
        client, route, monkeypatch, "full_refresh_append", after=CANCEL_AFTER_BATCHES_TWO_CHUNKS
    )
    assert 23_000 <= landed <= 24_000, landed
    resp = client.post(f"/api/v1/connectors/jobs/{job_id}/retry")
    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"]["rows_committed"] == landed
    resp = client.post(f"/api/v1/connectors/jobs/{job_id}/resume")
    if resp.status_code == 200:
        resumed = _wait(resp.json().get("job_id") or job_id)
        assert resumed["status"] == "failed", (resumed.get("status"), resumed.get("error"))
        assert "Cannot resume a streaming insert without a primary key" in str(resumed.get("error"))
    else:
        assert resp.status_code == 409, resp.text
    coll = route["coll"]
    assert coll.count_documents({}) == landed
    assert len({d["id"] for d in coll.find({}, {"_id": 0, "id": 1})}) == landed
