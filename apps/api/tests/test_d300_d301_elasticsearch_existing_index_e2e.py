"""D300 / D301: PostgreSQL -> Elasticsearch through Validate and the run.

D300 (QA, Nov 9): after PG -> new ES index overwrite (30 docs), an upsert keyed
on ``order_id`` and an append into that index were refused at preflight:
``g3_schema_contract: long_txt: Empty value into existing table with unknown
physical DDL`` and ``g6_target_ddl: Could not load destination schema``. Only
overwrite worked, and it drops the index.

An existing index's schema is its mapping (``GET /<index>/_mapping``,
https://www.elastic.co/guide/en/elasticsearch/reference/8.15/indices-get-mapping.html):
the preflight must read it as the destination schema, the upsert must update
the documents whose ``_id`` the key derives and leave the rest untouched, and
the append must add documents.

D301 (QA, Nov 9): a source with no primary key and no ``upsert_key`` into a new
index passed preflight (``upsert_key ''``), then the job failed "10 row(s) lack
document identity" and left an empty index behind. Validate must refuse
before Confirm and create nothing; an id is never invented.

No Elasticsearch server here: the shared in-memory client fake of
``tests/_r4_temporal_harness`` stands in for the cluster (mapping, documents,
bulk), and DataFlow's real planner, preflight, writer and verification run.
"""

from __future__ import annotations

import time
import uuid

import pytest

from tests._r4_temporal_harness import FakeElasticsearch
from tests._r4_temporal_harness import fake_es as _harness_es  # noqa: F401 - fixture


def _bulk_with_create_conflicts(client, actions, raise_on_error=False, **_):
    """``helpers.bulk`` as the cluster answers it, ``op_type`` included.

    ``create`` on an ``_id`` that exists is a per-item 409
    ``version_conflict_engine_exception`` and changes nothing
    (https://www.elastic.co/guide/en/elasticsearch/reference/8.15/docs-bulk.html);
    ``index`` replaces and ``update`` + ``doc_as_upsert`` merges.
    """
    import json

    from services.value_serializer import json_default

    written, errors = 0, []
    for action in actions:
        index = action["_index"]
        docs = FakeElasticsearch.store.setdefault(index, {"props": {}, "docs": {}})["docs"]
        doc_id = str(action["_id"])
        op = action.get("_op_type") or "index"
        if op == "create" and doc_id in docs:
            errors.append({"create": {
                "_index": index, "_id": doc_id, "status": 409,
                "error": {"type": "version_conflict_engine_exception",
                          "reason": f"[{doc_id}]: version conflict, document already exists"},
            }})
            continue
        if op == "update":
            patch = json.loads(json.dumps(action["doc"], default=json_default))
            docs[doc_id] = {**docs.get(doc_id, {}), **patch}
        else:
            docs[doc_id] = json.loads(json.dumps(action["_source"], default=json_default))
        written += 1
    return written, errors


@pytest.fixture
def fake_es(_harness_es, monkeypatch):
    import elasticsearch.helpers as helpers

    monkeypatch.setattr(helpers, "bulk", _bulk_with_create_conflicts)
    return _harness_es


@pytest.fixture
def pg():
    psycopg2 = pytest.importorskip("psycopg2")
    from conftest import pg_test_cfg

    cfg = pg_test_cfg()
    try:
        db = psycopg2.connect(
            host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
            user=cfg["username"], password=cfg["password"], connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    db.autocommit = True
    yield db, cfg
    db.close()


def pg_exec(db, sql: str, params=None) -> None:
    with db.cursor() as cur:
        cur.execute(sql, params)


def tag(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def connectors(pg):
    from services.connector_store import create_connector, delete_connector

    made: list[str] = []

    def make(kind: str, **extra) -> str:
        base = {"name": tag(f"d300-{kind}"), "type": kind}
        if kind == "postgresql":
            base.update(schema="public", **pg[1])
        else:
            base.update(host="127.0.0.1", port=9200, username="", password="")
        base.update(extra)
        cid = str(create_connector(base).id)
        made.append(cid)
        return cid

    yield make
    for cid in made:
        delete_connector(cid)


def stage(**kwargs):
    from src.ai.copilot.tools import get_pilot_tools

    return get_pilot_tools().execute("start_transfer", {k: v for k, v in kwargs.items() if v is not None})


def staged_summary(staged) -> dict:
    out = staged.output or {}
    pre = out.get("preflight") or (out.get("plan") or {}).get("preflight") or {}
    return {"error": staged.error, "blockers": pre.get("blockers")}


def confirm(staged) -> dict:
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import get_pilot_tools

    assert staged.success, staged_summary(staged)
    job_id = spend_pilot_ack(staged.output["ack_id"], "d300-test")["job_id"]
    deadline = time.time() + 180
    job: dict = {}
    while time.time() < deadline:
        job = dict(get_pilot_tools().execute("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(0.5)
    return job


def _seed(db, table: str, *, keyed: bool = True) -> None:
    key = "order_id int primary key" if keyed else "order_id int"
    pg_exec(db, f'CREATE TABLE public."{table}" ({key}, long_txt text, amount numeric(10,2))')
    for i in range(1, 31):
        pg_exec(
            db, f'INSERT INTO public."{table}" VALUES (%s, %s, %s)',
            (1000 + i, "" if i % 5 == 0 else f"text {i}", f"{i}.50"),
        )


def _gates(staged) -> dict[str, str]:
    out = staged.output or {}
    pre = out.get("preflight") or (out.get("plan") or {}).get("preflight") or {}
    return {g.get("id"): g.get("status") for g in pre.get("gates") or []}


def _docs(store: dict, index: str) -> dict[str, dict]:
    return dict((store.get(index) or {}).get("docs") or {})


def test_d300_upsert_and_append_into_an_index_datawrap_created(pg, connectors, fake_es):
    db = pg[0]
    src, index = tag("d300s"), tag("d300i")
    _seed(db, src)
    try:
        common = dict(
            source_connector_id=connectors("postgresql"), source_table=src,
            dest_connector_id=connectors("elasticsearch", database=index),
            dest_table=index, validation_mode="balanced",
        )
        first = confirm(stage(**common, sync_mode="full_refresh_overwrite"))
        assert first.get("status") == "completed", first.get("error")
        assert len(_docs(fake_es, index)) == 30
        before = _docs(fake_es, index)

        pg_exec(db, f'UPDATE public."{src}" SET long_txt = %s WHERE order_id = 1002', ("changed",))
        pg_exec(db, f'UPDATE public."{src}" SET amount = 99.25 WHERE order_id = 1005')
        staged = stage(**common, sync_mode="upsert", upsert_key="order_id")
        assert staged.success, staged_summary(staged)
        gates = _gates(staged)
        assert gates.get("g3_schema_contract") in {"pass", "warn"}, gates
        assert gates.get("g6_target_ddl") in {"pass", "warn"}, gates
        upserted = confirm(staged)
        assert str(upserted.get("status")).startswith("completed"), upserted.get("error")
        after = _docs(fake_es, index)
        assert set(after) == set(before)  # same _ids: updated in place, none added
        changed = {k for k in after if after[k] != before[k]}
        assert changed == {k for k, d in after.items() if d.get("order_id") in (1002, 1005)}
        by_key = {d["order_id"]: d for d in after.values()}
        assert by_key[1002]["long_txt"] == "changed"
        assert by_key[1010]["long_txt"] == ""  # '' stays a value in an existing index

        for i in (31, 32):
            pg_exec(
                db, f'INSERT INTO public."{src}" VALUES (%s, %s, %s)', (1000 + i, "", "1.00"),
            )
        pg_exec(db, f'DELETE FROM public."{src}" WHERE order_id <= 1030')
        staged = stage(**common, sync_mode="full_refresh_append")
        assert staged.success, staged_summary(staged)
        gates = _gates(staged)
        assert gates.get("g3_schema_contract") in {"pass", "warn"}, gates
        assert gates.get("g6_target_ddl") in {"pass", "warn"}, gates
        appended = confirm(staged)
        assert str(appended.get("status")).startswith("completed"), appended.get("error")
        final = _docs(fake_es, index)
        assert len(final) == 32
        assert {d["order_id"] for d in final.values()} == set(range(1001, 1033))

        # Append is INSERT-only (R6): re-sending a key the index holds must not
        # overwrite that document; it fails loudly or quarantines the row.
        pg_exec(db, f'UPDATE public."{src}" SET long_txt = %s WHERE order_id = 1031', ("again",))
        staged = stage(**common, sync_mode="full_refresh_append")
        assert staged.success, staged_summary(staged)
        collided = confirm(staged)
        assert collided.get("status") != "completed", collided
        assert _docs(fake_es, index) == final
    finally:
        pg_exec(db, f'DROP TABLE IF EXISTS public."{src}"')


def test_d301_keyless_source_into_new_index_is_refused_before_confirm(pg, connectors, fake_es):
    db = pg[0]
    src, index = tag("d301s"), tag("d301i")
    _seed(db, src, keyed=False)
    try:
        staged = stage(
            source_connector_id=connectors("postgresql"), source_table=src,
            dest_connector_id=connectors("elasticsearch", database=index),
            dest_table=index, sync_mode="full_refresh_overwrite", validation_mode="strict",
        )
        assert not staged.success, staged_summary(staged)
        assert "identity" in str(staged.error).lower() or "key" in str(staged.error).lower()
        assert index not in fake_es  # nothing created
    finally:
        pg_exec(db, f'DROP TABLE IF EXISTS public."{src}"')
