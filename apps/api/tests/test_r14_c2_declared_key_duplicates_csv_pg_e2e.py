"""QA C2 — a declared primary key is enforced on a CSV upload into PostgreSQL.

``bad.csv`` declared ``sku`` as its key and repeated ``SKU-00007``. In lenient
mode the duplicate-key finding was downgraded to "bad rows will quarantine",
but no writer holds a repeated key out, so both copies landed. The quarantine
replay then merged on ``sku`` and PostgreSQL could not build the unique index
over the two rows: HTTP 500 ``could not create unique index``.

Now: a repeated declared key refuses the run in every validation mode (the
duplicate policy decides, not leniency) and names the key values and routes;
a keyed replay over a table that already repeats its key is a typed 409 that
names the keys and the dedupe route; a clean replay and a second run work.
"""

from __future__ import annotations

import json
import uuid

import pytest

PG = {"host": "127.0.0.1", "port": 5432, "user": "dataflow", "password": "dataflow", "dbname": "dataflow"}


def _pg():
    import psycopg2

    try:
        conn = psycopg2.connect(**PG)
    except Exception as exc:  # pragma: no cover - environment
        pytest.fail(f"PostgreSQL is required for the C2 e2e: {exc}")
    conn.autocommit = True
    return conn


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "test-pii-key")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "")
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: False)

    import services.mongodb_service as mongo_mod
    from fastapi.testclient import TestClient
    from services.mongodb_service import MemoryMongoDBService
    from src.main import app

    memory = MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongo_mod, "_mongodb_service", memory)
    with TestClient(app) as c:
        c.memory = memory
        yield c


def _csv(*, duplicate: bool, bad: bool = True, unicode_name: str = "Ünïcode ✓") -> bytes:
    lines = ["sku,name,price"]
    for i in range(1, 21):
        name = {3: "", 4: " ", 5: unicode_name}.get(i, f"item {i}")
        lines.append(f"SKU-{i:05d},{name},{i}.50")
    if duplicate:
        lines.append("SKU-00007,dup item,9.99")
    lines.append("SKU-00030,bad price,abc" if bad else "SKU-00030,bad price,30.50")
    return ("\n".join(lines) + "\n").encode("utf-8")


def _upload(client, table: str, content: bytes, mode: str = "lenient"):
    form = {
        "destination_database": PG["dbname"],
        "destination_collection": table,
        "dest_type": "postgresql",
        "dest_host": PG["host"],
        "dest_port": str(PG["port"]),
        "dest_schema": "public",
        "dest_username": PG["user"],
        "dest_password": PG["password"],
        "async_mode": "false",
        "sync_mode": "full_refresh_overwrite",
        "validation_mode": mode,
        "schema_policy": "propagate_columns",
        "stream_contracts_json": json.dumps(
            [{"name": table, "sync_mode": "full_refresh_overwrite", "primary_key": "sku", "selected": True}]
        ),
    }
    return client.post(
        "/api/v1/connectors/transfer",
        data=form,
        files={"file": ("bad.csv", content, "text/csv")},
    )


def _job_id(resp) -> str:
    body = resp.json()
    if isinstance(body.get("detail"), dict):
        return str(body["detail"].get("job_id") or "")
    return str(body.get("job_id") or "")


def _dupes(cur, table: str) -> list:
    cur.execute(f'SELECT sku, count(*) FROM public."{table}" GROUP BY sku HAVING count(*) > 1')  # nosec B608
    return cur.fetchall()


def _table_exists(cur, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f'public."{table}"',))
    return cur.fetchone()[0] is not None


@pytest.mark.parametrize("mode", ["lenient", "strict"])
def test_repeated_declared_key_refuses_and_writes_nothing(client, mode):
    table = f"r14c2_{mode[:3]}_{uuid.uuid4().hex[:6]}"
    conn = _pg()
    cur = conn.cursor()
    try:
        resp = _upload(client, table, _csv(duplicate=True), mode)
        assert resp.status_code == 422, resp.text[:800]
        text = json.dumps(resp.json(), ensure_ascii=False)
        assert "SKU-00007×2" in text, text
        assert "in_file_dedupe=keep_last" in text and "duplicate_policy=upsert" in text, text
        job = client.memory.get_job(_job_id(resp)) or {}
        assert job.get("status") == "failed", job.get("status")
        # Never written twice: the run wrote nothing at all.
        assert not _table_exists(cur, table) or not _dupes(cur, table)
    finally:
        cur.execute(f'DROP TABLE IF EXISTS public."{table}"')  # nosec B608
        conn.close()


def test_replay_over_a_table_that_repeats_its_key_is_a_typed_409_then_works(client):
    table = f"r14c2_rp_{uuid.uuid4().hex[:6]}"
    conn = _pg()
    cur = conn.cursor()
    try:
        first = _upload(client, table, _csv(duplicate=False))
        assert first.status_code == 200, first.text[:800]
        job_id = _job_id(first)
        job = client.memory.get_job(job_id) or {}
        assert job.get("status") == "completed_with_quarantine", job.get("status")
        cur.execute(f'SELECT count(*) FROM public."{table}"')  # nosec B608
        assert cur.fetchone()[0] == 20
        cur.execute(f'SELECT name FROM public."{table}" WHERE sku = %s', ("SKU-00005",))  # nosec B608
        assert cur.fetchone()[0] == "Ünïcode ✓"
        cur.execute(f'SELECT name FROM public."{table}" WHERE sku IN (%s, %s) ORDER BY sku', ("SKU-00003", "SKU-00004"))  # nosec B608
        assert [r[0] for r in cur.fetchall()] in ([None, " "], ["", " "])

        # The table now repeats a key (an earlier build, or another writer).
        cur.execute(
            f'INSERT INTO public."{table}" (sku, name, price) VALUES (%s, %s, %s)',  # nosec B608
            ("SKU-00007", "dup item", "9.99"),
        )
        edit = [{"sku": "SKU-00030", "name": "bad price", "price": "30.50"}]
        refused = client.post(f"/api/v1/connectors/jobs/{job_id}/quarantine/replay", json={"rows": edit})
        assert refused.status_code == 409, refused.text[:800]
        detail = refused.json()["detail"]
        assert detail["code"] == "REPLAY_DESTINATION_DUPLICATE_KEYS", detail
        assert detail["key_columns"] == ["sku"]
        assert "SKU-00007×2" in detail["duplicate_keys"], detail
        assert "in_file_dedupe=keep_last" in detail["next_action"], detail
        assert _dupes(cur, table) == [("SKU-00007", 2)]  # nothing was changed

        # Dedupe the destination, then the same replay lands the corrected row.
        cur.execute(
            f'DELETE FROM public."{table}" WHERE sku = %s AND name = %s',  # nosec B608
            ("SKU-00007", "dup item"),
        )
        replay = client.post(f"/api/v1/connectors/jobs/{job_id}/quarantine/replay", json={"rows": edit})
        assert replay.status_code == 200, replay.text[:800]
        assert replay.json()["rows_written"] == 1, replay.json()
        cur.execute(f'SELECT price FROM public."{table}" WHERE sku = %s', ("SKU-00030",))  # nosec B608
        assert str(cur.fetchone()[0]).startswith("30.5")

        # Second run of the corrected file into the table DataFlow created.
        second = _upload(client, table, _csv(duplicate=False, bad=False))
        assert second.status_code == 200, second.text[:800]
        assert not _dupes(cur, table)
        cur.execute(f'SELECT count(*) FROM public."{table}"')  # nosec B608
        assert cur.fetchone()[0] == 21
        # ...and the same file with a repeated key is still refused on run 2.
        again = _upload(client, table, _csv(duplicate=True, bad=False))
        assert again.status_code == 422, again.text[:800]
        assert "SKU-00007×2" in json.dumps(again.json(), ensure_ascii=False)
        assert not _dupes(cur, table)
    finally:
        cur.execute(f'DROP TABLE IF EXISTS public."{table}"')  # nosec B608
        conn.close()


def test_engine_unique_index_failure_maps_to_the_typed_refusal():
    from services.quarantine_dlq import replay_unique_index_failure

    exc = RuntimeError(
        'could not create unique index "uidx_t_sku"\nDETAIL:  Key (sku)=(SKU-00007) is duplicated.\n'
    )
    refusal = replay_unique_index_failure(exc, table="t", key_columns=["sku"])
    assert refusal and refusal["code"] == "REPLAY_DESTINATION_DUPLICATE_KEYS"
    assert refusal["duplicate_keys"] == ["SKU-00007×2"]
    mysql = RuntimeError("(1062, \"Duplicate entry 'SKU-00007' for key 'uidx_t_sku'\")")
    assert replay_unique_index_failure(mysql, table="t", key_columns=["sku"])["duplicate_keys"] == ["SKU-00007×2"]
    # A row colliding on insert is not an index build — not this refusal.
    assert replay_unique_index_failure(
        RuntimeError("duplicate key value violates unique constraint"), table="t", key_columns=["sku"]
    ) is None
