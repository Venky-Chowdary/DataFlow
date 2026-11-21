"""PostgreSQL→PostgreSQL: a table with a ``jsonb`` column takes the binary COPY.

The mapper stamps a ``json`` cast on JSON columns, and that cast alone sent
every such table down the row path (about 200 rows/s on a mixed table). For
``jsonb`` → ``jsonb`` the binary COPY carries the datum exactly; ``json``
columns keep their text verbatim and stay on the row path.
"""

from __future__ import annotations

import os
import uuid

import pytest

from src.transfer.copy_route import _jsonb_carry_mappings


def test_json_cast_on_jsonb_is_a_plain_carry_and_nothing_else_changes():
    mappings = [
        {"source": "id", "target": "id", "transform": "integer"},
        {"source": "doc", "target": "doc", "transform": "json"},
        {"source": "raw", "target": "raw", "transform": "json"},
        {"source": "name", "target": "name", "transform": "upper"},
    ]
    declared = {"id": "bigint", "doc": "jsonb", "raw": "json", "name": "text"}
    out = _jsonb_carry_mappings(mappings, declared)
    assert [m.get("transform") for m in out] == ["integer", "", "json", "upper"]
    assert mappings[1]["transform"] == "json"  # the caller's mappings are not mutated


@pytest.mark.parametrize("mode", ["full_refresh_overwrite", "full_refresh_append"])
def test_live_jsonb_table_copies_byte_identical(mode):
    from tests.helpers.live_env import pg_creds, pg_up

    if not pg_up():
        pytest.skip("PostgreSQL not reachable")
    os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
    os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    import psycopg2

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    c = pg_creds()
    sfx = uuid.uuid4().hex[:8]
    src, dst = f"jb_src_{sfx}", f"jb_dst_{sfx}"
    conn = psycopg2.connect(host=c["host"], port=c["port"], user=c["username"],
                            password=c["password"], dbname=c["database"])
    conn.autocommit = True
    docs = [
        '{"a": 1.10, "b": [1, 2.50, "x"], "z": null}',
        '{"n": 12345678901234567890.123456789, "e": 1e400}',
        '{"dup": 1, "dup": 2}',
        '{"ünï": "çødé 🙂", "nested": {"k": {"k": [true, false]}}}',
        '[]', '"text"', '0.0000', 'null',
    ]
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE TABLE "{src}" (id bigint PRIMARY KEY, doc jsonb, note text)')
            for i in range(1, 2001):
                doc = docs[i % len(docs)] if i % 13 else None
                cur.execute(f'INSERT INTO "{src}" VALUES (%s, %s::jsonb, %s)',
                            (i, doc, "" if i % 7 == 0 else f"n{i}"))
        endpoint = dict(host=c["host"], port=c["port"], username=c["username"],
                        password=c["password"], database=c["database"], schema="public")
        req = TransferRequest(
            source=EndpointConfig(kind="database", format="postgresql", table=src, **endpoint),
            destination=EndpointConfig(kind="database", format="postgresql", table=dst, **endpoint),
            sync_mode=mode,
            stream_contracts=[{"name": src, "sync_mode": mode, "primary_key": ["id"],
                               "cursor_field": "id", "selected": True}],
            skip_preflight=True,
            validation_mode="strict",
        )
        res = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
        assert res.success, res.error
        summary = res.destination_summary or {}
        assert "copy_binary" in str(summary.get("load_methods") or summary.get("load_method")), summary
        assert (res.reconciliation or {}).get("passed") is True
        with conn.cursor() as cur:
            cur.execute(f'SELECT id, doc::text, note FROM "{src}" ORDER BY id')
            want = cur.fetchall()
            cur.execute(f'SELECT id, doc::text, note FROM "{dst}" ORDER BY id')
            got = cur.fetchall()
        assert got == want
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}"')
            cur.execute(f'DROP TABLE IF EXISTS "{dst}"')
        conn.close()


_DOCS = ['{"a": 1.10, "b": [1, 2]}', '{"z": null, "k": [true, 2.50]}', "[]", '"s"']


def _pg_conn():
    import psycopg2

    from tests.helpers.live_env import pg_creds

    c = pg_creds()
    conn = psycopg2.connect(host=c["host"], port=c["port"], user=c["username"],
                            password=c["password"], dbname=c["database"])
    conn.autocommit = True
    return conn, c


def _run(c, src, dst, mode):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    endpoint = dict(host=c["host"], port=c["port"], username=c["username"],
                    password=c["password"], database=c["database"], schema="public")
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", table=src, **endpoint),
        destination=EndpointConfig(kind="database", format="postgresql", table=dst, **endpoint),
        sync_mode=mode,
        stream_contracts=[{"name": src, "sync_mode": mode, "primary_key": ["id"],
                           "cursor_field": "id", "selected": True}],
        skip_preflight=True,
        validation_mode="strict",
    )
    res = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
    assert res.success, res.error
    summary = res.destination_summary or {}
    return str(summary.get("load_methods") or summary.get("load_method"))


@pytest.mark.parametrize("dest_type", ["text", "json", "jsonb"])
@pytest.mark.parametrize("mode", ["incremental_deduped", "upsert", "full_refresh_append"])
def test_existing_destination_column_stores_what_the_row_path_stores(mode, dest_type, monkeypatch):
    """Into an existing TEXT / JSON column the json cast decides the stored text, so
    the default route must store exactly what the row path stores; into JSONB it
    may take the binary COPY (same datum)."""
    from tests.helpers.live_env import pg_up

    if not pg_up():
        pytest.skip("PostgreSQL not reachable")
    os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
    os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    conn, c = _pg_conn()
    sfx = uuid.uuid4().hex[:8]
    src, dst_default, dst_row = f"jbx_s_{sfx}", f"jbx_d_{sfx}", f"jbx_r_{sfx}"
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE TABLE "{src}" (id bigint PRIMARY KEY, doc jsonb)')
            for i in range(1, 41):
                cur.execute(f'INSERT INTO "{src}" VALUES (%s, %s::jsonb)',
                            (i, _DOCS[i % len(_DOCS)] if i % 9 else None))
            for dst in (dst_default, dst_row):
                cur.execute(f'CREATE TABLE "{dst}" (id bigint PRIMARY KEY, doc {dest_type})')
        method = _run(c, src, dst_default, mode)
        with monkeypatch.context() as m:
            m.setenv("DATAWRAP_PG_PG_COPY", "0")
            row_method = _run(c, src, dst_row, mode)
        assert "copy" not in row_method, row_method
        with conn.cursor() as cur:
            cur.execute(f'SELECT id, doc::text FROM "{dst_default}" ORDER BY id')
            got = cur.fetchall()
            cur.execute(f'SELECT id, doc::text FROM "{dst_row}" ORDER BY id')
            want = cur.fetchall()
        assert len(want) == 40
        assert got == want, (method, got[:3], want[:3])
        if dest_type != "jsonb":
            assert "copy" not in method, method
    finally:
        with conn.cursor() as cur:
            for t in (src, dst_default, dst_row):
                cur.execute(f'DROP TABLE IF EXISTS "{t}"')
        conn.close()
