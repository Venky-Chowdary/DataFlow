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
