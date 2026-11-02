from __future__ import annotations

import hashlib
import json
import os
import socket
import uuid

import pytest

_PRECHUNKED_FLAG = "_df_prechunked"

_PG_PORT = int(os.environ.get("DATAFLOW_TEST_PGVECTOR_PORT", "5434"))
_QDRANT_PORT = int(os.environ.get("DATAFLOW_TEST_QDRANT_PORT", "6335"))


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


_BACKENDS = [
    pytest.param(
        "pgvector",
        id="pgvector-5434",
        marks=pytest.mark.skipif(
            not _reachable("localhost", _PG_PORT),
            reason=f"pgvector not reachable on localhost:{_PG_PORT}",
        ),
    ),
    pytest.param(
        "qdrant",
        id="qdrant-6335",
        marks=pytest.mark.skipif(
            not _reachable("localhost", _QDRANT_PORT),
            reason=f"Qdrant not reachable on localhost:{_QDRANT_PORT}",
        ),
    ),
]


@pytest.fixture(params=_BACKENDS)
def vector_destination(request):
    kind = str(request.param)
    name = f"gvec_{kind}_{uuid.uuid4().hex[:10]}"
    if kind == "pgvector":
        cfg = {
            "host": "localhost",
            "port": _PG_PORT,
            "database": "dataflow",
            "username": "dataflow",
            "password": "dataflow",
            "schema": "public",
            "connection_string": "",
            "ssl": False,
        }
    else:
        cfg = {
            "host": "localhost",
            "port": _QDRANT_PORT,
            "database": "",
            "username": "",
            "password": "",
            "schema": "",
            "connection_string": "",
            "ssl": False,
        }
    resource = {"kind": kind, "name": name, "cfg": cfg}
    try:
        yield resource
    finally:
        if kind == "pgvector":
            import psycopg2
            from psycopg2 import sql

            conn = psycopg2.connect(
                host="localhost",
                port=_PG_PORT,
                dbname="dataflow",
                user="dataflow",
                password="dataflow",
            )
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                            sql.Identifier("public"), sql.Identifier(name)
                        )
                    )
                conn.commit()
            finally:
                conn.close()
        else:
            import requests

            requests.delete(
                f"http://localhost:{_QDRANT_PORT}/collections/{name}",
                timeout=10,
            )


def _doc_rows(doc_id: str, count: int, *, tenant: str | None = None) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for index in range(count):
        row = {
            "doc_id": doc_id,
            "content": f"{doc_id} chunk {index} contains deterministic live test text.",
            "chunk_index": str(index),
            _PRECHUNKED_FLAG: "1",
        }
        if tenant is not None:
            row["tenant"] = tenant
        rows.append(row)
    return rows


def _write(resource, rows: list[dict[str, str]], pk_columns: list[str] | None = None):
    from connectors.pgvector_writer import write_mapped_rows as write_pgvector
    from connectors.qdrant_writer import write_mapped_rows as write_qdrant

    kind = resource["kind"]
    cfg = resource["cfg"]
    headers = list(rows[0])
    mappings = [
        {"source": column, "target": column, "target_type": "VARCHAR"}
        for column in headers
    ]
    options = {
        "host": cfg["host"],
        "port": cfg["port"],
        "database": cfg["database"],
        "username": cfg["username"],
        "password": cfg["password"],
        "schema": cfg["schema"],
        "connection_string": cfg["connection_string"],
        "ssl": cfg["ssl"],
        "table_name": resource["name"],
        "headers": headers,
        "data_rows": [[row.get(column, "") for column in headers] for row in rows],
        "mappings": mappings,
        "column_types": {column: "string" for column in headers},
        "destination_column_types": {
            column: "VARCHAR" for column in headers
        },
        "error_policy": "quarantine",
        "content_column": "content",
        "embedding_model": "hash/32",
        "conflict_columns": pk_columns or ["doc_id"],
        "write_mode": "upsert",
        "sync_mode": "cdc",
    }
    result = (write_pgvector if kind == "pgvector" else write_qdrant)(**options)
    assert result.ok, result.error
    return result


def _count_source(resource, source_id: str) -> int:
    if resource["kind"] == "pgvector":
        import psycopg2
        from psycopg2 import sql

        cfg = resource["cfg"]
        conn = psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            dbname=cfg["database"],
            user=cfg["username"],
            password=cfg["password"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SELECT count(*) FROM {}.{} WHERE source_id = %s").format(
                        sql.Identifier("public"), sql.Identifier(resource["name"])
                    ),
                    (source_id,),
                )
                return int(cur.fetchone()[0])
        finally:
            conn.close()
    import requests

    response = requests.post(
        f"http://localhost:{_QDRANT_PORT}/collections/{resource['name']}/points/count",
        json={
            "filter": {
                "must": [
                    {
                        "key": "source_id",
                        "match": {"any": [source_id]},
                    }
                ]
            },
            "exact": True,
        },
        timeout=30,
    )
    assert response.status_code == 200, response.text
    return int(response.json()["result"]["count"])


def _source_id(values: list[str]) -> str:
    return "\x1f".join(str(value) for value in values)


def _cdc_delete(resource, row: dict[str, str], pk_columns: list[str]) -> int:
    from connectors.table_manager import delete_by_primary_keys

    values = [str(row[column]) for column in pk_columns]
    key = "\x1f".join(values) if len(values) > 1 else values[0]
    return delete_by_primary_keys(
        db_type=resource["kind"],
        cfg=resource["cfg"],
        table_name=resource["name"],
        primary_key_column=pk_columns,
        keys=[key],
        schema="public" if resource["kind"] == "pgvector" else None,
    )


def test_live_delete_removes_every_chunk(vector_destination):
    resource = vector_destination
    doc = _doc_rows("live-delete-1", 10)[0]
    _write(resource, _doc_rows("live-delete-1", 10))
    source_id = _source_id(["live-delete-1"])
    assert _count_source(resource, source_id) == 10
    deleted = _cdc_delete(resource, doc, ["doc_id"])
    remaining = _count_source(resource, source_id)
    assert (deleted, remaining) == (10, 0)


def test_live_absent_delete_returns_zero(vector_destination):
    resource = vector_destination
    _write(resource, _doc_rows("present-doc", 1))
    assert _cdc_delete(resource, {"doc_id": "absent"}, ["doc_id"]) == 0
    assert _count_source(resource, _source_id(["present-doc"])) == 1


def test_live_shrinking_document_cleans_stale_chunks_only(vector_destination):
    resource = vector_destination
    _write(
        resource,
        _doc_rows("shrinking-doc", 10) + _doc_rows("unrelated-doc", 3),
    )
    result = _write(resource, _doc_rows("shrinking-doc", 1))
    shrinking_count = _count_source(resource, _source_id(["shrinking-doc"]))
    unrelated_count = _count_source(resource, _source_id(["unrelated-doc"]))
    assert (shrinking_count, unrelated_count) == (1, 3)
    assert result.ok


def test_live_composite_primary_key_delete(vector_destination):
    resource = vector_destination
    rows = _doc_rows("7", 4, tenant="west")
    _write(resource, rows, ["tenant", "doc_id"])
    source_id = _source_id(["west", "7"])
    assert _count_source(resource, source_id) == 4
    deleted = _cdc_delete(resource, rows[0], ["tenant", "doc_id"])
    remaining = _count_source(resource, source_id)
    assert (deleted, remaining) == (4, 0)


def test_live_unverified_delete_raises(vector_destination, monkeypatch):
    from services.vector_sync import VectorDeleteUnverifiedError

    resource = vector_destination
    row = _doc_rows("unverified-doc", 1)[0]
    _write(resource, [row])
    if resource["kind"] == "pgvector":
        with monkeypatch.context() as patcher:
            patcher.setattr(
                "services.vector_sync._pgvector_count_remaining",
                lambda *_args, **_kwargs: 1,
            )
            with pytest.raises(VectorDeleteUnverifiedError, match="1"):
                _cdc_delete(resource, row, ["doc_id"])
        assert _count_source(resource, "unverified-doc") == 1
    else:
        from services.vector_sync import _qdrant_count

        real_count = _qdrant_count
        calls = 0

        def post_count_nonzero(*args, **kwargs):
            nonlocal calls
            calls += 1
            return real_count(*args, **kwargs) if calls == 1 else 1

        with monkeypatch.context() as patcher:
            patcher.setattr("services.vector_sync._qdrant_count", post_count_nonzero)
            with pytest.raises(VectorDeleteUnverifiedError, match="1"):
                _cdc_delete(resource, row, ["doc_id"])


def test_live_rejected_chunk_skips_document_cleanup(vector_destination, monkeypatch):
    import services.vector_embedding as vector_embedding
    import services.vectorization as vectorization
    from services.vector_sync import vector_doc_key

    resource = vector_destination
    _write(resource, _doc_rows("rejected-doc", 3))
    source_id = vector_doc_key(["rejected-doc"])
    real_embed = vectorization.embed
    real_coerce = vector_embedding.coerce_embedding

    def one_bad_chunk(texts, *args, **kwargs):
        kwargs["use_cache"] = False
        vectors = real_embed(texts, *args, **kwargs)
        for index, text in enumerate(texts):
            if "chunk 1 contains deterministic live test text." in text:
                vectors[index] = [999.0] * 32
        return vectors

    def reject_marker(value, **kwargs):
        if isinstance(value, (list, tuple)) and value and value[0] == 999.0:
            return None, "deterministically rejected test chunk"
        return real_coerce(value, **kwargs)

    monkeypatch.setattr(vectorization, "embed", one_bad_chunk)
    monkeypatch.setattr(vector_embedding, "coerce_embedding", reject_marker)
    result = _write(resource, _doc_rows("rejected-doc", 2))
    assert result.meta["vector_stale_cleanup_skipped_docs"] == 1
    assert _count_source(resource, source_id) == 3


def test_live_qdrant_cleanup_failure_reports_upsert_landed(vector_destination, monkeypatch):
    if vector_destination["kind"] != "qdrant":
        pytest.skip("Qdrant-specific stale-cleanup failure case")
    from connectors.qdrant_writer import write_mapped_rows
    from services.vector_sync import vector_doc_key

    original = _write(vector_destination, _doc_rows("cleanup-failure", 1))
    assert original.rows_written == 1
    monkeypatch.setattr(
        "services.vector_sync.qdrant_delete_stale_chunks",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("injected cleanup failure")
        ),
    )
    row = _doc_rows("cleanup-failure", 1)[0]
    row["content"] = "cleanup-failure content changed before stale cleanup"
    headers = list(row)
    mappings = [
        {"source": column, "target": column, "target_type": "VARCHAR"}
        for column in headers
    ]
    cfg = vector_destination["cfg"]
    result = write_mapped_rows(
        host=cfg["host"],
        port=cfg["port"],
        database=cfg["database"],
        username=cfg["username"],
        password=cfg["password"],
        schema=cfg["schema"],
        connection_string=cfg["connection_string"],
        ssl=cfg["ssl"],
        table_name=vector_destination["name"],
        headers=headers,
        data_rows=[[row.get(column, "") for column in headers]],
        mappings=mappings,
        column_types={column: "string" for column in headers},
        destination_column_types={column: "VARCHAR" for column in headers},
        error_policy="quarantine",
        content_column="content",
        embedding_model="hash/32",
        conflict_columns=["doc_id"],
        write_mode="upsert",
        sync_mode="cdc",
    )
    assert result.ok is False
    assert "upsert landed" in (result.error or "").lower()
    assert "rerun" in (result.error or "").lower()
    assert _count_source(
        vector_destination, vector_doc_key(["cleanup-failure"])
    ) == 1


def _legacy_id(source_id: str, chunk_index: int, content: str) -> str:
    return hashlib.sha256(
        f"{source_id}:{chunk_index}:{content}".encode("utf-8")
    ).hexdigest()[:32]


def test_live_reindex_dry_run_migration_rerun_collision_and_audit(
    vector_destination, monkeypatch
):
    from services.vectorization import _stable_vector_row_id
    from services import vector_reindex

    resource = vector_destination
    kind = resource["kind"]
    cfg = resource["cfg"]
    name = resource["name"]
    movable = [
        {
            "source_id": "reindex-doc",
            "chunk_index": 0,
            "content": "legacy zero",
            "id": _legacy_id("reindex-doc", 0, "legacy zero"),
        },
        {
            "source_id": "reindex-doc",
            "chunk_index": 1,
            "content": "legacy one",
            "id": _legacy_id("reindex-doc", 1, "legacy one"),
        },
    ]
    collision = [
        {
            "source_id": "collision-doc",
            "chunk_index": 4,
            "content": "collision left",
            "id": _legacy_id("collision-doc", 4, "collision left"),
        },
        {
            "source_id": "collision-doc",
            "chunk_index": 4,
            "content": "collision right",
            "id": _legacy_id("collision-doc", 4, "collision right"),
        },
    ]
    seeded = movable + collision
    audit_events = []
    monkeypatch.setattr(
        vector_reindex,
        "append_audit_event",
        lambda **event: audit_events.append(event),
    )

    if kind == "pgvector":
        import psycopg2

        conn = psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            dbname=cfg["database"],
            user=cfg["username"],
            password=cfg["password"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
                cur.execute(
                    f"CREATE TABLE public.{name} ("
                    "id TEXT PRIMARY KEY, content TEXT, embedding vector(32), "
                    "metadata JSONB, source_id TEXT, chunk_index INTEGER, "
                    "created_at TIMESTAMPTZ DEFAULT now())"
                )
                vector_text = "[" + ",".join(["0.25"] * 32) + "]"
                for row in seeded:
                    cur.execute(
                        f"INSERT INTO public.{name} "
                        "(id, content, embedding, metadata, source_id, chunk_index) "
                        "VALUES (%s, %s, %s::vector, %s::jsonb, %s, %s)",
                        (
                            row["id"],
                            row["content"],
                            vector_text,
                            json.dumps({}),
                            row["source_id"],
                            row["chunk_index"],
                        ),
                    )
            conn.commit()
        finally:
            conn.close()
        stable_moved_ids = {
            _stable_vector_row_id(
                row["source_id"],
                row["chunk_index"],
                row["content"],
                multi_chunk=True,
            )
            for row in movable
        }
        dry = vector_reindex.reindex_to_stable_ids(
            kind, cfg, name, schema="public", dry_run=True, actor="test"
        )
        assert dry.scanned == 4
        assert dry.moved == 2
        assert dry.collisions == 2
        real = vector_reindex.reindex_to_stable_ids(
            kind, cfg, name, schema="public", dry_run=False, actor="test"
        )
        again = vector_reindex.reindex_to_stable_ids(
            kind, cfg, name, schema="public", dry_run=False, actor="test"
        )
        conn = psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            dbname=cfg["database"],
            user=cfg["username"],
            password=cfg["password"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT id FROM public.{name}")
                ids = {row[0] for row in cur.fetchall()}
        finally:
            conn.close()
        assert stable_moved_ids <= ids
        assert len(ids) == 4
    else:
        from connectors.qdrant_writer import _ensure_collection, qdrant_point_id, qdrant_rest

        session, base_url, headers = qdrant_rest(cfg)
        try:
            _ensure_collection(session, base_url, name, 32, headers)
            points = [
                {
                    "id": qdrant_point_id(row["id"]),
                    "vector": [0.25] * 32,
                    "payload": {
                        "source_id": row["source_id"],
                        "chunk_index": row["chunk_index"],
                        "content": row["content"],
                    },
                }
                for row in seeded
            ]
            response = session.put(
                f"{base_url}/collections/{name}/points?wait=true",
                json={"points": points},
                headers=headers,
                timeout=30,
            )
            assert response.status_code in {200, 201}, response.text
        finally:
            session.close()
        stable_moved_ids = {
            qdrant_point_id(
                _stable_vector_row_id(
                    row["source_id"],
                    row["chunk_index"],
                    row["content"],
                    multi_chunk=True,
                )
            )
            for row in movable
        }
        dry = vector_reindex.reindex_to_stable_ids(
            kind, cfg, name, dry_run=True, actor="test"
        )
        assert dry.scanned == 4
        assert dry.moved == 2
        assert dry.collisions == 2
        real = vector_reindex.reindex_to_stable_ids(
            kind, cfg, name, dry_run=False, actor="test"
        )
        again = vector_reindex.reindex_to_stable_ids(
            kind, cfg, name, dry_run=False, actor="test"
        )
        session, base_url, headers = qdrant_rest(cfg)
        try:
            response = session.post(
                f"{base_url}/collections/{name}/points/scroll",
                json={"limit": 100, "with_payload": True, "with_vector": False},
                headers=headers,
                timeout=30,
            )
            assert response.status_code == 200, response.text
            ids = {
                point["id"]
                for point in (response.json().get("result") or {}).get("points", [])
            }
        finally:
            session.close()
        assert stable_moved_ids <= ids
        assert len(ids) == 4

    assert real.moved == 2
    assert again.moved == 0
    assert all(event["action"] == "vector.reindex" for event in audit_events)
    assert all(event["resource"] == f"{kind}:{name}" for event in audit_events)
    assert all(event["actor"] == "test" for event in audit_events)
    assert all(
        "password" not in json.dumps(event["details"]).lower()
        and "legacy zero" not in json.dumps(event["details"])
        for event in audit_events
    )
