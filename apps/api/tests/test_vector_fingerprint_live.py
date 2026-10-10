from __future__ import annotations

import json
import os
import socket
import uuid
from collections.abc import Mapping
from typing import Any

import pytest

_PG_PORT = int(os.environ.get("DATAFLOW_TEST_PGVECTOR_PORT", "5434"))
_QDRANT_PORT = int(os.environ.get("DATAFLOW_TEST_QDRANT_PORT", "6335"))
_QDRANT_URL = f"http://localhost:{_QDRANT_PORT}"
_SIDECAR = "_df_vector_collections"


def _reachable(port: int) -> bool:
    try:
        with socket.create_connection(("localhost", port), timeout=1):
            return True
    except OSError:
        return False


_ENGINES = [
    pytest.param(
        "pgvector",
        id="pgvector-5434",
        marks=pytest.mark.skipif(
            not _reachable(_PG_PORT),
            reason=f"pgvector not reachable on localhost:{_PG_PORT}",
        ),
    ),
    pytest.param(
        "qdrant",
        id="qdrant-6335",
        marks=pytest.mark.skipif(
            not _reachable(_QDRANT_PORT),
            reason=f"Qdrant not reachable on localhost:{_QDRANT_PORT}",
        ),
    ),
]


@pytest.fixture(params=_ENGINES)
def fingerprint_destination(request):
    engine = str(request.param)
    name = f"gvec_m2_{engine}_{uuid.uuid4().hex[:10]}"
    if engine == "pgvector":
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
    resource = {"engine": engine, "name": name, "cfg": cfg}
    try:
        yield resource
    finally:
        if engine == "pgvector":
            import psycopg2
            from psycopg2 import sql

            conn = psycopg2.connect(
                host=cfg["host"],
                port=cfg["port"],
                dbname=cfg["database"],
                user=cfg["username"],
                password=cfg["password"],
            )
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT to_regclass(%s)", (f"public.{_SIDECAR}",))
                    if cur.fetchone()[0] is not None:
                        cur.execute(
                            sql.SQL("DELETE FROM {}.{} WHERE table_name = %s").format(
                                sql.Identifier("public"), sql.Identifier(_SIDECAR)
                            ),
                            (name,),
                        )
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

            requests.delete(f"{_QDRANT_URL}/collections/{name}", timeout=10)
            _delete_qdrant_fingerprint(name)


def _wire(value: Any) -> str:
    return json.dumps(value, separators=(",", ":")) if isinstance(value, (list, dict)) else str(value)


def _write(
    resource: Mapping[str, Any],
    rows: list[dict[str, Any]],
    *,
    model: str = "hash/32",
    chunk_size: int = 512,
    chunk_overlap: int = 50,
    skip_chunking: bool = True,
    embedding_column: str | None = None,
    **embedding_options: Any,
):
    from connectors.pgvector_writer import write_mapped_rows as write_pgvector
    from connectors.qdrant_writer import write_mapped_rows as write_qdrant

    cfg = resource["cfg"]
    headers = list(rows[0])
    mappings = [
        {"source": column, "target": column, "target_type": "VARCHAR"}
        for column in headers
    ]
    options = {
        **cfg,
        "table_name": resource["name"],
        "headers": headers,
        "data_rows": [[_wire(row.get(column, "")) for column in headers] for row in rows],
        "mappings": mappings,
        "column_types": {column: "string" for column in headers},
        "destination_column_types": {column: "VARCHAR" for column in headers},
        "error_policy": "quarantine",
        "content_column": "content",
        "embedding_column": embedding_column,
        "embedding_model": model,
        "chunk_size": chunk_size,
        "chunk_overlap": chunk_overlap,
        "skip_chunking": skip_chunking,
        "conflict_columns": ["doc_id"],
        "write_mode": "upsert",
        "sync_mode": "cdc",
    }
    options.update(embedding_options)
    writer = write_pgvector if resource["engine"] == "pgvector" else write_qdrant
    return writer(**options)


def _source_count(resource: Mapping[str, Any], source_id: str) -> int:
    if resource["engine"] == "pgvector":
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
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("SELECT count(*) FROM {}.{} WHERE source_id = %s").format(
                            sql.Identifier("public"), sql.Identifier(resource["name"])
                        ),
                        (source_id,),
                    )
                    return int(cur.fetchone()[0])
            except psycopg2.errors.UndefinedTable:
                return 0
        finally:
            conn.close()
    import requests

    response = requests.post(
        f"{_QDRANT_URL}/collections/{resource['name']}/points/count",
        json={
            "filter": {
                "must": [{"key": "source_id", "match": {"value": source_id}}]
            },
            "exact": True,
        },
        timeout=10,
    )
    if response.status_code == 404:
        return 0
    assert response.status_code == 200, response.text
    return int(response.json()["result"]["count"])


def _source_snapshot(resource: Mapping[str, Any], source_id: str) -> list[Any]:
    if resource["engine"] == "pgvector":
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
                    sql.SQL(
                        "SELECT id, content, embedding::text FROM {}.{} "
                        "WHERE source_id = %s ORDER BY id"
                    ).format(sql.Identifier("public"), sql.Identifier(resource["name"])),
                    (source_id,),
                )
                return list(cur.fetchall())
        finally:
            conn.close()
    import requests

    response = requests.post(
        f"{_QDRANT_URL}/collections/{resource['name']}/points/scroll",
        json={
            "filter": {
                "must": [{"key": "source_id", "match": {"value": source_id}}]
            },
            "limit": 100,
            "with_payload": True,
            "with_vector": True,
        },
        timeout=10,
    )
    assert response.status_code == 200, response.text
    points = response.json()["result"]["points"]
    return sorted(
        (
            point["id"],
            point.get("payload", {}).get("content"),
            tuple(point.get("vector") or []),
        )
        for point in points
    )


def _qdrant_point_id(collection: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"dataflow:fingerprint:{collection}"))


def _delete_qdrant_fingerprint(collection: str) -> None:
    import requests

    sidecar = requests.get(
        f"{_QDRANT_URL}/collections/{_SIDECAR}", timeout=10
    )
    if sidecar.status_code == 404:
        return
    assert sidecar.status_code == 200, sidecar.text
    response = requests.post(
        f"{_QDRANT_URL}/collections/{_SIDECAR}/points/delete?wait=true",
        json={"points": [_qdrant_point_id(collection)]},
        timeout=10,
    )
    assert response.status_code in {200, 404}, response.text


def _delete_fingerprint(resource: Mapping[str, Any]) -> None:
    if resource["engine"] == "pgvector":
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
                cur.execute("SELECT to_regclass(%s)", (f"public.{_SIDECAR}",))
                if cur.fetchone()[0] is not None:
                    cur.execute(
                        sql.SQL("DELETE FROM {}.{} WHERE table_name = %s").format(
                            sql.Identifier("public"), sql.Identifier(_SIDECAR)
                        ),
                        (resource["name"],),
                    )
            conn.commit()
        finally:
            conn.close()
    else:
        _delete_qdrant_fingerprint(str(resource["name"]))


def _read_fingerprint(resource: Mapping[str, Any]) -> dict[str, Any] | None:
    if resource["engine"] == "pgvector":
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
                cur.execute("SELECT to_regclass(%s)", (f"public.{_SIDECAR}",))
                if cur.fetchone()[0] is None:
                    return None
                cur.execute(
                    sql.SQL("SELECT fingerprint FROM {}.{} WHERE table_name = %s").format(
                        sql.Identifier("public"), sql.Identifier(_SIDECAR)
                    ),
                    (resource["name"],),
                )
                row = cur.fetchone()
                return dict(row[0]) if row else None
        finally:
            conn.close()
    import requests

    response = requests.post(
        f"{_QDRANT_URL}/collections/{_SIDECAR}/points",
        json={
            "ids": [_qdrant_point_id(str(resource["name"]))],
            "with_payload": True,
            "with_vector": False,
        },
        timeout=10,
    )
    assert response.status_code == 200, response.text
    points = response.json().get("result") or []
    payload = points[0].get("payload") if points else None
    fingerprint = payload.get("fingerprint") if isinstance(payload, dict) else None
    return dict(fingerprint) if isinstance(fingerprint, dict) else None


class _DeterministicSentenceTransformer:
    backend = "sentence_transformers"
    dimension = 32

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [[0.75] * self.dimension for _ in texts]


def _use_sentence_transformer_model(monkeypatch) -> None:
    from services import vectorization

    embedder = _DeterministicSentenceTransformer()
    monkeypatch.setattr(vectorization, "_get_embedder", lambda _model=None: embedder)


def test_model_mismatch_is_rejected_before_upsert(fingerprint_destination, monkeypatch):
    resource = fingerprint_destination
    doc_id = "fingerprint-model-lock"
    first = _write(
        resource,
        [{"doc_id": doc_id, "content": "model fingerprint test body"}],
    )
    assert first.ok, first.error
    source_id = doc_id
    count_before = _source_count(resource, source_id)
    snapshot_before = _source_snapshot(resource, source_id)

    _use_sentence_transformer_model(monkeypatch)
    rejected = _write(
        resource,
        [{"doc_id": doc_id, "content": "model fingerprint test body"}],
        model="sentence-transformers/all-MiniLM-L6-v2",
    )

    assert rejected.ok is False
    assert "model:" in (rejected.error or "")
    assert "sentence-transformers/all-MiniLM-L6-v2" in (rejected.error or "")
    assert "Write to a new collection/table" in (rejected.error or "")
    assert _source_count(resource, source_id) == count_before
    assert _source_snapshot(resource, source_id) == snapshot_before


def test_same_model_write_is_verified(fingerprint_destination):
    resource = fingerprint_destination
    row = {"doc_id": "fingerprint-same-model", "content": "repeat stable embedding"}
    assert _write(resource, [row]).ok

    repeated = _write(resource, [row])

    assert repeated.ok, repeated.error
    meta = dict(getattr(repeated, "meta", {}) or {})
    assert meta.get("vector_fingerprint_status") == "verified"
    assert meta.get("vector_fingerprint_digest")


def test_chunk_size_change_is_rejected(fingerprint_destination):
    resource = fingerprint_destination
    row = {
        "doc_id": "fingerprint-chunk-size",
        "content": "recursive chunk input " * 80,
    }
    first = _write(resource, [row], chunk_size=64, chunk_overlap=8, skip_chunking=False)
    assert first.ok, first.error
    count_before = _source_count(resource, row["doc_id"])

    changed = _write(
        resource,
        [row],
        chunk_size=96,
        chunk_overlap=8,
        skip_chunking=False,
    )

    assert changed.ok is False
    assert "chunker.chunk_size" in (changed.error or "")
    assert "Write to a new collection/table" in (changed.error or "")
    assert _source_count(resource, row["doc_id"]) == count_before


def test_legacy_target_is_adopted_with_warning(fingerprint_destination, caplog):
    import logging

    resource = fingerprint_destination
    row = {"doc_id": "fingerprint-legacy", "content": "legacy vector without sidecar"}
    assert _write(resource, [row]).ok
    _delete_fingerprint(resource)

    with caplog.at_level(logging.WARNING, logger="services.vector_fingerprint"):
        adopted = _write(resource, [row])

    assert adopted.ok, adopted.error
    meta = dict(getattr(adopted, "meta", {}) or {})
    assert meta.get("vector_fingerprint_status") == "adopted_unverified"
    assert any("existing vectors could not be fully verified" in item.message for item in caplog.records)


def test_conflicting_legacy_backend_stamp_is_rejected(fingerprint_destination):
    resource = fingerprint_destination
    row = {"doc_id": "fingerprint-legacy-backend", "content": "stamped legacy vector"}
    assert _write(resource, [row]).ok
    _delete_fingerprint(resource)
    if resource["engine"] == "pgvector":
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
                    sql.SQL(
                            "UPDATE {}.{} SET metadata = jsonb_set("
                            "COALESCE(metadata, '{{}}'::jsonb), "
                            "'{{_df_embedding_backend}}', to_jsonb(%s::text), true) "
                        "WHERE source_id = %s"
                    ).format(sql.Identifier("public"), sql.Identifier(resource["name"])),
                    ("tfidf_fallback", row["doc_id"]),
                )
            conn.commit()
        finally:
            conn.close()
    else:
        import requests

        response = requests.post(
            f"{_QDRANT_URL}/collections/{resource['name']}/points/scroll",
            json={
                "filter": {
                    "must": [
                        {"key": "source_id", "match": {"value": row["doc_id"]}}
                    ]
                },
                "limit": 100,
                "with_payload": False,
                "with_vector": False,
            },
            timeout=10,
        )
        assert response.status_code == 200, response.text
        point_ids = [
            point["id"] for point in response.json()["result"]["points"]
        ]
        response = requests.post(
            f"{_QDRANT_URL}/collections/{resource['name']}/points/payload?wait=true",
            json={
                "payload": {"_df_embedding_backend": "tfidf_fallback"},
                "points": point_ids,
            },
            timeout=10,
        )
        assert response.status_code == 200, response.text

    count_before = _source_count(resource, row["doc_id"])
    rejected = _write(resource, [row], model="hash/32")

    assert rejected.ok is False
    assert "tfidf_fallback" in (rejected.error or "")
    assert "hash" in (rejected.error or "")
    assert _source_count(resource, row["doc_id"]) == count_before


def test_qdrant_drop_clears_fingerprint_before_rewrite(fingerprint_destination, monkeypatch):
    resource = fingerprint_destination
    if resource["engine"] != "qdrant":
        pytest.skip("Qdrant collection-drop integration")

    from connectors.table_manager import drop_table

    row = {"doc_id": "fingerprint-drop", "content": "rewrite after collection drop"}
    assert _write(resource, [row]).ok
    assert drop_table("qdrant", resource["cfg"], resource["name"]) is True

    _use_sentence_transformer_model(monkeypatch)
    rewritten = _write(
        resource,
        [row],
        model="sentence-transformers/all-MiniLM-L6-v2",
    )

    assert rewritten.ok, rewritten.error
    meta = dict(getattr(rewritten, "meta", {}) or {})
    assert meta.get("vector_fingerprint_status") == "created"


def test_pgvector_drop_clears_fingerprint_before_rewrite(fingerprint_destination, monkeypatch):
    resource = fingerprint_destination
    if resource["engine"] != "pgvector":
        pytest.skip("pgvector table-drop integration")

    from connectors.table_manager import drop_table

    row = {"doc_id": "fingerprint-pg-drop", "content": "rewrite after pgvector drop"}
    assert _write(resource, [row]).ok
    assert _read_fingerprint(resource) is not None
    assert drop_table("pgvector", resource["cfg"], resource["name"]) is True
    assert _read_fingerprint(resource) is None

    _use_sentence_transformer_model(monkeypatch)
    rewritten = _write(
        resource,
        [row],
        model="sentence-transformers/all-MiniLM-L6-v2",
    )

    assert rewritten.ok, rewritten.error
    meta = dict(getattr(rewritten, "meta", {}) or {})
    assert meta.get("vector_fingerprint_status") == "created"


def test_fake_openai_usage_and_auth_failure_are_visible(fingerprint_destination):
    class Response:
        def __init__(self, status, body=None):
            self.status_code = status
            self.headers = {}
            self.body = body or {}
            self.text = "safe error"

        def json(self):
            return self.body

    class Session:
        def __init__(self, status):
            self.status = status
            self.calls = 0

        def post(self, _url, **_kwargs):
            self.calls += 1
            if self.status == 401:
                return Response(401)
            return Response(
                200,
                {
                    "data": [{"index": 0, "embedding": [0.25] * 32}],
                    "usage": {"prompt_tokens": 100},
                },
            )

    resource = fingerprint_destination
    auth_text = f"safe auth failure {resource['engine']}"
    failed = _write(
        resource,
        [{"doc_id": "provider-auth-fail", "content": auth_text}],
        model="openai/text-embedding-3-small",
        _embedding_session=Session(401),
        embedding_api_key="APIKEY_SENTINEL",
        embedding_dimensions=32,
        durable_embedding_cache=False,
    )
    assert failed.ok is False
    assert "EmbeddingAuthError" in (failed.error or "")
    assert _source_count(resource, "provider-auth-fail") == 0

    fake = Session(200)
    written = _write(
        resource,
        [
            {
                "doc_id": "provider-usage-success",
                "content": f"safe provider success {resource['engine']}",
            }
        ],
        model="openai/text-embedding-3-small",
        _embedding_session=fake,
        embedding_api_key="APIKEY_SENTINEL",
        embedding_dimensions=32,
        durable_embedding_cache=False,
    )
    assert written.ok, written.error
    usage = written.meta["embedding_usage"]
    assert usage["provider"] == "openai"
    assert usage["calls"] == 1
    assert usage["input_tokens"] == 100
    assert usage["estimated_cost_usd"] == pytest.approx(0.02 * 100 / 1_000_000)
    assert _source_count(resource, "provider-usage-success") == 1
    assert fake.calls == 1

    from connectors.table_manager import drop_table

    assert drop_table(
        resource["engine"], resource["cfg"], resource["name"]
    ) is True
    hashed = _write(
        resource,
        [{"doc_id": "provider-hash-cost", "content": "safe content"}],
        model="hash/32",
        durable_embedding_cache=False,
    )
    assert hashed.ok, hashed.error
    assert hashed.meta["embedding_usage"]["estimated_cost_usd"] == 0.0


def test_embedding_column_fingerprint_and_dimension_gate(fingerprint_destination):
    resource = fingerprint_destination
    row = {
        "doc_id": "fingerprint-source-embedding",
        "content": "source embedding column",
        "embedding": [0.125] * 32,
    }
    first = _write(resource, [row], embedding_column="embedding")
    assert first.ok, first.error
    fingerprint = _read_fingerprint(resource)
    assert fingerprint is not None
    assert fingerprint["provider"] == "source_embedding"
    assert fingerprint["model"] == "column:embedding"
    assert fingerprint["dimension"] == 32
    count_before = _source_count(resource, row["doc_id"])

    bad_dimension = dict(row, embedding=[0.25] * 31)
    rejected = _write(
        resource,
        [bad_dimension],
        embedding_column="embedding",
    )

    assert rejected.ok is False
    assert "dimension" in (rejected.error or "").lower()
    assert _source_count(resource, row["doc_id"]) == count_before
