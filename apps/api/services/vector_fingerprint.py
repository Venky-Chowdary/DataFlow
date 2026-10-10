"""Embedding identity and persistent vector-target fingerprint enforcement."""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

_PGVECTOR_SIDECAR = "_df_vector_collections"
_QDRANT_SIDECAR = "_df_vector_collections"
_FINGERPRINT_STATUSES = frozenset({"created", "verified", "adopted_unverified"})


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


@dataclass(frozen=True)
class EmbeddingFingerprint:
    provider: str
    model: str
    dimension: int
    distance: str
    chunker: dict[str, Any]

    def __post_init__(self) -> None:
        if not self.provider or not self.model or not self.distance:
            raise ValueError("fingerprint provider, model, and distance are required")
        if isinstance(self.dimension, bool) or int(self.dimension) <= 0:
            raise ValueError("fingerprint dimension must be a positive integer")
        object.__setattr__(self, "dimension", int(self.dimension))
        object.__setattr__(self, "chunker", dict(self.chunker))

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "model": self.model,
            "dimension": self.dimension,
            "distance": self.distance,
            "chunker": dict(self.chunker),
        }

    @property
    def digest(self) -> str:
        return hashlib.sha256(_canonical_json(self.to_dict()).encode("utf-8")).hexdigest()

    def diff(self, other: EmbeddingFingerprint) -> list[str]:
        diffs: list[str] = []
        for field in ("provider", "model", "dimension", "distance"):
            stored = getattr(self, field)
            incoming = getattr(other, field)
            if stored != incoming:
                diffs.append(
                    f"{field}: {stored!r} (stored) != {incoming!r} (incoming)"
                )
        chunker_keys = sorted(set(self.chunker) | set(other.chunker))
        for key in chunker_keys:
            stored = self.chunker.get(key)
            incoming = other.chunker.get(key)
            if stored != incoming:
                diffs.append(
                    f"chunker.{key}: {stored!r} (stored) != {incoming!r} (incoming)"
                )
        return diffs


class VectorFingerprintMismatchError(ValueError):
    """The target's stored embedding settings differ from the incoming write."""

    def __init__(
        self,
        stored: EmbeddingFingerprint | Mapping[str, Any],
        incoming: EmbeddingFingerprint,
        diffs: Sequence[str] | None = None,
    ) -> None:
        self.stored = stored.to_dict() if isinstance(stored, EmbeddingFingerprint) else dict(stored)
        self.incoming = incoming.to_dict()
        self.diffs = list(
            diffs
            if diffs is not None
            else (
                stored.diff(incoming)
                if isinstance(stored, EmbeddingFingerprint)
                else [
                    f"{key}: {value!r} (stored) != "
                    f"{incoming.to_dict().get(key)!r} (incoming)"
                    for key, value in stored.items()
                    if incoming.to_dict().get(key) != value
                ]
            )
        )
        detail = "; ".join(self.diffs) or "stored fingerprint data differs"
        stored_values = _canonical_json(self.stored)
        super().__init__(
            f"Vector embedding fingerprint mismatch: {detail}. "
            "Write to a new collection/table, or drop it (full refresh) to "
            "re-embed with the new settings; or set the Studio embedding "
            "model/chunk settings back to the stored values: "
            f"{stored_values}."
        )


def fingerprint_for_write(
    *,
    model: str | None,
    dimension: int,
    distance: str,
    chunk_size: int,
    chunk_overlap: int,
    skip_chunking: bool,
    embedding_column: str | None,
    chunk_strategy: str = "recursive",
    chunk_unit: str = "chars",
    chunk_tokenizer: str | None = None,
    text_template: str | None = None,
) -> EmbeddingFingerprint:
    """Build a credential-free identity for one vector write configuration."""
    from services import vectorization

    embedder = vectorization._get_embedder(model)
    resolved_model = str(
        model
        or vectorization.getenv_brand(
            "EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2"
        )
    ).strip()
    backend = str(getattr(embedder, "backend", "") or "").strip()
    if embedding_column:
        provider = "source_embedding"
        canonical_model = f"column:{embedding_column}"
    else:
        remote_providers = {
            "azure/": "azure_openai",
            "openai-compatible/": "openai_compatible",
            "cohere/": "cohere",
            "bedrock/": "bedrock",
        }
        remote_prefix = next(
            (prefix for prefix in remote_providers if resolved_model.startswith(prefix)),
            None,
        )
        if remote_prefix:
            provider = remote_providers[remote_prefix]
            canonical_model = resolved_model.split("/", 1)[1]
        else:
            canonical_model = resolved_model
        if not backend:
            if resolved_model.startswith(("hash/", "deterministic/")):
                backend = "hash"
            elif resolved_model.startswith("openai/") or resolved_model.startswith(
                "text-embedding-"
            ):
                backend = "openai"
            elif type(embedder).__name__ == "_SentenceTransformerEmbedder":
                backend = "sentence_transformers"
        if not remote_prefix:
            provider = backend
        if provider not in {
            "openai",
            "hash",
            "sentence_transformers",
            "tfidf_fallback",
            "azure_openai",
            "openai_compatible",
            "cohere",
            "bedrock",
        }:
            raise ValueError(
                f"unsupported embedding backend {provider!r}; refusing to "
                "persist an ambiguous vector fingerprint"
            )
        if canonical_model.startswith("deterministic/"):
            canonical_model = "hash/" + canonical_model.split("/", 1)[1]
        elif canonical_model.startswith("text-embedding-"):
            canonical_model = "openai/" + canonical_model
        if provider == "hash":
            suffix = canonical_model.split("/", 1)[1] if "/" in canonical_model else ""
            model_dimension = suffix if suffix.isdigit() else str(
                getattr(embedder, "dimension", dimension)
            )
            canonical_model = f"hash/{model_dimension}"
    if "://" in canonical_model:
        raise ValueError("embedding model identity must not contain a URL")
    chunker: dict[str, Any] = {
        "strategy": chunk_strategy,
        "chunk_size": int(chunk_size),
        "chunk_overlap": int(chunk_overlap),
        "skip_chunking": bool(skip_chunking),
    }
    if chunk_unit != "chars":
        chunker["unit"] = chunk_unit
    if chunk_tokenizer is not None:
        chunker["tokenizer"] = str(chunk_tokenizer)
    if text_template is not None:
        chunker["template_sha256"] = hashlib.sha256(
            text_template.encode("utf-8")
        ).hexdigest()
    return EmbeddingFingerprint(
        provider=provider,
        model=canonical_model,
        dimension=dimension,
        distance=str(distance),
        chunker=chunker,
    )


def _fingerprint_from_dict(value: Mapping[str, Any]) -> EmbeddingFingerprint:
    try:
        chunker = value["chunker"]
        if not isinstance(chunker, Mapping):
            raise TypeError("chunker must be an object")
        return EmbeddingFingerprint(
            provider=str(value["provider"]),
            model=str(value["model"]),
            dimension=int(value["dimension"]),
            distance=str(value["distance"]),
            chunker=dict(chunker),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"stored vector fingerprint is invalid: {exc}") from exc


def _legacy_backend_mismatch(
    backend: str, incoming: EmbeddingFingerprint
) -> VectorFingerprintMismatchError:
    stored = incoming.to_dict()
    stored["provider"] = backend
    stored["model"] = "legacy vectors (backend stamp only)"
    return VectorFingerprintMismatchError(
        stored,
        incoming,
        [
            f"provider: {backend!r} (stored) != "
            f"{incoming.provider!r} (incoming)"
        ],
    )


def _check_existing_fingerprint(
    stored_values: Mapping[str, Any],
    stored_digest: str,
    incoming: EmbeddingFingerprint,
) -> None:
    stored = _fingerprint_from_dict(stored_values)
    if stored.digest != str(stored_digest):
        raise ValueError("stored vector fingerprint digest does not match its payload")
    if stored.digest != incoming.digest:
        raise VectorFingerprintMismatchError(stored, incoming)


def _pgvector_ensure_sidecar(cursor: Any, schema: str) -> None:
    from psycopg2 import sql

    cursor.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {}.{} (
                table_name TEXT PRIMARY KEY,
                fingerprint JSONB NOT NULL,
                digest TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at timestamptz DEFAULT now(),
                updated_at timestamptz DEFAULT now()
            )
            """
        ).format(sql.Identifier(schema), sql.Identifier(_PGVECTOR_SIDECAR))
    )


def _pgvector_read_fingerprint(
    cursor: Any, schema: str, target: str
) -> tuple[dict[str, Any], str, str] | None:
    from psycopg2 import sql

    cursor.execute(
        sql.SQL("SELECT fingerprint, digest, status FROM {}.{} WHERE table_name = %s").format(
            sql.Identifier(schema), sql.Identifier(_PGVECTOR_SIDECAR)
        ),
        (target,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    values = row[0] if isinstance(row[0], Mapping) else {}
    return dict(values), str(row[1]), str(row[2])


def _pgvector_write_fingerprint(
    cursor: Any,
    schema: str,
    target: str,
    incoming: EmbeddingFingerprint,
    status: str,
) -> bool:
    from psycopg2 import sql

    cursor.execute(
        sql.SQL(
            """
            INSERT INTO {}.{} (table_name, fingerprint, digest, status)
            VALUES (%s, %s::jsonb, %s, %s)
            ON CONFLICT (table_name) DO NOTHING
            """
        ).format(sql.Identifier(schema), sql.Identifier(_PGVECTOR_SIDECAR)),
        (target, _canonical_json(incoming.to_dict()), incoming.digest, status),
    )
    return cursor.rowcount == 1


def _pgvector_delete_fingerprint(cursor: Any, schema: str, target: str) -> None:
    from psycopg2 import sql

    cursor.execute(
        sql.SQL("DELETE FROM {}.{} WHERE table_name = %s").format(
            sql.Identifier(schema), sql.Identifier(_PGVECTOR_SIDECAR)
        ),
        (target,),
    )


def delete_pgvector_fingerprint(cursor: Any, schema: str, target: str) -> None:
    try:
        _pgvector_delete_fingerprint(cursor, schema, target)
    except Exception as exc:
        if getattr(exc, "pgcode", None) != "42P01":
            raise


def _pgvector_legacy_backends(
    cursor: Any, schema: str, target: str
) -> set[str]:
    from psycopg2 import sql

    cursor.execute(
        sql.SQL(
            """
            SELECT metadata->>'_df_embedding_backend'
            FROM {}.{}
            WHERE metadata ? '_df_embedding_backend'
            LIMIT 100
            """
        ).format(sql.Identifier(schema), sql.Identifier(target))
    )
    return {
        str(row[0]).strip()
        for row in cursor.fetchall()
        if row and str(row[0] or "").strip()
    }


def _qdrant_point_id(collection: str) -> str:
    return str(
        uuid.uuid5(uuid.NAMESPACE_URL, f"dataflow:fingerprint:{collection}")
    )


def _qdrant_request(
    session: Any,
    method: str,
    url: str,
    *,
    headers: Mapping[str, str],
    payload: Mapping[str, Any] | None = None,
) -> Any:
    request = getattr(session, method)
    kwargs: dict[str, Any] = {"headers": dict(headers), "timeout": 30}
    if payload is not None:
        kwargs["data"] = json.dumps(payload, ensure_ascii=False, allow_nan=False)
    return request(url, **kwargs)


def _qdrant_ensure_sidecar(
    session: Any, base_url: str, headers: Mapping[str, str]
) -> None:
    url = f"{base_url}/collections/{_QDRANT_SIDECAR}"
    response = _qdrant_request(session, "get", url, headers=headers)
    if response.status_code == 200:
        return
    if response.status_code != 404:
        raise RuntimeError(
            f"Qdrant fingerprint sidecar probe failed: "
            f"{response.status_code} {response.text[:300]}"
        )
    response = _qdrant_request(
        session,
        "put",
        url,
        headers=headers,
        payload={"vectors": {"size": 1, "distance": "Cosine"}},
    )
    if response.status_code not in {200, 201, 409}:
        raise RuntimeError(
            f"Qdrant fingerprint sidecar create failed: "
            f"{response.status_code} {response.text[:300]}"
        )
    if response.status_code == 409:
        check = _qdrant_request(session, "get", url, headers=headers)
        if check.status_code != 200:
            raise RuntimeError(
                f"Qdrant fingerprint sidecar create race could not be verified: "
                f"{check.status_code} {check.text[:300]}"
            )


def _qdrant_read_fingerprint(
    session: Any,
    base_url: str,
    headers: Mapping[str, str],
    target: str,
) -> tuple[dict[str, Any], str, str] | None:
    sidecar_url = f"{base_url}/collections/{_QDRANT_SIDECAR}"
    exists = _qdrant_request(session, "get", sidecar_url, headers=headers)
    if exists.status_code == 404:
        return None
    if exists.status_code != 200:
        raise RuntimeError(
            f"Qdrant fingerprint sidecar probe failed: "
            f"{exists.status_code} {exists.text[:300]}"
        )
    response = _qdrant_request(
        session,
        "post",
        f"{sidecar_url}/points",
        headers=headers,
        payload={
            "ids": [_qdrant_point_id(target)],
            "with_payload": True,
            "with_vector": False,
        },
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"Qdrant fingerprint read failed: "
            f"{response.status_code} {response.text[:300]}"
        )
    result = response.json()
    points = result.get("result") if isinstance(result, dict) else None
    if not isinstance(points, list) or not points:
        return None
    payload = points[0].get("payload") if isinstance(points[0], dict) else None
    if not isinstance(payload, dict) or payload.get("collection") != target:
        return None
    fingerprint = payload.get("fingerprint")
    if not isinstance(fingerprint, Mapping):
        raise ValueError("stored Qdrant vector fingerprint payload is invalid")
    return dict(fingerprint), str(payload.get("digest") or ""), str(
        payload.get("status") or ""
    )


def _qdrant_write_fingerprint(
    session: Any,
    base_url: str,
    headers: Mapping[str, str],
    target: str,
    incoming: EmbeddingFingerprint,
    status: str,
) -> None:
    _qdrant_ensure_sidecar(session, base_url, headers)
    sidecar_url = f"{base_url}/collections/{_QDRANT_SIDECAR}"
    response = _qdrant_request(
        session,
        "put",
        f"{sidecar_url}/points?wait=true",
        headers=headers,
        payload={
            "points": [
                {
                    "id": _qdrant_point_id(target),
                    "vector": [0.0],
                    "payload": {
                        "collection": target,
                        "fingerprint": incoming.to_dict(),
                        "digest": incoming.digest,
                        "status": status,
                    },
                }
            ]
        },
    )
    if response.status_code not in {200, 201}:
        raise RuntimeError(
            f"Qdrant fingerprint write failed: "
            f"{response.status_code} {response.text[:300]}"
        )


def delete_qdrant_fingerprint(
    session: Any,
    base_url: str,
    headers: Mapping[str, str],
    target: str,
) -> None:
    """Remove the deterministic sidecar point for a Qdrant collection."""
    sidecar_url = f"{base_url}/collections/{_QDRANT_SIDECAR}"
    exists = _qdrant_request(session, "get", sidecar_url, headers=headers)
    if exists.status_code == 404:
        return
    if exists.status_code != 200:
        raise RuntimeError(
            f"Qdrant fingerprint sidecar probe failed: "
            f"{exists.status_code} {exists.text[:300]}"
        )
    response = _qdrant_request(
        session,
        "post",
        f"{sidecar_url}/points/delete?wait=true",
        headers=headers,
        payload={"points": [_qdrant_point_id(target)]},
    )
    if response.status_code not in {200, 404}:
        raise RuntimeError(
            f"Qdrant fingerprint delete failed: "
            f"{response.status_code} {response.text[:300]}"
        )


def _qdrant_legacy_backends(
    session: Any,
    base_url: str,
    headers: Mapping[str, str],
    target: str,
) -> set[str]:
    response = _qdrant_request(
        session,
        "post",
        f"{base_url}/collections/{target}/points/scroll",
        headers=headers,
        payload={"limit": 100, "with_payload": True, "with_vector": False},
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"Qdrant legacy embedding-backend sample failed: "
            f"{response.status_code} {response.text[:300]}"
        )
    result = response.json()
    body = result.get("result") if isinstance(result, dict) else None
    points = body.get("points") if isinstance(body, dict) else None
    backends: set[str] = set()
    for point in points if isinstance(points, list) else []:
        payload = point.get("payload") if isinstance(point, dict) else None
        if not isinstance(payload, dict):
            continue
        metadata = payload.get("metadata")
        backend = (
            metadata.get("_df_embedding_backend")
            if isinstance(metadata, Mapping)
            else None
        ) or payload.get("_df_embedding_backend")
        if str(backend or "").strip():
            backends.add(str(backend).strip())
    return backends


def _qdrant_target_count(session: Any, base_url: str, headers: Mapping[str, str], target: str) -> int:
    response = _qdrant_request(
        session,
        "post",
        f"{base_url}/collections/{target}/points/count",
        headers=headers,
        payload={"exact": True},
    )
    if response.status_code == 404:
        return 0
    if response.status_code != 200:
        raise RuntimeError(
            f"Qdrant target count failed: {response.status_code} {response.text[:300]}"
        )
    result = response.json()
    body = result.get("result") if isinstance(result, dict) else None
    try:
        return int(body.get("count", 0)) if isinstance(body, Mapping) else 0
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Qdrant returned an invalid target point count") from exc


def _enforce_pgvector(
    cursor: Any, schema: str, target: str, incoming: EmbeddingFingerprint
) -> str:
    _pgvector_ensure_sidecar(cursor, schema)
    stored = _pgvector_read_fingerprint(cursor, schema, target)
    if stored is not None:
        _check_existing_fingerprint(stored[0], stored[1], incoming)
        return "verified"

    from psycopg2 import sql

    cursor.execute(
        sql.SQL("SELECT count(*) FROM {}.{}").format(
            sql.Identifier(schema), sql.Identifier(target)
        )
    )
    nonempty = int(cursor.fetchone()[0]) > 0
    status = "created"
    if nonempty:
        backends = _pgvector_legacy_backends(cursor, schema, target)
        conflict = next(
            (backend for backend in sorted(backends) if backend != incoming.provider),
            None,
        )
        if conflict:
            raise _legacy_backend_mismatch(conflict, incoming)
        status = "adopted_unverified"
        logger.warning(
            "Adopting legacy pgvector target %s.%s with an unverified embedding "
            "fingerprint; existing vectors could not be fully verified",
            schema,
            target,
        )
    inserted = _pgvector_write_fingerprint(cursor, schema, target, incoming, status)
    reread = _pgvector_read_fingerprint(cursor, schema, target)
    if reread is None:
        raise RuntimeError("pgvector fingerprint write could not be read back")
    _check_existing_fingerprint(reread[0], reread[1], incoming)
    return status if inserted else "verified"


def _enforce_qdrant(
    session: Any,
    base_url: str,
    headers: Mapping[str, str],
    target: str,
    incoming: EmbeddingFingerprint,
) -> str:
    if target == _QDRANT_SIDECAR:
        raise ValueError(f"{_QDRANT_SIDECAR!r} is reserved for vector fingerprints")
    stored = _qdrant_read_fingerprint(session, base_url, headers, target)
    if stored is not None:
        _check_existing_fingerprint(stored[0], stored[1], incoming)
        return "verified"

    status = "created"
    if _qdrant_target_count(session, base_url, headers, target) > 0:
        backends = _qdrant_legacy_backends(session, base_url, headers, target)
        conflict = next(
            (backend for backend in sorted(backends) if backend != incoming.provider),
            None,
        )
        if conflict:
            raise _legacy_backend_mismatch(conflict, incoming)
        status = "adopted_unverified"
        logger.warning(
            "Adopting legacy Qdrant target %s with an unverified embedding "
            "fingerprint; existing vectors could not be fully verified",
            target,
        )
    # Qdrant does not offer a conditional payload write, so competing first
    # writers can replace one another between the upsert and this read-back.
    _qdrant_write_fingerprint(session, base_url, headers, target, incoming, status)
    reread = _qdrant_read_fingerprint(session, base_url, headers, target)
    if reread is None:
        raise RuntimeError("Qdrant fingerprint write could not be read back")
    _check_existing_fingerprint(reread[0], reread[1], incoming)
    return status


def _enforce_weaviate(
    cfg: Mapping[str, Any], target: str, incoming: EmbeddingFingerprint
) -> str:
    import uuid

    from connectors.weaviate_writer import _base_url, _headers, _requests_session
    from services.vector_sync import _m6_engine_rows

    sidecar = "DfVectorCollections"
    if target == sidecar:
        raise ValueError(f"{sidecar!r} is reserved for vector fingerprints")
    session = _requests_session()
    base_url = _base_url(
        str(cfg.get("host") or ""),
        int(cfg.get("port") or 8080),
        bool(cfg.get("ssl")),
        str(cfg.get("connection_string") or ""),
    )
    headers = _headers(str(cfg.get("api_key") or ""))
    object_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"dataflow-vector:{target}"))
    try:
        probe = session.get(f"{base_url}/v1/schema/{sidecar}", headers=headers, timeout=15)
        if probe.status_code == 404:
            created = session.post(
                f"{base_url}/v1/schema",
                headers=headers,
                json={
                    "class": sidecar,
                    "vectorizer": "none",
                    "properties": [
                        {"name": "target", "dataType": ["text"]},
                        {"name": "fingerprint", "dataType": ["text"]},
                        {"name": "digest", "dataType": ["text"]},
                        {"name": "status", "dataType": ["text"]},
                    ],
                },
                timeout=30,
            )
            if created.status_code not in {200, 201}:
                verify = session.get(
                    f"{base_url}/v1/schema/{sidecar}", headers=headers, timeout=15
                )
                if verify.status_code != 200:
                    raise RuntimeError(
                        f"Weaviate fingerprint class creation failed: {created.status_code}"
                    )
        elif probe.status_code != 200:
            raise RuntimeError(f"Weaviate fingerprint class probe failed: {probe.status_code}")

        object_url = f"{base_url}/v1/objects/{sidecar}/{object_id}"
        stored_resp = session.get(object_url, headers=headers, timeout=15)
        stored = None
        if stored_resp.status_code == 200:
            stored = (stored_resp.json().get("properties") or {})
        elif stored_resp.status_code != 404:
            raise RuntimeError(
                f"Weaviate fingerprint read failed: {stored_resp.status_code}"
            )
        if isinstance(stored, Mapping):
            values = json.loads(str(stored.get("fingerprint") or "{}"))
            _check_existing_fingerprint(
                values, str(stored.get("digest") or ""), incoming
            )
            return "verified"
        existing = [
            row
            for row in _m6_engine_rows("weaviate", cfg, target)
            if row.get("source_id")
        ]
        status = "created"
        if existing:
            backends = {
                str((row.get("metadata") or {}).get("_df_embedding_backend") or "")
                for row in existing
                if isinstance(row.get("metadata"), Mapping)
            }
            conflict = next(
                (backend for backend in sorted(backends) if backend and backend != incoming.provider),
                None,
            )
            if conflict:
                raise _legacy_backend_mismatch(conflict, incoming)
            status = "adopted_unverified"
        props = {
            "target": target,
            "fingerprint": _canonical_json(incoming.to_dict()),
            "digest": incoming.digest,
            "status": status,
        }
        written = session.post(
            f"{base_url}/v1/objects",
            headers=headers,
            json={"class": sidecar, "id": object_id, "properties": props},
            timeout=30,
        )
        if written.status_code not in {200, 201}:
            reread = session.get(object_url, headers=headers, timeout=15)
            if reread.status_code == 200:
                props = reread.json().get("properties") or {}
                _check_existing_fingerprint(
                    json.loads(str(props.get("fingerprint") or "{}")),
                    str(props.get("digest") or ""),
                    incoming,
                )
                return "verified"
            raise RuntimeError(f"Weaviate fingerprint write failed: {written.status_code}")
        reread = session.get(object_url, headers=headers, timeout=15)
        if reread.status_code != 200:
            raise RuntimeError("Weaviate fingerprint write could not be read back")
        props = reread.json().get("properties") or {}
        _check_existing_fingerprint(
            json.loads(str(props.get("fingerprint") or "{}")),
            str(props.get("digest") or ""),
            incoming,
        )
        return status
    finally:
        session.close()


def _enforce_pinecone(
    cfg: Mapping[str, Any], target: str, incoming: EmbeddingFingerprint
) -> str:
    import hashlib
    import json

    from connectors.pinecone_writer import _headers, _index_url, _requests_session
    from services.vector_sync import _m6_engine_rows

    namespace = "_df_fingerprints"
    if target == namespace:
        raise ValueError(f"{namespace!r} is reserved for vector fingerprints")
    session = _requests_session()
    base_url = _index_url(
        str(cfg.get("host") or ""), str(cfg.get("connection_string") or "")
    )
    headers = _headers(
        str(cfg.get("api_key") or cfg.get("password") or cfg.get("username") or "")
    )
    vector_id = hashlib.sha256(str(target).encode("utf-8")).hexdigest()
    try:
        stats = session.get(
            f"{base_url}/describe_index_stats", headers=headers, timeout=15
        )
        if stats.status_code != 200:
            raise RuntimeError(f"Pinecone index stats failed: {stats.status_code}")
        dimension = int((stats.json() or {}).get("dimension") or 0)
        if dimension != incoming.dimension or dimension <= 0:
            raise ValueError(
                f"Pinecone index dimension {dimension} does not match embedding dimension {incoming.dimension}"
            )
        fetch_params = [("ids", vector_id), ("namespace", namespace)]
        fetched = session.get(
            f"{base_url}/vectors/fetch",
            headers=headers,
            params=fetch_params,
            timeout=30,
        )
        if fetched.status_code not in {200, 404}:
            raise RuntimeError(f"Pinecone fingerprint read failed: {fetched.status_code}")
        vectors = (fetched.json() or {}).get("vectors") or {}
        stored = vectors.get(vector_id)
        if isinstance(stored, Mapping):
            metadata = stored.get("metadata") or {}
            _check_existing_fingerprint(
                json.loads(str(metadata.get("fingerprint") or "{}")),
                str(metadata.get("digest") or ""),
                incoming,
            )
            return "verified"
        existing = [
            row
            for row in _m6_engine_rows("pinecone", cfg, target)
            if row.get("source_id")
        ]
        status = "created"
        if existing:
            backends = {
                str((row.get("metadata") or {}).get("_df_embedding_backend") or "")
                for row in existing
                if isinstance(row.get("metadata"), Mapping)
            }
            conflict = next(
                (backend for backend in sorted(backends) if backend and backend != incoming.provider),
                None,
            )
            if conflict:
                raise _legacy_backend_mismatch(conflict, incoming)
            status = "adopted_unverified"
        upserted = session.post(
            f"{base_url}/vectors/upsert",
            headers=headers,
            json={
                "namespace": namespace,
                "vectors": [
                    {
                        "id": vector_id,
                        "values": [0.0] * dimension,
                        "metadata": {
                            "target": str(target),
                            "fingerprint": _canonical_json(incoming.to_dict()),
                            "digest": incoming.digest,
                            "status": status,
                        },
                    }
                ],
            },
            timeout=30,
        )
        if upserted.status_code not in {200, 201}:
            raise RuntimeError(f"Pinecone fingerprint write failed: {upserted.status_code}")
        reread = session.get(
            f"{base_url}/vectors/fetch",
            headers=headers,
            params=fetch_params,
            timeout=30,
        )
        if reread.status_code != 200:
            raise RuntimeError("Pinecone fingerprint write could not be read back")
        props = ((reread.json() or {}).get("vectors") or {}).get(vector_id, {}).get("metadata") or {}
        _check_existing_fingerprint(
            json.loads(str(props.get("fingerprint") or "{}")),
            str(props.get("digest") or ""),
            incoming,
        )
        return status
    finally:
        session.close()


def _enforce_milvus(
    cfg: Mapping[str, Any], target: str, incoming: EmbeddingFingerprint
) -> str:
    import hashlib

    from connectors.milvus_writer import (
        _auth_token,
        _base_url,
        _ensure_collection,
        _headers,
        _milvus_with_db,
        _ok_response,
        _requests_session,
    )
    from services.vector_sync import _m6_engine_rows
    from services.value_serializer import json_dumps_exact_numbers

    sidecar = "_df_vector_collections"
    if target == sidecar:
        raise ValueError(f"{sidecar!r} is reserved for vector fingerprints")
    session = _requests_session()
    base_url = _base_url(
        str(cfg.get("host") or ""),
        int(cfg.get("port") or 19530),
        bool(cfg.get("ssl")),
        str(cfg.get("connection_string") or ""),
    )
    headers = _headers(
        _auth_token(
            api_key=str(cfg.get("api_key") or ""),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
        )
    )
    db_name = str(cfg.get("database") or "")
    vector_id = hashlib.sha256(str(target).encode("utf-8")).hexdigest()
    try:
        _ensure_collection(
            session, base_url, headers, sidecar, dimension=1, db_name=db_name
        )
        rows = _m6_engine_rows("milvus", cfg, sidecar)
        stored_row = next(
            (
                row
                for row in rows
                if str(row.get("source_id") or "") == str(target)
            ),
            None,
        )
        if stored_row:
            metadata = stored_row.get("metadata") or {}
            _check_existing_fingerprint(
                json.loads(str(metadata.get("fingerprint") or "{}")),
                str(metadata.get("digest") or ""),
                incoming,
            )
            return "verified"
        existing = [
            row
            for row in _m6_engine_rows("milvus", cfg, target)
            if row.get("source_id")
        ]
        status = "created"
        if existing:
            backends = {
                str((row.get("metadata") or {}).get("_df_embedding_backend") or "")
                for row in existing
                if isinstance(row.get("metadata"), Mapping)
            }
            conflict = next(
                (backend for backend in sorted(backends) if backend and backend != incoming.provider),
                None,
            )
            if conflict:
                raise _legacy_backend_mismatch(conflict, incoming)
            status = "adopted_unverified"
        entity = {
            "id": vector_id,
            "vector": [0.0],
            "content": "",
            "source_id": str(target)[:256],
            "chunk_index": 0,
            "filename": "",
            "page": "",
            "heading": "",
            "element_type": "",
            "metadata": {
                "target": str(target),
                "fingerprint": _canonical_json(incoming.to_dict()),
                "digest": incoming.digest,
                "status": status,
            },
        }
        payload = _milvus_with_db(
            {"collectionName": sidecar, "data": [entity]}, db_name
        )
        written = session.post(
            f"{base_url}/v2/vectordb/entities/upsert",
            data=json_dumps_exact_numbers(payload),
            headers=headers,
            timeout=30,
        )
        body = written.json() if written.content else {}
        if not _ok_response(body if isinstance(body, dict) else {}, written.status_code):
            raise RuntimeError(f"Milvus fingerprint write failed: {written.status_code}")
        reread = _m6_engine_rows("milvus", cfg, sidecar)
        stored_row = next(
            (
                row
                for row in reread
                if str(row.get("source_id") or "") == str(target)
            ),
            None,
        )
        if not stored_row:
            raise RuntimeError("Milvus fingerprint write could not be read back")
        metadata = stored_row.get("metadata") or {}
        _check_existing_fingerprint(
            json.loads(str(metadata.get("fingerprint") or "{}")),
            str(metadata.get("digest") or ""),
            incoming,
        )
        return status
    finally:
        session.close()


def enforce_fingerprint(
    engine: str,
    cfg: Mapping[str, Any],
    target: str,
    incoming: EmbeddingFingerprint,
    *,
    schema: str | None = None,
    cursor: Any = None,
    session: Any = None,
    base_url: str | None = None,
    headers: Mapping[str, str] | None = None,
) -> str:
    """Create, verify, or safely adopt the fingerprint for a vector target."""
    normalized_engine = str(engine or "").strip().lower()
    log_target = f"{schema or 'public'}.{target}" if normalized_engine == "pgvector" else target
    try:
        if normalized_engine == "pgvector":
            if cursor is None:
                raise ValueError("pgvector fingerprint enforcement requires a cursor")
            status = _enforce_pgvector(cursor, schema or "public", target, incoming)
        elif normalized_engine == "qdrant":
            owned_session = session is None
            if owned_session:
                from connectors.qdrant_writer import qdrant_rest

                session, base_url, owned_headers = qdrant_rest(dict(cfg))
                headers = headers or owned_headers
            if session is None or base_url is None or headers is None:
                raise ValueError("Qdrant fingerprint enforcement requires REST access")
            try:
                status = _enforce_qdrant(
                    session, base_url, headers, target, incoming
                )
            finally:
                if owned_session:
                    session.close()
        elif normalized_engine == "weaviate":
            status = _enforce_weaviate(cfg, target, incoming)
        elif normalized_engine == "pinecone":
            status = _enforce_pinecone(cfg, target, incoming)
        elif normalized_engine == "milvus":
            status = _enforce_milvus(cfg, target, incoming)
        else:
            raise ValueError(f"unsupported vector fingerprint engine {engine!r}")
    except VectorFingerprintMismatchError:
        logger.info(
            "Vector fingerprint target=%s status=mismatch digest=%s",
            log_target,
            incoming.digest[:12],
        )
        raise
    logger.info(
        "Vector fingerprint target=%s status=%s digest=%s",
        log_target,
        status,
        incoming.digest[:12],
    )
    if status not in _FINGERPRINT_STATUSES:
        raise RuntimeError(f"invalid vector fingerprint status {status!r}")
    return status
