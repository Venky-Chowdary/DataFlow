"""Shared identity, deletion, and stale-chunk contract for vector stores."""

from __future__ import annotations

import json
import hashlib
import logging
from collections.abc import Mapping, Sequence
from typing import Any

from connectors.table_manager import DestinationDeleteError

logger = logging.getLogger(__name__)


def stale_cleanup_skipped_docs(
    rejected_source_ids: set[str],
    rejected_details: Sequence[Mapping[str, Any]],
    *,
    has_identityless: bool = False,
) -> int:
    rejected_row_labels = {
        str(detail.get("row"))
        for detail in rejected_details
        if str(detail.get("row") or "").strip()
    }
    return max(len(rejected_source_ids), len(rejected_row_labels)) + int(
        has_identityless
    )


def stale_cleanup_meta(deleted: int, skipped: int) -> dict[str, int]:
    return {
        "stale_chunks_deleted": int(deleted),
        "vector_stale_cleanup_skipped_docs": int(skipped),
    }


def log_stale_cleanup_skipped(
    engine_label: str, target: str, skipped: int, reason: str
) -> None:
    logger.warning(
        "%s stale cleanup skipped %d document(s) for %s because %s",
        engine_label,
        skipped,
        target,
        reason,
    )


class VectorDeleteUnverifiedError(DestinationDeleteError):
    """A vector delete completed but its destination read-back was nonzero."""

    def __init__(
        self, resource: str, remaining: int, *, expected: int | None = None
    ) -> None:
        self.remaining = int(remaining)
        self.expected = None if expected is None else int(expected)
        detail = (
            f"vector delete verification expected {self.expected} matching "
            f"points or rows but found {self.remaining}"
            if self.expected is not None
            else f"vector delete verification found {self.remaining} matching "
            "points or rows still remaining"
        )
        super().__init__(
            resource,
            RuntimeError(detail),
        )


def vector_doc_key(values: Sequence[Any]) -> str:
    """Join the ordered source-PK values exactly as vector writers do."""
    return "\x1f".join(str(value) for value in values)


def vector_record_key(
    record: Mapping[str, Any], identity_columns: Sequence[str] | None = None
) -> str:
    values = [
        record[column]
        for column in (identity_columns or ())
        if record.get(column) is not None and str(record[column]).strip()
    ]
    key = vector_doc_key(values)
    return key or str(
        record.get("id", record.get("_id", record.get("source_id", ""))) or ""
    )


def vector_chunk_identity(source_id: Any, chunk_index: Any) -> str:
    """Return a stable, engine-neutral id for one document chunk."""
    try:
        index = int(chunk_index)
    except (TypeError, ValueError):
        raise ValueError("vector chunk index must be an integer") from None
    source = str(source_id or "")
    if not source:
        return ""
    return hashlib.sha256(f"{source}\x1f{index}".encode("utf-8")).hexdigest()


def vector_document_hash(text: str, fingerprint_digest: str) -> str:
    return hashlib.sha256(
        f"{text}\x1f{fingerprint_digest}".encode("utf-8")
    ).hexdigest()


def vector_metadata_hash(metadata: Mapping[str, Any]) -> str:
    from services.value_serializer import sanitize_json_value

    safe = {
        key: value
        for key, value in metadata.items()
        if not str(key).startswith("_df_")
    }
    encoded = json.dumps(
        sanitize_json_value(safe),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        allow_nan=False,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def summarize_vector_document_rows(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        source_id = str(row.get("source_id") or "")
        if source_id:
            grouped.setdefault(source_id, []).append(row)
    summaries: dict[str, dict[str, Any]] = {}
    for source_id, source_rows in grouped.items():
        metadata_rows = []
        ids: set[str] = set()
        for row in source_rows:
            metadata = row.get("metadata")
            if not isinstance(metadata, Mapping):
                metadata = row
            metadata_rows.append(metadata)
            if row.get("id") is not None:
                ids.add(str(row["id"]))
        hashes = {str(meta.get("_df_doc_hash") or "") for meta in metadata_rows}
        counts = {meta.get("_df_chunk_count") for meta in metadata_rows}
        metadata_hashes = {str(meta.get("_df_metadata_hash") or "") for meta in metadata_rows}
        summaries[source_id] = {
            "doc_hash": next(iter(hashes)) if len(hashes) == 1 else "",
            "chunk_count": next(iter(counts)) if len(counts) == 1 else None,
            "chunk_ids": ids,
            "metadata_hash": (
                next(iter(metadata_hashes)) if len(metadata_hashes) == 1 else ""
            ),
            "actual_chunk_count": len(ids),
        }
    return summaries


def vector_document_is_unchanged(
    existing: Mapping[str, Any] | None,
    *,
    doc_hash: str,
    chunk_count: int,
    metadata_hash: str,
    enabled: bool = True,
) -> bool:
    """Skip only content/fingerprint/count matches with identical stored metadata.

    Metadata columns are part of the equality check, so changing configured
    metadata re-upserts even when the rendered document text is unchanged.
    """
    if not enabled or not existing or not doc_hash:
        return False
    try:
        stored_count = int(existing.get("chunk_count"))
        actual_count = int(existing.get("actual_chunk_count"))
    except (TypeError, ValueError):
        return False
    return (
        str(existing.get("doc_hash") or "") == doc_hash
        and stored_count == int(chunk_count)
        and actual_count == stored_count
        and str(existing.get("metadata_hash") or "") == metadata_hash
    )


def stamp_vector_document_metadata(
    rows: Sequence[dict[str, Any]], fingerprint_digest: str
) -> None:
    for row in rows:
        if row.get("_df_unchanged_skipped") or row.get("_df_embed_error"):
            continue
        text = str(row.pop("_df_document_text", row.get("content") or ""))
        try:
            chunk_count = int(row.pop("_df_chunk_count", 1))
        except (TypeError, ValueError):
            chunk_count = 1
        metadata_hash = str(row.pop("_df_metadata_hash", "") or "")
        metadata = dict(row.get("metadata") or {})
        doc_hash = vector_document_hash(text, fingerprint_digest)
        if not metadata_hash:
            metadata_hash = vector_metadata_hash(metadata)
        metadata["_df_doc_hash"] = doc_hash
        metadata["_df_chunk_count"] = chunk_count
        metadata["_df_metadata_hash"] = metadata_hash
        row["metadata"] = metadata


def pgvector_read_document_states(
    cursor: Any, schema: str, table: str, source_ids: Sequence[str]
) -> dict[str, dict[str, Any]]:
    if not source_ids:
        return {}
    from psycopg2 import sql

    relation = sql.SQL("{}.{}").format(
        sql.Identifier(schema or "public"), sql.Identifier(table)
    )
    try:
        cursor.execute(
            sql.SQL(
                "SELECT source_id, id, metadata FROM {} "
                "WHERE source_id = ANY(%s)"
            ).format(relation),
            (list(source_ids),),
        )
        rows = []
        for source_id, row_id, metadata in cursor.fetchall():
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except ValueError:
                    metadata = {}
            rows.append(
                {
                    "source_id": source_id,
                    "id": row_id,
                    "metadata": metadata if isinstance(metadata, Mapping) else {},
                }
            )
    except Exception as exc:
        if _pgvector_missing_table(exc):
            return {}
        raise
    return summarize_vector_document_rows(rows)


def qdrant_read_document_states(
    cfg: Mapping[str, Any], collection: str, source_ids: Sequence[str]
) -> dict[str, dict[str, Any]]:
    if not source_ids:
        return {}
    from connectors.qdrant_writer import qdrant_rest
    from services.value_serializer import json_default

    session, base_url, headers = qdrant_rest(dict(cfg))
    try:
        rows: list[dict[str, Any]] = []
        offset: Any = None
        while True:
            body: dict[str, Any] = {
                "filter": _qdrant_filter(source_ids),
                "limit": 256,
                "with_payload": True,
                "with_vector": False,
            }
            if offset is not None:
                body["offset"] = offset
            response = session.post(
                f"{base_url}/collections/{collection}/points/scroll",
                data=json.dumps(body, default=json_default),
                headers=headers,
                timeout=30,
            )
            if response.status_code == 404:
                return {}
            if response.status_code != 200:
                raise RuntimeError(
                    f"Qdrant vector document scroll failed: {response.status_code}"
                )
            result = (response.json() or {}).get("result") or {}
            points = result.get("points") or []
            for point in points:
                payload = point.get("payload") or {}
                rows.append(
                    {
                        "source_id": payload.get("source_id"),
                        "id": point.get("id"),
                        "metadata": payload,
                    }
                )
            offset = result.get("next_page_offset")
            if not points or offset is None:
                break
        return summarize_vector_document_rows(rows)
    finally:
        session.close()


def vector_dimension_hint(
    model: str | None,
    extra: Mapping[str, Any],
    *,
    records: Sequence[Mapping[str, Any]] = (),
    embedding_column: str | None = None,
) -> int | None:
    configured = extra.get("embedding_dimensions")
    if configured is not None:
        try:
            value = int(configured)
            return value if value > 0 else None
        except (TypeError, ValueError):
            return None
    if embedding_column:
        from services.vector_embedding import coerce_embedding

        for record in records:
            vector, error = coerce_embedding(record.get(embedding_column))
            if not error and vector:
                return len(vector)
    from services.brand_env import getenv_brand

    resolved = str(
        model
        or getenv_brand(
            "EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2"
        )
    ).strip()
    if resolved.startswith(("hash/", "deterministic/")):
        suffix = resolved.split("/", 1)[1]
        if suffix.isdigit():
            return max(8, int(suffix))
    name = resolved.split("/", 1)[-1]
    return {
        "text-embedding-3-small": 1536,
        "text-embedding-3-large": 3072,
        "text-embedding-ada-002": 1536,
    }.get(name)


def _rejected_doc_keys(
    headers: Sequence[str],
    data_rows: Sequence[Sequence[Any]],
    pk_columns: Sequence[str] | None,
    mappings: Sequence[Mapping[str, Any]] | None,
    rejected: Sequence[Mapping[str, Any]],
) -> set[str]:
    """Recover document keys for rejected input rows when row numbers are known."""
    if not pk_columns or not rejected:
        return set()
    header_indexes = {str(header): index for index, header in enumerate(headers)}
    source_by_target = {
        str(item.get("target") or "").strip(): str(item.get("source") or "").strip()
        for item in (mappings or [])
        if str(item.get("target") or "").strip()
    }
    keys: set[str] = set()
    for detail in rejected:
        try:
            row_number = int(detail.get("row"))
        except (TypeError, ValueError):
            continue
        row_index = row_number - 1
        if row_index < 0 or row_index >= len(data_rows):
            continue
        row = data_rows[row_index]
        values: list[Any] = []
        for column in pk_columns:
            name = str(column)
            source = source_by_target.get(name)
            header = name if name in header_indexes or not source else source
            index = header_indexes.get(header)
            value = row[index] if index is not None and index < len(row) else None
            if value is not None and str(value).strip():
                values.append(value)
        key = vector_doc_key(values)
        if key:
            keys.add(key)
    return keys


def _pgvector_connection(cfg: Mapping[str, Any]) -> Any:
    from connectors.postgresql_conn import get_connection

    return get_connection(
        host=str(cfg.get("host") or "localhost"),
        port=int(cfg.get("port") or 5432),
        database=str(cfg.get("database") or cfg.get("dbname") or ""),
        username=str(cfg.get("username") or cfg.get("user") or ""),
        password=str(cfg.get("password") or ""),
        connection_string=str(cfg.get("connection_string") or ""),
        ssl=bool(cfg.get("ssl", False)),
    )


def _pgvector_missing_table(exc: BaseException) -> bool:
    return getattr(exc, "pgcode", None) == "42P01"


def _qdrant_filter(doc_keys: Sequence[str]) -> dict[str, Any]:
    return {
        "must": [
            {
                "key": "source_id",
                "match": {"any": list(doc_keys)},
            }
        ]
    }


def _ensure_qdrant_source_id_index(
    session: Any,
    base_url: str,
    headers: dict[str, str],
    collection: str,
) -> None:
    from services.value_serializer import json_default

    resp = session.put(
        f"{base_url}/collections/{collection}/index",
        data=json.dumps(
            {"field_name": "source_id", "field_schema": "keyword"},
            default=json_default,
        ),
        headers=headers,
        timeout=30,
    )
    if resp.status_code not in {200, 201}:
        raise RuntimeError(
            f"Qdrant source_id keyword index failed: "
            f"{resp.status_code} {resp.text[:300]}"
        )


def _qdrant_count(
    session: Any,
    base_url: str,
    headers: dict[str, str],
    collection: str,
    doc_keys: Sequence[str],
) -> int:
    from services.value_serializer import json_default

    resp = session.post(
        f"{base_url}/collections/{collection}/points/count",
        data=json.dumps(
            {"filter": _qdrant_filter(doc_keys), "exact": True},
            default=json_default,
        ),
        headers=headers,
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(
            f"Qdrant exact source_id count failed: "
            f"{resp.status_code} {resp.text[:300]}"
        )
    body = resp.json() if resp.content else {}
    result = body.get("result") if isinstance(body, dict) else None
    try:
        count = int(result.get("count"))
    except (AttributeError, TypeError, ValueError) as exc:
        raise RuntimeError("Qdrant exact source_id count response was invalid") from exc
    if count < 0:
        raise RuntimeError("Qdrant exact source_id count was negative")
    return count


def _pgvector_count_remaining(cur: Any, relation: Any, keys: Sequence[str]) -> int:
    from psycopg2 import sql

    cur.execute(
        sql.SQL("SELECT count(*) FROM {} WHERE source_id = ANY(%s)").format(
            relation
        ),
        (list(keys),),
    )
    return int(cur.fetchone()[0])


def pgvector_delete_doc_keys(
    cfg: Mapping[str, Any],
    table: str,
    schema: str | None,
    doc_keys: Sequence[str],
) -> int:
    """Delete pgvector chunks by document key and verify in the same transaction."""
    keys = list(dict.fromkeys(str(key) for key in doc_keys if str(key)))
    name = str(table or "").strip()
    if not keys or not name:
        return 0
    schema_name = str(schema or cfg.get("schema") or "public")
    conn = _pgvector_connection(cfg)
    try:
        with conn.cursor() as cur:
            from psycopg2 import sql

            relation = sql.SQL("{}.{}").format(
                sql.Identifier(schema_name), sql.Identifier(name)
            )
            predicate = sql.SQL("source_id = ANY(%s)")
            try:
                cur.execute(
                    sql.SQL("DELETE FROM {} WHERE ").format(relation) + predicate,
                    (keys,),
                )
                deleted = max(int(cur.rowcount or 0), 0)
                remaining = _pgvector_count_remaining(cur, relation, keys)
                if remaining:
                    conn.rollback()
                    raise VectorDeleteUnverifiedError(name, remaining)
            except VectorDeleteUnverifiedError:
                raise
            except Exception as exc:
                if _pgvector_missing_table(exc):
                    conn.rollback()
                    return 0
                raise
        conn.commit()
        return deleted
    except DestinationDeleteError:
        raise
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            logger.warning("pgvector delete rollback failed for %s", name)
        raise DestinationDeleteError(name, exc) from exc
    finally:
        conn.close()


def qdrant_delete_doc_keys(
    cfg: Mapping[str, Any],
    collection: str,
    doc_keys: Sequence[str],
) -> int:
    """Delete Qdrant chunks by payload document key and verify the exact count."""
    from connectors.qdrant_writer import qdrant_rest
    from services.value_serializer import json_default

    keys = list(dict.fromkeys(str(key) for key in doc_keys if str(key)))
    name = str(collection or "").strip()
    if not keys or not name:
        return 0
    session, base_url, headers = qdrant_rest(cfg)
    try:
        exists = session.get(
            f"{base_url}/collections/{name}", headers=headers, timeout=10
        )
        if exists.status_code == 404:
            return 0
        if exists.status_code != 200:
            raise RuntimeError(
                f"Qdrant collection probe failed: "
                f"{exists.status_code} {exists.text[:300]}"
            )
        _ensure_qdrant_source_id_index(session, base_url, headers, name)
        total_deleted = 0
        for offset in range(0, len(keys), 256):
            batch = keys[offset : offset + 256]
            filt = _qdrant_filter(batch)
            before = _qdrant_count(session, base_url, headers, name, batch)
            resp = session.post(
                f"{base_url}/collections/{name}/points/delete?wait=true",
                data=json.dumps({"filter": filt}, default=json_default),
                headers=headers,
                timeout=30,
            )
            if resp.status_code not in {200, 201}:
                raise RuntimeError(
                    f"Qdrant source_id DELETE failed: "
                    f"{resp.status_code} {resp.text[:300]}"
                )
            remaining = _qdrant_count(session, base_url, headers, name, batch)
            if remaining:
                raise VectorDeleteUnverifiedError(name, remaining)
            total_deleted += before
        return total_deleted
    except DestinationDeleteError:
        raise
    except Exception as exc:
        raise DestinationDeleteError(name, exc) from exc
    finally:
        session.close()


def pgvector_delete_stale_chunks(
    cur: Any,
    schema: str,
    table: str,
    keep: dict[str, set[str]],
) -> int:
    """Remove outdated chunks for successfully written document identities."""
    if not keep:
        return 0
    from psycopg2 import sql

    source_ids = list(keep)
    keep_ids = sorted(
        {chunk_id for ids in keep.values() for chunk_id in ids}, key=str
    )
    relation = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))
    cur.execute(
        sql.SQL(
            "DELETE FROM {} WHERE source_id = ANY(%s) "
            "AND NOT (id = ANY(%s))"
        ).format(relation),
        (source_ids, keep_ids),
    )
    return max(int(cur.rowcount or 0), 0)


def qdrant_delete_stale_chunks(
    session: Any,
    base_url: str,
    headers: dict[str, str],
    collection: str,
    keep: dict[str, set[str]],
) -> int:
    """Remove outdated Qdrant chunks and verify kept identities exactly."""
    from services.value_serializer import json_default

    if not keep:
        return 0
    _ensure_qdrant_source_id_index(session, base_url, headers, collection)
    deleted = 0
    entries = list(keep.items())
    for offset in range(0, len(entries), 256):
        batch = entries[offset : offset + 256]
        source_ids = [sid for sid, _ids in batch]
        keep_ids = sorted(
            {chunk_id for _sid, ids in batch for chunk_id in ids}, key=str
        )
        filt: dict[str, Any] = {
            "must": [
                {
                    "key": "source_id",
                    "match": {"any": source_ids},
                }
            ]
        }
        if keep_ids:
            filt["must_not"] = [{"has_id": keep_ids}]
        before = _qdrant_count(
            session, base_url, headers, collection, source_ids
        )
        resp = session.post(
            f"{base_url}/collections/{collection}/points/delete?wait=true",
            data=json.dumps({"filter": filt}, default=json_default),
            headers=headers,
            timeout=30,
        )
        if resp.status_code not in {200, 201}:
            raise RuntimeError(
                f"Qdrant stale chunk DELETE failed: "
                f"{resp.status_code} {resp.text[:300]}"
            )
        remaining = _qdrant_count(
            session, base_url, headers, collection, source_ids
        )
        expected = sum(len(ids) for _sid, ids in batch)
        if remaining != expected:
            raise VectorDeleteUnverifiedError(
                collection,
                remaining,
                expected=expected,
            )
        deleted += max(before - remaining, 0)
    return deleted


def _m6_engine_rows(
    engine: str,
    cfg: Mapping[str, Any],
    target: str,
    source_ids: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    """Read identity and contract metadata from an M6 vector target."""
    rows: list[dict[str, Any]] = []
    if engine == "weaviate":
        import json

        from connectors.weaviate_writer import (
            _base_url,
            _headers,
            _requests_session,
            iter_weaviate_objects_after,
        )

        session = _requests_session()
        base_url = _base_url(
            str(cfg.get("host") or ""),
            int(cfg.get("port") or 8080),
            bool(cfg.get("ssl")),
            str(cfg.get("connection_string") or ""),
        )
        headers = _headers(str(cfg.get("api_key") or ""))
        try:
            probe = session.get(
                f"{base_url}/v1/schema/{target}", headers=headers, timeout=15
            )
            if probe.status_code == 404:
                return rows
            if probe.status_code != 200:
                raise RuntimeError(
                    f"Weaviate class probe failed: {probe.status_code}"
                )
            if source_ids:
                schema = probe.json() if probe.content else {}
                available_fields = {
                    str(prop.get("name") or "")
                    for prop in schema.get("properties") or []
                    if isinstance(prop, Mapping)
                }
                selected_fields = [
                    name
                    for name in (
                        "source_id",
                        "chunk_index",
                        "_df_doc_hash",
                        "_df_chunk_count",
                        "_df_metadata_hash",
                    )
                    if name in available_fields
                ]
                if "source_id" not in selected_fields:
                    return rows
                property_selection = " ".join(selected_fields)
                for offset in range(0, len(source_ids), 100):
                    batch = [str(value) for value in source_ids[offset : offset + 100]]
                    clauses = [
                        "{ path: [\"source_id\"], operator: Equal, valueText: "
                        f"{json.dumps(source_id)} }}"
                        for source_id in batch
                    ]
                    where = clauses[0] if len(clauses) == 1 else (
                        "{ operator: Or, operands: [" + ", ".join(clauses) + "] }"
                    )
                    query = (
                        f"{{ Get {{ {target}(where: {where}) "
                        f"{{ {property_selection} _additional {{ id }} }} }} }}"
                    )
                    response = session.post(
                        f"{base_url}/v1/graphql",
                        json={"query": query},
                        headers=headers,
                        timeout=30,
                    )
                    if response.status_code != 200:
                        raise RuntimeError(
                            f"Weaviate document-state query failed: {response.status_code}"
                        )
                    body = response.json() if response.content else {}
                    if body.get("errors"):
                        raise RuntimeError("Weaviate document-state query was rejected")
                    data = ((body.get("data") or {}).get("Get") or {}).get(target) or []
                    for obj in data:
                        additional = obj.get("_additional") or {}
                        metadata = {
                            key: obj.get(key)
                            for key in selected_fields
                        }
                        rows.append(
                            {
                                "id": str(additional.get("id") or ""),
                                "source_id": obj.get("source_id"),
                                "chunk_index": obj.get("chunk_index"),
                                "metadata": metadata,
                            }
                        )
                return rows
            for page in iter_weaviate_objects_after(
                session=session,
                base_url=base_url,
                headers=headers,
                class_name=target,
                include="",
            ):
                for obj in page:
                    props = obj.get("properties")
                    if isinstance(props, Mapping):
                        rows.append(
                            {
                                "id": str(obj.get("id") or ""),
                                "source_id": props.get("source_id"),
                                "chunk_index": props.get("chunk_index"),
                                "metadata": dict(props),
                            }
                        )
        finally:
            session.close()
        return rows
    if engine == "pinecone":
        import json

        from connectors.pinecone_writer import (
            _headers,
            _index_url,
            _pinecone_vector_id,
            _requests_session,
        )

        session = _requests_session()
        base_url = _index_url(
            str(cfg.get("host") or ""), str(cfg.get("connection_string") or "")
        )
        headers = _headers(
            str(cfg.get("api_key") or cfg.get("password") or cfg.get("username") or "")
        )
        namespace = str(target or cfg.get("schema") or "").strip()
        ids: list[str] = []
        token = ""
        try:
            while True:
                params: dict[str, Any] = {"limit": 99}
                if namespace:
                    params["namespace"] = namespace
                if token:
                    params["paginationToken"] = token
                listed = session.get(
                    f"{base_url}/vectors/list",
                    headers=headers,
                    params=params,
                    timeout=30,
                )
                if listed.status_code == 404:
                    return rows
                if listed.status_code != 200:
                    raise RuntimeError(
                        f"Pinecone vector list failed: {listed.status_code}"
                    )
                body = listed.json() if listed.content else {}
                for entry in body.get("vectors") or []:
                    vector_id = _pinecone_vector_id(entry)
                    if vector_id:
                        ids.append(vector_id)
                pagination = body.get("pagination")
                token = str(
                    pagination.get("next") or ""
                    if isinstance(pagination, Mapping)
                    else ""
                )
                if not token:
                    break
            for offset in range(0, len(ids), 100):
                batch = ids[offset : offset + 100]
                fetch_params = [("ids", vector_id) for vector_id in batch]
                if namespace:
                    fetch_params.append(("namespace", namespace))
                fetched = session.get(
                    f"{base_url}/vectors/fetch",
                    params=fetch_params,
                    headers=headers,
                    timeout=60,
                )
                if fetched.status_code != 200:
                    raise RuntimeError(
                        f"Pinecone vector fetch failed: {fetched.status_code}"
                    )
                vector_map = fetched.json().get("vectors") or {}
                for vector_id, vector in vector_map.items():
                    metadata = vector.get("metadata") or {}
                    rows.append(
                        {
                            "id": str(vector_id),
                            "source_id": metadata.get("source_id"),
                            "chunk_index": metadata.get("chunk_index"),
                            "metadata": metadata,
                        }
                    )
        finally:
            session.close()
        return rows
    if engine == "milvus":
        import json

        from connectors.milvus_writer import (
            _auth_token,
            _base_url,
            _milvus_describe_data,
            _requests_session,
            _headers,
            iter_milvus_query_pages,
            milvus_pk_info_from_describe_data,
        )

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
        try:
            described = _milvus_describe_data(
                session, base_url, headers, target, db_name
            )
            if not described:
                return rows
            pk_name, pk_type = milvus_pk_info_from_describe_data(described)
            fields = {
                str(field.get("fieldName") or field.get("name") or "")
                for field in described.get("fields") or []
                if isinstance(field, Mapping)
            }
            output_fields = [
                name
                for name in ("id", "source_id", "chunk_index", "metadata")
                if name in fields
            ]
            if source_ids and "source_id" not in fields:
                return rows
            if "id" not in output_fields:
                output_fields.insert(0, pk_name)
            from connectors.milvus_writer import milvus_quote_pk_expr

            source_batches = (
                [source_ids[offset : offset + 100] for offset in range(0, len(source_ids), 100)]
                if source_ids
                else [None]
            )
            for source_batch in source_batches:
                filter_expr = None
                if source_batch:
                    filter_expr = "source_id in [" + ",".join(
                        milvus_quote_pk_expr(str(value), integer=False)
                        for value in source_batch
                    ) + "]"
                for page in iter_milvus_query_pages(
                    session=session,
                    base_url=base_url,
                    headers=headers,
                    collection=target,
                    db_name=db_name,
                    pk_name=pk_name,
                    pk_type=pk_type,
                    output_fields=output_fields,
                    filter_expr=filter_expr,
                ):
                    for entity in page:
                        metadata = entity.get("metadata")
                        if isinstance(metadata, str):
                            try:
                                metadata = json.loads(metadata)
                            except (TypeError, ValueError):
                                metadata = {}
                        if not isinstance(metadata, Mapping):
                            metadata = {}
                        rows.append(
                            {
                                "id": str(entity.get(pk_name, entity.get("id", ""))),
                                "source_id": entity.get("source_id"),
                                "chunk_index": entity.get("chunk_index"),
                                "metadata": dict(metadata),
                            }
                        )
        finally:
            session.close()
        return rows
    raise ValueError(f"unsupported vector sync engine {engine!r}")


def vector_engine_read_document_states(
    engine: str,
    cfg: Mapping[str, Any],
    target: str,
    source_ids: Sequence[str],
) -> dict[str, dict[str, Any]]:
    wanted = {str(value) for value in source_ids if str(value)}
    if not wanted:
        return {}
    rows = _m6_engine_rows(engine, cfg, target, sorted(wanted))
    return summarize_vector_document_rows(rows)


def _m6_delete_ids(
    engine: str, cfg: Mapping[str, Any], target: str, ids: Sequence[str]
) -> int:
    values = list(dict.fromkeys(str(value) for value in ids if str(value)))
    if not values:
        return 0
    if engine == "weaviate":
        from connectors.weaviate_writer import _base_url, _headers, _requests_session

        session = _requests_session()
        base_url = _base_url(
            str(cfg.get("host") or ""),
            int(cfg.get("port") or 8080),
            bool(cfg.get("ssl")),
            str(cfg.get("connection_string") or ""),
        )
        headers = _headers(str(cfg.get("api_key") or ""))
        deleted = 0
        try:
            for object_id in values:
                response = session.delete(
                    f"{base_url}/v1/objects/{target}/{object_id}",
                    headers=headers,
                    timeout=30,
                )
                if response.status_code not in {200, 204, 404}:
                    raise RuntimeError(
                        f"Weaviate object delete failed: {response.status_code}"
                    )
                deleted += int(response.status_code != 404)
        finally:
            session.close()
        return deleted
    if engine == "pinecone":
        import json

        from connectors.pinecone_writer import _headers, _index_url, _requests_session

        session = _requests_session()
        base_url = _index_url(
            str(cfg.get("host") or ""), str(cfg.get("connection_string") or "")
        )
        headers = _headers(
            str(cfg.get("api_key") or cfg.get("password") or cfg.get("username") or "")
        )
        namespace = str(target or cfg.get("schema") or "").strip()
        try:
            for offset in range(0, len(values), 1000):
                payload: dict[str, Any] = {"ids": values[offset : offset + 1000]}
                if namespace:
                    payload["namespace"] = namespace
                response = session.post(
                    f"{base_url}/vectors/delete",
                    data=json.dumps(payload),
                    headers=headers,
                    timeout=60,
                )
                if response.status_code not in {200, 202}:
                    raise RuntimeError(
                        f"Pinecone vector delete failed: {response.status_code}"
                    )
        finally:
            session.close()
        return len(values)
    if engine == "milvus":
        from connectors.milvus_writer import (
            _auth_token,
            _base_url,
            _headers,
            _milvus_with_db,
            _ok_response,
            _requests_session,
            milvus_quote_pk_expr,
        )

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
        filter_expr = "id in [" + ",".join(
            milvus_quote_pk_expr(value, integer=False) for value in values
        ) + "]"
        payload = _milvus_with_db(
            {"collectionName": target, "filter": filter_expr},
            str(cfg.get("database") or ""),
        )
        try:
            response = session.post(
                f"{base_url}/v2/vectordb/entities/delete",
                json=payload,
                headers=headers,
                timeout=60,
            )
            body = response.json() if response.content else {}
            if not _ok_response(body, response.status_code):
                raise RuntimeError(
                    f"Milvus entity delete failed: {response.status_code}"
                )
        finally:
            session.close()
        return len(values)
    raise ValueError(f"unsupported vector sync engine {engine!r}")


def vector_engine_delete_doc_keys(
    engine: str,
    cfg: Mapping[str, Any],
    target: str,
    doc_keys: Sequence[str],
) -> int:
    keys = list(dict.fromkeys(str(value) for value in doc_keys if str(value)))
    if not keys:
        return 0
    try:
        states = vector_engine_read_document_states(engine, cfg, target, keys)
        ids = sorted(
            {
                str(vector_id)
                for state in states.values()
                for vector_id in state.get("chunk_ids", set())
            }
        )
        deleted = _m6_delete_ids(engine, cfg, target, ids)
        remaining = vector_engine_read_document_states(engine, cfg, target, keys)
        count = sum(int(state.get("actual_chunk_count") or 0) for state in remaining.values())
        if count:
            raise VectorDeleteUnverifiedError(target, count)
        return deleted
    except DestinationDeleteError:
        raise
    except Exception as exc:
        raise DestinationDeleteError(target, exc) from exc


def vector_engine_delete_stale_chunks(
    engine: str,
    cfg: Mapping[str, Any],
    target: str,
    keep: Mapping[str, set[str]],
) -> int:
    if not keep:
        return 0
    keys = list(keep)
    try:
        before = vector_engine_read_document_states(engine, cfg, target, keys)
        stale_ids = sorted(
            {
                str(vector_id)
                for source_id, state in before.items()
                for vector_id in state.get("chunk_ids", set()) - set(keep.get(source_id, set()))
            }
        )
        deleted = _m6_delete_ids(engine, cfg, target, stale_ids)
        after = vector_engine_read_document_states(engine, cfg, target, keys)
        for source_id in keys:
            actual = set((after.get(source_id) or {}).get("chunk_ids") or set())
            expected = set(keep.get(source_id) or set())
            if actual != expected:
                raise VectorDeleteUnverifiedError(
                    target,
                    len(actual),
                    expected=len(expected),
                )
        return deleted
    except VectorDeleteUnverifiedError:
        raise
    except Exception as exc:
        raise RuntimeError(
            f"{engine} stale chunk cleanup failed ({type(exc).__name__})"
        ) from exc


def prepare_vector_sync_write(
    engine: str,
    cfg: Mapping[str, Any],
    target: str,
    records: Sequence[Mapping[str, Any]],
    *,
    identity_columns: Sequence[str] | None,
    model: str | None,
    content_column: str | None,
    embedding_column: str | None,
    metadata_columns: Sequence[str] | None,
    exclude_pii_columns: Sequence[str] | None,
    chunk_size: int,
    chunk_overlap: int,
    skip_chunking: bool,
    durable_embedding_cache: bool | None,
    options: Mapping[str, Any],
    usage: Any = None,
    vectorizer: Any = None,
) -> dict[str, Any]:
    """Read document state, skip unchanged inputs, and embed through M1–M5 APIs."""
    from services.embedding_providers import create_embedding_usage, provider_extra_from_options
    from services.vectorization import vectorize_records

    embedding_extra = provider_extra_from_options(dict(options))
    usage = usage or create_embedding_usage(model, embedding_extra, embedding_column)
    enabled = str(options.get("vector_skip_unchanged", True)).strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    dimension = vector_dimension_hint(
        model, embedding_extra, records=records, embedding_column=embedding_column
    )
    fingerprint = None
    states: dict[str, dict[str, Any]] = {}
    source_ids = sorted(
        {
            vector_record_key(record, identity_columns)
            for record in records
            if vector_record_key(record, identity_columns)
        }
    )
    if dimension:
        from services.vector_fingerprint import fingerprint_for_write

        fingerprint = fingerprint_for_write(
            model=model,
            dimension=int(dimension),
            distance=str(options.get("distance") or "cosine"),
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            skip_chunking=skip_chunking,
            embedding_column=embedding_column,
            chunk_strategy=str(options.get("chunk_strategy") or "recursive"),
            chunk_unit=str(options.get("chunk_unit") or "chars"),
            chunk_tokenizer=(
                options.get("chunk_tokenizer")
                if isinstance(options.get("chunk_tokenizer"), str)
                else None
            ),
            text_template=(
                options.get("text_template")
                if isinstance(options.get("text_template"), str)
                else None
            ),
        )
    if enabled and source_ids:
        states = vector_engine_read_document_states(engine, cfg, target, source_ids)
    vector_rows = (vectorizer or vectorize_records)(
        records,
        content_column=content_column,
        embedding_column=embedding_column,
        metadata_columns=list(metadata_columns or []),
        exclude_pii_columns=list(exclude_pii_columns or []),
        model=model,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        skip_chunking=skip_chunking,
        chunk_strategy=str(options.get("chunk_strategy") or "recursive"),
        chunk_unit=str(options.get("chunk_unit") or "chars"),
        chunk_tokenizer=options.get("chunk_tokenizer"),
        text_template=options.get("text_template"),
        durable_embedding_cache=durable_embedding_cache,
        identity_columns=list(identity_columns or ()),
        usage=usage,
        embedding_extra=embedding_extra,
        existing_vector_docs=states,
        doc_fingerprint_digest=(fingerprint.digest if fingerprint else None),
        skip_unchanged=enabled and fingerprint is not None,
    )
    if fingerprint is None:
        inferred_dimension = next(
            (
                len(vector)
                for row in vector_rows
                if isinstance((vector := row.get("embedding")), (list, tuple))
                and vector
            ),
            None,
        )
        if inferred_dimension:
            from services.vector_fingerprint import fingerprint_for_write

            fingerprint = fingerprint_for_write(
                model=model,
                dimension=inferred_dimension,
                distance=str(options.get("distance") or "cosine"),
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                skip_chunking=skip_chunking,
                embedding_column=embedding_column,
                chunk_strategy=str(options.get("chunk_strategy") or "recursive"),
                chunk_unit=str(options.get("chunk_unit") or "chars"),
                chunk_tokenizer=(
                    options.get("chunk_tokenizer")
                    if isinstance(options.get("chunk_tokenizer"), str)
                    else None
                ),
                text_template=(
                    options.get("text_template")
                    if isinstance(options.get("text_template"), str)
                    else None
                ),
            )
            dimension = inferred_dimension
    if fingerprint is not None:
        stamp_vector_document_metadata(vector_rows, fingerprint.digest)
    skipped_ids = {
        str(row.get("source_id") or "")
        for row in vector_rows
        if row.get("_df_unchanged_skipped") and row.get("source_id")
    }
    output_rows = [
        row for row in vector_rows if not row.get("_df_unchanged_skipped")
    ]
    return {
        "rows": output_rows,
        "usage": usage,
        "fingerprint": fingerprint,
        "dimension_hint": dimension,
        "unchanged_skipped": len(skipped_ids),
        "unchanged_source_ids": skipped_ids,
        "embedded_docs": len(
            {
                str(row.get("source_id") or "")
                for row in output_rows
                if row.get("source_id")
            }
        ),
        "enabled": enabled,
    }
