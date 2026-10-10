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
