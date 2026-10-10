"""Shared identity, deletion, and stale-chunk contract for vector stores."""

from __future__ import annotations

import json
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
    keep_ids = sorted({chunk_id for ids in keep.values() for chunk_id in ids})
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
        keep_ids = sorted({chunk_id for _sid, ids in batch for chunk_id in ids})
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
