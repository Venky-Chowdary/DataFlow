from __future__ import annotations

from collections.abc import Iterator
from itertools import chain
from typing import Any
from services.reconciliation import (
    _ES_RECONCILE_PAGE,
    canonical_checksum_from_iter,
    json,
    load_http_json,
    logger,
)

def verify_weaviate_class(
    *,
    host: str = "",
    port: int = 8080,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    ssl: bool = False,
    api_key: str = "",
    class_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
) -> tuple[int, str]:
    """Independent Weaviate aggregate + object list for Gate-8."""
    try:
        from connectors.weaviate_writer import (
            _base_url,
            _class_name,
            _headers,
            _requests_session,
        )

        key = api_key or password or username or ""
        base_url = _base_url(host, port, ssl, connection_string)
        cls = _class_name(class_name)
        session = _requests_session()
        hdrs = _headers(key)
        id_cap = int(limit) if limit and int(limit) > 0 else 500
        ids = [str(x) for x in (written_ids or []) if x is not None][:id_cap]
        dict_rows: list[dict[str, Any]] = []
        if ids:
            for oid in ids:
                resp = session.get(
                    f"{base_url}/v1/objects/{cls}/{oid}",
                    headers=hdrs,
                    timeout=15,
                )
                if resp.status_code not in {200, 201}:
                    # Older Weaviate: /v1/objects/{uuid}
                    resp = session.get(
                        f"{base_url}/v1/objects/{oid}",
                        headers=hdrs,
                        timeout=15,
                    )
                if resp.status_code not in {200, 201}:
                    continue
                obj = load_http_json(resp) or {}
                if not isinstance(obj, dict):
                    continue
                props = obj.get("properties") if isinstance(obj.get("properties"), dict) else {}
                dict_rows.append({"id": obj.get("id") or oid, **props})
        if not dict_rows:
            agg = session.get(
                f"{base_url}/v1/objects",
                params={"class": cls, "limit": max(1, int(limit or 100) or 100)},
                headers=hdrs,
                timeout=30,
            )
            if agg.status_code not in {200, 201}:
                return -1, ""
            body = load_http_json(agg) or {}
            objects = body.get("objects") or []
            count = int(body.get("totalResults") or len(objects))
            for obj in objects:
                if not isinstance(obj, dict):
                    continue
                props = obj.get("properties") if isinstance(obj.get("properties"), dict) else {}
                dict_rows.append({"id": obj.get("id"), **props})
        else:
            count = len(dict_rows)
            # Prefer class cardinality when list endpoint works.
            try:
                agg = session.get(
                    f"{base_url}/v1/objects",
                    params={"class": cls, "limit": 1},
                    headers=hdrs,
                    timeout=15,
                )
                if agg.status_code in {200, 201}:
                    count = int((agg.json() or {}).get("totalResults") or count)
            except Exception:
                pass
        columns = target_columns or (
            sorted({k for r in dict_rows for k in r}) if dict_rows else ["id"]
        )
        return count, canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="weaviate",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Weaviate reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_milvus_collection(
    *,
    host: str = "",
    port: int = 19530,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    ssl: bool = False,
    api_key: str = "",
    database: str = "",
    collection: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
) -> tuple[int, str]:
    """Independent Milvus get_collection_stats + query for Gate-8."""
    try:
        from connectors.milvus_writer import (
            _auth_token,
            _base_url,
            _collection_name,
            _headers,
            _ok_response,
            _requests_session,
        )

        coll = _collection_name(collection)
        db_name = (database or "").strip()
        if db_name.lower() in {"", "test_db", "default", "public"}:
            db_name = ""
        token = _auth_token(api_key=api_key, username=username, password=password)
        base_url = _base_url(host, port, ssl, connection_string)
        session = _requests_session()
        hdrs = _headers(token)
        stats_payload: dict[str, Any] = {"collectionName": coll}
        if db_name:
            stats_payload["dbName"] = db_name
        stats = session.post(
            f"{base_url}/v2/vectordb/collections/get_stats",
            data=json.dumps(stats_payload),
            headers=hdrs,
            timeout=30,
        )
        body = stats.json() if stats.content else {}
        if not _ok_response(body if isinstance(body, dict) else {}, stats.status_code):
            return -1, ""
        data = body.get("data") if isinstance(body, dict) else {}
        count = int(
            (data or {}).get("rowCount")
            or (data or {}).get("row_count")
            or (data or {}).get("row_num")
            or 0
        )
        id_cap = int(limit) if limit and int(limit) > 0 else 500
        ids = [str(x) for x in (written_ids or []) if x is not None][:id_cap]
        query_payload: dict[str, Any] = {
            "collectionName": coll,
            "outputFields": ["id", "content", "source_id", "chunk_index"],
            "limit": max(1, id_cap if ids else int(limit or 100) or 100),
        }
        if ids:
            # Prefer keyed filter when writer stamped identities.
            quoted = ", ".join(json.dumps(i) for i in ids)
            query_payload["filter"] = f"id in [{quoted}]"
        else:
            query_payload["filter"] = ""
        if db_name:
            query_payload["dbName"] = db_name
        query = session.post(
            f"{base_url}/v2/vectordb/entities/query",
            data=json.dumps(query_payload),
            headers=hdrs,
            timeout=30,
        )
        qbody = load_http_json(query) if query.content else {}
        dict_rows: list[dict[str, Any]] = []
        if _ok_response(qbody if isinstance(qbody, dict) else {}, query.status_code):
            rows = qbody.get("data") if isinstance(qbody, dict) else []
            if isinstance(rows, list):
                for row in rows:
                    if isinstance(row, dict):
                        dict_rows.append({k: v for k, v in row.items() if k != "vector"})
        columns = target_columns or (
            sorted({k for r in dict_rows for k in r}) if dict_rows else ["id"]
        )
        return count, canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="milvus",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Milvus reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_elasticsearch_index(
    *,
    host: str = "",
    port: int = 9200,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    ssl: bool = False,
    api_key: str = "",
    index: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Elasticsearch search read-back for Gate-8."""
    try:
        from connectors.elasticsearch_reader import read_index_batch

        cfg = {
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "ssl": ssl,
            "api_key": api_key,
        }
        # A single 500-hit page was hashed against the source's whole-table
        # digest, so strict reconcile reported a checksum mismatch for every
        # index holding more than 500 documents even when all rows landed
        # intact. ``limit=0`` means the whole population, so page the index
        # with ``search_after`` until it is exhausted.
        page = max(int(limit or 0), 0) or _ES_RECONCILE_PAGE
        page = min(page, _ES_RECONCILE_PAGE)
        headers: list[str] = []

        def _row_iter() -> Iterator[dict[str, Any]]:
            nonlocal headers
            cursor: list | None = None
            seen = 0
            while True:
                batch, cursor = read_index_batch(
                    cfg=cfg,
                    index=index,
                    columns=target_columns,
                    limit=page,
                    search_after=cursor,
                )
                if not headers:
                    headers = list(batch.headers or target_columns or [])
                rows = list(batch.rows or [])
                for row in rows:
                    if isinstance(row, dict):
                        yield row
                    elif headers:
                        yield {
                            headers[i]: row[i] if i < len(row) else None
                            for i in range(len(headers))
                        }
                seen += len(rows)
                if not rows or cursor is None:
                    return
                if limit and seen >= int(limit):
                    return

        # ``columns`` must be known before hashing; the first page settles the
        # headers, and ``target_columns`` already carries the mapped projection.
        rows_iter = _row_iter()
        first = next(rows_iter, None)
        columns = list(target_columns or []) or (
            headers or (sorted(first.keys()) if isinstance(first, dict) else [])
        )
        replay: Iterator[dict[str, Any]] = (
            chain([first], rows_iter) if first is not None else iter(())
        )
        checksum = canonical_checksum_from_iter(
            replay,
            columns,
            limit=limit,
            dest_db_type="elasticsearch",
            dest_types=dest_types,
        )
        count = 0
        try:
            from connectors.elasticsearch_reader import _client

            es = _client(cfg)
            try:
                count = int(es.count(index=index).get("count", 0))
            finally:
                es.close()
        except Exception as exc:
            logger.warning("Exception suppressed: %s", exc, exc_info=exc)
            return -1, ""
        return count, checksum
    except Exception as exc:
        logger.warning("Elasticsearch reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""
