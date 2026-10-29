"""Gate-8 destination read-backs for warehouse, document, vector and SaaS targets (re-exported by services.reconciliation)."""

from __future__ import annotations

import logging
import json
from services.fingerprint_accumulator import fingerprint_checksum
from services.readback_projection import project_readback
from services.value_serializer import load_http_json
from typing import Any

logger = logging.getLogger("services.reconciliation")


def verify_pinecone_namespace(
    *,
    host: str = "",
    connection_string: str = "",
    username: str = "",
    password: str = "",
    api_key: str = "",
    namespace: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
) -> tuple[int, str]:
    """Independent Pinecone stats + metadata fetch for Gate-8 (embeddings opaque)."""

    from services.reconciliation import (
        canonical_checksum_from_iter,
    )

    try:
        from connectors.pinecone_writer import _headers, _index_url, _requests_session

        index_url = _index_url(host, connection_string)
        key = api_key or password or username or ""
        if not index_url or not key:
            return -1, ""
        session = _requests_session()
        hdrs = _headers(key)
        ns = (namespace or "").strip()
        stats = session.post(
            f"{index_url}/describe_index_stats",
            data=json.dumps({}),
            headers=hdrs,
            timeout=30,
        )
        if stats.status_code not in {200, 201}:
            return -1, ""
        body = stats.json() if stats.content else {}
        namespaces = body.get("namespaces") or {}
        if ns:
            count = int((namespaces.get(ns) or {}).get("vector_count") or 0)
        else:
            count = int(body.get("totalVectorCount") or body.get("total_vector_count") or 0)
            if not count and isinstance(namespaces, dict):
                count = sum(int((v or {}).get("vector_count") or 0) for v in namespaces.values())

        dict_rows: list[dict[str, Any]] = []
        # limit=0 means "full batch" for Gate-8 — still cap fetch for API safety.
        id_cap = int(limit) if limit and int(limit) > 0 else 500
        ids = [str(x) for x in (written_ids or []) if x is not None][:id_cap]
        if ids:
            params = [("ids", i) for i in ids]
            if ns:
                params.append(("namespace", ns))
            # requests supports list of tuples for repeated ids=
            fetch = session.get(
                f"{index_url}/vectors/fetch",
                params=params,
                headers=hdrs,
                timeout=30,
            )
            if fetch.status_code in {200, 201}:
                vectors = (load_http_json(fetch) or {}).get("vectors") or {}
                for vid, payload in vectors.items():
                    meta = payload.get("metadata") if isinstance(payload, dict) else {}
                    if not isinstance(meta, dict):
                        meta = {}
                    dict_rows.append({"id": vid, **meta})
        columns = target_columns or (
            sorted({k for r in dict_rows for k in r}) if dict_rows else ["id"]
        )
        return count, canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="pinecone",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Pinecone reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_qdrant_collection(
    *,
    host: str = "",
    port: int = 6333,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    ssl: bool = False,
    collection: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
) -> tuple[int, str]:
    """Independent Qdrant collection count + payload scroll/retrieve for Gate-8."""

    from services.reconciliation import (
        canonical_checksum_from_iter,
    )

    try:
        from connectors.qdrant_writer import _base_url, _headers, _requests_session

        api_key = password or username or ""
        base_url = connection_string.rstrip("/") if connection_string else _base_url(host, port, ssl)
        session = _requests_session()
        hdrs = _headers(api_key)
        info = session.get(f"{base_url}/collections/{collection}", headers=hdrs, timeout=15)
        if info.status_code != 200:
            return -1, ""
        body = info.json() or {}
        result = body.get("result") or body
        count = int(
            (result.get("points_count") if isinstance(result, dict) else None)
            or (result.get("indexed_vectors_count") if isinstance(result, dict) else None)
            or 0
        )
        id_cap = int(limit) if limit and int(limit) > 0 else 500
        ids = [str(x) for x in (written_ids or []) if x is not None][:id_cap]
        dict_rows: list[dict[str, Any]] = []
        if ids:
            retrieve = session.post(
                f"{base_url}/collections/{collection}/points",
                data=json.dumps({
                    "ids": ids,
                    "with_payload": True,
                    "with_vector": False,
                }),
                headers=hdrs,
                timeout=30,
            )
            if retrieve.status_code in {200, 201}:
                points = (load_http_json(retrieve) or {}).get("result") or []
                for pt in points:
                    if not isinstance(pt, dict):
                        continue
                    payload = pt.get("payload") if isinstance(pt.get("payload"), dict) else {}
                    dict_rows.append({"id": pt.get("id"), **payload})
        if not dict_rows:
            scroll = session.post(
                f"{base_url}/collections/{collection}/points/scroll",
                data=json.dumps({
                    "limit": max(1, id_cap if ids else int(limit or 100) or 100),
                    "with_payload": True,
                    "with_vector": False,
                }),
                headers=hdrs,
                timeout=30,
            )
            if scroll.status_code in {200, 201}:
                points = ((load_http_json(scroll) or {}).get("result") or {}).get("points") or []
                for pt in points:
                    if not isinstance(pt, dict):
                        continue
                    payload = pt.get("payload") if isinstance(pt.get("payload"), dict) else {}
                    dict_rows.append({"id": pt.get("id"), **payload})
        columns = target_columns or (
            sorted({k for r in dict_rows for k in r}) if dict_rows else ["id"]
        )
        return count, canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="qdrant",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Qdrant reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_bigquery_table(
    *,
    project_id: str,
    dataset_id: str,
    connection_string: str,
    host: str = "",
    port: int = 0,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    from services.reconciliation import (
        canonical_checksum_from_iter,
    )

    try:
        from connectors.bigquery_conn import get_client

        client = get_client(
            project_id=project_id,
            credentials_path=connection_string,
            host=host,
            port=port,
        )
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        from connectors.google_emulator import (
            google_emulator_retry,
            google_emulator_timeout,
            looks_like_google_emulator,
        )

        probe_kw: dict[str, Any] = {}
        if looks_like_google_emulator(
            endpoint=connection_string, host=host, port=port,
        ):
            probe_kw = {
                "retry": google_emulator_retry(),
                "timeout": google_emulator_timeout(),
            }
        table = client.get_table(table_id, **probe_kw)
        # Table.num_rows lags the streaming buffer and goccy reports 0.
        # Dest-engine COUNT(*) is the Gate-8 SSOT (same as dest_precount).
        from services.dest_precount import _bigquery_row_count

        counted = _bigquery_row_count(
            {
                "database": project_id,
                "schema": dataset_id,
                "host": host,
                "port": port,
                "connection_string": connection_string,
            },
            schema=dataset_id,
            table_name=table_name,
        )
        count = int(counted) if counted is not None else 0
        field_names = [field.name for field in table.schema] if table.schema else []

        def _row_iter():
            yielded = 0
            if probe_kw:
                # goccy list_rows retries until operator timeout. Query instead.
                from services.dest_precount import _bigquery_run_query

                qualified = f"`{project_id}`.`{dataset_id}`.`{table_name}`"
                sql = f"SELECT * FROM {qualified}"
                if limit:
                    sql += f" LIMIT {int(limit)}"
                for row in _bigquery_run_query(client, sql):
                    yield list(row.values()) if hasattr(row, "values") else list(row)
                return
            for row in client.list_rows(table_id):
                if limit and yielded >= limit:
                    break
                yield list(row.values()) if hasattr(row, "values") else list(row)
                yielded += 1

        columns, projected = project_readback(field_names, target_columns, _row_iter())
        return int(count), canonical_checksum_from_iter(
            projected,
            columns,
            limit=limit,
            dest_db_type="bigquery",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_s3_object(
    *,
    bucket: str,
    key: str,
    host: str,
    port: int,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    file_format: str = "",
) -> tuple[int, str]:
    """Gate-8 cell checksum of S3 GET streams. Never JSON-fallback empty.

    Dest COUNT of the same keys is ``destination_row_count``. This digest
    walks records (CSV RFC 4180, JSONL objects, JSON root array, Parquet/
    Avro values) off the GET body. Gzip CSV as UTF-8 JSON garbage is not
    dest=0. XML/Excel/ORC cell walk stays unmeasured. Missing object is
    ``(0, "")``.
    """
    try:
        from services.dest_precount import checksum_object_store

        return checksum_object_store(
            "s3",
            {
                "host": host,
                "port": port,
                "username": username,
                "password": password,
                "connection_string": connection_string,
                "ssl": ssl,
                "database": bucket,
                "file_format": file_format,
            },
            table_name=key,
            columns=target_columns,
            limit=limit,
            dest_db_type="s3",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_gcs_blob(
    *,
    bucket: str,
    key: str,
    host: str,
    port: int,
    connection_string: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    file_format: str = "",
) -> tuple[int, str]:
    """Gate-8 cell checksum of GCS GET streams. Never JSON-fallback empty."""
    try:
        from services.dest_precount import checksum_object_store

        return checksum_object_store(
            "gcs",
            {
                "host": host,
                "port": port,
                "connection_string": connection_string,
                "database": bucket,
                "file_format": file_format,
            },
            dest_db_type="gcs",
            dest_types=dest_types,
            table_name=key,
            columns=target_columns,
            limit=limit,
        )
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_adls_blob(
    *,
    container: str,
    key: str,
    host: str = "",
    port: int = 0,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    service_account: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    file_format: str = "",
) -> tuple[int, str]:
    """Gate-8 cell checksum of Azure Blob / ADLS GET streams. Never JSON ``[]``."""
    try:
        from services.dest_precount import checksum_object_store

        return checksum_object_store(
            "adls",
            {
                "host": host,
                "port": port,
                "username": username,
                "password": password,
                "connection_string": connection_string,
                "service_account": service_account,
                "database": container,
                "file_format": file_format,
            },
            table_name=key,
            columns=target_columns,
            limit=limit,
            dest_db_type="adls",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("ADLS reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_hubspot_object(
    *,
    host: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    object_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent HubSpot CRM read-back for Gate-8 (reverse-ETL honesty)."""

    from services.reconciliation import (
        _saas_readback_limit,
        _saas_readback_truncated,
        canonical_checksum_from_iter,
    )

    try:
        from connectors.hubspot import read_object

        cfg = {
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "table": object_name,
            "database": object_name,
        }
        batch = read_object(
            cfg=cfg,
            object=object_name,
            limit=_saas_readback_limit(limit),
        )
        headers = list(batch.headers or target_columns or [])
        rows = list(batch.rows or [])
        if _saas_readback_truncated("HubSpot", limit, len(rows)):
            return -1, ""
        # Convert matrix rows to dicts when ReadBatch stores tuples.
        dict_rows: list[Any] = []
        for row in rows:
            if isinstance(row, dict):
                dict_rows.append(row)
            elif headers:
                dict_rows.append(
                    {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}
                )
            else:
                dict_rows.append(row)
        columns = headers or target_columns or (
            sorted({k for r in dict_rows if isinstance(r, dict) for k in r}) if dict_rows else []
        )
        return len(dict_rows), canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="hubspot",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("HubSpot reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_salesforce_object(
    *,
    host: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    api_key: str = "",
    object_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Salesforce SOQL read-back for Gate-8 reverse-ETL."""

    from services.reconciliation import (
        _saas_readback_limit,
        _saas_readback_truncated,
        canonical_checksum_from_iter,
    )

    try:
        from connectors.salesforce import read_object

        cfg = {
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "api_key": api_key,
            "table": object_name,
            "database": object_name,
        }
        batch = read_object(
            cfg=cfg,
            object=object_name,
            limit=_saas_readback_limit(limit),
        )
        if _saas_readback_truncated("Salesforce", limit, len(batch.rows or [])):
            return -1, ""
        headers = list(batch.headers or target_columns or [])
        dict_rows: list[Any] = []
        for row in batch.rows or []:
            if isinstance(row, dict):
                # Strip Salesforce attributes envelope when present.
                d = {k: v for k, v in row.items() if k != "attributes"}
                dict_rows.append(d)
            elif headers:
                dict_rows.append(
                    {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}
                )
        columns = headers or target_columns or (
            sorted({k for r in dict_rows if isinstance(r, dict) for k in r}) if dict_rows else []
        )
        return len(dict_rows), canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="salesforce",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Salesforce reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_airtable_table(
    *,
    host: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    api_key: str = "",
    base_id: str,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Airtable read-back — flatten fields for Gate-8 fingerprints."""

    from services.reconciliation import (
        _saas_readback_limit,
        _saas_readback_truncated,
        canonical_checksum_from_iter,
    )

    try:
        from connectors.airtable import read_object
        from connectors.saas_typed_schema import flatten_airtable_record

        cfg = {
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string or base_id,
            "api_key": api_key,
            "database": base_id,
            "table": table_name,
            "type": "airtable",
        }
        batch = read_object(
            cfg=cfg,
            object=table_name,
            limit=_saas_readback_limit(limit),
        )
        if _saas_readback_truncated("Airtable", limit, len(batch.rows or [])):
            return -1, ""
        dict_rows: list[Any] = []
        for row in batch.rows or []:
            if isinstance(row, dict):
                flat, _schema = flatten_airtable_record(row)
                dict_rows.append(flat)
            elif batch.headers:
                headers = list(batch.headers)
                dict_rows.append(
                    {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}
                )
        columns = target_columns or (
            sorted({k for r in dict_rows if isinstance(r, dict) for k in r}) if dict_rows else []
        )
        return len(dict_rows), canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="airtable",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Airtable reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_stripe_object(
    *,
    host: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    api_key: str = "",
    object_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Stripe API read-back for Gate-8 reverse-ETL."""

    from services.reconciliation import (
        _verify_saas_read_object,
    )

    from connectors.stripe import read_object

    return _verify_saas_read_object(
        driver="stripe",
        read_object=read_object,
        cfg={
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "api_key": api_key,
            "table": object_name,
            "database": object_name,
            "type": "stripe",
        },
        object_name=object_name or "customers",
        target_columns=target_columns,
        limit=limit,
        dest_types=dest_types,
    )


def verify_shopify_object(
    *,
    host: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    api_key: str = "",
    object_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Shopify Admin REST read-back for Gate-8 reverse-ETL."""

    from services.reconciliation import (
        _verify_saas_read_object,
    )

    from connectors.shopify import read_object

    return _verify_saas_read_object(
        driver="shopify",
        read_object=read_object,
        cfg={
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "api_key": api_key,
            "table": object_name,
            "database": object_name,
            "shop": host,
            "type": "shopify",
        },
        object_name=object_name or "customers",
        target_columns=target_columns,
        limit=limit,
        dest_types=dest_types,
    )


def verify_zendesk_object(
    *,
    host: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    api_key: str = "",
    object_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Zendesk Support API read-back for Gate-8 reverse-ETL."""

    from services.reconciliation import (
        _verify_saas_read_object,
    )

    from connectors.zendesk import read_object

    return _verify_saas_read_object(
        driver="zendesk",
        read_object=read_object,
        cfg={
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "api_key": api_key,
            "table": object_name,
            "database": object_name,
            "type": "zendesk",
        },
        object_name=object_name or "tickets",
        target_columns=target_columns,
        limit=limit,
        dest_types=dest_types,
    )


def verify_notion_object(
    *,
    host: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    api_key: str = "",
    object_name: str,
    database_id: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Notion database/page read-back for Gate-8 reverse-ETL."""

    from services.reconciliation import (
        _verify_saas_read_object,
    )

    from connectors.notion import read_object

    return _verify_saas_read_object(
        driver="notion",
        read_object=read_object,
        cfg={
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "api_key": api_key,
            "table": object_name,
            "database": database_id or object_name,
            "type": "notion",
        },
        object_name=object_name or database_id or "",
        target_columns=target_columns,
        limit=limit,
        dest_types=dest_types,
    )


def verify_kafka_topic(
    *,
    host: str = "",
    port: int = 0,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    topic: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    schema_registry_url: str = "",
    security_options: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Bounded Kafka consume for Gate-8 — sample proof, not topic cardinality.

    ``security_options`` is the destination's declared protocol / mechanism
    (``kafka_security_options``) — the read-back dials the listener the
    producer wrote through (D83).
    """

    from services.reconciliation import (
        _saas_readback_limit,
        _saas_readback_truncated,
        canonical_checksum_from_iter,
    )

    try:
        from connectors.kafka_reader import read_topic_to_end

        cfg = {
            **(security_options or {}),
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "database": topic,
            "table": topic,
            "schema_registry_url": schema_registry_url,
        }
        # The whole log up to its end offsets, not one poll batch: a batch
        # under-counted every large produce and failed a job whose rows were
        # all written (D152). An unfinished read is unverified (-1), never a
        # short count.
        batch, complete = read_topic_to_end(
            cfg=cfg,
            topic=topic,
            columns=target_columns,
            limit=_saas_readback_limit(limit),
        )
        headers = list(batch.headers or target_columns or [])
        rows = list(batch.rows or [])
        if _saas_readback_truncated("Kafka", limit, len(rows)):
            return -1, ""
        if not complete:
            logger.warning("Kafka read-back of %s did not reach the end offsets", topic)
            return -1, ""
        dict_rows: list[Any] = []
        for row in rows:
            if isinstance(row, dict):
                dict_rows.append(row)
            elif headers:
                dict_rows.append(
                    {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}
                )
        # The digest is over what the job produced. The reader's broker
        # envelope (_kafka_topic/_partition/_offset/_key) is provenance the
        # source never had; digesting it failed every write (D152).
        columns = list(target_columns or []) or headers
        return len(dict_rows), canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="kafka",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Kafka reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_databricks_table(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    schema: str = "",
    table_name: str,
    http_path: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
    pk_column: str | None = None,
) -> tuple[int, str]:
    """Independent Databricks SQL warehouse read-back for Gate-8.

    ``written_ids`` + ``pk_column`` re-scope the digest to this batch's keys.
    """

    from services.reconciliation import (
        canonical_checksum_from_iter,
        keyed_readback_sa_clause,
        keyed_readback_scope,
        sa_streaming_result,
    )

    try:
        import sqlalchemy as sa

        from connectors.generic_sql import get_sqlalchemy_engine
        from connectors.sql_identifiers import quote_table_ref

        cfg: dict[str, Any] = {
            "type": "databricks",
            "host": host or "",
            "port": int(port or 443),
            "database": database or "",
            "username": username or "",
            "password": password or "",
            "connection_string": connection_string or "",
            "schema": schema or "",
            "http_path": http_path or "",
        }
        engine = get_sqlalchemy_engine(cfg)
        sch = (schema or database or "").strip() or None
        table_ref = quote_table_ref(table_name, schema=sch, dialect="ansi")
        with engine.connect() as conn:
            count = int(
                conn.execute(sa.text(f"SELECT COUNT(*) FROM {table_ref}")).scalar()  # nosec B608
                or 0
            )
            ids, pk = keyed_readback_scope(written_ids, pk_column)
            select = sa.text(f"SELECT * FROM {table_ref}")  # nosec B608
            if ids:
                where, params = keyed_readback_sa_clause(pk, ids, dialect="databricks")
                select = sa.text(
                    f"SELECT * FROM {table_ref} {where}"  # nosec B608
                ).bindparams(**params)
            names, result = sa_streaming_result(conn, select)
            columns, projected = project_readback(names, target_columns, (tuple(row) for row in result))
            checksum = canonical_checksum_from_iter(
                projected,
                columns,
                limit=limit,
                dest_db_type="databricks",
                dest_types=dest_types,
            )
        return count, checksum
    except Exception as exc:
        logger.warning("Databricks reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_mongodb_collection(
    *,
    host: str = "",
    port: int = 27017,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    database: str = "",
    ssl: bool = False,
    auth_source: str = "",
    table_name: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
    pk_column: str | None = None,
) -> tuple[int, str]:
    """Reconcile a MongoDB target by counting and fingerprinting documents.

    When ``written_ids`` + ``pk_column`` are set, the checksum fingerprints only
    those keys while ``count`` remains full-collection cardinality (upsert Gate-8).
    """

    from services.reconciliation import (
        canonical_checksum_from_iter,
        keyed_readback_scope,
    )

    try:
        from connectors.mongodb_common import (
            _mongo_client,
            normalize_mongodb_connection_string,
        )

        conn_str = normalize_mongodb_connection_string(
            connection_string or "",
            database=database,
            host=host,
            port=port,
            username=username,
            password=password,
            ssl=ssl,
            auth_source=auth_source,
        )
        client = _mongo_client(conn_str)
        db = client[database or "test"]
        coll = db[table_name]
        count = coll.count_documents({})
        ids, pk = keyed_readback_scope(written_ids, pk_column)
        query: dict[str, Any] = {}
        if ids:
            # Match string or numeric id forms (CSV upserts often land as int).
            expanded: list[Any] = []
            for raw in ids:
                expanded.append(raw)
                try:
                    if str(int(raw)) == raw:
                        expanded.append(int(raw))
                except (TypeError, ValueError):
                    pass
            query = {pk: {"$in": expanded}}

        def _doc_iter():
            yielded = 0
            for doc in coll.find(query):
                if limit and yielded >= limit:
                    break
                yield doc
                yielded += 1

        columns = target_columns or sorted(
            set(k for doc in coll.find(query).limit(100) for k in doc.keys())
        )
        checksum = canonical_checksum_from_iter(
            _doc_iter(),
            columns,
            limit=limit,
            dest_db_type="mongodb",
            dest_types=dest_types,
        )
        return int(count), checksum
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_redis_prefix(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
    prefix: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    written_ids: list[str] | None = None,
    pk_column: str | None = None,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Reconcile Redis keys under ``prefix:*`` (writer key layout).

    ``written_ids`` scopes the digest to the keys this batch wrote, the same way
    the SQL and warehouse read-backs do. An upsert into a keyspace that already
    holds other keys is otherwise incomparable: the whole-prefix digest includes
    rows the source never sent. Cardinality stays whole-prefix either way.

    ``dest_db_type`` / ``dest_types`` must be the same ``gate8_digest_types``
    basis the write pass fingerprinted on: a Redis doc stores ``"1000.00"`` as
    a JSON *string*, and hashing the read-back without the declared carrier
    type canonicalizes it to ``1000`` while the write pass hashed the bound
    text ``1000.00`` — a silent-scheme digest mismatch on faithful data (X-132).
    """

    from services.reconciliation import (
        canonical_checksum_from_iter,
        keyed_readback_scope,
    )

    try:
        from connectors.redis_reader import (
            _redis_client,
            keys_for_prefix,
            redis_json_row,
            scan_all_keys,
        )

        prefix = (prefix or "").strip()
        if not prefix:
            # No prefix is not the whole database. dbsize of unrelated hashes
            # was reported as rows of this destination.
            return -1, ""
        client = _redis_client(
            {
                "host": host,
                "port": port,
                "database": database,
                "username": username,
                "password": password,
                "connection_string": connection_string,
                "ssl": ssl,
            }
        )
        pattern = f"{prefix}:*"
        keys: list[str] = keys_for_prefix(scan_all_keys(client, pattern), prefix)
        total = len(keys)
        scoped_ids, _pk = keyed_readback_scope(written_ids, pk_column)
        if scoped_ids:
            from connectors.redis_reader import redis_key_for

            wanted = {redis_key_for(prefix, i) for i in scoped_ids}
            keys = [k for k in keys if k in wanted]

        def _row_iter():
            for key in keys:
                yield redis_json_row(client.get(key))

        columns = target_columns or []
        if not columns and keys:
            sample = next(_row_iter(), {})
            columns = sorted(sample.keys()) if sample else ["value"]
        checksum = canonical_checksum_from_iter(
            _row_iter(),
            columns,
            limit=limit,
            dest_db_type=dest_db_type,
            dest_types=dest_types,
        )
        return total, checksum
    except Exception as exc:
        logger.warning("Redis reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_dynamodb_table(
    *,
    connection_string: str,
    database: str,
    table_name: str,
    username: str = "local",
    password: str = "local",
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Reconcile a DynamoDB target by Scan count and item fingerprint."""

    from services.reconciliation import (
        canonical_checksum_from_iter,
    )

    try:
        import boto3
        from boto3.dynamodb.types import TypeDeserializer

        endpoint = connection_string or "http://localhost:8000"
        client = boto3.client(
            "dynamodb",
            endpoint_url=endpoint,
            aws_access_key_id=username.strip() or "local",
            aws_secret_access_key=password.strip() or "local",
            region_name="us-east-1",
        )
        paginator = client.get_paginator("scan")
        count = sum(
            page["Count"]
            for page in paginator.paginate(TableName=table_name, Select="COUNT")
        )

        deserializer = TypeDeserializer()

        def _item_iter():
            yielded = 0
            for page in paginator.paginate(TableName=table_name):
                for item in page.get("Items", []):
                    if limit and yielded >= limit:
                        break
                    yield {k: deserializer.deserialize(v) for k, v in item.items()}
                    yielded += 1

        columns = target_columns or []
        return int(count), canonical_checksum_from_iter(
            _item_iter(),
            columns,
            limit=limit,
            dest_db_type="dynamodb",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_iceberg_table(
    *,
    connection_string: str = "",
    warehouse: str = "",
    table_name: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
    schema: str = "",
    database: str = "",
    host: str = "",
    extra: dict[str, Any] | None = None,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Reconcile Iceberg from current-snapshot data files, not ``scan().to_arrow()``.

    Dest COUNT is dest-engine file footers (same leftover MERGE listing).
    Catalog ``SqlCatalog`` / ``scan().count()`` never close filesystem tables
    and never close leftover identity. Unreadable snapshot is unmeasured.

    Filesystem CoW writes live at ``warehouse / namespace / table``. The
    namespace is destination schema (SKU and cartesian bind ``default``).
    Counting with an empty schema looks at ``warehouse / table`` and reports
    dest=0 after a correct write — Gate-8 fail, later dest_precount=2.
    """

    from services.reconciliation import (
        _iter_fingerprints,
    )

    try:
        from services.dest_precount import destination_row_count, iceberg_target_sample

        warehouse_path = warehouse or database or connection_string or ""
        cfg: dict[str, Any] = {
            "connection_string": connection_string or warehouse_path,
            "database": database or warehouse_path,
            "warehouse": warehouse_path,
            "host": host or "",
            "schema": schema or "",
        }
        if extra:
            cfg["extra"] = extra
            for key, value in extra.items():
                cfg.setdefault(key, value)
        count = destination_row_count(
            "iceberg", cfg, schema=schema or "", table_name=table_name
        )
        if count is None:
            return -1, ""
        cols = [str(c) for c in (target_columns or []) if str(c).strip()]
        if not cols:
            return int(count), ""
        rows = iceberg_target_sample(
            cfg,
            schema=schema or "",
            table_name=table_name,
            columns=cols,
            limit=int(limit or 0) or None,
        )
        if rows is None:
            return int(count), ""
        checksum = fingerprint_checksum(
            _iter_fingerprints(
                rows, cols, dest_db_type="iceberg", dest_types=dest_types or {}
            )
        )
        return int(count), checksum
    except Exception as exc:
        logger.warning("Iceberg reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_clickhouse_table(
    *,
    host: str = "",
    port: int = 9000,
    database: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    schema: str = "",
    ssl: bool = False,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
    pk_column: str | None = None,
) -> tuple[int, str]:
    """Independent ClickHouse read-back with ``SELECT … FINAL`` (Airbyte-class).

    ReplacingMergeTree upsert is lazy — Gate-8 without FINAL would fingerprint
    duplicate keys and false-fail checksum reconcile.
    """

    from services.reconciliation import (
        canonical_checksum_from_iter,
        keyed_readback_sa_clause,
        keyed_readback_scope,
    )

    try:
        import sqlalchemy as sa

        from connectors.generic_sql import (
            clickhouse_final_table_sql,
            get_sqlalchemy_engine,
        )
        from connectors.sql_identifiers import quote_table_ref

        cfg: dict[str, Any] = {
            "type": "clickhouse",
            "host": host or "",
            "port": int(port or 9000),
            "database": database or "",
            "username": username or "",
            "password": password or "",
            "connection_string": connection_string or "",
            "schema": schema or "",
            "ssl": bool(ssl),
        }
        engine = get_sqlalchemy_engine(cfg)
        table_ref = quote_table_ref(
            table_name,
            schema=schema or None,
            dialect="clickhouse",
        )
        from_sql = clickhouse_final_table_sql(table_ref)
        with engine.connect() as conn:
            count = int(
                conn.execute(
                    sa.text(f"SELECT count() FROM {from_sql}")  # nosec B608
                ).scalar()
                or 0
            )
            ids, pk = keyed_readback_scope(written_ids, pk_column)
            select = sa.text(f"SELECT * FROM {from_sql}")  # nosec B608
            if ids:
                where, params = keyed_readback_sa_clause(pk, ids, dialect="clickhouse")
                select = sa.text(
                    f"SELECT * FROM {from_sql} {where}"  # nosec B608
                ).bindparams(**params)
            result = conn.execute(select)
            names = list(result.keys())
            columns, projected = project_readback(names, target_columns, (tuple(row) for row in result))
            checksum = canonical_checksum_from_iter(
                projected,
                columns,
                limit=limit,
                dest_db_type="clickhouse",
                dest_types=dest_types,
            )
        return count, checksum
    except Exception as exc:
        logger.warning("ClickHouse reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_generic_sql_table(
    *,
    dest: dict[str, Any],
    schema: str = "",
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    engine_hint: str = "",
    written_ids: list[str] | None = None,
    pk_column: str | None = None,
) -> tuple[int, str]:
    """Independent SQLAlchemy read-back for catalog ``generic_sql`` engines.

    Covers Teradata / HANA / Vertica / Informix / Trino / Firebird / Netezza and
    other SQLAlchemy-backed SKUs that share ``get_sqlalchemy_engine``. Returns
    ``(-1, "")`` only when the engine cannot connect or the table is missing —
    never invent a checksum from writer rowcounts (Gate-8 honesty).
    """

    from services.reconciliation import (
        canonical_checksum_from_iter,
        keyed_readback_sa_clause,
        keyed_readback_scope,
        sa_streaming_result,
    )

    try:
        import sqlalchemy as sa

        from connectors.generic_sql import get_sqlalchemy_engine
        from connectors.sql_identifiers import quote_table_ref

        hint = (
            (engine_hint or dest.get("type") or dest.get("engine") or "generic_sql")
            .strip()
            .lower()
        )
        cfg = dict(dest)
        cfg["type"] = hint if hint and hint != "generic_sql" else (
            str(dest.get("type") or "generic_sql")
        )
        engine = get_sqlalchemy_engine(cfg)
        dialect = (
            getattr(getattr(engine, "dialect", None), "name", None) or hint or "ansi"
        ).lower()
        sch = (schema or dest.get("schema") or "").strip() or None
        table_ref = quote_table_ref(table_name, schema=sch, dialect=dialect)
        with engine.connect() as conn:
            count = int(
                conn.execute(
                    sa.text(f"SELECT COUNT(*) FROM {table_ref}")  # nosec B608
                ).scalar()
                or 0
            )
            ids, pk = keyed_readback_scope(written_ids, pk_column)
            select = sa.text(f"SELECT * FROM {table_ref}")  # nosec B608
            if ids:
                where, params = keyed_readback_sa_clause(pk, ids, dialect=dialect)
                select = sa.text(
                    f"SELECT * FROM {table_ref} {where}"  # nosec B608
                ).bindparams(**params)
            names, result = sa_streaming_result(conn, select)
            columns, projected = project_readback(names, target_columns, (tuple(row) for row in result))
            checksum = canonical_checksum_from_iter(
                projected,
                columns,
                limit=limit,
                dest_db_type=hint or dialect,
                dest_types=dest_types,
            )
        return count, checksum
    except Exception as exc:
        logger.warning(
            "generic_sql reconciliation read-back failed (%s): %s",
            engine_hint or dest.get("type"),
            exc,
            exc_info=exc,
        )
        return -1, ""


def verify_duckdb_table(
    *,
    connection_string: str,
    database: str,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
    pk_column: str | None = None,
) -> tuple[int, str]:
    """Reconcile a DuckDB target through the same SQLAlchemy dialect as the writer.

    Native ``duckdb.connect`` treats ``duckdb:///abs`` as a filename, and a second
    native connection fails while the writer dialect still holds the file with a
    different config. Independent read-back uses the pooled engine instead.
    ``written_ids`` + ``pk_column`` re-scope the digest to this batch's keys.
    """

    from services.reconciliation import (
        canonical_checksum_from_iter,
        keyed_readback_sa_clause,
        keyed_readback_scope,
        sa_streaming_result,
    )

    try:
        import sqlalchemy as sa

        from connectors.generic_sql import get_sqlalchemy_engine
        from connectors.sql_identifiers import quote_table_ref
        from connectors.sqlite_common import duckdb_file_path

        path = duckdb_file_path(database, connection_string)
        if not path:
            return -1, ""
        if path.lower().startswith(("md:", "motherduck:")):
            return -1, ""
        if path == ":memory:":
            return -1, ""
        conn_s = (connection_string or "").strip()
        cfg: dict[str, Any] = {"type": "duckdb", "database": path}
        if conn_s.lower().startswith("duckdb:"):
            cfg["connection_string"] = conn_s
        engine = get_sqlalchemy_engine(cfg)
        table_ref = quote_table_ref(table_name, dialect="duckdb")
        with engine.connect() as conn:
            count = int(
                conn.execute(sa.text(f"SELECT COUNT(*) FROM {table_ref}")).scalar()  # nosec B608
                or 0
            )
            ids, pk = keyed_readback_scope(written_ids, pk_column)
            select = sa.text(f"SELECT * FROM {table_ref}")  # nosec B608
            if ids:
                where, params = keyed_readback_sa_clause(pk, ids, dialect="duckdb")
                select = sa.text(
                    f"SELECT * FROM {table_ref} {where}"  # nosec B608
                ).bindparams(**params)
            names, result = sa_streaming_result(conn, select)
            columns, projected = project_readback(
                names, target_columns, (tuple(row) for row in result)
            )
            checksum = canonical_checksum_from_iter(
                projected,
                columns,
                limit=limit,
                dest_db_type="duckdb",
                dest_types=dest_types,
            )
        return count, checksum
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""
