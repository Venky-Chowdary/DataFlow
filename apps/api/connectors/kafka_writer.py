"""Kafka destination writer — JSON produce with optional Schema Registry hook.

Credentials stay on the connector config (vaulted by the secret vault when
``api_key`` / SASL password fields are encrypted). This is a first-class
*destination* path for enterprises that already run Kafka; native CDC remains
the default capture path and does not require a broker.
"""

from __future__ import annotations

import hashlib
import json
import ssl
from typing import Any, Callable

from connectors.writer_common import (
    WriteResult,
    build_mapped_rows_with_details,
    resolve_target_columns,
    transform_error_policy,
)
from services.value_serializer import json_default


def _bootstrap(host: str, port: int, connection_string: str) -> str:
    if connection_string and "://" not in connection_string and "," in connection_string:
        return connection_string.strip()
    if connection_string and not connection_string.startswith(("http://", "https://", "file://")):
        # Plain bootstrap list
        if "bootstrap" in connection_string.lower() or ":" in connection_string:
            return connection_string.replace("bootstrap.servers=", "").strip()
    h = (host or "localhost").strip()
    p = int(port or 9092)
    return f"{h}:{p}"


def _producer(cfg: dict[str, Any]):
    try:
        from kafka import KafkaProducer  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "kafka-python is not installed. Install kafka-python to enable Kafka destinations."
        ) from exc

    bootstrap = _bootstrap(
        str(cfg.get("host") or ""),
        int(cfg.get("port") or 9092),
        str(cfg.get("connection_string") or ""),
    )
    kwargs: dict[str, Any] = {
        "bootstrap_servers": bootstrap.split(","),
        "value_serializer": lambda v: json.dumps(v, default=json_default).encode("utf-8"),
        "key_serializer": lambda v: v.encode("utf-8") if isinstance(v, str) else v,
        "acks": "all",
        "retries": 3,
    }
    security = str(cfg.get("schema") or cfg.get("security_protocol") or "").upper()
    username = str(cfg.get("username") or "")
    password = str(cfg.get("password") or cfg.get("api_key") or "")
    if username and password:
        kwargs["security_protocol"] = security if security in {"SASL_SSL", "SASL_PLAINTEXT"} else "SASL_SSL"
        kwargs["sasl_mechanism"] = str(cfg.get("database") or "PLAIN")  # PLAIN | SCRAM-SHA-256
        kwargs["sasl_plain_username"] = username
        kwargs["sasl_plain_password"] = password
        if kwargs["security_protocol"] == "SASL_SSL":
            kwargs["ssl_context"] = ssl.create_default_context()
    return KafkaProducer(**kwargs)


def _maybe_register_schema(registry_url: str, subject: str, schema_str: str) -> int | None:
    """Best-effort Confluent Schema Registry register (JSON schema). Returns schema id."""
    if not registry_url:
        return None
    try:
        import requests

        url = registry_url.rstrip("/") + f"/subjects/{subject}/versions"
        resp = requests.post(
            url,
            json={"schemaType": "JSON", "schema": schema_str},
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            timeout=15,
        )
        if resp.status_code in {200, 201}:
            return int(resp.json().get("id") or 0) or None
    except Exception:
        return None
    return None


def test_kafka(
    *,
    host: str = "",
    port: int = 9092,
    database: str = "",
    table: str = "",
    connection_string: str = "",
    api_key: str = "",
    username: str = "",
    password: str = "",
    ssl: bool = False,
    schema: str = "",
    **_kwargs: Any,
) -> tuple[bool, str]:
    try:
        producer = _producer({
            "host": host,
            "port": port or 9092,
            "connection_string": connection_string,
            "username": username,
            "password": password or api_key,
            "api_key": api_key,
            "schema": schema or ("SASL_SSL" if ssl else ""),
            "database": database,
        })
        producer.close()
        return True, f"Kafka broker reachable at {_bootstrap(host, port or 9092, connection_string)}"
    except Exception as exc:
        return False, f"Kafka connection failed: {exc}"


def write_mapped_rows(
    *,
    host: str = "",
    port: int = 9092,
    database: str = "",
    username: str = "",
    password: str = "",
    schema: str = "",
    connection_string: str = "",
    ssl: bool = False,
    table_name: str = "",
    headers: list[str] | None = None,
    data_rows: list[list[str]] | None = None,
    mappings: list[dict] | None = None,
    column_types: dict[str, str] | None = None,
    on_checkpoint: Callable[..., None] | None = None,
    create_table: bool = False,
    error_policy: str | None = None,
    write_mode: str = "append",
    conflict_columns: list[str] | None = None,
    api_key: str = "",
    schema_registry_url: str = "",
    **_kwargs: Any,
) -> WriteResult:
    headers = headers or []
    data_rows = data_rows or []
    mappings = mappings or []
    column_types = column_types or {}
    topic = (table_name or database or "dataflow.events").strip()

    target_cols, _ = resolve_target_columns(mappings, column_types, preserve_case=True)
    policy = transform_error_policy(error_policy)
    mapped_rows, transform_errors, rejected_details = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        error_policy=policy,
        dest_types={c: "string" for c in target_cols},
        preserve_case=True,
    )
    if transform_errors and policy == "fail":
        return WriteResult(
            ok=False, rows_written=0, table_name=topic, target_schema="",
            checksum="", chunks_completed=0,
            error=f"Transform errors: {'; '.join(transform_errors[:3])}",
            rejected_details=rejected_details, driver="kafka",
        )

    registry = (
        schema_registry_url
        or str(_kwargs.get("registry_url") or "")
        or (connection_string if "http" in (connection_string or "") else "")
    )
    if registry.startswith("http"):
        schema_obj = {
            "type": "object",
            "properties": {c: {"type": "string"} for c in target_cols},
        }
        _maybe_register_schema(registry, f"{topic}-value", json.dumps(schema_obj))

    try:
        producer = _producer({
            "host": host,
            "port": port or 9092,
            "connection_string": connection_string if "http" not in (connection_string or "") else "",
            "username": username,
            "password": password or api_key,
            "api_key": api_key,
            "schema": schema or ("SASL_SSL" if ssl else ""),
            "database": database if database and database.upper() in {
                "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"
            } else "PLAIN",
        })
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=0, table_name=topic, target_schema="",
            checksum="", chunks_completed=0, error=str(exc), driver="kafka",
        )

    written = 0
    digest = hashlib.sha256()
    key_col = (conflict_columns or [None])[0]
    try:
        for idx, row in enumerate(mapped_rows):
            if isinstance(row, dict):
                payload = row
            else:
                payload = {c: row[i] if i < len(row) else None for i, c in enumerate(target_cols)}
            key = (
                str(payload.get(key_col))
                if key_col and payload.get(key_col) is not None
                else None
            )
            fut = producer.send(topic, value=payload, key=key)
            fut.get(timeout=30)
            written += 1
            digest.update(json.dumps(payload, default=json_default, sort_keys=True).encode())
            if on_checkpoint and (idx + 1) % 100 == 0:
                on_checkpoint(written, len(mapped_rows), 1)
        producer.flush(timeout=30)
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=written, table_name=topic, target_schema="",
            checksum=digest.hexdigest()[:16], chunks_completed=1,
            error=f"Kafka produce failed: {exc}",
            rejected_details=rejected_details, driver="kafka",
        )
    finally:
        try:
            producer.close()
        except Exception:
            pass

    return WriteResult(
        ok=True,
        rows_written=written,
        table_name=topic,
        target_schema="",
        checksum=digest.hexdigest()[:16] if written else "",
        chunks_completed=1,
        rejected_details=rejected_details,
        rejected_rows=len(rejected_details),
        driver="kafka",
    )
