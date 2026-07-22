"""Confluent Schema Registry helpers — fail-closed when a registry URL is configured.

Wire format (Confluent): magic byte ``0x00`` + big-endian schema id (4 bytes) + payload.
JSON Schema subjects store the document body as UTF-8 JSON after the header.

This module is intentionally separate from DataFlow's internal
``services.schema_registry`` (transfer schema versioning / lineage).
"""

from __future__ import annotations

import json
import struct
from typing import Any

from services.value_serializer import json_default

CONFLUENT_MAGIC = 0


class SchemaRegistryError(RuntimeError):
    """Raised when Schema Registry is configured but unreachable or returns an error."""


def is_confluent_wire(payload: bytes | bytearray | memoryview | None) -> bool:
    if payload is None:
        return False
    data = bytes(payload)
    return len(data) >= 5 and data[0] == CONFLUENT_MAGIC


def split_confluent_wire(payload: bytes | bytearray | memoryview) -> tuple[int, bytes]:
    """Return ``(schema_id, body)`` from a Confluent-framed message."""
    data = bytes(payload)
    if not is_confluent_wire(data):
        raise SchemaRegistryError("Not a Confluent Schema Registry wire payload")
    schema_id = struct.unpack(">I", data[1:5])[0]
    return schema_id, data[5:]


def encode_confluent_json(schema_id: int, value: Any) -> bytes:
    """Frame a JSON-serializable value with the Confluent wire header."""
    body = json.dumps(value, default=json_default, separators=(",", ":")).encode("utf-8")
    return bytes([CONFLUENT_MAGIC]) + struct.pack(">I", int(schema_id)) + body


def fetch_schema(registry_url: str, schema_id: int, *, timeout: float = 15.0) -> dict[str, Any]:
    """GET ``/schemas/ids/{id}`` — fail-closed on any non-2xx or transport error."""
    base = (registry_url or "").strip().rstrip("/")
    if not base:
        raise SchemaRegistryError("Schema Registry URL is required to fetch schemas")
    try:
        import requests
    except ImportError as exc:
        raise SchemaRegistryError("requests is required for Schema Registry access") from exc

    url = f"{base}/schemas/ids/{int(schema_id)}"
    try:
        resp = requests.get(
            url,
            headers={"Accept": "application/vnd.schemaregistry.v1+json"},
            timeout=timeout,
        )
    except Exception as exc:
        raise SchemaRegistryError(f"Schema Registry fetch failed: {exc}") from exc
    if resp.status_code != 200:
        raise SchemaRegistryError(
            f"Schema Registry GET {url} returned HTTP {resp.status_code}: {resp.text[:200]}"
        )
    try:
        return resp.json()
    except Exception as exc:
        raise SchemaRegistryError(f"Schema Registry returned non-JSON body: {exc}") from exc


def register_json_schema(
    registry_url: str,
    subject: str,
    schema_str: str,
    *,
    timeout: float = 15.0,
) -> int:
    """POST a JSON Schema subject version — fail-closed (never swallow errors)."""
    base = (registry_url or "").strip().rstrip("/")
    if not base:
        raise SchemaRegistryError("Schema Registry URL is required to register schemas")
    subject = (subject or "").strip()
    if not subject:
        raise SchemaRegistryError("Schema Registry subject is required")
    try:
        import requests
    except ImportError as exc:
        raise SchemaRegistryError("requests is required for Schema Registry access") from exc

    url = f"{base}/subjects/{subject}/versions"
    try:
        resp = requests.post(
            url,
            json={"schemaType": "JSON", "schema": schema_str},
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            timeout=timeout,
        )
    except Exception as exc:
        raise SchemaRegistryError(f"Schema Registry register failed: {exc}") from exc
    if resp.status_code not in {200, 201}:
        raise SchemaRegistryError(
            f"Schema Registry POST {url} returned HTTP {resp.status_code}: {resp.text[:300]}"
        )
    try:
        schema_id = int(resp.json().get("id") or 0)
    except Exception as exc:
        raise SchemaRegistryError(f"Schema Registry register response missing id: {exc}") from exc
    if schema_id <= 0:
        raise SchemaRegistryError("Schema Registry register returned an invalid schema id")
    return schema_id


def decode_kafka_value(
    raw: bytes | bytearray | memoryview | str | None,
    *,
    registry_url: str = "",
) -> Any:
    """Decode a Kafka value: Confluent wire (+ optional registry fetch) or plain JSON/text.

    When ``registry_url`` is set and the payload is Confluent-framed, the schema id
    is fetched fail-closed before parsing the body. Plain JSON without a magic byte
    is still accepted (common for non-Registry producers).
    """
    if raw is None:
        return None
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return ""
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return raw

    data = bytes(raw)
    if not data:
        return None

    if is_confluent_wire(data):
        schema_id, body = split_confluent_wire(data)
        if (registry_url or "").strip():
            # Fail-closed: operator configured a registry — prove the id resolves.
            fetch_schema(registry_url, schema_id)
        try:
            return json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SchemaRegistryError(
                f"Confluent payload for schema id {schema_id} is not valid JSON: {exc}"
            ) from exc

    # Non-framed bytes — try UTF-8 JSON, else opaque string.
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return {"_kafka_value_b64": data.hex()}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text
