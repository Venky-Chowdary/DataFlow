"""Confluent Schema Registry helpers — fail-closed when a registry URL is configured.

Wire format (Confluent): magic byte ``0x00`` + big-endian schema id (4 bytes) + payload.

Decode dispatches on registry ``schemaType``:
* JSON — UTF-8 JSON body
* AVRO (default) — ``fastavro`` schemaless reader against the registered schema
* PROTOBUF / unknown — refuse (never silently JSON-decode binary contracts)

This module is intentionally separate from DataFlow's internal
``services.schema_registry`` (transfer schema versioning / lineage).
"""

from __future__ import annotations

import io
import json
import struct
from typing import Any
from urllib.parse import quote

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


def fetch_latest_subject_schema(
    registry_url: str,
    subject: str,
    *,
    timeout: float = 15.0,
) -> dict[str, Any]:
    """GET ``/subjects/{subject}/versions/latest`` — fail-closed."""
    base = (registry_url or "").strip().rstrip("/")
    subject = (subject or "").strip()
    if not base:
        raise SchemaRegistryError("Schema Registry URL is required to fetch subjects")
    if not subject:
        raise SchemaRegistryError("Schema Registry subject is required")
    try:
        import requests
    except ImportError as exc:
        raise SchemaRegistryError("requests is required for Schema Registry access") from exc

    url = f"{base}/subjects/{quote(subject, safe='')}/versions/latest"
    try:
        resp = requests.get(
            url,
            headers={"Accept": "application/vnd.schemaregistry.v1+json"},
            timeout=timeout,
        )
    except Exception as exc:
        raise SchemaRegistryError(f"Schema Registry subject fetch failed: {exc}") from exc
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


def _parse_registered_schema(schema_doc: dict[str, Any]) -> tuple[str, Any]:
    """Return ``(schema_type_upper, parsed_schema_object_or_str)``."""
    schema_type = str(schema_doc.get("schemaType") or "AVRO").strip().upper() or "AVRO"
    schema_str = schema_doc.get("schema")
    if schema_str is None:
        raise SchemaRegistryError("Schema Registry response missing schema body")
    if schema_type in {"AVRO", ""}:
        if isinstance(schema_str, (dict, list)):
            return "AVRO", schema_str
        try:
            return "AVRO", json.loads(schema_str)
        except Exception as exc:
            raise SchemaRegistryError(f"Registered Avro schema is not valid JSON: {exc}") from exc
    if schema_type == "JSON":
        if isinstance(schema_str, (dict, list)):
            return "JSON", schema_str
        try:
            return "JSON", json.loads(schema_str)
        except Exception:
            return "JSON", schema_str
    return schema_type, schema_str


def decode_avro_body(parsed_schema: Any, body: bytes) -> Any:
    """Decode a Confluent Avro payload body with ``fastavro.schemaless_reader``."""
    try:
        import fastavro
    except ImportError as exc:
        raise SchemaRegistryError("fastavro is required to decode Avro Schema Registry payloads") from exc
    try:
        return fastavro.schemaless_reader(io.BytesIO(body), parsed_schema)
    except Exception as exc:
        raise SchemaRegistryError(f"Avro schemaless decode failed: {exc}") from exc


def decode_with_registered_schema(schema_doc: dict[str, Any], body: bytes) -> Any:
    """Decode a Confluent body using the registry document — fail-closed on unsupported types."""
    schema_type, parsed = _parse_registered_schema(schema_doc)
    if schema_type == "JSON":
        try:
            return json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SchemaRegistryError(f"JSON Schema Registry payload is not valid JSON: {exc}") from exc
    if schema_type == "AVRO":
        return decode_avro_body(parsed, body)
    if schema_type == "PROTOBUF":
        raise SchemaRegistryError(
            "Protobuf Schema Registry payloads are not supported yet — "
            "refuse silent JSON decode of binary contracts"
        )
    raise SchemaRegistryError(
        f"Unsupported Schema Registry schemaType={schema_type!r} — refuse silent decode"
    )


def schema_map_from_registry_doc(schema_doc: dict[str, Any]) -> dict[str, str]:
    """Derive DataFlow logical field map from a registry subject/schema document."""
    schema_type, parsed = _parse_registered_schema(schema_doc)
    if schema_type == "AVRO":
        from services.avro_schema import schema_map_from_avro

        return schema_map_from_avro(parsed)
    if schema_type == "JSON" and isinstance(parsed, dict):
        props = parsed.get("properties")
        if isinstance(props, dict) and props:
            out: dict[str, str] = {}
            for name, prop in props.items():
                if isinstance(prop, dict):
                    jtype = str(prop.get("type") or "string").lower()
                    out[str(name)] = {
                        "string": "TEXT",
                        "integer": "INTEGER",
                        "number": "DECIMAL",
                        "boolean": "BOOLEAN",
                        "array": "ARRAY",
                        "object": "JSON",
                        "null": "TEXT",
                    }.get(jtype, "TEXT")
                else:
                    out[str(name)] = "TEXT"
            return out
    return {}


def decode_kafka_value(
    raw: bytes | bytearray | memoryview | str | None,
    *,
    registry_url: str = "",
) -> Any:
    """Decode a Kafka value: Confluent wire (+ registry-typed decode) or plain JSON/text.

    When ``registry_url`` is set and the payload is Confluent-framed, the schema id
    is fetched fail-closed and decoded per ``schemaType``. Plain JSON without a
    magic byte is still accepted (common for non-Registry producers).
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
            schema_doc = fetch_schema(registry_url, schema_id)
            return decode_with_registered_schema(schema_doc, body)
        # No registry URL — only accept UTF-8 JSON bodies (legacy JSON subjects).
        try:
            return json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SchemaRegistryError(
                f"Confluent payload for schema id {schema_id} is not valid JSON and "
                f"no schema_registry_url was configured to resolve Avro/Protobuf: {exc}"
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
