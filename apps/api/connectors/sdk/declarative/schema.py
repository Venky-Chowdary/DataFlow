from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

from connectors.sdk.declarative.errors import ManifestError, SchemaDriftError
from services.schema_drift import detect_schema_drift
from services.schema_fingerprint import fingerprint_schema
from services.schema_inference import infer_schema_map

_LOGICAL_TO_JSON: dict[str, str] = {
    "ARRAY": "array",
    "BINARY": "string",
    "BOOLEAN": "boolean",
    "DATE": "string",
    "DATETIME": "string",
    "DECIMAL": "number",
    "DOUBLE": "number",
    "FLOAT": "number",
    "INT": "integer",
    "INTEGER": "integer",
    "JSON": "object",
    "JSONB": "object",
    "BIGINT": "integer",
    "NUMERIC": "number",
    "OBJECT": "object",
    "REAL": "number",
    "SMALLINT": "integer",
    "STRING": "string",
    "TEXT": "string",
    "TIME": "string",
    "TIMESTAMP": "string",
    "TIMESTAMP_NTZ": "string",
    "TIMESTAMP_TZ": "string",
    "TIMESTAMPTZ": "string",
    "UUID": "string",
    "VARIANT": "object",
    "VARCHAR": "string",
}


def infer_json_schema(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    keys = list(dict.fromkeys(key for record in records for key in record))
    samples: dict[str, list[str]] = {}
    for key in keys:
        values = [record.get(key) for record in records if key in record]
        non_null = [value for value in values if value is not None]
        if non_null:
            samples[str(key)] = [
                json.dumps(value, sort_keys=True, separators=(",", ":"))
                if isinstance(value, (dict, list))
                else str(value)
                for value in non_null
            ]
    inferred, _notes = infer_schema_map(samples) if samples else ({}, {})
    properties: dict[str, Any] = {}
    required: list[str] = []
    for key in keys:
        values = [record.get(key) for record in records if key in record]
        non_null = [value for value in values if value is not None]
        logical: str | None = None
        if not non_null:
            properties[str(key)] = {
                "type": ["null"],
                "description": "Observed only null values; type is unknown.",
            }
        else:
            logical = str(inferred.get(str(key), "VARCHAR")).upper()
            observed_types: set[str] = set()
            for value in non_null:
                if isinstance(value, bool):
                    observed_types.add("boolean")
                elif isinstance(value, int):
                    observed_types.add("integer")
                elif isinstance(value, float):
                    observed_types.add("number")
                elif isinstance(value, dict):
                    observed_types.add("object")
                elif isinstance(value, list):
                    observed_types.add("array")
                else:
                    observed_types.add(_LOGICAL_TO_JSON.get(logical, "string"))
            if "number" in observed_types:
                observed_types.discard("integer")
            type_order = ("boolean", "integer", "number", "string", "array", "object", "null")
            field_types = [kind for kind in type_order if kind in observed_types]
            if len(non_null) < len(values):
                field_types.append("null")
            field_type: str | list[str] = (
                field_types[0] if len(field_types) == 1 else field_types
            )
            properties[str(key)] = {"type": field_type}
        if non_null and logical in {
            "DATE", "DATETIME", "TIME", "TIMESTAMP", "TIMESTAMP_NTZ",
            "TIMESTAMP_TZ", "TIMESTAMPTZ",
        }:
            properties[str(key)]["format"] = {
                "DATE": "date",
                "DATETIME": "date-time",
                "TIME": "time",
                "TIMESTAMP": "date-time",
                "TIMESTAMP_NTZ": "date-time",
                "TIMESTAMP_TZ": "date-time",
                "TIMESTAMPTZ": "date-time",
            }[logical]
        if non_null and logical == "BINARY":
            properties[str(key)]["contentEncoding"] = "base64"
        if records and all(key in record for record in records):
            required.append(str(key))
    result: dict[str, Any] = {"type": "object", "properties": properties}
    if required:
        result["required"] = required
    return result


def validate_stream_schema(
    schema: Mapping[str, Any],
    *,
    primary_key: Sequence[str] = (),
    cursor_field: str = "",
    path: str = "json_schema",
) -> None:
    if schema.get("type") != "object" or not isinstance(schema.get("properties"), Mapping):
        raise ManifestError(f"{path}: expected an object schema with properties", path=path)
    properties = schema["properties"]
    for key in primary_key:
        if key not in properties:
            raise ManifestError(
                f"{path}.primary_key: field {key!r} is absent from schema",
                path=f"{path}.primary_key",
            )
    if cursor_field and _schema_path_definition(schema, cursor_field) is None:
        raise ManifestError(
            f"{path}.cursor: field {cursor_field!r} is absent from schema",
            path=f"{path}.cursor",
        )


def _schema_path_definition(schema: Mapping[str, Any], path: str) -> Mapping[str, Any] | None:
    current: Mapping[str, Any] = schema
    for component in path.split("."):
        properties = current.get("properties")
        if not isinstance(properties, Mapping):
            return None
        definition = properties.get(component)
        if not isinstance(definition, Mapping):
            return None
        current = definition
    return current


def _schema_columns(schema: Mapping[str, Any]) -> tuple[list[str], dict[str, str]]:
    properties = schema.get("properties")
    if not isinstance(properties, Mapping):
        return [], {}
    columns: list[str] = []
    types: dict[str, str] = {}
    logical_by_json = {
        "array": "ARRAY",
        "boolean": "BOOLEAN",
        "integer": "INTEGER",
        "number": "DECIMAL",
        "object": "JSON",
        "string": "VARCHAR",
    }
    for key, definition in properties.items():
        if not isinstance(definition, Mapping):
            continue
        columns.append(str(key))
        json_type = definition.get("type")
        if isinstance(json_type, list):
            json_type = next((item for item in json_type if item != "null"), "VARCHAR")
        types[str(key)] = logical_by_json.get(str(json_type), "VARCHAR")
    return columns, types


def detect_stream_drift(
    old_json_schema: Mapping[str, Any],
    new_json_schema: Mapping[str, Any],
    *,
    primary_key: Sequence[str] = (),
    cursor_field: str = "",
) -> dict[str, Any]:
    old_properties = old_json_schema.get("properties")
    new_properties = new_json_schema.get("properties")
    if not isinstance(old_properties, Mapping) or not isinstance(new_properties, Mapping):
        raise ManifestError("schema drift requires object schemas with properties", path="json_schema")
    old_columns, old_types = _schema_columns(old_json_schema)
    new_columns, new_types = _schema_columns(new_json_schema)
    removed = sorted(field for field in old_properties if field not in new_properties)
    for field in primary_key:
        if (
            _schema_path_definition(old_json_schema, field) is not None
            and _schema_path_definition(new_json_schema, field) is None
        ):
            raise SchemaDriftError(f"schema drift removed primary key field {field!r}")
    if (
        cursor_field
        and _schema_path_definition(old_json_schema, cursor_field) is not None
        and _schema_path_definition(new_json_schema, cursor_field) is None
    ):
        raise SchemaDriftError(f"schema drift removed cursor field {cursor_field!r}")

    detect_schema_drift(
        source_columns=new_columns,
        source_schema=new_types,
        target_columns=[],
        target_schema={},
        stored_source_fp=fingerprint_schema(old_columns, old_types),
        previous_source_columns=old_columns,
        previous_source_schema=old_types,
        previous_primary_key=list(primary_key),
        live_primary_key=list(primary_key),
        cursor_fields=[cursor_field] if cursor_field and "." not in cursor_field else [],
    )

    added = sorted(field for field in new_properties if field not in old_properties)
    type_changed = []
    for field in old_properties:
        if field not in new_properties:
            continue
        old_type = old_properties[field].get("type") if isinstance(old_properties[field], Mapping) else None
        new_type = new_properties[field].get("type") if isinstance(new_properties[field], Mapping) else None
        if old_type != new_type:
            type_changed.append({"field": field, "old": old_type, "new": new_type})
    return {"added": added, "removed": removed, "type_changed": type_changed}
