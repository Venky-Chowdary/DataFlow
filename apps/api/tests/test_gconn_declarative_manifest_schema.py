from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from connectors.sdk.declarative.errors import ManifestError, SchemaDriftError
from connectors.sdk.declarative.manifest import Manifest, parse_manifest
from connectors.sdk.declarative.schema import (
    detect_stream_drift,
    infer_json_schema,
    validate_stream_schema,
)


def _manifest() -> dict:
    return {
        "name": "demo",
        "base_url": "https://api.example.test/v1/",
        "auth": {"type": "api_key", "location": "header", "name": "X-API-Key"},
        "streams": [
            {
                "name": "users",
                "path": "users",
                "records_path": "data",
                "primary_key": ["id"],
                "cursor": {
                    "field": "updated_at",
                    "request_param": "updated_since",
                    "format": "iso8601",
                    "lookback_s": 10,
                },
                "paginator": {"type": "cursor", "cursor_param": "after", "cursor_path": "next"},
                "json_schema": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "updated_at": {"type": "string", "format": "date-time"},
                    },
                },
            }
        ],
        "defaults": {
            "timeout_s": 12,
            "max_pages": 30,
            "page_size": 25,
            "retry": {"max_attempts": 3, "base_delay": 0.2, "max_delay": 5},
        },
        "rate_limit": {"per_second": 4},
    }


def test_manifest_parses_strict_frozen_defaults_and_nested_specs() -> None:
    manifest = parse_manifest(_manifest())

    assert isinstance(manifest, Manifest)
    assert manifest.defaults.timeout_s == 12
    assert manifest.defaults.retry.max_attempts == 3
    assert manifest.streams[0].paginator.type == "cursor"
    assert manifest.streams[0].paginator.page_size == 25
    assert manifest.streams[0].cursor.lookback_s == 10
    assert manifest.rate_limit_per_second == 4
    with pytest.raises(FrozenInstanceError):
        manifest.name = "changed"


@pytest.mark.parametrize(
    ("mutate", "path"),
    [
        (lambda raw: raw.update({"unknown": 1}), "unknown"),
        (lambda raw: raw.update({"base_url": "file:///tmp/data"}), "base_url"),
        (
            lambda raw: raw["streams"][0].update({"unexpected": True}),
            "streams[0].unexpected",
        ),
        (
            lambda raw: raw["streams"][0]["paginator"].update({"type": "mystery"}),
            "streams[0].paginator.type",
        ),
    ],
)
def test_manifest_errors_name_the_bad_path(mutate, path: str) -> None:
    raw = _manifest()
    mutate(raw)

    with pytest.raises(ManifestError, match=path.replace("[", r"\[").replace("]", r"\]")):
        parse_manifest(raw)


def test_schema_inference_preserves_null_only_and_nullable_types() -> None:
    schema = infer_json_schema(
        [
            {
                "id": 1,
                "only_null": None,
                "active": True,
                "profile": {"email": "one@example.test"},
                "tags": ["one"],
                "mixed": 1,
            },
            {
                "id": 2,
                "only_null": None,
                "active": None,
                "profile": None,
                "tags": None,
                "mixed": "two",
            },
        ]
    )

    props = schema["properties"]
    assert props["id"]["type"] == "integer"
    assert props["only_null"]["type"] == ["null"]
    assert "null" in props["only_null"]["description"].lower()
    assert props["active"]["type"] == ["boolean", "null"]
    assert props["profile"]["type"] == ["object", "null"]
    assert props["tags"]["type"] == ["array", "null"]
    assert props["mixed"]["type"] == ["integer", "string"]


def test_schema_validation_requires_primary_key_and_cursor_fields() -> None:
    with pytest.raises(ManifestError, match="primary_key"):
        validate_stream_schema(
            {"type": "object", "properties": {"updated_at": {"type": "string"}}},
            primary_key=["id"],
            cursor_field="updated_at",
            path="streams[0].json_schema",
        )

    raw = _manifest()
    raw["streams"][0]["json_schema"]["properties"].pop("id")
    with pytest.raises(ManifestError, match="primary_key"):
        parse_manifest(raw)

    with pytest.raises(ManifestError, match="cursor"):
        validate_stream_schema(
            {"type": "object", "properties": {"id": {"type": "integer"}}},
            primary_key=["id"],
            cursor_field="updated_at",
            path="streams[0].json_schema",
        )


def test_structural_stream_drift_reports_changes_and_protects_cursor_and_pk() -> None:
    old = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "updated_at": {"type": "string", "format": "date-time"},
            "legacy": {"type": "string"},
        },
    }
    new = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "added": {"type": "boolean"},
        },
    }

    drift = detect_stream_drift(old, new)
    assert drift["added"] == ["added"]
    assert drift["removed"] == ["legacy", "updated_at"]
    assert drift["type_changed"] == [
        {"field": "id", "old": "integer", "new": "string"}
    ]

    with pytest.raises(SchemaDriftError, match="primary key"):
        detect_stream_drift(old, new, primary_key=["legacy"])
    with pytest.raises(SchemaDriftError, match="cursor"):
        detect_stream_drift(old, new, cursor_field="updated_at")
