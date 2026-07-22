"""Proofs: Redshift SUPER/upsert honesty + Confluent Schema Registry fail-closed."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.confluent_schema_registry import (  # noqa: E402
    SchemaRegistryError,
    decode_kafka_value,
    encode_confluent_json,
    is_confluent_wire,
    register_json_schema,
    split_confluent_wire,
)
from connectors.postgresql_writer import uses_pg_on_conflict_upsert  # noqa: E402
from connectors.saas_common import is_auth_error  # noqa: E402
from services.schema_introspect import _pg_to_logical  # noqa: E402
from services.type_system import ddl_type  # noqa: E402
from services.connector_capability_registry import CAPABILITY_REGISTRY  # noqa: E402


def test_pg_to_logical_redshift_super_varbyte():
    assert _pg_to_logical("super") == "JSON"
    assert _pg_to_logical("varbyte") == "BINARY"
    assert _pg_to_logical("varbyte(1024)") == "BINARY"
    # Existing PG floats still honest.
    assert _pg_to_logical("double precision") == "FLOAT"
    assert _pg_to_logical("numeric(12,4)") == "DECIMAL(12,4)"


def test_redshift_ddl_json_is_super_not_varchar_max():
    assert ddl_type("redshift", "JSON") == "SUPER"
    assert ddl_type("redshift", "TEXT") == "VARCHAR(65535)"
    assert "VARCHAR(max)" not in ddl_type("redshift", "TEXT").lower()


def test_redshift_capability_honest_no_merge_no_varchar_max():
    caps = CAPABILITY_REGISTRY["redshift"]
    assert caps["supports_upsert"] is True
    assert caps["supports_merge"] is False
    issues = " ".join(caps.get("common_issues") or [])
    assert "VARCHAR(65535)" in issues
    assert "VARCHAR(max)" not in issues
    assert "ON CONFLICT" in issues or "delete+insert" in issues.lower()


def test_redshift_never_uses_on_conflict():
    assert uses_pg_on_conflict_upsert("postgresql") is True
    assert uses_pg_on_conflict_upsert("redshift") is False
    assert uses_pg_on_conflict_upsert("amazon_redshift") is False
    assert uses_pg_on_conflict_upsert("redshift_serverless") is False


def test_confluent_wire_roundtrip_json():
    framed = encode_confluent_json(42, {"id": 1, "name": "a"})
    assert is_confluent_wire(framed)
    sid, body = split_confluent_wire(framed)
    assert sid == 42
    assert json.loads(body) == {"id": 1, "name": "a"}


def test_decode_plain_json_without_registry():
    assert decode_kafka_value(b'{"a":1}') == {"a": 1}
    assert decode_kafka_value('{"a":1}') == {"a": 1}


def test_decode_confluent_wire_fetches_schema_when_registry_set():
    framed = encode_confluent_json(7, {"x": True})
    with patch(
        "connectors.confluent_schema_registry.fetch_schema",
        return_value={"schema": "{}", "schemaType": "JSON"},
    ) as fetch:
        out = decode_kafka_value(framed, registry_url="http://registry:8081")
    assert out == {"x": True}
    fetch.assert_called_once_with("http://registry:8081", 7)


def test_decode_confluent_wire_fail_closed_when_registry_unreachable():
    framed = encode_confluent_json(9, {"x": 1})

    def _boom(*_a, **_k):
        raise SchemaRegistryError("down")

    with patch("connectors.confluent_schema_registry.fetch_schema", side_effect=_boom):
        with pytest.raises(SchemaRegistryError):
            decode_kafka_value(framed, registry_url="http://registry:8081")


def test_register_json_schema_fail_closed_on_http_error():
    mock_resp = MagicMock()
    mock_resp.status_code = 500
    mock_resp.text = "boom"
    with patch("requests.post", return_value=mock_resp):
        with pytest.raises(SchemaRegistryError, match="500"):
            register_json_schema("http://registry:8081", "topic-value", '{"type":"object"}')


def test_kafka_writer_aborts_when_registry_register_fails():
    from connectors.kafka_writer import write_mapped_rows

    with patch(
        "connectors.confluent_schema_registry.register_json_schema",
        side_effect=SchemaRegistryError("register failed"),
    ):
        result = write_mapped_rows(
            host="localhost",
            port=9092,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="events",
            headers=["id"],
            data_rows=[["1"]],
            mappings=[{"source": "id", "target": "id", "transform": "none"}],
            column_types={"id": "string"},
            schema_registry_url="http://registry:8081",
        )
    assert result.ok is False
    assert "register failed" in (result.error or "")


def test_is_auth_error_detects_401_403():
    assert is_auth_error(Exception("401 Unauthorized"))
    assert is_auth_error(Exception("HTTP 403 Forbidden"))
    assert not is_auth_error(Exception("404 Not Found"))
    assert not is_auth_error(Exception("timeout"))


def test_salesforce_describe_auth_failure_not_swallowed():
    from connectors import salesforce as sf

    with patch.object(sf, "describe_sobject", side_effect=Exception("401 Unauthorized")):
        with patch.object(sf, "_access", return_value=("tok", "https://example.salesforce.com")):
            with pytest.raises(Exception, match="401"):
                sf.read_object(cfg={"api_key": "tok"}, object="Account", limit=1)


def test_hubspot_describe_auth_failure_not_swallowed():
    from connectors import hubspot as hs

    with patch.object(hs, "describe_properties", side_effect=Exception("403 Forbidden")):
        with patch.object(hs, "_access", return_value=("tok", "https://api.hubapi.com")):
            with pytest.raises(Exception, match="403"):
                hs.read_object(cfg={"api_key": "tok"}, object="contacts", limit=1)
