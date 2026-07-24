"""CRM Describe + Kafka sample typing — logical mapping honesty."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.schema_introspect import (  # noqa: E402
    _kafka_value_to_logical,
    hubspot_property_to_logical,
    salesforce_field_to_logical,
)
from src.transfer.connector_capabilities import get_capabilities  # noqa: E402
from src.transfer.registry import PRODUCTION_SKU  # noqa: E402


def test_salesforce_field_type_map():
    assert salesforce_field_to_logical("boolean") == "BOOLEAN"
    assert salesforce_field_to_logical("int") == "INTEGER"
    assert salesforce_field_to_logical("double") == "FLOAT"
    assert salesforce_field_to_logical("currency", precision=18, scale=2) == "DECIMAL(18,2)"
    assert salesforce_field_to_logical("percent", precision=5, scale=2) == "DECIMAL(5,2)"
    # Bare currency/percent must not invent untyped DECIMAL.
    assert salesforce_field_to_logical("currency") == "DECIMAL(18,2)"
    assert salesforce_field_to_logical("percent") == "DECIMAL(18,2)"
    assert salesforce_field_to_logical("date") == "DATE"
    assert salesforce_field_to_logical("datetime") == "TIMESTAMPTZ"
    assert salesforce_field_to_logical("base64") == "BINARY"
    assert salesforce_field_to_logical("address") == "JSON"
    assert salesforce_field_to_logical("id") == "TEXT"
    assert salesforce_field_to_logical("picklist") == "TEXT"
    assert salesforce_field_to_logical("email") == "TEXT"


def test_hubspot_property_type_map():
    assert hubspot_property_to_logical("bool") == "BOOLEAN"
    assert hubspot_property_to_logical("number") == "DECIMAL"
    assert hubspot_property_to_logical("number", number_display_hint="currency") == "DECIMAL(18,2)"
    assert hubspot_property_to_logical("number", name="num_employees") == "INTEGER"
    assert hubspot_property_to_logical(
        "number", field_type="calculation_equation"
    ) == "FLOAT"
    assert hubspot_property_to_logical("date") == "DATE"
    assert hubspot_property_to_logical("datetime") == "TIMESTAMPTZ"
    assert hubspot_property_to_logical("json") == "JSON"
    assert hubspot_property_to_logical("string") == "TEXT"
    assert hubspot_property_to_logical("enumeration") == "TEXT"
    assert hubspot_property_to_logical("string", field_type="booleancheckbox") == "BOOLEAN"


def test_kafka_value_logical_inference():
    assert _kafka_value_to_logical(True) == "BOOLEAN"
    assert _kafka_value_to_logical(42) == "INTEGER"
    assert _kafka_value_to_logical(1.5) == "FLOAT"
    assert _kafka_value_to_logical({"a": 1}) == "JSON"
    assert _kafka_value_to_logical([1, 2]) == "ARRAY"
    assert _kafka_value_to_logical(None) == ""


def test_crm_kafka_caps_claim_introspect():
    for driver in ("salesforce", "hubspot", "kafka"):
        caps = get_capabilities(driver, driver)
        assert caps.get("introspect") is True, driver
        assert caps.get("read") is True and caps.get("write") is True, driver


def test_kafka_empty_topic_does_not_invent_text_columns(monkeypatch):
    from connectors import kafka_reader as kr

    class _Empty:
        headers: list = []
        rows: list = []
        meta = {}

    monkeypatch.setattr(
        kr,
        "read_topic_batch",
        lambda **_kw: (_Empty(), None),
    )
    schema, native, warning = kr.infer_topic_schema({"host": "localhost"}, "empty.topic")
    assert schema == {}
    assert native == {}
    assert "no samples" in warning.lower()


def test_production_sku_includes_duplex_stores():
    needed = {
        ("database", "postgresql", "database", "dynamodb"),
        ("database", "postgresql", "database", "elasticsearch"),
        ("database", "postgresql", "database", "redis"),
        ("database", "postgresql", "database", "gcs"),
    }
    sku = set(PRODUCTION_SKU)
    missing = needed - sku
    assert not missing, missing
