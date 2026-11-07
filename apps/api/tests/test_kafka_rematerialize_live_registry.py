"""Kafka Schema Registry rematerialize when live carriers differ from Map stamps."""

from __future__ import annotations

from unittest.mock import patch


def test_kafka_rematerialize_when_registry_integer_vs_map_varchar():
    from connectors.kafka_writer import _kafka_rematerialize_if_physical_differs

    batch = _kafka_rematerialize_if_physical_differs(
        physical={"qty": "INTEGER", "QTY": "INTEGER"},
        dest_types={"qty": "VARCHAR"},
        target_cols=["qty"],
        headers=["qty"],
        data_rows=[["7"], ["x"]],
        mappings=[{"source": "qty", "target": "qty", "target_type": "VARCHAR"}],
        column_types={"qty": "VARCHAR"},
        logical_types=["VARCHAR"],
        policy="quarantine",
    )
    assert batch is not None
    mapped_rows, _errs, rejected, live = batch
    assert "INT" in str(live.get("qty") or "").upper()
    assert len(mapped_rows) + len(rejected) >= 1


def test_kafka_rematerialize_refuses_map_varchar_gap_fill():
    """Incomplete Registry map must not soft-invent Map VARCHAR for gaps."""
    from connectors.kafka_writer import _kafka_rematerialize_if_physical_differs

    batch = _kafka_rematerialize_if_physical_differs(
        physical={"id": "INTEGER"},
        dest_types={"id": "VARCHAR", "amount": "VARCHAR"},
        target_cols=["id", "amount"],
        headers=["id", "amount"],
        data_rows=[["1", "9.99"]],
        mappings=[
            {"source": "id", "target": "id", "target_type": "VARCHAR"},
            {"source": "amount", "target": "amount", "target_type": "VARCHAR"},
        ],
        column_types={"id": "VARCHAR", "amount": "VARCHAR"},
        logical_types=["VARCHAR", "VARCHAR"],
        policy="quarantine",
    )
    assert batch is None


def test_kafka_force_remap_when_carriers_match_partial_studio():
    from connectors.kafka_writer import _kafka_rematerialize_if_physical_differs

    batch = _kafka_rematerialize_if_physical_differs(
        physical={"qty": "INTEGER"},
        dest_types={"qty": "INTEGER"},
        target_cols=["qty"],
        headers=["qty"],
        data_rows=[["7"]],
        mappings=[{"source": "qty", "target": "qty", "target_type": "VARCHAR"}],
        column_types={"qty": "VARCHAR"},
        logical_types=["VARCHAR"],
        policy="quarantine",
        force_remap=True,
    )
    assert batch is not None
    _rows, _errs, _rej, live = batch
    assert "INT" in str(live.get("qty") or "").upper()


def test_kafka_schemaless_refuses_partial_studio():
    """Schemaless topic + partial Studio — fail-closed, no Map VARCHAR invent."""
    from connectors.kafka_writer import write_mapped_rows

    with patch("connectors.kafka_writer._producer") as prod:
        mock_prod = prod.return_value
        mock_prod.send.return_value.get.return_value = None
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
            headers=["id", "amount"],
            data_rows=[["1", "9.99"]],
            mappings=[
                {"source": "id", "target": "id", "target_type": "VARCHAR"},
                {"source": "amount", "target": "amount", "target_type": "VARCHAR"},
            ],
            column_types={"id": "VARCHAR", "amount": "VARCHAR"},
            destination_column_types={"id": "INTEGER"},
            create_table=True,
        )
    prod.assert_not_called()
    assert result.ok is False
    assert "amount" in (result.error or "").lower()


def test_kafka_registry_first_register_refuses_partial_studio():
    """Missing subject + create_table + partial Studio — refuse Map invent."""
    from connectors.kafka_writer import write_mapped_rows

    with patch(
        "connectors.kafka_writer._fetch_kafka_physical_types",
        return_value=({}, None, False),
    ), patch(
        "connectors.confluent_schema_registry.register_json_schema",
    ) as reg:
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
            headers=["id", "amount"],
            data_rows=[["1", "9.99"]],
            mappings=[
                {"source": "id", "target": "id", "target_type": "VARCHAR"},
                {"source": "amount", "target": "amount", "target_type": "VARCHAR"},
            ],
            column_types={"id": "VARCHAR", "amount": "VARCHAR"},
            destination_column_types={"id": "INTEGER"},
            schema_registry_url="http://registry:8081",
            create_table=True,
        )
    reg.assert_not_called()
    assert result.ok is False
    assert "amount" in (result.error or "").lower()


def test_logical_from_json_schema_decimal_media_type():
    from connectors.confluent_schema_registry import _logical_from_json_schema_prop

    assert (
        _logical_from_json_schema_prop(
            {
                "type": ["string", "null"],
                "contentMediaType": "application/x-decimal",
            }
        )
        == "DECIMAL"
    )
    assert (
        _logical_from_json_schema_prop({"type": ["integer", "null"]}) == "INTEGER"
    )
    assert _logical_from_json_schema_prop({"type": "number"}) == "FLOAT"


def test_fetch_kafka_physical_types_uses_live_subject():
    from connectors.kafka_writer import _fetch_kafka_physical_types

    doc = {
        "id": 42,
        "schemaType": "JSON",
        "schema": json_schema_body(),
    }
    with patch(
        "connectors.confluent_schema_registry.fetch_latest_subject_schema",
        return_value=doc,
    ):
        physical, schema_id, exists = _fetch_kafka_physical_types(
            "http://registry:8081", "orders", ["qty", "flag"]
        )
    assert exists is True
    assert schema_id == 42
    assert physical.get("qty") == "INTEGER"
    assert physical.get("flag") == "BOOLEAN"


def test_kafka_writer_uses_live_schema_id_not_register():
    from connectors.kafka_writer import write_mapped_rows

    register = patch(
        "connectors.confluent_schema_registry.register_json_schema",
    )
    fetch = patch(
        "connectors.kafka_writer._fetch_kafka_physical_types",
        return_value=({"id": "INTEGER"}, 99, True),
    )
    producer = patch(
        "connectors.kafka_writer._producer",
    )
    with fetch, register as reg, producer as prod:
        mock_prod = prod.return_value
        mock_prod.send.return_value.get.return_value = None
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
            mappings=[
                {"source": "id", "target": "id", "target_type": "VARCHAR"}
            ],
            column_types={"id": "VARCHAR"},
            schema_registry_url="http://registry:8081",
            create_table=False,
            conflict_columns=["id"],
        )
    reg.assert_not_called()
    assert prod.call_args.kwargs.get("schema_id") == 99 or (
        prod.call_args[1].get("schema_id") == 99
    )
    assert result.ok is True


def test_kafka_writer_refuses_register_when_subject_exists_untyped():
    from connectors.kafka_writer import write_mapped_rows

    with patch(
        "connectors.kafka_writer._fetch_kafka_physical_types",
        return_value=({}, 55, True),
    ), patch(
        "connectors.confluent_schema_registry.register_json_schema",
    ) as reg:
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
            mappings=[{"source": "id", "target": "id", "target_type": "VARCHAR"}],
            column_types={"id": "VARCHAR"},
            schema_registry_url="http://registry:8081",
            create_table=True,
        )
    reg.assert_not_called()
    assert result.ok is False
    assert "live field types" in (result.error or "").lower()


def test_kafka_writer_refuses_partial_registry_coverage():
    """Subject exists with only some mapped fields typed — refuse Map invent."""
    from connectors.kafka_writer import write_mapped_rows

    with patch(
        "connectors.kafka_writer._fetch_kafka_physical_types",
        return_value=({"id": "VARCHAR"}, 55, True),
    ), patch(
        "connectors.confluent_schema_registry.register_json_schema",
    ) as reg:
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
            headers=["id", "amount"],
            data_rows=[["1", "9.99"]],
            mappings=[
                {"source": "id", "target": "id", "target_type": "VARCHAR"},
                {"source": "amount", "target": "amount", "target_type": "VARCHAR"},
            ],
            column_types={"id": "VARCHAR", "amount": "VARCHAR"},
            schema_registry_url="http://registry:8081",
            create_table=True,
        )
    reg.assert_not_called()
    assert result.ok is False
    assert "amount" in (result.error or "").lower()
    assert "refuse" in (result.error or "").lower()


def json_schema_body() -> str:
    import json

    return json.dumps(
        {
            "type": "object",
            "properties": {
                "qty": {"type": ["integer", "null"]},
                "flag": {"type": ["boolean", "null"]},
            },
        }
    )
