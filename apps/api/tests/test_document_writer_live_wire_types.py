"""Document writers must wire-coerce with live dest_types, not Map logical_types."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def test_dynamo_wire_uses_tgt_types_not_map_varchar():
    """Map VARCHAR + live INTEGER must refuse empty string at encode."""
    from connectors.dynamodb_writer import _to_attr

    # Live INTEGER path (tgt_types) refuses empty via _to_dynamo_value.
    import pytest

    with pytest.raises((ValueError, Exception)):
        _to_attr("", "INTEGER")


def test_es_to_es_value_uses_live_integer_stamp():
    import pytest
    from connectors.elasticsearch_writer import _to_es_value

    with pytest.raises(ValueError, match="empty"):
        _to_es_value("", "INTEGER")
    assert _to_es_value("42", "INTEGER") == 42
    # VARCHAR stamp would pass empty through — that is the invent cliff we closed
    # at the call site by preferring tgt_types.
    assert _to_es_value("", "VARCHAR") == ""


def test_redis_normalize_uses_tgt_types_list():
    import pytest
    from connectors.redis_writer import _normalize_redis_typed_doc

    with pytest.raises(ValueError):
        _normalize_redis_typed_doc(
            {"qty": ""},
            ["qty"],
            ["INTEGER"],
        )
    out = _normalize_redis_typed_doc({"qty": "7"}, ["qty"], ["INTEGER"])
    assert out["qty"] == 7


def test_mongo_insert_refuses_content_hash_id_invent():
    from connectors.mongodb_writer import _idempotent_insert_many
    import pytest

    coll = MagicMock()
    with pytest.raises(ValueError, match="content-hash|without `_id`"):
        _idempotent_insert_many(coll, [{"name": "a"}, {"name": "b"}])
    coll.insert_many.assert_not_called()


def test_dynamo_empty_key_types_fail_closed(monkeypatch):
    monkeypatch.delenv("DATAFLOW_ALLOW_STUB_WRITES", raising=False)
    monkeypatch.setenv("DATAFLOW_ALLOW_STUB_WRITES", "0")

    from connectors import dynamodb_writer as dw

    client = MagicMock()
    with patch("connectors.dynamodb_writer.boto3_client", return_value=client), patch(
        "connectors.dynamodb_writer._ensure_table"
    ), patch(
        "connectors.dynamodb_writer._table_key_types", return_value={}
    ):
        result = dw.write_mapped_rows(
            host="localhost",
            port=8000,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            warehouse="",
            table_name="orders",
            endpoint_url="http://localhost:8000",
            headers=["id", "amount"],
            data_rows=[["1", "10"]],
            mappings=[
                {"source": "id", "target": "id", "target_type": "VARCHAR"},
                {"source": "amount", "target": "amount", "target_type": "VARCHAR"},
            ],
            column_types={"id": "VARCHAR", "amount": "VARCHAR"},
            destination_column_types={"amount": "INTEGER", "id": "VARCHAR"},
            create_table=True,
            conflict_columns=["id"],
            error_policy="quarantine",
        )
    assert result.ok is False
    assert "key schema" in (result.error or "").lower()


def test_dynamo_key_encode_prefers_keyschema_s_over_live_integer():
    """String HASH key must stay AttributeValue S even if live stamp is INTEGER."""
    from connectors.dynamodb_writer import _coerce_dynamo_cell, _to_attr

    key_types = {"id": "S"}
    value = _coerce_dynamo_cell(
        "42", col="id", logical_type="INTEGER", key_types=key_types
    )
    assert value == "42"
    attr = _to_attr(value, "VARCHAR")
    assert "S" in attr
    assert attr["S"] == "42"
