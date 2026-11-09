"""Elasticsearch document identity is the operator's key, not a guessed ``*_id``.

Map (stream contract or a mapping ``primary_key`` flag) and Advanced settings
(the same contract, or ``destination.extra.primary_key``) must reach the
writer. A column named ``order_id`` is not a document id until one of those
paths names it. Two named keys that disagree write nothing.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from services.primary_key import (
    bind_explicit_elasticsearch_identity,
    explicit_identity_target_columns,
    resolve_identity_key,
    resolve_primary_key_columns,
)


MAPPINGS = [
    {"source": "order_id", "target": "order_id"},
    {"source": "sku", "target": "sku"},
    {"source": "amount", "target": "amount"},
]


def _write(**kwargs):
    from connectors.elasticsearch_writer import write_mapped_rows

    client = MagicMock()
    client.indices.exists.return_value = True
    captured: list[dict] = []

    def fake_bulk(_client, actions, raise_on_error=False):
        items = list(actions)
        captured.extend(items)
        return (len(items), [])

    base = dict(
        host="localhost",
        port=9200,
        database="",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name="orders",
        headers=["order_id", "sku", "amount"],
        data_rows=[["o-1", "sku-9", "10"]],
        mappings=list(MAPPINGS),
        column_types={"order_id": "TEXT", "sku": "TEXT", "amount": "TEXT"},
        create_table=False,
        write_mode="insert",
        error_policy="fail",
    )
    base.update(kwargs)
    with patch("connectors.elasticsearch_writer._client", return_value=client):
        with patch("elasticsearch.helpers.bulk", side_effect=fake_bulk):
            result = write_mapped_rows(**base)
    return result, captured, client


def test_map_stream_contract_is_the_document_id():
    result, captured, _client = _write(
        conflict_columns=[],
        stream_contracts=[{"name": "orders", "selected": True, "primary_key": "order_id"}],
    )
    assert result.ok is True
    assert captured and captured[0]["_id"] == "o-1"


def test_map_row_flag_is_the_document_id_without_a_contract():
    flagged = [dict(m) for m in MAPPINGS]
    flagged[0] = {**flagged[0], "primary_key": True}
    result, captured, _client = _write(conflict_columns=[], mappings=flagged)
    assert result.ok is True
    assert captured[0]["_id"] == "o-1"


def test_advanced_endpoint_identity_is_the_document_id():
    result, captured, _client = _write(
        conflict_columns=[],
        destination_extra={"primary_key": "order_id"},
    )
    assert result.ok is True
    assert captured[0]["_id"] == "o-1"


def test_map_flag_and_advanced_contract_together_use_that_key():
    flagged = [dict(m) for m in MAPPINGS]
    flagged[0] = {**flagged[0], "is_primary_key": True}
    result, captured, _client = _write(
        conflict_columns=["order_id"],
        mappings=flagged,
        stream_contracts=[{"selected": True, "primary_key": "order_id"}],
        destination_extra={"primary_key": "order_id"},
    )
    assert result.ok is True
    assert captured[0]["_id"] == "o-1"
    assert result.rows_written == 1


def test_explicit_key_wins_over_a_column_named_id():
    result, captured, _client = _write(
        headers=["id", "order_id"],
        data_rows=[["row-id", "o-1"]],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "order_id", "target": "order_id"},
        ],
        column_types={"id": "TEXT", "order_id": "TEXT"},
        stream_contracts=[{"selected": True, "primary_key": "order_id"}],
    )
    assert result.ok is True
    assert captured[0]["_id"] == "o-1"


def test_disagreement_writes_nothing_and_does_not_open_a_client():
    flagged = [dict(m) for m in MAPPINGS]
    flagged[1] = {**flagged[1], "primary_key": True}

    def boom(*_args, **_kwargs):
        raise AssertionError("Elasticsearch client must not open when identity disagrees")

    with patch("connectors.elasticsearch_writer._client", side_effect=boom):
        from connectors.elasticsearch_writer import write_mapped_rows

        result = write_mapped_rows(
            host="localhost",
            port=9200,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="orders",
            headers=["order_id", "sku"],
            data_rows=[["o-1", "sku-9"]],
            mappings=flagged,
            column_types={"order_id": "TEXT", "sku": "TEXT"},
            create_table=False,
            conflict_columns=[],
            stream_contracts=[{"selected": True, "primary_key": "order_id"}],
            error_policy="fail",
        )
    assert result.ok is False
    assert result.rows_written == 0
    assert "disagrees" in (result.error or "").lower()


def test_order_id_alone_is_not_invented_as_a_document_id():
    result, captured, _client = _write(conflict_columns=[])
    assert result.ok is False
    assert result.rows_written == 0
    assert captured == []
    assert "lack document identity" in (result.error or "")
    assert "Advanced" in (result.error or "")


def test_mapped_id_field_still_addresses_the_document():
    result, captured, _client = _write(
        headers=["id", "amount"],
        data_rows=[["7", "10"]],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "amount", "target": "amount"},
        ],
        column_types={"id": "TEXT", "amount": "TEXT"},
        conflict_columns=[],
    )
    assert result.ok is True
    assert captured[0]["_id"] == "7"


def test_validate_does_not_treat_a_lone_star_id_as_an_elasticsearch_key():
    src, tgt = resolve_identity_key(
        mappings=MAPPINGS,
        source_columns=["order_id", "sku", "amount"],
        dest_kind="elasticsearch",
        purpose="uniqueness",
    )
    assert src is None and tgt is None
    sources, targets = resolve_primary_key_columns(
        MAPPINGS,
        ["order_id", "sku", "amount"],
        "elasticsearch",
    )
    assert sources == [] and targets == []


def test_validate_uses_the_advanced_contract_and_the_map_flag():
    sources, targets = resolve_primary_key_columns(
        MAPPINGS,
        ["order_id", "sku", "amount"],
        "elasticsearch",
        stream_contracts=[{"selected": True, "primary_key": "order_id"}],
    )
    assert sources == ["order_id"] and targets == ["order_id"]
    flagged = [dict(m) for m in MAPPINGS]
    flagged[0] = {**flagged[0], "primary_key": True}
    sources, targets = resolve_primary_key_columns(
        flagged, ["order_id", "sku", "amount"], "elasticsearch"
    )
    assert sources == ["order_id"] and targets == ["order_id"]


def test_bind_leaves_non_elasticsearch_keys_unchanged():
    assert bind_explicit_elasticsearch_identity(
        "postgresql", MAPPINGS, ["id"]
    ) == ["id"]


def test_bind_fills_an_append_endpoint_key_the_duplicate_policy_gate_skipped():
    cols = bind_explicit_elasticsearch_identity(
        "elasticsearch",
        MAPPINGS,
        [],
        destination_extra={"primary_key": "order_id"},
    )
    assert cols == ["order_id"]


def test_bind_refuses_a_split_key():
    flagged = [dict(m) for m in MAPPINGS]
    flagged[1] = {**flagged[1], "identity": True}
    with pytest.raises(ValueError, match="disagrees"):
        bind_explicit_elasticsearch_identity(
            "opensearch",
            flagged,
            [],
            stream_contracts=[{"selected": True, "primary_key": "order_id"}],
        )


def test_explicit_sources_that_agree_are_one_key():
    flagged = [dict(m) for m in MAPPINGS]
    flagged[0] = {**flagged[0], "primary_key": True}
    cols, err = explicit_identity_target_columns(
        flagged,
        conflict_columns=["order_id"],
        stream_contracts=[{"selected": True, "primary_key": "order_id"}],
        destination_extra={"primary_keys": ["order_id"]},
    )
    assert err is None
    assert cols == ["order_id"]


def test_stream_helper_binds_es_key_and_keeps_append_insert():
    from types import SimpleNamespace

    from src.transfer.stream_es_identity import bind_stream_elasticsearch_write_mode

    run = SimpleNamespace(
        cfg=SimpleNamespace(
            dest_type="elasticsearch",
            mappings=MAPPINGS,
            pk_target_cols=[],
            pk_source_cols=[],
            destination=SimpleNamespace(extra={"primary_key": "order_id"}),
            effective_sync="full_refresh_append",
            write_mode="insert",
        )
    )
    bind_stream_elasticsearch_write_mode(run, [])
    assert run.cfg.pk_target_cols == ["order_id"]
    assert run.cfg.pk_source_cols == ["order_id"]
    assert run.cfg.write_mode == "insert"


def test_stream_helper_still_refuses_keyless_upsert():
    from types import SimpleNamespace

    from src.transfer.stream_es_identity import bind_stream_elasticsearch_write_mode

    run = SimpleNamespace(
        cfg=SimpleNamespace(
            dest_type="postgresql",
            mappings=MAPPINGS,
            pk_target_cols=[],
            pk_source_cols=[],
            destination=SimpleNamespace(extra={}),
            effective_sync="incremental_deduped",
            write_mode="insert",
        )
    )
    with pytest.raises(ValueError, match="primary_key"):
        bind_stream_elasticsearch_write_mode(run, [])


def test_stream_write_forwards_elasticsearch_identity(monkeypatch):
    from src.transfer.models import EndpointConfig
    from src.transfer.stream_write import _write_batch

    captured: dict = {}

    def fake_write(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            ok=True,
            rows_written=1,
            checksum="abc",
            table_name="orders",
            target_schema="localhost",
            driver="elasticsearch-py",
            rejected_rows=0,
            rejected_details=[],
            warnings=[],
            coerced_null_rows=0,
            rows_skipped=0,
            load_method=None,
            meta={},
            error=None,
        )

    monkeypatch.setattr(
        "connectors.elasticsearch_writer.write_mapped_rows", fake_write
    )
    rows, checksum, summary = _write_batch(
        "elasticsearch",
        EndpointConfig(
            kind="database",
            format="elasticsearch",
            host="es.internal",
            port=9200,
            extra={"primary_key": "order_id"},
        ),
        {"host": "es.internal", "port": 9200, "database": "", "username": "", "password": ""},
        "orders",
        ["order_id"],
        [["o-1"]],
        MAPPINGS,
        {"order_id": "TEXT"},
        False,
        None,
        0,
        1,
        0,
        write_mode="upsert",
        conflict_columns=["order_id"],
    )
    assert rows == 1 and checksum == "abc"
    assert captured["conflict_columns"] == ["order_id"]
    assert captured["write_mode"] == "upsert"
    assert captured["destination_extra"]["primary_key"] == "order_id"
    assert summary["table"] == "orders"


def test_adapter_write_forwards_elasticsearch_identity(monkeypatch):
    from src.transfer.adapters import _write_destination_database
    from src.transfer.models import EndpointConfig

    captured: dict = {}

    def fake_write(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            ok=True,
            rows_written=1,
            checksum="abc",
            table_name="orders",
            target_schema="localhost",
            driver="elasticsearch-py",
            rejected_rows=0,
            rejected_details=[],
            warnings=[],
            error=None,
        )

    monkeypatch.setattr(
        "connectors.elasticsearch_writer.write_mapped_rows", fake_write
    )
    endpoint = EndpointConfig(
        kind="database",
        format="elasticsearch",
        host="es.internal",
        port=9200,
        table="orders",
        extra={"primary_key": "order_id"},
    )
    rows, _ddl, summary = _write_destination_database(
        endpoint,
        [{"order_id": "o-1", "sku": "sku-9"}],
        ["order_id", "sku"],
        {"order_id": "TEXT", "sku": "TEXT"},
        MAPPINGS[:2],
        write_mode="insert",
        conflict_columns=["order_id"],
    )
    assert rows == 1
    assert captured["conflict_columns"] == ["order_id"]
    assert captured["write_mode"] == "insert"
    assert captured["destination_extra"]["primary_key"] == "order_id"
    assert summary["type"] == "elasticsearch"
