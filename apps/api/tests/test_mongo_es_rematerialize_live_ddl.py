"""Mongo/ES rematerialize when live physical carriers differ from Map stamps."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock


def test_mongo_rematerialize_when_physical_decimal_vs_map_varchar():
    from connectors.mongodb_writer import _mongo_rematerialize_if_physical_differs

    batch = _mongo_rematerialize_if_physical_differs(
        physical={"amount": "DECIMAL", "AMOUNT": "DECIMAL"},
        dest_types={"amount": "VARCHAR"},
        target_cols=["amount"],
        headers=["amount"],
        data_rows=[["12.50"], ["not-a-number"]],
        mappings=[{"source": "amount", "target": "amount", "target_type": "VARCHAR"}],
        column_types={"amount": "VARCHAR"},
        logical_types=["VARCHAR"],
        policy="quarantine",
    )
    assert batch is not None
    mapped_rows, _errs, rejected, live = batch
    assert "DECIMAL" in str(live.get("amount") or "").upper()
    assert len(mapped_rows) + len(rejected) >= 1
    assert any((d.get("column") or "").lower() == "amount" for d in rejected) or len(
        mapped_rows
    ) == 1


def test_mongo_rematerialize_refuses_map_varchar_gap_fill():
    from connectors.mongodb_writer import _mongo_rematerialize_if_physical_differs

    batch = _mongo_rematerialize_if_physical_differs(
        physical={"amount": "DECIMAL"},
        dest_types={"amount": "VARCHAR", "note": "VARCHAR"},
        target_cols=["amount", "note"],
        headers=["amount", "note"],
        data_rows=[["12.50", "x"]],
        mappings=[
            {"source": "amount", "target": "amount", "target_type": "VARCHAR"},
            {"source": "note", "target": "note", "target_type": "VARCHAR"},
        ],
        column_types={"amount": "VARCHAR", "note": "VARCHAR"},
        logical_types=["VARCHAR", "VARCHAR"],
        policy="quarantine",
    )
    assert batch is None


def test_mongo_no_rematerialize_when_carriers_match():
    from connectors.mongodb_writer import _mongo_rematerialize_if_physical_differs

    assert (
        _mongo_rematerialize_if_physical_differs(
            physical={"amount": "DECIMAL"},
            dest_types={"amount": "DECIMAL"},
            target_cols=["amount"],
            headers=["amount"],
            data_rows=[["1"]],
            mappings=[{"source": "amount", "target": "amount", "target_type": "DECIMAL"}],
            column_types={"amount": "DECIMAL"},
            logical_types=["DECIMAL"],
            policy="quarantine",
        )
        is None
    )


def test_mongo_force_remap_when_carriers_match_partial_studio():
    from connectors.mongodb_writer import _mongo_rematerialize_if_physical_differs

    batch = _mongo_rematerialize_if_physical_differs(
        physical={"amount": "DECIMAL"},
        dest_types={"amount": "DECIMAL"},
        target_cols=["amount"],
        headers=["amount"],
        data_rows=[["12.50"]],
        mappings=[{"source": "amount", "target": "amount", "target_type": "VARCHAR"}],
        column_types={"amount": "VARCHAR"},
        logical_types=["VARCHAR"],
        policy="quarantine",
        force_remap=True,
    )
    assert batch is not None
    _rows, _errs, _rej, live = batch
    assert "DECIMAL" in str(live.get("amount") or "").upper()


def test_fetch_mongo_physical_types_majority_vote():
    from connectors.mongodb_writer import _fetch_mongo_physical_types

    class Cursor:
        def __init__(self, docs):
            self._docs = docs

        def limit(self, n):
            return self._docs[:n]

    class Coll:
        def find(self):
            return Cursor(
                [
                    {"amount": 1, "flag": True},
                    {"amount": 2, "flag": False},
                    {"amount": 3, "flag": True},
                ]
            )

    physical, exc = _fetch_mongo_physical_types(Coll(), ["amount", "flag"])
    assert exc is None
    assert physical.get("amount") == "INTEGER"
    assert "BOOL" in str(physical.get("flag") or "").upper()


def test_es_rematerialize_when_physical_long_vs_map_varchar():
    from connectors.elasticsearch_writer import _es_rematerialize_if_physical_differs

    batch = _es_rematerialize_if_physical_differs(
        physical={"qty": "INTEGER", "QTY": "INTEGER"},
        dest_types={"qty": "VARCHAR"},
        target_cols=["qty"],
        headers=["qty"],
        data_rows=[["12"], [""]],
        mappings=[{"source": "qty", "target": "qty", "target_type": "VARCHAR"}],
        column_types={"qty": "VARCHAR"},
        logical_types=["VARCHAR"],
        policy="quarantine",
    )
    assert batch is not None
    mapped_rows, _errs, rejected, live = batch
    assert "INT" in str(live.get("qty") or "").upper()
    assert any((d.get("column") or "").lower() == "qty" for d in rejected) or len(
        mapped_rows
    ) == 1


def test_es_rematerialize_refuses_map_varchar_gap_fill():
    from connectors.elasticsearch_writer import _es_rematerialize_if_physical_differs

    batch = _es_rematerialize_if_physical_differs(
        physical={"qty": "INTEGER"},
        dest_types={"qty": "VARCHAR", "note": "VARCHAR"},
        target_cols=["qty", "note"],
        headers=["qty", "note"],
        data_rows=[["12", "x"]],
        mappings=[
            {"source": "qty", "target": "qty", "target_type": "VARCHAR"},
            {"source": "note", "target": "note", "target_type": "VARCHAR"},
        ],
        column_types={"qty": "VARCHAR", "note": "VARCHAR"},
        logical_types=["VARCHAR", "VARCHAR"],
        policy="quarantine",
    )
    assert batch is None


def test_es_force_remap_when_carriers_match_partial_studio():
    from connectors.elasticsearch_writer import _es_rematerialize_if_physical_differs

    batch = _es_rematerialize_if_physical_differs(
        physical={"qty": "INTEGER"},
        dest_types={"qty": "INTEGER"},
        target_cols=["qty"],
        headers=["qty"],
        data_rows=[["12"]],
        mappings=[{"source": "qty", "target": "qty", "target_type": "VARCHAR"}],
        column_types={"qty": "VARCHAR"},
        logical_types=["VARCHAR"],
        policy="quarantine",
        force_remap=True,
    )
    assert batch is not None
    _rows, _errs, _rej, live = batch
    assert "INT" in str(live.get("qty") or "").upper()


def test_es_writer_refuses_logical_quarantine_invent_under_partial_studio():
    """After force_remap failure path: studio_err + incomplete dest must not Map-fill."""
    from unittest.mock import MagicMock, patch

    from connectors.elasticsearch_writer import write_mapped_rows

    client = MagicMock()
    client.indices.exists.return_value = True
    client.indices.get_mapping.return_value = {
        "orders": {"mappings": {"properties": {"id": {"type": "long"}}}}
    }
    # Partial Studio: id only — qty missing from Studio and mapping props.
    with patch("connectors.elasticsearch_writer._client", return_value=client):
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
            headers=["id", "qty"],
            data_rows=[["1", "7"]],
            mappings=[
                {"source": "id", "target": "id", "target_type": "VARCHAR"},
                {"source": "qty", "target": "qty", "target_type": "VARCHAR"},
            ],
            column_types={"id": "VARCHAR", "qty": "VARCHAR"},
            create_table=False,
            destination_column_types={"id": "INTEGER"},
            error_policy="quarantine",
            write_mode="insert",
            conflict_columns=["id"],
            api_key="",
        )
    assert result.ok is False
    assert "qty" in (result.error or "").lower() or "invent" in (result.error or "").lower()


def test_fetch_es_physical_types_from_mapping():
    from connectors.elasticsearch_writer import _fetch_es_physical_types

    client = MagicMock()
    client.indices.get_mapping.return_value = {
        "orders": {
            "mappings": {
                "properties": {
                    "qty": {"type": "long"},
                    "flag": {"type": "boolean"},
                    "note": {"type": "text"},
                }
            }
        }
    }
    physical, exc = _fetch_es_physical_types(client, "orders", ["qty", "flag", "note"])
    assert exc is None
    # ES long → width-preserving BIGINT (canonical invent never narrower).
    assert physical.get("qty") in {"INTEGER", "BIGINT"}
    assert "INT" in str(physical.get("qty") or "").upper()
    assert physical.get("flag") == "BOOLEAN"
    assert physical.get("note") == "TEXT"


def test_fetch_es_physical_types_resolves_alias_response_keys():
    """get_mapping is keyed by concrete index — alias name must still resolve."""
    from connectors.elasticsearch_writer import _fetch_es_physical_types

    client = MagicMock()
    client.indices.get_mapping.return_value = {
        "orders-v2": {
            "mappings": {
                "properties": {
                    "qty": {"type": "long"},
                }
            }
        }
    }
    physical, exc = _fetch_es_physical_types(client, "orders-alias", ["qty"])
    assert exc is None
    assert physical.get("qty") in {"INTEGER", "BIGINT"}
    assert "INT" in str(physical.get("qty") or "").upper()


def test_fetch_es_physical_types_surfaces_auth_exc():
    from connectors.elasticsearch_writer import _fetch_es_physical_types

    client = MagicMock()
    client.indices.get_mapping.side_effect = Exception("401 Unauthorized")
    physical, exc = _fetch_es_physical_types(client, "orders", ["qty"])
    assert physical == {}
    assert exc is not None
    assert "401" in str(exc)


def test_fetch_mongo_physical_types_surfaces_auth_exc():
    from connectors.mongodb_writer import _fetch_mongo_physical_types

    class Coll:
        def find(self):
            raise Exception("403 Forbidden")

    physical, exc = _fetch_mongo_physical_types(Coll(), ["amount"])
    assert physical == {}
    assert exc is not None


def test_mongo_create_new_refuses_partial_studio():
    from unittest.mock import MagicMock, patch

    from connectors.mongodb_writer import write_mapped_rows

    client = MagicMock()
    client.__getitem__.return_value.list_collection_names.return_value = []
    with patch("connectors.mongodb_common._mongo_client", return_value=client):
        result = write_mapped_rows(
            host="localhost",
            port=27017,
            database="testdb",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="orders",
            headers=["id", "qty"],
            data_rows=[["1", "7"]],
            mappings=[
                {"source": "id", "target": "id", "target_type": "VARCHAR"},
                {"source": "qty", "target": "qty", "target_type": "VARCHAR"},
            ],
            column_types={"id": "VARCHAR", "qty": "VARCHAR"},
            create_table=True,
            destination_column_types={"id": "INTEGER"},
        )
    assert result.ok is False
    assert "qty" in (result.error or "").lower()


def test_es_create_new_refuses_partial_studio():
    from unittest.mock import MagicMock, patch

    from connectors.elasticsearch_writer import write_mapped_rows

    client = MagicMock()
    client.indices.exists.return_value = False
    with patch("connectors.elasticsearch_writer._client", return_value=client):
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
            headers=["id", "qty"],
            data_rows=[["1", "7"]],
            mappings=[
                {"source": "id", "target": "id", "target_type": "VARCHAR"},
                {"source": "qty", "target": "qty", "target_type": "VARCHAR"},
            ],
            column_types={"id": "VARCHAR", "qty": "VARCHAR"},
            create_table=True,
            destination_column_types={"id": "INTEGER"},
            conflict_columns=["id"],
        )
    assert result.ok is False
    assert "qty" in (result.error or "").lower()
