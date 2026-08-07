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
    assert physical.get("qty") == "INTEGER"
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
    assert physical.get("qty") == "INTEGER"


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
    assert "403" in str(exc)
