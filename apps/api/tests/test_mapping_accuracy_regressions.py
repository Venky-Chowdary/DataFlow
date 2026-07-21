"""Regression tests for qualifier-aware mapping accuracy fixes."""

from __future__ import annotations

from services.schematic_index import schematic_match_boost
from services.semantic_mapper import _semantic_form, map_columns


def test_order_amt_does_not_steal_transaction_amount():
    out = map_columns(
        ["order_amt", "txn_amt", "AMT"],
        ["total_amount", "transaction_amount", "amount"],
    )
    by = {m["source"]: m["target"] for m in out}
    assert by["txn_amt"] == "transaction_amount"
    assert by["AMT"] == "amount"
    assert by["order_amt"] == "total_amount"


def test_ph_num_maps_to_phone_not_invented_column():
    out = map_columns(
        ["ph_num", "mobile_phone"],
        ["phone", "phone_number", "email"],
    )
    by = {m["source"]: m["target"] for m in out}
    assert by["ph_num"] == "phone"
    assert by["mobile_phone"] == "phone_number"
    assert all(not m.get("create_new") for m in out)


def test_schematic_rejects_conflicting_amount_qualifiers():
    assert schematic_match_boost("order_amt", "transaction_amount") is None
    assert schematic_match_boost("created_at", "updated_at") is None
    assert schematic_match_boost("AMT", "amount") == 0.99


def test_email_addr_does_not_double_expand():
    assert _semantic_form("email_addr") == "email_address"
    assert _semantic_form("usr_email") == "user_email"
    assert "address_address" not in _semantic_form("email_addr")


def test_objectid_still_avoids_decimal_id():
    samples = [
        "693486a0f0d881be6f0c470e",
        "69349183a44dd21d08a19c2c",
        "6934a44da44dd21d08a1ac18",
        "6934b905a44dd21d08a1caca",
    ]
    out = map_columns(
        ["_id"],
        ["id", "column_2", "column_5"],
        source_schemas=[{"name": "_id", "inferred_type": "VARCHAR", "samples": samples}],
        target_schemas=[
            {"name": "id", "inferred_type": "DECIMAL"},
            {"name": "column_2", "inferred_type": "VARCHAR"},
            {"name": "column_5", "inferred_type": "VARCHAR"},
        ],
        threshold=0.75,
        destination_db_type="snowflake",
    )
    assert len(out) == 1
    assert out[0]["target"].lower() != "id"
    assert out[0]["target"] == "_id" or out[0].get("create_new") is True
    assert out[0].get("create_new") is True or out[0]["target"] in {"column_2", "column_5", "_id"}
