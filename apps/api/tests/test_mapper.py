"""Tests for semantic mapper."""

from services.semantic_mapper import map_columns


def test_amt_maps_to_payment_amount():
    mappings = map_columns(["AMT"], ["payment_amount", "customer_id"])
    assert mappings[0]["source"] == "AMT"
    assert mappings[0]["target"] == "payment_amount"
    assert mappings[0]["confidence"] >= 0.85


def test_cust_id_maps_to_customer_id():
    mappings = map_columns(["CUST_ID"], ["customer_id", "payment_amount"])
    assert mappings[0]["target"] == "customer_id"
    assert mappings[0]["confidence"] >= 0.9


def test_txn_dt_maps_to_transaction_date():
    mappings = map_columns(["TXN_DT"], ["transaction_date", "amount"])
    assert mappings[0]["target"] == "transaction_date"


def test_infers_target_when_no_targets():
    mappings = map_columns(["PAY_AMT"], [])
    assert mappings[0]["target"] == "payment_amount"
