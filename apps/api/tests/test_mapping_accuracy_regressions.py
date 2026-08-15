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


def test_landing_amount_family_is_not_payment_identity():
    """Hero must not auto-pin order_amt → payment_amount at 96%.

    Same NUMERIC amount role is not the same business column. Qualifiers
    pick total_amount / payment_amount / tax_amount. tax_amt pins to
    tax_amount once the total_amount false-friend is demoted below G4.
    """
    out = map_columns(
        ["order_amt", "pay_amt", "tax_amt"],
        ["payment_amount", "tax_amount", "total_amount"],
    )
    by = {m["source"]: m for m in out}
    assert by["order_amt"]["target"] == "total_amount"
    assert by["pay_amt"]["target"] == "payment_amount"
    assert by["tax_amt"]["target"] == "tax_amount"
    assert by["order_amt"]["target"] != "payment_amount"
    assert float(by["order_amt"]["confidence"]) >= 0.85
    assert float(by["pay_amt"]["confidence"]) >= 0.85
    assert float(by["tax_amt"]["confidence"]) >= 0.85
    assert by["tax_amt"].get("requires_review") is not True


def test_cust_id_does_not_auto_pin_warehouse_customer_key():
    """CRM cust_id is not a warehouse surrogate customer_key.

    Lexical overlap was 0.97 with requires_review False — that would skip
    G4 strict (~0.85) and destroy trust. Propose the dest column, hold Map.
    """
    only_key = map_columns(["cust_id"], ["customer_key"])
    assert len(only_key) == 1
    row = only_key[0]
    assert row["target"] == "customer_key"
    assert row.get("create_new") is not True
    assert row.get("requires_review") is True
    assert float(row["confidence"]) < 0.85

    both = map_columns(["cust_id"], ["customer_id", "customer_key"])
    by = {m["source"]: m for m in both}
    assert by["cust_id"]["target"] == "customer_id"
    assert float(by["cust_id"]["confidence"]) >= 0.85
    assert by["cust_id"].get("requires_review") is not True


def test_order_amt_only_payment_amount_proposes_with_review():
    """Conflicting amount qualifiers must not silent-create a sibling column.

    When payment_amount is the only dest money column, propose it below G4
    so the operator confirms — never auto-pin, never hide the candidate.
    """
    out = map_columns(["order_amt"], ["payment_amount"])
    assert len(out) == 1
    row = out[0]
    assert row["target"] == "payment_amount"
    assert row.get("create_new") is not True
    assert row.get("requires_review") is True
    assert float(row["confidence"]) < 0.85


def test_tax_amt_does_not_auto_pin_generic_total_amount():
    """tax is a typed measure; total_amount is a generic bucket, not identity."""
    out = map_columns(["tax_amt"], ["total_amount"])
    assert len(out) == 1
    row = out[0]
    assert row["target"] == "total_amount"
    assert row.get("create_new") is not True
    assert row.get("requires_review") is True
    assert float(row["confidence"]) < 0.85


def test_identity_and_timestamp_polarity_stay_fail_closed():
    """Customer vs vendor ids, created vs updated — do not lift into a confident pin."""
    vendor = map_columns(["customer_id"], ["vendor_id"])
    assert vendor[0].get("requires_review") is True
    assert float(vendor[0]["confidence"]) < 0.85
    if not vendor[0].get("create_new"):
        # If the engine surfaces vendor_id at all, it cannot look auto-approved.
        assert vendor[0]["target"] == "vendor_id"

    updated = map_columns(["created_at"], ["updated_at"])
    assert updated[0].get("requires_review") is True
    assert float(updated[0]["confidence"]) < 0.85
    if not updated[0].get("create_new"):
        assert updated[0]["target"] == "updated_at"


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
    assert schematic_match_boost("customer_id", "customer_key") is None
    assert schematic_match_boost("cust_id", "customer_key") is None


def test_email_addr_does_not_double_expand():
    assert _semantic_form("email_addr") == "email_address"
    assert _semantic_form("usr_email") == "user_email"
    assert "address_address" not in _semantic_form("email_addr")


def test_order_qty_does_not_auto_pin_order_amt():
    """Quantity is not money — the shared ``order`` qualifier must not skip G4."""
    out = map_columns(["order_qty"], ["order_amt"])
    assert len(out) == 1
    row = out[0]
    assert row["target"] == "order_amt"
    assert row.get("create_new") is not True
    assert row.get("requires_review") is True
    assert float(row["confidence"]) < 0.85


def test_user_id_does_not_auto_pin_customer_id():
    """CRM user_id is not customer_id. Synonym collapse must not skip G4."""
    out = map_columns(["user_id"], ["customer_id"])
    assert len(out) == 1
    row = out[0]
    assert row.get("create_new") is not True
    assert row.get("requires_review") is True
    assert float(row["confidence"]) < 0.85


def test_dest_userid_collision_requires_review():
    """UserID vs userid is a dest identifier collision — never auto-approve."""
    out = map_columns(["user_id"], ["UserID", "userid"])
    assert len(out) == 1
    row = out[0]
    assert row["target"] in {"UserID", "userid"}
    assert row.get("create_new") is not True
    assert row.get("requires_review") is True
    assert float(row["confidence"]) < 0.85


def test_sku_does_not_auto_pin_product_id_when_sku_exists():
    out = map_columns(["sku"], ["product_sku", "product_id"])
    by = {m["source"]: m for m in out}
    assert by["sku"]["target"] == "product_sku"
    assert by["sku"].get("requires_review") is not True


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
