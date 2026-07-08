"""Extended semantic mapping accuracy tests."""

from services.semantic_mapper import map_columns


def test_amt_maps_to_payment_amount():
    mappings = map_columns(["AMT"], ["payment_amount", "customer_id"])
    assert mappings[0]["target"] == "payment_amount"
    assert mappings[0]["confidence"] >= 0.85


def test_cust_id_maps_to_customer_id():
    mappings = map_columns(["CUST_ID"], ["customer_id", "payment_amount"])
    assert mappings[0]["target"] == "customer_id"
    assert mappings[0]["confidence"] >= 0.9


def test_txn_dt_maps_to_transaction_date():
    mappings = map_columns(["TXN_DT"], ["transaction_date", "amount"])
    assert mappings[0]["target"] == "transaction_date"
    assert mappings[0]["confidence"] >= 0.85


def test_infers_target_when_no_targets():
    mappings = map_columns(["PAY_AMT"], [])
    assert mappings[0]["target"] == "payment_amount"
    assert mappings[0]["confidence"] >= 0.72


def test_hr_salary_amt():
    mappings = map_columns(
        ["salary_amt"],
        ["salary_amount", "employee_id"],
    )
    assert mappings[0]["target"] == "salary_amount"
    assert mappings[0]["confidence"] >= 0.85


def test_hr_emp_id():
    mappings = map_columns(
        ["emp_id"],
        ["employee_id", "full_name"],
    )
    assert mappings[0]["target"] == "employee_id"
    assert mappings[0]["confidence"] >= 0.9


def test_hire_dt():
    mappings = map_columns(
        ["hire_dt"],
        ["hire_date", "start_date"],
    )
    assert mappings[0]["target"] == "hire_date"
    assert mappings[0]["confidence"] >= 0.85


def test_dim_customer_id_schematic():
    mappings = map_columns(
        ["dim_customer_id"],
        ["customer_id", "order_id"],
    )
    assert mappings[0]["target"] == "customer_id"
    assert mappings[0]["confidence"] >= 0.9


def test_no_duplicate_targets():
    mappings = map_columns(
        ["AMT", "CUST_ID"],
        ["payment_amount", "customer_id"],
    )
    targets = [m["target"] for m in mappings]
    assert len(targets) == len(set(targets))


def test_exact_matches_win_over_schematic_synonyms():
    mappings = map_columns(
        ["amount", "payment_amount", "customer_id"],
        ["payment_amount", "customer_id", "amount"],
    )
    by_source = {m["source"]: m for m in mappings}
    assert by_source["amount"]["target"] == "amount"
    assert by_source["payment_amount"]["target"] == "payment_amount"
    assert by_source["customer_id"]["target"] == "customer_id"
    assert all(m["assignment_strategy"] == "optimal_bipartite_hungarian" for m in mappings)


def test_ambiguous_generic_id_requires_review():
    mappings = map_columns(["id", "customer_id"], ["customer_id", "order_id"])
    by_source = {m["source"]: m for m in mappings}
    assert by_source["customer_id"]["target"] == "customer_id"
    assert by_source["id"]["requires_review"] is True
    assert by_source["id"]["score_gap"] < 0.08
