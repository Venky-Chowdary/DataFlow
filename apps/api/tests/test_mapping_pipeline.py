from services.mapping_pipeline import classify_format, run_mapping_pipeline


def test_classify_payment_feed():
    r = classify_format(["CUST_ID", "AMT", "TXN_DT"])
    assert r["format"] == "payment_feed"


def test_pipeline_returns_agents():
    r = run_mapping_pipeline(["AMT", "CUST_ID"], ["payment_amount", "customer_id"])
    assert "mappings" in r
    assert len(r["agents_used"]) >= 5
    assert r["mappings"][0]["target"] == "payment_amount"
    assert "transforms" in r
    assert r["validation"]["agent"] == "ValidationCriticAgent"


def test_pipeline_flags_ambiguous_mapping_for_review():
    r = run_mapping_pipeline(["id", "customer_id"], ["customer_id", "order_id"])
    assert not r["validation"]["passed"]
    assert any("Ambiguous mapping" in issue for issue in r["validation"]["issues"])
