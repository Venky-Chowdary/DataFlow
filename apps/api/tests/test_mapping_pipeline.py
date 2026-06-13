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
