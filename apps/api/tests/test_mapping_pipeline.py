from services.mapping_pipeline import classify_format, run_mapping_pipeline


def test_classify_payment_feed():
    r = classify_format(["CUST_ID", "AMT", "TXN_DT"])
    assert r["format"] == "payment_feed"


def test_pipeline_returns_agents():
    r = run_mapping_pipeline(["AMT", "CUST_ID"], ["payment_amount", "customer_id"])
    assert "mappings" in r
    assert len(r["agents_used"]) >= 5
    assert "SampleValidatorAgent" in r["agents_used"]
    assert r["mappings"][0]["target"] == "payment_amount"
    assert "transforms" in r
    assert r["validation"]["agent"] == "ValidationCriticAgent"


def test_pipeline_flags_ambiguous_mapping_for_review():
    r = run_mapping_pipeline(["id", "customer_id"], ["customer_id", "order_id"])
    assert not r["validation"]["passed"]
    assert any("Ambiguous mapping" in issue for issue in r["validation"]["issues"])


def test_pipeline_uses_source_inferred_types_for_transforms():
    r = run_mapping_pipeline(
        ["amount"],
        ["amount"],
        source_schemas=[{"name": "amount", "inferred_type": "DECIMAL", "samples": ["1.00"]}],
        target_schemas=[{"name": "amount", "inferred_type": "NUMERIC", "samples": []}],
        confidence_threshold=0.5,
    )
    assert r["mappings"][0]["transform"] == "decimal"
    assert r["mappings"][0]["source_type"] == "DECIMAL"


def test_pipeline_entailment_prune_drops_phantom_targets():
    from services.mapping_pipeline import entailment_prune

    mappings = [{"source": "foo", "target": "phantom_col", "confidence": 0.9}]
    kept, pruned = entailment_prune(mappings, ["customer_id", "payment_amount"])
    assert pruned == ["foo"]
    assert kept == []


def test_pipeline_llm_disabled_skips_agent():
    r = run_mapping_pipeline(["AMT"], ["payment_amount"], use_llm=False)
    assert "LLMMappingAgent" not in r["agents_used"]
    assert r["llm"]["llm_used"] is False


def test_pipeline_does_not_restore_entailed_prunes():
    r = run_mapping_pipeline(
        ["PHANTOM_SRC"],
        ["payment_amount"],
        confidence_threshold=0.5,
        use_llm=False,
    )
    assert r["mappings"] == [] or all(m["target"] == "payment_amount" for m in r["mappings"])
    assert "plan_summary" in r


def test_pipeline_plan_summary():
    r = run_mapping_pipeline(["AMT"], ["payment_amount"], confidence_threshold=0.5, use_llm=False)
    assert r["plan_summary"]["mapped_count"] >= 1
    assert "coverage_pct" in r["plan_summary"]

