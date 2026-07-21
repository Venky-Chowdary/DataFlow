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


def test_pipeline_entailment_keeps_create_new():
    from services.mapping_pipeline import entailment_prune

    mappings = [
        {
            "source": "_id",
            "target": "_id_text",
            "confidence": 0.92,
            "create_new": True,
            "assignment_strategy": "create_compatible_new",
        }
    ]
    kept, pruned = entailment_prune(mappings, ["id", "name"])
    assert pruned == []
    assert len(kept) == 1
    assert kept[0]["target"] == "_id_text"


def test_pipeline_objectid_create_new_survives_existing_dest():
    """Mongo ObjectId must not vanish from Map when dest has DECIMAL id."""
    samples = [
        "693486a0f0d881be6f0c470e",
        "69349183a44dd21d08a19c2c",
        "6934a44da44dd21d08a1ac18",
        "6934b905a44dd21d08a1caca",
    ]
    r = run_mapping_pipeline(
        ["_id", "name"],
        ["id", "name"],
        source_schemas=[
            {"name": "_id", "inferred_type": "VARCHAR", "samples": samples},
            {"name": "name", "inferred_type": "VARCHAR", "samples": ["Ada"]},
        ],
        target_schemas=[
            {"name": "id", "inferred_type": "DECIMAL", "samples": []},
            {"name": "name", "inferred_type": "VARCHAR", "samples": []},
        ],
        destination_db_type="snowflake",
        use_llm=False,
        confidence_threshold=0.75,
    )
    assert len(r["mappings"]) == 2
    by_src = {m["source"]: m for m in r["mappings"]}
    assert "_id" in by_src
    assert by_src["_id"]["target"].lower() != "id"
    assert by_src["_id"].get("create_new") is True or by_src["_id"]["target"] not in {"id", "ID"}
    assert by_src["name"]["target"].lower() == "name"


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
    # Weak lexical match may still land on payment_amount; otherwise type-safe
    # create-new must survive (never silently empty the Map step).
    assert len(r["mappings"]) >= 1
    assert all(
        m["target"] == "payment_amount" or m.get("create_new")
        for m in r["mappings"]
    )
    assert "plan_summary" in r


def test_pipeline_plan_summary():
    r = run_mapping_pipeline(["AMT"], ["payment_amount"], confidence_threshold=0.5, use_llm=False)
    assert r["plan_summary"]["mapped_count"] >= 1
    assert "coverage_pct" in r["plan_summary"]


def test_pipeline_type_locked_blocks_type_change():
    r = run_mapping_pipeline(
        ["id"],
        ["id"],
        source_schemas=[{"name": "id", "inferred_type": "INTEGER", "samples": ["1"]}],
        target_schemas=[{"name": "id", "inferred_type": "VARCHAR", "samples": []}],
        schema_policy="type_locked",
        use_llm=False,
    )
    assert any("type_locked" in issue or "INTEGER" in issue for issue in r["quality_issues"])

