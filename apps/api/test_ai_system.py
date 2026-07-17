"""
DataTransfer.space — AI System Integration Tests

Tests RAG pipeline, enhanced mapping, synonym intelligence, and evaluation.
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))


def test_knowledge_base():
    from ai.knowledge.semantic_patterns import SEMANTIC_PATTERNS, get_pattern_count
    from ai.knowledge.synonyms import are_synonyms, get_synonym_count, resolve_canonical

    assert get_pattern_count() >= 200, f"Expected 200+ patterns, got {get_pattern_count()}"
    assert get_synonym_count() >= 500, f"Expected 500+ synonyms, got {get_synonym_count()}"

    # Synonym intelligence
    assert are_synonyms("AMT", "amount"), "AMT should match amount"
    assert are_synonyms("cust", "customer"), "cust should match customer"
    assert are_synonyms("qty", "quantity"), "qty should match quantity"
    assert not are_synonyms("email", "phone"), "email should not match phone"
    assert resolve_canonical("cust_name") == "name" or "cust" in resolve_canonical("cust_name")

    print(f"  Patterns: {get_pattern_count()}")
    print(f"  Synonyms: {get_synonym_count()}")
    print("  Synonym tests: PASSED")


def test_rag_pipeline():
    from ai.rag.pipeline import get_rag_pipeline

    pipeline = get_rag_pipeline()
    init = pipeline.initialize()
    assert init.get("ingested", 0) > 0 or pipeline.vector_store.document_count > 0

    status = pipeline.get_status()
    assert status["embedding_backend"] in ("sentence_transformers", "tfidf_fallback")
    assert "semantic_search" in status["capabilities"]

    # Test column analysis via RAG
    result = pipeline.analyze_column("email_address", ["test@example.com"])
    assert result.confidence > 0.5
    assert "email" in result.answer.lower() or "Email" in result.answer

    # Test mapping
    mapping = pipeline.suggest_mapping("AMT", "amount")
    assert mapping.confidence > 0.8

    print(f"  Documents: {status['document_count']}")
    print(f"  Embedding: {status['embedding_backend']}")
    print(f"  Column analysis confidence: {result.confidence:.2f}")
    print(f"  AMT->amount confidence: {mapping.confidence:.2f}")
    print("  RAG pipeline: PASSED")


def test_enhanced_mapping():
    from ai import generate_mappings_enhanced

    source = ["customer_id", "cust_name", "email", "mobile_phone", "amt"]
    target = ["id", "full_name", "email_address", "phone_number", "amount"]

    mappings = generate_mappings_enhanced(source, target)
    assert len(mappings) == 5

    mapping_dict = {m.source_column: m for m in mappings}
    assert mapping_dict["amt"].target_column == "amount", f"AMT should map to amount, got {mapping_dict['amt'].target_column}"
    assert mapping_dict["amt"].confidence > 0.8, f"AMT mapping confidence too low: {mapping_dict['amt'].confidence}"
    assert mapping_dict["cust_name"].confidence > 0.5

    print("  Enhanced mapping results:")
    for m in mappings:
        print(f"    {m.source_column} -> {m.target_column} ({m.confidence:.0%}) [{m.reason}]")
    print("  Enhanced mapping: PASSED")


def test_chain_of_thought():
    from ai.llm.chain import DataTransferReasoningChain

    chain = DataTransferReasoningChain()
    result = chain.analyze_column("ssn", ["123-45-6789"])
    assert result.confidence > 0.7
    assert result.answer["is_pii"] is True
    assert len(result.reasoning) >= 4

    print(f"  SSN analysis: {result.answer['semantic_type']} (confidence: {result.confidence:.0%})")
    print(f"  Reasoning steps: {len(result.reasoning)}")
    print("  Chain-of-thought: PASSED")


def test_evaluation():
    from ai.training.evaluation import DataTransferEvaluator

    evaluator = DataTransferEvaluator()

    mapping_result = evaluator.evaluate_column_mapping()
    assert mapping_result.score > 0.5, f"Mapping accuracy too low: {mapping_result.score}"

    synonym_result = evaluator.evaluate_synonym_recognition()
    assert synonym_result.score > 0.7, f"Synonym accuracy too low: {synonym_result.score}"

    print(f"  Mapping accuracy: {mapping_result.score:.0%} ({mapping_result.correct}/{mapping_result.total})")
    print(f"  Synonym accuracy: {synonym_result.score:.0%} ({synonym_result.correct}/{synonym_result.total})")
    print("  Evaluation: PASSED")


def test_sample_generator():
    from ai.training.sample_generator import DataTransferSampleGenerator

    gen = DataTransferSampleGenerator()
    datasets = gen.generate_all(rows=10)

    assert "logistics" in datasets
    assert "finance" in datasets
    assert "healthcare" in datasets
    assert "retail" in datasets

    logistics = datasets["logistics"]
    schema = gen.to_schema_dict(logistics)
    assert len(schema) == len(logistics.columns)

    print(f"  Generated {len(datasets)} industry datasets")
    for name, ds in datasets.items():
        print(f"    {name}: {ds.row_count} rows, {len(ds.columns)} columns")
    print("  Sample generator: PASSED")


def test_data_synthesis():
    from ai.training.data_synthesis import DataTransferDataSynthesizer

    synth = DataTransferDataSynthesizer()
    datasets = synth.synthesize_full_dataset()

    total = sum(len(d.examples) for d in datasets.values())
    assert total > 2000, f"Expected 2000+ training examples, got {total}"

    print(f"  Training datasets: {len(datasets)}")
    for name, ds in datasets.items():
        print(f"    {name}: {len(ds.examples)} examples")
    print("  Data synthesis: PASSED")


def test_natural_language():
    from ai import query_natural_language

    result = query_natural_language("What columns contain customer PII?")
    assert result["answer"]
    assert result["confidence"] > 0

    print(f"  NL query answer: {result['answer'][:100]}...")
    print(f"  Method: {result['method']}, Confidence: {result['confidence']:.0%}")
    print("  Natural language: PASSED")


if __name__ == "__main__":
    print("=" * 60)
    print("DataTransfer.space — AI System Integration Tests")
    print("=" * 60)

    tests = [
        ("Knowledge Base", test_knowledge_base),
        ("RAG Pipeline", test_rag_pipeline),
        ("Enhanced Mapping", test_enhanced_mapping),
        ("Chain-of-Thought", test_chain_of_thought),
        ("Evaluation Metrics", test_evaluation),
        ("Sample Generator", test_sample_generator),
        ("Data Synthesis", test_data_synthesis),
        ("Natural Language", test_natural_language),
    ]

    passed = 0
    failed = 0
    for name, test_fn in tests:
        print(f"\n[{name}]")
        try:
            test_fn()
            passed += 1
        except Exception as e:
            print(f"  FAILED: {e}")
            failed += 1

    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed out of {len(tests)}")
    print("=" * 60)
    sys.exit(1 if failed > 0 else 0)
