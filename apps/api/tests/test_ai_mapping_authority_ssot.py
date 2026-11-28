"""AI / RAG / Pilot mapping must use map_columns — no weaker parallel scorer.

Informatica CLAIRE and Fivetran AI agents still invent mappings from synonyms
and last-token overlap. DataFlow RAG used to return 0.92 for user_id→customer_id
because both end in ``id``. That is the hole this suite closes.
"""

from __future__ import annotations
from services.platform_config import data_dir

import json
from pathlib import Path


PROOF_DIR = data_dir() / "proofs"


def test_rag_suggest_mapping_uses_map_columns_ssot() -> None:
    from src.ai.rag.pipeline import get_rag_pipeline

    rag = get_rag_pipeline()
    pin = rag.suggest_mapping("AMT", "amount")
    # The confidence Map's own pipeline gives (names only, no destination
    # types: reviewed), never the bare scorer's 0.99 (accuracy review 2).
    from services.semantic_mapper import pair_mapping_authority

    auth = pair_mapping_authority("AMT", "amount")
    assert auth["proposed_target"] == "amount"
    assert abs(pin.confidence - auth["confidence"]) < 1e-9
    assert pin.method == "map_columns_ssot"

    false_friend = rag.suggest_mapping("user_id", "customer_id")
    assert false_friend.confidence < 0.85
    assert "review" in (false_friend.answer + false_friend.reasoning).lower()

    qty = rag.suggest_mapping("order_qty", "order_amt")
    assert qty.confidence < 0.85


def test_retriever_mapping_confidence_matches_mapper() -> None:
    from services.semantic_mapper import pair_mapping_authority
    from src.ai.rag.retriever import DataTransferRetriever

    retriever = DataTransferRetriever()
    info = retriever.retrieve_for_mapping("user_id", "customer_id")
    auth = pair_mapping_authority("user_id", "customer_id")
    assert info["authority"] == "mapping_pipeline.run_mapping_pipeline"
    assert abs(float(info["mapping_confidence"]) - auth["confidence"]) < 1e-9
    assert info["requires_review"] is True
    assert float(info["mapping_confidence"]) < 0.85


def test_smart_mapper_map_columns_is_ssot_not_pair_scorer() -> None:
    """Direct SmartMapper calls must hold the same false friends as Map SSOT."""
    from src.ai.semantic_engine import SmartMapper, SemanticAnalyzer

    mapper = SmartMapper(SemanticAnalyzer())
    rows = mapper.map_columns(["user_id", "AMT"], ["customer_id", "amount"])
    by = {m.source_column: m for m in rows}
    assert by["AMT"].target_column == "amount"
    # Names only, no destination types: Map's pipeline keeps it for review, so
    # the AI surface may not present the bare scorer's 0.99 (accuracy review 2).
    from services.semantic_mapper import authority_mappings

    pipe = {m["source"]: m for m in authority_mappings(["user_id", "AMT"], ["customer_id", "amount"])}
    assert abs(by["AMT"].confidence - float(pipe["AMT"]["confidence"])) < 1e-9
    assert by["user_id"].confidence < 0.85


def test_reasoning_chain_and_enhanced_mapper_hold_false_friends() -> None:
    from src.ai.enhanced_engine import generate_mappings_enhanced
    from src.ai.llm.chain import DataTransferReasoningChain

    chain = DataTransferReasoningChain()
    result = chain.map_columns(["user_id", "order_qty"], ["customer_id", "order_amt"])
    by = {m["source_column"]: m for m in result.answer["mappings"]}
    assert float(by["user_id"]["confidence"]) < 0.85
    assert by["user_id"].get("requires_review") is True
    assert float(by["order_qty"]["confidence"]) < 0.85

    enhanced = generate_mappings_enhanced(
        ["AMT", "user_id"],
        ["amount", "customer_id"],
    )
    by_e = {m.source_column: m for m in enhanced}
    assert by_e["AMT"].target_column == "amount"
    assert by_e["AMT"].method == "map_columns_ssot"
    assert by_e["user_id"].confidence < 0.85


def test_ai_mapping_authority_proof_artifact(tmp_path: Path) -> None:
    from services.semantic_mapper import pair_mapping_authority
    from src.ai.rag.pipeline import get_rag_pipeline

    rag = get_rag_pipeline()
    cases = [
        ("AMT", "amount", True),
        ("user_id", "customer_id", False),
        ("order_qty", "order_amt", False),
        ("sku", "product_id", False),
        ("cust_id", "customer_id", True),
    ]
    rows = []
    correct = 0
    for src, tgt, auto_ok in cases:
        suggestion = rag.suggest_mapping(src, tgt)
        auth = pair_mapping_authority(src, tgt)
        aligned = abs(suggestion.confidence - auth["confidence"]) < 1e-9
        if auto_ok:
            # The right pair: the surface shows exactly Map's confidence and target.
            ok = aligned and auth["proposed_target"] == tgt
        else:
            ok = aligned and suggestion.confidence < 0.85
        correct += int(ok)
        rows.append({
            "source": src,
            "target": tgt,
            "auto_approve_allowed": auto_ok,
            "rag_confidence": suggestion.confidence,
            "mapper_confidence": auth["confidence"],
            "aligned": aligned,
            "correct": ok,
        })
    proof = {
        "metric": "ai_rag_mapping_authority_ssot",
        "score": round(correct / len(cases), 4),
        "correct": correct,
        "total": len(cases),
        "floor": 1.0,
        "passed": correct == len(cases),
        "honesty": (
            "RAG confidence must equal Map's pipeline (run_mapping_pipeline, no LLM). "
            "False-friends stay below G4. "
            "Not a claim that Pilot LLM is better than Informatica CLAIRE on breadth."
        ),
        "cases": rows,
    }
    PROOF_DIR.mkdir(parents=True, exist_ok=True)
    artifact = PROOF_DIR / "ai_rag_mapping_authority.json"
    artifact.write_text(json.dumps(proof, indent=2), encoding="utf-8")
    (tmp_path / "ai_rag_mapping_authority.json").write_text(
        json.dumps(proof, indent=2), encoding="utf-8"
    )
    assert proof["passed"], proof
