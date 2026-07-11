"""Tests for LLM-assisted mapping layer (deterministic fallback paths)."""

from unittest.mock import MagicMock, patch

from services.llm_mapping import (
    _extract_json,
    _normalize_llm_mapping,
    llm_provider_available,
    refine_mappings_with_llm,
)


def test_extract_json_plain():
    parsed = _extract_json('{"mappings": []}')
    assert parsed == {"mappings": []}


def test_extract_json_codeblock():
    text = '```json\n{"mappings": [{"source": "a", "target": "b"}]}\n```'
    parsed = _extract_json(text)
    assert parsed["mappings"][0]["source"] == "a"


def test_normalize_llm_mapping_resolves_case():
    item = {"source": "AMT", "target": "PAYMENT_AMOUNT", "confidence": 0.9}
    norm = _normalize_llm_mapping(item, ["payment_amount"], ["AMT"])
    assert norm is not None
    assert norm["target"] == "payment_amount"
    assert norm["source"] == "AMT"


def test_normalize_llm_mapping_rejects_unknown_target():
    item = {"source": "AMT", "target": "phantom", "confidence": 0.9}
    assert _normalize_llm_mapping(item, ["payment_amount"], ["AMT"]) is None


def test_refine_disabled_returns_baseline():
    baseline = [{"source": "id", "target": "id", "confidence": 0.95}]
    merged, meta = refine_mappings_with_llm(
        baseline, ["id"], ["id"], enabled=False,
    )
    assert merged == baseline
    assert meta["llm_used"] is False


@patch("services.llm_mapping.llm_provider_available", return_value=False)
def test_refine_no_provider_keeps_baseline(_mock_avail):
    baseline = [{"source": "AMT", "target": "amount", "confidence": 0.8}]
    merged, meta = refine_mappings_with_llm(
        baseline, ["AMT"], ["amount"], enabled=True,
    )
    assert merged == baseline
    assert meta["llm_error"] == "no_cloud_or_local_llm"


@patch("services.llm_mapping.llm_provider_available", return_value=True)
def test_refine_with_mock_llm(_mock_avail):
    baseline = [{"source": "AMT", "target": "amount", "confidence": 0.7}]
    mock_response = MagicMock()
    mock_response.success = True
    mock_response.content = '{"mappings": [{"source": "AMT", "target": "payment_amount", "confidence": 0.92, "reason": "synonym"}]}'
    mock_response.provider = "mock"

    mock_chain = MagicMock()
    mock_chain.generate.return_value = mock_response

    with patch("src.ai.llm.fallback.DataTransferFallbackChain", return_value=mock_chain):
        merged, meta = refine_mappings_with_llm(
            baseline,
            ["AMT"],
            ["payment_amount", "amount"],
            enabled=True,
        )

    assert meta["llm_used"] is True
    assert meta["strategy"] == "hybrid_llm_bm25"
    amt = next(m for m in merged if m["source"] == "AMT")
    assert amt["target"] == "payment_amount"


def test_llm_provider_available_does_not_crash():
    # Smoke test — returns bool without raising
    assert isinstance(llm_provider_available(), bool)
