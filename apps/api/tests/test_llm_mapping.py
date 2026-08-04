"""Tests for LLM-assisted mapping layer (deterministic fallback paths)."""

from unittest.mock import MagicMock, patch

from services.llm_mapping import (
    _build_prompt,
    _extract_json,
    _normalize_llm_mapping,
    _sanitize_samples,
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


def test_sanitize_samples_masks_pii():
    samples = {
        "email": ["alice@example.com", "bob@example.com"],
        "ssn": ["123-45-6789"],
        "phone": ["555-123-4567"],
        "amount": ["$1,000.00"],
        "url": ["https://example.com/private"],
    }
    sanitized = _sanitize_samples(samples)
    assert sanitized["email"] == ["<redacted>", "<redacted>"]
    assert sanitized["ssn"] == ["<redacted>"]
    assert sanitized["phone"] == ["<redacted>"]
    assert sanitized["amount"] == ["$1,000.00"]
    assert sanitized["url"] == ["<redacted>"]


def test_build_prompt_redacts_pii_in_context():
    prompt = _build_prompt(
        ["email", "amount"],
        ["email", "amount"],
        {
            "email": ["alice@example.com"],
            "amount": ["$1,000.00"],
        },
        [],
    )
    assert "alice@example.com" not in prompt
    assert "<redacted>" in prompt
    assert "$1,000.00" in prompt


@patch("services.llm_mapping.llm_provider_available", return_value=True)
def test_llm_invented_transform_requires_human_accept(_mock_avail):
    """LLM-invented transforms must not auto-apply — human accept on Map."""
    baseline = [{"source": "AMT", "target": "amount", "confidence": 0.7, "transform": None}]
    mock_response = MagicMock()
    mock_response.success = True
    mock_response.content = (
        '{"mappings": [{"source": "AMT", "target": "amount", "confidence": 0.92, '
        '"reason": "currency", "transformation": "currency"}]}'
    )
    mock_response.provider = "mock"
    mock_chain = MagicMock()
    mock_chain.generate.return_value = mock_response

    with patch("src.ai.llm.fallback.DataTransferFallbackChain", return_value=mock_chain):
        merged, meta = refine_mappings_with_llm(
            baseline, ["AMT"], ["amount"], enabled=True,
        )

    assert meta["llm_used"] is True
    amt = next(m for m in merged if m["source"] == "AMT")
    assert amt.get("llm_invented_transform") is True
    assert amt.get("suggested_transform") == "currency"
    assert amt.get("transform") in (None, "", "none")
    assert amt.get("requires_review") is True


def test_ai_never_decides_preflight_gates():
    from services.llm_policy import ai_may_decide_preflight_gate

    for gid in (
        "g4_mapping_confidence",
        "g8_reconciliation",
        "g9_data_integrity",
        "g1_source",
    ):
        assert ai_may_decide_preflight_gate(gid) is False


def test_pipeline_does_not_reapply_held_llm_transform():
    """attach_transforms must not infer over an LLM invent hold."""
    from services.transform_resolver import attach_transforms_to_mappings, resolve_transform

    held = {
        "source": "AMT",
        "target": "amount",
        "confidence": 0.92,
        "transform": None,
        "llm_invented_transform": True,
        "suggested_transform": "currency",
        "requires_review": True,
    }
    assert resolve_transform(held, column_types={"AMT": "VARCHAR"}) == "none"
    attached = attach_transforms_to_mappings(
        [held], column_types={"AMT": "VARCHAR"}, dest_types={"amount": "DECIMAL"},
    )
    assert attached[0]["transform"] == "none"
    assert attached[0]["suggested_transform"] == "currency"
    assert attached[0]["llm_invented_transform"] is True

