"""List-price estimates only; actual provider invoices may differ."""

PRICING_AS_OF = "2026-10"
PRICING_USD_PER_1M_INPUT_TOKENS = {
    "openai/text-embedding-3-small": 0.02,
    "openai/text-embedding-3-large": 0.13,
    "openai/text-embedding-ada-002": 0.10,
    "cohere/embed-english-v3.0": 0.10,
    "cohere/embed-multilingual-v3.0": 0.10,
    "bedrock/amazon.titan-embed-text-v2:0": 0.02,
}


def estimated_cost_usd(
    provider: str, model: str, input_tokens: int, base_model: str | None = None
) -> float | None:
    if provider in {"hash", "local", "sentence_transformers", "tfidf_fallback"}:
        return 0.0
    price_key = f"{provider}/{model}"
    if provider == "azure_openai":
        price_key = f"openai/{base_model}" if base_model else ""
    price = PRICING_USD_PER_1M_INPUT_TOKENS.get(price_key)
    return None if price is None else price * max(0, input_tokens) / 1_000_000
