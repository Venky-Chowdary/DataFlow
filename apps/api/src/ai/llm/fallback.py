"""
DataTransfer.space — LLM Fallback Chain

Graceful degradation: LLM → RAG → Pattern matching.
"""

from __future__ import annotations
import json
from dataclasses import dataclass

from .provider import (
    DataTransferLLMProvider,
    DataTransferAnthropicProvider,
    DataTransferOpenAIProvider,
    DataTransferOllamaProvider,
    DataTransferLocalProvider,
    LLMResponse,
)


@dataclass
class FallbackResult:
    content: str
    success: bool
    method: str  # "anthropic", "openai", "ollama", "rag", "pattern", "local"
    reasoning: str = ""
    provider: str = ""
    confidence: float = 0.0


class DataTransferFallbackChain:
    """
    Fallback chain for AI generation:
    1. Anthropic (if API key available)
    2. OpenAI (if API key available)
    3. Ollama (if running locally)
    4. RAG + knowledge base
    5. Pattern matching (always available)
    """

    def __init__(self):
        self.providers: list[DataTransferLLMProvider] = [
            DataTransferAnthropicProvider(),
            DataTransferOpenAIProvider(),
            DataTransferOllamaProvider(),
            DataTransferLocalProvider(),
        ]
        self._rag = None
        self._chain = None

    @property
    def rag(self):
        if self._rag is None:
            from ..rag.pipeline import get_rag_pipeline
            self._rag = get_rag_pipeline()
        return self._rag

    @property
    def reasoning_chain(self):
        if self._chain is None:
            from .chain import DataTransferReasoningChain
            self._chain = DataTransferReasoningChain()
        return self._chain

    def get_available_providers(self) -> list[str]:
        return [p.name for p in self.providers if p.is_available()]

    def generate(self, prompt: str, system: str = "") -> LLMResponse:
        """Try providers in order until one succeeds."""
        for provider in self.providers:
            if provider.is_available():
                response = provider.generate(prompt, system)
                if response.success:
                    return response
        return LLMResponse(content="", success=False, provider="none")

    def analyze_with_fallback(
        self,
        column_name: str,
        sample_values: list[str] | None = None,
    ) -> FallbackResult:
        """Analyze column with full fallback chain."""
        # Try chain-of-thought first (always works)
        result = self.reasoning_chain.analyze_column(column_name, sample_values)
        answer = result.answer if isinstance(result.answer, dict) else {}

        return FallbackResult(
            content=json.dumps(answer, indent=2),
            success=True,
            method=result.method,
            reasoning="\n".join(f"Step {s.step}: {s.description} → {s.result}" for s in result.reasoning),
            confidence=result.confidence,
            provider="chain_of_thought",
        )

    def map_with_fallback(
        self,
        source_columns: list[str],
        target_columns: list[str],
        source_samples: dict[str, list[str]] | None = None,
    ) -> FallbackResult:
        """Map columns with full fallback chain."""
        result = self.reasoning_chain.map_columns(source_columns, target_columns, source_samples)
        return FallbackResult(
            content=json.dumps(result.answer, indent=2),
            success=True,
            method=result.method,
            reasoning="\n".join(f"Step {s.step}: {s.description} → {s.result}" for s in result.reasoning),
            confidence=result.confidence,
            provider="chain_of_thought",
        )

    def query_with_fallback(self, question: str) -> FallbackResult:
        """Answer natural language query with fallback."""
        # Try LLM first

        rag_response = self.rag.query(question)

        if rag_response.method == "llm":
            return FallbackResult(
                content=rag_response.answer,
                success=True,
                method="llm",
                reasoning=rag_response.reasoning,
                confidence=rag_response.confidence,
                provider="llm",
            )

        # RAG fallback
        return FallbackResult(
            content=rag_response.answer,
            success=True,
            method="rag",
            reasoning=rag_response.reasoning,
            confidence=rag_response.confidence,
            provider="rag",
        )

    def get_status(self) -> dict:
        return {
            "available_providers": self.get_available_providers(),
            "fallback_order": ["anthropic", "openai", "ollama", "rag", "pattern", "local"],
            "rag_status": self.rag.get_status(),
        }
