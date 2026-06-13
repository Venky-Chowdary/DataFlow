"""
DataTransfer.space — LLM Module

Provider abstraction with chain-of-thought reasoning and fallback chain.
"""

from .provider import (
    DataTransferLLMProvider,
    DataTransferOpenAIProvider,
    DataTransferAnthropicProvider,
    DataTransferOllamaProvider,
    DataTransferLocalProvider,
    LLMResponse,
)
from .prompts import (
    SCHEMA_ANALYSIS_PROMPT,
    COLUMN_MAPPING_PROMPT,
    PII_DETECTION_PROMPT,
    TRANSFORMATION_PROMPT,
    NATURAL_LANGUAGE_PROMPT,
    CHAIN_OF_THOUGHT_TEMPLATE,
)
from .chain import DataTransferReasoningChain
from .fallback import DataTransferFallbackChain

__all__ = [
    "DataTransferLLMProvider",
    "DataTransferOpenAIProvider",
    "DataTransferAnthropicProvider",
    "DataTransferOllamaProvider",
    "DataTransferLocalProvider",
    "LLMResponse",
    "SCHEMA_ANALYSIS_PROMPT",
    "COLUMN_MAPPING_PROMPT",
    "PII_DETECTION_PROMPT",
    "TRANSFORMATION_PROMPT",
    "NATURAL_LANGUAGE_PROMPT",
    "CHAIN_OF_THOUGHT_TEMPLATE",
    "DataTransferReasoningChain",
    "DataTransferFallbackChain",
]
