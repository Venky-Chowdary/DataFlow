"""
DataTransfer.space — LLM Provider Abstraction

Supports OpenAI, Anthropic, Ollama, and local fallback.
Works without API keys using local reasoning.
"""

from __future__ import annotations
import os
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class LLMResponse:
    """Response from an LLM provider."""
    content: str
    success: bool = True
    provider: str = "local"
    model: str = "local"
    reasoning: str = ""
    tokens_used: int = 0
    metadata: dict = field(default_factory=dict)


class DataTransferLLMProvider(ABC):
    """Abstract LLM provider interface."""

    name: str = "base"

    @abstractmethod
    def is_available(self) -> bool:
        pass

    @abstractmethod
    def generate(self, prompt: str, system: str = "", max_tokens: int = 1024) -> LLMResponse:
        pass


class DataTransferOpenAIProvider(DataTransferLLMProvider):
    name = "openai"

    def __init__(self, model: str | None = None):
        self.model = model or os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
        self._client = None
        self._init_client()

    def _init_client(self):
        try:
            from services.integrations_store import resolve_provider_api_key

            api_key = resolve_provider_api_key("openai")
            if api_key:
                try:
                    from openai import OpenAI

                    self._client = OpenAI(api_key=api_key)
                except ImportError:
                    pass
        except Exception:
            self._client = None

    def is_available(self) -> bool:
        return self._client is not None

    def generate(self, prompt: str, system: str = "", max_tokens: int = 1024) -> LLMResponse:
        if not self.is_available():
            return LLMResponse(content="", success=False, provider=self.name)

        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        try:
            response = self._client.chat.completions.create(
                model=self.model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=0.1,
            )
            content = response.choices[0].message.content or ""
            return LLMResponse(
                content=content,
                success=True,
                provider=self.name,
                model=self.model,
                tokens_used=response.usage.total_tokens if response.usage else 0,
            )
        except Exception as e:
            return LLMResponse(content="", success=False, provider=self.name, metadata={"error": str(e)})


class DataTransferAnthropicProvider(DataTransferLLMProvider):
    name = "anthropic"

    def __init__(self, model: str | None = None):
        from services.integrations_store import resolve_provider_model

        self.model = model or resolve_provider_model("anthropic", "claude-sonnet-4-20250514")
        self._client = None
        self._init_client()

    def _init_client(self):
        try:
            from services.integrations_store import resolve_provider_api_key

            api_key = resolve_provider_api_key("anthropic")
            if api_key:
                try:
                    import anthropic

                    self._client = anthropic.Anthropic(api_key=api_key)
                except ImportError:
                    pass
        except Exception:
            self._client = None

    def is_available(self) -> bool:
        return self._client is not None

    def generate(self, prompt: str, system: str = "", max_tokens: int = 1024) -> LLMResponse:
        if not self.is_available():
            return LLMResponse(content="", success=False, provider=self.name)

        try:
            response = self._client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                system=system or "You are a data engineering expert for DataTransfer.space.",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
            )
            content = response.content[0].text if response.content else ""
            return LLMResponse(
                content=content,
                success=True,
                provider=self.name,
                model=self.model,
                tokens_used=response.usage.input_tokens + response.usage.output_tokens,
            )
        except Exception as e:
            return LLMResponse(content="", success=False, provider=self.name, metadata={"error": str(e)})

    def generate_agent(
        self,
        messages: list[dict],
        system: str = "",
        tools: list[dict] | None = None,
        max_tokens: int = 4096,
    ) -> dict:
        """Anthropic-style agent turn with optional tool use."""
        if not self.is_available():
            return {"success": False, "error": "Anthropic not available"}

        try:
            kwargs: dict = {
                "model": self.model,
                "max_tokens": max_tokens,
                "system": system or "You are Data Pilot for DataTransfer.space.",
                "messages": messages,
                "temperature": 0.2,
            }
            if tools:
                kwargs["tools"] = tools
            response = self._client.messages.create(**kwargs)

            text_parts: list[str] = []
            tool_calls: list[dict] = []
            for block in response.content:
                if block.type == "text":
                    text_parts.append(block.text)
                elif block.type == "tool_use":
                    tool_calls.append({
                        "id": block.id,
                        "name": block.name,
                        "input": block.input,
                    })

            return {
                "success": True,
                "content": "\n".join(text_parts).strip(),
                "tool_calls": tool_calls,
                "stop_reason": response.stop_reason,
                "usage": {
                    "input": response.usage.input_tokens,
                    "output": response.usage.output_tokens,
                },
            }
        except Exception as e:
            return {"success": False, "error": str(e)}


class DataTransferOllamaProvider(DataTransferLLMProvider):
    name = "ollama"

    def __init__(self, model: str | None = None, base_url: str | None = None):
        from services.integrations_store import resolve_ollama_base_url, resolve_provider_model

        self.model = model or resolve_provider_model("ollama", "llama3.2")
        self.base_url = base_url or resolve_ollama_base_url()
        self._available = None

    def is_available(self) -> bool:
        if self._available is not None:
            return self._available
        try:
            import httpx
            resp = httpx.get(f"{self.base_url}/api/tags", timeout=2.0)
            self._available = resp.status_code == 200
        except Exception:
            self._available = False
        return self._available

    def generate(self, prompt: str, system: str = "", max_tokens: int = 1024) -> LLMResponse:
        if not self.is_available():
            return LLMResponse(content="", success=False, provider=self.name)

        try:
            import httpx
            resp = httpx.post(
                f"{self.base_url}/api/generate",
                json={
                    "model": self.model,
                    "prompt": f"{system}\n\n{prompt}" if system else prompt,
                    "stream": False,
                    "options": {"num_predict": max_tokens, "temperature": 0.1},
                },
                timeout=60.0,
            )
            data = resp.json()
            return LLMResponse(
                content=data.get("response", ""),
                success=True,
                provider=self.name,
                model=self.model,
            )
        except Exception as e:
            return LLMResponse(content="", success=False, provider=self.name, metadata={"error": str(e)})


class DataTransferLocalProvider(DataTransferLLMProvider):
    """Local reasoning without external API — uses RAG + knowledge base."""

    name = "local"

    def is_available(self) -> bool:
        return True

    def generate(self, prompt: str, system: str = "", max_tokens: int = 1024) -> LLMResponse:
        """Generate structured response using local knowledge."""
        from ..knowledge.synonyms import resolve_canonical, are_synonyms
        from ..knowledge.semantic_patterns import SEMANTIC_PATTERNS

        reasoning_steps = []
        answer_parts = []

        # Extract column names from prompt
        import re
        columns = re.findall(r"'([^']+)'|\"([^\"]+)\"|`([^`]+)`", prompt)
        flat_cols = [c for group in columns for c in group if c]

        for col in flat_cols[:5]:
            canonical = resolve_canonical(col)
            matched = None
            for pattern in SEMANTIC_PATTERNS:
                all_terms = [p.lower() for p in pattern.patterns + pattern.synonyms]
                if col.lower().replace("-", "_") in all_terms or canonical in all_terms:
                    matched = pattern.name
                    break
            reasoning_steps.append(
                f"Column '{col}' → canonical '{canonical}' → type '{matched or 'unknown'}'"
            )
            if matched:
                answer_parts.append(f"{col}: {matched}")

        content = json.dumps({
            "analysis": answer_parts,
            "reasoning": reasoning_steps,
            "method": "local_knowledge",
        }, indent=2)

        return LLMResponse(
            content=content,
            success=True,
            provider=self.name,
            model="local_knowledge",
            reasoning="\n".join(reasoning_steps),
        )


MODEL_CAPABILITY_MATRIX = [
    {
        "provider": "anthropic",
        "label": "Anthropic Claude",
        "default_model": "claude-sonnet-4-20250514",
        "env_key": "ANTHROPIC_API_KEY",
        "package": "anthropic",
        "tier": "cloud",
        "roles": ["agent_tool_use", "schema_reasoning", "migration_planning", "policy_explanation"],
        "best_for": "Long-horizon Data Pilot agent runs, tool use, schema-policy reasoning, and migration plan review.",
    },
    {
        "provider": "openai",
        "label": "OpenAI",
        "default_model": "gpt-4o-mini",
        "env_key": "OPENAI_API_KEY",
        "package": "openai",
        "tier": "cloud",
        "roles": ["copilot_chat", "rag_answering", "mapping_explanation", "fallback_generation"],
        "best_for": "Fast grounded chat, mapping explanation, RAG answers, and second-line cloud fallback.",
    },
    {
        "provider": "ollama",
        "label": "Ollama",
        "default_model": "llama3.2",
        "env_key": "",
        "package": "httpx",
        "tier": "local",
        "roles": ["private_local_generation", "offline_assist", "fallback_generation"],
        "best_for": "Private/local assistant mode when cloud credentials are unavailable.",
    },
    {
        "provider": "local",
        "label": "Local deterministic engine",
        "default_model": "local_knowledge",
        "env_key": "",
        "package": "",
        "tier": "deterministic",
        "roles": ["semantic_rules", "rag_retrieval", "preflight_gates", "mapping_assignment"],
        "best_for": "Always-on fail-closed semantic analysis, RAG retrieval, preflight, and deterministic mapping safeguards.",
    },
]


def _package_available(package: str) -> bool:
    if not package:
        return True
    try:
        __import__(package)
        return True
    except Exception:
        return False


def get_model_capabilities() -> dict:
    """Expose model/provider readiness without making network calls to cloud APIs."""
    from services.integrations_store import get_ai_provider_configs

    stored = get_ai_provider_configs()
    providers = {
        "anthropic": DataTransferAnthropicProvider(),
        "openai": DataTransferOpenAIProvider(),
        "ollama": DataTransferOllamaProvider(),
        "local": DataTransferLocalProvider(),
    }
    rows = []
    for item in MODEL_CAPABILITY_MATRIX:
        provider = providers[item["provider"]]
        env_key = item.get("env_key") or ""
        persisted = stored.get(item["provider"], {})
        configured = bool(os.environ.get(env_key)) if env_key else item["provider"] in {"ollama", "local"}
        if env_key and persisted.get("configured"):
            configured = True
        if item["provider"] == "ollama" and persisted.get("base_url"):
            configured = True
        installed = _package_available(item.get("package", ""))
        available = provider.is_available()
        rows.append({
            **item,
            "configured": configured,
            "package_installed": installed,
            "available": available,
            "status": "ready" if available else "configure" if item["tier"] == "cloud" else "offline",
        })

    active = next((p for p in rows if p["available"]), rows[-1])
    return {
        "active_provider": active["provider"],
        "active_model": active["default_model"],
        "agent_mode": (
            "anthropic_tools" if providers["anthropic"].is_available()
            else "openai_tools" if providers["openai"].is_available()
            else "ollama_tools" if providers["ollama"].is_available()
            else "local_tools"
        ),
        "fallback_order": ["anthropic", "openai", "ollama", "rag", "local"],
        "providers": rows,
        "guarantees": [
            "Cloud models are used only when their API key and SDK are configured.",
            "RAG and deterministic mapping continue when cloud providers are unavailable.",
            "Preflight gates and schema-policy blockers do not depend on probabilistic model output.",
            "Ambiguous mappings remain reviewable instead of being silently auto-applied.",
        ],
    }
