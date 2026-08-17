"""
Datawrap — LLM Provider Abstraction

Supports OpenAI, Anthropic, Ollama, and local fallback.
Works without API keys using local reasoning.
"""

from __future__ import annotations

import json
import os
from services.brand_env import getenv_brand
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

try:
    from services.value_serializer import json_default
except ImportError:
    json_default = str  # fallback when running outside api root


def _is_valid_api_key(value: str) -> bool:
    """Reject masked or sentinel keys that may leak through the store."""
    if not value or not isinstance(value, str):
        return False
    stripped = value.strip()
    return not (stripped.startswith("[") or stripped.startswith("•"))


# Process-wide: a 401 on one OpenAI/Anthropic instance must disable all instances
# until the process restarts (operator saves a new key and restarts the API).
_AUTH_FAILED_PROVIDERS: set[str] = set()


def clear_auth_failures() -> None:
    """Test helper — reset soft auth disables after a key rotation."""
    _AUTH_FAILED_PROVIDERS.clear()


def _provider_auth_failed(name: str) -> bool:
    return name in _AUTH_FAILED_PROVIDERS


def _mark_provider_auth_failed(name: str, err: str) -> bool:
    """Return True if this error should soft-disable the provider."""
    low = (err or "").lower()
    if "401" in low or "invalid_api_key" in low or "incorrect api key" in low or "unauthorized" in low:
        _AUTH_FAILED_PROVIDERS.add(name)
        return True
    return False


# A key check is a person waiting on a button, so it is bounded: one attempt,
# a short deadline, and a plain timeout message instead of SDK backoff.
VERIFY_TIMEOUT_SECONDS = 12.0


def verify_cloud_api_key(provider: str, api_key: str) -> tuple[bool, str]:
    """Live-check a cloud key before persisting. Returns (ok, error_message)."""
    name = (provider or "").strip().lower()
    if name not in {"openai", "anthropic"}:
        return True, ""
    if not _is_valid_api_key(api_key):
        return False, "API key is empty or masked"
    try:
        if name == "openai":
            from openai import OpenAI

            OpenAI(
                api_key=api_key.strip(),
                timeout=VERIFY_TIMEOUT_SECONDS,
                max_retries=0,
            ).models.list()
            _AUTH_FAILED_PROVIDERS.discard("openai")
            return True, ""
        import anthropic
        from services.integrations_store import resolve_provider_model

        model = resolve_provider_model("anthropic", "claude-sonnet-4-20250514")
        anthropic.Anthropic(
            api_key=api_key.strip(),
            timeout=VERIFY_TIMEOUT_SECONDS,
            max_retries=0,
        ).messages.create(
            model=model,
            max_tokens=1,
            messages=[{"role": "user", "content": "ping"}],
        )
        _AUTH_FAILED_PROVIDERS.discard("anthropic")
        return True, ""
    except Exception as e:
        _mark_provider_auth_failed(name, str(e))
        msg = str(e)
        low = msg.lower()
        if "invalid_api_key" in low or "incorrect api key" in low or "401" in msg:
            return False, "Incorrect API key — paste a valid key from the provider console"
        if "timeout" in low or "timed out" in low:
            return (
                False,
                f"{name} did not answer within {int(VERIFY_TIMEOUT_SECONDS)}s — "
                "the key was not verified. Try again.",
            )
        return False, msg[:240]


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
        if _provider_auth_failed(self.name):
            self._client = None
            return
        try:
            from services.integrations_store import resolve_provider_api_key

            api_key = resolve_provider_api_key("openai")
            if api_key and _is_valid_api_key(api_key):
                try:
                    from openai import OpenAI

                    self._client = OpenAI(api_key=api_key)
                except ImportError:
                    pass
        except Exception:
            self._client = None

    def is_available(self) -> bool:
        return self._client is not None and not _provider_auth_failed(self.name)

    def _mark_auth_failure(self, err: str) -> None:
        if _mark_provider_auth_failed(self.name, err):
            self._client = None

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
            self._mark_auth_failure(str(e))
            return LLMResponse(content="", success=False, provider=self.name, metadata={"error": str(e)})

    def generate_agent(
        self,
        messages: list[dict],
        system: str = "",
        tools: list[dict] | None = None,
        max_tokens: int = 4096,
    ) -> dict:
        """OpenAI chat.completions turn with optional native function/tool calling."""
        if not self.is_available():
            return {"success": False, "error": "OpenAI not available"}

        try:
            openai_tools = None
            if tools:
                openai_tools = []
                for tool in tools:
                    # Accept Anthropic-shaped TOOL_DEFINITIONS or already-OpenAI shapes.
                    if tool.get("type") == "function" and isinstance(tool.get("function"), dict):
                        openai_tools.append(tool)
                        continue
                    openai_tools.append({
                        "type": "function",
                        "function": {
                            "name": tool["name"],
                            "description": tool.get("description") or "",
                            "parameters": tool.get("input_schema")
                            or tool.get("parameters")
                            or {"type": "object", "properties": {}},
                        },
                    })

            kwargs: dict = {
                "model": self.model,
                "messages": (
                    ([{"role": "system", "content": system}] if system else [])
                    + list(messages)
                ),
                "max_tokens": max_tokens,
                "temperature": 0.1,
            }
            if openai_tools:
                kwargs["tools"] = openai_tools
                kwargs["tool_choice"] = "auto"

            response = self._client.chat.completions.create(**kwargs)
            choice = response.choices[0].message
            tool_calls: list[dict] = []
            for tc in choice.tool_calls or []:
                raw_args = tc.function.arguments or "{}"
                try:
                    parsed = json.loads(raw_args) if isinstance(raw_args, str) else (raw_args or {})
                except Exception:
                    parsed = {}
                if not isinstance(parsed, dict):
                    parsed = {}
                tool_calls.append({
                    "id": tc.id,
                    "name": tc.function.name,
                    "input": parsed,
                })

            usage = response.usage
            return {
                "success": True,
                "content": (choice.content or "").strip(),
                "tool_calls": tool_calls,
                "stop_reason": "tool_calls" if tool_calls else (choice.finish_reason or "stop"),
                "usage": {
                    "input": getattr(usage, "prompt_tokens", 0) or 0,
                    "output": getattr(usage, "completion_tokens", 0) or 0,
                },
            }
        except Exception as e:
            self._mark_auth_failure(str(e))
            return {"success": False, "error": str(e)}


class DataTransferAnthropicProvider(DataTransferLLMProvider):
    name = "anthropic"

    def __init__(self, model: str | None = None):
        from services.integrations_store import resolve_provider_model

        self.model = model or resolve_provider_model("anthropic", "claude-sonnet-4-20250514")
        self._client = None
        self._init_client()

    def _init_client(self):
        if _provider_auth_failed(self.name):
            self._client = None
            return
        try:
            from services.integrations_store import resolve_provider_api_key

            api_key = resolve_provider_api_key("anthropic")
            if api_key and _is_valid_api_key(api_key):
                try:
                    import anthropic

                    self._client = anthropic.Anthropic(api_key=api_key)
                except ImportError:
                    pass
        except Exception:
            self._client = None

    def is_available(self) -> bool:
        return self._client is not None and not _provider_auth_failed(self.name)

    def _mark_auth_failure(self, err: str) -> None:
        if _mark_provider_auth_failed(self.name, err):
            self._client = None

    def generate(self, prompt: str, system: str = "", max_tokens: int = 1024) -> LLMResponse:
        if not self.is_available():
            return LLMResponse(content="", success=False, provider=self.name)

        try:
            response = self._client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                system=system or "You are a data engineering expert for Datawrap.",
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
            self._mark_auth_failure(str(e))
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
                "system": system or "You are Datawrap Pilot for Datawrap.",
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
            self._mark_auth_failure(str(e))
            return {"success": False, "error": str(e)}


class DataTransferOllamaProvider(DataTransferLLMProvider):
    name = "ollama"

    def __init__(self, model: str | None = None, base_url: str | None = None):
        from services.integrations_store import (
            resolve_ollama_base_url,
            resolve_provider_model,
        )

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
        from ..knowledge.semantic_patterns import SEMANTIC_PATTERNS
        from ..knowledge.synonyms import resolve_canonical

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
        }, indent=2, default=json_default)

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
        "best_for": "Long-horizon Datawrap Pilot agent runs, tool use, schema-policy reasoning, and migration plan review.",
    },
    {
        "provider": "openai",
        "label": "OpenAI",
        "default_model": "gpt-4o-mini",
        "env_key": "OPENAI_API_KEY",
        "package": "openai",
        "tier": "cloud",
        "roles": ["agent_tool_use", "copilot_chat", "rag_answering", "mapping_explanation", "fallback_generation"],
        "best_for": "Datawrap Pilot tool loops, grounded chat, mapping explanation, RAG answers, and cloud fallback.",
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
        "label": "Datawrap Pilot local engine",
        "default_model": "pilot_local_engine",
        "env_key": "",
        "package": "",
        "tier": "deterministic",
        "roles": [
            "agent_tool_use",
            "copilot_chat",
            "semantic_rules",
            "rag_retrieval",
            "preflight_gates",
            "mapping_assignment",
        ],
        "best_for": (
            "Primary Datawrap Pilot chatbot — local NL→tools→compose (local tool loop) for "
            "aggregates, schema, transfers-with-Confirm, jobs, product how-tos. "
            "OpenAI/Anthropic/Ollama are optional polish add-ons (engine=hybrid only)."
        ),
    },
]


def _saved_cloud_providers() -> list[str]:
    """Cloud providers the operator enabled and gave a resolvable key."""
    try:
        from services.integrations_store import configured_ai_providers

        return list(configured_ai_providers())
    except Exception:
        return []


def usable_cloud_providers() -> list[str]:
    """Saved providers Pilot can actually call — a rejected key is not usable.

    A 401 seen this process means the engine must stop promising that provider,
    even though the key is still saved and still shown as configured.
    """
    return [p for p in _saved_cloud_providers() if not _provider_auth_failed(p)]


def _package_available(package: str) -> bool:
    if not package:
        return True
    try:
        __import__(package)
        return True
    except Exception:
        return False


def pilot_engine_decision() -> dict:
    """Which engine Pilot will use, and the reason — never a credential.

    Precedence: DATAFLOW_PILOT_ENGINE, then the saved workspace preference,
    then ``auto``. Under ``auto`` a cloud provider the operator configured in
    Settings turns Pilot hybrid; with nothing configured Pilot stays on the
    local engine, which always works offline.
    """
    from services.integrations_store import get_pilot_engine_preference

    configured = usable_cloud_providers()
    rejected = [p for p in _saved_cloud_providers() if _provider_auth_failed(p)]

    env_raw = (getenv_brand("PILOT_ENGINE") or "").strip().lower()
    if env_raw in {"local", "hybrid", "cloud"}:
        return {
            "engine": env_raw,
            "source": "environment",
            "reason": f"DATAFLOW_PILOT_ENGINE={env_raw} pins the engine.",
            "configured_providers": configured,
        }

    try:
        saved = get_pilot_engine_preference()
    except Exception:
        saved = "auto"

    if saved == "local":
        return {
            "engine": "local",
            "source": "workspace_setting",
            "reason": "Settings → AI Models pins Pilot to the local engine.",
            "configured_providers": configured,
        }

    if saved in {"hybrid", "cloud"}:
        reason = (
            f"Settings → AI Models selects {saved}; configured provider(s): "
            f"{', '.join(configured)}."
            if configured
            else (
                f"Settings → AI Models selects {saved}, but no provider key is "
                "configured — Pilot answers on the local engine until one is saved."
            )
        )
        return {
            "engine": saved,
            "source": "workspace_setting",
            "reason": reason,
            "configured_providers": configured,
        }

    if configured:
        return {
            "engine": "hybrid",
            "source": "configured_provider",
            "reason": (
                f"Provider key configured for {', '.join(configured)} — Pilot runs its "
                "tools locally, then uses that provider for the answer."
            ),
            "configured_providers": configured,
        }

    if rejected:
        return {
            "engine": "local",
            "source": "default",
            "reason": (
                f"The saved key for {', '.join(rejected)} was rejected — Pilot uses the "
                "local engine until a valid key is saved."
            ),
            "configured_providers": [],
        }

    return {
        "engine": "local",
        "source": "default",
        "reason": "No AI provider key configured — Pilot uses the local engine.",
        "configured_providers": [],
    }


def resolve_pilot_engine() -> str:
    """Single source of truth for Pilot engine selection."""
    return pilot_engine_decision()["engine"]


def pick_narration_provider():
    """Optional polish provider when the engine is hybrid/cloud.

    Never required for Pilot to work. A provider the operator configured with a
    key wins, because that is the one they asked us to use; otherwise a
    self-hosted Ollama is preferred over nothing.
    """
    builders = {
        "openai": (DataTransferOpenAIProvider, "openai_polish"),
        "anthropic": (DataTransferAnthropicProvider, "anthropic_polish"),
    }
    configured = usable_cloud_providers()

    for name in configured:
        builder, method = builders.get(name, (None, ""))
        if builder is None:
            continue
        candidate = builder()
        if candidate.is_available():
            return candidate, method

    ollama = DataTransferOllamaProvider()
    if ollama.is_available():
        return ollama, "ollama_polish"
    for name, (builder, method) in builders.items():
        if name in configured:
            continue
        candidate = builder()
        if candidate.is_available():
            return candidate, method
    return None, ""


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
        persisted = stored.get(item["provider"], {})
        # `configured` from the store already means "a key resolves and the
        # provider is enabled" — env or encrypted store, never a masked value.
        if item["provider"] == "local":
            configured = True
        else:
            configured = bool(persisted.get("configured"))
        installed = _package_available(item.get("package", ""))
        available = provider.is_available()
        blocked_reason = ""
        provider_off = item["tier"] == "cloud" and not persisted.get("enabled", True)
        if provider_off:
            # Turning a provider off is the operator's own decision, so it
            # outranks any stale rejection cached for its withheld key.
            available = False
            status = "configure"
            blocked_reason = "Disabled in Settings — enable it to let Pilot use it."
        elif _provider_auth_failed(item["provider"]):
            available = False
            status = "invalid_key"
            blocked_reason = (
                "The provider rejected this key (401). Save a valid key, or press "
                "Test key to re-check it."
            )
        elif available:
            status = "ready"
        elif item["tier"] == "cloud":
            status = "configure"
            if not installed:
                blocked_reason = (
                    f"The {item['package']} Python package is not installed on the API host."
                )
            elif not persisted.get("enabled", True):
                blocked_reason = "Disabled in Settings — enable it to let Pilot use it."
            elif not configured:
                blocked_reason = "No API key saved for this provider."
            else:
                blocked_reason = "A key is saved but did not load — re-save it."
        else:
            status = "offline"
            if item["provider"] == "ollama":
                blocked_reason = "No Ollama server answered at the configured base URL."
        rows.append({
            **item,
            "configured": configured,
            "package_installed": installed,
            "available": available,
            "status": status,
            "blocked_reason": blocked_reason,
        })

    active_local = next((p for p in rows if p["provider"] == "local"), rows[-1])
    decision = pilot_engine_decision()
    engine = decision["engine"]
    if engine in {"hybrid", "cloud"}:
        _picked, picked_mode = pick_narration_provider()
        picked_name = picked_mode.replace("_polish", "")
        active = next(
            (p for p in rows if p["provider"] == picked_name and p["available"]),
            active_local,
        )
        agent_mode = picked_mode or "local_tools"
    else:
        active = active_local
        agent_mode = "local_tools"
    return {
        "active_provider": active["provider"],
        "active_model": active.get("default_model") or active.get("model") or "local",
        "agent_mode": agent_mode,
        "pilot_engine": engine,
        "pilot_engine_source": decision["source"],
        "pilot_engine_reason": decision["reason"],
        "configured_providers": decision["configured_providers"],
        "fallback_order": ["local", "ollama", "anthropic", "openai"],
        "providers": rows,
        "guarantees": [
            "Primary chatbot = Datawrap local engine (NL → tools → compose). Works with zero cloud keys.",
            "Save a provider key in Settings and Pilot uses it automatically; with no key saved Pilot stays local.",
            "DATAFLOW_PILOT_ENGINE, when set, overrides the workspace choice.",
            "Cloud providers are optional and only narrate: tools, gates and proofs always run locally, so a provider outage changes wording, never correctness.",
            "Grounded tool results are executed once; mutations always require operator Confirm.",
            "Saving a cloud key in Settings live-checks it; invalid keys are rejected.",
        ],
    }
