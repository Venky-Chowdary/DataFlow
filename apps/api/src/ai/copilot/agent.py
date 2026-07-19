"""
DataTransfer.space — Copilot Agent

Customer-facing conversational agent — analyzes real data and responds
in natural language, not just transfer step suggestions.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from ..knowledge.copilot_knowledge import (
    CONVERSATION_TEMPLATES,
    COPILOT_PERSONA,
    INTENT_PATTERNS,
)
from ..knowledge.synonyms import are_synonyms, resolve_canonical
from .data_analyst import get_data_analyst


@dataclass
class CopilotMessage:
    role: str
    content: str


@dataclass
class CopilotResponse:
    answer: str
    intent: str
    confidence: float
    method: str
    reasoning: str = ""
    suggested_actions: list[dict] = field(default_factory=list)
    # Mutations / run-now style actions — UI must Confirm before applying.
    pending_actions: list[dict] = field(default_factory=list)
    needs_clarification: str = ""
    suggested_prompts: list[str] = field(default_factory=list)
    sources: list[dict] = field(default_factory=list)
    data_insight: dict | None = None
    tools_used: list[dict] = field(default_factory=list)


class DataTransferCopilotAgent:
    """Talks about your actual data in natural language."""

    def __init__(self):
        self._rag = None
        self._fallback = None
        self.analyst = get_data_analyst()

    @property
    def rag(self):
        if self._rag is None:
            from ..rag.pipeline import get_rag_pipeline
            self._rag = get_rag_pipeline()
        return self._rag

    @property
    def fallback(self):
        if self._fallback is None:
            from ..llm.fallback import DataTransferFallbackChain
            self._fallback = DataTransferFallbackChain()
        return self._fallback

    def chat(
        self,
        message: str,
        history: list[dict] | None = None,
        data_context: dict | None = None,
    ) -> CopilotResponse:
        """Delegate to Data Pilot — Anthropic/Cursor-style agent with tools."""
        from .pilot_agent import get_pilot_agent
        return get_pilot_agent().chat(message, history, data_context)

    def _chat_legacy(
        self,
        message: str,
        history: list[dict] | None = None,
        data_context: dict | None = None,
    ) -> CopilotResponse:
        message = message.strip()
        if not message:
            return CopilotResponse(
                answer=(
                    "Hi! I analyze your actual data — columns, PII, quality, and mappings. "
                    "Upload a file in Transfer or ask me about your HR, logistics, or payment datasets."
                ),
                intent="greeting",
                confidence=1.0,
                method="greeting",
                suggested_prompts=self.get_suggested_prompts()[:4],
            )

        intent = self._detect_intent(message)
        self.rag.ingestion.ensure_knowledge_loaded()

        # ── Priority 1: Real data analysis ──
        if self.analyst.wants_data_analysis(message, intent) or data_context:
            hint = self.analyst.extract_dataset_hint(message)
            insight = self.analyst.analyze_context(data_context, hint)
            if insight:
                answer = self.analyst.compose_response(insight, message, intent)
                return CopilotResponse(
                    answer=answer,
                    intent=intent,
                    confidence=0.93,
                    method="data_analysis",
                    reasoning=f"Analyzed dataset '{insight.dataset_name}' with {len(insight.columns)} columns",
                    suggested_prompts=self._data_follow_ups(insight),
                    data_insight={
                        "dataset": insight.dataset_name,
                        "columns": len(insight.columns),
                        "rows": insight.row_count,
                        "pii_count": len(insight.pii_columns),
                        "quality_score": insight.quality_score,
                    },
                )

        # ── Priority 2: Column-level semantic engine ──
        structured = self._structured_response(message, intent)
        if structured:
            return structured

        # ── Priority 3: Product how-to (only explicit product questions) ──
        if self._is_product_howto(message):
            template = self._match_template(message, intent)
            if template:
                return template

        # ── Priority 4: Trained RAG conversations ──
        retrieval = self.rag.retriever.retrieve(message, n_results=8)
        copilot_docs = [
            d for d in retrieval.documents
            if d.metadata.get("type") in ("copilot_training", "copilot_knowledge")
        ]
        if copilot_docs:
            best = copilot_docs[0]
            answer = self._extract_assistant_from_doc(best.text)
            if answer and "Go to **Transfer**" not in answer[:50]:
                return CopilotResponse(
                    answer=answer,
                    intent=intent,
                    confidence=min(best.score + 0.2, 0.95),
                    method="trained_rag",
                    suggested_prompts=self.get_suggested_prompts()[:3],
                )

        # ── Priority 5: External LLM ──
        llm_response = self._generate_with_llm(message, intent, copilot_docs, history, data_context)
        if llm_response:
            return llm_response

        # ── Fallback: offer data analysis ──
        datasets = self.analyst.list_datasets()
        if datasets:
            names = ", ".join(d["name"] for d in datasets[:4])
            return CopilotResponse(
                answer=(
                    f"I can analyze your data directly. Available datasets: **{names}**.\n\n"
                    f"Try: \"What's in the HR data?\" or \"Find PII in logistics CSV\""
                ),
                intent=intent,
                confidence=0.6,
                method="fallback",
                suggested_prompts=self.get_suggested_prompts(),
            )

        return CopilotResponse(
            answer="Upload a file in Transfer and I'll analyze your columns, detect PII, and suggest mappings in plain language.",
            intent="product_help",
            confidence=0.5,
            method="fallback",
            suggested_prompts=self.get_suggested_prompts(),
        )

    def _is_product_howto(self, message: str) -> bool:
        lower = message.lower()
        return any(p in lower for p in (
            "how do i", "how to", "what are preflight", "compare", "different from",
            "configure sso", "add a connector", "set up",
        ))

    def _detect_intent(self, message: str) -> str:
        lower = message.lower()
        scores: dict[str, int] = {}
        for intent, keywords in INTENT_PATTERNS.items():
            score = sum(1 for kw in keywords if kw in lower)
            if score:
                scores[intent] = score
        return max(scores, key=scores.get) if scores else "product_help"

    def _match_template(self, message: str, intent: str) -> CopilotResponse | None:
        lower = message.lower()
        best_score = 0
        best_template = None
        for tmpl in CONVERSATION_TEMPLATES:
            score = 0
            if tmpl["intent"] == intent:
                score += 2
            tmpl_words = set(re.findall(r"\w+", tmpl["user"].lower()))
            msg_words = set(re.findall(r"\w+", lower))
            score += len(tmpl_words & msg_words)
            if score > best_score:
                best_score = score
                best_template = tmpl
        if best_template and best_score >= 4:
            return CopilotResponse(
                answer=best_template["assistant"],
                intent=best_template["intent"],
                confidence=min(0.7 + best_score * 0.05, 0.95),
                method="template",
                suggested_actions=best_template.get("actions", []),
                suggested_prompts=self._follow_up_prompts(best_template["intent"]),
            )
        return None

    def _generate_with_llm(
        self, message: str, intent: str, copilot_docs: list,
        history: list[dict] | None, data_context: dict | None,
    ) -> CopilotResponse | None:
        from ..llm.prompts import COPILOT_CHAT_PROMPT

        context_parts = []
        if data_context:
            context_parts.append(f"Active user data: {data_context.get('name', 'upload')}, columns: {data_context.get('columns', [])}")
        for doc in copilot_docs[:5]:
            context_parts.append(doc.text[:300])
        context = "\n\n".join(context_parts) or "No context."

        history_text = ""
        if history:
            for msg in history[-6:]:
                history_text += f"{msg.get('role', 'user').capitalize()}: {msg.get('content', '')}\n"

        prompt = COPILOT_CHAT_PROMPT.format(
            persona=COPILOT_PERSONA,
            intent=intent,
            history=history_text or "None.",
            context=context,
            message=message,
        )
        response = self.fallback.generate(prompt, system=COPILOT_PERSONA)
        if not response.success or not response.content.strip():
            return None
        if response.provider in ("local", "local_knowledge", "chain_of_thought", "none"):
            return None
        content = response.content.strip()
        if content.startswith("{"):
            return None
        return CopilotResponse(
            answer=content, intent=intent, confidence=0.88,
            method=response.provider or "llm",
        )

    def _structured_response(self, message: str, intent: str) -> CopilotResponse | None:
        lower = message.lower()
        map_match = re.search(r"(?:does|can|will)\s+(\w+)\s+(?:map to|match)\s+(\w+)", lower)
        if map_match:
            src, tgt = map_match.group(1), map_match.group(2)
            if are_synonyms(src, tgt):
                return CopilotResponse(
                    answer=f"Yes — **{src}** and **{tgt}** are the same concept (canonical: `{resolve_canonical(src)}`). I'd map them at ~92% confidence.",
                    intent="mapping_help", confidence=0.92, method="synonym_engine",
                )
        return None

    def _extract_assistant_from_doc(self, text: str) -> str | None:
        for marker in ("Assistant answer:", "Assistant:"):
            if marker in text:
                return text.split(marker, 1)[1].strip()
        return None

    def _data_follow_ups(self, insight) -> list[str]:
        name = insight.dataset_name.replace("sample_", "").replace("_", " ")
        prompts = [f"Show sample rows from {name}", f"Map {name} columns to MongoDB"]
        if insight.pii_columns:
            prompts.insert(0, f"What compliance applies to {name}?")
        else:
            prompts.insert(0, f"Check {name} for PII")
        return prompts

    def _follow_up_prompts(self, intent: str) -> list[str]:
        return self.get_suggested_prompts()[:3]

    def get_suggested_prompts(self) -> list[str]:
        from .pilot_agent import get_pilot_agent
        return get_pilot_agent().get_suggested_prompts()


_agent: DataTransferCopilotAgent | None = None


def get_copilot_agent() -> DataTransferCopilotAgent:
    global _agent
    if _agent is None:
        _agent = DataTransferCopilotAgent()
    return _agent
