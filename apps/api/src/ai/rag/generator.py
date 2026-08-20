"""
Datawrap — RAG Generator

LLM prompt templates for schema analysis, mapping, and transformation.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .evidence import retains_evidence
from .lexical_index import content_terms
from .product_docs import ProductDocHit, compose_documented_answer, nearest_articles
from .retriever import RetrievalResult

# An answer nothing in the documentation covers is reported at this confidence and
# never above it: the operator must be able to tell a documented answer from a guess.
UNGROUNDED_CONFIDENCE = 0.1


@dataclass
class RAGResponse:
    """Generated response from RAG pipeline."""
    answer: str
    reasoning: str
    sources: list[dict]
    confidence: float
    method: str  # "product_doc", "product_doc_llm", "trained_rag", "rag", "pattern", "ungrounded"
    grounded: bool = False


class DataTransferRAGGenerator:
    """Generate responses using retrieved context and optional LLM."""

    def __init__(self):
        self._llm = None
        try:
            from ..llm.fallback import DataTransferFallbackChain
            self._llm = DataTransferFallbackChain()
        except ImportError:
            pass

    def generate_schema_analysis(
        self,
        column_name: str,
        retrieval: RetrievalResult,
        sample_values: list[str] | None = None,
    ) -> RAGResponse:
        """Generate schema analysis for a column."""
        pattern = retrieval.matched_pattern or "Unknown"
        canonical = retrieval.canonical_form or column_name

        reasoning_steps = [
            f"1. Analyzed column name: '{column_name}'",
            f"2. Resolved canonical form: '{canonical}'",
            f"3. Matched semantic pattern: '{pattern}'",
            f"4. Retrieved {len(retrieval.documents)} relevant knowledge documents",
        ]

        if sample_values:
            reasoning_steps.append(
                f"5. Validated against {len(sample_values)} sample values"
            )

        answer = (
            f"Column '{column_name}' maps to semantic type '{pattern}' "
            f"(canonical: {canonical}). "
            f"Confidence: {retrieval.confidence:.1%}."
        )

        if retrieval.documents:
            top_doc = retrieval.documents[0]
            if "is_pii" in top_doc.metadata and top_doc.metadata.get("is_pii"):
                answer += " This column contains PII and requires compliance handling."

        return RAGResponse(
            answer=answer,
            reasoning="\n".join(reasoning_steps),
            sources=[{"text": d.text, "score": d.score, "metadata": d.metadata} for d in retrieval.documents],
            confidence=retrieval.confidence,
            method="rag",
        )

    def generate_mapping_suggestion(
        self,
        source_col: str,
        target_col: str,
        mapping_info: dict,
    ) -> RAGResponse:
        """Generate mapping suggestion between columns."""
        confidence = mapping_info.get("mapping_confidence", 0.5)
        requires_review = bool(mapping_info.get("requires_review"))
        authority_reason = str(mapping_info.get("authority_reasoning") or "")
        is_synonym = mapping_info.get("are_synonyms", False)

        if authority_reason:
            reason = authority_reason
            method = "map_columns_ssot"
        elif is_synonym:
            reason = f"'{source_col}' and '{target_col}' are synonyms"
            method = "synonym"
        elif mapping_info.get("same_canonical"):
            reason = "Both columns resolve to canonical form"
            method = "canonical"
        else:
            src_pattern = mapping_info.get("source", {}).get("pattern")
            tgt_pattern = mapping_info.get("target", {}).get("pattern")
            if src_pattern and src_pattern == tgt_pattern:
                reason = f"Both match semantic type '{src_pattern}'"
                method = "semantic"
            else:
                reason = "Partial match based on retrieved context"
                method = "rag"

        review_note = " Review required before auto-approve." if requires_review else ""
        answer = (
            f"Map '{source_col}' → '{target_col}'. "
            f"Reason: {reason}. Confidence: {confidence:.1%}.{review_note}"
        )

        return RAGResponse(
            answer=answer,
            reasoning=reason,
            sources=[],
            confidence=confidence,
            method=method,
        )

    def generate_natural_language_response(
        self,
        query: str,
        retrieval: RetrievalResult,
    ) -> RAGResponse:
        """Answer a product question from documentation, or state that none covers it."""
        if retrieval.product_docs:
            return self._answer_from_documentation(query, retrieval)

        for doc in retrieval.documents:
            if doc.metadata.get("type") == "copilot_training":
                answer = self._extract_copilot_answer(doc.text)
                if answer:
                    return RAGResponse(
                        answer=answer,
                        reasoning="Matched trained copilot conversation",
                        sources=[
                            {"text": d.text[:150], "score": d.score, "type": "copilot_training"}
                            for d in retrieval.documents[:3]
                        ],
                        confidence=min(retrieval.confidence + 0.15, 0.92),
                        method="trained_rag",
                        grounded=True,
                    )

        return self._answer_without_evidence(query)

    def _answer_from_documentation(
        self,
        query: str,
        retrieval: RetrievalResult,
    ) -> RAGResponse:
        hits = retrieval.product_docs
        documented = compose_documented_answer(hits)
        sources = [hit.as_source() for hit in hits]
        confidence = min(0.5 + 0.45 * retrieval.top_grounding, 0.95)
        cited = ", ".join(hit.chunk.citation for hit in hits[:2])

        narrated = self._narrate(query, hits)
        if narrated:
            return RAGResponse(
                answer=narrated,
                reasoning=f"Answered from {cited}; narration constrained to those sections",
                sources=sources,
                confidence=confidence,
                method="product_doc_llm",
                grounded=True,
            )

        return RAGResponse(
            answer=documented,
            reasoning=f"Answered from {cited}",
            sources=sources,
            confidence=confidence,
            method="product_doc",
            grounded=True,
        )

    def _narrate(self, query: str, hits: list[ProductDocHit]) -> str | None:
        """Optional LLM rewrite of the cited sections, rejected unless it kept the evidence.

        A provider that answers from its own prior instead of the passages is the
        failure mode that made a configured key look like working RAG, so the
        rewrite must retain the question's grounded terms or it is discarded and the
        documented text is served instead.
        """
        if not self._llm:
            return None
        context = self._format_doc_context(hits)
        try:
            from ..llm.prompts import NATURAL_LANGUAGE_PROMPT

            prompt = NATURAL_LANGUAGE_PROMPT.format(query=query, context=context)
            llm_response = self._llm.generate_prose(
                prompt,
                system=(
                    "You are the Datawrap product assistant. Answer only from the "
                    "documentation passages provided. Never add features, limits or "
                    "numbers that are not in them; if they do not answer the question, "
                    "say so plainly."
                ),
            )
        except Exception as exc:
            logging.getLogger(__name__).warning(
                "LLM narration failed, serving documented text: %s", exc, exc_info=exc,
            )
            return None
        if not llm_response.success or not (llm_response.content or "").strip():
            return None
        answer = llm_response.content.strip()
        if answer.startswith(("{", "[")):
            # A structured document is not an answer to an operator's question.
            return None
        return answer if self._retains_evidence(answer, hits) else None

    @staticmethod
    def _retains_evidence(answer: str, hits: list[ProductDocHit]) -> bool:
        """Whether the narration still talks about the retrieved passages."""
        return retains_evidence(
            answer,
            matched_terms={term for hit in hits for term in hit.matched_terms},
            context_terms={t for hit in hits for t in content_terms(hit.chunk.text)},
        )

    def _answer_without_evidence(self, query: str) -> RAGResponse:
        leads = nearest_articles(query)
        lines = [
            "The Datawrap documentation I can search does not cover that, so I will not "
            "answer it from guesswork.",
        ]
        if leads:
            lines.append("Closest guides: " + ", ".join(leads) + ".")
        lines.append(
            "I can answer questions about connectors, mapping, preflight gates, sync "
            "modes, quarantine, reconcile proof, schedules, MCP and the API — or ask me "
            "to read a live table.",
        )
        return RAGResponse(
            answer="\n".join(lines),
            reasoning="No documentation section covered enough of the question to answer it",
            sources=[],
            confidence=UNGROUNDED_CONFIDENCE,
            method="ungrounded",
            grounded=False,
        )

    def suggest_transformations(
        self,
        source_type: str,
        target_type: str,
        semantic_type: str | None = None,
    ) -> RAGResponse:
        """Suggest data transformations."""
        from ..knowledge.type_conversions import (
            get_compatible_types,
            suggest_type_conversion,
        )

        conversion = suggest_type_conversion(source_type, target_type)
        compatible = get_compatible_types(source_type)

        if conversion:
            answer = (
                f"Transform {source_type} → {target_type} using method '{conversion['method']}'. "
                f"Lossy: {conversion.get('lossy', False)}."
            )
            if conversion.get("note"):
                answer += f" Note: {conversion['note']}"
            confidence = 0.90
        else:
            answer = (
                f"No direct conversion from {source_type} to {target_type}. "
                f"Compatible types from {source_type}: {', '.join(compatible) or 'none'}."
            )
            confidence = 0.50

        transforms = []
        if semantic_type:
            from ..knowledge.semantic_patterns import get_pattern_by_name
            pattern = get_pattern_by_name(semantic_type)
            if pattern and pattern.transformations:
                transforms = pattern.transformations
                answer += f" Recommended transforms for {semantic_type}: {', '.join(transforms)}."

        return RAGResponse(
            answer=answer,
            reasoning=f"Checked type conversion matrix for {source_type} → {target_type}",
            sources=[],
            confidence=confidence,
            method="pattern",
        )

    @staticmethod
    def _format_doc_context(hits: list[ProductDocHit]) -> str:
        return "\n\n".join(
            f"[{i + 1}] {hit.chunk.citation}\n{hit.chunk.text}"
            for i, hit in enumerate(hits)
        )

    def _format_context(self, retrieval: RetrievalResult) -> str:
        if not retrieval.documents:
            return "No relevant context found."
        parts = []
        for doc in retrieval.documents[:5]:
            parts.append(f"- {doc.text} (relevance: {doc.score:.2f})")
        return "\n".join(parts)

    def _extract_copilot_answer(self, text: str) -> str | None:
        for marker in ("Assistant answer:", "Assistant:"):
            if marker in text:
                return text.split(marker, 1)[1].strip()
        return None
