"""
DataTransfer.space — RAG Generator

LLM prompt templates for schema analysis, mapping, and transformation.
"""

from __future__ import annotations
from dataclasses import dataclass

from .retriever import RetrievalResult


@dataclass
class RAGResponse:
    """Generated response from RAG pipeline."""
    answer: str
    reasoning: str
    sources: list[dict]
    confidence: float
    method: str  # "llm", "rag", "pattern"


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
        is_synonym = mapping_info.get("are_synonyms", False)

        if is_synonym:
            reason = f"'{source_col}' and '{target_col}' are synonyms"
            method = "synonym"
        elif mapping_info.get("same_canonical"):
            reason = f"Both columns resolve to canonical form"
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

        answer = (
            f"Map '{source_col}' → '{target_col}'. "
            f"Reason: {reason}. Confidence: {confidence:.1%}."
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
        """Answer a natural language data query."""
        context = self._format_context(retrieval)

        if self._llm:
            try:
                from ..llm.prompts import NATURAL_LANGUAGE_PROMPT
                prompt = NATURAL_LANGUAGE_PROMPT.format(
                    query=query,
                    context=context,
                )
                llm_response = self._llm.generate(prompt, system="You are the DataTransfer.space AI assistant.")
                if llm_response.success:
                    return RAGResponse(
                        answer=llm_response.content,
                        reasoning=llm_response.reasoning or "LLM-generated response",
                        sources=[{"text": d.text, "score": d.score} for d in retrieval.documents[:3]],
                        confidence=min(retrieval.confidence + 0.1, 0.99),
                        method="llm",
                    )
            except Exception:
                pass

        # RAG-only fallback — prefer copilot training docs
        if retrieval.documents:
            for doc in retrieval.documents:
                if doc.metadata.get("type") == "copilot_training":
                    answer = self._extract_copilot_answer(doc.text)
                    if answer:
                        return RAGResponse(
                            answer=answer,
                            reasoning="Matched trained copilot conversation",
                            sources=[{"text": d.text[:150], "score": d.score} for d in retrieval.documents[:3]],
                            confidence=min(retrieval.confidence + 0.15, 0.92),
                            method="trained_rag",
                        )
            top_docs = retrieval.documents[:3]
            summary = "\n\n".join(d.text[:200] for d in top_docs)
            answer = summary
        else:
            answer = (
                "I can help you move data anywhere. Try asking:\n"
                "• \"Move my CSV to MongoDB\"\n"
                "• \"How does AI column mapping work?\"\n"
                "• \"Check my data for PII\""
            )

        return RAGResponse(
            answer=answer,
            reasoning=f"Retrieved {len(retrieval.documents)} documents for query",
            sources=[{"text": d.text, "score": d.score} for d in retrieval.documents],
            confidence=retrieval.confidence,
            method="rag",
        )

    def suggest_transformations(
        self,
        source_type: str,
        target_type: str,
        semantic_type: str | None = None,
    ) -> RAGResponse:
        """Suggest data transformations."""
        from ..knowledge.type_conversions import suggest_type_conversion, get_compatible_types

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
