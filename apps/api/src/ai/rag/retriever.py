"""
DataTransfer.space — RAG Retriever

Semantic search over known patterns, synonyms, and type mappings.
"""

from __future__ import annotations
from dataclasses import dataclass

from ..knowledge.synonyms import resolve_canonical, are_synonyms, CANONICAL_FORMS
from ..knowledge.semantic_patterns import SEMANTIC_PATTERNS
from .vector_store import VectorDocument, get_vector_store
from .document_ingestion import DataTransferDocumentIngestion


@dataclass
class RetrievalResult:
    """Result from RAG retrieval."""
    query: str
    documents: list[VectorDocument]
    canonical_form: str | None
    matched_pattern: str | None
    synonym_matches: list[str]
    confidence: float


class DataTransferRetriever:
    """Retrieve relevant knowledge for a query or column name."""

    def __init__(self):
        self.vector_store = get_vector_store()
        self.ingestion = DataTransferDocumentIngestion()

    def retrieve(
        self,
        query: str,
        n_results: int = 5,
        doc_type: str | None = None,
    ) -> RetrievalResult:
        """Retrieve relevant documents for a query."""
        self.ingestion.ensure_knowledge_loaded()

        filter_meta = {"type": doc_type} if doc_type else None
        docs = self.vector_store.search(query, n_results=n_results, filter_metadata=filter_meta)

        canonical = resolve_canonical(query)
        synonym_matches = []
        if canonical in CANONICAL_FORMS.values() or query.lower() in CANONICAL_FORMS:
            resolved = CANONICAL_FORMS.get(query.lower(), canonical)
            synonym_matches = [resolved]

        matched_pattern = self._match_pattern(query)
        confidence = self._calculate_confidence(query, docs, matched_pattern)

        return RetrievalResult(
            query=query,
            documents=docs,
            canonical_form=canonical,
            matched_pattern=matched_pattern,
            synonym_matches=synonym_matches,
            confidence=confidence,
        )

    def retrieve_for_column(
        self,
        column_name: str,
        sample_values: list[str] | None = None,
        n_results: int = 3,
    ) -> RetrievalResult:
        """Retrieve knowledge relevant to a column name and samples."""
        query_parts = [f"column {column_name}"]
        if sample_values:
            query_parts.append(f"values: {', '.join(str(v) for v in sample_values[:3])}")
        query = " ".join(query_parts)
        return self.retrieve(query, n_results=n_results)

    def retrieve_for_mapping(
        self,
        source_col: str,
        target_col: str,
    ) -> dict:
        """Retrieve knowledge for column mapping."""
        src_result = self.retrieve(source_col, n_results=3, doc_type="semantic_pattern")
        tgt_result = self.retrieve(target_col, n_results=3, doc_type="semantic_pattern")

        is_synonym = are_synonyms(source_col, target_col)
        src_canonical = resolve_canonical(source_col)
        tgt_canonical = resolve_canonical(target_col)

        return {
            "source": {
                "column": source_col,
                "canonical": src_canonical,
                "pattern": src_result.matched_pattern,
                "confidence": src_result.confidence,
            },
            "target": {
                "column": target_col,
                "canonical": tgt_canonical,
                "pattern": tgt_result.matched_pattern,
                "confidence": tgt_result.confidence,
            },
            "are_synonyms": is_synonym,
            "same_canonical": src_canonical == tgt_canonical,
            "mapping_confidence": self._mapping_confidence(
                source_col, target_col, is_synonym, src_canonical, tgt_canonical
            ),
        }

    def _match_pattern(self, query: str) -> str | None:
        normalized = query.lower().replace("-", "_").replace(" ", "_")
        for pattern in SEMANTIC_PATTERNS:
            for p in pattern.patterns + pattern.synonyms:
                if p.lower() == normalized or p.lower() in normalized:
                    return pattern.name
        canonical = resolve_canonical(query)
        for pattern in SEMANTIC_PATTERNS:
            if canonical in [s.lower() for s in pattern.synonyms + pattern.patterns]:
                return pattern.name
        return None

    def _calculate_confidence(
        self,
        query: str,
        docs: list[VectorDocument],
        matched_pattern: str | None,
    ) -> float:
        scores = [d.score for d in docs if d.score > 0]
        vector_score = max(scores) if scores else 0.0
        pattern_boost = 0.3 if matched_pattern else 0.0
        synonym_boost = 0.2 if resolve_canonical(query) != query.lower() else 0.0
        return min(vector_score + pattern_boost + synonym_boost, 0.99)

    def _mapping_confidence(
        self,
        source: str,
        target: str,
        is_synonym: bool,
        src_canonical: str,
        tgt_canonical: str,
    ) -> float:
        if source.lower() == target.lower():
            return 0.98
        if is_synonym or src_canonical == tgt_canonical:
            return 0.92
        if src_canonical.split("_")[-1] == tgt_canonical.split("_")[-1]:
            return 0.80
        return 0.5
