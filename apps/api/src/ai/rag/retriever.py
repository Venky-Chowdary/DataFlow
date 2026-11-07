"""
Datawrap — RAG Retriever

Semantic search over known patterns, synonyms, and type mappings.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ..knowledge.semantic_patterns import SEMANTIC_PATTERNS
from ..knowledge.synonyms import CANONICAL_FORMS, are_synonyms, resolve_canonical
from .document_ingestion import DataTransferDocumentIngestion
from .product_docs import ProductDocHit, product_doc_search
from .vector_store import VectorDocument, get_vector_store


@dataclass
class RetrievalResult:
    """Result from RAG retrieval."""
    query: str
    documents: list[VectorDocument]
    canonical_form: str | None
    matched_pattern: str | None
    synonym_matches: list[str]
    confidence: float
    product_docs: list[ProductDocHit] = field(default_factory=list)

    @property
    def grounded(self) -> bool:
        """True when a documentation section covers the question.

        Retrieved vector documents are not evidence on their own: the fallback
        embedding scores unrelated passages 0.3, so "documents came back" once
        justified narrating an answer about anything.
        """
        return bool(self.product_docs)

    @property
    def top_grounding(self) -> float:
        return max((h.grounding for h in self.product_docs), default=0.0)


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
        product_docs = (
            [] if doc_type else product_doc_search(query, limit=min(n_results, 4))
        )
        if product_docs:
            confidence = max(
                confidence,
                min(0.5 + 0.45 * product_docs[0].grounding, 0.95),
            )

        return RetrievalResult(
            query=query,
            documents=docs,
            canonical_form=canonical,
            matched_pattern=matched_pattern,
            synonym_matches=synonym_matches,
            confidence=confidence,
            product_docs=product_docs,
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
        """Retrieve knowledge for column mapping.

        Confidence and review come from ``map_columns`` (Map SSOT). RAG
        documents explain; they must not auto-approve user_id→customer_id
        because both end in ``id``.
        """
        from services.semantic_mapper import pair_mapping_authority

        src_result = self.retrieve(source_col, n_results=3, doc_type="semantic_pattern")
        tgt_result = self.retrieve(target_col, n_results=3, doc_type="semantic_pattern")

        is_synonym = are_synonyms(source_col, target_col)
        src_canonical = resolve_canonical(source_col)
        tgt_canonical = resolve_canonical(target_col)
        authority = pair_mapping_authority(source_col, target_col)

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
            "mapping_confidence": authority["confidence"],
            "requires_review": authority["requires_review"],
            "create_new": authority["create_new"],
            "proposed_target": authority["proposed_target"],
            "authority_reasoning": authority["reasoning"],
            "authority": authority["authority"],
            "review_kind": authority.get("review_kind"),
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
        is_synonym: bool = False,
        src_canonical: str = "",
        tgt_canonical: str = "",
    ) -> float:
        """Deprecated private score — kept for call sites; delegates to Map SSOT."""
        from services.semantic_mapper import pair_mapping_authority

        return pair_mapping_authority(source, target)["confidence"]
