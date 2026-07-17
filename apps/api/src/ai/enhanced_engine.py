"""
DataTransfer.space — Enhanced Semantic Engine

RAG + LLM powered semantic analysis with chain-of-thought reasoning.
Upgrades the base pattern engine with embedding similarity and retrieval.
"""

from __future__ import annotations
import json
from dataclasses import dataclass, field

from .semantic_engine import (
    SemanticAnalyzer,
    SmartMapper,
    ColumnAnalysis,
    SchemaAnalysis,
    MappingSuggestion,
    DataCategory,
    ComplianceFramework,
)
from .knowledge.synonyms import resolve_canonical, get_synonym_count
from .knowledge.semantic_patterns import get_pattern_count


@dataclass
class EnhancedColumnAnalysis(ColumnAnalysis):
    """Column analysis with RAG and chain-of-thought metadata."""
    canonical_form: str = ""
    rag_confidence: float = 0.0
    reasoning_steps: list[str] = field(default_factory=list)
    method: str = "enhanced"
    rag_sources: list[dict] = field(default_factory=list)


@dataclass
class EnhancedMappingSuggestion(MappingSuggestion):
    """Mapping suggestion with enhanced reasoning."""
    canonical_source: str = ""
    canonical_target: str = ""
    reasoning: str = ""
    method: str = "enhanced"


class EnhancedSemanticAnalyzer:
    """
    RAG-enhanced semantic analyzer.
    Combines pattern matching, embedding similarity, RAG retrieval,
    and chain-of-thought reasoning for maximum accuracy.
    """

    def __init__(self):
        self.base_analyzer = SemanticAnalyzer()
        self._fallback = None
        self._rag = None
        self._corrections: dict[str, str] = {}

    @property
    def fallback(self):
        if self._fallback is None:
            from .llm.fallback import DataTransferFallbackChain
            self._fallback = DataTransferFallbackChain()
        return self._fallback

    @property
    def rag(self):
        if self._rag is None:
            from .rag.pipeline import get_rag_pipeline
            self._rag = get_rag_pipeline()
        return self._rag

    def analyze_column(
        self,
        column_name: str,
        sample_values: list[str] | None = None,
    ) -> EnhancedColumnAnalysis:
        """RAG-enhanced column analysis with chain-of-thought."""
        if sample_values is None:
            sample_values = []

        # Check user corrections first (self-learning)
        if column_name in self._corrections:
            corrected_type = self._corrections[column_name]
            base = self.base_analyzer.analyze_column(column_name, sample_values)
            return EnhancedColumnAnalysis(
                **{f.name: getattr(base, f.name) for f in base.__dataclass_fields__.values()},
                semantic_type=corrected_type,
                canonical_form=resolve_canonical(column_name),
                rag_confidence=0.99,
                reasoning_steps=["User correction applied"],
                method="user_correction",
            )

        # Chain-of-thought analysis
        cot_result = self.fallback.analyze_with_fallback(column_name, sample_values)
        answer = json.loads(cot_result.content) if cot_result.success else {}

        # Base analysis for statistics
        base = self.base_analyzer.analyze_column(column_name, sample_values)

        # RAG retrieval
        rag_response = self.rag.analyze_column(column_name, sample_values)

        # Merge results with calibrated confidence
        semantic_type = answer.get("semantic_type") or base.semantic_type
        confidence = answer.get("confidence", base.confidence)
        is_pii = answer.get("is_pii", base.is_pii)

        category = base.category
        if answer.get("category"):
            try:
                category = DataCategory(answer["category"])
            except ValueError:
                pass

        compliance = base.compliance
        if answer.get("compliance"):
            compliance = []
            for c in answer["compliance"]:
                try:
                    compliance.append(ComplianceFramework(c))
                except ValueError:
                    pass

        return EnhancedColumnAnalysis(
            column_name=column_name,
            inferred_type=answer.get("inferred_type", base.inferred_type),
            semantic_type=semantic_type,
            category=category,
            confidence=confidence,
            is_pii=is_pii,
            compliance=compliance if compliance else base.compliance,
            suggested_transformations=answer.get("transformations", base.suggested_transformations),
            null_percentage=base.null_percentage,
            unique_percentage=base.unique_percentage,
            sample_values=base.sample_values,
            statistics=base.statistics,
            warnings=base.warnings,
            canonical_form=answer.get("canonical_form", resolve_canonical(column_name)),
            rag_confidence=rag_response.confidence,
            reasoning_steps=cot_result.reasoning.split("\n") if cot_result.reasoning else [],
            method=cot_result.method,
            rag_sources=rag_response.sources[:3],
        )

    def analyze_schema(self, columns: dict[str, list[str]]) -> SchemaAnalysis:
        """RAG-enhanced schema analysis."""
        analyses = []
        pii_columns = []
        compliance_map: dict[ComplianceFramework, list[str]] = {}

        for col_name, values in columns.items():
            analysis = self.analyze_column(col_name, values)
            analyses.append(analysis)
            if analysis.is_pii:
                pii_columns.append(col_name)
                for c in analysis.compliance:
                    if c not in compliance_map:
                        compliance_map[c] = []
                    compliance_map[c].append(col_name)

        avg_confidence = sum(a.confidence for a in analyses) / len(analyses) if analyses else 0
        avg_null = sum(a.null_percentage for a in analyses) / len(analyses) if analyses else 0
        quality_score = (avg_confidence * 0.6 + (100 - avg_null) / 100 * 0.4) * 100

        recommendations = []
        if pii_columns:
            recommendations.append(f"PII detected in {len(pii_columns)} columns — apply encryption or masking")
        rag_boosted = sum(1 for a in analyses if hasattr(a, "rag_confidence") and a.rag_confidence > 0.8)
        if rag_boosted:
            recommendations.append(f"RAG validated {rag_boosted}/{len(analyses)} columns with high confidence")

        return SchemaAnalysis(
            columns=analyses,
            pii_columns=pii_columns,
            compliance_requirements=compliance_map,
            quality_score=round(quality_score, 1),
            recommendations=recommendations,
        )

    def learn_correction(self, column_name: str, correct_semantic_type: str):
        """Self-learning from user corrections."""
        self._corrections[column_name] = correct_semantic_type
        self.rag.learn_correction(column_name, correct_semantic_type)

    def natural_language_query(self, query: str) -> dict:
        """Handle natural language data queries."""
        result = self.fallback.query_with_fallback(query)
        return {
            "answer": result.content,
            "reasoning": result.reasoning,
            "confidence": result.confidence,
            "method": result.method,
        }


class EnhancedSmartMapper:
    """RAG + chain-of-thought enhanced column mapper."""

    def __init__(self, analyzer: EnhancedSemanticAnalyzer | None = None):
        self.analyzer = analyzer or EnhancedSemanticAnalyzer()
        self.base_mapper = SmartMapper(SemanticAnalyzer())

    def map_columns(
        self,
        source_columns: list[str],
        target_columns: list[str],
        source_samples: dict[str, list[str]] | None = None,
    ) -> list[EnhancedMappingSuggestion]:
        """Generate mappings using chain-of-thought + RAG."""
        result = self.analyzer.fallback.map_with_fallback(
            source_columns, target_columns, source_samples,
        )
        answer = json.loads(result.content) if result.success else {}
        mappings_data = answer.get("mappings", [])

        enhanced = []
        for m in mappings_data:
            enhanced.append(EnhancedMappingSuggestion(
                source_column=m["source_column"],
                target_column=m["target_column"],
                confidence=m["confidence"],
                reason=m["reason"],
                transformation_needed=m.get("transformation_needed", False),
                suggested_transformation=m.get("suggested_transformation"),
                canonical_source=resolve_canonical(m["source_column"]),
                canonical_target=resolve_canonical(m["target_column"]),
                reasoning=result.reasoning,
                method=result.method,
            ))

        return enhanced


# Singleton instances
_enhanced_analyzer = EnhancedSemanticAnalyzer()
_enhanced_mapper = EnhancedSmartMapper(_enhanced_analyzer)


def analyze_column_enhanced(name: str, samples: list[str] | None = None) -> EnhancedColumnAnalysis:
    return _enhanced_analyzer.analyze_column(name, samples or [])


def analyze_schema_enhanced(columns: dict[str, list[str]]) -> SchemaAnalysis:
    return _enhanced_analyzer.analyze_schema(columns)


def generate_mappings_enhanced(
    source_columns: list[str],
    target_columns: list[str],
    source_samples: dict[str, list[str]] | None = None,
) -> list[EnhancedMappingSuggestion]:
    return _enhanced_mapper.map_columns(source_columns, target_columns, source_samples)


def query_natural_language(query: str) -> dict:
    return _enhanced_analyzer.natural_language_query(query)


def get_ai_capabilities() -> dict:
    """Report AI system capabilities and status."""
    from .rag.pipeline import get_rag_pipeline
    from .llm.fallback import DataTransferFallbackChain

    rag = get_rag_pipeline()
    fallback = DataTransferFallbackChain()

    return {
        "semantic_patterns": get_pattern_count(),
        "synonym_entries": get_synonym_count(),
        "industries": 10,
        "rag": rag.get_status(),
        "llm_providers": fallback.get_status(),
        "methods": ["chain_of_thought", "rag_retrieval", "embedding_similarity", "synonym_matching", "pattern_matching"],
    }
