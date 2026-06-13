"""
DataTransfer.space — Chain-of-Thought Reasoning

Multi-step reasoning for complex schema analysis and mapping.
"""

from __future__ import annotations
import json
import re
from dataclasses import dataclass, field

from .provider import DataTransferLLMProvider, LLMResponse, DataTransferLocalProvider
from .prompts import (
    CHAIN_OF_THOUGHT_TEMPLATE,
    SCHEMA_ANALYSIS_PROMPT,
    COLUMN_MAPPING_PROMPT,
    PII_DETECTION_PROMPT,
)
from ..knowledge.synonyms import resolve_canonical, are_synonyms, CANONICAL_FORMS
from ..knowledge.semantic_patterns import SEMANTIC_PATTERNS, get_pattern_by_name
from ..knowledge.type_conversions import suggest_type_conversion


@dataclass
class ReasoningStep:
    step: int
    description: str
    result: str
    confidence: float = 0.0


@dataclass
class ChainResult:
    answer: dict | str
    reasoning: list[ReasoningStep]
    confidence: float
    method: str
    provider: str = "local"


class DataTransferReasoningChain:
    """
    Chain-of-thought reasoning engine for data analysis.
    Combines LLM reasoning with RAG context and knowledge base.
    """

    def __init__(self, llm_provider: DataTransferLLMProvider | None = None):
        self.llm = llm_provider or DataTransferLocalProvider()
        self._rag = None

    @property
    def rag(self):
        if self._rag is None:
            from ..rag.pipeline import get_rag_pipeline
            self._rag = get_rag_pipeline()
        return self._rag

    def analyze_column(
        self,
        column_name: str,
        sample_values: list[str] | None = None,
    ) -> ChainResult:
        """Multi-step column analysis with chain-of-thought."""
        steps: list[ReasoningStep] = []

        # Step 1: Normalize and tokenize
        normalized = column_name.lower().replace("-", "_").replace(" ", "_")
        steps.append(ReasoningStep(1, "Normalize column name", normalized, 1.0))

        # Step 2: Synonym resolution
        canonical = resolve_canonical(column_name)
        steps.append(ReasoningStep(
            2, "Resolve synonyms",
            f"'{column_name}' → '{canonical}'",
            0.95 if canonical != normalized else 0.7,
        ))

        # Step 3: Pattern matching
        matched_pattern = None
        best_confidence = 0.0
        for pattern in SEMANTIC_PATTERNS:
            all_terms = [p.lower() for p in pattern.patterns + pattern.synonyms]
            if normalized in all_terms or canonical in all_terms:
                matched_pattern = pattern
                best_confidence = pattern.base_confidence
                break
            for term in all_terms:
                if term in normalized or normalized in term:
                    if pattern.base_confidence > best_confidence:
                        matched_pattern = pattern
                        best_confidence = pattern.base_confidence * 0.9

        steps.append(ReasoningStep(
            3, "Match semantic pattern",
            matched_pattern.name if matched_pattern else "No match",
            best_confidence,
        ))

        # Step 4: RAG retrieval boost
        rag_result = self.rag.analyze_column(column_name, sample_values)
        rag_confidence = rag_result.confidence
        steps.append(ReasoningStep(
            4, "RAG retrieval validation",
            f"Retrieved {len(rag_result.sources)} documents, confidence {rag_confidence:.2f}",
            rag_confidence,
        ))

        # Step 5: Sample data validation
        data_confidence = 0.8
        if sample_values and matched_pattern and matched_pattern.sample_patterns:
            import re as regex
            non_empty = [v for v in sample_values if v and str(v).strip()]
            if non_empty:
                match_count = sum(
                    1 for v in non_empty[:50]
                    if any(regex.match(p, str(v).strip(), regex.IGNORECASE)
                           for p in matched_pattern.sample_patterns)
                )
                data_confidence = match_count / min(len(non_empty), 50)
                data_confidence = max(data_confidence, 0.3)

        steps.append(ReasoningStep(
            5, "Validate sample data",
            f"Data pattern match rate: {data_confidence:.0%}",
            data_confidence,
        ))

        # Step 6: Calibrated confidence
        final_confidence = self._calibrate_confidence(
            best_confidence, rag_confidence, data_confidence,
            matched_pattern is not None,
        )
        steps.append(ReasoningStep(
            6, "Calibrate final confidence",
            f"Final: {final_confidence:.1%}",
            final_confidence,
        ))

        answer = {
            "column_name": column_name,
            "semantic_type": matched_pattern.name if matched_pattern else None,
            "category": matched_pattern.category.value if matched_pattern else None,
            "is_pii": matched_pattern.is_pii if matched_pattern else False,
            "compliance": matched_pattern.compliance if matched_pattern else [],
            "transformations": matched_pattern.transformations if matched_pattern else [],
            "canonical_form": canonical,
            "inferred_type": matched_pattern.data_type if matched_pattern else "string",
            "confidence": final_confidence,
        }

        return ChainResult(
            answer=answer,
            reasoning=steps,
            confidence=final_confidence,
            method="chain_of_thought",
        )

    def map_columns(
        self,
        source_columns: list[str],
        target_columns: list[str],
        source_samples: dict[str, list[str]] | None = None,
    ) -> ChainResult:
        """Multi-step column mapping with chain-of-thought."""
        steps: list[ReasoningStep] = []
        mappings = []
        used_targets = set()

        steps.append(ReasoningStep(
            1, "Initialize mapping",
            f"{len(source_columns)} source → {len(target_columns)} target columns",
            1.0,
        ))

        for i, src_col in enumerate(source_columns):
            src_analysis = self.analyze_column(
                src_col, (source_samples or {}).get(src_col, [])
            )
            best_target = None
            best_score = 0.0
            best_reason = ""

            for tgt_col in target_columns:
                if tgt_col in used_targets:
                    continue

                # Synonym check
                if are_synonyms(src_col, tgt_col):
                    score, reason = 0.95, "Synonym match"
                elif src_col.lower().replace("_", "") == tgt_col.lower().replace("_", ""):
                    score, reason = 0.98, "Normalized exact match"
                elif src_col.lower() == tgt_col.lower():
                    score, reason = 0.96, "Case-insensitive match"
                else:
                    tgt_analysis = self.analyze_column(tgt_col)
                    src_ans = src_analysis.answer if isinstance(src_analysis.answer, dict) else {}
                    tgt_ans = tgt_analysis.answer if isinstance(tgt_analysis.answer, dict) else {}

                    if src_ans.get("semantic_type") and src_ans["semantic_type"] == tgt_ans.get("semantic_type"):
                        score, reason = 0.88, f"Same semantic type: {src_ans['semantic_type']}"
                    elif resolve_canonical(src_col) == resolve_canonical(tgt_col):
                        score, reason = 0.90, "Same canonical form"
                    else:
                        rag_mapping = self.rag.suggest_mapping(src_col, tgt_col)
                        score = rag_mapping.confidence
                        reason = rag_mapping.reasoning or "RAG similarity"

                if score > best_score:
                    best_score = score
                    best_target = tgt_col
                    best_reason = reason

            if best_target and best_score > 0.5:
                src_ans = src_analysis.answer if isinstance(src_analysis.answer, dict) else {}
                tgt_analysis = self.analyze_column(best_target)
                tgt_ans = tgt_analysis.answer if isinstance(tgt_analysis.answer, dict) else {}

                transform = None
                if src_ans.get("inferred_type") != tgt_ans.get("inferred_type"):
                    conv = suggest_type_conversion(
                        src_ans.get("inferred_type", "string"),
                        tgt_ans.get("inferred_type", "string"),
                    )
                    transform = conv["method"] if conv else None

                mappings.append({
                    "source_column": src_col,
                    "target_column": best_target,
                    "confidence": round(best_score, 3),
                    "reason": best_reason,
                    "transformation_needed": transform is not None,
                    "suggested_transformation": transform,
                })
                used_targets.add(best_target)
            else:
                mappings.append({
                    "source_column": src_col,
                    "target_column": "<unmapped>",
                    "confidence": 0.0,
                    "reason": "No suitable match found",
                    "transformation_needed": False,
                    "suggested_transformation": None,
                })

        avg_confidence = (
            sum(m["confidence"] for m in mappings if m["confidence"] > 0) /
            max(sum(1 for m in mappings if m["confidence"] > 0), 1)
        )

        steps.append(ReasoningStep(
            2, "Complete mapping",
            f"Mapped {sum(1 for m in mappings if m['confidence'] > 0.5)}/{len(source_columns)} columns",
            avg_confidence,
        ))

        return ChainResult(
            answer={"mappings": mappings},
            reasoning=steps,
            confidence=avg_confidence,
            method="chain_of_thought",
        )

    def _calibrate_confidence(
        self,
        pattern_conf: float,
        rag_conf: float,
        data_conf: float,
        has_pattern: bool,
    ) -> float:
        """Calibrate confidence from multiple signals."""
        weights = []
        scores = []

        if has_pattern:
            weights.append(0.35)
            scores.append(pattern_conf)
        weights.append(0.35)
        scores.append(rag_conf)
        weights.append(0.30)
        scores.append(data_conf)

        total_weight = sum(weights)
        calibrated = sum(w * s for w, s in zip(weights, scores)) / total_weight
        return round(min(calibrated, 0.99), 3)
