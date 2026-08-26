"""
Datawrap — Chain-of-Thought Reasoning

Multi-step reasoning for complex schema analysis and mapping.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..knowledge.semantic_patterns import SEMANTIC_PATTERNS
from ..knowledge.synonyms import resolve_canonical
from .provider import DataTransferLLMProvider, DataTransferLocalProvider


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
            from services.transform_engine import apply_transform

            non_empty = [v for v in sample_values if v and str(v).strip()]
            if non_empty:
                use_write_bind = "standardize_iso8601" in (matched_pattern.transformations or [])
                match_count = 0
                for raw in non_empty[:50]:
                    text = str(raw).strip()
                    if use_write_bind:
                        parsed, err = apply_transform(text, "datetime")
                        if parsed is not None and not err:
                            match_count += 1
                    elif any(
                        regex.match(p, text, regex.IGNORECASE)
                        for p in matched_pattern.sample_patterns
                    ):
                        match_count += 1
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

        inferred = matched_pattern.data_type if matched_pattern else "string"
        transforms = list(matched_pattern.transformations) if matched_pattern else []
        if sample_values:
            from services.transform_engine import samples_are_auto_ambiguous_dates

            texts = [str(v) for v in sample_values if v is not None and str(v).strip()]
            if samples_are_auto_ambiguous_dates(texts):
                if inferred in {"date", "datetime"}:
                    inferred = "string"
                transforms = [t for t in transforms if t != "standardize_iso8601"]

        answer = {
            "column_name": column_name,
            "semantic_type": matched_pattern.name if matched_pattern else None,
            "category": matched_pattern.category.value if matched_pattern else None,
            "is_pii": matched_pattern.is_pii if matched_pattern else False,
            "compliance": matched_pattern.compliance if matched_pattern else [],
            "transformations": transforms,
            "canonical_form": canonical,
            "inferred_type": inferred,
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

        from services.semantic_mapper import authority_mappings

        source_schemas = None
        if source_samples:
            source_schemas = [
                {
                    "name": col,
                    "inferred_type": "VARCHAR",
                    "samples": list(source_samples.get(col) or [])[:8],
                }
                for col in source_columns
            ]
        assigned = authority_mappings(
            source_columns,
            target_columns,
            source_schemas=source_schemas,
        )
        by_source = {str(m.get("source")): m for m in assigned}

        for src_col in source_columns:
            row = by_source.get(src_col) or {}
            tgt = str(row.get("target") or "")
            conf = float(row.get("confidence") or 0)
            if tgt and not row.get("create_new") and conf > 0.5:
                from services.transform_engine import infer_transform_for_mapping

                samples = list((source_samples or {}).get(src_col) or [])
                inferred = infer_transform_for_mapping(
                    src_col, tgt, "string", None, samples,
                )
                transform = inferred if inferred and inferred != "none" else None
                mappings.append({
                    "source_column": src_col,
                    "target_column": tgt,
                    "confidence": round(conf, 3),
                    "reason": str(row.get("reasoning") or "map_columns SSOT"),
                    "requires_review": bool(row.get("requires_review")),
                    "transformation_needed": transform is not None,
                    "suggested_transformation": transform,
                })
                used_targets.add(tgt)
            else:
                mappings.append({
                    "source_column": src_col,
                    "target_column": "<unmapped>" if not tgt or row.get("create_new") else tgt,
                    "confidence": round(conf, 3) if tgt else 0.0,
                    "reason": str(row.get("reasoning") or "No suitable match found"),
                    "requires_review": True,
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
