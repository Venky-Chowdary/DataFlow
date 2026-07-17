"""
DataTransfer.space — Training Data Synthesis

Generate training data from known patterns and industry schemas.
"""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass, field

from ..knowledge.semantic_patterns import SEMANTIC_PATTERNS
from ..knowledge.synonyms import SYNONYM_DICTIONARY


@dataclass
class TrainingExample:
    """A single training example."""
    id: str
    input: dict
    output: dict
    category: str
    difficulty: str = "medium"


@dataclass
class TrainingDataset:
    """A collection of training examples."""
    name: str
    examples: list[TrainingExample] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)

    def to_jsonl(self) -> str:
        lines = []
        for ex in self.examples:
            lines.append(json.dumps({
                "id": ex.id,
                "input": ex.input,
                "output": ex.output,
                "category": ex.category,
            }))
        return "\n".join(lines)

    def save(self, path: str):
        with open(path, "w") as f:
            f.write(self.to_jsonl())


class DataTransferDataSynthesizer:
    """Generate training data from knowledge base patterns."""

    def __init__(self, seed: int = 42):
        self.rng = random.Random(seed)

    def synthesize_column_classification(self, count: int = 500) -> TrainingDataset:
        """Generate column name → semantic type classification examples."""
        examples = []
        for _ in range(count):
            pattern = self.rng.choice(SEMANTIC_PATTERNS)
            all_names = pattern.patterns + pattern.synonyms
            col_name = self.rng.choice(all_names)

            examples.append(TrainingExample(
                id=str(uuid.uuid4()),
                input={"column_name": col_name},
                output={
                    "semantic_type": pattern.name,
                    "category": pattern.category.value,
                    "is_pii": pattern.is_pii,
                    "data_type": pattern.data_type,
                },
                category="column_classification",
            ))

        return TrainingDataset(
            name="column_classification",
            examples=examples,
            metadata={"count": len(examples), "source": "semantic_patterns"},
        )

    def synthesize_column_mapping(self, count: int = 500) -> TrainingDataset:
        """Generate source → target column mapping examples."""
        examples = []
        for _ in range(count):
            pattern = self.rng.choice(SEMANTIC_PATTERNS)
            source_names = pattern.patterns[:3]
            target_names = pattern.synonyms[:3] or pattern.patterns[3:6]

            if not source_names or not target_names:
                continue

            src = self.rng.choice(source_names)
            tgt = self.rng.choice(target_names)

            examples.append(TrainingExample(
                id=str(uuid.uuid4()),
                input={"source_column": src, "target_columns": target_names + pattern.patterns},
                output={
                    "target_column": tgt,
                    "semantic_type": pattern.name,
                    "confidence": pattern.base_confidence,
                    "reason": "synonym_match",
                },
                category="column_mapping",
            ))

        return TrainingDataset(
            name="column_mapping",
            examples=examples,
            metadata={"count": len(examples), "source": "synonym_dictionary"},
        )

    def synthesize_synonym_recognition(self, count: int = 1000) -> TrainingDataset:
        """Generate synonym pair recognition examples."""
        examples = []
        canonicals = list(SYNONYM_DICTIONARY.keys())

        for _ in range(count):
            canonical = self.rng.choice(canonicals)
            synonyms = SYNONYM_DICTIONARY[canonical]
            if len(synonyms) < 2:
                continue

            pair = self.rng.sample(synonyms, 2)
            is_synonym = self.rng.random() > 0.3

            if not is_synonym:
                other_canonical = self.rng.choice([c for c in canonicals if c != canonical])
                pair[1] = self.rng.choice(SYNONYM_DICTIONARY[other_canonical])

            examples.append(TrainingExample(
                id=str(uuid.uuid4()),
                input={"name1": pair[0], "name2": pair[1]},
                output={
                    "are_synonyms": is_synonym,
                    "canonical": canonical if is_synonym else None,
                },
                category="synonym_recognition",
            ))

        return TrainingDataset(
            name="synonym_recognition",
            examples=examples,
            metadata={"count": len(examples), "source": "synonym_dictionary"},
        )

    def synthesize_pii_detection(self, count: int = 300) -> TrainingDataset:
        """Generate PII detection examples."""
        examples = []
        pii_patterns = [p for p in SEMANTIC_PATTERNS if p.is_pii]
        non_pii = [p for p in SEMANTIC_PATTERNS if not p.is_pii]

        for _ in range(count // 2):
            pattern = self.rng.choice(pii_patterns)
            col_name = self.rng.choice(pattern.patterns + pattern.synonyms)
            examples.append(TrainingExample(
                id=str(uuid.uuid4()),
                input={"column_name": col_name},
                output={
                    "is_pii": True,
                    "semantic_type": pattern.name,
                    "compliance": pattern.compliance,
                    "risk_level": "high" if pattern.base_confidence > 0.9 else "medium",
                },
                category="pii_detection",
            ))

        for _ in range(count // 2):
            pattern = self.rng.choice(non_pii)
            col_name = self.rng.choice(pattern.patterns)
            examples.append(TrainingExample(
                id=str(uuid.uuid4()),
                input={"column_name": col_name},
                output={"is_pii": False, "semantic_type": pattern.name},
                category="pii_detection",
            ))

        return TrainingDataset(
            name="pii_detection",
            examples=examples,
            metadata={"count": len(examples)},
        )

    def synthesize_full_dataset(self) -> dict[str, TrainingDataset]:
        """Generate all training datasets."""
        return {
            "column_classification": self.synthesize_column_classification(500),
            "column_mapping": self.synthesize_column_mapping(500),
            "synonym_recognition": self.synthesize_synonym_recognition(1000),
            "pii_detection": self.synthesize_pii_detection(300),
        }
