"""
DataTransfer.space — Evaluation Metrics

Mapping accuracy, PII detection recall, type inference accuracy.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from ..knowledge.synonyms import are_synonyms


@dataclass
class EvaluationResult:
    """Results from an evaluation run."""
    metric_name: str
    score: float
    total: int
    correct: int
    details: dict = field(default_factory=dict)
    duration_ms: float = 0.0


class DataTransferEvaluator:
    """Evaluate AI system accuracy across multiple metrics."""

    def __init__(self):
        self._engine = None
        self._fallback = None

    @property
    def fallback(self):
        if self._fallback is None:
            from ..llm.fallback import DataTransferFallbackChain
            self._fallback = DataTransferFallbackChain()
        return self._fallback

    def evaluate_column_classification(self, test_cases: list[dict] | None = None) -> EvaluationResult:
        """Evaluate semantic type classification accuracy."""
        start = time.time()

        if test_cases is None:
            test_cases = self._default_classification_cases()

        correct = 0
        results = []

        for case in test_cases:
            col_name = case["column_name"]
            expected = case["expected_type"]

            result = self.fallback.analyze_with_fallback(col_name, case.get("samples"))
            import json
            answer = json.loads(result.content)
            predicted = answer.get("semantic_type")

            is_correct = predicted == expected
            if is_correct:
                correct += 1
            results.append({"column": col_name, "expected": expected, "predicted": predicted, "correct": is_correct})

        duration = (time.time() - start) * 1000
        return EvaluationResult(
            metric_name="column_classification_accuracy",
            score=correct / len(test_cases) if test_cases else 0,
            total=len(test_cases),
            correct=correct,
            details={"cases": results[:20]},
            duration_ms=duration,
        )

    def evaluate_column_mapping(self, test_cases: list[dict] | None = None) -> EvaluationResult:
        """Evaluate column mapping accuracy."""
        start = time.time()

        if test_cases is None:
            test_cases = self._default_mapping_cases()

        correct = 0
        results = []

        for case in test_cases:
            result = self.fallback.map_with_fallback(
                [case["source"]], [case["target"]], case.get("samples"),
            )
            import json
            answer = json.loads(result.content)
            mappings = answer.get("mappings", [])
            if mappings and mappings[0].get("target_column") == case["target"]:
                correct += 1
                results.append({"source": case["source"], "target": case["target"], "correct": True})
            else:
                predicted = mappings[0].get("target_column") if mappings else None
                results.append({"source": case["source"], "expected": case["target"], "predicted": predicted, "correct": False})

        duration = (time.time() - start) * 1000
        return EvaluationResult(
            metric_name="column_mapping_accuracy",
            score=correct / len(test_cases) if test_cases else 0,
            total=len(test_cases),
            correct=correct,
            details={"cases": results},
            duration_ms=duration,
        )

    def evaluate_synonym_recognition(self, test_cases: list[dict] | None = None) -> EvaluationResult:
        """Evaluate synonym recognition accuracy."""
        start = time.time()

        if test_cases is None:
            test_cases = self._default_synonym_cases()

        correct = 0
        for case in test_cases:
            predicted = are_synonyms(case["name1"], case["name2"])
            if predicted == case["expected"]:
                correct += 1

        duration = (time.time() - start) * 1000
        return EvaluationResult(
            metric_name="synonym_recognition_accuracy",
            score=correct / len(test_cases) if test_cases else 0,
            total=len(test_cases),
            correct=correct,
            duration_ms=duration,
        )

    def evaluate_pii_detection(self, test_cases: list[dict] | None = None) -> EvaluationResult:
        """Evaluate PII detection recall and precision."""
        start = time.time()

        if test_cases is None:
            test_cases = self._default_pii_cases()

        tp = fp = fn = 0

        for case in test_cases:
            result = self.fallback.analyze_with_fallback(case["column_name"], case.get("samples"))
            import json
            answer = json.loads(result.content)
            predicted_pii = answer.get("is_pii", False)
            expected_pii = case["expected_pii"]

            if predicted_pii and expected_pii:
                tp += 1
            elif predicted_pii and not expected_pii:
                fp += 1
            elif not predicted_pii and expected_pii:
                fn += 1

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

        duration = (time.time() - start) * 1000
        return EvaluationResult(
            metric_name="pii_detection_f1",
            score=f1,
            total=len(test_cases),
            correct=tp,
            details={"precision": precision, "recall": recall, "f1": f1, "tp": tp, "fp": fp, "fn": fn},
            duration_ms=duration,
        )

    def run_full_evaluation(self) -> dict:
        """Run all evaluation metrics."""
        return {
            "column_classification": self.evaluate_column_classification().__dict__,
            "column_mapping": self.evaluate_column_mapping().__dict__,
            "synonym_recognition": self.evaluate_synonym_recognition().__dict__,
            "pii_detection": self.evaluate_pii_detection().__dict__,
        }

    def _default_classification_cases(self) -> list[dict]:
        return [
            {"column_name": "email", "expected_type": "Email Address"},
            {"column_name": "cust_id", "expected_type": "Customer ID"},
            {"column_name": "AMT", "expected_type": "Currency Amount"},
            {"column_name": "ssn", "expected_type": "Social Security Number"},
            {"column_name": "phone_number", "expected_type": "Phone Number"},
            {"column_name": "dob", "expected_type": "Date of Birth"},
            {"column_name": "qty", "expected_type": "Quantity"},
            {"column_name": "created_at", "expected_type": "Timestamp"},
            {"column_name": "zip_code", "expected_type": "Postal Code"},
            {"column_name": "sku", "expected_type": "Product ID"},
        ]

    def _default_mapping_cases(self) -> list[dict]:
        return [
            {"source": "cust_name", "target": "customer_name"},
            {"source": "AMT", "target": "amount"},
            {"source": "email_addr", "target": "email_address"},
            {"source": "mobile_phone", "target": "phone_number"},
            {"source": "cust_id", "target": "customer_id"},
            {"source": "qty", "target": "quantity"},
            {"source": "order_amt", "target": "total_amount"},
            {"source": "fname", "target": "first_name"},
        ]

    def _default_synonym_cases(self) -> list[dict]:
        return [
            {"name1": "AMT", "name2": "amount", "expected": True},
            {"name1": "cust", "name2": "customer", "expected": True},
            {"name1": "qty", "name2": "quantity", "expected": True},
            {"name1": "email", "name2": "phone", "expected": False},
            {"name1": "fname", "name2": "first_name", "expected": True},
            {"name1": "ssn", "name2": "social_security", "expected": True},
            {"name1": "zip", "name2": "postal_code", "expected": True},
            {"name1": "sku", "name2": "product_id", "expected": True},
        ]

    def _default_pii_cases(self) -> list[dict]:
        return [
            {"column_name": "ssn", "expected_pii": True, "samples": ["123-45-6789"]},
            {"column_name": "email", "expected_pii": True, "samples": ["test@example.com"]},
            {"column_name": "phone", "expected_pii": True, "samples": ["555-1234"]},
            {"column_name": "dob", "expected_pii": True},
            {"column_name": "order_id", "expected_pii": False},
            {"column_name": "quantity", "expected_pii": False},
            {"column_name": "status", "expected_pii": False},
            {"column_name": "credit_card", "expected_pii": True},
        ]
