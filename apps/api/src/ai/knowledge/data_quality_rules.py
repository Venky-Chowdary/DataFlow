"""
DataTransfer.space — Data Quality Rules

Validation patterns and quality checks for data columns.
"""

from __future__ import annotations

import re
from typing import Any

DATA_QUALITY_RULES: dict[str, dict] = {
    "completeness": {
        "name": "Completeness",
        "description": "Percentage of non-null values",
        "thresholds": {"good": 95, "acceptable": 80, "poor": 50},
    },
    "uniqueness": {
        "name": "Uniqueness",
        "description": "Percentage of unique values (for identifiers)",
        "thresholds": {"good": 99, "acceptable": 90, "poor": 50},
    },
    "validity": {
        "name": "Validity",
        "description": "Percentage of values matching expected format",
        "thresholds": {"good": 98, "acceptable": 90, "poor": 70},
    },
    "consistency": {
        "name": "Consistency",
        "description": "Data format consistency across records",
        "thresholds": {"good": 99, "acceptable": 95, "poor": 80},
    },
    "timeliness": {
        "name": "Timeliness",
        "description": "Data freshness relative to expected update frequency",
        "thresholds": {"good": 24, "acceptable": 72, "poor": 168},  # hours
    },
}

# Format validation patterns by semantic type
FORMAT_VALIDATORS: dict[str, list[str]] = {
    "Email Address": [r"^[\w\.-]+@[\w\.-]+\.\w{2,}$"],
    "Phone Number": [r"^\+?1?\d{10,14}$", r"^\(\d{3}\)\s?\d{3}-\d{4}$", r"^\d{3}-\d{3}-\d{4}$"],
    "Social Security Number": [r"^\d{3}-\d{2}-\d{4}$", r"^\d{9}$"],
    "Credit Card Number": [r"^\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}$", r"^\d{16}$"],
    "Postal Code": [r"^\d{5}(-\d{4})?$", r"^[A-Z]\d[A-Z]\s?\d[A-Z]\d$"],
    "Currency Amount": [r"^\$?\d+\.?\d{0,2}$", r"^-?\d+\.?\d{0,2}$"],
    "Date": [r"^\d{4}-\d{2}-\d{2}$", r"^\d{2}/\d{2}/\d{4}$"],
    "Timestamp": [r"^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}"],
    "URL": [r"^https?://"],
    "IP Address": [r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", r"^[0-9a-fA-F:]+$"],
    "Currency Code": [r"^[A-Z]{3}$"],
    "Boolean Flag": [r"^(true|false|1|0|yes|no|y|n)$"],
}


def validate_column_quality(
    column_name: str,
    values: list[Any],
    semantic_type: str | None = None,
) -> dict:
    """
    Run data quality checks on a column.
    Returns quality metrics and issues.
    """
    if not values:
        return {"score": 0, "issues": ["No data provided"], "metrics": {}}

    total = len(values)
    non_empty = [v for v in values if v is not None and str(v).strip()]
    non_empty_count = len(non_empty)

    completeness = (non_empty_count / total * 100) if total > 0 else 0
    unique_count = len(set(str(v) for v in non_empty))
    uniqueness = (unique_count / non_empty_count * 100) if non_empty_count > 0 else 0

    validity = 100.0
    validity_issues = []
    if semantic_type and semantic_type in FORMAT_VALIDATORS:
        patterns = FORMAT_VALIDATORS[semantic_type]
        valid_count = 0
        for val in non_empty[:100]:
            val_str = str(val).strip()
            if any(re.match(p, val_str, re.IGNORECASE) for p in patterns):
                valid_count += 1
        sample_size = min(len(non_empty), 100)
        validity = (valid_count / sample_size * 100) if sample_size > 0 else 100
        if validity < 90:
            validity_issues.append(f"Only {validity:.0f}% of values match expected {semantic_type} format")

    issues = []
    if completeness < 80:
        issues.append(f"Low completeness: {completeness:.1f}%")
    if validity < 90:
        issues.extend(validity_issues)

    # Weighted quality score
    score = completeness * 0.4 + validity * 0.4 + min(uniqueness, 100) * 0.2

    return {
        "score": round(score, 1),
        "metrics": {
            "completeness": round(completeness, 1),
            "uniqueness": round(uniqueness, 1),
            "validity": round(validity, 1),
            "total_records": total,
            "non_empty_records": non_empty_count,
            "unique_values": unique_count,
        },
        "issues": issues,
        "grade": "A" if score >= 90 else "B" if score >= 80 else "C" if score >= 70 else "D" if score >= 50 else "F",
    }
