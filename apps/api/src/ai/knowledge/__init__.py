"""
DataTransfer.space — Knowledge Base

Comprehensive semantic knowledge for data understanding:
- 200+ semantic patterns
- Synonym dictionary
- Type conversion rules
- Industry schemas
- Data quality rules
"""

from .data_quality_rules import DATA_QUALITY_RULES, validate_column_quality
from .industry_schemas import INDUSTRY_SCHEMAS, get_industry_schema
from .semantic_patterns import SEMANTIC_PATTERNS, get_all_patterns, get_pattern_by_name
from .synonyms import (
    CANONICAL_FORMS,
    SYNONYM_DICTIONARY,
    are_synonyms,
    expand_synonyms,
    resolve_canonical,
)
from .type_conversions import TYPE_CONVERSION_MATRIX, suggest_type_conversion

__all__ = [
    "SEMANTIC_PATTERNS",
    "get_all_patterns",
    "get_pattern_by_name",
    "SYNONYM_DICTIONARY",
    "CANONICAL_FORMS",
    "expand_synonyms",
    "resolve_canonical",
    "are_synonyms",
    "TYPE_CONVERSION_MATRIX",
    "suggest_type_conversion",
    "INDUSTRY_SCHEMAS",
    "get_industry_schema",
    "DATA_QUALITY_RULES",
    "validate_column_quality",
]
