"""
DataTransfer.space — Knowledge Base

Comprehensive semantic knowledge for data understanding:
- 200+ semantic patterns
- Synonym dictionary
- Type conversion rules
- Industry schemas
- Data quality rules
"""

from .semantic_patterns import SEMANTIC_PATTERNS, get_all_patterns, get_pattern_by_name
from .synonyms import (
    SYNONYM_DICTIONARY,
    CANONICAL_FORMS,
    expand_synonyms,
    resolve_canonical,
    are_synonyms,
)
from .type_conversions import TYPE_CONVERSION_MATRIX, suggest_type_conversion
from .industry_schemas import INDUSTRY_SCHEMAS, get_industry_schema
from .data_quality_rules import DATA_QUALITY_RULES, validate_column_quality

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
