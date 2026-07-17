"""
DataTransfer.space — AI Module

Core AI capabilities for intelligent data understanding:
- Semantic analysis (pattern + RAG + LLM)
- PII detection
- Smart mapping with synonym intelligence
- Compliance detection
- Natural language queries
"""

from .enhanced_engine import (
    EnhancedColumnAnalysis,
    EnhancedMappingSuggestion,
    EnhancedSemanticAnalyzer,
    EnhancedSmartMapper,
    analyze_column_enhanced,
    analyze_schema_enhanced,
    generate_mappings_enhanced,
    get_ai_capabilities,
    query_natural_language,
)
from .semantic_engine import (
    ColumnAnalysis,
    ComplianceFramework,
    DataCategory,
    MappingSuggestion,
    SchemaAnalysis,
    SemanticAnalyzer,
    SmartMapper,
    analyze_column,
    analyze_schema,
    detect_pii,
    generate_mappings,
)

__all__ = [
    # Base engine
    "analyze_column",
    "analyze_schema",
    "generate_mappings",
    "detect_pii",
    "SemanticAnalyzer",
    "SmartMapper",
    "ColumnAnalysis",
    "SchemaAnalysis",
    "MappingSuggestion",
    "DataCategory",
    "ComplianceFramework",
    # Enhanced engine
    "analyze_column_enhanced",
    "analyze_schema_enhanced",
    "generate_mappings_enhanced",
    "query_natural_language",
    "get_ai_capabilities",
    "EnhancedSemanticAnalyzer",
    "EnhancedSmartMapper",
    "EnhancedColumnAnalysis",
    "EnhancedMappingSuggestion",
]
