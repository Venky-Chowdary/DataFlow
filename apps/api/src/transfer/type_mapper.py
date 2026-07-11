"""Map inferred column types to destination-native DDL types."""

from __future__ import annotations

try:
    from ai.knowledge.type_conversions import suggest_type_conversion
except ImportError:  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.ai.knowledge.type_conversions import suggest_type_conversion

try:
    from services.type_system import (
        build_column_types,
        ddl_type,
        default_mappings,
        normalize_logical_type,
    )
except ImportError:  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.services.type_system import (
        build_column_types,
        ddl_type,
        default_mappings,
        normalize_logical_type,
    )


def normalize_inferred(inferred: str) -> str:
    return normalize_logical_type(inferred)
