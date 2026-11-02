"""Kernel Conversion Classification Engine facade (Phase C2/C3).

SSOT remains ``services.conversion_contract``; kernel is the mandated import
path for Map/Validate/Execute so ConversionClass cannot fork.
"""

from __future__ import annotations

from services.conversion_contract import (
    CONVERSION_CONTRACT_VERSION,
    ConversionClass,
    classify_conversion,
    classify_mapping,
    create_new_mapping_reason,
)

# Stable alias — call sites should prefer kernel.classify_mapping.
classify_mapping_conversion = classify_mapping

__all__ = [
    "CONVERSION_CONTRACT_VERSION",
    "ConversionClass",
    "classify_conversion",
    "classify_mapping",
    "classify_mapping_conversion",
    "create_new_mapping_reason",
]
