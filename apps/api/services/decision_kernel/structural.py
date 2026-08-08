"""Kernel Structural Type Engine facade (Phase C4).

Strategies for Array / Object / Map / XML / Variant. Default is never silent
flatten — JSON / document wire is the safe default; child-table normalize and
hybrid require operator choice (``struct_policy``).

Implementation lives in ``services.structural_array`` + ``json_intelligence``
until the god-module split; this module is the mandated import path.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from services.json_intelligence import (
    ARRAY_POLICY_EXPLODE,
    ARRAY_POLICY_HYBRID,
    ARRAY_POLICY_NORMALIZE_CHILD,
    STRUCT_POLICY_FLATTEN_DEEP,
    STRUCT_POLICY_FLATTEN_TOP_LEVEL,
    STRUCT_POLICY_STORE_AS_JSON,
    normalize_struct_policy,
)

# Flatten is never the silent default — only with explicit operator ack.
_SILENT_FLATTEN_POLICIES = frozenset(
    {
        STRUCT_POLICY_FLATTEN_TOP_LEVEL,
        STRUCT_POLICY_FLATTEN_DEEP,
        "flatten",
        "silent_flatten",
        "auto_flatten",
        "flatten_keys",
        "top_level",
        "expand",
        "flatten_deep",
        "deep",
        "deep_flatten",
    }
)
from services.structural_array import (
    STRUCTURAL_ARRAY_EMPTY,
    STRUCTURAL_ARRAY_MIXED,
    STRUCTURAL_ARRAY_OF_OBJECT,
    STRUCTURAL_ARRAY_OF_PRIMITIVE,
    STRUCTURAL_NOT_ARRAY,
    array_strategy_gate_issues,
    build_normalized_child_batches,
    classify_array_samples,
    propose_child_table_spec,
    recommend_array_strategies,
    stamp_mapping_array_strategies,
    validate_child_table_spec,
)


class StructuralStrategy(str, Enum):
    """Operator-visible structural migration strategies (never silent flatten)."""

    STORE_AS_JSON = STRUCT_POLICY_STORE_AS_JSON
    NORMALIZE_CHILD = ARRAY_POLICY_NORMALIZE_CHILD
    HYBRID = ARRAY_POLICY_HYBRID
    EXPLODE = ARRAY_POLICY_EXPLODE
    UNSUPPORTED = "unsupported"


def default_structural_strategy() -> StructuralStrategy:
    """Fail-closed default — document wire, never invent typed flatten."""
    return StructuralStrategy.STORE_AS_JSON


def assert_no_silent_flatten(
    struct_policy: str | None,
    *,
    operator_ack_flatten: bool = False,
) -> str:
    """Normalize policy; empty/unknown/silent-flatten → JSON document wire.

    Explicit flatten policies require ``operator_ack_flatten=True`` — otherwise
    they collapse to store-as-JSON (never silent flatten).
    """
    raw = str(struct_policy or "").strip().lower()
    if raw in _SILENT_FLATTEN_POLICIES and not operator_ack_flatten:
        return str(StructuralStrategy.STORE_AS_JSON.value)
    normalized = normalize_struct_policy(struct_policy or "")
    if not normalized:
        return str(StructuralStrategy.STORE_AS_JSON.value)
    if normalized in _SILENT_FLATTEN_POLICIES and not operator_ack_flatten:
        return str(StructuralStrategy.STORE_AS_JSON.value)
    return str(normalized)


def classify_structural_column(
    samples: list[Any] | None,
    *,
    dest_db: str = "",
    parent_column: str = "",
) -> dict[str, Any]:
    """Profile samples + attach recommended strategies (JSON default first)."""
    profile = classify_array_samples(samples)
    strategies = recommend_array_strategies(
        profile, dest_db=dest_db, parent_column=parent_column
    )
    recommended = next(
        (s for s in strategies if s.get("recommended")),
        strategies[0] if strategies else None,
    )
    return {
        **profile,
        "strategies": strategies,
        "recommended_strategy": (recommended or {}).get("id")
        or default_structural_strategy().value,
        "default_never_silent_flatten": True,
    }


__all__ = [
    "ARRAY_POLICY_EXPLODE",
    "ARRAY_POLICY_HYBRID",
    "ARRAY_POLICY_NORMALIZE_CHILD",
    "STRUCTURAL_ARRAY_EMPTY",
    "STRUCTURAL_ARRAY_MIXED",
    "STRUCTURAL_ARRAY_OF_OBJECT",
    "STRUCTURAL_ARRAY_OF_PRIMITIVE",
    "STRUCTURAL_NOT_ARRAY",
    "STRUCT_POLICY_STORE_AS_JSON",
    "StructuralStrategy",
    "array_strategy_gate_issues",
    "assert_no_silent_flatten",
    "build_normalized_child_batches",
    "classify_array_samples",
    "classify_structural_column",
    "default_structural_strategy",
    "normalize_struct_policy",
    "propose_child_table_spec",
    "recommend_array_strategies",
    "stamp_mapping_array_strategies",
    "validate_child_table_spec",
]
