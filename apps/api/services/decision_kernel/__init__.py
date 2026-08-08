"""Migration Decision Kernel — single source of truth for migration decisions.

Phase A surface: width-preserving canonical carriers.
Phase C1–C11 surface: immutable Decision Artifact + Type/Conversion/Structural/
Invent/Profile/Validation/Risk/Proof engines. Map / Validate / Execute / Proof /
UI consume these helpers — never re-derive business decisions independently.
"""

from __future__ import annotations

from services.decision_kernel.conversion import (
    CONVERSION_CONTRACT_VERSION,
    ConversionClass,
    classify_conversion,
    classify_mapping,
    classify_mapping_conversion,
)
from services.decision_kernel.ddl import (
    DdlIdentityError,
    approved_mapping_ddl_fingerprint,
    assert_ddl_identity,
    ddl_identity_report,
)
from services.decision_kernel.execute_gate import (
    build_artifact_from_mappings,
    enforce_decision_artifact,
)
from services.decision_kernel.invent import (
    InventContext,
    InventRefused,
    invent_context_from_sync_mode,
    invent_dest_type,
)
from services.decision_kernel.models import (
    DECISION_ARTIFACT_SCHEMA,
    AssignmentStrategy,
    CanonicalType,
    ColumnSpec,
    ConversionDecision,
    DecisionArtifact,
    DdlPlan,
    MappingDecision,
    ProofPlan,
    RiskLevel,
    build_decision_artifact,
    compute_content_hash,
    decision_artifact_from_dict,
)
from services.decision_kernel.profile import (
    ColumnProfile,
    SchemaProfile,
    profile_column,
    profile_columns,
)
from services.decision_kernel.proof import (
    PROOF_ENGINE_VERSION,
    attach_artifact_to_signed_pack,
    build_migration_proof_pack,
    build_proof_plan,
    extract_population_checksum,
)
from services.decision_kernel.risk import (
    aggregate_route_risk,
    assess_mapping_risk,
    risk_level_for_conversion,
)
from services.decision_kernel.structural import (
    StructuralStrategy,
    assert_no_silent_flatten,
    classify_structural_column,
    default_structural_strategy,
    stamp_mapping_array_strategies,
)
from services.decision_kernel.types import (
    create_new_mapping_target_type,
    ddl_invent_never_narrower_than_table,
    ddl_type,
    float_width_carrier,
    integer_width_carrier,
    is_lossy_coercion,
    is_precision_collapse_coercion,
    materialize_dest_ddl,
    normalize_logical_type,
)
from services.decision_kernel.validation import (
    ValidationClass,
    classify_gate_results,
    orchestrate_validation_summary,
    validation_class_for_gate,
)

__all__ = [
    "CONVERSION_CONTRACT_VERSION",
    "DECISION_ARTIFACT_SCHEMA",
    "PROOF_ENGINE_VERSION",
    "AssignmentStrategy",
    "CanonicalType",
    "ColumnProfile",
    "ColumnSpec",
    "ConversionClass",
    "ConversionDecision",
    "DecisionArtifact",
    "DdlIdentityError",
    "DdlPlan",
    "InventContext",
    "InventRefused",
    "MappingDecision",
    "ProofPlan",
    "RiskLevel",
    "SchemaProfile",
    "StructuralStrategy",
    "ValidationClass",
    "aggregate_route_risk",
    "approved_mapping_ddl_fingerprint",
    "assert_ddl_identity",
    "assert_no_silent_flatten",
    "assess_mapping_risk",
    "attach_artifact_to_signed_pack",
    "build_artifact_from_mappings",
    "build_decision_artifact",
    "build_migration_proof_pack",
    "build_proof_plan",
    "classify_conversion",
    "classify_gate_results",
    "classify_mapping",
    "classify_mapping_conversion",
    "classify_structural_column",
    "compute_content_hash",
    "create_new_mapping_target_type",
    "ddl_identity_report",
    "ddl_invent_never_narrower_than_table",
    "ddl_type",
    "decision_artifact_from_dict",
    "default_structural_strategy",
    "enforce_decision_artifact",
    "extract_population_checksum",
    "float_width_carrier",
    "integer_width_carrier",
    "invent_context_from_sync_mode",
    "invent_dest_type",
    "is_lossy_coercion",
    "is_precision_collapse_coercion",
    "materialize_dest_ddl",
    "normalize_logical_type",
    "orchestrate_validation_summary",
    "profile_column",
    "profile_columns",
    "risk_level_for_conversion",
    "stamp_mapping_array_strategies",
    "validation_class_for_gate",
]
