"""DataFlow preflight — G1–G9 fail-fast validation before any data transfer."""

from preflight.constraint_hints import (
    assess_constraint_compatibility,
    constraint_findings_block_transfer,
    constraint_hint_messages,
    referential_integrity_posture,
)
from preflight.engine import PreflightEngine, PreflightResult
from preflight.gates import GateId, GateStatus
from preflight.models import (
    ColumnMapping,
    ColumnSchema,
    DestinationConfig,
    PreflightContext,
    SourceConfig,
    TransferPlan,
)

__all__ = [
    "PreflightEngine",
    "PreflightResult",
    "GateId",
    "GateStatus",
    "ColumnMapping",
    "ColumnSchema",
    "DestinationConfig",
    "PreflightContext",
    "SourceConfig",
    "TransferPlan",
    "assess_constraint_compatibility",
    "constraint_findings_block_transfer",
    "constraint_hint_messages",
    "referential_integrity_posture",
]
