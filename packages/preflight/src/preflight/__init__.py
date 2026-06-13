"""DataFlow preflight — 8-gate fail-fast validation before any data transfer."""

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
]
