"""Compatibility shim: canonical implementation now lives in services.replay_safety."""
from __future__ import annotations

from services.replay_safety import (
    AMBIGUOUS_OUTCOME_SIGNALS,
    ReplaySafety,
    classify_replay_safety,
    destination_has_chunk_ledger,
    error_outcome_is_ambiguous,
)

__all__ = [
    "AMBIGUOUS_OUTCOME_SIGNALS",
    "ReplaySafety",
    "classify_replay_safety",
    "destination_has_chunk_ledger",
    "error_outcome_is_ambiguous",
]
