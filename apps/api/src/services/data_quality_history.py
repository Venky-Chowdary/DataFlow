"""Compatibility shim: canonical implementation now lives in services.data_quality_history."""
from __future__ import annotations

from services.data_quality_history import (
    ColumnProfile,
    detect_anomalies,
    load_historical_profile,
    profile_batch,
    profile_column,
    save_profile,
    validate_batch_against_history,
)

__all__ = [
    "ColumnProfile",
    "detect_anomalies",
    "load_historical_profile",
    "profile_batch",
    "profile_column",
    "save_profile",
    "validate_batch_against_history",
]
