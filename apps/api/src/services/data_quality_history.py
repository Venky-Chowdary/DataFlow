"""Compatibility shim: canonical implementation now lives in services.data_quality_history."""
from __future__ import annotations

from services.data_quality_history import (
    ColumnProfile,
    build_load_history_report,
    compare_route_to_history,
    detect_anomalies,
    load_historical_profile,
    load_run_history,
    profile_batch,
    profile_column,
    quarantine_histogram,
    save_profile,
    validate_batch_against_history,
)

__all__ = [
    "ColumnProfile",
    "build_load_history_report",
    "compare_route_to_history",
    "detect_anomalies",
    "load_historical_profile",
    "load_run_history",
    "profile_batch",
    "profile_column",
    "quarantine_histogram",
    "save_profile",
    "validate_batch_against_history",
]
