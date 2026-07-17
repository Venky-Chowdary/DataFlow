"""Compatibility shim: canonical implementation now lives in services.preflight_service."""
from __future__ import annotations

from services.preflight_service import (
    _PREFLIGHT_ROOT,
    VALIDATION_CONFIDENCE_THRESHOLDS,
    FilePreflightContext,
    _available_staging_bytes,
    apply_policy_gates,
    confidence_threshold_for_mode,
    inspect_destination_for_preflight,
    is_compliance_only_block,
    probe_destination,
    run_file_preflight,
    run_transfer_policy_gates,
)

__all__ = ['_PREFLIGHT_ROOT', 'FilePreflightContext', 'VALIDATION_CONFIDENCE_THRESHOLDS', 'confidence_threshold_for_mode', 'run_transfer_policy_gates', 'is_compliance_only_block', 'apply_policy_gates', 'run_file_preflight', 'probe_destination', '_available_staging_bytes', 'inspect_destination_for_preflight']
