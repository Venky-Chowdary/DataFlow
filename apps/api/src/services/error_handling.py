"""Compatibility shim: canonical implementation now lives in services.error_handling."""
from __future__ import annotations

from services.error_handling import (
    QuarantineRecord,
    RetryBudget,
    TransferCancelled,
    build_error_report,
    classify_error,
    quarantine_record,
    should_quarantine,
    with_retry,
)

__all__ = ['TransferCancelled', 'RetryBudget', 'QuarantineRecord', 'classify_error', 'with_retry', 'quarantine_record', 'should_quarantine', 'build_error_report']
