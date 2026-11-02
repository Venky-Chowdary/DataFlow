"""Stable public surface for reconciliation (Phase F8).

New call sites MUST import from this module (or ``services.decision_kernel``
proof helpers), not deep into ``reconciliation.py`` internals. Implementation
remains in ``reconciliation.py`` until verify_* engines are split per dialect.
"""

from __future__ import annotations

from services.reconciliation import (
    FingerprintAccumulator,
    aggregate_checksum,
    canonical_checksum,
    canonical_checksum_from_iter,
    checksum_rows,
    fingerprint_checksum,
    iter_select_row_dicts,
    reconcile,
    stamp_post_write_phase,
    stream_select_checksum,
)
from services.verification_ladder import (
    DEFAULT_SCREENING_LIMIT,
    attach_ladder_to_reconcile_report,
    run_five_layer_verification,
)

__all__ = [
    "DEFAULT_SCREENING_LIMIT",
    "FingerprintAccumulator",
    "aggregate_checksum",
    "attach_ladder_to_reconcile_report",
    "canonical_checksum",
    "canonical_checksum_from_iter",
    "checksum_rows",
    "fingerprint_checksum",
    "iter_select_row_dicts",
    "reconcile",
    "run_five_layer_verification",
    "stamp_post_write_phase",
    "stream_select_checksum",
]
