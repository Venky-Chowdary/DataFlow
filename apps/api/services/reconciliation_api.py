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
    reconcile,
    stamp_post_write_phase,
)

__all__ = [
    "FingerprintAccumulator",
    "aggregate_checksum",
    "canonical_checksum",
    "canonical_checksum_from_iter",
    "checksum_rows",
    "fingerprint_checksum",
    "reconcile",
    "stamp_post_write_phase",
]
