"""Fingerprint accumulator spill-to-disk and exact checksum tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.reconciliation import (  # noqa: E402
    FingerprintAccumulator,
    _hash_fingerprints,
    fingerprint_checksum,
)


def test_small_in_memory_checksum_matches_hash():
    fingerprints = [(f"key{i}", f"fp{i}") for i in range(100)]
    acc = FingerprintAccumulator(threshold=1000)
    acc.add_many(fingerprints)
    assert acc.digest() == _hash_fingerprints(fingerprints)
    assert not acc.chunk_files


def test_spills_to_disk_when_threshold_crossed(tmp_path: Path, monkeypatch):
    monkeypatch.setattr("services.reconciliation.SPILL_THRESHOLD", 5)
    fingerprints = [(f"key{i}", f"fp{i}") for i in range(20)]
    acc = FingerprintAccumulator(threshold=5)
    acc.add_many(fingerprints)
    assert acc.chunk_files
    assert acc.digest() == _hash_fingerprints(fingerprints)


def test_fingerprint_checksum_uses_spill_for_large_input():
    fingerprints = [(f"key{i}", f"fp{i}") for i in range(2000)]
    checksum = fingerprint_checksum(fingerprints)
    expected = _hash_fingerprints(fingerprints)
    assert checksum == expected


def test_order_independence():
    """Checksums must be identical regardless of row arrival order."""
    fingerprints = [(f"key{i}", f"fp{i}") for i in range(50)]
    acc1 = FingerprintAccumulator()
    acc1.add_many(fingerprints)
    acc2 = FingerprintAccumulator()
    acc2.add_many(list(reversed(fingerprints)))
    assert acc1.digest() == acc2.digest()
