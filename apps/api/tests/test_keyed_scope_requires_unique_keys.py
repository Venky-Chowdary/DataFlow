"""A key-scoped digest is only comparable when the key identifies one row.

Appending the same batch twice into a destination without a unique constraint
leaves two rows per key. The written-key read-back then returns 2x the batch, so
its digest can never equal the source's, and a correct append failed itself with
nothing but two hashes — the same shape as the customer's 710k run. A merge owns
its conflict target and a declared PK/unique constraint rejects the duplicate; an
identity inferred from Map guarantees neither, so an append keeps the destination
delta as its identity.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.reconcile_coverage import (  # noqa: E402
    WHOLE_TABLE_NOT_COMPARABLE,
    WRITTEN_BATCH_KEYS,
    append_row_count_report,
)
from services.destination_key_collision_probe import (  # noqa: E402
    sync_mode_appends_without_key_resolution,
)
from services.reconciliation import reconcile  # noqa: E402


def _keyed_readback_scope(*, sync_mode: str, key_enforced: bool) -> str:
    """The predicate under test, as the reconcile step applies it."""
    identifies_one_row = key_enforced or not sync_mode_appends_without_key_resolution(
        sync_mode
    )
    return WRITTEN_BATCH_KEYS if identifies_one_row else ""


def test_an_append_on_an_unenforced_key_cannot_scope_the_digest() -> None:
    assert _keyed_readback_scope(sync_mode="incremental_append", key_enforced=False) == ""
    # The same append into a table that actually enforces the key is comparable:
    # the destination rejects a second copy, so the key names one row.
    assert (
        _keyed_readback_scope(sync_mode="incremental_append", key_enforced=True)
        == WRITTEN_BATCH_KEYS
    )


def test_merge_writes_keep_the_keyed_scope_without_a_constraint() -> None:
    """Upsert resolves the key itself, so its conflict target is the scope."""
    for mode in ("incremental_deduped", "upsert", "incremental_dedup_history"):
        assert (
            _keyed_readback_scope(sync_mode=mode, key_enforced=False)
            == WRITTEN_BATCH_KEYS
        )


def test_append_delta_still_proves_the_run_the_keyed_scope_cannot() -> None:
    """3 appended into a table that held 3 identical-key rows: delta is the proof."""
    report = append_row_count_report(
        source_rows=3,
        target_rows=6,
        expected_rows=3,
        source_checksum="src",
        target_checksum="dst",
        sample_note="",
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        sample_compare=None,
        target_rows_before=3,
    ).to_dict()
    assert report["passed"] is True
    assert report["assurance_level"] == "row_count"
    assert report["checksum_scope"] == WHOLE_TABLE_NOT_COMPARABLE
    # Cardinality proof must never be dressed up as per-cell proof.
    assert report["population_proof"] is False


def test_a_wrong_delta_still_fails_with_the_keyed_scope_dropped() -> None:
    report = reconcile(
        source_rows=3,
        target_rows=6,
        source_checksum="src",
        target_checksum="dst",
        strict_checksum=True,
        allow_extra_rows=True,
        target_rows_before=4,
        checksum_scope="",
    ).to_dict()
    assert report["passed"] is False
    assert "Append delta mismatch" in report["message"]
