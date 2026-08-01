"""Regression tests for the wave-95 engine hardening pass.

Each test here pins a defect that was silent in production: rows skipped by a
non-unique keyset bookmark, an unbounded reconcile accumulator that OOMed after
the data had already landed, sync-mode tokens that degraded to full-read+insert,
a checkpoint write whose failure nobody reported, and concurrent schema
absorption that could lose a discovered column.
"""

from __future__ import annotations

import sys
import threading
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.reconciliation import (  # noqa: E402
    FingerprintAccumulator,
    canonical_checksum_from_iter,
)
from services.sync_cursor import (  # noqa: E402
    CANONICAL_SYNC_MODES,
    max_cursor_value,
    normalize_sync_mode,
    requires_incremental,
    requires_upsert,
)


class TestSyncModeTaxonomyIsUnified:
    """Four layers spelled these differently; only one vocabulary may reach the engine."""

    @pytest.mark.parametrize(
        "spoken,canonical",
        [
            # schedule_store vocabulary
            ("incremental", "incremental_append"),
            ("scd2", "scd2"),
            ("mirror", "mirror"),
            ("reverse_etl", "reverse_etl"),
            # copilot/transfer_tools vocabulary
            ("incremental_upsert", "incremental_deduped"),
            ("cdc_incremental", "cdc"),
            # sync_cursor's own historical spellings
            ("full_refresh_mirror", "mirror"),
            ("upsert", "upsert"),
            ("merge", "upsert"),
            # UI / API aliases
            ("append", "full_refresh_append"),
            ("overwrite", "full_refresh_overwrite"),
            ("Full Refresh Overwrite", "full_refresh_overwrite"),
        ],
    )
    def test_every_dialect_resolves_to_one_canonical_mode(self, spoken, canonical):
        assert normalize_sync_mode(spoken) == canonical
        assert canonical in CANONICAL_SYNC_MODES

    def test_bare_incremental_actually_reads_incrementally(self):
        """`incremental` fell through unmapped, so every run re-read the source."""
        assert requires_incremental(normalize_sync_mode("incremental")) is True

    def test_incremental_upsert_actually_upserts(self):
        """It normalized to itself, matched no upsert set, and wrote duplicate rows."""
        assert requires_upsert("incremental_upsert") is True
        assert requires_incremental(normalize_sync_mode("incremental_upsert")) is True

    def test_cdc_incremental_is_cdc(self):
        assert requires_upsert("cdc_incremental") is True
        assert requires_incremental(normalize_sync_mode("cdc_incremental")) is True

    def test_scd2_is_incremental_and_key_idempotent(self):
        assert requires_incremental(normalize_sync_mode("scd2")) is True
        assert requires_upsert("scd2") is True

    def test_bare_upsert_does_not_demand_a_cursor(self):
        """Upsert describes the write, not the read — full scan, idempotent write."""
        assert requires_upsert("upsert") is True
        assert requires_incremental(normalize_sync_mode("upsert")) is False

    def test_upsert_never_becomes_destructive_mirror(self):
        """`mirror` also deletes rows the source dropped; upsert must not alias to it."""
        from services.sync_cursor import _SYNC_MODE_ALIASES

        assert normalize_sync_mode("upsert") != "mirror"
        assert "mirror" not in {
            v for k, v in _SYNC_MODE_ALIASES.items() if k not in {"full_refresh_mirror"}
        }

    def test_omitted_mode_is_still_non_destructive(self):
        from services.sync_cursor import is_overwrite_sync

        assert normalize_sync_mode(None) == "full_refresh_append"
        assert is_overwrite_sync(None) is False

    def test_unknown_mode_is_reported_not_swallowed(self, caplog):
        with caplog.at_level("WARNING"):
            normalize_sync_mode("teleport_mode")
        assert "teleport_mode" in caplog.text


class TestKeysetBookmarkTieBreak:
    """A strict `>` on a non-unique bookmark drops every tied row at a page edge."""

    def test_composite_watermark_carries_the_primary_key(self):
        rows = [["2024-01-01", "a"], ["2024-01-01", "b"], ["2024-01-01", "c"]]
        headers = ["updated_at", "id"]
        assert max_cursor_value(rows, headers, "updated_at", "id") == "2024-01-01|c"

    def test_without_a_tiebreak_the_watermark_cannot_distinguish_peers(self):
        """This is the shape of the bug: the bookmark alone loses b and c."""
        rows = [["2024-01-01", "a"], ["2024-01-01", "b"], ["2024-01-01", "c"]]
        headers = ["updated_at", "id"]
        bare = max_cursor_value(rows, headers, "updated_at")
        assert bare == "2024-01-01"
        # Seeking `updated_at > '2024-01-01'` skips the untransferred peers.
        assert "|" not in bare

    def test_stream_requires_unique_evidence_before_using_keyset(self):
        """No PK evidence must fall back to OFFSET rather than risk silent loss."""
        source = Path("src/transfer/stream.py").read_text(encoding="utf-8")
        assert "keyset_unique or bool(keyset_tiebreak)" in source, (
            "keyset pagination must require a unique bookmark or a PK tie-break"
        )
        assert "cursor_primary_key=keyset_tiebreak or None" in source, (
            "the keyset read must pass its tie-break through to the reader"
        )


class TestReconcileChecksumIsMemoryBounded:
    """Strict reconcile defaults to limit=0 — it must not hold every row in RAM."""

    def test_from_iter_uses_the_spilling_accumulator(self):
        source = Path("services/reconciliation.py").read_text(encoding="utf-8")
        head = source.split("def canonical_checksum_from_iter", 1)[1].split("def ", 1)[0]
        assert "FingerprintAccumulator()" in head
        assert "fingerprints.append" not in head

    def test_digest_is_unchanged_by_the_spill(self):
        """A spilling run and an in-memory run must agree, or checksums break."""
        rows = [{"id": str(i), "v": f"value-{i}"} for i in range(500)]
        expected = canonical_checksum_from_iter(iter(rows), ["id", "v"])

        spilled = FingerprintAccumulator(threshold=16)
        plain = FingerprintAccumulator(threshold=10**9)
        for i in range(500):
            key, fp = str(i), f"fp-{i}"
            spilled.add(key, fp)
            plain.add(key, fp)
        assert spilled.chunk_files or spilled.total == 500
        assert spilled.digest() == plain.digest()
        assert expected  # sanity: the real path still produces a digest

    def test_order_independent_across_spill_boundaries(self):
        forward = FingerprintAccumulator(threshold=8)
        reverse = FingerprintAccumulator(threshold=8)
        pairs = [(str(i), f"fp-{i}") for i in range(64)]
        for key, fp in pairs:
            forward.add(key, fp)
        for key, fp in reversed(pairs):
            reverse.add(key, fp)
        assert forward.digest() == reverse.digest()

    def test_limit_still_truncates(self):
        rows = [{"id": str(i)} for i in range(100)]
        few = canonical_checksum_from_iter(iter(rows), ["id"], limit=10)
        many = canonical_checksum_from_iter(iter(rows), ["id"], limit=0)
        assert few != many


class TestCheckpointFailureIsSurfaced:
    """A rejected checkpoint write means resume is gone; that must not be silent."""

    def _service(self, ok: bool):
        from services.checkpoint_service import Checkpoint, CheckpointService

        class _Store:
            def update_job_status(self, *a, **k):
                return ok

        return CheckpointService(mongo=_Store()), Checkpoint(job_id="j1")

    def test_failed_save_marks_the_service_degraded(self, caplog):
        svc, cp = self._service(ok=False)
        with caplog.at_level("WARNING"):
            assert svc.save(cp) is False
        assert svc.degraded is True
        assert svc.failed_saves == 1
        assert "resumed" in caplog.text.lower() or "resume" in caplog.text.lower()

    def test_successful_save_stays_healthy(self):
        svc, cp = self._service(ok=True)
        assert svc.save(cp) is True
        assert svc.degraded is False

    def test_repeated_failures_log_once_but_keep_counting(self, caplog):
        svc, cp = self._service(ok=False)
        with caplog.at_level("WARNING"):
            for _ in range(5):
                svc.save(cp)
        assert svc.failed_saves == 5
        assert caplog.text.count("Checkpoint write failed") == 1

    def test_stream_surfaces_the_degraded_checkpoint_to_the_operator(self):
        source = Path("src/transfer/stream.py").read_text(encoding="utf-8")
        assert "Checkpoint persistence is failing" in source
        assert "_checkpoint_degraded" in source


class TestSchemalessAbsorptionIsThreadSafe:
    """Chunk writers rebind shared mapping state; interleaving lost a column."""

    def test_absorb_is_guarded_by_a_lock(self):
        source = Path("src/transfer/stream.py").read_text(encoding="utf-8")
        body = source.split("def _absorb_schemaless_discovered_attrs", 1)[1]
        body = body.split("\n    if not schema:", 1)[0]
        assert "with _schema_absorb_lock:" in body
        assert "_schema_absorb_lock = threading.Lock()" in source

    def test_concurrent_union_under_a_lock_keeps_every_attribute(self):
        """Models the read-modify-write the writers perform on `columns`."""
        from connectors.header_union import union_attribute_keys

        lock = threading.Lock()
        columns = ["id"]
        discovered = [f"attr_{i}" for i in range(64)]

        def absorb(name: str) -> None:
            nonlocal columns
            with lock:
                columns = union_attribute_keys(columns, [name])

        threads = [threading.Thread(target=absorb, args=(n,)) for n in discovered]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert set(discovered).issubset(set(columns)), "a discovered attribute was lost"
