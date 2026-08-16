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
        """Encoded with the canonical separator, which no column value can hold.

        A pipe can appear inside a text cursor value, so a watermark joined with
        one was indistinguishable from a bare value carrying a pipe.
        """
        from services.keyset_pagination import KEYSET_SEP

        rows = [["2024-01-01", "a"], ["2024-01-01", "b"], ["2024-01-01", "c"]]
        headers = ["updated_at", "id"]
        assert (
            max_cursor_value(rows, headers, "updated_at", "id")
            == f"2024-01-01{KEYSET_SEP}c"
        )

    def test_without_a_tiebreak_the_watermark_cannot_distinguish_peers(self):
        """This is the shape of the bug: the bookmark alone loses b and c."""
        rows = [["2024-01-01", "a"], ["2024-01-01", "b"], ["2024-01-01", "c"]]
        headers = ["updated_at", "id"]
        from services.keyset_pagination import KEYSET_SEP

        bare = max_cursor_value(rows, headers, "updated_at")
        assert bare == "2024-01-01"
        # Seeking `updated_at > '2024-01-01'` skips the untransferred peers.
        assert KEYSET_SEP not in bare

    def test_stream_requires_unique_evidence_before_using_keyset(self):
        """No PK evidence must fall back to OFFSET rather than risk silent loss.

        Phase F2: full composite ``keyset_order_cols`` (or incremental cursor)
        gates seek reads; OFFSET when the PK list is empty.
        """
        from services.keyset_pagination import decide_keyset_pagination

        common = dict(
            src_type="postgresql",
            keyset_col="updated_at",
            keyset_tiebreak="",
            incremental=False,
            offset=0,
            chunk_index=0,
            cursor_after=None,
            snapshot_scan=False,
        )
        no_key = decide_keyset_pagination(keyset_order_cols=[], **common)
        assert no_key.use_keyset is False
        assert no_key.pagination_mode == "offset"

        with_key = decide_keyset_pagination(keyset_order_cols=["id"], **common)
        assert with_key.use_keyset is True
        assert with_key.pagination_mode == "keyset"

        # An unsupported source cannot seek even with a declared key.
        assert (
            decide_keyset_pagination(
                keyset_order_cols=["id"], **{**common, "src_type": "csv"}
            ).pagination_mode
            == "offset"
        )

        source = Path("src/transfer/stream.py").read_text(encoding="utf-8")
        assert "decide_keyset_pagination(" in source, (
            "stream must use the shared decision, not a second copy of it"
        )
        assert "cursor_key_columns" in source, (
            "composite keyset must pass ordered key columns to the reader"
        )

    def test_a_resume_without_a_bookmark_falls_back_instead_of_reseeking(self):
        """Offset says rows landed; seeking from the top would re-read them."""
        from services.keyset_pagination import decide_keyset_pagination

        decision = decide_keyset_pagination(
            src_type="postgresql",
            keyset_order_cols=["id"],
            keyset_col="id",
            keyset_tiebreak="",
            incremental=False,
            offset=5000,
            chunk_index=2,
            cursor_after=None,
            snapshot_scan=False,
        )
        assert decision.use_keyset is False
        assert decision.resume_fallback is True
        assert decision.pagination_mode == "offset"

    def test_an_incremental_cursor_seeks_with_a_tiebreak(self):
        from services.keyset_pagination import decide_keyset_pagination

        decision = decide_keyset_pagination(
            src_type="mysql",
            keyset_order_cols=[],
            keyset_col="updated_at",
            keyset_tiebreak="id",
            incremental=True,
            offset=0,
            chunk_index=0,
            cursor_after=None,
            snapshot_scan=False,
        )
        assert decision.use_keyset is True
        assert decision.order_cols == ["updated_at", "id"]


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
    """A rejected checkpoint write means resume is gone; the job must hard-fail."""

    def _service(self, ok: bool):
        from services.checkpoint_service import Checkpoint, CheckpointService

        class _Store:
            def update_job_status(self, *a, **k):
                return ok

        return CheckpointService(mongo=_Store()), Checkpoint(job_id="j1")

    def test_failed_save_marks_has_failed_saves(self, caplog):
        svc, cp = self._service(ok=False)
        with caplog.at_level("ERROR"):
            assert svc.save(cp) is False
        assert svc.has_failed_saves is True
        assert svc.degraded is True
        assert svc.failed_saves == 1
        assert "refusing to continue" in caplog.text.lower()

    def test_require_save_raises_on_failure(self):
        from services.checkpoint_service import (
            CHECKPOINT_PERSISTENCE_FAILED,
            CheckpointPersistenceError,
        )

        svc, cp = self._service(ok=False)
        with pytest.raises(CheckpointPersistenceError, match="refusing to continue"):
            svc.require_save(cp)
        assert svc.has_failed_saves is True
        assert CHECKPOINT_PERSISTENCE_FAILED in str(
            CheckpointPersistenceError(CHECKPOINT_PERSISTENCE_FAILED)
        )

    def test_successful_save_stays_healthy(self):
        svc, cp = self._service(ok=True)
        assert svc.save(cp) is True
        assert svc.has_failed_saves is False
        assert svc.degraded is False
        svc.require_save(cp)  # must not raise

    def test_repeated_failures_log_once_but_keep_counting(self, caplog):
        svc, cp = self._service(ok=False)
        with caplog.at_level("ERROR"):
            for _ in range(5):
                svc.save(cp)
        assert svc.failed_saves == 5
        assert svc.has_failed_saves is True
        assert caplog.text.count("Checkpoint write failed") == 1

    def test_stream_fail_closes_on_checkpoint_persistence(self):
        source = Path("src/transfer/stream.py").read_text(encoding="utf-8")
        assert "require_save(checkpoint)" in source
        assert "Checkpoint persistence is failing" not in source
        assert "_checkpoint_degraded" not in source
        assert "refusing to continue" in Path(
            "services/checkpoint_service.py"
        ).read_text(encoding="utf-8")


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
