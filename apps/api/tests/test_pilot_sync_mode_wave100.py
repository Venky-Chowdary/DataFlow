"""Pilot sync-mode phrases must resolve through the canonical helper (D1)."""

from __future__ import annotations

from src.ai.copilot.transfer_tools import normalize_sync_mode, sync_mode_from_phrase


class TestPilotSyncModeDoesNotWipeOnSubstring:
    def test_table_named_replacements_is_not_overwrite(self) -> None:
        assert normalize_sync_mode("sync replacements to warehouse") == "full_refresh_append"

    def test_explicit_overwrite_phrase_is_destructive(self) -> None:
        assert normalize_sync_mode("overwrite the destination") == "full_refresh_overwrite"
        assert normalize_sync_mode("replace all") == "full_refresh_overwrite"

    def test_full_refresh_alone_is_append_not_overwrite(self) -> None:
        """'full refresh' without 'overwrite' must stay non-destructive."""
        assert sync_mode_from_phrase("full refresh") == "full_refresh_append"
        assert normalize_sync_mode("full refresh") == "full_refresh_append"

    def test_upsert_phrase_canonicalises(self) -> None:
        # Canonical alias table maps incremental_upsert → incremental_deduped.
        assert normalize_sync_mode("incremental upsert") == "incremental_deduped"

    def test_cdc_phrase(self) -> None:
        assert normalize_sync_mode("cdc") in {"cdc", "cdc_incremental"}
        # Whatever the canonical form is, it must not be a full refresh.
        assert "full_refresh" not in normalize_sync_mode("change data capture")
