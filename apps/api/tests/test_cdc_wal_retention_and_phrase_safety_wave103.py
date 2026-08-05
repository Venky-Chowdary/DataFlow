"""Regressions for WAL-retention safety and Pilot sync-phrase safety.

Both guard against silent, destructive behaviour that unit-level refactors
reintroduced once already:

* advancing a replication slot during the initial dump can discard the WAL the
  snapshot→streaming handoff resumes from;
* substring phrase matching can read a table named ``replacements`` as an
  instruction to overwrite a destination.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc

_CFG = {
    "host": "localhost",
    "port": 5432,
    "database": "test",
    "username": "",
    "password": "",
    "connection_string": "",
    "ssl": False,
    "schema": "public",
}


def _cdc() -> PostgreSqlChangeStreamCdc:
    return PostgreSqlChangeStreamCdc(
        _CFG,
        table="orders",
        primary_key="id",
        cursor_key="pg:test:orders:wave103",
        columns=["id", "amount"],
        output_plugin="test_decoding",
    )


def test_heartbeat_never_advances_slot_during_initial_snapshot() -> None:
    """The dump's handoff LSN must stay inside the slot's retained WAL."""
    cdc = _cdc()
    cdc.phase = "snapshot"

    with patch.object(cdc, "_advance_idle_slot") as advance:
        cdc.heartbeat()

    advance.assert_not_called()


def test_heartbeat_advances_slot_when_streaming_and_idle() -> None:
    """Outside the dump the slot must still be released, or WAL grows forever."""
    cdc = _cdc()
    cdc.phase = "streaming"
    cdc._pending_ack_lsn = ""

    with patch.object(cdc, "_advance_idle_slot") as advance, patch.object(
        cdc, "_incremental_snapshot_open", return_value=False
    ):
        cdc.heartbeat()

    advance.assert_called_once()


def test_heartbeat_holds_wal_while_an_incremental_snapshot_window_is_open() -> None:
    cdc = _cdc()
    cdc.phase = "streaming"
    cdc._pending_ack_lsn = ""

    with patch.object(cdc, "_advance_idle_slot") as advance, patch.object(
        cdc, "_incremental_snapshot_open", return_value=True
    ):
        cdc.heartbeat()

    advance.assert_not_called()


def test_idle_slot_advance_skips_when_peek_finds_pending_changes() -> None:
    """A non-empty peek proves undecoded changes exist — never discard them."""
    cdc = _cdc()
    cdc.phase = "streaming"

    conn = MagicMock()
    cur = MagicMock()
    # pg_current_wal_lsn, then the peek returns a row → pending work.
    cur.fetchone.side_effect = [("0/16B3600",), (1,)]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn):
        cdc._advance_idle_slot()

    executed = " ".join(str(c.args[0]) for c in cur.execute.call_args_list)
    assert "pg_replication_slot_advance" not in executed


@pytest.mark.parametrize(
    ("spoken", "expected"),
    [
        # Word-boundary matching: a table named "replacements" is not consent
        # to wipe the destination.
        ("copy replacements", "full_refresh_append"),
        ("load into truncated_events", "full_refresh_append"),
        ("sync overwritten_rows", "full_refresh_append"),
        # Genuine operator intent still resolves to overwrite.
        ("replace the table", "full_refresh_overwrite"),
        ("truncate and load", "full_refresh_overwrite"),
        ("overwrite", "full_refresh_overwrite"),
    ],
)
def test_sync_phrase_matching_is_word_boundary(spoken: str, expected: str) -> None:
    from src.ai.copilot.transfer_tools import normalize_sync_mode

    assert normalize_sync_mode(spoken) == expected


def test_every_pilot_sync_mode_resolves_to_a_canonical_engine_mode() -> None:
    """A Pilot token the engine does not know degrades to full-read + insert."""
    from services.sync_cursor import CANONICAL_SYNC_MODES
    from services.sync_cursor import normalize_sync_mode as canonical
    from src.ai.copilot.transfer_tools import SYNC_MODES

    for mode in SYNC_MODES:
        assert canonical(mode) in CANONICAL_SYNC_MODES, mode


def test_unknown_phrase_falls_back_to_non_destructive_default() -> None:
    from src.ai.copilot.transfer_tools import normalize_sync_mode

    assert normalize_sync_mode("nonsense words") == "full_refresh_append"
    assert normalize_sync_mode("") == "full_refresh_append"
