"""M4 — the opening write message must not quote the full table count on a
bounded incremental run (a 6-row delta on a 500K table said "Streaming
500,000 rows in batches…")."""

from __future__ import annotations

from types import SimpleNamespace


def test_opening_message_delta_omits_table_count():
    from services.batch_progress import opening_batch_message

    msg = opening_batch_message(500_000, delta=True)
    assert "500,000" not in msg
    assert "new or updated" in msg.lower()


def test_opening_message_full_run_unchanged():
    from services.batch_progress import opening_batch_message

    msg = opening_batch_message(500_000)
    assert msg == "Streaming 500,000 rows in batches…"


def test_bounded_scope_flags_delta(monkeypatch):
    import services.sync_cursor as sc
    from src.transfer.engine import _bounded_incremental_read

    monkeypatch.setattr(
        sc,
        "resolve_incremental_read_scope",
        lambda **kw: SimpleNamespace(bounded=True),
    )
    request = SimpleNamespace(
        sync_mode="incremental_append",
        stream_contracts=[{"cursor_field": "updated_at"}],
        source=SimpleNamespace(format="postgresql", database="d", table="t",
                               collection=""),
        destination=SimpleNamespace(format="sqlite", database="d", table="o",
                                    collection=""),
    )
    assert _bounded_incremental_read(request) is True


def test_unbounded_scope_keeps_table_count(monkeypatch):
    import services.sync_cursor as sc
    from src.transfer.engine import _bounded_incremental_read

    monkeypatch.setattr(
        sc,
        "resolve_incremental_read_scope",
        lambda **kw: SimpleNamespace(bounded=False),
    )
    request = SimpleNamespace(
        sync_mode="incremental_append",
        stream_contracts=[],
        source=SimpleNamespace(format="postgresql", database="d", table="t",
                               collection=""),
        destination=SimpleNamespace(format="sqlite", database="d", table="o",
                                    collection=""),
    )
    assert _bounded_incremental_read(request) is False
