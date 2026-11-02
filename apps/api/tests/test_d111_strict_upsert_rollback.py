"""D111 — a strict keyed upsert that fails verification on a populated
destination must restore the pre-run state, not leave its batch in place.

The fix row-copies the occupied destination to ``__dfbak_<job>`` before the
first write and restores it (DELETE + INSERT FROM backup) through the
existing ``route_backups`` settle path. These tests pin the wiring with
fake connections — the live end-to-end proof is the strict-upsert gate leg.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any


class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.conn.sql.append(str(sql))
        if "information_schema.tables" in str(sql):
            self._has = True

    def fetchone(self):
        if "_df_restore_diff" in self.conn.sql[-1]:
            return (0,)  # the restore's backup-vs-live verify: no difference
        return (1,) if getattr(self, "_has", True) else None

    def fetchall(self):
        return []  # pg_trigger: the table carries no user triggers

    def __getattr__(self, name):
        # SAVEPOINT / RELEASE etc. route through execute in the real code.
        raise AttributeError(name)


class _FakeConn:
    def __init__(self, has_backup=True):
        self.sql: list[str] = []
        self.autocommit = None
        self._cursor = _FakeCursor(self)
        self._cursor._has = has_backup
        self.closed = False

    def cursor(self):
        self._cursor._has = True
        return self._cursor

    def commit(self):
        pass

    def close(self):
        self.closed = True


def test_create_pg_table_backup_row_copies_live_table(monkeypatch):
    from connectors import table_manager

    fake = _FakeConn()
    monkeypatch.setattr(table_manager, "_pg_route_backup_connection", lambda cfg: fake)

    pair = table_manager.create_pg_table_backup(
        {"host": "h"}, schema="public", table="dst", job_id="abcdef0123456789"
    )
    assert pair == {
        "table": "dst",
        "backup": "dst__dfbak_abcdef01",
        "schema": "public",
    }
    joined = "\n".join(fake.sql)
    assert 'CREATE TABLE "public"."dst__dfbak_abcdef01" (LIKE "public"."dst" INCLUDING ALL)' in joined
    assert 'INSERT INTO "public"."dst__dfbak_abcdef01" SELECT * FROM "public"."dst"' in joined
    assert fake.closed


def test_restore_pg_route_backups_restores_pre_run_state(monkeypatch):
    from connectors import table_manager

    fake = _FakeConn()
    monkeypatch.setattr(table_manager, "_pg_route_backup_connection", lambda cfg: fake)

    restored = table_manager.restore_pg_route_backups(
        {"host": "h"},
        {"dst": {"table": "dst", "backup": "dst__dfbak_abcdef01", "schema": "public"}},
    )
    assert restored == ["dst"]
    joined = "\n".join(fake.sql)
    assert 'DELETE FROM "public"."dst"' in joined
    assert 'INSERT INTO "public"."dst" SELECT * FROM "public"."dst__dfbak_abcdef01"' in joined
    assert 'DROP TABLE "public"."dst__dfbak_abcdef01"' in joined


def _request_with_backups(backups):
    return SimpleNamespace(
        destination=SimpleNamespace(extra=dict(backups), format="postgresql"),
        sync_mode="incremental_upsert",
        validation_mode="strict",
    )


def test_failed_strict_upsert_restores_from_backup(monkeypatch):
    import src.transfer.engine as engine_mod
    from src.transfer.engine_destination import _failed_batch_undo_note_impl

    calls = {}

    def _fake_settle(destination, *, restore):
        calls["restore"] = restore
        return True

    monkeypatch.setattr(engine_mod, "_settle_overwrite_backup", _fake_settle)

    wm_calls = {}
    import services.batch_undo as bu

    monkeypatch.setattr(
        bu,
        "restore_route_watermark_after_undo",
        lambda summary: wm_calls.setdefault("n", 1) or "",
    )

    backups = {"route_backups": {"dst": {"table": "dst", "backup": "dst__dfbak_j1", "schema": "public"}}}
    request = _request_with_backups(backups)
    dest_summary: dict[str, Any] = {"table": "dst"}
    note = _failed_batch_undo_note_impl(request, dest_summary, "verify failed")
    assert calls["restore"] is True
    assert dest_summary["partial_batch_undo"] == "restored"
    assert "The destination table was restored" in note
    assert wm_calls.get("n") == 1  # D110: the failed run must not keep the watermark


def test_backup_absent_falls_back_to_honest_retained_note(monkeypatch):
    """Without an armed backup, a populated-destination failure still reports
    honestly that the batch was left in place (no silent false 'restored')."""
    from services.batch_undo import _decide

    # Populated destination, no backup → retained (pre-D111 honest report).
    decision, note = _decide(
        SimpleNamespace(kind="database", extra={"dest_rows_before": 5}),
        {},
        "incremental_upsert",
    )
    assert decision == "retained"
    assert "left in place" in note


def test_success_path_discards_backup(monkeypatch):
    """A passing run drops the backup via the same settle path (restore=False)."""
    import src.transfer.engine as engine_mod

    calls = {}
    monkeypatch.setattr(
        "connectors.table_manager.discard_pg_route_backups",
        lambda cfg, backups: calls.setdefault("discarded", list(backups)),
    )
    monkeypatch.setattr(
        "src.transfer.adapters.resolve_connector_config",
        lambda destination: {"host": "h"},
    )

    destination = SimpleNamespace(
        extra={
            "route_backups": {
                "dst": {"table": "dst", "backup": "dst__dfbak_j1", "schema": "public"}
            }
        },
        format="postgresql",
    )
    ok = engine_mod._settle_overwrite_backup(destination, restore=False)
    assert ok is True
    assert calls["discarded"] == ["dst"]
    assert "route_backups" not in destination.extra
