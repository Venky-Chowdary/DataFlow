"""Re-review finding 11 (LOW-MEDIUM): the widen preview is the DDL the apply executes.

The preview came from ``_build_widen_ddl`` while the apply ran
``widen_existing_columns_native``, which adds what the preview never showed:
MySQL's preserved column clause (NOT NULL / DEFAULT / COMMENT), the keyed
carrier substitution, one combined MODIFY, the UTC pin. The preview is now a
dry run of the very helper the apply calls, with the same inputs.
"""

from __future__ import annotations

import pytest

from connectors import schema_drift


class _Cursor:
    def __init__(self) -> None:
        self.executed: list[str] = []

    def execute(self, sql, *_a):
        self.executed.append(str(sql))

    def fetchall(self):
        return []


@pytest.fixture()
def mysql_catalog(monkeypatch):
    monkeypatch.setattr(schema_drift, "_fetch_existing_columns",
                        lambda cursor, dialect, schema, table: {"code": "varchar(3)", "id": "int"})
    monkeypatch.setattr(schema_drift, "_key_widen_dependents", lambda *a, **k: [])
    monkeypatch.setattr(schema_drift, "_mysql_preserved_column_clause",
                        lambda cursor, schema, table, col, new_type: " NOT NULL DEFAULT 'x' COMMENT 'kept'")
    monkeypatch.setattr(schema_drift, "_mysql_keyed_widen_carrier",
                        lambda cursor, schema, table, col, new_type, existing, dialect, growing: (new_type, False))


def test_a_dry_run_executes_nothing_and_returns_what_a_real_run_executes(mysql_catalog):
    dry_cursor, real_cursor = _Cursor(), _Cursor()
    args = ("mysql", None, "orders", ["code"], ["VARCHAR(20)"])
    dry = schema_drift.widen_existing_columns_native(dry_cursor, *args, backfill=True, dry_run=True)
    assert dry_cursor.executed == []
    real = schema_drift.widen_existing_columns_native(real_cursor, *args, backfill=True)
    assert real_cursor.executed == real == dry
    assert "NOT NULL DEFAULT 'x' COMMENT 'kept'" in dry[0]
    # The old preview (the bare builder) never showed the preserved clause.
    assert dry[0] != schema_drift._build_widen_ddl("mysql", None, "orders", "code", "VARCHAR(20)", "varchar(3)")


def test_the_fix_previews_and_applies_the_same_helper_call(monkeypatch, tmp_path):
    import services.mongodb_service as mongodb_service
    from services import audit_log, parity_cache, parity_fixes
    from services.mongodb_service import MemoryMongoDBService
    from services.parity_report import build_parity_report

    parity_cache.clear()
    svc = MemoryMongoDBService()
    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: svc)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setenv("DATAFLOW_LLM_ENABLED", "false")
    calls: list[tuple] = []

    def statements(job, table, column, new_type, *, dry_run):
        calls.append((table, column, new_type, dry_run))
        return ["ALTER TABLE `orders` MODIFY COLUMN `code` VARCHAR(20) NOT NULL"]

    monkeypatch.setattr(parity_fixes, "widen_statements", statements)
    monkeypatch.setattr(parity_fixes, "_live_types", lambda job, ev, columns, report=None: {
        "source": {"code": "VARCHAR(20)"}, "dest": {"code": "varchar(3)"}, "dialect": "mysql"})
    job_id = svc.create_transfer_job({
        "name": "o", "workspace_id": "w",
        "reconciliation": {"passed": False, "source_rows": 10, "target_rows": 10,
                           "sample_compare": {"rows_compared": 10, "mismatches": [
                               {"key": "1", "target": "code", "source_value": "abcdef", "target_value": "abc"}]}},
        "destination_summary": {"table": "orders", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"},
        "transfer_request": {"source": {"kind": "database", "format": "mysql", "table": "o"},
                             "destination": {"kind": "database", "format": "mysql", "table": "orders"},
                             "sync_mode": "full_refresh_overwrite"},
    })
    svc.update_job_status(job_id, "completed")
    widen = next(f for f in build_parity_report(svc.get_job(job_id), job_id=job_id)["fixes"]
                 if f["action"] == "widen_column")
    assert widen["applicable"], widen["not_applicable_reason"]
    assert widen["preview"] == "ALTER TABLE `orders` MODIFY COLUMN `code` VARCHAR(20) NOT NULL"
    report = parity_fixes.approve_fix(job_id, widen["id"], actor="dba@example.com", note="go")
    done = next(f for f in report["fixes"] if f["id"] == widen["id"])
    assert done["status"] == "applied" and done["result"] == "Ran: " + widen["preview"]
    previews = [c for c in calls if c[3]]
    applies = [c for c in calls if not c[3]]
    assert applies == [("orders", "code", "VARCHAR(20)", False)]
    assert {c[:3] for c in previews} == {applies[0][:3]}
    parity_cache.clear()
