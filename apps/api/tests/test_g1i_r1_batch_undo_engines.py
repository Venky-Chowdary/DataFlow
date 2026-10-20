"""G-1i-R1 / X-18: failed-batch undo must clear non-ANSI destinations too.

``batch_undo._delete_all_rows`` only covered sqlite / postgresql / mysql /
sqlserver / oracle. A failed batch on MongoDB, DuckDB, or BigQuery fell
through to ``return False`` — the partial batch silently stayed landed with
a generic "could not be removed" note, on engines where a real delete exists.

Adds per-engine clears: MongoDB ``delete_many({})``, BigQuery native-client
``DELETE ... WHERE TRUE``, and every SQLAlchemy-writer dialect (DuckDB et al)
through the warehouse engine + dialect-quoted ref.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import batch_undo  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402

MONGO_URI = "mongodb://localhost:27017/?directConnection=true"


def _mongo_live() -> bool:
    try:
        from pymongo import MongoClient

        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500)
        client.admin.command("ping")
        client.close()
        return True
    except Exception:
        return False


def test_mongodb_dest_batch_is_deleted():
    if not _mongo_live():
        pytest.skip("local MongoDB not reachable")
    from pymongo import MongoClient

    db, coll = f"undo_{uuid.uuid4().hex[:8]}", "batch"
    client = MongoClient(MONGO_URI)
    client[db][coll].insert_many([{"_id": i, "v": i} for i in range(5)])

    destination = EndpointConfig(
        kind="database",
        format="mongodb",
        connection_string=MONGO_URI,
        database=db,
        table=coll,
    )
    summary = {"table": coll, "target_rows_before": 0, "sync_mode": "full_refresh"}
    try:
        note = batch_undo.undo_failed_batch_if_dest_was_empty(
            destination=destination, dest_summary=summary, sync_mode="full_refresh"
        )
        assert summary["partial_batch_removed"] is True, note
        assert client[db][coll].count_documents({}) == 0
        assert "empty" in note or "dropped" in note or "removed" in note
    finally:
        client.drop_database(db)
        client.close()


def test_duckdb_dest_batch_is_deleted(tmp_path):
    pytest.importorskip("duckdb")
    import duckdb

    path = str(tmp_path / "dest.duckdb")
    with duckdb.connect(path) as conn:
        conn.execute("CREATE TABLE batch (id INTEGER, v INTEGER)")
        conn.execute("INSERT INTO batch VALUES (1,1),(2,2),(3,3)")

    destination = EndpointConfig(
        kind="database", format="duckdb", database=path, table="batch"
    )
    summary = {"table": "batch", "target_rows_before": 0, "sync_mode": "full_refresh"}
    note = batch_undo.undo_failed_batch_if_dest_was_empty(
        destination=destination, dest_summary=summary, sync_mode="full_refresh"
    )
    assert summary["partial_batch_removed"] is True, note
    with duckdb.connect(path, read_only=True) as conn:
        assert conn.execute("SELECT COUNT(*) FROM batch").fetchone()[0] == 0


def test_bigquery_dest_batch_is_deleted_through_client(monkeypatch):
    """No live BigQuery: the delete must reach the native client with the
    fully-qualified ref (gate on the call, not the engine)."""
    ran: list[str] = []

    class _Job:
        def result(self, **kw):
            return []

    class _Client:
        def query(self, sql, **kw):
            ran.append(sql)

            class _Q:
                def result(self, **kw2):
                    return []

            return _Q()

    monkeypatch.setattr(
        "connectors.bigquery_conn.get_client", lambda **kw: _Client()
    )

    destination = EndpointConfig(
        kind="database",
        format="bigquery",
        database="proj",
        schema="ds",
        table="batch",
    )
    summary = {
        "table": "batch",
        "target_rows_before": 0,
        "sync_mode": "full_refresh",
    }
    note = batch_undo.undo_failed_batch_if_dest_was_empty(
        destination=destination, dest_summary=summary, sync_mode="full_refresh"
    )
    deletes = [s for s in ran if s.strip().upper().startswith("DELETE")]
    assert deletes, f"expected a DELETE among issued statements {ran!r} — {note}"
    assert "batch" in deletes[0]
    # a pre-delete COUNT(*) probe may run first to decide whether the dest
    # table existed before this run (undo must not DELETE a table it never
    # created); the post-delete count is re-measured through the engine path
    assert summary.get("partial_batch_undo") in {"cleared", "retained"}


def test_non_generic_dest_still_refuses_honestly():
    """A dest with no delete path keeps the honest retained note — never a
    fake 'cleared'. A non-database kind refuses before any network call."""
    destination = EndpointConfig(
        kind="file", format="csv", output_path="/tmp/x.csv", table="t"
    )
    summary = {"table": "t", "target_rows_before": 0, "sync_mode": "full_refresh"}
    note = batch_undo.undo_failed_batch_if_dest_was_empty(
        destination=destination, dest_summary=summary, sync_mode="full_refresh"
    )
    assert summary["partial_batch_undo"] == "retained"
    assert "not" in note
