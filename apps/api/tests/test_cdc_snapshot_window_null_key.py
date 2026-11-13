"""A NULL-key row in an incremental snapshot chunk reaches the writer's quarantine (review F6).

``SnapshotWindow.add_snapshot_rows`` skipped a row whose key is NULL
(``if pk is None: continue``): it was neither written nor counted, so the run's
rejected count said nothing about it. The row is now emitted like every other
snapshot row; the writer's dense-upsert key check holds it out with a finding,
so the rejected count matches what did not land. A NULL-key live event inside
the window is kept the same way.
"""

from __future__ import annotations

import uuid

import pytest

from services.cdc_snapshot_window import merge_snapshot_chunk_with_stream
from services.value_serializer import WireCell, SQL_NULL_SENTINEL


def test_a_null_key_snapshot_row_is_emitted_not_dropped():
    emitted, stats = merge_snapshot_chunk_with_stream(
        window_id="w", primary_key="sku",
        snapshot_rows=[{"sku": "a", "qty": 1}, {"sku": None, "qty": 9},
                       {"sku": WireCell(SQL_NULL_SENTINEL), "qty": 8}],
        stream_events=[{"op": "c", "row": {"sku": None, "qty": 7}}],
    )
    assert sorted(r["qty"] for r in emitted if not r.get("__deleted")) == [1, 7, 8, 9], emitted
    assert stats["snapshot_rows"] == 3, stats


def test_a_literal_key_spelling_the_marker_is_an_ordinary_key():
    emitted, _stats = merge_snapshot_chunk_with_stream(
        window_id="w", primary_key="sku",
        snapshot_rows=[{"sku": SQL_NULL_SENTINEL, "qty": 2}],
        stream_events=[{"op": "u", "row": {"sku": SQL_NULL_SENTINEL, "qty": 20}}],
    )
    assert [(r["sku"], r["qty"]) for r in emitted] == [(SQL_NULL_SENTINEL, 20)]


def test_live_null_key_snapshot_row_is_quarantined_and_counted(tmp_path, monkeypatch):
    """The emitted rows through the real CDC apply into PostgreSQL."""
    from services import audit_log, quarantine_dlq
    from tests.helpers.live_env import pg_creds, pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not reachable")
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(quarantine_dlq, "DLQ_PATH", tmp_path / "dlq.jsonl")
    monkeypatch.setattr(quarantine_dlq, "_dlq_coll", lambda: None)
    from services.cdc_engine import ChangeBatch
    from src.transfer.cdc_transfer import _apply_change_batch
    from src.transfer.models import EndpointConfig
    from tests.c3_live_helpers import pg_sql

    emitted, _stats = merge_snapshot_chunk_with_stream(
        window_id="w", primary_key="sku",
        snapshot_rows=[{"sku": "a", "qty": "1"}, {"sku": WireCell(SQL_NULL_SENTINEL), "qty": "9"}],
        stream_events=[],
    )
    c = pg_creds()
    table = f"swn_{uuid.uuid4().hex[:8]}"
    cfg = {"host": c["host"], "port": int(c["port"]), "database": c["database"],
           "username": c["username"], "password": c["password"], "schema": "public"}
    destination = EndpointConfig(kind="database", format="postgresql", table=table, **cfg)
    maps = [{"source": "sku", "target": "sku", "source_type": "TEXT", "target_type": "TEXT"},
            {"source": "qty", "target": "qty", "source_type": "INTEGER", "target_type": "INTEGER"}]
    try:
        _rows, _sum, summary, _deleted = _apply_change_batch(
            "postgresql", destination, dict(cfg, type="postgresql"), table,
            ChangeBatch(inserts=[r for r in emitted if not r.get("__deleted")]),
            maps, {"sku": "TEXT", "qty": "INTEGER"}, ["sku", "qty"], "sku", 0, 1,
            job_id="swn" + uuid.uuid4().hex[:8],
        )
        assert pg_sql(f'SELECT sku, qty FROM public."{table}"', fetch=True) == [("a", 1)]
        assert int(summary.get("rejected_rows") or 0) == 1, summary.get("rejected_details")
    finally:
        pg_sql(f'DROP TABLE IF EXISTS public."{table}"')
        pg_sql(f'DROP TABLE IF EXISTS public."{table}_df_quarantine"')
