"""Live PG: a filtered incremental snapshot backfills only matching rows while
stream events for every row (matching or not) keep flowing."""

from __future__ import annotations

import time
import uuid

import pytest

from tests.test_cdc_postgres_streaming_transport_live import CFG, _ready, _slot, _sql, _wait

pytestmark = pytest.mark.skipif(
    not _ready(), reason="PostgreSQL with wal_level=logical not reachable on localhost:5432"
)


def test_filtered_incremental_snapshot_reads_only_matching_rows(tmp_path, monkeypatch):
    import services.cdc_incremental_snapshot as snap_mod
    from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc

    monkeypatch.setattr(snap_mod, "_PATH", str(tmp_path / "signals.json"))
    monkeypatch.setattr(snap_mod, "_signals_coll", lambda: None)
    table = "cdc_filt_" + uuid.uuid4().hex[:8]
    _sql(f"CREATE TABLE {table} (id INT PRIMARY KEY, region TEXT, amount INT)")
    _sql(
        f"INSERT INTO {table} SELECT g, CASE WHEN g % 2 = 0 THEN 'EU' ELSE 'US' END, g * 10 "
        "FROM generate_series(1, 40) g"
    )
    cdc = PostgreSqlChangeStreamCdc(
        CFG, table=table, primary_key="id", cursor_key=f"filt-{table}", schema="public"
    )
    try:
        assert cdc.is_available() is True
        list(cdc.snapshot())
        sig = snap_mod.request_incremental_snapshot(
            cdc.source_key,
            table,
            primary_key="id",
            chunk_size=7,
            row_filter={"and": [{"column": "region", "op": "eq", "value": "EU"},
                                {"column": "amount", "op": "gte", "value": 100}]},
        )
        _sql(f"UPDATE {table} SET amount = amount + 1 WHERE id = 3")  # US row, outside filter
        snap_ids: list[int] = []
        stream_ids: list[int] = []
        end = time.monotonic() + 30
        while time.monotonic() < end:
            for batch in cdc.poll():
                rows = list(batch.inserts) + list(getattr(batch, "updates", []) or [])
                target = snap_ids if getattr(batch, "is_snapshot", False) or str(
                    getattr(batch, "op", "")
                ).startswith("snap") else stream_ids
                target.extend(int(r["id"]) for r in rows)
                if batch.resume_token:
                    cdc.ack(batch.resume_token)
            done = snap_mod.get_signal(sig.id)
            if done and done.status in {"completed", "failed"} and 3 in stream_ids + snap_ids:
                break
        final = snap_mod.get_signal(sig.id)
        assert final is not None and final.status == "completed", final
        expected = {i for i in range(1, 41) if i % 2 == 0 and i * 10 >= 100}
        captured = set(snap_ids) | set(stream_ids)
        assert expected.issubset(captured), sorted(expected - captured)
        assert final.rows_snapshotted == len(expected), final
        assert 3 in captured, "stream event for a row outside the filter must still flow"
        outside = {i for i in captured if i % 2 == 1 or i * 10 < 100} - {3}
        assert not outside, f"filter leaked rows into the snapshot: {sorted(outside)}"
    finally:
        cdc.close()
        _wait(lambda: not _slot(cdc.slot_name)[1], 10)
        _sql("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s", (cdc.slot_name,))
        _sql(f"DROP PUBLICATION IF EXISTS {cdc.publication_name}")
        _sql(f"DROP TABLE IF EXISTS {table}")
