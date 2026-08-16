"""CDC incremental snapshot signal store."""

from __future__ import annotations

import services.cdc_incremental_snapshot as snap_mod
from services.cdc_incremental_snapshot import (
    claim_next_signal,
    complete_signal,
    list_signals,
    mark_chunk,
    request_incremental_snapshot,
)


def test_incremental_snapshot_lifecycle(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(snap_mod, "_PATH", str(tmp_path / "signals.json"))
    monkeypatch.setattr(snap_mod, "_DATA_DIR", str(tmp_path))
    sig = request_incremental_snapshot("src:pg", "orders", primary_key="id", chunk_size=100)
    assert sig.status == "pending"
    claimed = claim_next_signal("src:pg")
    assert claimed is not None
    assert claimed.id == sig.id
    assert claimed.status == "running"
    # Running signals are resumable (chunked incremental snapshot).
    resumed = claim_next_signal("src:pg")
    assert resumed is not None and resumed.id == sig.id
    # No second pending signal to steal.
    assert request_incremental_snapshot("src:pg", "other", primary_key="id").status == "pending"
    mark_chunk(sig.id, last_pk="50", rows=50)
    done = complete_signal(sig.id)
    assert done is not None
    assert done.status == "completed"
    assert done.rows_snapshotted >= 50
    listed = list_signals("src:pg", status="completed")
    assert any(s.id == sig.id for s in listed)


def test_interleave_fast_forwards_signal_from_dest_open(tmp_path, monkeypatch) -> None:
    """Dest Open last_pk seeks the chunk SELECT past dest-closed keys."""
    from services.cdc_incremental_runner import interleave_incremental_snapshot

    monkeypatch.setattr(snap_mod, "_PATH", str(tmp_path / "signals.json"))
    monkeypatch.setattr(snap_mod, "_DATA_DIR", str(tmp_path))
    sig = request_incremental_snapshot("src:pg", "orders", primary_key="id", chunk_size=10)
    seen: list[str] = []

    def fetch(signal):
        seen.append(str(signal.last_pk or ""))
        return [{"id": "z", "v": "new"}], "z", True

    batches = list(
        interleave_incremental_snapshot(
            "src:pg",
            table="orders",
            fetch_chunk=fetch,
            dest_resume={"signal_id": sig.id, "last_pk": "m"},
        )
    )
    assert seen == ["m"]
    assert len(batches) == 1
    assert batches[0].inserts[0]["id"] == "z"
    claimed = claim_next_signal("src:pg", table="orders")
    assert claimed is None or claimed.last_pk == "z"


def test_debezium_envelope_parse() -> None:
    from connectors.kafka_debezium_bridge import debezium_to_row, parse_debezium_envelope

    env = {
        "payload": {
            "op": "u",
            "before": {"id": 1, "n": "a"},
            "after": {"id": 1, "n": "b"},
            "source": {"table": "orders", "ts_ms": 123, "lsn": 99},
        }
    }
    change = parse_debezium_envelope(env)
    assert change is not None
    assert change.op == "u"
    assert change.table == "orders"
    row = debezium_to_row(change)
    assert row is not None
    assert row["n"] == "b"
    assert row["__op"] == "u"

    tomb = parse_debezium_envelope({"op": "d", "before": {"id": 1}, "source": {"table": "orders"}})
    assert tomb is not None
    trow = debezium_to_row(tomb)
    assert trow is not None
    assert trow.get("__deleted") is True


def test_when_needed_gap_dest_keyed_is_incremental_not_blocking() -> None:
    """Healthcare/banking cutover: dest already keyed → DDD-3, not a table lock."""
    from services.cdc_snapshot_mode import (
        KIND_BLOCKING,
        KIND_INCREMENTAL,
        SnapshotMode,
        adapter_supports_incremental_interleave,
        classify_snapshot_plan,
    )

    blocking = classify_snapshot_plan(
        SnapshotMode.WHEN_NEEDED, watermark="lsn-old", retention_status="gap"
    )
    assert blocking["kind"] == KIND_BLOCKING
    assert blocking["run_snapshot"] is True

    plan = classify_snapshot_plan(
        SnapshotMode.WHEN_NEEDED,
        watermark="lsn-old",
        retention_status="gap",
        dest_already_keyed=True,
        incremental_capable=True,
    )
    assert plan["kind"] == KIND_INCREMENTAL
    assert plan["run_snapshot"] is False
    assert plan["run_stream"] is True
    assert plan["lost_window"] is True
    assert plan["migration_proven"] is False
    assert plan["next_action"] == "incremental_snapshot_then_stream"

    keyed_but_query_cdc = classify_snapshot_plan(
        SnapshotMode.WHEN_NEEDED,
        watermark="lsn-old",
        retention_status="gap",
        dest_already_keyed=True,
        incremental_capable=False,
    )
    assert keyed_but_query_cdc["kind"] == KIND_BLOCKING

    class _LogReader:
        source_key = "src:pg"

    class CdcEngine:
        source_key = "src:query"

    assert adapter_supports_incremental_interleave(_LogReader()) is True
    assert adapter_supports_incremental_interleave(CdcEngine()) is False
    assert adapter_supports_incremental_interleave(None) is False


def test_enqueue_gap_recovery_skips_in_flight_signal(tmp_path, monkeypatch) -> None:
    from services.cdc_incremental_snapshot import enqueue_gap_recovery_snapshots

    monkeypatch.setattr(snap_mod, "_PATH", str(tmp_path / "signals.json"))
    monkeypatch.setattr(snap_mod, "_DATA_DIR", str(tmp_path))
    first = enqueue_gap_recovery_snapshots("src:pg", [("orders", "id")])
    assert len(first) == 1
    again = enqueue_gap_recovery_snapshots("src:pg", [("orders", "id"), ("lines", "id")])
    assert [s.table for s in again] == ["lines"]


def test_measure_dest_already_keyed_unmeasured_is_false(monkeypatch) -> None:
    from services.cdc_snapshot_mode import measure_dest_already_keyed

    monkeypatch.setattr(
        "services.dest_precount.destination_row_count", lambda *_a, **_k: None
    )
    assert (
        measure_dest_already_keyed(
            "postgresql", {}, [("orders", ["id"])], schema="public"
        )
        is False
    )

    monkeypatch.setattr(
        "services.dest_precount.destination_row_count", lambda *_a, **_k: 3
    )
    monkeypatch.setattr(
        "services.dest_precount.destination_key_list",
        lambda *_a, **_k: [("1",), ("2",), ("3",)],
    )
    assert (
        measure_dest_already_keyed(
            "postgresql", {}, [("orders", ["id"])], schema="public"
        )
        is True
    )
