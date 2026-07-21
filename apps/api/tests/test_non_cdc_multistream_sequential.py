"""Non-CDC multi-stream sequential execute proofs."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from services.sync_cursor import SyncContract, resolve_selected_sync_contracts
from src.transfer.stream import run_non_cdc_multi_stream_sequential


def test_selected_multi_stream_count() -> None:
    contracts = [
        {"name": "orders", "selected": True, "sync_mode": "incremental"},
        {"name": "users", "selected": True, "sync_mode": "incremental"},
        {"name": "skip_me", "selected": False, "sync_mode": "incremental"},
    ]
    selected = resolve_selected_sync_contracts(contracts)
    assert len(selected) == 2
    assert all(c.name in {"orders", "users"} for c in selected)


def test_non_cdc_sequential_uses_per_stream_mappings_and_remaps_tables() -> None:
    source = MagicMock()
    source.format = "postgresql"
    source.kind = "database"
    source.table = "a,b"
    source.collection = ""
    destination = MagicMock()
    destination.format = "postgresql"
    destination.kind = "database"
    destination.table = "out"
    destination.collection = ""

    shared = [{"source": "id", "target": "id"}]
    stream_a = [{"source": "id", "target": "id_a"}]
    stream_b = [{"source": "id", "target": "id_b"}]
    contracts = [
        {
            "name": "a",
            "selected": True,
            "sync_mode": "incremental",
            "primary_key": "id",
            "cursor_field": "updated_at",
            "mappings": stream_a,
        },
        {
            "name": "b",
            "selected": True,
            "sync_mode": "incremental",
            "primary_key": "id",
            "cursor_field": "updated_at",
            "mappings": stream_b,
        },
    ]
    selected = [
        SyncContract(name="a", sync_mode="incremental", primary_key="id", cursor_field="updated_at"),
        SyncContract(name="b", sync_mode="incremental", primary_key="id", cursor_field="updated_at"),
    ]

    seen_maps: list[list] = []
    seen_tables: list[str] = []

    def _fake_stream(src, dest, mappings, schema, *args, **kwargs):
        seen_maps.append(list(mappings))
        seen_tables.append(str(src.table))
        single = kwargs.get("stream_contracts") or []
        assert len(single) == 1
        assert single[0]["name"] in {"a", "b"}
        assert schema == {}
        return 2, [f"STREAM {src.table}"], {"watermark": "1", "sync_mode": "incremental"}, ["id"]

    with patch(
        "src.transfer.stream.stream_database_transfer",
        side_effect=_fake_stream,
    ), patch(
        "src.transfer.stream._drop_destination_endpoint",
        return_value=False,
    ):
        rows, ddl, summary, _ = run_non_cdc_multi_stream_sequential(
            source,
            destination,
            shared,
            {"id": "INTEGER"},
            None,
            sync_mode="incremental",
            stream_contracts=contracts,
            selected=selected,
            job_id="j1",
            limit=0,
        )

    assert rows == 4
    assert seen_maps == [stream_a, stream_b]
    assert seen_tables == ["a", "b"]
    assert source.table == "a,b"  # restored
    assert destination.table == "out"
    assert summary["multi_stream"] is True
    assert summary["multi_stream_mode"] == "sequential"
    assert len(summary["streams"]) == 2
    assert summary["streams"][0]["name"] == "a"
    assert summary["streams"][1]["status"] == "completed"
    assert any("MULTI-STREAM sequential" in line for line in ddl)


def test_non_cdc_sequential_fail_fast_records_failed_stream() -> None:
    source = MagicMock()
    source.format = "postgresql"
    source.kind = "database"
    source.table = "a,b"
    source.collection = ""
    destination = MagicMock()
    destination.format = "postgresql"
    destination.kind = "database"
    destination.table = "out"
    destination.collection = ""

    selected = [
        SyncContract(name="a", sync_mode="full_refresh_overwrite"),
        SyncContract(name="b", sync_mode="full_refresh_overwrite"),
    ]
    contracts = [
        {"name": "a", "selected": True, "sync_mode": "full_refresh_overwrite"},
        {"name": "b", "selected": True, "sync_mode": "full_refresh_overwrite"},
    ]

    def _fake_stream(src, dest, mappings, schema, *args, **kwargs):
        if src.table == "b":
            raise RuntimeError("dest write failed")
        return 1, [], {}, ["id"]

    with patch(
        "src.transfer.stream.stream_database_transfer",
        side_effect=_fake_stream,
    ), patch(
        "src.transfer.stream._drop_destination_endpoint",
        return_value=True,
    ):
        try:
            run_non_cdc_multi_stream_sequential(
                source,
                destination,
                [],
                {},
                None,
                sync_mode="full_refresh_overwrite",
                stream_contracts=contracts,
                selected=selected,
                job_id="j1",
            )
            raised = False
        except RuntimeError as exc:
            raised = True
            assert "dest write failed" in str(exc)

    assert raised
    assert source.table == "a,b"
