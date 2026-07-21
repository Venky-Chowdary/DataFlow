"""Unit test: multi-stream CDC prefers per-stream mappings from contracts."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from services.sync_cursor import SyncContract
from src.transfer.cdc_transfer import _run_cdc_multi_stream


def test_multi_stream_uses_per_stream_mappings() -> None:
    source = MagicMock()
    source.format = "postgresql"
    source.table = "a,b"
    source.collection = ""
    destination = MagicMock()
    destination.format = "postgresql"
    destination.table = "out"
    destination.collection = ""

    shared = [{"source": "id", "target": "id"}]
    stream_a = [{"source": "id", "target": "id_a"}, {"source": "x", "target": "x"}]
    stream_b = [{"source": "id", "target": "id_b"}]
    contracts = [
        {"name": "a", "selected": True, "sync_mode": "cdc", "primary_key": "id", "mappings": stream_a},
        {"name": "b", "selected": True, "sync_mode": "cdc", "primary_key": "id", "mappings": stream_b},
    ]
    selected = [
        SyncContract(name="a", sync_mode="cdc", primary_key="id"),
        SyncContract(name="b", sync_mode="cdc", primary_key="id"),
    ]

    seen: list[list] = []

    def _fake_single(src, dest, mappings, *args, **kwargs):
        seen.append(list(mappings))
        return 1, [], {"cdc": {}}, ["id"]

    with patch("src.transfer.cdc_transfer._run_cdc_single_stream", side_effect=_fake_single), \
         patch(
             "src.transfer.cdc_transfer._run_cdc_shared_multi_table",
             side_effect=RuntimeError("force sequential path for mapping unit test"),
         ):
        _run_cdc_multi_stream(
            source,
            destination,
            shared,
            {},
            None,
            sync_mode="cdc",
            stream_contracts=contracts,
            selected=selected,
            job_id="j1",
            checkpoint=None,
            checkpoint_service=None,
            backfill_new_fields=False,
            validation_mode="strict",
            limit=0,
        )

    assert len(seen) == 2
    assert seen[0] == stream_a
    assert seen[1] == stream_b
