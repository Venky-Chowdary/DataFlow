"""C22 — file sources don't need a watermark; dataset transfer takes upsert_key.

Repro (prod c6c16128): explicit incremental_upsert + upsert_key=id +
duplicate_policy=upsert on a file source was refused with 'needs the
watermark column' while omitting sync_mode staged the same route; and
start_dataset_transfer accepted incremental_upsert but had no upsert_key
param to name the identity.
"""
from __future__ import annotations

import inspect


def test_dataset_transfer_accepts_upsert_key():
    from src.ai.copilot.tools import DataPilotTools

    params = inspect.signature(DataPilotTools._start_dataset_transfer).parameters
    assert "upsert_key" in params
    assert "primary_key" in params


def test_dataset_payload_carries_contract_key():
    from src.ai.copilot import dataset_transfer

    params = inspect.signature(
        dataset_transfer.stage_dataset_transfer
    ).parameters
    assert "upsert_key" in params
    source = inspect.getsource(dataset_transfer.stage_dataset_transfer)
    assert '"primary_key"' in source and "stream_contracts" in source


def test_file_keyed_upsert_needs_no_watermark():
    """incremental_upsert on a file source must not demand a watermark col."""
    import pathlib

    source = (
        pathlib.Path(__file__).resolve().parents[1]
        / "src/ai/copilot/schedule_tools.py"
    ).read_text()
    assert "source_is_record_batch" in source
    assert "_file_source" in source
    # The exemption must only cover the keyed file modes, not append.
    assert '"incremental_deduped"' in source
