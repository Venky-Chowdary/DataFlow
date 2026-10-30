"""Incremental / CDC leftover MERGE is a no-op until overwrite proves S.

``100%`` is not claimed here. Complete overwrite leftover MERGE stays measured
on the row-conservation sqlite fixture.
"""

from __future__ import annotations

import sqlite3
import sys
import uuid
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.dest_precount import destination_row_count
from services.row_conservation import apply_inferred_leftover_deletes
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _seed(path: Path, table: str, rows: list[tuple[int, str]]) -> None:
    con = sqlite3.connect(path)
    try:
        con.execute(f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY, label TEXT)')
        con.executemany(f'INSERT INTO "{table}" VALUES (?, ?)', rows)
        con.commit()
    finally:
        con.close()


def test_cdc_complete_snapshot_flag_still_noops_leftover_merge(tmp_path: Path) -> None:
    path = tmp_path / "cdc_leftover.db"
    _seed(path, "items", [(1, "a"), (2, "b"), (3, "c"), (99, "ghost")])
    cfg = {"database": str(path)}
    deleted = apply_inferred_leftover_deletes(
        db_type="sqlite",
        cfg=cfg,
        schema="",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,), (3,)],
        complete_snapshot=True,
        sync_mode="cdc",
    )
    assert deleted is None
    assert destination_row_count("sqlite", cfg, schema="", table_name="items") == 4

    incremental = apply_inferred_leftover_deletes(
        db_type="sqlite",
        cfg=cfg,
        schema="",
        table_name="items",
        key_columns=["id"],
        keys=[(1,)],
        complete_snapshot=True,
        sync_mode="incremental_deduped",
    )
    assert incremental is None
    assert destination_row_count("sqlite", cfg, schema="", table_name="items") == 4


def test_incremental_execute_does_not_delete_dest_leftovers(tmp_path: Path) -> None:
    src_t = "inc_src_" + uuid.uuid4().hex[:8]
    dst_t = "inc_dst_" + uuid.uuid4().hex[:8]
    src = tmp_path / "inc_src.db"
    dst = tmp_path / "inc_dst.db"
    _seed(src, src_t, [(1, "a"), (2, "b")])
    _seed(dst, dst_t, [(1, "a"), (2, "b"), (99, "ghost")])
    maps = [
        {"source": "id", "target": "id", "confidence": 0.99},
        {"source": "label", "target": "label", "confidence": 0.99},
    ]
    result = UniversalTransferEngine().execute_tracked(
        TransferRequest(
            source=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(src),
                table=src_t,
                connection_string=f"sqlite:///{src}",
                ssl=False,
            ),
            destination=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(dst),
                table=dst_t,
                connection_string=f"sqlite:///{dst}",
                ssl=False,
            ),
            mappings=maps,
            sync_mode="incremental_deduped",
            stream_contracts=[
                {
                    "name": src_t,
                    "sync_mode": "incremental_deduped",
                    "cursor_field": "id",
                    "cursor_semantics": "monotonic_sequence",
                    "primary_key": "id",
                    "selected": True,
                }
            ],
            skip_preflight=False,
            validation_mode="balanced",
        ),
        uuid.uuid4().hex[:24],
    )
    assert result.success, result.error
    dest_n = destination_row_count(
        "sqlite", {"database": str(dst)}, schema="", table_name=dst_t
    )
    assert dest_n == 3, f"incremental leftover MERGE deleted dest keys: COUNT={dest_n}"
    leftover = apply_inferred_leftover_deletes(
        db_type="sqlite",
        cfg={"database": str(dst)},
        schema="",
        table_name=dst_t,
        key_columns=["id"],
        keys=[(1,), (2,)],
        complete_snapshot=False,
        sync_mode="incremental_deduped",
    )
    assert leftover is None
    assert destination_row_count(
        "sqlite", {"database": str(dst)}, schema="", table_name=dst_t
    ) == 3


def test_overwrite_complete_snapshot_still_leftover_merges(tmp_path: Path) -> None:
    """Complete overwrite leftover MERGE is not this no-op. Incremental is."""
    path = tmp_path / "ow_leftover.db"
    _seed(path, "items", [(1, "a"), (2, "b"), (99, "ghost")])
    cfg = {"database": str(path)}
    deleted = apply_inferred_leftover_deletes(
        db_type="sqlite",
        cfg=cfg,
        schema="",
        table_name="items",
        key_columns=["id"],
        keys=[(1,), (2,)],
        complete_snapshot=True,
        sync_mode="full_refresh_overwrite",
    )
    assert deleted == 1
    assert destination_row_count("sqlite", cfg, schema="", table_name="items") == 2


def test_incremental_batch_scoped_verify_still_catches_tampered_batch_row(
    tmp_path: Path,
) -> None:
    """X-OW1: batch-scoping must not disable value proof.

    A pre-existing row outside the batch (id=99) is legal and must survive;
    a *batch* row whose values were tampered after the write must still fail
    reconciliation — the ladder grades the written keys, not the whole table.
    """
    src_t = "inc_src_" + uuid.uuid4().hex[:8]
    dst_t = "inc_dst_" + uuid.uuid4().hex[:8]
    src = tmp_path / "inc_src.db"
    dst = tmp_path / "inc_dst.db"
    _seed(src, src_t, [(1, "a"), (2, "b")])
    _seed(dst, dst_t, [(1, "a"), (2, "b"), (99, "ghost")])
    maps = [
        {"source": "id", "target": "id", "confidence": 0.99},
        {"source": "label", "target": "label", "confidence": 0.99},
    ]
    request = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(src),
            table=src_t,
            connection_string=f"sqlite:///{src}",
            ssl=False,
        ),
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(dst),
            table=dst_t,
            connection_string=f"sqlite:///{dst}",
            ssl=False,
        ),
        mappings=maps,
        sync_mode="incremental_deduped",
        stream_contracts=[
            {
                "name": src_t,
                "sync_mode": "incremental_deduped",
                "cursor_field": "id",
                "cursor_semantics": "monotonic_sequence",
                "primary_key": "id",
                "selected": True,
            }
        ],
        skip_preflight=False,
        validation_mode="balanced",
    )
    cfg = {"database": str(dst)}

    clean = UniversalTransferEngine().execute_tracked(
        request, uuid.uuid4().hex[:24]
    )
    assert clean.success, clean.error
    assert destination_row_count("sqlite", cfg, schema="", table_name=dst_t) == 3

    # Tamper a batch row after the accepted run and re-run the same batch:
    # the keyed re-scope must still see the divergence and fail closed.
    con = sqlite3.connect(dst)
    try:
        con.execute(f'UPDATE "{dst_t}" SET label=? WHERE id=1', ("TAMPERED",))
        con.commit()
    finally:
        con.close()

    # D188: the re-run is a quiet poll — row 1 is outside its cursor window.
    # The divergence is measured and named as out-of-window drift (with the
    # destination-only ghost), not hidden and not this run's failure.
    tampered = UniversalTransferEngine().execute_tracked(
        request, uuid.uuid4().hex[:24]
    )
    assert tampered.success, tampered.error
    drift = (tampered.reconciliation or {}).get("out_of_window_drift") or {}
    assert drift.get("changed") == 1 and drift.get("destination_only") == 1, drift
    assert drift.get("sample_keys") == ["1", "99"], drift
    assert "Out-of-window drift" in str(tampered.reconciliation.get("message"))

    # Operators who treat any divergence as fatal opt back into fail-closed.
    import os

    os.environ["DATAFLOW_INCREMENTAL_DRIFT_FAILS"] = "1"
    try:
        strict = UniversalTransferEngine().execute_tracked(
            request, uuid.uuid4().hex[:24]
        )
    finally:
        os.environ.pop("DATAFLOW_INCREMENTAL_DRIFT_FAILS", None)
    assert not strict.success
    # Either way the ghost and the tampered row are left for the operator,
    # never silently deleted or overwritten back.
    assert destination_row_count("sqlite", cfg, schema="", table_name=dst_t) == 3
    con = sqlite3.connect(dst)
    try:
        assert con.execute(
            f'SELECT label FROM "{dst_t}" WHERE id=1'
        ).fetchone()[0] == "TAMPERED"
    finally:
        con.close()
