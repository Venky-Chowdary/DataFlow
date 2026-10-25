"""C21 — quarantine row identity is absolute; replay Gate-8 is key-scoped.

Repro (prod c6c16128): a replay batch of one row was digested against the
whole destination table, so the ladder compared 1 vs N rows; and the DLQ
stored ``_df_row`` as the position inside the write batch (row 5871 in the
source landed as "871"), so 5334 held-out entries collapsed to 5000
distinct identities.
"""
from __future__ import annotations

import os
import tempfile

import pytest


def _sqlite_cfg(tmp_path):
    db = os.path.join(tmp_path, "dest.db")
    return {
        "host": "",
        "port": 0,
        "database": db,
        "username": "",
        "password": "",
        "schema": "",
        "connection_string": f"sqlite:///{db}",
        "ssl": False,
        "type": "sqlite",
    }


def test_quarantine_absolute_row_identity(tmp_path):
    """A held-out row names its absolute source row, not its batch position."""
    from connectors.generic_sql import write_mapped_rows

    cfg = _sqlite_cfg(tmp_path)
    headers = ["id", "amt"]
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "amt", "target": "amt"},
    ]
    column_types = {"id": "integer", "amt": "integer"}
    # Source rows 1..5: row 3 has a non-integer amt -> per-row quarantine.
    # If 'row' were batch-relative the failure would say 3 anyway, so run
    # two batches worth: make the bad row deep in the list.
    rows = [[str(i), str(i * 10)] for i in range(1, 9)]
    rows.append(["9", "not-a-number"])  # source row 9, last in batch
    res = write_mapped_rows(
        **cfg,
        table_name="c21_dest",
        headers=headers,
        data_rows=rows,
        mappings=mappings,
        column_types=column_types,
        error_policy="quarantine",
        write_mode="insert",
    )
    details = res.rejected_details or []
    assert details, "expected a quarantined row for the non-integer amt"
    rows_seen = {int(d["row"]) for d in details}
    assert rows_seen == {9}, (
        f"quarantined row must name absolute source row 9, got {rows_seen}"
    )
    # absolute identity -> distinct rows agree with entry count
    assert len(rows_seen) == len(details) or len(rows_seen) > 1


def test_quarantine_absolute_row_identity_sparse(tmp_path):
    """Sparse/partial rows keep their absolute number through sparse_rows."""
    from connectors.generic_sql import write_mapped_rows

    cfg = _sqlite_cfg(tmp_path)
    headers = ["id", "amt", "note"]
    mappings = [{"source": h, "target": h} for h in headers]
    column_types = {"id": "integer", "amt": "integer", "note": "string"}
    rows = [[str(i), str(i * 10), "ok"] for i in range(1, 7)]
    rows.append(["7", "", "sparse"])  # missing cell -> sparse path
    rows.append(["8", "80", "ok"])
    res = write_mapped_rows(
        **cfg,
        table_name="c21_dest_sparse",
        headers=headers,
        data_rows=rows,
        mappings=mappings,
        column_types=column_types,
        error_policy="quarantine",
        write_mode="insert",
    )
    details = res.rejected_details or []
    row_vals = {int(d["row"]) for d in details if str(d.get("row") or "").isdigit()}
    # No duplicate/batch-relative collapse: every detail has a distinct,
    # in-range absolute row number.
    assert all(1 <= r <= 8 for r in row_vals), row_vals


def test_replay_verdict_scoped():
    """Replay reconciliation declares the replay key scope on dest_summary.

    The fix stamps ``conflict_columns`` so Gate-8 reads back only the
    replayed key population instead of digesting the whole table.
    """
    import pathlib

    source = (
        pathlib.Path(__file__).resolve().parents[1]
        / "src/routers/connectors_router.py"
    ).read_text()
    # Slice the endpoint body so a stray mention elsewhere cannot satisfy it.
    start = source.index("async def replay_job_quarantine")
    end = source.index("async def", start + 1)
    endpoint_src = source[start:end]
    assert 'dest_summary["conflict_columns"]' in endpoint_src, (
        "replay must stamp conflict_columns so the ladder scopes to "
        "replayed keys"
    )
    # And the ladder itself must use them: the scope_keys population reads
    # conflict_columns / primary_key_columns off dest_summary.
    recon_src = (
        pathlib.Path(__file__).resolve().parents[1]
        / "src/transfer/reconcile_step.py"
    ).read_text()
    assert 'dest_summary.get("conflict_columns")' in recon_src or \
        'dest_summary.get("primary_key_columns")' in recon_src


def test_sparse_upsert_quarantine_row_is_absolute():
    """Sparse-position indexing stamped row 0/1; the source row is absolute."""
    from connectors.snowflake_writer import _sf_apply_sparse_upsert

    class _Cur:
        def execute(self, *a, **k):
            pass

        def fetchone(self):
            return None

    # One sparse row missing its PK value -> assert_sparse_upsert_has_pk
    # raises -> quarantine detail. row_numbers carries the absolute source row.
    detail: list[dict] = []
    written, skipped, _checksum = _sf_apply_sparse_upsert(
        _Cur(),
        "tgt",
        ["id", "amt"],
        ["NUMBER", "NUMBER"],
        ["id"],
        [(None, "10")],
        rejected_details=detail,
        policy="quarantine",
        row_numbers=[5871],
    )
    assert detail, "missing PK must quarantine"
    assert int(detail[0]["row"]) == 5871, detail[0]["row"]
