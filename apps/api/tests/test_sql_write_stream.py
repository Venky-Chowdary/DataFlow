"""SQL mapped-image stream — one bundle in RAM, fail-closed, checksum-identical.

Proves the warehouse write path no longer concatenates every accepted tuple
before COPY/INSERT. Peak finished-bundle size is the materialize batch.
Fail/FAIL_JOB collect every reject and do not expose a write callback.
Checksum of streamed accepted rows matches a full-list FingerprintAccumulator.
In-bundle last-write-wins; cross-bundle duplicate PKs both survive (ON CONFLICT).
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.postgresql_writer import (  # noqa: E402
    _pg_materialize_mapped_batch,
    iter_pg_finished_bundles,
)
from connectors.sql_write_materialize import (  # noqa: E402
    SqlWriteAccumulator,
    dest_types_signature,
    finish_sql_mapped_bundle,
    iter_mapped_bundles_from_source,
)
from connectors.writer_common import row_checksum  # noqa: E402
from services.reconciliation import _iter_fingerprints  # noqa: E402


def _text_mappings():
    return [
        {"source": "id", "target": "id"},
        {"source": "note", "target": "note"},
    ]


def _map_kwargs(**overrides):
    base = dict(
        headers=["id", "note"],
        mappings=_text_mappings(),
        target_cols=["id", "note"],
        column_types={"id": "TEXT", "note": "TEXT"},
        dest_types={"id": "TEXT", "note": "TEXT"},
        error_policy="quarantine",
        dest_kind="postgresql",
        preserve_case=True,
        batch_size=2,
    )
    base.update(overrides)
    return base


def test_iter_mapped_bundles_never_holds_more_than_batch():
    rows = [[str(i), f"n{i}"] for i in range(5)]
    sizes: list[int] = []
    for bundle in iter_mapped_bundles_from_source(data_rows=rows, **_map_kwargs()):
        sizes.append(len(bundle.mapped_rows))
        assert len(bundle.mapped_rows) <= 2
        del bundle
    assert sizes == [2, 2, 1]


def test_finish_in_bundle_dedupe_last_wins_not_cross_bundle():
    rows = [["1", "first"], ["1", "second"], ["1", "third"]]
    bundles = list(
        iter_mapped_bundles_from_source(
            data_rows=rows,
            **_map_kwargs(batch_size=2),
        )
    )
    finished = [
        finish_sql_mapped_bundle(
            bundle,
            target_cols=["id", "note"],
            target_types=["TEXT", "TEXT"],
            policy="quarantine",
            dialect_label="PostgreSQL",
            dest_db="postgresql",
            write_mode="upsert",
            conflict_columns=["id"],
        )
        for bundle in bundles
    ]
    # Bundle 0: last-wins keeps "second". Bundle 1: "third" survives — no
    # forward seen-set. Dest ON CONFLICT applies the later image.
    assert [row[1] for row in finished[0].dense_rows] == ["second"]
    assert [row[1] for row in finished[1].dense_rows] == ["third"]


def test_accumulator_checksum_matches_full_accepted_image():
    rows = [[str(i), f"n{i}"] for i in range(6)]
    acc = SqlWriteAccumulator(
        target_cols=["id", "note"],
        dest_db_type="postgresql",
        dest_types={"id": "TEXT", "note": "TEXT"},
        dialect_label="PostgreSQL",
    )
    accepted: list[tuple] = []
    for bundle in iter_mapped_bundles_from_source(data_rows=rows, **_map_kwargs()):
        finished = finish_sql_mapped_bundle(
            bundle,
            target_cols=["id", "note"],
            target_types=["TEXT", "TEXT"],
            policy="quarantine",
            dialect_label="PostgreSQL",
            dest_db="postgresql",
        )
        acc.note_rejects(finished.rejected_details, finished.transform_errors)
        acc.add_accepted(finished.checksum_rows)
        accepted.extend(finished.checksum_rows)
        assert len(finished.dense_rows) <= 2
        del finished
        del bundle
    assert acc.digest() == row_checksum(
        accepted,
        ["id", "note"],
        dest_db_type="postgresql",
        dest_types={"id": "TEXT", "note": "TEXT"},
    )
    assert acc.accepted_row_count == 6
    assert acc.gate8_meta()["source_row_count"] == 6
    assert len(acc.sample_rows) == 6  # under the 50-row sample cap


def test_fail_scan_collects_every_reject_and_does_not_write():
    rows = [["1", "10"], ["2", "bad"], ["3", "nope"], ["4", "40"]]
    writes: list[int] = []
    acc = SqlWriteAccumulator(
        target_cols=["id", "amount"],
        dest_db_type="postgresql",
        dest_types={"id": "INTEGER", "amount": "INTEGER"},
        dialect_label="PostgreSQL",
    )
    kwargs = _map_kwargs(
        headers=["id", "amount"],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "amount", "target": "amount"},
        ],
        target_cols=["id", "amount"],
        column_types={"id": "INTEGER", "amount": "INTEGER"},
        dest_types={"id": "INTEGER", "amount": "INTEGER"},
        error_policy="fail",
        batch_size=1,
    )
    for bundle in iter_mapped_bundles_from_source(data_rows=rows, **kwargs):
        finished = finish_sql_mapped_bundle(
            bundle,
            target_cols=["id", "amount"],
            target_types=["INTEGER", "INTEGER"],
            policy="fail",
            dialect_label="PostgreSQL",
            dest_db="postgresql",
        )
        acc.note_rejects(finished.rejected_details, finished.transform_errors)
        if acc.abort_error("fail"):
            acc.stop_writing()
        else:
            writes.append(len(finished.dense_rows))
            acc.add_accepted(finished.checksum_rows)
        del finished
    abort = acc.abort_error("fail")
    assert abort is not None
    assert "blocks partial write" in abort
    rejected_rows = sorted(
        int(d["row"]) for d in acc.rejected_details if d.get("row") is not None
    )
    assert rejected_rows == [2, 3]
    # First bundle is clean; after reject #2 we stop writing. Bundle 3 is still
    # scanned. No primary write after the abort flag.
    assert writes == [1]
    assert acc.writing is False


def test_pg_finished_bundles_peak_and_row_numbers_global():
    rows = [["1", "10"], ["2", "bad"], ["3", "30"]]
    sizes: list[int] = []
    details: list[dict] = []
    for finished in iter_pg_finished_bundles(
        headers=["id", "note"],
        data_rows=rows,
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "note", "target": "amount"},
        ],
        target_cols=["id", "amount"],
        column_types={"id": "INTEGER", "amount": "INTEGER"},
        dest_types={"id": "INTEGER", "amount": "INTEGER"},
        logical_types=["INTEGER", "INTEGER"],
        policy="quarantine",
        engine="postgresql",
        conflict_columns=None,
        write_mode="insert",
        materialize_batch=1,
    ):
        sizes.append(len(finished.dense_rows) + len(finished.sparse_rows))
        details.extend(finished.rejected_details)
        del finished
    assert max(sizes) <= 1
    rejected_rows = sorted(
        int(d["row"]) for d in details if d.get("row") is not None
    )
    assert 2 in rejected_rows


def test_pg_materialize_retain_still_matches_records_and_rows():
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "note", "target": "note"},
    ]
    common = dict(
        mappings=mappings,
        target_cols=["id", "note"],
        column_types={"id": "TEXT", "note": "TEXT"},
        dest_types={"id": "TEXT", "note": "TEXT"},
        logical_types=["TEXT", "TEXT"],
        policy="quarantine",
        engine="postgresql",
        conflict_columns=None,
        write_mode="insert",
        materialize_batch=1,
    )
    from_records = _pg_materialize_mapped_batch(
        headers=["id", "note"],
        data_rows=[],
        records=[{"id": "1", "note": "ok"}, {"id": "2", "note": "next"}],
        **common,
    )
    from_rows = _pg_materialize_mapped_batch(
        headers=["id", "note"],
        data_rows=[["1", "ok"], ["2", "next"]],
        **common,
    )
    assert from_records.mapped_rows == from_rows.mapped_rows
    assert from_records.source_row_count == 2


def test_dest_types_signature_detects_carrier_change():
    cols = ["id", "flag"]
    map_types = {"id": "INTEGER", "flag": "BOOLEAN"}
    live_types = {"id": "INTEGER", "flag": "TEXT"}
    assert dest_types_signature(map_types, cols) != dest_types_signature(live_types, cols)
    assert dest_types_signature(map_types, cols) == dest_types_signature(
        {"id": "integer", "flag": "boolean"}, cols
    )


def test_accumulator_fingerprints_match_iter_fingerprints():
    rows = [("1", "a"), ("2", "b")]
    acc = SqlWriteAccumulator(
        target_cols=["id", "note"],
        dest_db_type="postgresql",
        dest_types={"id": "TEXT", "note": "TEXT"},
    )
    acc.add_accepted(rows)
    from services.fingerprint_accumulator import FingerprintAccumulator

    full = FingerprintAccumulator()
    full.add_many(
        _iter_fingerprints(
            rows,
            ["id", "note"],
            dest_db_type="postgresql",
            dest_types={"id": "TEXT", "note": "TEXT"},
        )
    )
    assert acc.digest() == full.digest()
