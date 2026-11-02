"""Source-row spool — no expanded matrix list, DF_MISSING survives JSONL."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.object_store_common import serialize_object_store_body  # noqa: E402
from connectors.object_store_materialize import materialize_object_store_export  # noqa: E402
from connectors.source_row_spool import (  # noqa: E402
    SourceRowSpool,
    matrix_row_from_record,
    resolve_source_spill_max,
)
from services.value_serializer import DF_MISSING_SENTINEL  # noqa: E402


def _common(**overrides):
    base = dict(
        headers=["id", "note"],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "note", "target": "note"},
        ],
        target_cols=["id", "note"],
        column_types={"id": "TEXT", "note": "TEXT"},
        dest_types={"id": "TEXT", "note": "TEXT"},
        error_policy="quarantine",
        dest_kind="s3",
        dialect_label="S3",
        spill_max_size=8 * 1024 * 1024,
        dest_db_type="s3",
    )
    base.update(overrides)
    return base


def test_resolve_source_spill_max_floors_zero():
    assert resolve_source_spill_max({}) == 8 * 1024 * 1024
    assert resolve_source_spill_max({"source_spill_max": 0}) == 1
    assert resolve_source_spill_max({"source_spill_max": 64}) == 64


def test_matrix_row_absent_key_is_missing_not_null():
    row = matrix_row_from_record({"id": "1"}, ["id", "note"])
    assert row[0] == "1"
    assert row[1] == DF_MISSING_SENTINEL


def test_spool_missing_survives_jsonl_round_trip():
    spool = SourceRowSpool(spill_max_size=10_000)
    try:
        spool.ingest_matrix(
            ["id", "note"],
            [["1", DF_MISSING_SENTINEL], ["2", None]],
        )
        assert spool.row_count == 2
        bundles = list(spool.iter_bundles(10))
        assert bundles[0][1][0][1] == DF_MISSING_SENTINEL
        assert bundles[0][1][1][1] is None
    finally:
        spool.close()


def test_spool_rolls_to_disk_above_spill_max():
    spool = SourceRowSpool(spill_max_size=32)
    try:
        spool.ingest_matrix(
            ["id", "note"],
            [[str(i), "n" * 20] for i in range(40)],
        )
        assert spool.spilled is True
        assert spool.row_count == 40
        starts = [start for start, _ in spool.iter_bundles(7)]
        assert starts[0] == 1
        assert starts[1] == 8
    finally:
        spool.close()


def test_spool_explode_row_count_without_expanded_list():
    from services.json_intelligence import ARRAY_POLICY_EXPLODE

    mappings = [
        {"source": "id", "target": "id"},
        {"source": "tags", "target": "tags", "struct_policy": ARRAY_POLICY_EXPLODE},
    ]
    spool = SourceRowSpool(spill_max_size=10_000)
    try:
        spool.ingest_records(
            ["id", "tags"],
            [{"id": "1", "tags": '["a","b","c"]'}],
            mappings,
        )
        assert spool.row_count == 3
        sizes = [len(bundle) for _, bundle in spool.iter_bundles(2)]
        assert sizes == [2, 1]
        assert "tags_elem" in spool.headers
    finally:
        spool.close()


def test_materialize_from_records_matches_data_rows_bytes():
    records = [{"id": "1", "note": "ok"}, {"id": "2", "note": "next"}]
    rows = [["1", "ok"], ["2", "next"]]
    from_records = materialize_object_store_export(
        key="exports/a.jsonl",
        data_rows=[],
        records=records,
        batch_size=1,
        **_common(),
    )
    from_rows = materialize_object_store_export(
        key="exports/a.jsonl",
        data_rows=rows,
        batch_size=1,
        **_common(),
    )
    try:
        assert from_records.export.read_all() == from_rows.export.read_all()
        expected, _ = serialize_object_store_body(
            key="exports/a.jsonl",
            mapped_rows=[("1", "ok"), ("2", "next")],
            target_cols=["id", "note"],
            dest_types={"id": "TEXT", "note": "TEXT"},
        )
        assert from_records.export.read_all() == expected
    finally:
        from_records.export.close()
        from_rows.export.close()


def test_materialize_explode_via_records_writes_one_row_per_element():
    from services.json_intelligence import ARRAY_POLICY_EXPLODE

    mappings = [
        {"source": "id", "target": "id"},
        {"source": "tags", "target": "tags", "struct_policy": ARRAY_POLICY_EXPLODE},
        {"source": "tags_elem", "target": "tag"},
    ]
    mat = materialize_object_store_export(
        key="exports/a.jsonl",
        headers=["id", "tags"],
        records=[{"id": "1", "tags": '["a","b"]'}],
        mappings=mappings,
        target_cols=["id", "tag"],
        column_types={"id": "TEXT", "tag": "TEXT"},
        dest_types={"id": "TEXT", "tag": "TEXT"},
        error_policy="quarantine",
        dest_kind="s3",
        dialect_label="S3",
        spill_max_size=8 * 1024 * 1024,
        dest_db_type="s3",
        batch_size=1,
    )
    assert mat.abort_error is None
    assert mat.rows_written == 2
    try:
        body = mat.export.read_all().decode("utf-8")
    finally:
        mat.export.close()
    assert '"tag": "a"' in body
    assert '"tag": "b"' in body
