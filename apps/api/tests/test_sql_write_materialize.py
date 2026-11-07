"""SQL/warehouse source-row spool — STRUCT explode never becomes a Python list."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.sql_write_materialize import (  # noqa: E402
    SQL_SPOOL_WRITE_KINDS,
    build_mapped_rows_from_source,
    ingest_sql_source_spool,
    resolve_sql_materialize_batch,
    sample_sql_source_values,
)
from connectors.writer_common import build_mapped_rows_with_details  # noqa: E402
from services.fingerprint_accumulator import FingerprintAccumulator  # noqa: E402
from services.json_intelligence import ARRAY_POLICY_EXPLODE  # noqa: E402
from services.reconciliation import _iter_fingerprints  # noqa: E402
from services.value_serializer import DF_MISSING_SENTINEL  # noqa: E402


def _explode_mappings():
    return [
        {"source": "id", "target": "id"},
        {"source": "tags", "target": "tags", "struct_policy": ARRAY_POLICY_EXPLODE},
        {"source": "tags_elem", "target": "tag"},
    ]


def test_resolve_sql_materialize_batch_floors_zero():
    assert resolve_sql_materialize_batch({}) == 1024
    assert resolve_sql_materialize_batch({"sql_materialize_batch": 0}) == 1
    assert resolve_sql_materialize_batch({"sql_materialize_batch": 64}) == 64
    assert resolve_sql_materialize_batch({"materialize_batch": 8}) == 8


def test_sql_spool_kinds_cover_warehouse_writers():
    for kind in (
        "postgresql",
        "redshift",
        "mysql",
        "snowflake",
        "bigquery",
        "sqlite",
        "generic_sql",
    ):
        assert kind in SQL_SPOOL_WRITE_KINDS


def test_ingest_explode_row_count_without_expanded_list():
    spool = ingest_sql_source_spool(
        headers=["id", "tags"],
        records=[{"id": "1", "tags": '["a","b","c"]'}],
        mappings=_explode_mappings(),
        spill_max=10_000,
    )
    try:
        assert spool.row_count == 3
        sizes = [len(bundle) for _, bundle in spool.iter_bundles(2)]
        assert sizes == [2, 1]
        assert "tags_elem" in spool.headers
    finally:
        spool.close()


def test_map_from_source_does_not_call_list_form_struct(monkeypatch):
    def _boom(*_args, **_kwargs):
        raise AssertionError("materialize_struct_policies must not run for SQL spool")

    monkeypatch.setattr(
        "services.json_intelligence.materialize_struct_policies", _boom
    )
    result = build_mapped_rows_from_source(
        headers=["id", "tags"],
        records=[{"id": "1", "tags": '["a","b"]'}],
        mappings=_explode_mappings(),
        target_cols=["id", "tag"],
        column_types={"id": "TEXT", "tag": "TEXT"},
        dest_types={"id": "TEXT", "tag": "TEXT"},
        error_policy="quarantine",
        dest_kind="postgresql",
        preserve_case=True,
        batch_size=1,
    )
    assert result.source_row_count == 2
    assert [row[1] for row in result.mapped_rows] == ["a", "b"]
    assert result.batch_sizes == [1, 1]


def test_map_from_records_matches_data_rows():
    records = [{"id": "1", "note": "ok"}, {"id": "2", "note": "next"}]
    rows = [["1", "ok"], ["2", "next"]]
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "note", "target": "note"},
    ]
    kwargs = dict(
        mappings=mappings,
        target_cols=["id", "note"],
        column_types={"id": "TEXT", "note": "TEXT"},
        dest_types={"id": "TEXT", "note": "TEXT"},
        error_policy="quarantine",
        dest_kind="postgresql",
        preserve_case=True,
        batch_size=1,
    )
    from_records = build_mapped_rows_from_source(
        headers=["id", "note"], records=records, **kwargs
    )
    from_rows = build_mapped_rows_from_source(
        headers=["id", "note"], data_rows=rows, **kwargs
    )
    assert from_records.mapped_rows == from_rows.mapped_rows
    assert from_records.source_row_count == 2


def test_explode_quarantine_row_numbers_are_global():
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "tags", "target": "tags", "struct_policy": ARRAY_POLICY_EXPLODE},
        {"source": "tags_elem", "target": "amount"},
    ]
    result = build_mapped_rows_from_source(
        headers=["id", "tags"],
        records=[{"id": "1", "tags": '["10","bad","30"]'}],
        mappings=mappings,
        target_cols=["id", "amount"],
        column_types={"id": "INTEGER", "amount": "INTEGER"},
        dest_types={"id": "INTEGER", "amount": "INTEGER"},
        error_policy="quarantine",
        dest_kind="postgresql",
        preserve_case=True,
        batch_size=1,
    )
    rejected_rows = sorted(
        int(d["row"]) for d in result.rejected_details if d.get("row") is not None
    )
    assert 2 in rejected_rows
    assert [row[0] for row in result.mapped_rows] == [1, 1]
    assert [row[1] for row in result.mapped_rows] == [10, 30]


def test_fail_policy_collects_every_reject_across_bundles():
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "amount", "target": "amount"},
    ]
    result = build_mapped_rows_from_source(
        headers=["id", "amount"],
        data_rows=[["1", "10"], ["2", "bad"], ["3", "nope"], ["4", "40"]],
        mappings=mappings,
        target_cols=["id", "amount"],
        column_types={"id": "INTEGER", "amount": "INTEGER"},
        dest_types={"id": "INTEGER", "amount": "INTEGER"},
        error_policy="fail",
        dest_kind="postgresql",
        preserve_case=True,
        batch_size=1,
    )
    rejected_rows = sorted(
        int(d["row"]) for d in result.rejected_details if d.get("row") is not None
    )
    assert rejected_rows == [2, 3]
    assert result.source_row_count == 4


def test_checksum_matches_full_accepted_image():
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "note", "target": "note"},
    ]
    rows = [["1", "a"], ["2", "b"], ["3", "c"]]
    bundled = build_mapped_rows_from_source(
        headers=["id", "note"],
        data_rows=rows,
        mappings=mappings,
        target_cols=["id", "note"],
        column_types={"id": "TEXT", "note": "TEXT"},
        dest_types={"id": "TEXT", "note": "TEXT"},
        error_policy="quarantine",
        dest_kind="postgresql",
        preserve_case=True,
        batch_size=1,
    )
    full, _, _ = build_mapped_rows_with_details(
        headers=["id", "note"],
        data_rows=rows,
        mappings=mappings,
        target_cols=["id", "note"],
        column_types={"id": "TEXT", "note": "TEXT"},
        dest_types={"id": "TEXT", "note": "TEXT"},
        error_policy="quarantine",
        dest_kind="postgresql",
        preserve_case=True,
        struct_already_materialized=True,
    )
    acc = FingerprintAccumulator()
    acc.add_many(
        _iter_fingerprints(
            bundled.mapped_rows,
            ["id", "note"],
            dest_db_type="postgresql",
            dest_types={"id": "TEXT", "note": "TEXT"},
        )
    )
    full_acc = FingerprintAccumulator()
    full_acc.add_many(
        _iter_fingerprints(
            full,
            ["id", "note"],
            dest_db_type="postgresql",
            dest_types={"id": "TEXT", "note": "TEXT"},
        )
    )
    assert bundled.mapped_rows == full
    assert acc.digest() == full_acc.digest()


def test_missing_sentinel_survives_sql_spool_map():
    result = build_mapped_rows_from_source(
        headers=["id", "note"],
        data_rows=[["1", DF_MISSING_SENTINEL]],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "note", "target": "note"},
        ],
        target_cols=["id", "note"],
        column_types={"id": "TEXT", "note": "TEXT"},
        dest_types={"id": "TEXT", "note": "TEXT"},
        error_policy="quarantine",
        dest_kind="postgresql",
        preserve_case=True,
        batch_size=1,
    )
    assert result.mapped_rows[0][1] == DF_MISSING_SENTINEL


def test_sample_sql_source_values_from_records_not_explode():
    samples = sample_sql_source_values(
        ["id", "tags"],
        [],
        [{"source": "id", "target": "id"}, {"source": "tags", "target": "tags"}],
        records=[{"id": "1", "tags": '["a","b"]'}],
    )
    assert samples["id"] == ["1"]
    assert samples["tags"] == ['["a","b"]']


def test_pg_materialize_records_matches_data_rows():
    from connectors.postgresql_writer import _pg_materialize_mapped_batch

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


def test_mysql_materialize_explode_row_count():
    from connectors.mysql_writer import _mysql_materialize_mapped_batch

    batch = _mysql_materialize_mapped_batch(
        headers=["id", "tags"],
        data_rows=[],
        records=[{"id": "1", "tags": '["a","b","c"]'}],
        mappings=_explode_mappings(),
        target_cols=["id", "tag"],
        column_types={"id": "TEXT", "tag": "TEXT"},
        dest_types={"id": "TEXT", "tag": "TEXT"},
        logical_types=["TEXT", "TEXT"],
        policy="quarantine",
        conflict_columns=None,
        write_mode="insert",
        materialize_batch=1,
    )
    assert batch.source_row_count == 3
    assert [row[1] for row in batch.mapped_rows] == ["a", "b", "c"]


def test_rejected_row_count_uses_expanded_source():
    from connectors.writer_common import _rejected_row_count

    assert (
        _rejected_row_count(
            [],
            [("1", "a"), ("1", "c")],
            [{"row": 2}],
            "quarantine",
            source_row_count=3,
        )
        == 1
    )
