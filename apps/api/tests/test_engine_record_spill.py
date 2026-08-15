"""Engine-level record spill — dict list released, spool is the write source."""

from __future__ import annotations

import sys
from pathlib import Path
_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.engine_record_spill import (  # noqa: E402
    ENGINE_SPILL_SUMMARY_KEY,
    MIRROR_PK_SUMMARY_KEY,
    spill_engine_write_records,
    spool_write_kinds,
)
from connectors.sql_write_materialize import build_mapped_rows_from_source  # noqa: E402
from connectors.source_row_spool import SourceRowSpool  # noqa: E402
from services.json_intelligence import ARRAY_POLICY_EXPLODE  # noqa: E402
from services.value_serializer import DF_MISSING_SENTINEL  # noqa: E402
from src.transfer.reconcile_step import _compute_source_checksum  # noqa: E402


def test_spool_write_kinds_include_sql_and_object_store():
    kinds = spool_write_kinds()
    assert "postgresql" in kinds
    assert "s3" in kinds
    assert "sqlite" in kinds


def test_spill_clears_records_and_preserves_missing():
    records = [
        {"id": "1", "note": DF_MISSING_SENTINEL},
        {"id": "2", "note": "ok"},
    ]
    spill = spill_engine_write_records(
        records, ["id", "note"], [{"source": "id", "target": "id"}], extra={}
    )
    try:
        assert records == []
        assert spill.unexpanded_row_count == 2
        assert spill.source_row_count == 2
        bundles = list(spill.spool.iter_bundles(10))
        assert bundles[0][1][0][1] == DF_MISSING_SENTINEL
    finally:
        spill.close()


def test_spill_without_clear_keeps_caller_list():
    records = [{"id": "1"}]
    spill = spill_engine_write_records(
        records, ["id"], None, extra={}, clear_records=False
    )
    try:
        assert records == [{"id": "1"}]
        assert spill.source_row_count == 1
    finally:
        spill.close()


def test_spill_explode_row_count_and_mirror_keys():
    records = [{"id": "1", "tags": '["a","b"]'}]
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "tags", "target": "tags", "struct_policy": ARRAY_POLICY_EXPLODE},
    ]
    spill = spill_engine_write_records(
        records,
        ["id", "tags"],
        mappings,
        extra={},
        collect_pk_sources=["id"],
    )
    try:
        assert records == []
        assert spill.unexpanded_row_count == 1
        assert spill.source_row_count == 2
        assert spill.mirror_pk_tuples == [("1",)]
    finally:
        spill.close()


def test_writer_does_not_reingest_when_spool_provided(monkeypatch):
    ingest_calls = {"n": 0}
    real_ingest = SourceRowSpool.ingest_records

    def _counted(self, columns, records, mappings=None):
        ingest_calls["n"] += 1
        return real_ingest(self, columns, records, mappings)

    monkeypatch.setattr(SourceRowSpool, "ingest_records", _counted)
    records = [{"id": "1", "note": "a"}, {"id": "2", "note": "b"}]
    spill = spill_engine_write_records(
        records,
        ["id", "note"],
        [{"source": "id", "target": "id"}, {"source": "note", "target": "note"}],
        extra={},
    )
    try:
        assert ingest_calls["n"] == 1
        mapped = build_mapped_rows_from_source(
            headers=["id", "note"],
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
            source_spool=spill.spool,
            batch_size=1,
        )
        assert ingest_calls["n"] == 1
        assert mapped.source_row_count == 2
        assert [row[0] for row in mapped.mapped_rows] == ["1", "2"]
    finally:
        spill.close()


def test_gate8_spool_checksum_matches_records():
    records = [{"id": "1", "note": "a"}, {"id": "2", "note": "b"}]
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "note", "target": "note"},
    ]
    columns = ["id", "note"]
    from_records, provenance_rows = _compute_source_checksum(
        records,
        columns,
        mappings,
        {"id": "TEXT", "note": "TEXT"},
        "",
        dest_db_type="postgresql",
        dest_types={"id": "TEXT", "note": "TEXT"},
    )
    spill = spill_engine_write_records(list(records), columns, mappings, extra={})
    try:
        from_spool, provenance_spool = _compute_source_checksum(
            [],
            columns,
            mappings,
            {"id": "TEXT", "note": "TEXT"},
            "should-not-use",
            dest_db_type="postgresql",
            dest_types={"id": "TEXT", "note": "TEXT"},
            source_spool=spill.spool,
        )
    finally:
        spill.close()
    assert provenance_rows == "remapped_source_rows"
    assert provenance_spool == "remapped_source_rows"
    assert from_spool == from_records
    assert from_spool != "should-not-use"


def test_write_destination_releases_records_for_sqlite(tmp_path):
    from src.transfer.adapters import write_destination_database
    from src.transfer.models import EndpointConfig

    db_path = str(tmp_path / "spill.db")
    endpoint = EndpointConfig(
        kind="database",
        format="sqlite",
        database=db_path,
        table="t_spill",
    )
    records = [{"id": "1", "note": "a"}, {"id": "2", "note": "b"}]
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "note", "target": "note"},
    ]
    written, _ddl, summary = write_destination_database(
        endpoint,
        records,
        ["id", "note"],
        {"id": "TEXT", "note": "TEXT"},
        mappings,
        write_mode="insert",
        release_records=True,
        retain_engine_spill=True,
    )
    spill = summary.pop(ENGINE_SPILL_SUMMARY_KEY, None)
    try:
        assert written == 2
        assert records == []
        assert summary["engine_record_spill"]["unexpanded_row_count"] == 2
        assert spill is not None
        assert spill.source_row_count == 2
    finally:
        if spill is not None:
            spill.close()


def test_write_destination_default_keeps_caller_records(tmp_path):
    from src.transfer.adapters import write_destination_database
    from src.transfer.models import EndpointConfig

    db_path = str(tmp_path / "keep.db")
    endpoint = EndpointConfig(
        kind="database",
        format="sqlite",
        database=db_path,
        table="t_keep",
    )
    records = [{"id": "1", "note": "a"}]
    written, _ddl, summary = write_destination_database(
        endpoint,
        records,
        ["id", "note"],
        {"id": "TEXT", "note": "TEXT"},
        [{"source": "id", "target": "id"}, {"source": "note", "target": "note"}],
        write_mode="insert",
    )
    assert written == 1
    assert records == [{"id": "1", "note": "a"}]
    assert ENGINE_SPILL_SUMMARY_KEY not in summary
    assert "engine_record_spill" in summary


def test_mirror_keys_collected_when_requested():
    records = [{"id": "10", "note": "x"}, {"id": "11", "note": "y"}]
    spill = spill_engine_write_records(
        records,
        ["id", "note"],
        [{"source": "id", "target": "id"}],
        extra={},
        collect_pk_sources=["id"],
    )
    try:
        assert spill.mirror_pk_tuples == [("10",), ("11",)]
        assert MIRROR_PK_SUMMARY_KEY  # constant exists for engine handoff
    finally:
        spill.close()


def test_object_store_materialize_uses_engine_spool(monkeypatch):
    from connectors.object_store_materialize import materialize_object_store_export

    ingest_calls = {"n": 0}
    real = SourceRowSpool.ingest_records

    def _counted(self, columns, records, mappings=None):
        ingest_calls["n"] += 1
        return real(self, columns, records, mappings)

    monkeypatch.setattr(SourceRowSpool, "ingest_records", _counted)
    records = [{"id": "1", "note": "z"}]
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "note", "target": "note"},
    ]
    spill = spill_engine_write_records(records, ["id", "note"], mappings, extra={})
    try:
        assert ingest_calls["n"] == 1
        mat = materialize_object_store_export(
            key="exports/a.jsonl",
            headers=["id", "note"],
            mappings=mappings,
            target_cols=["id", "note"],
            column_types={"id": "TEXT", "note": "TEXT"},
            dest_types={"id": "TEXT", "note": "TEXT"},
            error_policy="quarantine",
            dest_kind="s3",
            dialect_label="S3",
            spill_max_size=8 * 1024 * 1024,
            dest_db_type="s3",
            source_spool=spill.spool,
            batch_size=1,
        )
        assert ingest_calls["n"] == 1
        assert mat.abort_error is None
        assert mat.rows_written == 1
        mat.export.close()
    finally:
        spill.close()
