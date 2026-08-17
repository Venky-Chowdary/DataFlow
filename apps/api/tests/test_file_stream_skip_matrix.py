"""File-stream spool destinations skip the retained records_to_matrix copy."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.engine_record_spill import (  # noqa: E402
    fingerprints_from_spool,
    spill_engine_write_records,
)
from connectors.writer_common import (  # noqa: E402
    map_rows_for_fingerprint,
    row_fingerprints,
)
from services.reconciliation import FingerprintAccumulator  # noqa: E402
from services.value_serializer import DF_MISSING_SENTINEL  # noqa: E402
from src.transfer.adapters import records_to_matrix  # noqa: E402
from src.transfer.file_stream import stream_file_to_database  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402


def _csv_bytes(rows: int = 4) -> bytes:
    lines = ["id,name"]
    for i in range(rows):
        lines.append(f"{i},row-{i}")
    return "\n".join(lines).encode()


def _digest(fingerprints) -> str:
    acc = FingerprintAccumulator()
    acc.add_many(fingerprints)
    return acc.digest()


def test_sqlite_file_stream_does_not_call_records_to_matrix(monkeypatch):
    calls = {"matrix": 0, "spool": 0}

    def _blocked(*_a, **_k):
        calls["matrix"] += 1
        raise AssertionError("records_to_matrix must not run for spool dests")

    def fake_write_batch(*args, **kwargs):
        data_rows = kwargs.get("data_rows")
        if data_rows is None and len(args) > 5:
            data_rows = args[5]
        spool = kwargs.get("source_spool")
        assert not data_rows
        assert spool is not None
        assert getattr(spool, "row_count", 0) == 4
        calls["spool"] += 1
        return 4, "checksum", {"rejected_rows": 0}

    monkeypatch.setattr("src.transfer.file_stream.records_to_matrix", _blocked)
    monkeypatch.setattr("src.transfer.file_stream._write_batch", fake_write_batch)

    dest = EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string="sqlite:///:memory:",
        table="import",
    )
    written, _ddl, summary, _cols = stream_file_to_database(
        content=_csv_bytes(4),
        filename="rows.csv",
        destination=dest,
        mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        schema={"id": "INTEGER", "name": "TEXT"},
        validation_mode="balanced",
    )
    assert written == 4
    assert calls["matrix"] == 0
    assert calls["spool"] == 1
    assert summary.get("checksum")


def test_postgresql_file_stream_skips_matrix(monkeypatch):
    seen = {"matrix": 0}

    def _blocked(*_a, **_k):
        seen["matrix"] += 1
        raise AssertionError("records_to_matrix must not run for postgresql")

    def fake_write_batch(*args, **kwargs):
        spool = kwargs.get("source_spool")
        assert spool is not None
        return int(spool.row_count), "checksum", {"rejected_rows": 0}

    monkeypatch.setattr("src.transfer.file_stream.records_to_matrix", _blocked)
    monkeypatch.setattr("src.transfer.file_stream._write_batch", fake_write_batch)

    dest = EndpointConfig(
        kind="database",
        format="postgresql",
        host="127.0.0.1",
        database="df",
        table="import",
    )
    written, _ddl, _summary, _cols = stream_file_to_database(
        content=_csv_bytes(3),
        filename="rows.csv",
        destination=dest,
        mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        schema={},
        validation_mode="balanced",
    )
    assert written == 3
    assert seen["matrix"] == 0


def test_s3_file_stream_skips_matrix(monkeypatch):
    def _blocked(*_a, **_k):
        raise AssertionError("records_to_matrix must not run for s3")

    def fake_write_batch(*args, **kwargs):
        assert kwargs.get("source_spool") is not None
        return 2, "checksum", {"rejected_rows": 0}

    monkeypatch.setattr("src.transfer.file_stream.records_to_matrix", _blocked)
    monkeypatch.setattr("src.transfer.file_stream._write_batch", fake_write_batch)

    dest = EndpointConfig(kind="object_store", format="s3", database="bucket", table="part")
    written, _ddl, _summary, _cols = stream_file_to_database(
        content=_csv_bytes(2),
        filename="rows.csv",
        destination=dest,
        mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        schema={},
        validation_mode="balanced",
    )
    assert written == 2


def test_kafka_file_stream_still_builds_matrix(monkeypatch):
    """Non-spool writers still need the matrix — that list is the write image."""
    seen = {"matrix": 0}

    real = records_to_matrix

    def _counted(records, columns):
        seen["matrix"] += 1
        return real(records, columns)

    def fake_write_batch(*args, **kwargs):
        data_rows = kwargs.get("data_rows")
        if data_rows is None and len(args) > 5:
            data_rows = args[5]
        assert data_rows
        assert kwargs.get("source_spool") is None
        return len(data_rows), "checksum", {"rejected_rows": 0}

    monkeypatch.setattr("src.transfer.file_stream.records_to_matrix", _counted)
    monkeypatch.setattr("src.transfer.file_stream._write_batch", fake_write_batch)

    dest = EndpointConfig(
        kind="streaming",
        format="kafka",
        host="127.0.0.1",
        database="df",
        table="events",
    )
    written, _ddl, _summary, _cols = stream_file_to_database(
        content=_csv_bytes(2),
        filename="rows.csv",
        destination=dest,
        mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        schema={},
        validation_mode="balanced",
    )
    assert written == 2
    assert seen["matrix"] == 1


def test_spool_fingerprints_match_matrix_remap():
    records = [
        {"id": "1", "note": DF_MISSING_SENTINEL},
        {"id": "2", "note": "ok"},
    ]
    columns = ["id", "note"]
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "note", "target": "note"},
    ]
    headers, data_rows = records_to_matrix(records, columns)
    mapped, _ = map_rows_for_fingerprint(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=["id", "note"],
        column_types={"id": "TEXT", "note": "TEXT"},
        error_policy="quarantine",
        dest_types={"id": "TEXT", "note": "TEXT"},
        preserve_case=True,
        dest_kind="postgresql",
        empty_cells_as_null=True,
    )
    from_matrix = row_fingerprints(
        mapped, ["id", "note"], dest_db_type="postgresql", dest_types={"id": "TEXT", "note": "TEXT"}
    )
    spill = spill_engine_write_records(list(records), columns, mappings, extra={})
    try:
        from_spool = fingerprints_from_spool(
            spill.spool,
            mappings,
            ["id", "note"],
            column_types={"id": "TEXT", "note": "TEXT"},
            dest_db_type="postgresql",
            dest_types={"id": "TEXT", "note": "TEXT"},
            error_policy="quarantine",
            empty_cells_as_null=True,
        )
        assert spill.spool.iter_bundles(10).__next__()[1][0][1] == DF_MISSING_SENTINEL
    finally:
        spill.close()
    assert _digest(from_spool) == _digest(from_matrix)
    assert from_spool


def test_overwrite_keys_collected_before_spill(monkeypatch):
    def fake_write_batch(*args, **kwargs):
        spool = kwargs.get("source_spool")
        return int(spool.row_count), "checksum", {"rejected_rows": 0}

    monkeypatch.setattr("src.transfer.file_stream._write_batch", fake_write_batch)

    dest = EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string="sqlite:///:memory:",
        table="import",
    )
    _written, _ddl, summary, _cols = stream_file_to_database(
        content=_csv_bytes(3),
        filename="rows.csv",
        destination=dest,
        mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        schema={},
        sync_mode="full_refresh_overwrite",
        stream_contracts=[
            {"selected": True, "primary_key": "id", "sync_mode": "full_refresh_overwrite"}
        ],
        validation_mode="balanced",
    )
    from services.dest_precount import OVERWRITE_SOURCE_KEYS_KEY

    keys = summary.get(OVERWRITE_SOURCE_KEYS_KEY)
    assert keys
    assert len(keys) == 3


def test_file_stream_batch_rows_stay_unexpanded_on_explode(monkeypatch):
    """Gate-8 file cardinality stays 1:1 with source records, not STRUCT explode."""
    from services.json_intelligence import ARRAY_POLICY_EXPLODE

    seen = {}

    def fake_write_batch(*args, **kwargs):
        spool = kwargs.get("source_spool")
        seen["expanded"] = int(spool.row_count)
        return int(spool.row_count), "checksum", {"rejected_rows": 0}

    monkeypatch.setattr("src.transfer.file_stream._write_batch", fake_write_batch)

    dest = EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string="sqlite:///:memory:",
        table="import",
    )
    content = b'id,tags\n1,"[""a"",""b""]"\n'
    _written, _ddl, summary, _cols = stream_file_to_database(
        content=content,
        filename="rows.csv",
        destination=dest,
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "tags", "target": "tags", "struct_policy": ARRAY_POLICY_EXPLODE},
        ],
        schema={"id": "INTEGER", "tags": "STRING"},
        validation_mode="balanced",
    )
    assert seen["expanded"] == 2
    assert summary.get("source_row_count") == 1
    assert summary.get("source_row_count_source") == "batch_rows"
