"""Fixed-width dest export is the inverse of tabular FWF ingest.

#136 made ``fixed_width`` a transfer-live *source*. Dest export still refused
and ``write_destination_file`` would have landed JSON under a ``.fwf`` name
(D11). This file proves the export is ``#layout:`` plus right-padded records,
overflow is refused (never silent truncate), empty population is still a
layout header so COUNT is a measured 0, and dest COUNT is
``iter_fixed_width_dicts`` on disk — never the writer's ack.
"""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.dest_precount import count_artifact_rows  # noqa: E402
from services.fixed_width_layout import (  # noqa: E402
    FixedWidthError,
    count_fixed_width_records,
    dump_fixed_width_records,
    iter_fixed_width_dicts,
    layout_from_char_carriers,
    layout_header_line,
    resolve_dest_export_layout,
)
from src.transfer.adapters import write_destination_file  # noqa: E402
from src.transfer.connector_capabilities import dest_ready, get_capabilities  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from src.transfer.registry import PRODUCTION_SKU, validate_transfer  # noqa: E402
from tests.helpers.live_env import pg_creds, pg_up  # noqa: E402

RECORDS = [
    {"id": "1", "amount": "1000.00", "flag": "yes"},
    {"id": "2", "amount": "2000.50", "flag": "no"},
]
COLUMNS = ["id", "amount", "flag"]
LAYOUT = (("id", 8), ("amount", 16), ("flag", 8))
LAYOUT_PAYLOAD = [
    {"name": "id", "width": 8},
    {"name": "amount", "width": 16},
    {"name": "flag", "width": 8},
]


def test_fixed_width_is_a_live_file_export() -> None:
    caps = get_capabilities("fixed_width")
    assert caps.get("file_export") is True
    assert dest_ready(caps) is True
    ok, msg = validate_transfer("database", "sqlite", "file_export", "fixed_width")
    assert ok, msg
    ok, msg = validate_transfer("file", "csv", "file_export", "fixed_width")
    assert ok, msg
    ok, msg = validate_transfer("file", "fixed_width", "file_export", "fixed_width")
    assert ok, msg
    assert ("database", "sqlite", "file_export", "fixed_width") in PRODUCTION_SKU
    assert ("file", "csv", "file_export", "fixed_width") in PRODUCTION_SKU
    assert ("file", "fixed_width", "file_export", "fixed_width") in PRODUCTION_SKU


def test_dump_round_trips_padded_cells() -> None:
    body = dump_fixed_width_records(RECORDS, LAYOUT)
    assert body.startswith(b"#layout:")
    assert layout_header_line(LAYOUT).encode() in body
    rows = list(iter_fixed_width_dicts(body))
    assert rows == RECORDS
    assert count_fixed_width_records(body) == 2


def test_empty_population_is_layout_header_not_json() -> None:
    body = dump_fixed_width_records([], LAYOUT)
    assert body.startswith(b"#layout:")
    assert b"[" not in body
    assert b"{" not in body
    assert count_fixed_width_records(body) == 0
    assert list(iter_fixed_width_dicts(body)) == []


def test_dump_refuses_overflow_instead_of_truncate() -> None:
    with pytest.raises(FixedWidthError, match="silent truncate"):
        dump_fixed_width_records(
            [{"id": "too-wide", "amount": "1", "flag": "x"}],
            LAYOUT,
        )


def test_dump_refuses_nested_cells() -> None:
    with pytest.raises(FixedWidthError, match="nested"):
        dump_fixed_width_records(
            [{"id": "1", "amount": "1", "flag": {"inner": "2"}}],
            LAYOUT,
        )


def test_no_layout_is_refused_not_guessed() -> None:
    with pytest.raises(FixedWidthError, match="declared layout"):
        resolve_dest_export_layout(
            declared=None,
            columns=COLUMNS,
            dest_types={"id": "TEXT", "amount": "TEXT", "flag": "TEXT"},
        )


def test_char_n_stamps_are_a_layout() -> None:
    inferred = layout_from_char_carriers(
        COLUMNS,
        {"id": "CHAR(8)", "amount": "VARCHAR(16)", "flag": "NCHAR(8)"},
    )
    assert inferred == LAYOUT
    resolved = resolve_dest_export_layout(
        declared=None,
        columns=COLUMNS,
        dest_types={"id": "CHAR(8)", "amount": "VARCHAR(16)", "flag": "NCHAR(8)"},
    )
    assert resolved == LAYOUT


def test_layout_mismatch_fails_closed() -> None:
    with pytest.raises(FixedWidthError, match="do not match"):
        resolve_dest_export_layout(
            declared=(("id", 8), ("other", 8)),
            columns=COLUMNS,
        )


def test_layout_reorders_to_export_column_order() -> None:
    resolved = resolve_dest_export_layout(
        declared=(("flag", 8), ("id", 8), ("amount", 16)),
        columns=COLUMNS,
    )
    assert resolved == LAYOUT


def test_write_destination_file_fwf_is_not_json() -> None:
    content, filename, summary = write_destination_file(
        EndpointConfig(
            kind="file_export",
            format="fixed_width",
            table="t",
            extra={"fixed_width_layout": LAYOUT_PAYLOAD},
        ),
        RECORDS,
        COLUMNS,
        source_format="postgresql",
        column_types={"id": "TEXT", "amount": "TEXT", "flag": "TEXT"},
    )
    assert filename.endswith(".fwf")
    assert summary["mime"] == "text/plain"
    assert summary["rows"] == 2
    assert content.startswith(b"#layout:")
    assert list(iter_fixed_width_dicts(content)) == RECORDS


def test_write_destination_file_fwf_from_char_stamps() -> None:
    content, filename, summary = write_destination_file(
        EndpointConfig(kind="file_export", format="fwf"),
        RECORDS,
        COLUMNS,
        source_format="sqlite",
        column_types={
            "id": "CHAR(8)",
            "amount": "VARCHAR(16)",
            "flag": "CHAR(8)",
        },
    )
    assert filename.endswith(".fwf")
    assert summary["rows"] == 2
    assert list(iter_fixed_width_dicts(content)) == RECORDS


def test_write_destination_file_empty_fwf() -> None:
    content, filename, summary = write_destination_file(
        EndpointConfig(
            kind="file_export",
            format="fixed_width",
            extra={"write_options": {"fixed_width_layout": LAYOUT_PAYLOAD}},
        ),
        [],
        COLUMNS,
        source_format="sqlite",
    )
    assert filename.endswith(".fwf")
    assert summary["rows"] == 0
    assert count_fixed_width_records(content) == 0
    assert content.startswith(b"#layout:")


def test_write_destination_file_refuses_missing_layout() -> None:
    with pytest.raises(FixedWidthError, match="declared layout"):
        write_destination_file(
            EndpointConfig(kind="file_export", format="fixed_width"),
            RECORDS,
            COLUMNS,
            source_format="sqlite",
            column_types={"id": "TEXT", "amount": "TEXT", "flag": "TEXT"},
        )


def test_count_artifact_rows_fwf(tmp_path: Path) -> None:
    path = tmp_path / "export.fwf"
    path.write_bytes(dump_fixed_width_records(RECORDS, LAYOUT))
    assert count_artifact_rows(path, fmt="fixed_width") == 2
    assert count_artifact_rows(path) == 2
    empty = tmp_path / "empty.fwf"
    empty.write_bytes(dump_fixed_width_records([], LAYOUT))
    assert count_artifact_rows(empty, fmt="fixed_width") == 0
    nested = tmp_path / "bad.fwf"
    nested.write_bytes(b'[{"id": "1"}]\n')
    assert count_artifact_rows(nested, fmt="fixed_width") is None


def test_csv_to_fwf_export_artifact_count() -> None:
    csv_content = b"id,flag\n1,yes\n2,no\n"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="file_export",
            format="fixed_width",
            extra={
                "fixed_width_layout": [
                    {"name": "id", "width": 8},
                    {"name": "flag", "width": 8},
                ]
            },
        ),
        source_filename="flags.csv",
        source_content=csv_content,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    )
    result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == 2
    assert result.destination_summary.get("filename", "").endswith(".fwf")
    recon = result.reconciliation or {}
    assert recon.get("artifact_row_count") == 2
    assert recon.get("dest_count_source") == "artifact_readback"
    path = result.destination_summary["path"]
    rows = list(iter_fixed_width_dicts(Path(path)))
    assert [r["flag"] for r in rows] == ["yes", "no"]
    assert Path(path).read_bytes().startswith(b"#layout:")


def test_sqlite_to_fwf_to_sqlite_roundtrip() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        source_db = os.path.join(tmp, "source.db")
        target_db = os.path.join(tmp, "target.db")
        conn = sqlite3.connect(source_db)
        try:
            conn.execute("CREATE TABLE ledger (id TEXT, amount TEXT, flag TEXT)")
            conn.execute("INSERT INTO ledger VALUES ('1', '1000.00', 'yes')")
            conn.execute("INSERT INTO ledger VALUES ('2', '2000.50', 'no')")
            conn.commit()
        finally:
            conn.close()

        engine = UniversalTransferEngine()
        export_result = engine.execute_tracked(
            TransferRequest(
                source=EndpointConfig(
                    kind="database",
                    format="sqlite",
                    connection_string=source_db,
                    database=source_db,
                    table="ledger",
                ),
                destination=EndpointConfig(
                    kind="file_export",
                    format="fixed_width",
                    extra={"fixed_width_layout": LAYOUT_PAYLOAD},
                ),
                skip_preflight=True,
            ),
            uuid.uuid4().hex[:24],
        )
        assert export_result.success is True, export_result.error
        export_path = export_result.destination_summary["path"]
        body = Path(export_path).read_bytes()
        assert count_fixed_width_records(body) == 2
        assert count_artifact_rows(export_path, fmt="fixed_width") == 2
        assert export_result.reconciliation.get("artifact_row_count") == 2
        flags = [r["flag"] for r in iter_fixed_width_dicts(body)]
        assert flags == ["yes", "no"]
        amounts = [r["amount"] for r in iter_fixed_width_dicts(body)]
        assert amounts == ["1000.00", "2000.50"]

        import_result = engine.execute_tracked(
            TransferRequest(
                source=EndpointConfig(kind="file", format="fixed_width"),
                source_filename="ledger.fwf",
                source_content=body,
                destination=EndpointConfig(
                    kind="database",
                    format="sqlite",
                    connection_string=target_db,
                    database=target_db,
                    table="ledger",
                ),
                sync_mode="full_refresh_overwrite",
                skip_preflight=True,
                mappings=[
                    {"source": "id", "target": "id"},
                    {"source": "amount", "target": "amount"},
                    {"source": "flag", "target": "flag"},
                ],
            ),
            uuid.uuid4().hex[:24],
        )
        assert import_result.success is True, import_result.error
        back = sqlite3.connect(target_db)
        try:
            n = back.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
            rows = list(
                back.execute("SELECT id, amount, flag FROM ledger ORDER BY id")
            )
        finally:
            back.close()
        assert n == 2
        assert [str(r[2]) for r in rows] == ["yes", "no"]
        assert [str(r[1]) for r in rows] == ["1000.00", "2000.50"]


@pytest.mark.skipif(not pg_up(), reason="Postgres not authenticated")
def test_postgres_to_fwf_dest_count() -> None:
    creds = pg_creds()
    table = f"fwf_dest_{uuid.uuid4().hex[:10]}"
    import psycopg2

    conn = psycopg2.connect(
        host=creds["host"],
        port=int(creds["port"]),
        dbname=creds["database"],
        user=creds["username"],
        password=creds["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE "{table}" (id TEXT, amount TEXT, flag TEXT)'
            )
            cur.execute(
                f"INSERT INTO \"{table}\" VALUES ('1', '1000.00', 'yes'), "
                f"('2', '2000.50', 'no')"
            )
        conn.commit()
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=EndpointConfig(
                    kind="database",
                    format="postgresql",
                    host=str(creds["host"]),
                    port=int(creds["port"]),
                    database=str(creds["database"]),
                    username=str(creds["username"]),
                    password=str(creds["password"]),
                    schema="public",
                    table=table,
                ),
                destination=EndpointConfig(
                    kind="file_export",
                    format="fixed_width",
                    extra={"fixed_width_layout": LAYOUT_PAYLOAD},
                ),
                skip_preflight=True,
                mappings=[
                    {"source": "id", "target": "id", "confidence": 0.99},
                    {"source": "amount", "target": "amount", "confidence": 0.99},
                    {"source": "flag", "target": "flag", "confidence": 0.99},
                ],
            ),
            uuid.uuid4().hex[:24],
        )
        assert result.success is True, result.error
        path = result.destination_summary["path"]
        assert count_artifact_rows(path, fmt="fixed_width") == 2
        flags = [r["flag"] for r in iter_fixed_width_dicts(Path(path))]
        assert flags == ["yes", "no"]
        assert Path(path).read_bytes().startswith(b"#layout:")
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.commit()
        conn.close()
