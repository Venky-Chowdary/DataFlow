"""SQLite -> CSV file export -> SQLite roundtrip proves database/file/database integrity."""

from __future__ import annotations

import csv
import io
import os
import sqlite3
import sys
import tempfile
import uuid
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.reconciliation import normalize_cell
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

EXPECTED = [
    {"id": "1", "name": "Alice", "amount": "100.50", "active": "1", "created_at": "2024-01-15T09:30:00Z"},
    {"id": "2", "name": "Bob", "amount": "250.00", "active": "0", "created_at": "2024-06-01 14:00:00+00:00"},
    {"id": "3", "name": "Carol", "amount": "1,000.00", "active": "1", "created_at": "2024-12-31"},
]


def _write_source_db(path: str) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE roundtrip (id TEXT, name TEXT, amount TEXT, active TEXT, created_at TEXT)")
        for row in EXPECTED:
            conn.execute(
                "INSERT INTO roundtrip VALUES (?, ?, ?, ?, ?)",
                (row["id"], row["name"], row["amount"], row["active"], row["created_at"]),
            )
        conn.commit()
    finally:
        conn.close()


def test_sqlite_to_csv_to_sqlite_roundtrip():
    with tempfile.TemporaryDirectory() as tmp:
        source_db = os.path.join(tmp, "source.db")
        target_db = os.path.join(tmp, "target.db")
        _write_source_db(source_db)

        # 1. Database -> CSV file export
        export_request = TransferRequest(
            source=EndpointConfig(
                kind="database",
                format="sqlite",
                connection_string=source_db,
                database=source_db,
                table="roundtrip",
            ),
            destination=EndpointConfig(kind="file_export", format="csv"),
            skip_preflight=True,
        )
        engine = UniversalTransferEngine()
        export_result = engine.execute_tracked(export_request, uuid.uuid4().hex[:24])
        assert export_result.success is True, export_result.error
        assert export_result.records_transferred == len(EXPECTED)
        assert "filename" in export_result.destination_summary
        export_path = export_result.destination_summary["path"]
        assert os.path.exists(export_path)
        with open(export_path, "rb") as f:
            csv_bytes = f.read()

        # Sanity: exported CSV parses back to the same rows (column order may differ)
        reader = csv.DictReader(io.StringIO(csv_bytes.decode("utf-8")))
        exported_rows = list(reader)
        assert len(exported_rows) == len(EXPECTED)

        # 2. CSV file -> new SQLite database
        import_request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            source_filename="roundtrip_export.csv",
            source_content=csv_bytes,
            destination=EndpointConfig(
                kind="database",
                format="sqlite",
                connection_string=target_db,
                database=target_db,
                table="roundtrip_imported",
            ),
            sync_mode="full_refresh_overwrite",
            stream_contracts=[{
                "name": "roundtrip_imported",
                "sync_mode": "full_refresh_overwrite",
                "primary_key": "id",
                "selected": True,
            }],
            skip_preflight=True,
        )
        import_result = engine.execute_tracked(import_request, uuid.uuid4().hex[:24])
        assert import_result.success is True, import_result.error
        assert import_result.records_transferred == len(EXPECTED)
        assert import_result.reconciliation.get("passed") is True
        assert import_result.reconciliation.get("source_checksum") == import_result.reconciliation.get("target_checksum")

        # 3. Verify target SQLite content
        conn = sqlite3.connect(target_db)
        try:
            cur = conn.execute("SELECT id, name, amount, active, created_at FROM roundtrip_imported ORDER BY id")
            target_rows = [
                {"id": r[0], "name": r[1], "amount": r[2], "active": r[3], "created_at": r[4]}
                for r in cur.fetchall()
            ]
        finally:
            conn.close()

        assert len(target_rows) == len(EXPECTED)
        for expected, target in zip(EXPECTED, target_rows):
            # Values are preserved semantically; exact string formatting (commas, date
            # punctuation, boolean literal) is normalized during the typed transfer.
            for col in ("id", "name", "active", "created_at"):
                assert normalize_cell(target[col]) == normalize_cell(expected[col]), col
            # Locale-formatted amounts like "1,000.00" are parsed to the same numeric value.
            clean_expected = expected["amount"].replace(",", "")
            assert normalize_cell(target["amount"]) == normalize_cell(clean_expected)
