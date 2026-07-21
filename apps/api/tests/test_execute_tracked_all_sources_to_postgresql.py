"""Read from every supported local emulator source and write to PostgreSQL.

This is a broad "source → PostgreSQL" smoke matrix that proves each connector
class can both be written to and read from, not just the write path tested by
`test_live_emulator_matrix.py`.
"""

from __future__ import annotations

import sys
import uuid
from dataclasses import replace
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.adapters import write_destination_database
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

# Import the emulator definitions and the reachability helper from the live test.
from tests.test_live_emulator_matrix import CASES as _CASES
from tests.test_live_emulator_matrix import _is_reachable


def _prepare_source(source: EndpointConfig) -> EndpointConfig:
    """Return a source endpoint with a unique table/collection/key name."""
    suffix = uuid.uuid4().hex[:8]
    if source.format in ("s3", "gcs", "adls"):
        new_table = f"payments_{source.format}_{suffix}.json"
    else:
        new_table = f"payments_{source.format}_{suffix}"
    return replace(source, table=new_table)


@pytest.mark.parametrize("source", [pytest.param(c.values[0], id=c.id) for c in _CASES])
def test_all_source_to_postgresql(source: EndpointConfig):
    if not _is_reachable("localhost", 5432):
        pytest.skip("PostgreSQL emulator not reachable on localhost:5432")

    # pgvector and qdrant are destination-only vector stores in this release.
    if source.format in ("pgvector", "qdrant"):
        pytest.skip("destination-only vector store cannot act as a transfer source")

    if source.format == "snowflake":
        pytest.importorskip("fakesnow")
    elif not _is_reachable(source.host, source.port):
        pytest.skip(f"{source.format} emulator not reachable on {source.host}:{source.port}")

    source = _prepare_source(source)
    dest_table = f"from_{source.format}_{uuid.uuid4().hex[:8]}"

    records = [
        {"id": "1", "amount": "1000.00"},
        {"id": "2", "amount": "2000.50"},
    ]
    columns = ["id", "amount"]
    schema = {"id": "INTEGER", "amount": "DECIMAL"}
    mappings = [{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}]

    destination = EndpointConfig(
        kind="database",
        format="postgresql",
        host="localhost",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        table=dest_table,
    )

    request = TransferRequest(
        source=source,
        destination=destination,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    )

    engine = UniversalTransferEngine()
    # snowflake_conn auto-patches local accounts — do not nest fakesnow.patch().
    rows, _, summary = write_destination_database(
        source, records, columns, schema, mappings
    )
    assert rows == 2, f"{source.format}: expected 2 rows seeded, got {rows} (summary={summary})"
    assert summary.get("error") is None, f"{source.format}: {summary.get('error')}"

    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, f"{source.format}: {result.error}"
    assert result.records_transferred == 2, (
        f"{source.format}: expected 2 transferred, got {result.records_transferred}"
    )
    assert result.reconciliation.get("passed") is True, f"{source.format}: {result.reconciliation}"
