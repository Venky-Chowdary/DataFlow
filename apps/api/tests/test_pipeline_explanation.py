"""Unit tests for human-readable pipeline explanations."""

from __future__ import annotations

import sys
from pathlib import Path


_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.pipeline_explanation import (  # noqa: E402
    _sync_mode_note,
    build_pipeline_explanation,
)
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def test_sync_mode_note_describes_business_behavior():
    assert "cleared" in _sync_mode_note("full_refresh_overwrite").lower()
    assert "without changing existing" in _sync_mode_note("append").lower()
    assert "merged by primary key" in _sync_mode_note("upsert").lower()
    assert "watermark" in _sync_mode_note("incremental").lower()
    assert "soft deletes" in _sync_mode_note("cdc").lower()


def test_pipeline_explanation_includes_sync_behavior():
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", format="postgresql", table="products"),
        sync_mode="append",
    )
    explanation = build_pipeline_explanation(
        request=request,
        columns=["id", "name"],
        source_schema={"id": "integer", "name": "string"},
        mappings=[{"source": "id", "target": "product_id", "transform": "integer", "confidence": 0.95}],
        reconciliation={"passed": True, "message": "checksums matched"},
        destination_summary={"rows_written": 10},
    )
    assert "sync mode: append" in explanation
    assert "without changing existing" in explanation
    assert "id (integer) → product_id" in explanation
    assert "transform: integer" in explanation
    assert "checksums matched" in explanation


def test_multi_table_explanation_names_every_contract_not_the_last_table():
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", table="orders"),
        destination=EndpointConfig(
            kind="database", format="postgresql", database="dataflow", table="orders"
        ),
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        stream_contracts=[
            {
                "name": "customers",
                "selected": True,
                "mappings": [
                    {"source": "id", "target": "id", "source_type": "INTEGER", "confidence": 0.99},
                    {"source": "email", "target": "email", "source_type": "TEXT", "confidence": 0.9},
                ],
            },
            {
                "name": "orders",
                "selected": True,
                "mappings": [
                    {
                        "source": "amount",
                        "target": "amount",
                        "source_type": "DECIMAL",
                        "transform": "decimal",
                        "confidence": 0.95,
                    }
                ],
            },
        ],
    )
    explanation = build_pipeline_explanation(
        request=request,
        columns=["id", "customer_id", "amount", "updated_at"],
        source_schema={"id": "INT4", "amount": "DECIMAL"},
        mappings=[{"source": "amount", "target": "amount", "transform": "decimal", "confidence": 0.95}],
        reconciliation={
            "passed": True,
            "message": "Checksum matches orders (2 rows). This digest is not the whole job.",
        },
        destination_summary={
            "multi_stream": True,
            "table": "orders",
            "rows_written": 4,
            "streams": [{"name": "customers"}, {"name": "orders"}],
        },
        rows_written=4,
    )
    first = explanation.splitlines()[0]
    assert first == "Transfer: database/postgresql (customers, orders) → database/postgresql (customers, orders) — 2 tables"
    assert "Each selected table is cleared" in explanation
    assert "customers: 2 columns (id, email)" in explanation
    assert "orders: 1 column (amount)" in explanation
    assert "email (TEXT) → email" in explanation
    assert "Source inferred 4 columns" not in explanation
    assert "not the whole job" in explanation
    assert "Rows written: 4" in explanation
