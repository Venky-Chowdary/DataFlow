"""Unit tests for human-readable pipeline explanations."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

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
