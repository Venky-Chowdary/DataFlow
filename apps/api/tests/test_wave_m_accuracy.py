"""Wave M accuracy: existence honesty + CDC empty-string watermark."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.transfer.cdc_transfer import CdcEngine
from src.transfer.endpoint_intelligence import _attach_db_sample
from src.transfer.models import EndpointConfig


def test_query_cdc_empty_string_watermark_does_not_resnapshot():
    reader = CdcEngine(
        src_type="postgresql",
        src_cfg={"database": "db"},
        table_name="t",
        primary_key="id",
        cursor_field="cursor",
        watermark="",
        batch_size=10,
        columns=["id", "cursor"],
        schema={"id": "INTEGER", "cursor": "VARCHAR"},
    )
    with patch.object(reader, "snapshot") as snap, patch.object(reader, "_yield_batches") as yb:
        yb.return_value = iter([])
        list(reader.poll())
        snap.assert_not_called()
        yb.assert_called_once()


def test_redis_empty_scan_still_exists():
    out: dict = {
        "kind": "database",
        "format": "redis",
        "connected": True,
        "objects": [],
        "columns": [],
        "schema": {},
        "row_estimate": 0,
        "auto_create": [],
        "message": "ok",
    }
    endpoint = EndpointConfig(kind="database", format="redis", table="orders:*")
    empty = MagicMock()
    empty.headers = ["key", "value"]
    empty.rows = []
    empty.total_rows = 0
    with patch(
        "src.transfer.endpoint_intelligence.resolve_connector_config",
        return_value={"type": "redis", "host": "localhost"},
    ), patch(
        "connectors.redis_reader.read_keys_batch",
        return_value=(empty, None),
    ):
        _attach_db_sample(out, endpoint)
    assert out["table_exists"] is True
    assert "logical prefixes" in (out.get("message") or "").lower()
