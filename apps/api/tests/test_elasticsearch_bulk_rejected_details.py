"""Elasticsearch bulk failures must land in rejected_details (no silent ok)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.elasticsearch_writer import write_mapped_rows  # noqa: E402


def test_es_bulk_errors_materialize_rejected_details_and_fail_strict():
    client = MagicMock()
    client.indices.exists.return_value = True
    bulk_errors = [
        {"index": {"_id": "bad-1", "status": 400, "error": {"type": "mapper_parsing_exception", "reason": "failed"}}}
    ]

    with patch("connectors.elasticsearch_writer._client", return_value=client):
        with patch("elasticsearch.helpers.bulk", return_value=(0, bulk_errors)):
            result = write_mapped_rows(
                host="localhost",
                port=9200,
                database="",
                username="",
                password="",
                schema="",
                connection_string="",
                ssl=False,
                table_name="events",
                headers=["id", "note"],
                data_rows=[["bad-1", "hello"]],
                mappings=[
                    {"source": "id", "target": "_id", "transform": "direct"},
                    {"source": "note", "target": "note", "transform": "direct"},
                ],
                column_types={"id": "TEXT", "note": "TEXT"},
                create_table=False,
                error_policy="fail",
            )

    assert result.ok is False
    assert result.rejected_rows >= 1
    assert any("elasticsearch bulk" in str(d.get("reason", "")).lower() for d in result.rejected_details)
    assert "bulk" in (result.error or "").lower()


def test_es_bulk_errors_quarantine_policy_keeps_partial_ok():
    client = MagicMock()
    client.indices.exists.return_value = True
    bulk_errors = [{"index": {"_id": "1", "error": "version_conflict"}}]

    with patch("connectors.elasticsearch_writer._client", return_value=client):
        with patch("elasticsearch.helpers.bulk", return_value=(1, bulk_errors)):
            result = write_mapped_rows(
                host="localhost",
                port=9200,
                database="",
                username="",
                password="",
                schema="",
                connection_string="",
                ssl=False,
                table_name="events",
                headers=["id"],
                data_rows=[["1"]],
                mappings=[{"source": "id", "target": "_id", "transform": "direct"}],
                column_types={"id": "TEXT"},
                create_table=False,
                error_policy="quarantine",
            )

    assert result.ok is True
    assert result.rejected_details
    assert result.rejected_rows >= 1
