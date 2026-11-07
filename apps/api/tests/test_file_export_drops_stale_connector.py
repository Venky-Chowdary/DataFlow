"""A file export never inherits a database destination's saved connector.

Studio QA (job 6ae8e3ec62db5e71dd4d3507): switching Destination from a saved
PostgreSQL connector to File export (JSON) submitted ``connector_id`` alongside
``kind=file_export``. Resolution expanded the connector and overwrote the
export format with its driver type, so the run failed as
``file/csv → file_export/postgresql not yet live``.
"""

from __future__ import annotations

from src.transfer.adapters import resolve_endpoint
from src.transfer.endpoint_kind import normalize_destination
from src.transfer.models import EndpointConfig


def test_file_export_endpoint_drops_connector_id() -> None:
    ep = EndpointConfig(kind="file_export", format="json", connector_id="pg-1", table="csv_import")
    assert ep.connector_id is None
    assert ep.format == "json"


def test_from_dict_file_export_drops_connector_id() -> None:
    ep = EndpointConfig.from_dict("file_export", {"format": "csv", "connector_id": "pg-1"})
    assert ep.connector_id is None


def test_resolve_keeps_export_format() -> None:
    ep = EndpointConfig(kind="file_export", format="json", connector_id="pg-1")
    assert resolve_endpoint(ep).format == "json"


def test_file_kind_normalized_to_export_drops_connector_id() -> None:
    ep = EndpointConfig(kind="file", format="json", connector_id="pg-1")
    out = normalize_destination(ep)
    assert out.kind == "file_export"
    assert out.connector_id is None


def test_database_endpoint_keeps_connector_id() -> None:
    ep = EndpointConfig(kind="database", format="postgresql", connector_id="pg-1")
    assert ep.connector_id == "pg-1"
