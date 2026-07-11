"""Tests for honest platform status endpoint."""

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    from src.main import app
    return TestClient(app)


def test_platform_status_shape(client):
    res = client.get("/api/v1/transfer/platform")
    assert res.status_code == 200
    data = res.json()
    assert "catalog_total" in data
    assert "transfer_ready" in data
    assert "live_route_combinations" in data
    assert "llm_mapping_available" in data
    assert data["preflight_gates"] == 9


def test_capabilities_includes_format_conversion(client):
    res = client.get("/api/v1/transfer/capabilities")
    assert res.status_code == 200
    data = res.json()
    assert "format_conversion" in data
    assert "platform" in data
    assert "matrix" in data["format_conversion"]
