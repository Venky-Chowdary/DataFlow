"""Tests for AI Connector Factory."""

from services.connector_factory import generate_connector_from_openapi

SAMPLE_SPEC = {
    "openapi": "3.0.0",
    "info": {"title": "Payments API", "version": "2.1.0"},
    "servers": [{"url": "https://api.payments.example.com/v2"}],
    "paths": {
        "/payments": {
            "get": {"operationId": "listPayments", "summary": "List payments"},
            "post": {"operationId": "createPayment", "summary": "Create payment"},
        },
        "/customers/{id}": {
            "get": {"operationId": "getCustomer", "summary": "Get customer"},
        },
    },
    "security": [{"bearerAuth": []}],
}


def test_generate_connector_from_openapi():
    result = generate_connector_from_openapi(SAMPLE_SPEC)
    assert result["connector_id"] == "payments_api"
    assert result["endpoint_count"] == 3
    assert "listPayments" in result["plugin_code"]
    assert result["auth_type"] == "bearer"
    assert result["certification"]["status"] == "draft"
