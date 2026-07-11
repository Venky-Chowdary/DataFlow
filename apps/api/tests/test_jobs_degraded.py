"""Job Theater must not break when the job store is unavailable."""

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    from src.main import app
    return TestClient(app)


def test_jobs_list_degrades_when_store_unavailable(client, monkeypatch):
    import sys

    def _boom():
        raise ConnectionError("MongoDB unavailable at mongodb://localhost:27017/")

    module = sys.modules["src.routers.connectors_router"]
    monkeypatch.setattr(module, "get_mongodb_service", _boom)

    res = client.get("/api/v1/connectors/jobs")
    assert res.status_code == 200
    data = res.json()
    assert data["jobs"] == []
    assert data["count"] == 0
    assert data["degraded"] is True
    assert data["persistence"] == "unavailable"


def test_get_database_raises_clear_error_without_client():
    from src.services.mongodb_service import MongoDBService

    svc = MongoDBService(connection_string="mongodb://127.0.0.1:1/")
    svc.client = None
    monkey = svc

    def _fail_connect():
        return False

    monkey.connect = _fail_connect  # type: ignore[method-assign]
    with pytest.raises(ConnectionError):
        monkey.get_database()
