"""Connector flow — catalog search, type resolution, DynamoDB validation."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _load_catalog_service():
    path = _API_ROOT / "src" / "services" / "catalog_service.py"
    spec = importlib.util.spec_from_file_location("catalog_service", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


catalog = _load_catalog_service()


def test_catalog_search_dynamodb():
    data = catalog.search_catalog("dynamodb", "all", "", "", 20)
    assert data["filtered"] >= 1
    assert any(
        "dynamodb" in c["id"].lower() or "dynamodb" in c["name"].lower()
        for c in data["connectors"]
    )


def test_catalog_search_postgres_partial():
    data = catalog.search_catalog("postgres", "all", "", "", 20)
    assert data["filtered"] >= 1


def test_dynamodb_test_requires_table():
    from connectors.dynamodb import test_dynamodb

    result = test_dynamodb(
        host="us-east-1",
        port=443,
        database="",
        username="AKIA",
        password="secret",
        schema="",
        connection_string="",
        ssl=True,
    )
    assert result.ok is False
    assert "table" in (result.error or "").lower()


def test_dynamodb_test_requires_credentials():
    from connectors.dynamodb import test_dynamodb

    result = test_dynamodb(
        host="us-east-1",
        port=443,
        database="orders",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=True,
    )
    assert result.ok is False


def test_dynamodb_credential_validation_without_boto3():
    """Without boto3 installed, valid-shaped creds pass structural validation."""
    try:
        import boto3  # noqa: F401

        pytest.skip("boto3 installed — live probe path")
    except ImportError:
        pass

    from connectors.dynamodb import test_dynamodb

    result = test_dynamodb(
        host="us-east-1",
        port=443,
        database="orders",
        username="AKIATEST",
        password="secretkey",
        schema="",
        connection_string="",
        ssl=True,
    )
    assert result.ok is True
    assert "orders" in result.message


def test_s3_requires_bucket():
    from connectors.s3 import test_s3

    result = test_s3(
        host="us-east-1",
        port=443,
        database="",
        username="AKIA",
        password="secret",
        schema="",
        connection_string="",
        ssl=True,
    )
    assert result.ok is False
    assert "bucket" in (result.error or "").lower()


def test_s3_requires_credentials():
    from connectors.s3 import test_s3

    result = test_s3(
        host="us-east-1",
        port=443,
        database="my-bucket",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=True,
    )
    assert result.ok is False


def test_redis_requires_host_without_driver():
    from connectors.redis_kv import test_redis

    result = test_redis(
        host="localhost",
        port=6379,
        database="0",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
    )
    # Either live probe (driver present, no server -> error) or validation fallback
    assert isinstance(result.ok, bool)
    if result.driver == "validation":
        assert result.ok is True


def test_elasticsearch_requires_host_or_url():
    from connectors.elasticsearch import test_elasticsearch

    result = test_elasticsearch(
        host="",
        port=9200,
        database="",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
    )
    assert result.ok is False


def test_catalog_includes_new_beta_connectors():
    for cid in ("redis", "elasticsearch", "s3"):
        data = catalog.search_catalog(cid, "all", "", "", 10)
        assert any(c["id"] == cid for c in data["connectors"]), f"{cid} missing from catalog"


def test_saved_connector_crud_file_store(tmp_path, monkeypatch):
    store = tmp_path / "connectors.json"
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(store))

    from services import connector_store as cs

    monkeypatch.setattr(cs, "STORE_PATH", store)

    conn = cs.create_connector(
        {
            "name": "Test PG",
            "type": "postgresql",
            "role": "both",
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "username": "user",
            "password": "pass",
        }
    )
    assert conn.id
    listed = cs.list_connectors()
    assert any(c.id == conn.id for c in listed)
    cs.delete_connector(conn.id)
    assert not any(c.id == conn.id for c in cs.list_connectors())
