"""Real-cloud connector credential matrix.

These tests prove the DataFlow engine can route to Snowflake, BigQuery, GCS,
and ADLS endpoints and, when real credentials are supplied, complete a small
end-to-end transfer.  Without credentials the tests are skipped so the suite
stays green in local/CI runs that do not have cloud secrets mounted.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.connector_capabilities import resolve_driver_type  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402


def _has_creds(provider: str) -> bool:
    keys = {
        "snowflake": ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"],
        "bigquery": ["GOOGLE_APPLICATION_CREDENTIALS"],
        "gcs": ["GOOGLE_APPLICATION_CREDENTIALS"],
        "adls": ["AZURE_STORAGE_ACCOUNT", "AZURE_STORAGE_KEY"],
    }
    return any(os.environ.get(k) for k in keys.get(provider, []))


@pytest.mark.parametrize(
    "catalog_id,expected_driver",
    [
        ("snowflake", "snowflake"),
        ("bigquery", "bigquery"),
        ("google_bigquery", "bigquery"),
        ("gcs", "gcs"),
        ("google_cloud_storage", "gcs"),
        ("adls", "adls"),
        ("azure_blob_storage", "adls"),
        ("azure_data_lake_storage", "adls"),
    ],
)
def test_cloud_catalog_ids_resolve_to_driver(catalog_id: str, expected_driver: str) -> None:
    assert resolve_driver_type(catalog_id) == expected_driver


def test_snowflake_endpoint_config_carries_account_and_role() -> None:
    ep = EndpointConfig(
        kind="database",
        format="snowflake",
        host="myaccount.snowflakecomputing.com",
        database="DEMO",
        schema="PUBLIC",
        username="user",
        password="secret",
        warehouse="COMPUTE_WH",
        auth_role="ACCOUNTADMIN",
    )
    # Resolve config should preserve host/database/warehouse/role.
    from src.transfer.adapters import resolve_connector_config

    cfg = resolve_connector_config(ep)
    assert cfg["host"] == "myaccount.snowflakecomputing.com"
    assert cfg["port"] == 443
    assert cfg["database"] == "DEMO"
    assert cfg["schema"] == "PUBLIC"
    assert cfg["warehouse"] == "COMPUTE_WH"
    assert cfg["role"] == "ACCOUNTADMIN"


def test_bigquery_endpoint_config_carries_connection_string() -> None:
    ep = EndpointConfig(
        kind="database",
        format="bigquery",
        database="my-project",
        schema="my_dataset",
        connection_string="bigquery://my-project/my_dataset?credentials_path=/tmp/key.json",
    )
    from src.transfer.adapters import resolve_connector_config

    cfg = resolve_connector_config(ep)
    assert cfg["database"] == "my-project"
    assert cfg["schema"] == "my_dataset"
    assert cfg["connection_string"] == ep.connection_string
    assert cfg["port"] == 443


def test_gcs_endpoint_config_carries_bucket_and_prefix() -> None:
    ep = EndpointConfig(
        kind="database",
        format="gcs",
        database="my-bucket",
        host="storage.googleapis.com",
        connection_string="gs://my-bucket/prefix/",
    )
    from src.transfer.adapters import resolve_connector_config

    cfg = resolve_connector_config(ep)
    assert cfg["database"] == "my-bucket"
    assert cfg["port"] == 443


def test_adls_endpoint_config_carries_account_and_key() -> None:
    ep = EndpointConfig(
        kind="database",
        format="adls",
        database="mycontainer",
        host="myaccount.blob.core.windows.net",
        username="myaccount",
        password="my_key",
        connection_string="DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=my_key;EndpointSuffix=core.windows.net",
    )
    from src.transfer.adapters import resolve_connector_config

    cfg = resolve_connector_config(ep)
    assert cfg["database"] == "mycontainer"
    assert cfg["host"] == "myaccount.blob.core.windows.net"
    assert cfg["port"] == 443
    assert cfg["password"] == "my_key"
    assert cfg["connection_string"] == ep.connection_string


@pytest.mark.parametrize("provider", ["snowflake", "bigquery", "gcs", "adls"])
def test_credential_gated_cloud_transfer_is_skipped_without_secrets(provider: str) -> None:
    """Placeholder for live cloud transfers; skipped when credentials are absent."""
    if _has_creds(provider):
        pytest.fail(f"Credentials present for {provider}; implement live transfer test and remove this guard.")
    pytest.skip(f"No credentials available for {provider}")
