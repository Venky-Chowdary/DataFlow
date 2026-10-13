from __future__ import annotations

import os
import socket
import uuid
from urllib.error import URLError
from urllib.request import urlopen

import pytest

REST_URI = os.environ.get("DATAFLOW_ICEBERG_REST_URI", "http://127.0.0.1:8181").rstrip("/")
REST_WAREHOUSE = os.environ.get(
    "DATAFLOW_ICEBERG_REST_WAREHOUSE", "file:///tmp/iceberg-rest-wh"
)
S3_ENDPOINT = os.environ.get("DATAFLOW_ICEBERG_S3_ENDPOINT", "http://127.0.0.1:9000")


def _rest_reachable() -> bool:
    try:
        host = REST_URI.split("://", 1)[-1].split("/", 1)[0]
        hostname, _, port_s = host.partition(":")
        with socket.create_connection((hostname, int(port_s or "8181")), timeout=1.5):
            pass
        with urlopen(f"{REST_URI}/v1/config", timeout=2) as response:
            return int(getattr(response, "status", 0) or 0) == 200
    except (OSError, URLError, ValueError):
        return False


requires_rest = pytest.mark.skipif(
    not _rest_reachable(), reason=f"Iceberg REST catalog not reachable at {REST_URI}"
)


def test_rest_catalog_includes_s3_and_client_properties() -> None:
    from connectors.iceberg_catalog import parse_iceberg_catalog_config

    config = parse_iceberg_catalog_config(
        {
            "connection_string": REST_URI,
            "warehouse": REST_WAREHOUSE,
            "region": "eu-west-1",
            "table": "events",
            "extra": {
                "catalog_type": "rest",
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": "rest-access",
                "s3.secret-access-key": "rest-secret",
                "s3.path-style-access": "true",
                "s3.region": "us-east-1",
                "client.s3.signer": "s3v4",
            },
        }
    )

    props = config["properties"]
    assert props["s3.endpoint"] == S3_ENDPOINT
    assert props["s3.access-key-id"] == "rest-access"
    assert props["s3.secret-access-key"] == "rest-secret"
    assert props["s3.path-style-access"] == "true"
    assert props["s3.region"] == "us-east-1"
    assert props["client.s3.signer"] == "s3v4"
    assert props["client.region"] == "eu-west-1"


def test_rest_owned_properties_take_precedence() -> None:
    from connectors.iceberg_catalog import parse_iceberg_catalog_config

    config = parse_iceberg_catalog_config(
        {
            "connection_string": REST_URI,
            "warehouse": "s3://catalog-owned/warehouse",
            "api_key": "endpoint-token",
            "region": "eu-west-1",
            "table": "events",
            "extra": {
                "catalog_type": "rest",
                "uri": "http://override.invalid",
                "warehouse": "s3://extra-owned/warehouse",
                "token": "extra-token",
                "client.region": "ap-south-1",
                "s3.endpoint": S3_ENDPOINT,
            },
        }
    )

    props = config["properties"]
    assert props["uri"] == REST_URI
    assert props["warehouse"] == "s3://catalog-owned/warehouse"
    assert props["token"] == "endpoint-token"
    assert props["client.region"] == "ap-south-1"


def test_rest_catalog_includes_flattened_connector_storage_properties() -> None:
    from connectors.iceberg_catalog import parse_iceberg_catalog_config

    config = parse_iceberg_catalog_config(
        {
            "connection_string": REST_URI,
            "warehouse": REST_WAREHOUSE,
            "table": "events",
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": "flattened-access",
            "s3.secret-access-key": "flattened-secret",
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        }
    )

    props = config["properties"]
    assert props["s3.endpoint"] == S3_ENDPOINT
    assert props["s3.access-key-id"] == "flattened-access"
    assert props["s3.secret-access-key"] == "flattened-secret"
    assert props["s3.path-style-access"] == "true"
    assert props["s3.region"] == "us-east-1"


def test_glue_storage_properties_keep_existing_precedence() -> None:
    from connectors.iceberg_catalog import _glue_props

    props = _glue_props(
        "s3://warehouse",
        {"region": "eu-west-1"},
        {
            "s3.endpoint": S3_ENDPOINT,
            "s3.secret-access-key": "glue-path-secret",
            "client.region": "ap-south-1",
        },
    )

    assert props["s3.endpoint"] == S3_ENDPOINT
    assert props["s3.secret-access-key"] == "glue-path-secret"
    assert props["client.region"] == "ap-south-1"
    assert props["glue.region"] == "eu-west-1"


def test_unreachable_catalog_error_does_not_reveal_storage_secrets(caplog) -> None:
    from connectors.iceberg_catalog import load_catalog

    secrets = ("s3-secret-sentinel", "glue-secret-sentinel")
    caplog.set_level("DEBUG")
    with pytest.raises(Exception) as exc_info:
        load_catalog(
            {
                "connection_string": "http://127.0.0.1:1/v1",
                "warehouse": "s3://warehouse",
                "table": "events",
                "s3.secret-access-key": secrets[0],
                "extra": {
                    "catalog_type": "rest",
                    "glue.secret-access-key": secrets[1],
                },
            }
        )

    output = f"{caplog.text}\n{exc_info.value}"
    assert all(secret not in output for secret in secrets)


@requires_rest
def test_live_rest_writer_result_does_not_reveal_storage_secrets(caplog) -> None:
    from connectors.iceberg_writer import write_mapped_rows

    secrets = ("live-s3-secret-sentinel", "live-glue-secret-sentinel")
    caplog.set_level("DEBUG")
    result = write_mapped_rows(
        connection_string=REST_URI,
        warehouse=REST_WAREHOUSE,
        table_name=f"default.redaction_{uuid.uuid4().hex[:8]}",
        headers=["id", "value"],
        data_rows=[["1", "sentinel-test"]],
        mappings=[
            {"source": "id", "target": "id", "transform": "direct"},
            {"source": "value", "target": "value", "transform": "direct"},
        ],
        write_mode="append",
        create_table=True,
        **{
            "catalog_type": "rest",
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": os.environ.get("MINIO_ROOT_USER", "dataflow"),
            "s3.secret-access-key": secrets[0],
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
        extra={
            "catalog_type": "rest",
            "glue.secret-access-key": secrets[1],
        },
    )

    destination_summary = {
        "table": result.table_name,
        "error": result.error,
        "meta": result.meta,
        "warnings": result.warnings,
    }
    output = (
        f"{caplog.text}\n{result!r}\n"
        f"{getattr(result, 'error', '')}\n{destination_summary!r}"
    )
    assert all(secret not in output for secret in secrets)
