import logging

import pytest

import services.dest_precount as dest_precount


SECRET_SENTINEL = "COUNT_CONFIG_SECRET_SENTINEL"


def test_s3_count_uses_resolved_properties_for_flattened_connector_config(
    monkeypatch, caplog
):
    captured = {}

    def count_object_store_key(kind, cfg, bucket, key, *, missing=None):
        captured.update(kind=kind, cfg=cfg, bucket=bucket, key=key, missing=missing)
        return 23

    monkeypatch.setattr(
        dest_precount, "_count_object_store_key", count_object_store_key
    )
    endpoint = {
        "type": "iceberg",
        "connection_string": "http://rest-catalog:8181",
        "warehouse": "file:///tmp/iceberg-wh",
        "schema": "default",
        "table": "orders",
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "count-user",
        "s3.secret-access-key": SECRET_SENTINEL,
        "s3.path-style-access": "true",
        "s3.region": "us-east-1",
        "extra": {
            "job_id": "job-1",
            "catalog_type": "rest",
            "warehouse": "file:///tmp/iceberg-wh",
        },
    }

    with caplog.at_level(logging.WARNING, logger="services.dest_precount"):
        result = dest_precount._count_iceberg_data_uri(
            "s3://warehouse/default/orders/data/part.parquet",
            endpoint=endpoint,
        )

    cfg = captured["cfg"]
    assert result == 23
    assert captured["kind"] == "s3"
    assert cfg["endpoint_url"] == "http://minio:9000"
    assert cfg["connection_string"] == "http://minio:9000"
    assert cfg["username"] == "count-user"
    assert cfg["password"] == SECRET_SENTINEL
    assert cfg["path_style"] is True
    assert cfg["host"] == "us-east-1"
    assert SECRET_SENTINEL not in caplog.text
    assert SECRET_SENTINEL not in str(result)


@pytest.mark.parametrize("failure", ["parse", "endpoint", "credentials"])
def test_s3_count_unresolvable_config_is_unmeasured_and_redacted(
    monkeypatch, caplog, failure
):
    endpoint = {
        "type": "iceberg",
        "connection_string": "http://rest-catalog:8181",
        "warehouse": "file:///tmp/iceberg-wh",
        "schema": "default",
        "table": "orders",
        "extra": {"catalog_type": "rest", "warehouse": "file:///tmp/iceberg-wh"},
    }
    if failure != "credentials":
        endpoint.update(
            {
                "s3.access-key-id": "count-user",
                "s3.secret-access-key": SECRET_SENTINEL,
            }
        )
    if failure != "endpoint":
        endpoint["s3.endpoint"] = "http://minio:9000"

    if failure == "parse":
        import connectors.iceberg_catalog as iceberg_catalog

        def fail_parse(_endpoint):
            raise ValueError(SECRET_SENTINEL)

        monkeypatch.setattr(
            iceberg_catalog, "parse_iceberg_catalog_config", fail_parse
        )

    def unexpected_count(*args, **kwargs):
        pytest.fail("object-store COUNT must not run without complete config")

    monkeypatch.setattr(dest_precount, "_count_object_store_key", unexpected_count)

    with caplog.at_level(logging.WARNING, logger="services.dest_precount"):
        result = dest_precount._count_iceberg_data_uri(
            "s3://warehouse/default/orders/data/part.parquet",
            endpoint=endpoint,
        )

    assert result is None
    assert "ValueError" in caplog.text
    assert SECRET_SENTINEL not in caplog.text
