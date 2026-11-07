"""OpenShift hosting plane — never a dest engine, never a k8s API write."""

from __future__ import annotations

import pytest

from services.openshift_dest import (
    OpenShiftDestError,
    accuracy_contract,
    apply_openshift_hosting,
    classify_openshift_store,
    resolve_openshift_service_host,
)
from src.transfer.connector_capabilities import resolve_driver_type


def test_openshift_catalog_tile_is_postgresql_hosted_alias() -> None:
    assert resolve_driver_type("openshift") == "postgresql"
    assert resolve_driver_type("cnpg") == "postgresql"
    assert resolve_driver_type("crunchy_pgo") == "postgresql"
    row = __import__(
        "services.catalog_service", fromlist=["enrich_catalog_entry"]
    ).enrich_catalog_entry(
        {
            "id": "openshift",
            "name": "OpenShift PostgreSQL",
            "category": "database",
            "status": "live",
            "description": "",
        }
    )
    assert row["driver_type"] == "postgresql"
    assert row["is_hosted_alias"] is True
    assert row["alias_of"] == "postgresql"


def test_service_dns_is_cluster_local() -> None:
    assert (
        resolve_openshift_service_host(service="orders-pg", namespace="payments")
        == "orders-pg.payments.svc.cluster.local"
    )


def test_refuse_openshift_as_a_store() -> None:
    with pytest.raises(OpenShiftDestError, match="hosting plane"):
        classify_openshift_store("openshift")


def test_apply_fills_empty_host_from_service() -> None:
    cfg = apply_openshift_hosting(
        {
            "type": "openshift",
            "host": "",
            "openshift_service": "orders-pg",
            "openshift_namespace": "payments",
        }
    )
    assert cfg["host"] == "orders-pg.payments.svc.cluster.local"
    assert cfg["type"] == "postgresql"
    assert cfg["openshift_hosting"] is True
    assert cfg["port"] == 5432


def test_unrelated_namespace_extra_does_not_rewrite_host() -> None:
    cfg = apply_openshift_hosting(
        {
            "type": "snowflake",
            "host": "xy123.snowflakecomputing.com",
            "namespace": "ANALYTICS",
            "service": "not-openshift",
        }
    )
    assert cfg["host"] == "xy123.snowflakecomputing.com"
    assert cfg["type"] == "snowflake"
    assert "openshift_hosting" not in cfg


def test_apply_does_not_clobber_port_forward_host() -> None:
    cfg = apply_openshift_hosting(
        {
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 15432,
            "openshift_service": "orders-pg",
            "openshift_namespace": "payments",
        }
    )
    assert cfg["host"] == "127.0.0.1"
    assert cfg["port"] == 15432
    assert cfg["openshift_store"] == "postgresql"


def test_refuse_blank_openshift_without_a_store_host() -> None:
    with pytest.raises(OpenShiftDestError, match="not a destination"):
        apply_openshift_hosting({"type": "openshift", "host": ""})


def test_accuracy_contract_never_claims_streams_exactly_once() -> None:
    c = accuracy_contract()
    assert "at-least-once" in c["cdc"]
    assert "named fixture" in c["one_hundred_percent"]
    assert "GSI / LSI copies" in c["not_migrated"]
