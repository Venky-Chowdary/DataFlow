"""CLM-3, CLM-4, CLM-7, CLM-11, CLM-17: a catalog tier is what CI proved.

Certified means a green, non-skipped CI run of a job that proves the engine
(services/connector_ci_evidence.py), read through services/connector_proof.py.
An implemented engine without that proof is "Implemented — not yet proven".
A wire-compatible brand needs evidence under its own id. OpenSearch and Redis
Cluster are Planned: their drivers cannot serve them.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _doc(*green: str) -> dict:
    return {
        "schema_version": 1,
        "source_run": "test",
        "engines": {e: {"status": "ci_green", "live_verified": True} for e in green},
        "live_verified": sorted(green),
    }


@pytest.fixture()
def evidence(tmp_path, monkeypatch):
    """Point the catalog at an evidence document the test writes."""
    from services import catalog_service, connector_proof

    def _use(doc: dict | None) -> None:
        path = tmp_path / "evidence.json"
        if doc is None:
            monkeypatch.setenv(connector_proof.EVIDENCE_ENV, str(tmp_path / "absent.json"))
        else:
            path.write_text(json.dumps(doc), encoding="utf-8")
            monkeypatch.setenv(connector_proof.EVIDENCE_ENV, str(path))
        connector_proof.reset_cache()
        catalog_service._enriched_connectors.cache_clear()

    yield _use
    monkeypatch.delenv(connector_proof.EVIDENCE_ENV, raising=False)
    connector_proof.reset_cache()
    catalog_service._enriched_connectors.cache_clear()


def _row(cid: str) -> dict:
    from services.catalog_service import get_connector_by_id

    row = get_connector_by_id(cid)
    assert row, cid
    return row


@pytest.mark.parametrize("cid", ["opensearch", "amazon_elasticsearch", "redis_cluster"])
def test_opensearch_and_redis_cluster_are_planned_with_why(evidence, cid) -> None:
    evidence(_doc("redis", "postgresql"))
    from src.transfer.connector_capabilities import (
        _TRANSFER_READY_HOSTED_TWINS,
        TRANSFER_READY_CATALOG_IDS,
    )

    assert cid not in _TRANSFER_READY_HOSTED_TWINS
    assert cid not in TRANSFER_READY_CATALOG_IDS
    row = _row(cid)
    assert row["certification_tier"] == "planned"
    assert row["capability_label"] == "Planned"
    assert not row["transfer_ready"] and not row["source_ready"] and not row["dest_ready"]
    assert row["planned_reason"].startswith("Planned:")


def test_nothing_is_certified_without_evidence(evidence) -> None:
    evidence(None)
    from services.catalog_service import enriched_connectors

    rows = enriched_connectors()
    assert not [r["id"] for r in rows if r["certification_tier"] == "certified"]
    pg = _row("postgresql")
    assert pg["certification_tier"] == "implemented"
    assert pg["capability_label"] == "Implemented — not yet proven"
    # Implemented engines still work; they are just not called proven.
    assert pg["source_ready"] and pg["dest_ready"]
    assert pg["production_schedules"] is False


def test_certified_follows_the_engine_evidence(evidence) -> None:
    evidence(_doc("postgresql"))
    assert _row("postgresql")["certification_tier"] == "certified"
    assert _row("postgresql")["production_schedules"] is True
    assert _row("postgresql")["ci_evidence"]["status"] == "ci_green"
    # Same engine, managed hosting: the PostgreSQL run proves it.
    assert _row("amazon_rds_postgresql")["certification_tier"] == "certified"
    for cid in ("mysql", "kafka", "elasticsearch", "oracle", "sql_server"):
        row = _row(cid)
        assert row["certification_tier"] == "implemented", cid
        assert row["capability_label"] == "Implemented — not yet proven", cid


@pytest.mark.parametrize("cid", ["qdrant", "weaviate", "pinecone", "milvus", "kafka", "apache_kafka"])
def test_kafka_and_vector_stores_are_not_badged_without_ci(evidence, cid) -> None:
    """CLM-11: no CI job proves them, so they are never Certified / Transfer-ready."""
    evidence(_doc("postgresql", "mysql", "mongodb", "redis", "sqlite", "files"))
    row = _row(cid)
    assert row["certification_tier"] == "implemented", row["certification_tier"]
    assert row["capability_label"] == "Implemented — not yet proven"


@pytest.mark.parametrize(
    ("brand", "base"),
    [
        ("mariadb", "mysql"),
        ("gaussdb", "mysql"),
        ("goldendb", "mysql"),
        ("documentdb", "mongodb"),
        ("azure_cosmos_db", "mongodb"),
        ("redis_enterprise", "redis"),
        ("amazon_elasticache_redis", "redis"),
        ("azure_cache_redis", "redis"),
        ("google_memorystore_redis", "redis"),
        ("oracle_autonomous_warehouse", "oracle"),
    ],
)
def test_compatible_brands_need_their_own_evidence(evidence, brand, base) -> None:
    from src.transfer.connector_capabilities import _WIRE_COMPATIBLE_UNCERTIFIED_BRANDS

    assert brand in _WIRE_COMPATIBLE_UNCERTIFIED_BRANDS
    evidence(_doc(base))
    assert _row(base)["certification_tier"] == "certified"
    assert _row(brand)["certification_tier"] != "certified", brand
    evidence(_doc(base, brand))
    assert _row(brand)["certification_tier"] == "certified", brand


def test_role_wording_is_generated_from_the_picker_flags(evidence) -> None:
    """CLM-17: Qdrant has a reader; its tile must not say Destination-only."""
    evidence(_doc())
    from services.catalog_service import enriched_connectors

    qd = _row("qdrant")
    assert qd["source_ready"] and qd["dest_ready"]
    assert qd["role_label"] == "Source and destination"
    assert _row("pinecone")["role_label"] == "Destination only"
    for row in enriched_connectors():
        text = str(row.get("description") or "").lower()
        if "destination-only" in text or "destination only" in text:
            assert not row.get("source_ready"), row["id"]
        if "source-only" in text or "source only" in text:
            assert not row.get("dest_ready"), row["id"]
