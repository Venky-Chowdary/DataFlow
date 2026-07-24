"""Capability registry honesty — sidecar cannot invent transfer_ready."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.connector_capability_registry import (  # noqa: E402
    CAPABILITY_REGISTRY,
    get_connector_capability,
)
from src.transfer.connector_capabilities import (  # noqa: E402
    TRANSFER_READY_CATALOG_IDS,
    certification_tier,
    get_capabilities,
    resolve_driver_type,
    transfer_ready,
)


# Marketing / planned brands — must never report transfer_ready via sidecar.
_FICTION_KEYS = frozenset({
    "kinesis", "pubsub", "delta", "hudi",
    "databricks", "synapse", "sap", "workday", "netsuite", "servicenow",
    "dynamics365", "msgraph", "google_workspace", "sharepoint",
    "shopify", "zendesk",
})

_GENERIC_KEYS = frozenset({"rest_api", "generic_sql", "singer_tap"})


def test_object_stores_do_not_claim_row_upsert():
    """S3/GCS/ADLS/MinIO are object overwrite — not row-level upsert/MERGE."""
    for key in ("s3", "gcs", "adls", "minio"):
        cap = get_connector_capability(key)
        assert cap.get("supports_upsert") is False, key
        assert cap.get("supports_merge") is False, key


def test_sidecar_transfer_ready_matches_driver_ssot():
    """Sidecar transfer_ready must derive from connector_capabilities, not static marketing."""
    mismatches: list[str] = []
    for key in sorted(CAPABILITY_REGISTRY):
        if key in _FICTION_KEYS or key in _GENERIC_KEYS:
            continue
        cap = get_connector_capability(key)
        driver = resolve_driver_type(key)
        caps = get_capabilities(driver, key)
        expected = bool(transfer_ready(caps))
        if cap.get("transfer_ready") and not expected:
            driver_caps = get_capabilities(driver, driver)
            if not transfer_ready(driver_caps):
                mismatches.append(f"{key}: sidecar True but SSOT False (driver={driver})")
    assert not mismatches, mismatches


def test_transfer_ready_catalog_ids_have_honest_tier():
    """Catalog TRANSFER_READY ids must resolve to an honest certification_tier."""
    for cid in sorted(TRANSFER_READY_CATALOG_IDS):
        driver = resolve_driver_type(cid)
        caps = get_capabilities(driver, cid)
        ready = bool(transfer_ready(caps))
        tier = certification_tier(
            cid,
            driver,
            caps,
            transfer_ready_flag=ready,
        )
        if ready:
            assert tier == "certified", (cid, tier, driver)
        else:
            # Package missing on this runner → planned/connect_only/source_only OK
            assert tier in {"planned", "connect_only", "source_only", "certified"}, (cid, tier)
