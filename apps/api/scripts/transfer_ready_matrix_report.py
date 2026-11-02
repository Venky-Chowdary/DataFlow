#!/usr/bin/env python3
"""Phase E2 — publish TRANSFER_READY / PRODUCTION_SKU certification matrix.

Writes ``data/proofs/transfer_ready_matrix.json`` for CI / buyer evidence.
Does not invent green: routes are taken from ``PRODUCTION_SKU`` and catalog
enrichment tiers — runtime pass/fail belongs to warehouse_sku / live matrices.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def main() -> int:
    from src.transfer.connector_capabilities import (
        enrich_catalog_entry,
        transfer_live_driver_types,
    )
    from src.transfer.registry import PRODUCTION_SKU

    unique = sorted(transfer_live_driver_types())
    routes = []
    for src_kind, src_fmt, dst_kind, dst_fmt in PRODUCTION_SKU:
        routes.append(
            {
                "source_kind": src_kind,
                "source_format": src_fmt,
                "dest_kind": dst_kind,
                "dest_format": dst_fmt,
                "certification": "PRODUCTION_SKU",
            }
        )

    # Distinct engines appearing on either side of SKU routes.
    engines: set[str] = set()
    for r in routes:
        if r["source_kind"] in ("database", "warehouse", "saas", "lakehouse"):
            engines.add(r["source_format"])
        if r["dest_kind"] in ("database", "warehouse", "saas", "lakehouse", "file_export"):
            engines.add(r["dest_format"])

    # Alias honesty sample — postgresql_rds is not a second engine.
    alias_sample = enrich_catalog_entry(
        {
            "id": "postgresql_rds",
            "name": "PostgreSQL (RDS)",
            "category": "database",
            "status": "live",
            "description": "",
        }
    )

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": 1,
        "unique_transfer_ready_drivers": unique,
        "unique_driver_count": len(unique),
        "production_sku_route_count": len(routes),
        "production_sku_routes": routes,
        "sku_engine_formats": sorted(engines),
        "alias_honesty_sample": {
            "catalog_id": "postgresql_rds",
            "is_hosted_alias": alias_sample.get("is_hosted_alias"),
            "alias_of": alias_sample.get("alias_of"),
            "transfer_ready": alias_sample.get("transfer_ready"),
        },
        "claims": {
            "tile_count_is_not_live": True,
            "saas_incremental_sync": False,
            "exactly_once_cdc": False,
        },
    }

    dest = _API_ROOT / "data" / "proofs" / "transfer_ready_matrix.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {dest} ({len(routes)} PRODUCTION_SKU routes, {len(unique)} unique drivers)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
