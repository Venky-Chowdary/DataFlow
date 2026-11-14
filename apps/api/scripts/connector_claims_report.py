#!/usr/bin/env python3
"""Generate every public connector claim from CI evidence (CLM-5, CLM-6, CLM-7).

Two outputs, both committed and both checked by
``tests/test_clm_public_claims_generated.py``:

1. ``apps/api/data/connector_ci_evidence.json`` - the evidence snapshot the
   catalog tiers read (``services/connector_proof.py``). Written only with
   ``--results`` (the ``live-engine-*`` artifacts of one CI run) and
   ``--source-run`` (that run's URL), so the snapshot always names its run.
2. ``apps/web/src/lib/connectorClaims.generated.ts`` - what the landing,
   docs, integrations and pricing pages say: proven vs pending driver counts,
   each marketing family's proven members, and the run the proof came from.

Usage::

    gh run download <run> -p 'live-engine-*' -D /tmp/live
    python apps/api/scripts/connector_claims_report.py \\
        --results /tmp/live --source-run https://github.com/<org>/<repo>/actions/runs/<run>
    python apps/api/scripts/connector_claims_report.py          # claims only
    python apps/api/scripts/connector_claims_report.py --check  # CI drift check
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

CLAIMS_PATH = _API_ROOT.parents[1] / "apps" / "web" / "src" / "lib" / "connectorClaims.generated.ts"

# Display names for evidence engines (the proof table on the docs page).
ENGINE_NAMES = {
    "postgresql": "PostgreSQL", "mysql": "MySQL", "mongodb": "MongoDB", "redis": "Redis",
    "kafka": "Apache Kafka", "sqlserver": "SQL Server", "oracle": "Oracle",
    "bigquery": "BigQuery", "snowflake": "Snowflake", "gcs": "Google Cloud Storage",
    "adls": "Azure Data Lake Storage", "sqlite": "SQLite", "files": "File formats",
}

# CDC sources Validate accepts, by display name (preflight_cursor_gate.CDC_CAPABLE_SOURCES).
CDC_SOURCES = [
    ("PostgreSQL", "postgresql"), ("MySQL", "mysql"), ("MongoDB", "mongodb"),
    ("SQL Server", "sqlserver"), ("Oracle", "oracle"),
]

# Public marketing families: display name -> driver type. A family's badge and
# its proven / pending members are computed from evidence, never written.
MARKETING_FAMILIES: list[tuple[str, list[tuple[str, str]]]] = [
    ("Databases", [
        ("PostgreSQL", "postgresql"), ("MySQL", "mysql"), ("MongoDB", "mongodb"),
        ("SQLite", "sqlite"), ("SQL Server", "sqlserver"), ("Oracle", "oracle"),
    ]),
    ("Warehouses & lakehouse", [
        ("Snowflake", "snowflake"), ("BigQuery", "bigquery"), ("Apache Iceberg", "iceberg"),
    ]),
    ("Object storage", [
        ("Amazon S3", "s3"), ("Azure Data Lake Storage", "adls"), ("Google Cloud Storage", "gcs"),
    ]),
    ("Files", [
        ("CSV", "csv"), ("JSON", "json"), ("Parquet", "parquet"), ("Excel", "excel"),
        ("Avro", "avro"), ("ORC", "orc"),
    ]),
    ("Cache & search", [("Redis", "redis"), ("Elasticsearch", "elasticsearch"), ("DynamoDB", "dynamodb")]),
    ("Streaming", [("Apache Kafka", "kafka")]),
    ("Vector databases", [
        ("pgvector", "pgvector"), ("Pinecone", "pinecone"), ("Qdrant", "qdrant"),
        ("Weaviate", "weaviate"), ("Milvus", "milvus"),
    ]),
    ("Applications & APIs", [
        ("Salesforce", "salesforce"), ("HubSpot", "hubspot"), ("Stripe", "stripe"),
        ("REST APIs", "rest_api"),
    ]),
]


def write_snapshot(results: list[str], source_run: str, out: Path) -> dict[str, Any]:
    from services.connector_ci_evidence import evidence_document, load_manifest, load_results

    doc = evidence_document(load_manifest(), load_results(Path(p) for p in results))
    doc["source_run"] = source_run
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return doc


def build_claims() -> dict[str, Any]:
    """Public claims from the evidence snapshot plus the product's driver set."""
    from services.connector_proof import driver_proven, engine_for, evidence_snapshot, proven_and_pending
    from src.transfer.connector_capabilities import transfer_live_driver_types

    snap = evidence_snapshot()
    drivers = transfer_live_driver_types(declared=True)
    split = proven_and_pending(drivers)
    engines = snap.get("engines") or {}
    families = []
    for family, items in MARKETING_FAMILIES:
        rows = [
            {
                "name": name,
                "driver": driver,
                "proven": driver_proven(driver),
                "emulated": bool((engines.get(engine_for(driver)) or {}).get("emulated")),
            }
            for name, driver in items
        ]
        families.append({"family": family, "items": rows})
    from services.preflight_cursor_gate import CDC_CAPABLE_SOURCES

    proof_rows = []
    for engine, record in sorted(engines.items()):
        if not (record or {}).get("live_verified"):
            continue
        green = [r for r in record.get("runs") or [] if r.get("status") == "green"]
        proof_rows.append(
            {
                "engine": engine,
                "name": ENGINE_NAMES.get(engine, engine),
                "jobs": sorted({str(r.get("job")) for r in green}),
                "executed": sum(int(r.get("executed") or 0) for r in green),
                "emulated": bool(record.get("emulated")),
            }
        )
    cdc = [
        {"name": name, "driver": driver, "proven": driver_proven(driver)}
        for name, driver in CDC_SOURCES
        if driver in CDC_CAPABLE_SOURCES
    ]
    return {
        "about": (
            "Generated by apps/api/scripts/connector_claims_report.py from "
            "apps/api/data/connector_ci_evidence.json. Do not edit by hand."
        ),
        "evidence_source_run": str(snap.get("source_run") or ""),
        "evidence_generated_at": str(snap.get("generated_at") or ""),
        "engine_status": {e: str((v or {}).get("status") or "") for e, v in sorted(engines.items())},
        "proven_drivers": split["proven"],
        "pending_drivers": split["pending"],
        "proven_count": len(split["proven"]),
        "pending_count": len(split["pending"]),
        "families": families,
        "proof_rows": proof_rows,
        "cdc_sources": cdc,
    }


def render(claims: dict[str, Any]) -> str:
    body = json.dumps(claims, indent=2, sort_keys=True, ensure_ascii=False)
    return (
        "/**\n"
        " * Generated by apps/api/scripts/connector_claims_report.py from\n"
        " * apps/api/data/connector_ci_evidence.json. Do not edit by hand:\n"
        " * tests/test_clm_public_claims_generated.py fails when it drifts.\n"
        " */\n"
        f"{_TS_TYPES}\n"
        f"export const CONNECTOR_CLAIMS: ConnectorClaims = {body};\n"
    )


_TS_TYPES = """export interface ConnectorClaims {
  about: string;
  evidence_source_run: string;
  evidence_generated_at: string;
  engine_status: Record<string, string>;
  proven_drivers: string[];
  pending_drivers: string[];
  proven_count: number;
  pending_count: number;
  families: { family: string; items: { name: string; driver: string; proven: boolean; emulated: boolean }[] }[];
  proof_rows: { engine: string; name: string; jobs: string[]; executed: number; emulated: boolean }[];
  cdc_sources: { name: string; driver: string; proven: boolean }[];
}
"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--results", action="append", default=[], help="live-engine result JSON or folder")
    parser.add_argument("--source-run", default="", help="URL of the CI run the results came from")
    parser.add_argument("--claims-out", default=str(CLAIMS_PATH))
    parser.add_argument("--check", action="store_true", help="fail when the committed claims drift")
    args = parser.parse_args(argv)

    from services import connector_proof

    if args.results:
        if not args.source_run.startswith("https://github.com/"):
            parser.error("--source-run must be the GitHub Actions run URL the results came from")
        write_snapshot(args.results, args.source_run, connector_proof.SNAPSHOT)
        connector_proof.reset_cache()
    text = render(build_claims())
    out = Path(args.claims_out)
    if args.check:
        current = out.read_text(encoding="utf-8") if out.is_file() else ""
        if current != text:
            print(f"{out} is stale: run scripts/connector_claims_report.py", file=sys.stderr)
            return 1
        return 0
    out.write_text(text, encoding="utf-8")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
