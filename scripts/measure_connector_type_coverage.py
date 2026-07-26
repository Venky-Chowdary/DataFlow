#!/usr/bin/env python3
"""Measured TRANSFER_READY × logical-type × proof-tier coverage (honest).

Run from apps/api:
  PYTHONPATH=. python ../../scripts/measure_connector_type_coverage.py

Writes docs/proof/connector_type_coverage.json — catalog count ≠ live proof.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1] / "apps" / "api"
sys.path.insert(0, str(API_ROOT))

from services.type_system import DDL_TYPES  # noqa: E402
from src.transfer.connector_capabilities import (  # noqa: E402
    TRANSFER_READY_CATALOG_IDS,
    resolve_driver_type,
)
from src.transfer.registry import PRODUCTION_SKU  # noqa: E402

LOGICALS = [
    "string",
    "text",
    "integer",
    "decimal",
    "float",
    "boolean",
    "date",
    "datetime",
    "time",
    "uuid",
    "json",
    "array",
    "binary",
    "vector",
    "geography",
    "interval",
]

ALIASES = {
    "sql_server": "sqlserver",
    "amazon_dynamodb": "dynamodb",
    "amazon_s3": "s3",
    "amazon_redshift": "redshift",
    "google_bigquery": "bigquery",
    "google_cloud_storage": "gcs",
    "azure_data_lake": "adls",
    "azure_data_lake_storage": "adls",
    "azure_blob_storage": "azure_blob",
    "apache_kafka": "kafka",
    "apache_iceberg": "iceberg",
    "mongodb_atlas": "mongodb",
    "elastic_cloud": "elasticsearch",
    "amazon_elasticsearch": "elasticsearch",
    "elasticsearch_aws": "elasticsearch",
    "elasticsearch_gcp": "elasticsearch",
    "amazon_elasticache_redis": "redis",
    "azure_cache_redis": "redis",
    "google_memorystore_redis": "redis",
    "redis_enterprise": "redis",
    "csv___tsv": "csv",
    "csv_upload": "csv",
    "tsv_upload": "tsv",
    "excel_workbook": "excel",
    "json_documents": "json",
    "jsonl_stream": "jsonl",
    "parquet_lake": "parquet",
    "ndjson": "jsonl",
}

DIALECT_FOR_FAMILY = {
    "postgresql": "postgresql",
    "mysql": "mysql",
    "sqlserver": "sqlserver",
    "sqlite": "sqlite",
    "oracle": "oracle",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "mongodb": "mongodb",
    "dynamodb": "dynamodb",
    "redis": "redis",
    "elasticsearch": "elasticsearch",
    "opensearch": "elasticsearch",
    "iceberg": "iceberg",
    "generic_sql": "generic_sql",
    "duckdb": "duckdb",
    "pgvector": "postgresql",
}

PROOF_SUITES = {
    "test_production_sku_matrix.py",
    "test_production_sku_honesty.py",
    "test_universal_type_harness.py",
    "test_universal_bind_wire_matrix.py",
    "test_typed_fidelity_transfer_matrix_e2e.py",
    "test_cross_type_accuracy.py",
    "test_cross_schema_edge_types.py",
    "test_schema_engine_proof_matrix.py",
    "test_create_new_all_destinations_matrix.py",
    "test_mapping_dest_family_matrix.py",
    "test_data_rule_scenario_matrix.py",
    "test_execute_tracked_cross_sql_matrix.py",
    "test_execute_tracked_universal_matrix.py",
    "test_zero_loss_matrix.py",
    "test_connector_bidirectional_matrix.py",
    "test_live_emulator_matrix.py",
    "test_local_db_transfer_matrix_e2e.py",
}


def family(cid: str) -> str:
    d = resolve_driver_type(cid) or cid
    d = ALIASES.get(d, d)
    for prefix in (
        "postgresql_",
        "mysql_",
        "snowflake_",
        "bigquery_",
        "s3_",
        "gcs_",
        "redshift_",
        "oracle_",
    ):
        if d.startswith(prefix):
            return prefix[:-1]
    return d


def main() -> int:
    ready = sorted(TRANSFER_READY_CATALOG_IDS)
    families = sorted({family(c) for c in ready})

    sku_formats = {sf for _, sf, _, _ in PRODUCTION_SKU} | {df for _, _, _, df in PRODUCTION_SKU}

    tests_root = API_ROOT / "tests"
    test_hits: dict[str, set[str]] = defaultdict(set)
    pat_cache = {f: re.compile(rf"\b{re.escape(f)}\b", re.I) for f in families}
    for path in tests_root.rglob("test_*.py"):
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for fam, pat in pat_cache.items():
            if pat.search(text):
                test_hits[fam].add(path.name)

    quick = [
        "tests/test_catalog_honesty.py",
        "tests/test_production_sku_honesty.py",
        "tests/test_universal_type_harness.py",
        "tests/test_type_system.py",
        "tests/test_cross_type_accuracy.py",
        "tests/test_create_new_all_destinations_matrix.py",
        "tests/test_mapping_dest_family_matrix.py",
        "tests/test_schema_engine_proof_matrix.py",
    ]
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", *quick, "-q", "--tb=no"],
        cwd=str(API_ROOT),
        capture_output=True,
        text=True,
    )
    summary_lines = [
        ln
        for ln in (proc.stdout + proc.stderr).splitlines()
        if "passed" in ln or "failed" in ln
    ]
    summary = summary_lines[-1] if summary_lines else "no summary"

    rows = []
    for fam in families:
        dialect = DIALECT_FOR_FAMILY.get(fam)
        ddl_map = DDL_TYPES.get(dialect or "", {}) if dialect else {}
        type_cov: dict[str, str] = {}
        for logical in LOGICALS:
            if not dialect:
                type_cov[logical] = (
                    "wire_path"
                    if logical
                    in {
                        "string",
                        "text",
                        "json",
                        "binary",
                        "integer",
                        "decimal",
                        "boolean",
                        "date",
                        "datetime",
                    }
                    else "n/a_non_sql"
                )
            else:
                type_cov[logical] = "ddl" if logical in ddl_map else "gap"

        sku_hit = fam in sku_formats or any(
            s.startswith(fam) or fam.startswith(s) for s in sku_formats
        )
        hits = sorted(test_hits.get(fam, []))
        proof_hits = [
            h
            for h in hits
            if h in PROOF_SUITES or "matrix" in h or "fidelity" in h or "type" in h
        ]
        core_ok = all(
            type_cov[L] == "ddl"
            for L in (
                "string",
                "integer",
                "decimal",
                "boolean",
                "date",
                "datetime",
                "json",
            )
        )
        if sku_hit and dialect and core_ok:
            tier = "strong_unit"
        elif sku_hit or proof_hits:
            tier = "partial"
        else:
            tier = "thin"

        rows.append(
            {
                "family": fam,
                "catalog_tiles": sum(1 for c in ready if family(c) == fam),
                "ddl_dialect": dialect or "—",
                "sku": sku_hit,
                "proof_tier": tier,
                "test_files": len(hits),
                "matrix_files": len(proof_hits),
                "core_types_ddl": sum(
                    1
                    for L in (
                        "string",
                        "integer",
                        "decimal",
                        "boolean",
                        "date",
                        "datetime",
                        "json",
                        "uuid",
                        "binary",
                    )
                    if type_cov.get(L) == "ddl"
                ),
                "type_gaps": [L for L, v in type_cov.items() if v == "gap"],
                "type_cov": type_cov,
            }
        )

    tiers = Counter(row["proof_tier"] for row in rows)
    gap_types: Counter[str] = Counter()
    for row in rows:
        for g in row["type_gaps"]:
            gap_types[g] += 1

    out = {
        "generated": "measured_from_code",
        "honesty": (
            "TRANSFER_READY catalog ids ≠ live PRODUCTION_SKU proof. "
            "strong_unit = DDL core types + SKU mention + unit matrices. "
            "Does NOT claim zero runtime errors on every route."
        ),
        "transfer_ready_catalog_ids": len(ready),
        "driver_families": len(families),
        "production_sku_routes": len(PRODUCTION_SKU),
        "ddl_dialects": sorted(DDL_TYPES.keys()),
        "logical_types": LOGICALS,
        "quick_pytest_summary": summary,
        "quick_pytest_exit": proc.returncode,
        "tier_counts": dict(tiers),
        "families_in_sku": sum(1 for row in rows if row["sku"]),
        "type_gap_frequency": dict(gap_types.most_common()),
        "rows": rows,
    }

    proof_path = API_ROOT.parents[1] / "docs" / "proof" / "connector_type_coverage.json"
    proof_path.parent.mkdir(parents=True, exist_ok=True)
    proof_path.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print("families", len(families))
    print("tiers", dict(tiers))
    print("sku families", out["families_in_sku"])
    print("pytest", summary, "exit", proc.returncode)
    print("wrote", proof_path)
    print(f"{'family':22} {'tiles':5} {'sku':3} {'tier':12} {'coreDDL':7} {'tests':5} gaps")
    for row in rows:
        gaps = ",".join(row["type_gaps"][:4])
        print(
            f"{row['family']:22} {row['catalog_tiles']:5} {str(row['sku']):3} "
            f"{row['proof_tier']:12} {row['core_types_ddl']:7} {row['test_files']:5} {gaps}"
        )
    return 0 if proc.returncode == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
