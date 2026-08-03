"""Audit probe: create-new TIMESTAMPTZ / UUID / ObjectId honesty across dialects."""
from __future__ import annotations

import json
import sys
from pathlib import Path

API = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(API))
sys.path.insert(0, str(API.parents[1] / "packages" / "preflight" / "src"))

from services.type_system import (  # noqa: E402
    assess_create_new_type_risk,
    create_new_mapping_target_type,
    ddl_type,
    is_lossy_coercion,
    is_timezone_polarity_loss,
)
from services.mapping_pipeline import run_mapping_pipeline  # noqa: E402

DBS = [
    "snowflake", "oracle", "postgresql", "mysql", "bigquery",
    "sqlserver", "redshift", "databricks", "mongodb", "duckdb",
]


def probe_type(src: str) -> list[dict]:
    rows = []
    for db in DBS:
        phys = create_new_mapping_target_type(src, db)
        risks = assess_create_new_type_risk(src, phys, destination_db_type=db)
        rows.append({
            "db": db,
            "src": src,
            "ddl": ddl_type(db, src),
            "stamp": phys,
            "risks": [r["kind"] for r in risks],
            "lossy": is_lossy_coercion(src, phys),
            "tz_loss": is_timezone_polarity_loss(src, phys) if "TIME" in src.upper() or "DATE" in src.upper() else None,
            "greenwash": (
                phys.upper().replace(" ", "") == src.upper().replace(" ", "")
                and db not in {"postgresql", "redshift", "duckdb", "mongodb"}
                and src.upper() in {"TIMESTAMPTZ", "UUID", "OBJECTID"}
            ),
        })
    return rows


def probe_pipeline(src_type: str, db: str) -> dict:
    r = run_mapping_pipeline(
        source_columns=["col"],
        target_columns=[],
        source_schemas=[{"name": "col", "inferred_type": src_type, "samples": []}],
        destination_db_type=db,
        destination_table_exists=False,
        use_llm=False,
    )
    row = r["mappings"][0]
    return {
        "db": db,
        "src": src_type,
        "target_type": row.get("target_type"),
        "risks": [x.get("kind") for x in (row.get("create_new_risks") or [])],
        "fidelity": row.get("fidelity"),
        "requires_review": row.get("requires_review"),
    }


def main() -> None:
    report = {
        "timestamptz": probe_type("TIMESTAMPTZ"),
        "uuid": probe_type("UUID"),
        "objectid": probe_type("OBJECTID"),
        "pipeline": [
            probe_pipeline("TIMESTAMPTZ", db)
            for db in ["snowflake", "oracle", "mysql", "bigquery", "databricks"]
        ],
    }
    # Flag silent create-new: no risks but dest cannot preserve domain.
    silent = []
    for family, rows in report.items():
        if family == "pipeline":
            for row in rows:
                if not row["risks"] and row["db"] in {"mysql", "bigquery", "databricks", "oracle"}:
                    # mysql should have risks; snowflake/oracle LTZ may be intentional
                    if row["db"] in {"mysql", "bigquery", "databricks"}:
                        silent.append(row)
            continue
        for row in rows:
            if row.get("greenwash") and not row["risks"]:
                silent.append(row)
    report["silent_greenwash_candidates"] = silent
    out = API / "data" / "proofs" / "connector_type_honesty_audit.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"silent": silent, "out": str(out)}, indent=2))
    for row in report["timestamptz"]:
        print(
            f"{row['db']:12} stamp={row['stamp']!s:42} "
            f"risks={row['risks']} lossy={row['lossy']} tz={row['tz_loss']}"
        )


if __name__ == "__main__":
    main()
