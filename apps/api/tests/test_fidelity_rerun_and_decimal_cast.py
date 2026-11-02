"""CSV overwrite reruns and a fitting decimal cast are not fidelity collapse.

A real DECIMAL narrowing still is.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.coercion_probe import analyze_coercion
from services.root_cause_engine import build_root_causes
from services.type_coercion import is_lossy_coercion


def test_csv_text_into_existing_numeric_columns_is_not_a_collapse():
    report = analyze_coercion(
        sample_rows=[
            {"id": "1", "amount": "12.50"},
            {"id": "2", "amount": "100.00"},
        ],
        mappings=[
            {"source": "id", "target": "id", "source_type": "VARCHAR", "target_type": "INTEGER"},
            {
                "source": "amount",
                "target": "amount",
                "source_type": "VARCHAR",
                "target_type": "NUMERIC(12,2)",
            },
        ],
        source_types={"id": "VARCHAR", "amount": "VARCHAR"},
        dest_types={"id": "INTEGER", "amount": "NUMERIC(12,2)"},
        dest_db_type="postgresql",
        table_exists=True,
    )
    assert report["has_blocking_failures"] is False, report["columns"]
    assert all(not col.get("fidelity_collapse") for col in report["columns"])


def test_fractional_text_into_integer_still_collapses():
    assert is_lossy_coercion(
        "VARCHAR",
        "INTEGER",
        dest_db="postgresql",
        dest_table_exists=True,
        population=["12.50"],
    )


def test_unmeasured_text_to_integer_stays_lossy():
    assert is_lossy_coercion("VARCHAR", "INTEGER", dest_db="postgresql", dest_table_exists=True)


def test_fitting_decimal_cast_is_not_a_fidelity_root():
    preflight = {
        "gates": [
            {
                "id": "g3_type_compat",
                "status": "block",
                "message": "1 type coercion issue(s): amount DECIMAL(10,2) cast to DECIMAL(18,2)",
                "details": {
                    "issues": ["amount DECIMAL(10,2) cast to DECIMAL(18,2)"],
                    "issues_detail": [
                        {
                            "source": "amount",
                            "source_type": "DECIMAL(10,2)",
                            "target_type": "DECIMAL(18,2)",
                            "message": "decimal cast",
                        }
                    ],
                },
            }
        ],
        "blockers": [],
    }
    kinds = [root.kind for root in build_root_causes(preflight)]
    assert "fidelity_collapse" not in kinds


def test_real_decimal_narrowing_stays_a_fidelity_root():
    assert is_lossy_coercion(
        "DECIMAL(38,10)",
        "DECIMAL(18,2)",
        dest_db="sqlserver",
        dest_table_exists=True,
    )
    preflight = {
        "gates": [
            {
                "id": "g3_type_compat",
                "status": "block",
                "message": "declared fidelity collapse DECIMAL(38,10) → DECIMAL(18,2)",
                "details": {
                    "fidelity_collapse": True,
                    "issues_detail": [
                        {
                            "source": "amount",
                            "source_type": "DECIMAL(38,10)",
                            "target_type": "DECIMAL(18,2)",
                            "fidelity_collapse": True,
                        }
                    ],
                },
            }
        ],
        "blockers": [],
    }
    kinds = [root.kind for root in build_root_causes(preflight)]
    assert "fidelity_collapse" in kinds
