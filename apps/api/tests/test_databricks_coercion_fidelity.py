"""Databricks/Delta create-new VARCHAR(n) + coercion fidelity_collapse honesty."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.coercion_probe import analyze_coercion  # noqa: E402
from services.type_system import ddl_type, is_precision_collapse_coercion  # noqa: E402


def test_databricks_create_new_preserves_varchar_width():
    assert ddl_type("databricks", "VARCHAR(64)") == "VARCHAR(64)"
    assert ddl_type("delta", "VARCHAR(32)") == "VARCHAR(32)"
    assert ddl_type("databricks", "STRING") == "STRING"


def test_decimal_to_float_is_precision_collapse():
    assert is_precision_collapse_coercion("DECIMAL(20,6)", "FLOAT") is True
    assert is_precision_collapse_coercion("FLOAT", "DECIMAL(12,4)") is True


def test_coercion_probe_marks_fidelity_collapse_when_samples_ok():
    report = analyze_coercion(
        sample_rows=[{"amount": "1.50"}, {"amount": "2.25"}],
        mappings=[
            {
                "source": "amount",
                "target": "amount",
                "source_type": "DECIMAL(20,6)",
                "target_type": "DOUBLE",
            }
        ],
        source_types={"amount": "DECIMAL(20,6)"},
        dest_types={"amount": "DOUBLE"},
        dest_db_type="databricks",
        table_exists=True,
    )
    cols = report.get("columns") or []
    assert cols, report
    col = cols[0]
    assert col["severity"] == "block"
    assert col.get("fidelity_collapse") is True
    assert (col.get("framing") or {}).get("kind") == "fidelity_collapse"
    assert col["ok"] == 2
    assert col["failed"] == 0
