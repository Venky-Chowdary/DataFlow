"""FK orphan examples use present_cell_text, not str(value).

str(True) invented True in the operator message. Reader-wired
SQL_NULL_SENTINEL is not a customer token. Coverage stays
population_orphan_probe / dest RI scan — a display fix does not
invent RI proof.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.destination_ri_probe import _orphan_example_text  # noqa: E402
from services.population_orphan_probe import probe_population_fk_orphans  # noqa: E402
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402


def _report(examples: list[object], *, count: int | None = None):
    with patch(
        "services.population_orphan_probe._sql_population_orphan_scan",
        return_value={"orphan_count": count if count is not None else len(examples), "examples": examples},
    ):
        return probe_population_fk_orphans(
            child_table="orders",
            mappings=[{"source": "customer_id", "target": "customer_id"}],
            foreign_keys=[
                {
                    "columns": ["customer_id"],
                    "referenced_table": "customers",
                    "referenced_columns": ["id"],
                }
            ],
            source_config={"type": "postgresql", "host": "localhost", "database": "t"},
            validation_mode="strict",
            fk_risk_acknowledged=False,
        )


def test_examples_use_write_path_text():
    report = _report([True, SQL_NULL_SENTINEL, 99])
    assert report["checks"][0]["examples"] == ["true", "", "99"]
    message = report["findings"][0]["message"]
    assert "true" in message
    assert "True" not in message
    assert SQL_NULL_SENTINEL not in message
    assert report["population_proof"] is False
    assert report["coverage"] == "population_orphan_probe"


def test_dest_ri_example_text_matches_write_path():
    assert _orphan_example_text((True, SQL_NULL_SENTINEL, 99)) == "true++99"
    assert _orphan_example_text((2, 101)) == "2+101"
    assert _orphan_example_text((99,)) == "99"


def test_int_examples_still_surface():
    report = _report([99, 100], count=3)
    assert report["checks"][0]["examples"] == ["99", "100"]
    assert "99" in report["findings"][0]["message"]
    assert report["orphan_count"] == 3
