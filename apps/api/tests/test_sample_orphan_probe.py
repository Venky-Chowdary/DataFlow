"""Sample orphan probe — high-quality unit tests (no invent of population RI)."""

from __future__ import annotations

from unittest.mock import patch

from preflight.constraint_hints import referential_integrity_posture
from services.sample_orphan_probe import (
    distinct_fk_values,
    orphan_values,
    probe_sample_fk_orphans,
)


def test_orphan_values_detects_missing_parents():
    assert orphan_values([1, 2, 3, 2], [1, 3]) == [2]


def test_distinct_fk_values_skips_null_and_blank():
    rows = [
        {"customer_id": 1},
        {"customer_id": None},
        {"customer_id": ""},
        {"customer_id": 1},
        {"customer_id": 2},
    ]
    assert distinct_fk_values(rows, "customer_id") == [1, 2]


def test_sample_probe_skips_without_connector():
    report = probe_sample_fk_orphans(
        sample_rows=[{"customer_id": 9}],
        mappings=[{"source": "customer_id", "target": "customer_id"}],
        foreign_keys=[
            {
                "columns": ["customer_id"],
                "referenced_table": "customers",
                "referenced_columns": ["id"],
            }
        ],
    )
    assert report["ran"] is False
    assert report["population_proof"] is False


def test_sample_probe_finds_orphans_via_parent_lookup():
    fks = [
        {
            "columns": ["customer_id"],
            "referenced_table": "customers",
            "referenced_columns": ["id"],
        }
    ]
    sample = [
        {"id": 1, "customer_id": 10},
        {"id": 2, "customer_id": 99},
    ]
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "customer_id", "target": "customer_id"},
    ]
    with patch(
        "services.sample_orphan_probe._sql_existing_parent_keys",
        return_value=[10],
    ):
        report = probe_sample_fk_orphans(
            sample_rows=sample,
            mappings=mappings,
            foreign_keys=fks,
            source_config={"type": "postgresql", "host": "localhost", "database": "t"},
            validation_mode="strict",
            fk_risk_acknowledged=False,
        )
    assert report["ran"] is True
    assert report["population_proof"] is False
    assert report["coverage"] == "sample_orphan_probe"
    assert report["orphan_count"] == 1
    assert report["findings"]
    assert report["findings"][0]["code"] == "fk_orphan_in_sample"
    assert report["findings"][0]["severity"] == "block"
    assert report["findings"][0]["population_proof"] is False


def test_sample_probe_ack_downgrades_severity():
    with patch(
        "services.sample_orphan_probe._sql_existing_parent_keys",
        return_value=[],
    ):
        report = probe_sample_fk_orphans(
            sample_rows=[{"customer_id": 1}],
            mappings=[{"source": "customer_id", "target": "customer_id"}],
            foreign_keys=[
                {
                    "columns": ["customer_id"],
                    "referenced_table": "customers",
                    "referenced_columns": ["id"],
                }
            ],
            source_config={"type": "sqlite", "database": ":memory:"},
            validation_mode="strict",
            fk_risk_acknowledged=True,
        )
    assert report["findings"][0]["severity"] == "info"


def test_referential_integrity_sample_never_proven():
    findings = [
        {
            "code": "fk_orphan_in_sample",
            "coverage": "sample_orphan_probe",
            "severity": "info",
            "message": "x",
        }
    ]
    posture = referential_integrity_posture(
        findings,
        sample_orphan_probe_ran=True,
        sample_orphan_count=0,
        population_orphan_probe_ran=False,
    )
    assert posture["proven"] is False
    assert posture["coverage"] == "sample_orphan_probe"


def test_referential_integrity_population_requires_zero_count():
    # Legacy flag alone must not invent proven.
    posture = referential_integrity_posture(
        [],
        population_orphan_probe_ran=True,
        population_orphan_count=None,
    )
    assert posture["proven"] is False

    posture_ok = referential_integrity_posture(
        [],
        population_orphan_probe_ran=True,
        population_orphan_count=0,
    )
    assert posture_ok["proven"] is True
    assert posture_ok["coverage"] == "population_orphan_probe"
