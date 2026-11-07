"""Module 11 — Population orphan scan is the only path to RI proven."""

from __future__ import annotations

from unittest.mock import patch

from preflight.constraint_hints import referential_integrity_posture
from services.population_orphan_probe import probe_population_fk_orphans


def test_population_probe_fail_closed_without_connector():
    report = probe_population_fk_orphans(
        child_table="orders",
        mappings=[{"source": "customer_id", "target": "customer_id"}],
        foreign_keys=[
            {
                "columns": ["customer_id"],
                "referenced_table": "customers",
                "referenced_columns": ["id"],
            }
        ],
        validation_mode="strict",
    )
    assert report["ran"] is False
    assert report["population_proof"] is False
    assert report["complete"] is False
    assert report["findings"]
    assert report["findings"][0]["code"] == "population_orphan_probe_unavailable"


def test_population_probe_composite_zero_orphans_proven():
    with patch(
        "services.population_orphan_probe._sql_population_orphan_scan",
        return_value={"orphan_count": 0, "examples": []},
    ) as scan:
        report = probe_population_fk_orphans(
            child_table="orders",
            mappings=[],
            foreign_keys=[
                {
                    "columns": ["a", "b"],
                    "referenced_table": "parents",
                    "referenced_columns": ["x", "y"],
                }
            ],
            source_config={"type": "postgresql", "host": "localhost", "database": "t"},
            validation_mode="strict",
        )
    assert scan.called
    assert scan.call_args.kwargs["child_columns"] == ["a", "b"]
    assert scan.call_args.kwargs["parent_columns"] == ["x", "y"]
    assert report["ran"] is True
    assert report["complete"] is True
    assert report["population_proof"] is True
    assert report["findings"] == []


def test_population_probe_composite_arity_mismatch_never_proven():
    report = probe_population_fk_orphans(
        child_table="orders",
        mappings=[],
        foreign_keys=[
            {
                "columns": ["a", "b"],
                "referenced_table": "parents",
                "referenced_columns": ["id"],
            }
        ],
        source_config={"type": "postgresql", "host": "localhost", "database": "t"},
        validation_mode="strict",
    )
    assert report["ran"] is True
    assert report["complete"] is False
    assert report["population_proof"] is False
    assert report["findings"][0]["code"] == "composite_fk_not_probed"


def test_population_probe_zero_orphans_proven():
    with patch(
        "services.population_orphan_probe._sql_population_orphan_scan",
        return_value={"orphan_count": 0, "examples": []},
    ):
        report = probe_population_fk_orphans(
            child_table="orders",
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
        )
    assert report["ran"] is True
    assert report["complete"] is True
    assert report["orphan_count"] == 0
    assert report["population_proof"] is True
    assert report["findings"] == []


def test_population_probe_finds_orphans():
    with patch(
        "services.population_orphan_probe._sql_population_orphan_scan",
        return_value={"orphan_count": 3, "examples": [99, 100]},
    ):
        report = probe_population_fk_orphans(
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
    assert report["ran"] is True
    assert report["population_proof"] is False
    assert report["orphan_count"] == 3
    assert report["findings"][0]["code"] == "fk_orphan_in_population"
    assert report["findings"][0]["severity"] == "block"


def test_ri_posture_population_supersedes_sample_findings():
    """Clean population scan proves RI even when sample findings exist."""
    findings = [
        {
            "code": "fk_orphan_in_sample",
            "coverage": "sample_orphan_probe",
            "severity": "info",
            "message": "sample only",
        }
    ]
    posture = referential_integrity_posture(
        findings,
        sample_orphan_probe_ran=True,
        sample_orphan_count=1,
        population_orphan_probe_ran=True,
        population_orphan_count=0,
    )
    assert posture["proven"] is True
    assert posture["coverage"] == "population_orphan_probe"


def test_ri_posture_incomplete_population_count_none_not_proven():
    posture = referential_integrity_posture(
        [],
        population_orphan_probe_ran=True,
        population_orphan_count=None,
    )
    assert posture["proven"] is False
