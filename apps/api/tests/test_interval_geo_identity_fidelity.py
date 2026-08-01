"""INTERVAL YM/DS, geography SRID/polarity, GENERATED ALWAYS omit — enterprise SSOT."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.writer_common import (  # noqa: E402
    omit_generated_always_columns,
    quarantine_unfit_specialty_types,
)
from services.schema_inference import (  # noqa: E402
    geography_wire_srid,
    interval_wire_family,
)
from services.type_system import (  # noqa: E402
    ddl_type,
    geography_contract_would_collapse,
    interval_family,
    interval_family_would_collapse,
    is_generated_always_column,
    is_precision_collapse_coercion,
    normalize_logical_type,
    parse_geography_srid,
    spatial_polarity,
)


def test_interval_family_polarity():
    assert interval_family("INTERVAL YEAR TO MONTH") == "ym"
    assert interval_family("INTERVAL DAY TO SECOND") == "ds"
    assert interval_family("INTERVAL") is None
    assert interval_family_would_collapse(
        "INTERVAL YEAR TO MONTH", "INTERVAL DAY TO SECOND"
    ) is True
    assert interval_family_would_collapse(
        "INTERVAL DAY TO SECOND", "INTERVAL DAY TO SECOND"
    ) is False
    assert is_precision_collapse_coercion(
        "INTERVAL YEAR TO MONTH", "INTERVAL DAY TO SECOND"
    ) is True


def test_interval_ddl_preserves_oracle_family():
    assert ddl_type("oracle", "INTERVAL YEAR TO MONTH") == "INTERVAL YEAR TO MONTH"
    assert ddl_type("oracle", "INTERVAL DAY TO SECOND") == "INTERVAL DAY TO SECOND"


def test_interval_wire_family_detection():
    assert interval_wire_family("P1Y2M") == "ym"
    assert interval_wire_family("P1DT2H") == "ds"
    assert interval_wire_family("1-2") == "ym"
    assert interval_wire_family("1 02:03:04") == "ds"


def test_specialty_quarantine_interval_family_mismatch():
    details: list[dict] = []
    out = quarantine_unfit_specialty_types(
        [("P1Y2M",), ("P1DT2H",)],
        ["dur"],
        ["INTERVAL DAY TO SECOND"],
        details,
        policy="quarantine",
    )
    assert out == [("P1DT2H",)]
    assert details and "family" in details[0]["reason"]


def test_geography_srid_and_polarity():
    assert spatial_polarity("GEOGRAPHY(Point,4326)") == "geography"
    assert spatial_polarity("GEOMETRY(Point,4326)") == "geometry"
    assert spatial_polarity("SDO_GEOMETRY") is None  # Oracle single spatial carrier
    assert parse_geography_srid("GEOGRAPHY(Point,4326)") == 4326
    assert geography_contract_would_collapse(
        "GEOMETRY(Point,4326)", "GEOGRAPHY(Point,4326)"
    ) is True
    assert geography_contract_would_collapse(
        "GEOGRAPHY(Point,4326)", "GEOGRAPHY(Point,3857)"
    ) is True
    assert geography_contract_would_collapse(
        "GEOGRAPHY(Point,4326)", "GEOGRAPHY(Point,4326)"
    ) is False
    assert is_precision_collapse_coercion(
        "GEOMETRY", "GEOGRAPHY"
    ) is True


def test_geography_wire_srid_quarantine():
    assert geography_wire_srid("SRID=4326;POINT(1 2)") == 4326
    details: list[dict] = []
    out = quarantine_unfit_specialty_types(
        [("SRID=3857;POINT(1 2)",), ("SRID=4326;POINT(1 2)",)],
        ["geom"],
        ["GEOGRAPHY(Point,4326)"],
        details,
        policy="quarantine",
    )
    assert out == [("SRID=4326;POINT(1 2)",)]
    assert details and "SRID" in details[0]["reason"]


def test_generated_always_omit_and_g3():
    assert is_generated_always_column("INTEGER GENERATED ALWAYS") is True
    assert is_generated_always_column("INTEGER AUTO_INCREMENT") is False
    assert normalize_logical_type("INTEGER GENERATED ALWAYS") == "integer"
    assert is_precision_collapse_coercion("INTEGER", "INTEGER GENERATED ALWAYS") is True

    cols, types, rows, omitted = omit_generated_always_columns(
        ["id", "name"],
        ["INTEGER GENERATED ALWAYS", "VARCHAR"],
        [(1, "a"), (2, "b")],
    )
    assert cols == ["name"]
    assert types == ["VARCHAR"]
    assert rows == [("a",), ("b",)]
    assert omitted == ["id"]
