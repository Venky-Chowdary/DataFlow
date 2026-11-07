"""GEOGRAPHY / INTERVAL wire validation + write-path quarantine."""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.writer_common import quarantine_unfit_specialty_types  # noqa: E402
from services.schema_inference import (  # noqa: E402
    is_geography_wire,
    is_interval_wire,
    is_zero_duration_interval_bind,
)


def test_geography_wire_accepts_wkt_geojson_ewkb():
    assert is_geography_wire("POINT(1 2)")
    assert is_geography_wire("SRID=4326;POINT(-122.4 37.8)")
    assert is_geography_wire('{"type":"Point","coordinates":[-122.4,37.8]}')
    assert is_geography_wire({"type": "Point", "coordinates": [0, 0]})
    assert is_geography_wire(b"\x01\x01\x00\x00\x00")
    assert is_geography_wire("0101000000000000000000f03f0000000000000040")
    assert is_geography_wire(None) is True


def test_geography_wire_rejects_garbage():
    assert is_geography_wire("") is False
    assert is_geography_wire("not a shape") is False
    assert is_geography_wire("12345") is False
    assert is_geography_wire("{}") is False


def test_interval_wire_accepts_iso_and_sql():
    assert is_interval_wire("P1D")
    assert is_interval_wire("PT15M")
    assert is_interval_wire("1 day")
    assert is_interval_wire("2 hours 30 minutes")
    assert is_interval_wire(timedelta(days=1))
    assert is_interval_wire(None) is True


def test_interval_wire_rejects_ambiguous_numeric():
    assert is_interval_wire("") is False
    assert is_interval_wire("hello") is False
    assert is_interval_wire(42) is False
    assert is_interval_wire("12:30:00") is False  # TIME-shaped, hours <= 23
    assert is_interval_wire(0) is False
    assert is_interval_wire("00:00:00") is False


def test_zero_duration_interval_bind_is_dest_typed_only():
    """Dest INTERVAL already named the type — zero duration must bind.

    Global ``is_interval_wire`` still refuses inventing INTERVAL from 0 /
    midnight TIME so a TIME column is not remapped.
    """
    assert is_zero_duration_interval_bind(0) is True
    assert is_zero_duration_interval_bind(0.0) is True
    assert is_zero_duration_interval_bind("0") is True
    assert is_zero_duration_interval_bind("00:00:00") is True
    assert is_zero_duration_interval_bind("0:00:00") is True
    assert is_zero_duration_interval_bind(timedelta(0)) is True
    assert is_zero_duration_interval_bind(42) is False
    assert is_zero_duration_interval_bind("12:30:00") is False
    assert is_zero_duration_interval_bind(False) is False


def test_quarantine_specialty_holds_out_bad_geography():
    rows = [("not geometry", "ok"), ("POINT(0 0)", "fine")]
    details: list[dict] = []
    out = quarantine_unfit_specialty_types(
        rows,
        ["geom", "label"],
        ["GEOMETRY", "VARCHAR"],
        details,
        policy="quarantine",
    )
    assert out == [("POINT(0 0)", "fine")]
    assert details and "geography" in details[0]["reason"]


def test_quarantine_specialty_keeps_zero_duration_interval():
    rows = [(0,), ("00:00:00",), ("P3D",)]
    details: list[dict] = []
    out = quarantine_unfit_specialty_types(
        rows,
        ["dur"],
        ["INTERVAL"],
        details,
        policy="quarantine",
    )
    assert out == rows
    assert details == []


def test_quarantine_specialty_holds_out_bad_interval():
    rows = [("nope",), ("P3D",)]
    details: list[dict] = []
    out = quarantine_unfit_specialty_types(
        rows,
        ["dur"],
        ["INTERVAL"],
        details,
        policy="quarantine",
    )
    assert out == [("P3D",)]
    assert details and "interval" in details[0]["reason"]


def test_string_carrier_skips_specialty_quarantine():
    """Databricks/Iceberg STRING carriers must not invent geography binds."""
    rows = [("anything goes",)]
    details: list[dict] = []
    out = quarantine_unfit_specialty_types(
        rows,
        ["geom"],
        ["STRING"],
        details,
        policy="quarantine",
    )
    assert out == rows
    assert details == []


def test_oracle_sdo_geometry_specialty_quarantine():
    """SDO_GEOMETRY normalizes to geography and still fail-closes bad wire."""
    from services.type_system import normalize_logical_type

    assert normalize_logical_type("MDSYS.SDO_GEOMETRY") == "geography"
    rows = [("garbage",), ("POINT(1 2)",)]
    details: list[dict] = []
    out = quarantine_unfit_specialty_types(
        rows,
        ["shape"],
        ["SDO_GEOMETRY"],
        details,
        policy="quarantine",
    )
    assert out == [("POINT(1 2)",)]
    assert details and "geography" in details[0]["reason"]


def test_oracle_sdo_geometry_sa_bind_is_native_not_text():
    from sqlalchemy import Text

    from connectors.generic_sql import _sa_type_for_logical

    geo = _sa_type_for_logical("SDO_GEOMETRY", "oracle", "oracle")
    assert type(geo).__name__ == "_DialectNativeType"
    assert geo.get_col_spec() == "SDO_GEOMETRY"
    # MySQL interval is a TEXT carrier — stay honest.
    assert isinstance(_sa_type_for_logical("interval", "mysql", "mysql"), Text)
    # Postgres INTERVAL stays native.
    pg_iv = _sa_type_for_logical("INTERVAL", "postgresql", "postgresql")
    assert type(pg_iv).__name__ == "_DialectNativeType"
    assert pg_iv.get_col_spec() == "INTERVAL"
