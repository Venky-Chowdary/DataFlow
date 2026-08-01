"""Wave 62: PG pg_enum domain carriers + MySQL YEAR/BIT(1) bind polarity."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_format_enum_domain_carrier_and_pg_enum_fetch():
    from services.schema_introspect import (
        _pg_fetch_enum_labels,
        format_enum_domain_carrier,
    )

    assert format_enum_domain_carrier(["sad", "ok", "happy"]) == (
        "ENUM('sad','ok','happy')"
    )
    assert format_enum_domain_carrier(["it's"]) == "ENUM('it\\'s')"

    cur = MagicMock()
    cur.fetchall.return_value = [
        (100, "sad"),
        (100, "ok"),
        (100, "happy"),
        (200, "a"),
    ]
    labels = _pg_fetch_enum_labels(cur, [100, 200, 100])
    assert labels[100] == ["sad", "ok", "happy"]
    assert labels[200] == ["a"]
    # Ordered IN query with deduped oids.
    sql = cur.execute.call_args[0][0]
    assert "pg_enum" in sql.lower()
    assert cur.execute.call_args[0][1] == (100, 200)


def test_pg_fetch_columns_applies_enum_domain(monkeypatch):
    from services import schema_introspect as si

    cur = MagicMock()
    # name, dtype, nullable, identity, default, coll, det, gen, oid, typtype
    cur.fetchall.side_effect = [
        [
            ("mood", "mood", "YES", "", None, "", True, "", 42, "e"),
            ("note", "text", "YES", "", None, "", True, "", 25, "b"),
        ],
        [(42, "sad"), (42, "ok")],
    ]

    cols = si._pg_fetch_columns(cur, "public", "t")
    by_name = {c["name"]: c["inferred_type"] for c in cols}
    assert by_name["mood"] == "ENUM('sad','ok')"
    assert by_name["note"] == "TEXT"


def test_coerce_year_string_zero_vs_numeric_zero():
    from connectors.sql_bind import coerce_year_wire, normalize_sql_bind_value
    from services.type_system import expand_mysql_year, year_value_fits

    assert expand_mysql_year(0) == 0
    assert expand_mysql_year("0") == 2000
    assert expand_mysql_year("00") == 2000
    assert expand_mysql_year(69) == 2069
    assert expand_mysql_year(70) == 1970
    assert coerce_year_wire("0") == 2000
    assert coerce_year_wire(0) == 0
    assert normalize_sql_bind_value("00", "YEAR", engine="mysql") == 2000
    assert year_value_fits("0") is True
    assert year_value_fits(1800) is False
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_year_wire(1800)


def test_year_quarantine_expands_string_zero():
    from connectors.writer_common import quarantine_unfit_years

    details: list[dict] = []
    out = quarantine_unfit_years(
        [("0",), (1800,), (0,)],
        ["y"],
        ["YEAR"],
        details,
        policy="quarantine",
    )
    assert out == [(2000,), (0,)]
    assert details and "1901" in details[0]["reason"]


def test_bit1_boolean_polarity_mysql():
    from connectors.sql_bind import normalize_sql_bind_value

    assert normalize_sql_bind_value("0", "BIT(1)", engine="mysql") == 0
    assert normalize_sql_bind_value("1", "BIT(1)", engine="mysql") == 1
    assert normalize_sql_bind_value(False, "BIT", engine="mysql") == 0
    assert normalize_sql_bind_value(True, "BIT(1)", engine="postgresql") is True
