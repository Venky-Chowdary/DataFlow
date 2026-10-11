"""Canonical carrier properties behind the temporal / NULL / Decimal audit.

Each property is stated once against the shared layer and swept across the
engines it applies to, so a connector cannot pass by special-casing a value.
These cases are deterministic and need only ``requirements.txt``; the
``check_*`` helpers are reused by the hypothesis sweep in
``test_temporal_audit_canonical_carriers_property.py``.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

sa = pytest.importorskip("sqlalchemy")
from sqlalchemy.dialects import mssql  # noqa: E402

from connectors.elasticsearch_writer import _to_es_value  # noqa: E402
from connectors.generic_sql import (  # noqa: E402
    _logical_type_from_sa,
    _sa_type_for_logical,
    _to_sa_value,
)
from connectors.writer_common import quarantine_cell_wire  # noqa: E402
from services.schema_inference import infer_column  # noqa: E402
from services.type_system import temporal_precision_would_narrow  # noqa: E402
from services.value_serializer import (  # noqa: E402
    SQL_NULL_SENTINEL,
    is_frame_missing,
    present_cell_text,
)
from services.vector_embedding import vector_cell_token  # noqa: E402

ENGINES = [
    ("postgresql", "postgresql"),
    ("mysql", "mysql"),
    ("mssql", "sqlserver"),
    ("sqlite", "sqlite"),
    ("snowflake", "snowflake"),
    ("oracle", "oracle"),
    ("duckdb", "duckdb"),
]

def _inferred(values: list[str]) -> str:
    return str(infer_column(values, field_name="created")["logical_type"]).upper()


# --- 1. date vs datetime ----------------------------------------------------


def check_one_real_time_of_day_makes_a_timestamp(midnights: list[date], stamp: datetime) -> None:
    values = [f"{d.isoformat()}T00:00:00" for d in midnights] + [stamp.isoformat()]
    assert _inferred(values) == "TIMESTAMP"
    assert _inferred([d.isoformat() for d in midnights] + [stamp.isoformat(sep=" ")]) == "TIMESTAMP"


def check_date_only_column_stays_a_date(days: list[date]) -> None:
    assert _inferred([d.isoformat() for d in days]) == "DATE"
    assert _inferred([f"{d.isoformat()}T00:00:00" for d in days]) == "DATE"


@pytest.mark.parametrize(
    ("midnights", "stamp"),
    [
        ([date(2024, 1, 15), date(2024, 3, 1)], datetime(2024, 2, 28, 14, 30)),
        ([date(1900, 1, 1)], datetime(2200, 12, 31, 23, 59, 59)),
        ([date(2024, 1, 15)] * 5, datetime(2024, 1, 15, 0, 0, 1)),
        ([date(1999, 12, 31), date(2000, 2, 29)], datetime(2000, 1, 1, 12, 0)),
    ],
)
def test_one_real_time_of_day_makes_the_column_a_timestamp(midnights, stamp):
    check_one_real_time_of_day_makes_a_timestamp(midnights, stamp)


@pytest.mark.parametrize(
    "days",
    [[date(2024, 1, 15), date(2024, 3, 1)], [date(1900, 1, 1)], [date(2000, 2, 29)] * 4],
)
def test_a_date_only_column_stays_a_date(days):
    check_date_only_column_stays_a_date(days)


@pytest.mark.parametrize(("dialect", "db_type"), ENGINES)
def test_declared_date_binds_a_date_and_datetime_binds_a_datetime(dialect, db_type):
    date_t = _sa_type_for_logical("DATE", dialect, db_type)
    stamp_t = _sa_type_for_logical("TIMESTAMP", dialect, db_type)
    bound_date = _to_sa_value("2024-01-15", "DATE", date_t, dialect_name=dialect, db_type=db_type)
    assert type(bound_date) is date
    bound = _to_sa_value(
        "2024-02-28T14:30:00", "TIMESTAMP", stamp_t, dialect_name=dialect, db_type=db_type
    )
    if isinstance(bound, str):
        # SQL Server binds naive DATETIME2 as precision-exact text.
        bound = datetime.fromisoformat(bound[:26])
    assert isinstance(bound, datetime)
    assert (bound.hour, bound.minute) == (14, 30)


# --- 2. SQL Server temporal family vs precision-bearing DDL -----------------


@pytest.mark.parametrize("stamp", ["TIMESTAMPTZ", "DATETIMEOFFSET", "TIMESTAMP WITH TIME ZONE"])
@pytest.mark.parametrize(
    ("dialect", "db_type"),
    [e for e in ENGINES if e[0] not in {"mysql", "oracle"}],
)
def test_zone_aware_stamps_keep_timezone_polarity(stamp, dialect, db_type):
    sa_t = _sa_type_for_logical(stamp, dialect, db_type)
    assert getattr(sa_t, "timezone", False) is True, (dialect, sa_t)


@pytest.mark.parametrize("precision", range(0, 8))
def test_sqlserver_datetimeoffset_keeps_declared_precision_and_polarity(precision):
    sa_t = _sa_type_for_logical(f"DATETIMEOFFSET({precision})", "mssql", "sqlserver")
    assert sa_t.timezone is True
    assert str(sa_t.compile(dialect=mssql.dialect())) == f"DATETIMEOFFSET({precision})"
    assert _logical_type_from_sa(sa_t) == f"DATETIMEOFFSET({precision})"


@pytest.mark.parametrize("precision", range(0, 8))
def test_sqlserver_datetime2_keeps_declared_precision_naive(precision):
    sa_t = _sa_type_for_logical(f"TIMESTAMP_NTZ({precision})", "mssql", "sqlserver")
    assert sa_t.timezone is False
    assert _logical_type_from_sa(sa_t) == f"DATETIME2({precision})"


def test_sqlserver_bare_datetime2_is_the_seven_digit_family():
    sa_t = _sa_type_for_logical("datetime", "mssql", "sqlserver")
    assert str(sa_t.compile(dialect=mssql.dialect())) == "DATETIME2"
    assert _logical_type_from_sa(sa_t) == _logical_type_from_sa(mssql.DATETIME2(precision=7))


def check_sqlserver_aware_bind_is_the_same_instant(stamp: datetime, minutes: int) -> None:
    aware = stamp.replace(tzinfo=timezone(timedelta(minutes=minutes)))
    sa_t = _sa_type_for_logical("TIMESTAMPTZ", "mssql", "sqlserver")
    out = _to_sa_value(aware.isoformat(), "TIMESTAMPTZ", sa_t, dialect_name="mssql", db_type="sqlserver")
    assert out.tzinfo is not None
    assert out == aware


@pytest.mark.parametrize("minutes", [-14 * 60, -330, 0, 330, 345, 14 * 60])
@pytest.mark.parametrize(
    "stamp",
    [datetime(1900, 1, 1), datetime(2024, 2, 28, 14, 30), datetime(2200, 12, 31, 23, 59, 59)],
)
def test_sqlserver_aware_bind_is_the_same_instant(stamp, minutes):
    check_sqlserver_aware_bind_is_the_same_instant(stamp, minutes)


# --- 3. NaN is SQL NULL at the NULL-polarity boundary -----------------------

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")

_FRAME_MISSING = [float("nan"), np.float64("nan"), np.float32("nan"), pd.NA, pd.NaT]


@pytest.mark.parametrize("missing", _FRAME_MISSING, ids=repr)
def test_frame_missing_is_sql_null_never_text(missing):
    assert is_frame_missing(missing)
    assert quarantine_cell_wire(missing) == SQL_NULL_SENTINEL
    assert present_cell_text(missing) is None
    assert vector_cell_token(missing) == ""


@pytest.mark.parametrize("present", ["", " ", "nan", "NaN", Decimal("NaN"), 0, 0.0, False, b""], ids=repr)
def test_present_values_are_not_frame_missing(present):
    assert not is_frame_missing(present)
    assert quarantine_cell_wire(present) != SQL_NULL_SENTINEL


def test_oracle_empty_string_stays_distinct_from_sql_null():
    assert quarantine_cell_wire("") == ""
    assert quarantine_cell_wire(None) == SQL_NULL_SENTINEL
    assert quarantine_cell_wire(float("nan")) == SQL_NULL_SENTINEL


# --- 4. Elasticsearch Decimal keeps fixed-point text and scale --------------


def check_es_decimal_binds_fixed_point_text(value: Decimal) -> None:
    scale = max(0, -value.as_tuple().exponent)
    out = _to_es_value(value, f"DECIMAL(38,{scale})")
    assert isinstance(out, str)
    assert out == format(value, "f")
    assert Decimal(out) == value
    assert Decimal(out).as_tuple().exponent == value.as_tuple().exponent


@pytest.mark.parametrize(
    "text",
    ["10.5000", "0", "0.00", "-0.001", "1000", "0.0000000001", "-99999999999999999.9", "12345678901234567.1234567890"],
)
def test_es_decimal_binds_fixed_point_text_with_scale(text):
    check_es_decimal_binds_fixed_point_text(Decimal(text))


# --- 5. Declared nanoseconds narrow on every microsecond destination --------

_DEST_TIMESTAMP = {
    "postgresql": "TIMESTAMP(6)",
    "mysql": "DATETIME(6)",
    "duckdb": "TIMESTAMP",
    "sqlserver": "DATETIME2(7)",
    "oracle": "TIMESTAMP(9)",
    "snowflake": "TIMESTAMP_NTZ(9)",
}
_DEST_CAP = {"postgresql": 6, "mysql": 6, "duckdb": 6, "sqlserver": 7, "oracle": 9, "snowflake": 9}


@pytest.mark.parametrize("dest", sorted(_DEST_TIMESTAMP))
@pytest.mark.parametrize("declared", range(0, 10))
def test_declared_source_precision_narrows_exactly_when_digits_drop(dest, declared):
    narrows = temporal_precision_would_narrow(
        f"TIMESTAMP_NTZ({declared})", _DEST_TIMESTAMP[dest], dest_db=dest
    )
    cap = _DEST_CAP[dest]
    if declared <= cap:
        assert narrows is False
    elif declared == 7:
        # SQL Server's DATETIME2 default into a destination at its own maximum.
        assert narrows is False
    else:
        assert narrows is True


@pytest.mark.parametrize("dest", ["postgresql", "mysql", "duckdb"])
def test_bare_snowflake_ceiling_is_not_declared_evidence(dest):
    assert not temporal_precision_would_narrow("TIMESTAMP_NTZ", _DEST_TIMESTAMP[dest], dest_db=dest)


def check_typed_decimal_fit_is_exact(value: Decimal) -> None:
    from connectors.sql_bind import coerce_decimal_wire
    from connectors.writer_common import fits_decimal

    assert fits_decimal(value, 16, 6)
    assert coerce_decimal_wire(value, ddl_type="DECIMAL(16,6)") == value
    scale = max(0, -value.as_tuple().exponent)
    if scale:
        narrower = scale - 1
        significant = value != value.quantize(Decimal(1).scaleb(-narrower))
        assert fits_decimal(value, 16, narrower) is (not significant)


@pytest.mark.parametrize(
    "text",
    ["12.345", "1.000", "0.123", "1234.567", "-999999999.999999", "0.000001", "100", "2.50"],
)
def test_typed_decimal_fit_is_exact_never_a_locale_reparse(text):
    check_typed_decimal_fit_is_exact(Decimal(text))
