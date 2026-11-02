"""Go-live timestamp defects: Oracle fractional seconds, MySQL TIMESTAMP, DST.

DEF-B-029: a plan of TIMESTAMP(7) must not CREATE Oracle TIMESTAMP(6), and
reading the column back must keep the scale the catalog stored.

RETEST-R1 case 3.3: a MySQL TIMESTAMP stamp stays TIMESTAMP on MySQL and
MariaDB. A PostgreSQL timestamptz still lands on DATETIME(6). A naive
MySQL TIMESTAMP sample reaches PostgreSQL TIMESTAMPTZ without a zone
declaration; DATETIME is not rewritten.

DEF-C-048: an ambiguous America/New_York wall clock follows PostgreSQL
(the later occurrence). A spring-forward gap is an error, not an invented
instant.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

sa = pytest.importorskip("sqlalchemy")

from connectors.generic_sql import _sa_type_for_logical  # noqa: E402
from services.schema_introspect import (  # noqa: E402
    _oracle_apply_timestamp_scale,
    _oracle_to_logical,
)
from services.source_engine_scope import bind_source_engine  # noqa: E402
from services.transform_engine import apply_transform  # noqa: E402
from services.type_system import (  # noqa: E402
    is_lossy_coercion,
    temporal_precision_would_narrow,
)


def _ddl(logical: str, dialect_name: str, db_type: str = "") -> str:
    from sqlalchemy.dialects import mysql, oracle

    dialect = {
        "mysql": mysql.dialect(),
        "mariadb": mysql.dialect(),
        "oracle": oracle.dialect(),
    }[dialect_name]
    return str(
        _sa_type_for_logical(logical, dialect_name, db_type or dialect_name).compile(
            dialect=dialect
        )
    )


def test_oracle_timestamp_keeps_declared_fractional_seconds() -> None:
    assert _ddl("TIMESTAMP(7) WITH TIME ZONE", "oracle") == (
        "TIMESTAMP(7) WITH TIME ZONE"
    )
    assert _ddl("TIMESTAMP_TZ(7)", "oracle") == "TIMESTAMP(7) WITH TIME ZONE"
    assert _ddl("TIMESTAMP_NTZ(7)", "oracle") == "TIMESTAMP(7)"
    assert _ddl("TIMESTAMP(7)", "oracle") == "TIMESTAMP(7)"
    assert _ddl("TIMESTAMP_LTZ(9)", "oracle") == "TIMESTAMP(9) WITH LOCAL TIME ZONE"
    # Oracle accepts at most 9 fractional digits.
    assert _ddl("TIMESTAMP(12)", "oracle") == "TIMESTAMP(9)"
    # No declared precision stays the stock type. Oracle's own default is 6.
    assert _ddl("datetime", "oracle") == "TIMESTAMP"
    assert _ddl("TIMESTAMP WITH TIME ZONE", "oracle") == "TIMESTAMP WITH TIME ZONE"


def test_oracle_catalog_keeps_timestamp_scale() -> None:
    assert _oracle_to_logical("TIMESTAMP(6)") == "TIMESTAMP_NTZ(6)"
    assert _oracle_to_logical("TIMESTAMP(7) WITH TIME ZONE") == "TIMESTAMP_TZ(7)"
    assert _oracle_to_logical("TIMESTAMP WITH TIME ZONE") == "TIMESTAMP_TZ"
    assert _oracle_apply_timestamp_scale("TIMESTAMP", 7) == "TIMESTAMP(7)"
    assert _oracle_apply_timestamp_scale("TIMESTAMP WITH TIME ZONE", 7) == (
        "TIMESTAMP(7) WITH TIME ZONE"
    )
    # A type string that already names (n) is not rewritten from DATA_SCALE.
    assert _oracle_apply_timestamp_scale("TIMESTAMP(6)", 7) == "TIMESTAMP(6)"
    assert _oracle_apply_timestamp_scale("NUMBER", 2) == "NUMBER"
    assert _oracle_to_logical(_oracle_apply_timestamp_scale("TIMESTAMP", 7)) == (
        "TIMESTAMP_NTZ(7)"
    )
    assert _oracle_to_logical(
        _oracle_apply_timestamp_scale("TIMESTAMP WITH TIME ZONE", 7)
    ) == "TIMESTAMP_TZ(7)"

    # The column the writer just created matches the source. An explicit (6)
    # live column, and a bare TIMESTAMP WITH TIME ZONE (Oracle default 6),
    # still narrow a (7) source.
    assert (
        temporal_precision_would_narrow(
            "TIMESTAMP_TZ(7)", "TIMESTAMP_TZ(7)", dest_db="oracle"
        )
        is False
    )
    assert (
        is_lossy_coercion("TIMESTAMP_TZ(7)", "TIMESTAMP_TZ(7)", dest_db="oracle")
        is False
    )
    assert (
        temporal_precision_would_narrow(
            "TIMESTAMP_TZ(7)", "TIMESTAMP(6) WITH TIME ZONE", dest_db="oracle"
        )
        is True
    )
    assert (
        temporal_precision_would_narrow(
            "TIMESTAMP_NTZ(7)", "TIMESTAMP(7)", dest_db="oracle"
        )
        is False
    )
    assert (
        temporal_precision_would_narrow(
            "TIMESTAMP_TZ(7)", "TIMESTAMP WITH TIME ZONE", dest_db="oracle"
        )
        is True
    )


def test_mysql_timestamp_stamp_stays_timestamp() -> None:
    assert _ddl("TIMESTAMP(6)", "mysql") == "TIMESTAMP(6)"
    assert _ddl("TIMESTAMP(6)", "mariadb", "mariadb") == "TIMESTAMP(6)"
    assert _ddl("TIMESTAMP(9)", "mysql") == "TIMESTAMP(6)"
    with bind_source_engine("mysql"):
        assert _ddl("TIMESTAMPTZ", "mysql") == "TIMESTAMP(6)"
        assert _ddl("TIMESTAMP", "mysql") == "TIMESTAMP(6)"
        assert _ddl("TIMESTAMPTZ", "mariadb", "mariadb") == "TIMESTAMP(6)"
    with bind_source_engine("mariadb"):
        assert _ddl("TIMESTAMPTZ", "mysql") == "TIMESTAMP(6)"


def test_postgres_timestamptz_stays_mysql_datetime() -> None:
    assert _ddl("TIMESTAMPTZ", "mysql") == "DATETIME(6)"
    assert _ddl("TIMESTAMP WITH TIME ZONE", "mysql") == "DATETIME(6)"
    assert _ddl("TIMESTAMP", "mysql") == "DATETIME(6)"
    assert _ddl("TIMESTAMP_NTZ(6)", "mysql") == "DATETIME(6)"
    assert _ddl("datetime", "mysql") == "DATETIME(6)"
    with bind_source_engine("postgresql"):
        assert _ddl("TIMESTAMPTZ", "mysql") == "DATETIME(6)"
        assert _ddl("TIMESTAMP(6)", "mysql") == "TIMESTAMP(6)"


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(timezone.utc)


def test_ambiguous_new_york_wall_clock_matches_postgres() -> None:
    value, err = apply_transform("2026-11-01 01:30", "assume_timezone:America/New_York")
    assert err is None
    # Later occurrence (EST). The earlier one is 05:30Z and disagrees with
    # PostgreSQL ``ts_local AT TIME ZONE 'America/New_York'``.
    assert _utc(value) == datetime(2026, 11, 1, 6, 30, tzinfo=timezone.utc)


def test_spring_forward_gap_is_quarantined() -> None:
    value, err = apply_transform("2026-03-08 02:30", "assume_timezone:America/New_York")
    assert value is None
    assert err is not None
    assert "America/New_York" in err
    assert "does not exist" in err


def test_leap_day_and_unambiguous_instants_stay() -> None:
    leap, err = apply_transform(
        "2028-02-29 23:59:59.999999", "assume_timezone:America/New_York"
    )
    assert err is None
    assert _utc(leap) == datetime(2028, 3, 1, 4, 59, 59, 999999, tzinfo=timezone.utc)
    summer, err = apply_transform("2026-07-04 12:00", "assume_timezone:America/New_York")
    assert err is None
    assert _utc(summer) == datetime(2026, 7, 4, 16, 0, tzinfo=timezone.utc)
    # An offset the source already stated is not moved.
    stated, err = apply_transform(
        "2024-01-05T10:30:00+05:00", "assume_timezone:America/New_York"
    )
    assert err is None
    assert stated == "2024-01-05T10:30:00+05:00"


def test_mysql_timestamp_reaches_postgres_without_rewriting_datetime() -> None:
    """A naive MySQL TIMESTAMP sample is the pinned UTC instant.

    Setting source_timezone to clear that refusal also rewrote DATETIME.
    The catalog instant is wired on its own. The wall clock is not.
    """
    from services.coercion_probe import analyze_coercion

    sample = [{"ts": "2024-03-01 12:00:00", "wall": "2024-03-01 12:00:00"}]
    mappings = [
        {"source": "ts", "target": "ts", "target_type": "TIMESTAMPTZ"},
        {"source": "wall", "target": "wall", "target_type": "TIMESTAMP"},
    ]
    with bind_source_engine("mysql"):
        report = analyze_coercion(
            sample_rows=sample,
            mappings=mappings,
            source_types={"ts": "TIMESTAMPTZ(6)", "wall": "TIMESTAMP_NTZ(6)"},
            dest_types={"ts": "TIMESTAMPTZ", "wall": "TIMESTAMP"},
            dest_db_type="postgresql",
            table_exists=True,
        )
    by_source = {col["source"]: col for col in report["columns"]}
    assert report["has_blocking_failures"] is False
    assert by_source["ts"]["failed"] == 0
    assert "assume_timezone" not in str(by_source["ts"].get("transform") or "")
    assert by_source["wall"]["source_type"] == "TIMESTAMP_NTZ(6)"

    with bind_source_engine("mysql"):
        wall_as_instant = analyze_coercion(
            sample_rows=[{"wall": "2024-03-01 12:00:00"}],
            mappings=[{"source": "wall", "target": "wall", "target_type": "TIMESTAMPTZ"}],
            source_types={"wall": "TIMESTAMP_NTZ(6)"},
            dest_types={"wall": "TIMESTAMPTZ"},
            dest_db_type="postgresql",
            table_exists=True,
        )
    assert wall_as_instant["has_blocking_failures"] is True

    with bind_source_engine("postgresql"):
        other = analyze_coercion(
            sample_rows=[{"ts": "2024-03-01 12:00:00"}],
            mappings=[{"source": "ts", "target": "ts", "target_type": "TIMESTAMPTZ"}],
            source_types={"ts": "TIMESTAMPTZ"},
            dest_types={"ts": "TIMESTAMPTZ"},
            dest_db_type="postgresql",
            table_exists=True,
        )
    assert other["has_blocking_failures"] is True


def test_oracle_timestamp_tz_on_postgres_timestamptz_is_not_a_collapse() -> None:
    """DEF-B-009. PostgreSQL has one aware timestamp.

    TIMESTAMP_TZ and TIMESTAMPTZ are two spellings of that instant, so the
    pair is not a fidelity collapse. An offset label on MySQL DATETIME(6)
    stays a collapse (DEF-B-016).
    """
    from services.mapping_proof import mapping_fidelity

    assert is_lossy_coercion(
        "TIMESTAMP_TZ", "TIMESTAMPTZ", dest_db="postgresql"
    ) is False
    assert is_lossy_coercion(
        "TIMESTAMP_TZ(6)", "TIMESTAMPTZ(6)", dest_db="postgresql"
    ) is False
    verdict = mapping_fidelity(
        {"source": "ts", "target": "ts", "transform": "none"},
        declared_source_type="TIMESTAMP_TZ",
        declared_target_type="TIMESTAMPTZ",
        destination_db_type="postgresql",
        dest_table_exists=True,
    )
    assert verdict["verdict"] == "preserve"
    assert verdict["requires_risk_contract"] is False
    assert is_lossy_coercion(
        "TIMESTAMP WITH TIME ZONE", "DATETIME(6)", dest_db="mysql"
    ) is True
    assert is_lossy_coercion(
        "TIMESTAMP_TZ", "DATETIME(6)", dest_db="mysql"
    ) is True
