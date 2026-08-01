"""Wave 41: ClickHouse FINAL Gate-8 + INTERVAL/GEOGRAPHY bind coerce + driver resolve."""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_clickhouse_final_table_sql_appends_once():
    from connectors.generic_sql import clickhouse_final_table_sql

    assert clickhouse_final_table_sql("`db`.`t`") == "`db`.`t` FINAL"
    assert clickhouse_final_table_sql("`db`.`t` FINAL") == "`db`.`t` FINAL"
    with pytest.raises(ValueError):
        clickhouse_final_table_sql("")


def test_verify_target_routes_clickhouse_and_generic_sql_hint():
    from services.reconciliation import verify_target

    with patch(
        "services.reconciliation.verify_clickhouse_table",
        return_value=(5, "ch"),
    ) as mocked:
        assert verify_target(
            "clickhouse",
            {"host": "localhost", "type": "clickhouse"},
            schema="default",
            table_name="events",
            fallback_rows=-1,
            fallback_checksum="",
        ) == (5, "ch")
        assert mocked.called

    with patch(
        "services.reconciliation.verify_clickhouse_table",
        return_value=(3, "gs"),
    ) as mocked2:
        assert verify_target(
            "generic_sql",
            {"host": "localhost", "type": "clickhouse", "database": "default"},
            schema="default",
            table_name="events",
            fallback_rows=-1,
            fallback_checksum="",
        ) == (3, "gs")
        assert mocked2.called


def test_verify_clickhouse_table_uses_final_in_sql():
    from services.reconciliation import verify_clickhouse_table

    executed: list[str] = []

    class _Result:
        def __init__(self, rows=None, keys=None):
            self._rows = rows or []
            self._keys = keys or []

        def scalar(self):
            return 2

        def keys(self):
            return self._keys

        def __iter__(self):
            return iter(self._rows)

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            text = str(getattr(stmt, "text", stmt))
            executed.append(text)
            if "count()" in text.lower():
                return _Result()
            return _Result(rows=[(1, "a"), (2, "b")], keys=["id", "v"])

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class _Engine:
        def connect(self):
            return _Conn()

    with patch(
        "connectors.generic_sql.get_sqlalchemy_engine",
        return_value=_Engine(),
    ):
        count, chk = verify_clickhouse_table(
            host="localhost",
            database="default",
            table_name="events",
            limit=10,
        )
    assert count == 2
    assert chk
    assert any(" FINAL" in sql for sql in executed)
    assert all("FINAL" in sql for sql in executed if "FROM" in sql.upper())


def test_reconcile_step_resolves_driver_aliases():
    text = Path(__file__).resolve().parents[1].joinpath(
        "src/transfer/reconcile_step.py"
    ).read_text(encoding="utf-8")
    assert "resolve_driver_type" in text


def test_coerce_interval_wire_family_and_bq():
    from connectors.sql_bind import coerce_interval_wire, normalize_sql_bind_value

    assert coerce_interval_wire("P1DT2H", ddl_type="INTERVAL DAY TO SECOND").startswith(
        "P1DT2H"
    ) or coerce_interval_wire("P1DT2H", ddl_type="INTERVAL DAY TO SECOND") == "P1DT2H"
    with pytest.raises(ValueError, match="family"):
        coerce_interval_wire("P1Y2M", ddl_type="INTERVAL DAY TO SECOND")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_interval_wire("not-an-interval", ddl_type="INTERVAL")
    bq = coerce_interval_wire(
        timedelta(days=1, hours=2),
        ddl_type="INTERVAL",
        engine="bigquery",
    )
    assert "0-0 1 2:" in bq
    assert normalize_sql_bind_value("P1Y2M", "INTERVAL YEAR TO MONTH") == "P1Y2M"


def test_coerce_geography_wire_srid_and_wkt():
    from connectors.sql_bind import coerce_geography_wire, normalize_sql_bind_value

    assert (
        coerce_geography_wire(
            "SRID=4326;POINT(1 2)",
            ddl_type="GEOGRAPHY(Point,4326)",
        )
        == "SRID=4326;POINT(1 2)"
    )
    with pytest.raises(ValueError, match="SRID"):
        coerce_geography_wire(
            "SRID=3857;POINT(1 2)",
            ddl_type="GEOGRAPHY(Point,4326)",
        )
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_geography_wire("hello", ddl_type="GEOGRAPHY")
    assert normalize_sql_bind_value(
        "POINT(0 0)", "GEOMETRY"
    ) == "POINT(0 0)"
