"""Wave 49: TIMESTAMPTZ polarity SSOT + Hive/Impala MERGE (append-only fallback)."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_sql_base_type_preserves_timestamptz_with_precision():
    from connectors.sql_temporal import sql_base_type

    assert sql_base_type("TIMESTAMP(6) WITH TIME ZONE") == "TIMESTAMPTZ"
    assert sql_base_type("timestamp(3) with time zone") == "TIMESTAMPTZ"
    assert sql_base_type("TIMESTAMPTZ(3)") == "TIMESTAMPTZ"
    assert sql_base_type("TIMESTAMP WITHOUT TIME ZONE") == "TIMESTAMP"
    assert sql_base_type("TIMESTAMP(6) WITHOUT TIME ZONE") == "TIMESTAMP"
    assert sql_base_type("DATETIME(6)") == "DATETIME"
    assert sql_base_type("DECIMAL(10,2)") == "DECIMAL"
    assert sql_base_type("TIME(6) WITH TIME ZONE") == "TIME WITH TIME ZONE"
    assert sql_base_type("DATETIMEOFFSET(7)") == "TIMESTAMPTZ"


def test_coerce_sql_temporal_timestamptz_keeps_aware_utc():
    from connectors.sql_temporal import coerce_sql_temporal

    got = coerce_sql_temporal(
        "2024-06-01T12:00:00Z", "TIMESTAMP(6) WITH TIME ZONE"
    )
    assert isinstance(got, datetime)
    assert got.tzinfo is not None
    assert got == datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    ntz = coerce_sql_temporal("2024-06-01T12:00:00Z", "TIMESTAMP(6) WITHOUT TIME ZONE")
    assert isinstance(ntz, datetime)
    assert ntz.tzinfo is None
    assert ntz == datetime(2024, 6, 1, 12, 0, 0)


def test_normalize_sql_bind_timestamptz_precision_ddl():
    from connectors.sql_bind import normalize_sql_bind_value

    got = normalize_sql_bind_value(
        "2024-06-01T15:30:00+03:00",
        "TIMESTAMP(6) WITH TIME ZONE",
        engine="postgresql",
    )
    assert isinstance(got, datetime)
    assert got.tzinfo is not None
    assert got.utcoffset() is not None
    assert got.astimezone(timezone.utc) == datetime(
        2024, 6, 1, 12, 30, 0, tzinfo=timezone.utc
    )


def test_generic_sql_to_sa_value_uses_ddl_tz_polarity():
    from connectors.generic_sql import _to_sa_value

    # logical collapsed to datetime must still honor ddl_type WITH TIME ZONE.
    got = _to_sa_value(
        "2024-08-09T01:58:42Z",
        "datetime",
        None,
        "TIMESTAMP WITH TIME ZONE",
        "postgresql",
    )
    assert isinstance(got, datetime)
    assert got.tzinfo is not None


def test_upsert_batch_routes_hive_impala_and_skips_delete():
    from connectors import generic_sql as gs

    calls: list[str] = []

    def _tr(*_a, **_k):  # noqa: ANN001
        calls.append("merge")
        return 2

    class _Col:
        def __eq__(self, other):  # noqa: ANN001
            return ("eq", other)

    class _Table:
        name = "t"
        schema = None
        c = {"id": _Col(), "v": _Col()}

        def insert(self):
            calls.append("insert")
            return "INSERT"

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            text = str(getattr(stmt, "text", stmt))
            if "DELETE" in text.upper():
                calls.append("delete")
            return MagicMock(rowcount=1)

        def rollback(self):
            calls.append("rollback")

    orig = gs._trino_merge_upsert
    gs._trino_merge_upsert = _tr  # type: ignore[assignment]
    try:
        for dialect in ("hive", "impala"):
            n = gs._upsert_batch(
                _Conn(),
                _Table(),
                [{"id": 1, "v": "x"}],
                ["id"],
                ["id", "v"],
                dialect,
            )
            assert n == 2
    finally:
        gs._trino_merge_upsert = orig  # type: ignore[assignment]

    assert calls == ["merge", "merge"]

    # Fallback path: MERGE boom → append-only, never DELETE.
    def _boom(*_a, **_k):  # noqa: ANN001
        import sqlalchemy as sa

        raise sa.exc.SQLAlchemyError("not ACID")

    calls.clear()
    gs._trino_merge_upsert = _boom  # type: ignore[assignment]
    try:
        n = gs._upsert_batch(
            _Conn(),
            _Table(),
            [{"id": 1, "v": "x"}],
            ["id"],
            ["id", "v"],
            "hive",
        )
    finally:
        gs._trino_merge_upsert = orig  # type: ignore[assignment]

    assert n == 1
    assert "delete" not in calls
    assert "insert" in calls or "rollback" in calls


def test_logical_type_from_sa_preserves_timestamptz_repr():
    from connectors.generic_sql import _logical_type_from_sa

    class _TzCol:
        timezone = None

        def __repr__(self):
            return "TIMESTAMP(6) WITH TIME ZONE"

    class _NtzCol:
        timezone = None

        def __repr__(self):
            return "TIMESTAMP WITHOUT TIME ZONE"

    assert _logical_type_from_sa(_TzCol()) == "timestamptz"
    assert _logical_type_from_sa(_NtzCol()) == "timestamp_ntz"
