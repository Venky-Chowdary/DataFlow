"""D155 — Oracle TIMESTAMP WITH TIME ZONE must keep its offset on every read.

python-oracledb returns TSTZ as a *naive* datetime, so an un-projected
``SELECT *`` turns ``08:31+05:30`` into bare ``08:31``. The typed batch readers
already rendered TSTZ through ``TO_CHAR``; the raw introspection sample, the raw
read fallbacks and the Gate-8 read-back did not, so tz-aware destinations were
blocked at g3 and MariaDB landed the wall clock as if it were UTC.

Oracle is not available here, so a fake connection plays python-oracledb: the
zero-row probe reports ``DB_TYPE_TIMESTAMP_TZ`` and a bare column comes back
naive, exactly as the real driver does.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from connectors.generic_sql import _read_table_raw, _sample_raw_table, _tz_safe_projection
from connectors.oracle_tstz import is_oracle_tz_type_name, parse_oracle_tstz_text
from connectors.sql_temporal import coerce_sql_temporal

_IST = timezone(timedelta(hours=5, minutes=30))
_EDT = timezone(timedelta(hours=-4))
INSTANTS = [
    datetime(2024, 1, 1, 8, 31, 0, 123456, tzinfo=_IST),
    datetime(2024, 6, 1, 23, 15, 0, tzinfo=_EDT),
    datetime(2024, 12, 31, 0, 0, 0, tzinfo=timezone.utc),
]


class _DbType:
    def __init__(self, name: str) -> None:
        self.name = name


_DESCRIPTION = [
    ("ID", _DbType("DB_TYPE_NUMBER"), None, None, 10, 0, False),
    ("TS", _DbType("DB_TYPE_TIMESTAMP_TZ"), None, None, None, 6, True),
]


def _to_char(dt: datetime) -> str:
    # What Oracle prints for 'YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM'.
    off = dt.utcoffset() or timedelta(0)
    sign = "-" if off < timedelta(0) else "+"
    mins = abs(int(off.total_seconds())) // 60
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + f"{sign}{mins // 60:02d}:{mins % 60:02d}"


class _Result:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows
        self.cursor = SimpleNamespace(description=_DESCRIPTION)

    def keys(self) -> list[str]:
        return ["id", "ts"]

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)

    def scalar(self) -> int:
        return len(INSTANTS)

    def partitions(self, _size: int):
        yield list(self._rows)


class _FakeOracleConn:
    """python-oracledb semantics: a bare TSTZ column is returned naive."""

    def __init__(self) -> None:
        self.sql: list[str] = []

    def execute(self, stmt: Any, params: Any = None) -> _Result:
        sql = str(stmt)
        self.sql.append(sql)
        if "1=0" in sql:
            return _Result([])
        if "COUNT(*)" in sql:
            return _Result([])
        if re.search(r"TO_CHAR\(\"TS\"", sql, re.I):
            return _Result([(i, _to_char(dt)) for i, dt in enumerate(INSTANTS)])
        return _Result([(i, dt.replace(tzinfo=None)) for i, dt in enumerate(INSTANTS)])


def test_sample_raw_table_returns_aware_instants() -> None:
    conn = _FakeOracleConn()
    headers, rows = _sample_raw_table(conn, "T", "APP", dialect="oracle")
    assert headers == ["ID", "TS"]
    assert [row[1] for row in rows] == INSTANTS
    assert all(row[1].tzinfo is not None for row in rows)
    assert "FETCH FIRST 200 ROWS ONLY" in conn.sql[-1]


def test_raw_read_fallback_keeps_offset_on_the_wire() -> None:
    conn = _FakeOracleConn()
    _headers, rows = _read_table_raw(conn, "T", "APP", 0, 10, dialect="oracle")
    wire = [row[1] for row in rows]
    assert [parse_oracle_tstz_text(w) for w in wire] == INSTANTS


def test_non_oracle_raw_sample_is_untouched() -> None:
    conn = _FakeOracleConn()
    _sample_raw_table(conn, "t", None, dialect="postgresql")
    assert conn.sql == ['SELECT * FROM "t" LIMIT 200']


def test_projection_keys_off_the_type_name() -> None:
    import sqlalchemy as sa

    class _NativeType(sa.types.UserDefinedType):
        cache_ok = True
        col_spec = "TIMESTAMP(6) WITH TIME ZONE"

        def get_col_spec(self, **_kw: Any) -> str:
            return self.col_spec

    table = sa.Table("t", sa.MetaData(), sa.Column("ts", _NativeType()))
    projected = _tz_safe_projection({"type": "oracle"}, list(table.c))
    assert "to_char" in str(projected[0]).lower()
    for name in (
        "TIMESTAMP(6) WITH TIME ZONE",
        "TIMESTAMP(3) WITH LOCAL TIME ZONE",
        "DB_TYPE_TIMESTAMP_TZ",
        "DB_TYPE_TIMESTAMP_LTZ",
    ):
        assert is_oracle_tz_type_name(name)
    assert not is_oracle_tz_type_name("TIMESTAMP(6)")
    assert not is_oracle_tz_type_name("DB_TYPE_TIMESTAMP")


def test_gate8_readback_digests_the_aware_instant() -> None:
    from services.reconciliation import canonical_checksum_from_iter
    from services.reconciliation_oracle import read_oracle_target_sample, verify_oracle_table

    conn = _FakeOracleConn()
    engine = SimpleNamespace(connect=lambda: _Ctx(conn))
    with (
        patch("connectors.generic_sql.get_sqlalchemy_engine", return_value=engine),
        patch(
            "services.reconciliation_oracle.oracle_catalog_identity",
            return_value=(None, None, {}),
        ),
    ):
        count, checksum = verify_oracle_table(
            host="h", port=1521, database="d", username="APP", password="p",
            connection_string="", schema="APP", table_name="T",
        )
        sample = read_oracle_target_sample(
            {"host": "h"}, schema="APP", table_name="T", cols=["ID", "TS"],
            keys=[], sort_key=None, limit=10,
        )
    expected = canonical_checksum_from_iter(
        [(i, dt) for i, dt in enumerate(INSTANTS)], ["id", "ts"], dest_db_type="oracle"
    )
    naive = canonical_checksum_from_iter(
        [(i, dt.replace(tzinfo=None)) for i, dt in enumerate(INSTANTS)],
        ["id", "ts"],
        dest_db_type="oracle",
    )
    assert count == len(INSTANTS)
    assert checksum == expected != naive
    assert [row["TS"] for row in sample] == INSTANTS


class _Ctx:
    def __init__(self, conn: _FakeOracleConn) -> None:
        self.conn = conn

    def __enter__(self) -> _FakeOracleConn:
        return self.conn

    def __exit__(self, *_exc: Any) -> None:
        return None


@pytest.mark.parametrize("instant", INSTANTS)
def test_offset_text_lands_as_utc_in_mariadb_datetime(instant: datetime) -> None:
    # The MariaDB "wrong instant" was the naive read, not the writer: offset
    # wire text into a zone-less DATETIME is stored as its UTC clock.
    bound = coerce_sql_temporal(_to_char(instant), "DATETIME(6)", engine="mariadb")
    assert bound == instant.astimezone(timezone.utc).replace(tzinfo=None)


def test_ltz_is_rendered_without_a_tzh_tzm_element():
    """D167: Oracle rejects TZH/TZM on WITH LOCAL TIME ZONE (ORA-01821)."""
    import sqlalchemy as sa

    from connectors.oracle_tstz import oracle_tz_select_list, oracle_tz_to_char

    description = [
        ("ID", _DbType("DB_TYPE_NUMBER")),
        ("A", _DbType("DB_TYPE_TIMESTAMP_TZ")),
        ("B", _DbType("DB_TYPE_TIMESTAMP_LTZ")),
        ("C", _DbType("DB_TYPE_TIMESTAMP")),
    ]
    sql = oracle_tz_select_list(description)
    tz_item = re.search(r'TO_CHAR\("A".*? AS "A"', sql).group(0)
    ltz_item = re.search(r'TO_CHAR\(SYS_EXTRACT_UTC\("B"\).*? AS "B"', sql).group(0)
    assert "TZH:TZM" in tz_item and "SYS_EXTRACT_UTC" not in tz_item
    assert "SYS_EXTRACT_UTC" in ltz_item and "TZH" not in ltz_item and "+00:00" in ltz_item
    assert '"C"' in sql and 'TO_CHAR("C"' not in sql
    assert "TZH" not in oracle_tz_to_char('"B"', "ltz")
    # The format's ':MI' ':SS' ':TZM' must not be read as bind variables.
    assert sa.text(f"SELECT {sql} FROM t").compile().params == {}


def test_typed_projection_renders_ltz_as_utc_text():
    import sqlalchemy as sa
    from sqlalchemy.dialects import oracle

    table = sa.Table(
        "t",
        sa.MetaData(),
        sa.Column("tz", oracle.TIMESTAMP(timezone=True)),
        sa.Column("ltz", oracle.TIMESTAMP(local_timezone=True)),
    )
    projected = _tz_safe_projection({"type": "oracle"}, list(table.c))
    sql = str(
        sa.select(*projected).compile(
            dialect=oracle.dialect(), compile_kwargs={"literal_binds": True}
        )
    )
    tz_part, ltz_part = sql.split(" AS tz, ")
    assert "TZH:TZM" in tz_part
    assert "sys_extract_utc(t.ltz)" in ltz_part and "TZH" not in ltz_part
