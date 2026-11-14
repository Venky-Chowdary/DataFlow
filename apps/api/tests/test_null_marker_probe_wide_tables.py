"""Gate-8's NULL-marker probe on a table of 1,200 text columns (data audit DATA-9).

The probe built one ``OR`` over every text column. SQLite parses that chain
as a left-deep tree, so 1,000+ columns exceeded its expression depth: the
probe raised ``OperationalError`` and strict Gate-8 failed a perfect copy
(checksum matched, every cell exact). The predicate is now built in column
chunks sized to the engine, combined as a balanced tree, with the four
constant marker texts inline (no bind-parameter limit applies). One scan, so
a row is still counted once whichever columns hold the marker.
"""

from __future__ import annotations

import uuid

import pytest

from services.value_serializer import SQL_NULL_SENTINEL
from src.transfer.models import EndpointConfig
from tests.test_cr_d125_file_quoted_empty_fidelity import _isolated  # noqa: F401 — isolated engine stores

WIDE = 1200
#: Every engine gets all 1,200. (MySQL's table is MyISAM: InnoDB caps a table
#: at 1,017 columns and an 8 KB row, MyISAM at 4,096 columns.)
WIDTH = {"sqlite": WIDE, "postgresql": WIDE, "mysql": WIDE}
COLS = [f"c{i:04d}" for i in range(WIDE)]


def _cols(endpoint) -> list[str]:
    return COLS[: WIDTH[endpoint.format]]


def _maps(endpoint) -> list[dict]:
    return [{"source": "id", "target": "id"}] + [{"source": c, "target": c} for c in _cols(endpoint)]


def _row(endpoint, i: int, marker_at: int | None = None) -> dict:
    cols = _cols(endpoint)
    row = {"id": i, **{c: f"v{i}" for c in cols}}
    if marker_at is not None:
        row[cols[min(marker_at, len(cols) - 1)]] = SQL_NULL_SENTINEL
    return row


def _endpoint(kind: str, tmp_path, table: str) -> EndpointConfig:
    from tests.helpers.live_env import mysql_creds, mysql_up, pg_creds, pg_up

    if kind == "sqlite":
        return EndpointConfig(kind="database", format="sqlite", database=str(tmp_path / "w.db"), table=table)
    if kind == "postgresql":
        if not pg_up():
            pytest.skip("live PostgreSQL not reachable")
        c = pg_creds()
        return EndpointConfig(kind="database", format="postgresql", host=c["host"], port=int(c["port"]),
                              database=c["database"], username=c["username"], password=c["password"],
                              schema="public", table=table)
    if not mysql_up():
        pytest.skip("live MySQL not reachable")
    c = mysql_creds()
    return EndpointConfig(kind="database", format="mysql", host=c["host"], port=int(c["port"]),
                          database=c["database"], username=c["username"], password=c["password"], table=table)


@pytest.fixture(params=["sqlite", "postgresql", "mysql"])
def wide(request, tmp_path):
    import sqlalchemy as sa

    from connectors.generic_sql import get_sqlalchemy_engine
    from src.transfer.adapters import resolve_connector_config

    table = f"mw_{uuid.uuid4().hex[:8]}"
    endpoint = _endpoint(request.param, tmp_path, table)
    engine = get_sqlalchemy_engine(resolve_connector_config(endpoint))
    tbl = sa.Table(table, sa.MetaData(), sa.Column("id", sa.Integer, primary_key=True),
                   *[sa.Column(c, sa.Text) for c in _cols(endpoint)], schema=endpoint.schema or None,
                   **({"mysql_engine": "MyISAM"} if request.param == "mysql" else {}))
    with engine.begin() as conn:
        tbl.create(conn)
    try:
        yield endpoint, engine, tbl
    finally:
        with engine.begin() as conn:
            tbl.drop(conn, checkfirst=True)


def _gate(endpoint):
    from services.null_marker_guard import apply_null_marker_gate

    return apply_null_marker_gate(
        {"passed": True, "message": "Checksum matches."},
        endpoint=endpoint, mappings=_maps(endpoint), records=[_row(endpoint, 1), _row(endpoint, 2)], rows_written=2,
        dest_summary={"table": endpoint.table, "target_rows_before": 0, "sync_mode": "append"},
        validation_mode="strict",
    )


def test_a_perfect_wide_copy_passes_strict(wide):
    endpoint, engine, tbl = wide
    with engine.begin() as conn:
        conn.execute(tbl.insert(), [_row(endpoint, 1), _row(endpoint, 2)])
    recon = _gate(endpoint)
    assert recon["passed"] is True, recon.get("message")
    assert recon["null_marker_check"]["status"] == "passed", recon["null_marker_check"]


@pytest.mark.parametrize("marker_at", [0, WIDE // 2, WIDE - 1])
def test_a_leak_in_any_column_of_a_wide_table_is_found_once(wide, marker_at):
    endpoint, engine, tbl = wide
    # Two markers in one row (first and chosen column) still count one row.
    row = _row(endpoint, 2, marker_at)
    row[COLS[0]] = SQL_NULL_SENTINEL
    with engine.begin() as conn:
        conn.execute(tbl.insert(), [_row(endpoint, 1), row])
    check = _gate(endpoint)["null_marker_check"]
    assert check["status"] == "leaked", check
    assert check["destination_rows"] == 1 and check["destination_raw_rows"] == 1, check


def test_engine_run_pg_to_sqlite_of_1200_text_columns_passes_strict(tmp_path):
    """The audit's repro: PostgreSQL -> SQLite, strict overwrite, one perfect row."""
    from tests.c3_live_helpers import pg_endpoint, pg_sql
    from tests.helpers.live_env import pg_up
    from tests.test_cr_d125_file_quoted_empty_fidelity import run_transfer

    if not pg_up():
        pytest.skip("live PostgreSQL not reachable")
    src = f"mws_{uuid.uuid4().hex[:8]}"
    cols = ", ".join(f'"{c}" text' for c in COLS)
    pg_sql(f'CREATE TABLE public."{src}" (id int PRIMARY KEY, {cols})')
    try:
        pg_sql(f'INSERT INTO public."{src}" VALUES (1, {", ".join(["%s"] * WIDE)})', tuple(f"v{i}" for i in range(WIDE)))
        dest = EndpointConfig(kind="database", format="sqlite", database=str(tmp_path / "d.db"), table="w")
        result = run_transfer(pg_endpoint(src), dest)
        check = ((result.destination_summary or {}).get("reconciliation") or {}).get("null_marker_check") \
            or (getattr(result, "reconciliation", None) or {}).get("null_marker_check")
        assert result.success is True, result.error
        assert check and check["status"] == "passed", check
    finally:
        pg_sql(f'DROP TABLE IF EXISTS public."{src}"')
