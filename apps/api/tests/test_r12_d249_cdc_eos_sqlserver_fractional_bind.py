"""D249: the CDC applier keeps DATETIME2/DATETIMEOFFSET(6) microseconds on SQL Server.

``cdc_eos_sa`` binds cells by planned DDL, not by a reflected SQLAlchemy type,
so the generic writer's DATETIME2 bind never ran. A raw ``datetime`` reached
pymssql as a TDS DATETIME and 30/30 rows landed as ``.654000`` for
``.654321`` (Postgres -> SQL Server CDC, prod 38eae820).
"""

from __future__ import annotations

import os
import urllib.parse
import uuid
from datetime import datetime, timezone

import pytest

from connectors.cdc_eos_sa import _coerce_eos_row, _upsert_row

LOGICAL = {"id": "INT4", "ts": "TIMESTAMP_NTZ(6)", "tz": "TIMESTAMPTZ(6)"}
ROW = {
    "id": 1,
    "ts": datetime(2026, 11, 2, 23, 30, 0, 654321),
    "tz": datetime(2024, 3, 10, 8, 15, 30, 12345, tzinfo=timezone.utc),
}


def test_sqlserver_fractional_cells_bind_as_full_precision_text():
    out = _coerce_eos_row(dict(ROW), LOGICAL, "mssql")
    assert out["id"] == 1
    assert out["ts"] == "2026-11-02 23:30:00.654321"
    assert isinstance(out["tz"], str) and ".012345" in out["tz"], out["tz"]


@pytest.mark.parametrize("dialect", ["postgresql", "mysql"])
def test_other_engines_keep_driver_datetime(dialect):
    out = _coerce_eos_row({"ts": ROW["ts"]}, {"ts": "TIMESTAMP_NTZ(6)"}, dialect)
    assert out["ts"] == ROW["ts"]


def _pymssql_engine():
    pw = os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD")
    if not pw:
        pytest.skip("DATAFLOW_TEST_MSSQL_PASSWORD not set (live SQL Server leg)")
    pytest.importorskip("pymssql")
    import sqlalchemy as sa

    host = os.environ.get("DATAFLOW_TEST_MSSQL_HOST", "127.0.0.1")
    port = os.environ.get("DATAFLOW_TEST_MSSQL_PORT", "1433")
    user = os.environ.get("DATAFLOW_TEST_MSSQL_USER", "sa")
    db = os.environ.get("DATAFLOW_TEST_MSSQL_DATABASE", "master")
    eng = sa.create_engine(
        f"mssql+pymssql://{user}:{urllib.parse.quote_plus(pw)}@{host}:{port}/{db}"
    )
    try:
        with eng.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"SQL Server not reachable via pymssql: {exc}")
    return eng


def test_pymssql_cdc_upsert_lands_all_six_digits():
    import sqlalchemy as sa

    eng = _pymssql_engine()
    table = f"d249_{uuid.uuid4().hex[:8]}"
    cols = ["id", "ts", "tz", "_df_lsn"]
    with eng.begin() as conn:
        conn.execute(sa.text(
            f"CREATE TABLE [{table}] (id INT PRIMARY KEY, ts DATETIME2(6), "
            "tz DATETIMEOFFSET(6), _df_lsn NVARCHAR(64))"
        ))
    try:
        with eng.begin() as conn:
            for lsn, row in (("0/1", ROW), ("0/2", {**ROW, "ts": datetime(2026, 11, 2, 23, 30, 1, 123457)})):
                values = _coerce_eos_row(dict(row), LOGICAL, "mssql")
                values["_df_lsn"] = lsn
                assert _upsert_row(conn, f"[{table}]", cols, ["id"], values, "mssql") == 1
        with eng.connect() as conn:
            got = conn.execute(sa.text(
                f"SELECT CONVERT(varchar(30), ts, 121), CONVERT(varchar(40), tz, 121) FROM [{table}]"
            )).fetchone()
        assert got[0] == "2026-11-02 23:30:01.123457", got
        assert got[1].startswith("2024-03-10 08:15:30.012345"), got
    finally:
        with eng.begin() as conn:
            conn.execute(sa.text(f"DROP TABLE [{table}]"))
