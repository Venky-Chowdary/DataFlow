"""D335 live on SQL Server (CI cdc-sqlserver): PG -> SQL Server incremental_append.

QA r66-r75: key 31 appended twice on run 3, with or without ADD COLUMN, under
a green "Append delta verified". Local reproduction and the SQL Server value
shapes (DATETIME 1/300 s, DATETIME2(7) text): ``test_d335_boundary_lookup_by_key.py``.
"""

from __future__ import annotations

import ipaddress
import os
import socket
from pathlib import Path

import pytest

from services.brand_env import getenv_brand

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402



def _cfg() -> dict:
    host = getenv_brand("SQLSERVER_HOST", "localhost")
    cfg = {
        "type": "sqlserver",
        "host": host,
        "port": int(getenv_brand("SQLSERVER_PORT", "1433") or 1433),
        "database": getenv_brand("SQLSERVER_DATABASE", "dataflow"),
        "username": getenv_brand("SQLSERVER_USER", "sa"),
        "password": getenv_brand(
            "SQLSERVER_PASSWORD", os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD", "")
        ),
    }
    name = str(host or "").strip().strip("[]").lower()
    try:
        loopback = name in {"localhost", "localhost."} or ipaddress.ip_address(name).is_loopback
    except ValueError:
        loopback = False
    if loopback:
        cfg["trust_server_certificate"] = True
    return cfg


def _ready() -> bool:
    from tests.c3_live_helpers import sqlalchemy_sql

    cfg = _cfg()
    if not cfg["password"]:
        return False
    try:
        with socket.create_connection((cfg["host"], cfg["port"]), timeout=2):
            pass
        return sqlalchemy_sql(cfg, "SELECT 1", fetch=True) == [(1,)]
    except Exception:  # noqa: BLE001 - not reachable: the module skips
        return False


pytestmark = [
    pytest.mark.timeout(900),
    pytest.mark.skipif(
        not _ready(),
        reason="SQL Server not reachable (set DATAFLOW_SQLSERVER_* / DATAFLOW_TEST_MSSQL_PASSWORD)",
    ),
]


def _endpoint(table: str):
    from src.transfer.models import EndpointConfig

    cfg = _cfg()
    extra = {"trust_server_certificate": True} if cfg.get("trust_server_certificate") else {}
    return EndpointConfig(
        kind="database", format="sqlserver", host=cfg["host"], port=cfg["port"],
        database=cfg["database"], username=cfg["username"], password=cfg["password"],
        schema="dbo", table=table, extra=extra,
    )


def _ids(table: str) -> list[int]:
    from tests.c3_live_helpers import sqlalchemy_sql

    rows = sqlalchemy_sql(
        _cfg(),
        f"IF OBJECT_ID('dbo.{table}') IS NOT NULL SELECT id FROM dbo.{table} ORDER BY id",  # nosec B608
        fetch=True,
    )
    return [int(r[0]) for r in rows or []]


def _drop(table: str) -> None:
    from tests.c3_live_helpers import sqlalchemy_sql

    sqlalchemy_sql(_cfg(), f"IF OBJECT_ID('dbo.{table}') IS NOT NULL DROP TABLE dbo.{table}")


@pytest.fixture()
def store(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def test_live_pg_to_sqlserver_incremental_append_never_reappends_the_watermark_row(store):
    from tests.c3_live_helpers import pg_endpoint, pg_sql
    from tests.helpers.d335_boundary_scenario import run_boundary_scenario

    run_boundary_scenario(
        store,
        pg_sql=pg_sql,
        pg_endpoint=pg_endpoint,
        dest_endpoint=_endpoint,
        dest_ids=_ids,
        drop_dest=_drop,
    )


def test_live_pg_to_sqlserver_datetime_destination_compares_at_1_300_s(store):
    """A pre-existing DATETIME cursor column stores 1/300 s ticks: .003 is held
    as 0.00333..., .017 as 0.01666... The boundary row is found at the carrier's
    precision, never appended again. The source values are ones DATETIME can
    store (last millisecond digit 0, 3 or 7): the writer quarantines any other
    value rather than rounding it after commit. TIMESTAMP(3) -> DATETIME is not
    a narrowing at plan, so the run is not refused before it reaches the
    boundary."""
    from tests.c3_live_helpers import pg_endpoint, pg_sql, sqlalchemy_sql
    from tests.helpers.d335_boundary_scenario import run_boundary_scenario

    def _pre_create(table: str) -> None:
        sqlalchemy_sql(
            _cfg(),
            f"CREATE TABLE dbo.{table} (id BIGINT, note NVARCHAR(40), updated_at DATETIME)",
        )

    run_boundary_scenario(
        store,
        pg_sql=pg_sql,
        pg_endpoint=pg_endpoint,
        dest_endpoint=_endpoint,
        dest_ids=_ids,
        drop_dest=_drop,
        source_cursor_type="TIMESTAMP(3)",
        ts=lambda i: f"2026-11-24 10:{i:02d}:00.{(i * 10 + (3, 7, 0)[i % 3]) % 1000:03d}",
        pre_create=_pre_create,
    )


def _rows(table: str) -> list[tuple]:
    from tests.c3_live_helpers import sqlalchemy_sql

    cols = [
        str(r[0]).lower()
        for r in sqlalchemy_sql(
            _cfg(),
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' "
            f"AND TABLE_NAME = '{table}' ORDER BY ORDINAL_POSITION",  # nosec B608 - test-owned name
            fetch=True,
        )
        or []
    ]
    if not cols:
        return []
    wanted = [c for c in ("id", "name", "note", "region") if c in cols]
    rows = sqlalchemy_sql(
        _cfg(), f"SELECT {', '.join(wanted)} FROM dbo.{table}", fetch=True  # nosec B608
    )
    out = []
    for row in rows or []:
        rec = dict(zip(wanted, row))
        out.append((int(rec["id"]), rec.get("name"), rec.get("note"), rec.get("region")))
    return out


def test_live_pg_to_sqlserver_transformed_append_chain_is_exact(store):
    """D359 (QA r67/r72: run 3 after ADD COLUMN failed "Transform-applied proof
    failed ... changed: 31" over the boundary key's two copies). The 3-run chain
    of ``tests/helpers/d359_ia_chain_scenario.py``: keyless destination, recipe
    on ``name``, Map transform on ``note``, ADD COLUMN before run 2. Every run
    green, the destination equal to the model row by row."""
    from tests.c3_live_helpers import pg_endpoint, pg_sql
    from tests.helpers.d359_ia_chain_scenario import run_chain

    run_chain(
        store,
        src_exec=pg_sql,
        src_endpoint=pg_endpoint,
        dest_endpoint=_endpoint,
        dest_rows=_rows,
        drop_dest=_drop,
        cursor_type="TIMESTAMP(6)",
        upper_dest=True,
    )
