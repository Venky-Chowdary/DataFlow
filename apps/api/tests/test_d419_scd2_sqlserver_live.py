"""D419 live on SQL Server (CI cdc-sqlserver, pyodbc and pymssql legs).

QA r57-r75: PG → SQL Server scd2 runs 2-3 FAILED "SCD2 history verification
failed: version recount: 0 versions closed this run, expected 2" over an exact
history; the closed version's ``valid_to`` was stored to the millisecond
(…23.661000) and its successor's ``valid_from`` to the microsecond (…23.661678)
in ``datetimeoffset(7)``: a 678 µs window with no current version. The close
UPDATE bound the stamp as a raw driver parameter (pymssql renders milliseconds
and drops the offset); it is now ISO text cast server-side. The driver model
and the same scenario on local engines: ``test_d419_scd2_close_stamp_precision.py``.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from tests.test_d335_boundary_sqlserver_live import _cfg, _drop, _endpoint, _ready  # noqa: E402

pytestmark = [
    pytest.mark.timeout(900),
    pytest.mark.skipif(
        not _ready(),
        reason="SQL Server not reachable (set DATAFLOW_SQLSERVER_* / DATAFLOW_TEST_MSSQL_PASSWORD)",
    ),
]


@pytest.fixture()
def store(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _history(table: str) -> list[tuple]:
    from tests.c3_live_helpers import sqlalchemy_sql

    # Text with all seven digits and the offset: no driver rounds it on the way out.
    return sqlalchemy_sql(
        _cfg(),
        "SELECT id, CONVERT(VARCHAR(40), valid_from, 127), CONVERT(VARCHAR(40), valid_to, 127), "
        f"CAST(is_current AS INT) FROM dbo.{table}",  # nosec B608
        fetch=True,
    ) or []


def test_live_pg_to_sqlserver_scd2_counts_closed_versions_with_no_gap(store):
    from tests.c3_live_helpers import pg_endpoint, pg_sql
    from tests.helpers.d419_scd2_scenario import run_scd2_history_scenario

    run_scd2_history_scenario(
        store,
        pg_sql=pg_sql,
        pg_endpoint=pg_endpoint,
        dest_endpoint=_endpoint,
        dest_history=_history,
        drop_dest=_drop,
    )
