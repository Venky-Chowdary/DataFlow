"""D335 live on Oracle (CI oracle-live): PG -> Oracle incremental_append.

QA r64-r75: the watermark row appended again on run 2 (key 30) and run 3
(key 31), with or without ADD COLUMN, under a green "Append delta verified".
Local reproduction (fake of Oracle's cursor-text conversion):
``test_d335_boundary_lookup_by_key.py``.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402

pytestmark = pytest.mark.timeout(900)


def _oracle_conn():
    from services.brand_env import getenv_brand
    from tests import typed_fidelity_helpers as tf

    tf.require_oracle_env()
    oracledb = pytest.importorskip("oracledb")
    return oracledb.connect(
        user=getenv_brand("ORACLE_USER", "dataflow"),
        password=getenv_brand("ORACLE_PASSWORD", "dataflow"),
        dsn=f"{getenv_brand('ORACLE_HOST', 'localhost')}:"
        f"{getenv_brand('ORACLE_PORT', '1521')}/"
        f"{getenv_brand('ORACLE_SERVICE') or getenv_brand('ORACLE_DATABASE', 'ORCLPDB1')}",
    )


def _owner() -> str:
    from tests import typed_fidelity_helpers as tf

    return str(tf.oracle_endpoint("x").schema or "").upper()


def _stored(cur, table: str) -> str | None:
    cur.execute(
        "SELECT table_name FROM all_tables WHERE owner = :o AND UPPER(table_name) = :t",
        o=_owner(),
        t=table.upper(),
    )
    row = cur.fetchone()
    return row[0] if row else None


def _ids(table: str) -> list[int]:
    conn = _oracle_conn()
    try:
        cur = conn.cursor()
        stored = _stored(cur, table)
        if not stored:
            return []
        cur.execute(f'SELECT * FROM "{_owner()}"."{stored}" WHERE 1=0')  # nosec B608
        id_col = next(d[0] for d in cur.description if d[0].upper() == "ID")
        cur.execute(f'SELECT "{id_col}" FROM "{_owner()}"."{stored}" ORDER BY 1')  # nosec B608
        return [int(r[0]) for r in cur.fetchall()]
    finally:
        conn.close()


def _drop(table: str) -> None:
    conn = _oracle_conn()
    try:
        cur = conn.cursor()
        stored = _stored(cur, table)
        if stored:
            cur.execute(f'DROP TABLE "{_owner()}"."{stored}" PURGE')  # nosec B608
    finally:
        conn.close()


@pytest.fixture()
def store(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def test_live_pg_to_oracle_incremental_append_never_reappends_the_watermark_row(store):
    from tests import typed_fidelity_helpers as tf
    from tests.c3_live_helpers import pg_endpoint, pg_sql
    from tests.helpers.d335_boundary_scenario import run_boundary_scenario

    _oracle_conn().close()
    run_boundary_scenario(
        store,
        pg_sql=pg_sql,
        pg_endpoint=pg_endpoint,
        dest_endpoint=tf.oracle_endpoint,
        dest_ids=_ids,
        drop_dest=_drop,
    )
