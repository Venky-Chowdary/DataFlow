"""D359 live on Oracle (CI oracle-live): PG -> Oracle transformed incremental_append chain.

The 3-run chain of ``tests/helpers/d359_ia_chain_scenario.py`` (keyless
destination, recipe on ``name``, Map transform on ``note``, ADD COLUMN before
run 2, the all-NULL watermark row re-read by run 3): every run green, the
destination equal to the model row by row, no watermark row appended twice.
Local legs (SQLite, PostgreSQL): ``test_d359_ia_chain_transforms_e2e.py``.
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


def _rows(table: str) -> list[tuple]:
    from tests.helpers.d359_ia_chain_scenario import rows_by_name

    conn = _oracle_conn()
    try:
        cur = conn.cursor()
        stored = _stored(cur, table)
        if not stored:
            return []
        cur.execute(f'SELECT * FROM "{_owner()}"."{stored}"')  # nosec B608 - test-owned name
        return rows_by_name(cur.description, cur.fetchall())
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


def test_live_pg_to_oracle_transformed_append_chain_is_exact(store):
    from tests import typed_fidelity_helpers as tf
    from tests.c3_live_helpers import pg_endpoint, pg_sql
    from tests.helpers.d359_ia_chain_scenario import run_chain

    _oracle_conn().close()
    run_chain(
        store,
        src_exec=pg_sql,
        src_endpoint=pg_endpoint,
        dest_endpoint=tf.oracle_endpoint,
        dest_rows=_rows,
        drop_dest=_drop,
        cursor_type="TIMESTAMP(6)",
        upper_dest=True,
    )
