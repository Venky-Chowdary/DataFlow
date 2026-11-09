"""Oracle identity census, catalog spelling and SCD2 close-stamp precision (live).

Oracle folds unquoted names to upper case and rejects an unquoted identifier
that starts with ``_``. Before these fixes the destination key census quoted
the logical ``"id"`` (ORA-00904) and aliased its composite subquery
``_df_key_hits`` (ORA-00911), so keyed append/upsert reruns lost their keyed
expected delta and failed reconcile as a plain append. SCD2 bound the close
stamp as DATE, dropping the microseconds ``valid_from`` keeps.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

from services.dest_precount import destination_key_hits
from services.dialect_profiles import match_stored_names, stored_identifier
from services.scd2_engine import _oracle_stamp_bind
from tests.test_execute_tracked_universal_matrix import (
    _build_db_endpoint,
    _endpoint_reachable,
)


def test_match_stored_names_prefers_exact_then_unique_casefold() -> None:
    stored = ["ID", "REGION", "_deleted", "MixedCase", "dup", "DUP"]
    assert match_stored_names(stored, ["id", "region", "_deleted", "MixedCase"]) == [
        "ID",
        "REGION",
        "_deleted",
        "MixedCase",
    ]
    assert match_stored_names(stored, ["Dup", "missing"]) == ["Dup", "missing"]


def test_oracle_stored_identifier_folds_only_unquoted_names() -> None:
    names = ["is_current", "_deleted", "date", "id", "MixedCase", "valid_from"]
    assert [stored_identifier("oracle", n) for n in names] == [
        "IS_CURRENT",
        "_deleted",
        "date",
        "ID",
        "MixedCase",
        "VALID_FROM",
    ]


def test_oracle_stamp_bind_keeps_microseconds() -> None:
    naive = datetime(2026, 11, 8, 20, 1, 16, 187007)  # noqa: DTZ001 - naive branch
    assert _oracle_stamp_bind(naive) == (
        "2026-11-08 20:01:16.187007",
        "TO_TIMESTAMP(:ts, 'YYYY-MM-DD HH24:MI:SS.FF6')",
    )
    aware = naive.replace(tzinfo=timezone.utc)
    param, sql = _oracle_stamp_bind(aware)
    assert param == "2026-11-08 20:01:16.187007 +00:00"
    assert sql.startswith("TO_TIMESTAMP_TZ(")


@pytest.fixture()
def oracle_cfg(tmp_path: Path):
    os.environ.setdefault("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    ep = _build_db_endpoint("oracle", tmp_path, "dst", "census")
    if not _endpoint_reachable(ep):
        pytest.skip("Oracle not reachable on the configured host/port")
    cfg = dict(vars(ep))
    cfg.update(cfg.get("extra") or {})
    import oracledb

    conn = oracledb.connect(
        user=cfg["username"],
        password=cfg["password"],
        dsn=f'{cfg["host"]}:{cfg["port"]}/{cfg["database"]}',
    )
    table = f"df_census_{uuid.uuid4().hex[:8]}"
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {table} (region VARCHAR2(8), id NUMBER(10), ts TIMESTAMP(6))")
    cur.executemany(
        f"INSERT INTO {table} (region, id) VALUES (:1, :2)",
        [("eu", 1), ("us", 1), ("eu", 2)],
    )
    conn.commit()
    try:
        yield cfg, table, conn
    finally:
        cur.execute(f"DROP TABLE {table} PURGE")
        conn.close()


def test_single_key_census_uses_catalog_spelling(oracle_cfg) -> None:
    cfg, table, _ = oracle_cfg
    hits = destination_key_hits(
        "oracle",
        cfg,
        schema=cfg.get("schema") or "",
        table_name=table,
        key_columns=["id"],
        keys=[(1,), (2,), (99,)],
    )
    assert hits == 2


def test_composite_key_census_runs_on_oracle(oracle_cfg) -> None:
    cfg, table, _ = oracle_cfg
    hits = destination_key_hits(
        "oracle",
        cfg,
        schema=cfg.get("schema") or "",
        table_name=table,
        key_columns=["region", "id"],
        keys=[("eu", 1), ("us", 1), ("us", 2)],
    )
    assert hits == 2


def test_close_stamp_bind_round_trips_microseconds(oracle_cfg) -> None:
    _, table, conn = oracle_cfg
    stamp = datetime(2026, 11, 8, 20, 1, 16, 187007)  # noqa: DTZ001 - TIMESTAMP is naive
    param, sql = _oracle_stamp_bind(stamp)
    cur = conn.cursor()
    cur.execute(f"UPDATE {table} SET ts = {sql} WHERE id = 1", ts=param)
    conn.commit()
    cur.execute(f"SELECT DISTINCT ts FROM {table} WHERE id = 1")
    assert cur.fetchall() == [(stamp,)]
