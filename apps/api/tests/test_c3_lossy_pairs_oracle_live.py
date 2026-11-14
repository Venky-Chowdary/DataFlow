"""C3 live proof on Oracle (CI ``cdc-oracle`` job: Oracle only, opt-in).

An Oracle ``DATE`` keeps a time of day to the second. From a live Oracle table:

* the catalog type the product introspects, with the row's live time of day,
  is graded ``lossy_cast`` by the Validate verdict into an existing PostgreSQL
  ``DATE``, and create-new picks ``TIMESTAMP(0)`` (graded clean);
* real engine path into a SQLite file (the only destination that job has): an
  existing ``DATE`` column is refused at Validate; create-new keeps the time.

Skips unless ``DATAFLOW_ORACLE_ENABLE=1`` and the ``DATAFLOW_ORACLE_*`` login are set.
"""

from __future__ import annotations

import socket
import sqlite3

import pytest

from services.brand_env import getenv_brand
from tests.c3_live_helpers import refused_at_validate, run_transfer, sqlalchemy_sql, token
from tests.test_commit_safe_review_round2 import isolated  # noqa: F401 — fixture


def _cfg() -> dict:
    service = (getenv_brand("ORACLE_SERVICE") or getenv_brand("ORACLE_DATABASE") or "ORCLPDB1").strip()
    user = getenv_brand("ORACLE_USER", "").strip()
    return {
        "type": "oracle",
        "host": getenv_brand("ORACLE_HOST", "localhost").strip(),
        "port": int(getenv_brand("ORACLE_PORT", "1521") or 1521),
        "database": service,
        "service_name": service,
        "username": user,
        "password": getenv_brand("ORACLE_PASSWORD", "").strip(),
        "schema": (getenv_brand("ORACLE_SCHEMA") or user).strip().upper(),
    }


def _ready() -> bool:
    if getenv_brand("ORACLE_ENABLE", "").strip().lower() not in {"1", "true", "yes", "on"}:
        return False
    cfg = _cfg()
    if not (cfg["username"] and cfg["password"]):
        return False
    try:
        with socket.create_connection((cfg["host"], cfg["port"]), timeout=3):
            pass
        return sqlalchemy_sql(cfg, "SELECT 1 FROM DUAL", fetch=True) == [(1,)]
    except Exception:  # noqa: BLE001 — not reachable: the module skips
        return False


pytestmark = pytest.mark.skipif(
    not _ready(),
    reason="Oracle live IT disabled — set DATAFLOW_ORACLE_ENABLE=1 plus DATAFLOW_ORACLE_USER/PASSWORD",
)


@pytest.fixture()
def oracle_dates():
    table = f"C3OD_{token()}".upper()
    cfg = _cfg()
    sqlalchemy_sql(cfg, f"CREATE TABLE {table} (ID NUMBER(10) PRIMARY KEY, AT DATE)")
    sqlalchemy_sql(
        cfg,
        f"INSERT INTO {table} VALUES (1, TO_DATE('2024-03-10 13:45:07', 'YYYY-MM-DD HH24:MI:SS'))",
    )
    try:
        yield table
    finally:
        sqlalchemy_sql(cfg, f"DROP TABLE {table} PURGE")


def _endpoint(table: str):
    from src.transfer.models import EndpointConfig

    cfg = _cfg()
    return EndpointConfig(
        kind="database", format="oracle", host=cfg["host"], port=cfg["port"],
        database=cfg["database"], username=cfg["username"], password=cfg["password"],
        schema=cfg["schema"], table=table, extra={"service_name": cfg["service_name"]},
    )


def test_live_oracle_date_is_graded_by_what_oracle_stores(oracle_dates):
    from services.mapping_proof import mapping_fidelity
    from services.source_engine_scope import bind_source_engine
    from services.type_system import create_new_mapping_target_type
    from src.transfer.adapters_introspect import _introspect_table_schema

    cfg = _cfg()
    types = _introspect_table_schema("oracle", cfg, oracle_dates, ["ID", "AT"])
    declared = types.get("AT") or types.get("at") or ""
    assert declared.upper().startswith("DATE"), types
    live = sqlalchemy_sql(cfg, f"SELECT TO_CHAR(AT, 'YYYY-MM-DD HH24:MI:SS') FROM {oracle_dates}", fetch=True)
    assert live == [("2024-03-10 13:45:07",)], live  # the time of day is stored

    mapping = {"source": "AT", "target": "at", "source_type": declared, "target_type": "DATE"}
    with bind_source_engine("oracle"):
        verdict = mapping_fidelity(
            mapping, destination_db_type="postgresql", dest_table_exists=True,
            population=[live[0][0]],
        )
        carrier = create_new_mapping_target_type(declared, "postgresql", source_db="oracle")
    assert verdict["verdict"] == "lossy_cast" and verdict["requires_risk_contract"], verdict
    assert carrier == "TIMESTAMP(0)", carrier


def test_live_oracle_date_into_existing_date_is_refused_and_create_new_keeps_time(
    isolated, oracle_dates, tmp_path  # noqa: F811
):
    from src.transfer.models import EndpointConfig

    refused_db = tmp_path / "refused.sqlite"
    with sqlite3.connect(refused_db) as conn:
        conn.execute(f"CREATE TABLE {oracle_dates} (ID INTEGER PRIMARY KEY, AT DATE)")
    dest = EndpointConfig(kind="database", format="sqlite", database=str(refused_db), table=oracle_dates)
    result = run_transfer(isolated, _endpoint(oracle_dates), dest)
    assert refused_at_validate(result, "AT"), result.error
    with sqlite3.connect(refused_db) as conn:
        assert conn.execute(f"SELECT count(*) FROM {oracle_dates}").fetchone()[0] == 0

    created_db = tmp_path / "created.sqlite"
    dest = EndpointConfig(kind="database", format="sqlite", database=str(created_db), table=oracle_dates)
    result = run_transfer(isolated, _endpoint(oracle_dates), dest)
    assert result.success, result.error
    with sqlite3.connect(created_db) as conn:
        (at,) = conn.execute(f"SELECT AT FROM {oracle_dates}").fetchone()
    assert "13:45:07" in str(at), at
