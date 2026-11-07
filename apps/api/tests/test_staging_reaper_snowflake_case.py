"""Staging reaper and object picker must match Snowflake's stored (UPPER) names.

DataFlow names its scratch tables ``_dataflow_stg_*`` / ``_df_mirrorkeys_*`` in
lower case, but Snowflake stores what is created unquoted-or-folded as UPPER
case, and ``LIKE`` and ``=`` over its catalog are case-sensitive. The reaper's
``table_name LIKE '_dataflow_stg_%'`` therefore never matched, so a Snowflake
spool left by a killed run was never swept, and the object picker (a
case-sensitive ``startswith``) listed the spool as if it were the operator's
data.
"""

from __future__ import annotations

import time

import pytest

pytest.importorskip("fakesnow")

from tests import test_d120_snowflake_strict_e2e as _strict  # noqa: E402

snowflake_semantics = _strict.snowflake_semantics

OLD = int(time.time()) - 7 * 3600  # past STAGING_TTL_SECONDS
FRESH = int(time.time())


def _cfg():
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.models import EndpointConfig

    return resolve_connector_config(
        EndpointConfig(
            kind="database", format="snowflake", host="localhost", port=443,
            database="dataflow", username="test", password="test", schema="public",
            table="t",
        )
    )


def _tables() -> set[str]:
    from connectors.snowflake_conn import get_connection

    conn = get_connection(
        account="localhost", username="test", password="test",
        database="dataflow", schema="PUBLIC", warehouse="", connection_string="",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'PUBLIC'"
            )
            return {str(r[0]) for r in cur.fetchall()}
    finally:
        conn.close()


def _create(*names: str) -> None:
    from connectors.snowflake_conn import get_connection

    conn = get_connection(
        account="localhost", username="test", password="test",
        database="dataflow", schema="PUBLIC", warehouse="", connection_string="",
    )
    try:
        with conn.cursor() as cur:
            for n in names:
                cur.execute(f'CREATE TABLE "PUBLIC"."{n}" ("ID" INTEGER)')
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("prefix", "stem"),
    [("_dataflow_stg_", "JOB1"), ("_df_mirrorkeys_", "")],
)
def test_reaper_sweeps_snowflake_orphans(
    tmp_path, monkeypatch, snowflake_semantics, prefix, stem
):
    from connectors.generic_sql import get_sqlalchemy_engine
    from services.engine_pool import release_engine, reset_for_tests
    from services.staging_reaper import reap_orphan_staging

    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    # A pooled engine holds the previous test's (closed) fakesnow catalog.
    reset_for_tests()
    mid = f"{stem}_" if stem else ""
    stored = prefix.upper()
    orphan = f"{stored}{mid}{OLD}_AB12CD34"
    live = f"{stored}{mid}{FRESH}_EF56AB78"
    mine = f"{stored}{mid}{OLD}_0000KEEP"
    # Unique per case: with DATAFLOW_FAKESNOW_KEEP_PATCH=1 (CI) the fake catalog
    # outlives the case, so a shared name would already exist on the next one.
    real = f"REAL_TABLE_{prefix.strip('_').upper()}"
    _create(orphan, live, mine, real)

    engine = get_sqlalchemy_engine(_cfg())
    try:
        with engine.connect() as conn:
            dropped = reap_orphan_staging(
                conn, prefix, "public", "snowflake",
                keep=f"{prefix}{mid}{OLD}_0000keep".lower(),
            )
    finally:
        release_engine(engine)

    assert dropped == [orphan]
    left = _tables()
    assert orphan not in left
    assert {live, mine, real} <= left


def test_object_picker_hides_snowflake_spools():
    from src.transfer.endpoint_intelligence import _operator_visible_objects

    names = ["_DATAFLOW_STG_JOB1_1_AB", "_DF_MIRRORKEYS_1_AB", "_DF_DLQ_X", "ORDERS"]
    assert [o["name"] for o in _operator_visible_objects(names, "table")] == ["ORDERS"]
