"""D72 — PostgreSQL UUID -> Snowflake was blocked by its own identity mapping.

Snowflake has no UUID type; create-new lands a UUID as ``VARCHAR(36)``, the
exact canonical wire. The create-new risk stamp still emitted a locking
``uuid_domain`` chip for it, which marked the mapping for review and capped
its confidence at 0.70, so G4 refused the run ("below the Map confidence
floor") with nothing the operator could remap to.
"""

from __future__ import annotations

import uuid

import psycopg2
import pytest

from services.create_new_risk_stamp import create_new_risk_locks_review
from services.type_coercion import assess_create_new_type_risk
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests import test_d120_snowflake_strict_e2e as _strict
from tests.test_d120_snowflake_strict_e2e import PG_CFG, _snowflake_rows

# Real-Snowflake rules over fakesnow (see test_d120_snowflake_strict_e2e).
snowflake_semantics = _strict.snowflake_semantics
engine_env = _strict.engine_env

_U1 = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"


def _locking(src: str, tgt: str, db: str) -> list[dict]:
    risks = assess_create_new_type_risk(src, tgt, destination_db_type=db)
    return [r for r in risks if create_new_risk_locks_review(r)]


def test_d72_uuid_exact_wire_on_snowflake_is_stated_not_held():
    risks = assess_create_new_type_risk("uuid", "VARCHAR(36)", destination_db_type="snowflake")
    assert risks, "the unenforced UUID domain must still be stated on Map"
    assert not _locking("uuid", "VARCHAR(36)", "snowflake")
    # A bare VARCHAR is still an opaque-text collapse there.
    assert _locking("uuid", "VARCHAR", "snowflake")


@pytest.fixture()
def pg_uuid_source():
    try:
        conn = psycopg2.connect(
            host=PG_CFG["host"], port=PG_CFG["port"], dbname=PG_CFG["database"],
            user=PG_CFG["username"], password=PG_CFG["password"], connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    table = "d72_src_" + uuid.uuid4().hex[:8]
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE {table} (id INT PRIMARY KEY, code UUID, "
            "label TEXT, amt NUMERIC(12,2))"
        )
        cur.execute(
            f"INSERT INTO {table} VALUES (1, %s, 'x', 1), (2, NULL, 'y', 2)", (_U1,)
        )
    try:
        yield table
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()


def test_d72_pg_uuid_lands_in_snowflake(engine_env, pg_uuid_source):
    dst = "d72_dst_" + uuid.uuid4().hex[:8]
    job_id = "d72" + uuid.uuid4().hex[:16]
    engine_env.update_job_status(job_id, "pending", transfer_request={})
    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql", schema="public",
            table=pg_uuid_source, **PG_CFG,
        ),
        destination=EndpointConfig(
            kind="database", format="snowflake", host="localhost", port=443,
            database="dataflow", username="test", password="test",
            schema="public", table=dst,
        ),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": pg_uuid_source, "sync_mode": "full_refresh_overwrite",
            "primary_key": "id", "selected": True,
        }],
    )
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success is True, result.error
    assert (result.reconciliation or {}).get("passed") is True, result.reconciliation
    rows = _snowflake_rows(dst)
    assert rows[1][0] == _U1
    assert rows[2][0] is None
