"""D158 (retest) — the MCP plan route reads a JSON column's samples as JSON.

Run 2 into the SQL Server table run 1 created was refused by
``g4_mapping_confidence`` ("1 ambiguous mapping(s) require review:
payload->payload (gap 0.00)") even with a signed Risk Contract. The route
(``plan_transfer``) sampled the source with ``str(v)``, so a driver-parsed
JSON/JSONB cell (a ``dict``) reached the mapper as a Python repr
(``{'k': 1}``), which is not JSON: every sample failed the ``json`` parse and
the exact-name pair was demoted below the Map floor. D179 fixed the same
rendering in the engine/plan-mapping path; this is the route's own sampler.
"""

from __future__ import annotations

import json
import os
import socket
import uuid

import pytest

from tests.test_d158_pg_sqlserver_rerun_fidelity import _PG, _mssql, _run, isolated  # noqa: F401
from tests.test_d147_d150_mongo_mcp_plan_route import store  # noqa: F401


def test_column_samples_carry_containers_as_json_not_a_python_repr():
    from src.ai.copilot.transfer_tools import _column_samples

    out = _column_samples(
        [
            {"payload": {"k": 1, "n": [1, None]}, "tags": ["a", "b"], "name": "x"},
            {"payload": None, "tags": [], "name": ""},
        ],
        ["payload", "tags", "name"],
    )
    assert [json.loads(s) for s in out["payload"]] == [{"k": 1, "n": [1, None]}]
    assert [json.loads(s) for s in out["tags"]] == [["a", "b"], []]
    assert out["name"] == ["x"]


def _up(host: str, port: int) -> bool:
    try:
        socket.create_connection((host, int(port)), timeout=2).close()
        return True
    except OSError:
        return False


_live = pytest.mark.skipif(
    not (
        _PG["host"]
        and os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD")
        and _up(_PG["host"], _PG["port"])
        and _up("127.0.0.1", 1433)
    ),
    reason="live PG + SQL Server not reachable",
)

_ACCEPTANCE = {
    "approved_by": "qa@example.com",
    "reason": "accepted",
    "execution_policy": "CAST_AND_CONTINUE",
}


@_live
@pytest.mark.parametrize("pg_type", ["JSONB", "JSON"])
@pytest.mark.parametrize("signed", [False, True], ids=["unsigned", "signed_contract"])
def test_plan_of_run_two_into_own_sqlserver_table_is_not_blocked_on_the_json_column(
    isolated, store, pg_type, signed  # noqa: F811
):
    import psycopg2

    from src.ai.copilot import transfer_tools

    table = "d158p_" + uuid.uuid4().hex[:10]
    pg = psycopg2.connect(
        host=_PG["host"], port=_PG["port"], user=_PG["username"],
        password=_PG["password"], dbname=_PG["database"],
    )
    cur = pg.cursor()
    source = store.create_connector(
        {"name": "s" + uuid.uuid4().hex[:6], "type": "postgresql", "role": "source",
         "host": _PG["host"], "port": _PG["port"], "database": _PG["database"],
         "username": _PG["username"], "password": _PG["password"],
         "workspace_id": "", "ssl": False}
    )
    dest = store.create_connector(
        {"name": "d" + uuid.uuid4().hex[:6], "type": "sqlserver", "role": "destination",
         "host": "127.0.0.1", "port": 1433, "database": "dataflow", "username": "sa",
         "password": os.environ["DATAFLOW_TEST_MSSQL_PASSWORD"], "workspace_id": "",
         "ssl": False, "extra": {"trust_server_certificate": True}}
    )
    try:
        cur.execute(
            f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, name VARCHAR(50), "  # nosec B608
            f"note TEXT, payload {pg_type}, updated_at BIGINT)"
        )
        for i in range(1, 6):
            cur.execute(
                f"INSERT INTO {table} VALUES (%s, %s, %s, %s, %s)",  # nosec B608
                (i, f"n{i}", f"note {i}", json.dumps({"k": i}), i),
            )
        pg.commit()
        first = _run(isolated, table)
        assert first.success, first.error

        cur.execute(
            f"UPDATE {table} SET payload = %s, updated_at = 100 WHERE id = 2",  # nosec B608
            (json.dumps({"k": 99}),),
        )
        pg.commit()

        result = transfer_tools.plan_transfer(
            source_connector_id=str(source.id),
            source_table=table,
            dest_connector_id=str(dest.id),
            dest_table=table,
            sync_mode="incremental_upsert",
            validation_mode="strict",
            upsert_key="id",
            cursor_column="updated_at",
            cursor_semantics="modification_timestamp",
            **({"risk_acceptance": _ACCEPTANCE} if signed else {}),
        )
        assert result.success, result.error
        plan = result.output
        blockers = (plan.get("preflight") or {}).get("blockers") or []
        assert not [b for b in blockers if "confidence" in b.get("message", "").lower()], blockers
        by_source = {m["source"]: m for m in plan["engine_mappings"]}
        assert by_source["payload"]["target"] == "payload"
        assert not by_source["payload"]["requires_review"], by_source["payload"]
        assert by_source["payload"].get("sample_parse_rate", 1.0) == 1.0
    finally:
        try:
            conn = _mssql()
            conn.execute(f"DROP TABLE IF EXISTS [{table}]")  # nosec B608
            conn.commit()
            conn.close()
        finally:
            cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
            pg.commit()
            pg.close()
