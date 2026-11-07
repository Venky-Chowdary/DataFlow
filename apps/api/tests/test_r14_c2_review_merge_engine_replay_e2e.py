"""Review follow-up to C2: a MERGE engine's replay is not refused for a repeated key.

Snowflake (fakesnow) MERGEs on the key itself and DataFlow builds no unique
index there, so a keyed quarantine replay over a table that already repeats a
key must merge, not answer the PostgreSQL-only 409
REPLAY_DESTINATION_DUPLICATE_KEYS the C2 probe returned for every database.
"""

from __future__ import annotations

import uuid

import pytest

pytest.importorskip("fakesnow")

from tests import r4_route_harness as h  # noqa: E402
from tests import test_r12_snowflake_transform_e2e as sfmod  # noqa: E402
from tests.test_d120_snowflake_strict_e2e import snowflake_semantics  # noqa: E402,F401
from tests.test_r12_snowflake_transform_e2e import client  # noqa: E402,F401

pytestmark = pytest.mark.timeout(600)


def test_snowflake_replay_over_a_repeated_key_merges_instead_of_409(client):  # noqa: F811
    src = sfmod._seed_pg("id integer PRIMARY KEY, ratio text, label text",
                         [(1, "3", "a"), (2, "abc", "b"), (3, "7", "Zoë ✓")])
    dst = f"r14rvsf_{uuid.uuid4().hex[:6]}"
    recipe = {"steps": [{"op": "cast_column", "column": "ratio",
                         "options": {"to_type": "integer"}, "on_error": "divert"}]}
    contracts = [{"name": src, "sync_mode": "upsert", "primary_key": "id", "selected": True}]
    try:
        job = sfmod._run(client, source=src, dest=dst, validation_mode="lenient",
                         sync_mode="upsert", stream_contracts_json=contracts, **sfmod._shaped(recipe))
        assert job.get("status") == "completed_with_quarantine", (job.get("status"), job.get("error"))
        job_id = str(job.get("job_id") or job.get("_id"))
        conn = sfmod._sf_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f'INSERT INTO "PUBLIC"."{dst.upper()}" ("ID", "RATIO", "LABEL") VALUES (1, 3, \'a\')'
                )
        finally:
            conn.close()
        assert len(sfmod._sf(f'SELECT "ID" FROM "PUBLIC"."{dst.upper()}" WHERE "ID" = 1')) == 2
        replay = client.post(f"/api/v1/connectors/jobs/{job_id}/quarantine/replay",
                             json={"rows": [{"id": 2, "ratio": "5", "label": "b"}]})
        assert replay.status_code == 200, replay.text[:800]
        assert "REPLAY_DESTINATION_DUPLICATE_KEYS" not in replay.text
        assert replay.json()["rows_written"] == 1, replay.json()
        got = sfmod._sf(f'SELECT "ID", "RATIO", "LABEL" FROM "PUBLIC"."{dst.upper()}" WHERE "ID" = 2')
        assert [tuple(r) for r in got] == [(2, 5, "b")], got
    finally:
        h.drop_table("postgresql", src)


def test_sql_server_replay_over_a_repeated_key_merges_instead_of_409(client):  # noqa: F811
    import json as _json
    import os as _os

    h.mssql_conn().close()
    src = sfmod._seed_pg("id integer PRIMARY KEY, ratio text, label text",
                         [(1, "3", "a"), (2, "abc", "b"), (3, "7", "Zoë ✓")])
    dst = f"r14rvms_{uuid.uuid4().hex[:6]}"
    # An operator table with no unique key (SQL Server MERGEs on the key itself).
    h.execute("sqlserver", f"CREATE TABLE [{dst}] (id int, ratio int, label nvarchar(max))")
    recipe = {"steps": [{"op": "cast_column", "column": "ratio",
                         "options": {"to_type": "integer"}, "on_error": "divert"}]}
    contracts = [{"name": src, "sync_mode": "upsert", "primary_key": "id", "selected": True}]
    data = {
        "source_kind": "database", "dest_kind": "database", "source_table": src, "dest_table": dst,
        "async_mode": "false", "validation_mode": "lenient", "sync_mode": "upsert",
        "source_format": "postgresql", "source_schema": "public", **sfmod._side("source", sfmod.PG_ENDPOINT),
        "dest_format": "sqlserver", "dest_host": "127.0.0.1", "dest_port": "1433", "dest_database": "dataflow",
        "dest_username": _os.environ.get("DATAFLOW_TEST_MSSQL_USER", "sa"),
        "dest_password": _os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD", ""), "dest_schema": "dbo",
        "dest_extra_json": _json.dumps({"trust_server_certificate": True}),
        "stream_contracts_json": _json.dumps(contracts),
        "ddl_changes_acknowledged": "true",
    }
    for key, value in sfmod._shaped(recipe).items():
        data[key] = _json.dumps(value) if isinstance(value, (dict, list)) else str(value)
    try:
        resp = client.post("/api/v1/transfer/run", data=data)
        assert resp.status_code == 200, resp.text[:1500]
        job = h.wait_job(str(resp.json().get("job_id") or ""))
        assert job.get("status") == "completed_with_quarantine", (job.get("status"), job.get("error"))
        job_id = str(job.get("job_id") or job.get("_id"))
        # The table now repeats id=1 (another writer).
        h.execute("sqlserver", f"INSERT INTO [{dst}] VALUES (1, 3, N'a')")
        assert len(h.query("sqlserver", f"SELECT id FROM [{dst}] WHERE id = 1")) == 2
        replay = client.post(f"/api/v1/connectors/jobs/{job_id}/quarantine/replay",
                             json={"rows": [{"id": 2, "ratio": "5", "label": "b"}]})
        assert replay.status_code == 200, replay.text[:800]
        assert "REPLAY_DESTINATION_DUPLICATE_KEYS" not in replay.text
        got = h.query("sqlserver", f"SELECT id, ratio, label FROM [{dst}] WHERE id = 2")
        assert got == [(2, 5, "b")], got
    finally:
        h.drop_table("postgresql", src)
        h.drop_table("sqlserver", dst)
