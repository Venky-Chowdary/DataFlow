"""D402 re-review (bcb0cdd07) — five follow-ups, each on the route that showed it.

1. A dedupe_key PATCH on a schedule whose destination holds no SQL key (an S3
   export) was refused 400 "keys could not be read (RuntimeError)". It saves;
   an unreachable SQL destination is named plainly.
2. A PostgreSQL column with a nondeterministic ICU collation kept through an
   overwrite's TRUNCATE calls 'a' and 'A' one key: the exact check failed a
   correct load after the table was replaced. Now asked before any write.
3. SQL Server pads trailing spaces under every collation, _BIN2 included
   (facts only; the live leg needs the CI SQL Server).
4. A PATCH carrying stream_contracts with dedupe_key skipped the keyed-table
   refusal.
5. dedupe_check_billed_scan is a JSON boolean: "false" never opts in, a
   non-boolean is a 422, and a refusal names each blocking key once.
"""

from __future__ import annotations

import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from tests.conftest import pg_test_cfg  # noqa: E402
from tests.test_d402_dedupe_every_file_route_e2e import _BUCKET  # noqa: E402
from tests.test_d402_schedule_dedupe_key_e2e import _drop, _run, _stage  # noqa: E402
from tests.test_d402_start_transfer_object_store_e2e import _connectors  # noqa: E402
from tests.test_upsert_infile_duplicate_start_transfer_e2e import transfer_client  # noqa: E402,F401

CSV = b"code,v\na,1\nA,2\nb,3\nb ,4\nc,5\nc,6\n"
_MAPPINGS = [{"source": c, "target": c, "confidence": 1.0} for c in ("id", "name", "note", "amount")]


def _pg_exec(statement: str) -> list[tuple]:
    import psycopg2

    cfg = pg_test_cfg()
    conn = psycopg2.connect(host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
                            user=cfg["username"], password=cfg["password"])
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(statement)
            return [tuple(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


def _s3_dest_connector(endpoint: str) -> str:
    from services.connector_store import create_connector

    return create_connector({
        "name": f"d402r2-s3dst-{uuid.uuid4().hex[:6]}", "type": "s3", "role": "destination",
        "host": "us-east-1", "port": int(endpoint.rsplit(":", 1)[1]), "database": _BUCKET,
        "username": "test", "password": "test", "schema": "", "ssl": False,
        "endpoint_url": endpoint, "path_style": True,
    }).id


def test_dedupe_key_patch_on_an_object_destination_saves(transfer_client, local_object_store) -> None:
    key = _stage(local_object_store)
    source_id, _pg_dest = _connectors(local_object_store, "s3")
    dest_id = _s3_dest_connector(local_object_store)
    created = transfer_client.post("/api/v1/schedules/", json={
        "name": f"d402r2-obj-{uuid.uuid4().hex[:6]}", "source_connector_id": source_id,
        "source_table": key, "dest_connector_id": dest_id, "dest_table": "exports/d402.csv",
        "interval": "daily", "sync_mode": "full_refresh_overwrite", "mappings": _MAPPINGS,
    })
    assert created.status_code == 201, created.text
    patched = transfer_client.patch(f"/api/v1/schedules/{created.json()['id']}",
                                    json={"dedupe_key": "id", "in_file_dedupe": "keep_last"})
    assert patched.status_code == 200, patched.text
    assert patched.json()["dedupe_key"] == "id"


def test_dedupe_key_patch_names_an_unreachable_sql_destination_plainly(
    transfer_client, local_object_store
) -> None:
    import socket

    from services.connector_store import update_connector

    key = _stage(local_object_store)
    source_id, dest_id = _connectors(local_object_store, "s3")
    created = transfer_client.post("/api/v1/schedules/", json={
        "name": f"d402r2-dead-{uuid.uuid4().hex[:6]}", "source_connector_id": source_id,
        "source_table": key, "dest_connector_id": dest_id,
        "dest_table": f"d402r2_{uuid.uuid4().hex[:6]}", "interval": "daily",
        "sync_mode": "full_refresh_overwrite", "mappings": _MAPPINGS,
    })
    assert created.status_code == 201, created.text
    closed = socket.socket()
    closed.bind(("127.0.0.1", 0))
    dead = closed.getsockname()[1]
    closed.close()
    update_connector(dest_id, {"port": dead})
    refused = transfer_client.patch(f"/api/v1/schedules/{created.json()['id']}",
                                    json={"dedupe_key": "id", "in_file_dedupe": "keep_last"})
    assert refused.status_code == 400, refused.text
    assert "could not connect to the destination" in refused.text, refused.text
    assert "Error)" not in refused.text and "RuntimeError" not in refused.text, refused.text


def test_patch_with_stream_contracts_still_refuses_a_key_the_table_cannot_hold(
    transfer_client, local_object_store
) -> None:
    key = _stage(local_object_store)
    source_id, dest_id = _connectors(local_object_store, "s3")
    table = f"d402r2_sc_{uuid.uuid4().hex[:6]}"
    try:
        created = transfer_client.post("/api/v1/schedules/", json={
            "name": f"d402r2-sc-{uuid.uuid4().hex[:6]}", "source_connector_id": source_id,
            "source_table": key, "dest_connector_id": dest_id, "dest_table": table,
            "interval": "daily", "sync_mode": "full_refresh_overwrite",
            "schema_policy": "propagate_columns", "validation_mode": "strict",
            "plan_mappings": True, "dedupe_key": "id",
        })
        assert created.status_code == 201, created.text
        saved = created.json()
        job, _rows = _run(saved["id"])
        assert job.get("status") == "completed", job.get("message")
        contracts = [{**c, "primary_key": ["id", "name"]} for c in saved["stream_contracts"]]
        refused = transfer_client.patch(f"/api/v1/schedules/{saved['id']}", json={
            "dedupe_key": "id,name", "stream_contracts": contracts})
        assert refused.status_code == 400, refused.text
        # Each blocking key is named once ("(id)", not "(id), (id)").
        message = refused.json()["error"]["message"]
        assert message.count("(id)") == 1, message
    finally:
        _drop(table)


@pytest.fixture()
def pg_ci_table():
    """An existing PostgreSQL table whose key column is case-insensitive (ICU)."""
    coll = f"d402_ci_{uuid.uuid4().hex[:6]}"
    table = f"d402r2_icu_{uuid.uuid4().hex[:6]}"
    _pg_exec(f"CREATE COLLATION {coll} (provider = icu, locale = 'und-u-ks-level2', deterministic = false)")
    _pg_exec(f'CREATE TABLE "{table}" (code TEXT COLLATE {coll}, v BIGINT)')
    _pg_exec(f"INSERT INTO \"{table}\" VALUES ('seed', 0)")
    try:
        yield table
    finally:
        _pg_exec(f'DROP TABLE IF EXISTS "{table}"')
        _pg_exec(f"DROP COLLATION IF EXISTS {coll}")


def _pg_run(monkeypatch, table: str, choice: str):
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    extra = {"in_file_dedupe": "keep_last", "dedupe_key": "code",
             **({"dedupe_key_equality": choice} if choice else {})}
    cfg = pg_test_cfg()
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv", extra=dict(extra)),
        source_filename="codes.csv", source_content=CSV,
        destination=EndpointConfig.from_dict("database", {
            "kind": "database", "format": "postgresql", **cfg, "schema": "public",
            "table": table, "extra": dict(extra)}),
        sync_mode="full_refresh_overwrite", schema_policy="propagate_columns",
        validation_mode="strict",
    )
    job_id = uuid.uuid4().hex[:24]
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    return result, dict(fake.get_job(job_id) or {})


@pytest.mark.parametrize("choice", ["", "exact", "destination"])
def test_pg_nondeterministic_key_collation_is_decided_before_the_write(
    monkeypatch, pg_ci_table, choice
) -> None:
    result, job = _pg_run(monkeypatch, pg_ci_table, choice)
    message = str(job.get("message") or result.error or "")
    rows = sorted((bytes(k), v) for k, v in _pg_exec(
        f'SELECT convert_to(code, \'UTF8\'), v FROM "{pg_ci_table}"'))
    if not choice:
        assert result.success is False and "'a' = 'A'" in message, message
        assert "dedupe_key_equality=exact" in message, message
        assert rows == [(b"seed", 0)], "the refused run touched the table"
        return
    assert result.success is True and job.get("status") == "completed", message
    assert "dedupe" not in message.lower() and "unverif" not in message.lower(), message
    # PostgreSQL never pads: 'b' and 'b ' are two keys under any collation.
    expected = {
        "exact": [(b"A", 2), (b"a", 1), (b"b", 3), (b"b ", 4), (b"c", 6)],
        "destination": [(b"A", 2), (b"b", 3), (b"b ", 4), (b"c", 6)],
    }[choice]
    assert rows == expected, rows


def test_sql_server_pads_trailing_spaces_under_every_collation() -> None:
    """Facts only; the live grouping runs on the CI SQL Server."""
    from services.collation_equality import (
        _collision_buckets,
        read_column_collation,
        sqlserver_collation_fact,
    )

    class _Row:
        def __init__(self, value):
            self._value = value

        def fetchone(self):
            return (self._value,)

    class _Conn:
        def execute(self, *_a, **_k):
            return _Row("Latin1_General_100_BIN2")

    for name in ("Latin1_General_100_BIN2", "Latin1_General_BIN", "SQL_Latin1_General_CP1_CI_AS"):
        assert sqlserver_collation_fact(name).pad == "PAD SPACE", name
    fact = read_column_collation(_Conn(), "sqlserver", schema="dbo", table="t", column="code")
    assert fact is not None and fact.pad == "PAD SPACE"
    assert _collision_buckets(["b", "b ", "b\t"], fact) == [["b", "b "]]


def test_billed_scan_opt_in_is_a_json_boolean(transfer_client) -> None:
    from types import SimpleNamespace

    from services.dedupe_postwrite import billed_scan_opted_in, stamp_dedupe_request

    def req(value):
        extra = {"in_file_dedupe": "keep_last", "dedupe_key": "id", "dedupe_check_billed_scan": value}
        return SimpleNamespace(source=SimpleNamespace(extra=extra),
                               destination=SimpleNamespace(extra=dict(extra)),
                               stream_contracts=[], mappings=[])

    assert billed_scan_opted_in(req(True)) is True
    for value in ("false", "true", 1, "0", None):
        assert billed_scan_opted_in(req(value)) is False, value
    summary: dict = {}
    stamp_dedupe_request(summary, req("false"))
    assert "dedupe_check_billed_scan" not in summary
    resp = transfer_client.post("/api/v1/transfer/execute", json={
        "source": {"kind": "file", "format": "csv"},
        "destination": {"kind": "database", "format": "snowflake", "table": "t",
                        "extra": {"dedupe_check_billed_scan": "false"}},
        "sync_mode": "full_refresh_overwrite",
    })
    assert resp.status_code == 422, resp.text
    assert "dedupe_check_billed_scan must be true or false" in resp.text, resp.text
