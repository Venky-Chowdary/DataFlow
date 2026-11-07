"""Review follow-ups to C2 (f7a7a65c9) and MySQL-D4 (ec43b044b).

1. The replay's duplicate-key probe runs only where the writer builds a merge
   index (PostgreSQL / MySQL / MariaDB): a Snowflake (MERGE) replay over a
   table holding a repeated key is not refused; a key an existing unique index
   covers is not probed. One bounded probe for plan and replay.
2. Repeated NULL keys are not duplicates for a PostgreSQL unique index.
3. Every duplicate route the refusal offers is followed end to end.
4. Lenient keeps an unfinished uniqueness probe's own handling.
5. One finding-row counter: dict-valued rows never crash a reader, and two
   diverted rows with identical records are two rows.
6. A divert names the input value the step read.
7. A key value that holds ", " is not split.
"""

from __future__ import annotations

import json
import uuid

import pytest

from tests import r4_route_harness as h

PG = {"host": "127.0.0.1", "port": 5432, "user": "dataflow", "password": "dataflow", "dbname": "dataflow"}
PG_CFG = {"type": "postgresql", "host": "127.0.0.1", "port": 5432, "database": "dataflow",
          "username": "dataflow", "password": "dataflow", "schema": "public"}


def _pg():
    import psycopg2

    conn = psycopg2.connect(**PG)
    conn.autocommit = True
    return conn


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.delenv("DATAFLOW_SEED_DEMO", raising=False)
    monkeypatch.setenv("DATAFLOW_UPLOAD_DIR", str(tmp_path / "uploads"))
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _dups_csv() -> bytes:
    rows = ["sku,name,price"] + [f"SKU-{i:05d},item {i},{i}.50" for i in range(1, 21)]
    rows.append("SKU-00007,dup item,9.99")
    return ("\n".join(rows) + "\n").encode()


def _run_file(client, table: str, *, sync_mode: str, extra_field: str = "", extra: dict | None = None):
    data = {
        "source_kind": "file", "dest_kind": "database", "dest_format": "postgresql",
        "dest_host": PG["host"], "dest_port": str(PG["port"]), "dest_database": PG["dbname"],
        "dest_username": PG["user"], "dest_password": PG["password"], "dest_schema": "public",
        "dest_table": table, "async_mode": "false", "sync_mode": sync_mode,
        "validation_mode": "lenient", "schema_policy": "propagate_columns",
        "stream_contracts_json": json.dumps(
            [{"name": table, "sync_mode": sync_mode, "primary_key": "sku", "selected": True}]
        ),
    }
    if extra_field:
        data[extra_field] = json.dumps(extra or {})
    return client.post("/api/v1/transfer/run", data=data, files={"file": ("dups.csv", _dups_csv(), "text/csv")})


# ── 3. every offered route gets past the block and does what it says ────────
@pytest.mark.parametrize(
    "route, sync_mode, field, extra, rows, sku7",
    [
        ("keep_last via destination", "full_refresh_overwrite", "dest_extra_json",
         {"in_file_dedupe": "keep_last"}, 20, "dup item"),
        ("keep_last via source", "full_refresh_overwrite", "source_extra_json",
         {"in_file_dedupe": "keep_last"}, 20, "dup item"),
        ("append via destination", "full_refresh_overwrite", "dest_extra_json",
         {"duplicate_policy": "append"}, 21, None),
        ("sync_mode=upsert", "upsert", "", None, 20, "dup item"),
    ],
)
def test_each_offered_duplicate_route_loads(client, route, sync_mode, field, extra, rows, sku7):
    from services.data_integrity import DUPLICATE_KEY_ROUTES

    token = {"keep_last via destination": "in_file_dedupe=keep_last",
             "keep_last via source": "in_file_dedupe=keep_last",
             "append via destination": "duplicate_policy=append",
             "sync_mode=upsert": "sync_mode=upsert"}[route]
    assert token in DUPLICATE_KEY_ROUTES
    table = f"r14rv_{uuid.uuid4().hex[:6]}"
    conn = _pg()
    cur = conn.cursor()
    try:
        blocked = _run_file(client, table, sync_mode="full_refresh_overwrite")
        assert blocked.status_code == 422 and "SKU-00007×2" in blocked.text, blocked.text[:400]
        resp = _run_file(client, table, sync_mode=sync_mode, extra_field=field, extra=extra)
        assert resp.status_code == 200, (route, resp.text[:800])
        cur.execute(f'SELECT count(*), count(DISTINCT sku) FROM public."{table}"')  # nosec B608
        total, distinct = cur.fetchone()
        assert (total, distinct) == (rows, 20), (route, total, distinct)
        if sku7 is not None:
            cur.execute(f'SELECT name FROM public."{table}" WHERE sku = %s', ("SKU-00007",))  # nosec B608
            assert [r[0] for r in cur.fetchall()] == [sku7], route
    finally:
        cur.execute(f'DROP TABLE IF EXISTS public."{table}"')  # nosec B608
        conn.close()


# ── 1 / 2. the probe: index engines only, NULL keys are not duplicates ──────
def test_null_keys_are_not_duplicates_and_real_ones_are():
    from services.upsert_ddl_gate import count_dest_duplicate_keys, probe_dest_duplicate_keys

    table = f"r14nk_{uuid.uuid4().hex[:6]}"
    conn = _pg()
    cur = conn.cursor()
    try:
        cur.execute(f'CREATE TABLE public."{table}" (sku text, region text, v int)')  # nosec B608
        cur.executemany(
            f'INSERT INTO public."{table}" VALUES (%s, %s, %s)',  # nosec B608
            [(None, "eu", 1), (None, "eu", 2), (None, None, 3), ("A, B", "eu", 4), ("C", "us", 5)],
        )
        res = count_dest_duplicate_keys(PG_CFG, table, ["sku"])
        assert res["ran"] and res["duplicate_keys"] == 0, res
        res = probe_dest_duplicate_keys(PG_CFG, table, ["sku", "region"])
        assert res["ran"] and res["duplicate_keys"] == 0, res
        cur.execute(f"INSERT INTO public.\"{table}\" VALUES ('A, B', 'eu', 6)")  # nosec B608
        res = count_dest_duplicate_keys(PG_CFG, table, ["sku"])
        assert res["duplicate_keys"] == 1 and res["samples"][0]["values"] == {"sku": "A, B"}, res
        # PostgreSQL itself agrees: the index builds over the NULL keys.
        cur.execute(f'DELETE FROM public."{table}" WHERE v = 6')  # nosec B608
        cur.execute(f'CREATE UNIQUE INDEX ON public."{table}" (sku)')  # nosec B608
    finally:
        cur.execute(f'DROP TABLE IF EXISTS public."{table}"')  # nosec B608
        conn.close()


def test_replay_probe_runs_only_where_the_writer_builds_a_merge_index(monkeypatch):
    import services.upsert_ddl_gate as gate
    from services.quarantine_dlq import replay_destination_duplicate_keys

    calls: list[str] = []
    monkeypatch.setattr(gate, "probe_dest_duplicate_keys", lambda *a, **k: calls.append("probe") or {
        "ran": True, "duplicate_keys": 1, "duplicate_rows": 2,
        "samples": [{"values": {"id": "7"}, "count": 2}], "message": ""})
    monkeypatch.setattr(gate, "destination_unique_covering", lambda *a, **k: False)
    for merge_engine in ("snowflake", "mssql", "oracle", "bigquery", "databricks"):
        assert replay_destination_duplicate_keys(
            {}, dest_kind=merge_engine, table="t", key_columns=["id"]
        ) is None, merge_engine
    assert calls == []
    refusal = replay_destination_duplicate_keys({}, dest_kind="postgresql", table="t", key_columns=["id"])
    assert refusal and refusal["duplicate_keys"] == ["7×2"] and calls == ["probe"]
    # An existing unique key on the replay columns cannot repeat: no probe.
    monkeypatch.setattr(gate, "destination_unique_covering", lambda *a, **k: True)
    assert replay_destination_duplicate_keys({}, dest_kind="mysql", table="t", key_columns=["id"]) is None
    assert calls == ["probe"]


# ── 4. lenient keeps an unfinished probe's own handling ─────────────────────
def test_lenient_keeps_an_unfinished_uniqueness_probe_its_own_note():
    from services.data_integrity import LENIENT_DUPLICATE_KEY_NOTE, run_integrity_audit

    report = run_integrity_audit(
        mappings=[{"source": "sku", "target": "sku", "confidence": 1}],
        source_columns=["sku"],
        source_schemas=[{"name": "sku", "inferred_type": "VARCHAR"}],
        sample_rows=[{"sku": "A"}, {"sku": "B"}],
        validation_mode="lenient",
        destination_db_type="postgresql",
        sync_mode="full_refresh_overwrite",
        contract_primary_key="sku",
        source_duplicate_probe_status="timeout",
        source_duplicate_probe_message="probe timed out after 8s",
        source_duplicate_probe_expected=True,
    )
    check = next(c for c in report["checks"] if c["check"] == "duplicate_keys")
    assert check["blocks_transfer"] is False, check
    assert check.get("note") != LENIENT_DUPLICATE_KEY_NOTE
    assert "did not complete" in str(check.get("note")), check


# ── 5. one finding-row counter ──────────────────────────────────────────────
def _divert(row_index: int, record: dict) -> dict:
    return {"row": record, "source_row_index": row_index, "column": "ratio",
            "value": record.get("ratio"), "reason": "transform divert", "policy": "quarantine",
            "shape_op": "cast_column"}


def test_finding_row_counter_handles_records_and_identical_diverts(monkeypatch):
    from services.quarantine_dlq import count_finding_rows
    from services.quarantine_from_preflight import preflight_quarantine_row_count

    same = {"id": "2", "ratio": "abc"}
    details = [_divert(2, same), _divert(3, dict(same)), {"row": 22, "column": "price"}]
    assert count_finding_rows(details) == 3
    assert preflight_quarantine_row_count(details) == 3

    from services.mongodb_service import MemoryMongoDBService
    import services.mongodb_service as mongo_mod
    from src.ai.copilot.tools import get_pilot_tools

    memory = MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongo_mod, "_mongodb_service", memory)
    job_id = memory.create_transfer_job({"status": "completed_with_quarantine"})
    memory.update_job_status(job_id, "completed_with_quarantine", rejected_rows=2,
                             rejected_details=[_divert(2, same), _divert(3, dict(same))])
    res = get_pilot_tools().execute("get_job", {"job_id": str(job_id)})
    assert res.success, res.error


# ── 6. the divert names the value the step read ─────────────────────────────
def test_divert_names_the_input_a_derive_step_read():
    from services.shape_apply import ShapeRunner
    from services.shape_models import ShapeRecipe

    recipe = ShapeRecipe.parse({"steps": [{
        "op": "derive_column", "options": {"to": "double", "expression": "[qty] * 2"},
        "on_error": "divert"}]})
    runner = ShapeRunner(recipe)
    out = runner.records([{"qty": "4"}, {"qty": "abc"}])
    assert [r.get("double") for r in out] and len(out) == 1, out
    (detail,) = runner.divert_details(runner.take_diverted())
    assert detail["column"] == "qty" and detail["value"] == "abc", detail
    assert detail.get("value") is not None


# ── 7. a key value holding ", " is not split ────────────────────────────────
def test_index_build_failure_keeps_a_comma_value_whole():
    from services.quarantine_dlq import replay_unique_index_failure

    exc = RuntimeError(
        'could not create unique index "uidx_t_sku"\nDETAIL:  Key (sku)=(A, B) is duplicated.\n'
    )
    assert replay_unique_index_failure(exc, table="t", key_columns=["sku"])["duplicate_keys"] == ["A, B×2"]
    composite = RuntimeError(
        'could not create unique index "uidx_t_a_b"\nDETAIL:  Key (a, b)=(x, y, z) is duplicated.\n'
    )
    refusal = replay_unique_index_failure(composite, table="t", key_columns=["a", "b"])
    assert refusal["duplicate_keys"] == [] and refusal["key_columns"] == ["a", "b"]
    # SQL Server / Oracle MERGE builds no index: their messages are not this refusal.
    assert replay_unique_index_failure(
        RuntimeError("The CREATE UNIQUE INDEX statement terminated"), table="t", key_columns=["id"]
    ) is None
