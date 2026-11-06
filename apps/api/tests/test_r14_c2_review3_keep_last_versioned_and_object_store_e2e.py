"""Third review of 4e2266415 — keep_last where the read defines "last".

1. Explicit keep_last on a cursor-ordered keyed sync (incremental_deduped with
   a cursor, the shape every schedule stamps) is the automatic choice spelled
   out: it runs and the latest version of each key lands. Where the read has
   no order (overwrite of a table) it is refused with routes for that sync.
3. The lenient note for a table names the table's routes, never keep_last.
7. A kept PostgreSQL column narrower than the data (VARCHAR(3) 'abcdef',
   NUMERIC(6,2) 12345.67): strict refuses with the prior rows intact, lenient
   quarantines the cells; the COPY overwrite swap never CASTs a value down to
   the column's width.
8. An approved widen is the Gate-8 carrier; a scale-only overflow is
   quarantined as rounding.
"""

from __future__ import annotations

import json
import uuid

import pytest

from tests import r4_route_harness as h
from tests import test_r12_snowflake_transform_e2e as sfmod

pytestmark = pytest.mark.timeout(900)

PG_DEST = {
    "dest_format": "postgresql", "dest_host": h.PG["host"], "dest_port": str(h.PG["port"]),
    "dest_database": h.PG["database"], "dest_username": h.PG["username"],
    "dest_password": h.PG["password"], "dest_schema": "public",
}


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.delenv("DATAFLOW_SEED_DEMO", raising=False)
    monkeypatch.setenv("DATAFLOW_UPLOAD_DIR", str(tmp_path / "uploads"))
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


VERSIONS = [("K1", "v1-1", 1), ("K2", "v2-1", 2), ("K3", "v2-2", 3), ("K3", "v2-3", 4), ("K4", "Zoë ✓", 5)]


def _versioned_run(client, src, dst, dedupe: str | None):
    contract = {"name": src, "sync_mode": "incremental_deduped", "primary_key": "k",
                "cursor_field": "n", "cursor_semantics": "modification_timestamp", "selected": True}
    data = {"source_kind": "database", "dest_kind": "database", "source_table": src, "dest_table": dst,
            "async_mode": "false", "validation_mode": "strict", "sync_mode": "incremental_deduped",
            "source_format": "postgresql", "source_schema": "public", **sfmod._side("source", sfmod.PG_ENDPOINT),
            **PG_DEST, "stream_contracts_json": json.dumps([contract])}
    if dedupe is not None:
        # The schedule runner stamps both sides (schedule_runner: src/dst extra).
        stamp = {"in_file_dedupe": dedupe, "duplicate_policy": "upsert"}
        data["source_extra_json"] = json.dumps(stamp)
        data["dest_extra_json"] = json.dumps(stamp)
    return client.post("/api/v1/transfer/run", data=data)


@pytest.mark.parametrize("dedupe", [None, "keep_last"])
def test_schedule_shaped_keep_last_on_a_versioned_table_lands_the_latest_version(client, dedupe):
    src = sfmod._seed_pg("k text, v text, n int", VERSIONS)
    dst = f"r14vk_{uuid.uuid4().hex[:6]}"
    try:
        for run in (1, 2):
            resp = _versioned_run(client, src, dst, dedupe)
            assert resp.status_code == 200, (run, resp.text[:1200])
            got = dict(h.query("postgresql", f'SELECT k, v FROM public."{dst}"'))  # nosec B608
            assert got == {"K1": "v1-1", "K2": "v2-1", "K3": "v2-3", "K4": "Zoë ✓"}, (run, got)
    finally:
        h.drop_table("postgresql", src)
        h.drop_table("postgresql", dst)


def test_keyed_table_without_a_cursor_refuses_keep_last_with_keyed_routes():
    from services.data_integrity import (
        DUPLICATE_KEY_ROUTES_TABLE_KEYED,
        keep_last_table_refusal,
    )

    no_cursor = [{"name": "t", "sync_mode": "incremental_deduped", "primary_key": "k", "selected": True}]
    refusal = keep_last_table_refusal("incremental_deduped", stream_contracts=no_cursor, stream_name="t")
    assert DUPLICATE_KEY_ROUTES_TABLE_KEYED in refusal and "append" not in refusal
    cursor = [{**no_cursor[0], "cursor_field": "n"}]
    assert keep_last_table_refusal("incremental_deduped", stream_contracts=cursor, stream_name="t") == ""
    assert keep_last_table_refusal("incremental_deduped", source_type="kafka") == ""
    assert "append" in keep_last_table_refusal("full_refresh_overwrite", stream_contracts=cursor)


def test_lenient_note_on_a_table_names_the_table_routes():
    from services.data_integrity import DUPLICATE_KEY_ROUTES, DUPLICATE_KEY_ROUTES_TABLE, run_integrity_audit

    report = run_integrity_audit(
        mappings=[{"source": "sku", "target": "sku", "confidence": 1}],
        source_columns=["sku"],
        source_schemas=[{"name": "sku", "inferred_type": "VARCHAR"}],
        sample_rows=[{"sku": "A"}, {"sku": "A"}],
        validation_mode="lenient",
        destination_db_type="postgresql",
        sync_mode="full_refresh_overwrite",
        contract_primary_key="sku",
        database_extract=True,
    )
    check = next(c for c in report["checks"] if c["check"] == "duplicate_keys")
    assert check["blocks_transfer"] is True
    assert DUPLICATE_KEY_ROUTES_TABLE in check["note"], check["note"]
    assert DUPLICATE_KEY_ROUTES not in check["note"] and "keep_last" not in check["note"]


# ── 2. object-store sources (kind=database, record-batch) ───────────────────
def test_keep_last_on_an_object_store_source_reads_whole_and_collapses():
    from types import SimpleNamespace

    from services.duplicate_policy import keep_last_reads_whole, keep_last_records
    from src.transfer.engine import takes_streaming_route
    from src.transfer.models import EndpointConfig, TransferRequest

    rows = [{"sku": "A", "v": "1"}, {"sku": "B", "v": "2"}, {"sku": "A", "v": "3"}]
    contracts = [{"name": "t", "sync_mode": "full_refresh_overwrite", "primary_key": "sku", "selected": True}]
    for fmt in ("s3", "gcs", "adls", "sftp"):
        req = SimpleNamespace(
            source=EndpointConfig(kind="database", format=fmt, extra={"in_file_dedupe": "keep_last"}),
            destination=EndpointConfig(kind="database", format="postgresql"),
            stream_contracts=contracts,
        )
        assert keep_last_reads_whole(req), fmt
        assert keep_last_records(req, rows) == [{"sku": "A", "v": "3"}, {"sku": "B", "v": "2"}], fmt
    table = SimpleNamespace(
        source=EndpointConfig(kind="database", format="postgresql", extra={"in_file_dedupe": "keep_last"}),
        destination=EndpointConfig(kind="database", format="postgresql"),
        stream_contracts=contracts,
    )
    assert not keep_last_reads_whole(table) and keep_last_records(table, rows) == rows
    # The table stream does not collapse, so such a run takes the buffered route.
    s3 = TransferRequest(
        source=EndpointConfig(kind="database", format="s3", table="k.csv", extra={"in_file_dedupe": "keep_last"}),
        destination=EndpointConfig(kind="database", format="postgresql", table="t"),
        stream_contracts=contracts,
        sync_mode="full_refresh_overwrite",
    )
    assert takes_streaming_route(s3) is False


def test_s3_csv_with_keep_last_lands_one_row_per_key(tmp_path, monkeypatch, local_object_store):
    if not local_object_store:
        pytest.skip("no local object store endpoint (moto)")
    from urllib.parse import urlparse

    import boto3

    from tests.conftest import LOCAL_OBJECT_STORE_BUCKET

    h.isolate(tmp_path, monkeypatch)
    monkeypatch.delenv("DATAFLOW_SEED_DEMO", raising=False)
    key = f"r14/dups_{uuid.uuid4().hex[:6]}.csv"
    body = "sku,name\n" + "".join(f"SKU-{i:02d},item {i}\n" for i in range(1, 11)) + "SKU-07,dup item\n"
    boto3.client("s3", endpoint_url=local_object_store, aws_access_key_id="test",
                 aws_secret_access_key="test", region_name="us-east-1").put_object(
        Bucket=LOCAL_OBJECT_STORE_BUCKET, Key=key, Body=body.encode())
    from fastapi.testclient import TestClient
    from src.main import app

    parsed = urlparse(local_object_store)
    dst = f"r14s3_{uuid.uuid4().hex[:6]}"
    contract = [{"name": key, "sync_mode": "full_refresh_overwrite", "primary_key": "sku", "selected": True}]
    data = {"source_kind": "database", "dest_kind": "database", "source_format": "s3",
            "source_host": parsed.hostname or "127.0.0.1", "source_port": str(parsed.port or 443),
            "source_database": LOCAL_OBJECT_STORE_BUCKET, "source_username": "test",
            "source_password": "test", "source_connection_string": local_object_store,
            "source_table": key, "dest_table": dst, "async_mode": "false",
            "validation_mode": "lenient", "sync_mode": "full_refresh_overwrite", **PG_DEST,
            "stream_contracts_json": json.dumps(contract),
            "source_extra_json": json.dumps({"in_file_dedupe": "keep_last"})}
    try:
        resp = TestClient(app).post("/api/v1/transfer/run", data=data)
        assert resp.status_code == 200, resp.text[:1200]
        got = h.query("postgresql", f'SELECT count(*), count(DISTINCT sku) FROM public."{dst}"')[0]  # nosec B608
        assert got == (10, 10), got
        assert h.query("postgresql", f'SELECT name FROM public."{dst}" WHERE sku = \'SKU-07\'') == [("dup item",)]  # nosec B608
    finally:
        h.drop_table("postgresql", dst)


# ── 4. a schedule's keep_last is judged at save, and a saved one parks ──────
def _pg_connectors():
    from services.connector_store import create_connector

    sfx = uuid.uuid4().hex[:6]
    common = {k: h.PG[k] for k in ("host", "port", "database", "username", "password")}
    src = create_connector({"name": f"r14-src-{sfx}", "type": "postgresql", "schema": "public", **common})
    dst = create_connector({"name": f"r14-dst-{sfx}", "type": "postgresql", "schema": "public", **common})
    return src, dst


def _schedule_payload(src, dst, table, **over):
    payload = {
        "name": f"r14-keep-last-{uuid.uuid4().hex[:6]}", "source_connector_id": src.id,
        "source_table": table, "dest_connector_id": dst.id, "dest_table": f"r14ks_{uuid.uuid4().hex[:6]}",
        "interval": "daily", "enabled": False, "primary_key": "k", "in_file_dedupe": "keep_last",
        "mappings": [{"source": "k", "target": "k", "confidence": 1}],
    }
    payload.update(over)
    return payload


def test_schedule_save_refuses_keep_last_a_table_read_cannot_honour(tmp_path, monkeypatch):
    from services import schedule_store
    from services.duplicate_policy import DuplicatePolicyError

    h.isolate(tmp_path, monkeypatch)
    src, dst = _pg_connectors()
    table = sfmod._seed_pg("k text, v text, n int", VERSIONS)
    try:
        with pytest.raises(DuplicatePolicyError) as refused:
            schedule_store.create_schedule(_schedule_payload(src, dst, table, sync_mode="full_refresh_overwrite"))
        assert "keep_last" in str(refused.value) and "append" in str(refused.value)
        # A cursor-ordered keyed sync honours it: saved.
        ok = schedule_store.create_schedule(_schedule_payload(
            src, dst, table, sync_mode="incremental_deduped", cursor_column="n"))
        assert ok.in_file_dedupe == "keep_last"
        # ...and changing that schedule to a read with no order is refused too.
        with pytest.raises(DuplicatePolicyError):
            schedule_store.update_schedule(ok.id, {"sync_mode": "full_refresh_overwrite", "cursor_column": ""})
    finally:
        h.drop_table("postgresql", table)


def test_a_saved_schedule_that_cannot_honour_keep_last_parks_once(tmp_path, monkeypatch):
    import services.schedule_runner as runner
    from services import schedule_store

    h.isolate(tmp_path, monkeypatch)
    src, dst = _pg_connectors()
    table = sfmod._seed_pg("k text, v text, n int", VERSIONS)
    try:
        sched = schedule_store.create_schedule(_schedule_payload(
            src, dst, table, sync_mode="incremental_deduped", cursor_column="n", enabled=True))
        # Saved before the save-time rule existed: rewrite the stored row directly.
        rows = schedule_store._load_all()
        stale = next(s for s in rows if s.id == sched.id)
        stale.sync_mode = "full_refresh_overwrite"
        stale.cursor_column = ""
        schedule_store._save_all(rows)
        parked: list[str] = []
        monkeypatch.setattr(runner, "_park_on_decision", lambda sid, exc, **kw: parked.append(str(exc)))
        assert runner._dispatch_transfer(sched.id) is None
        assert len(parked) == 1 and "Schedule cannot run as saved" in parked[0], parked
        assert "keep_last" in parked[0] and "append" in parked[0]
    finally:
        h.drop_table("postgresql", table)


# ── 5. only a value-decided refusal drops sample rows by value ──────────────
def test_held_out_sample_filter_drops_by_value_only_for_cast_refusals():
    from src.transfer.reconcile_step import _without_held_out_cells

    sample = [{"k": "A", "v": "1"}, {"k": "B", "v": "abc"}, {"k": "C", "v": "2"}]
    unique = [{"row": 4, "column": "k", "value": "A",
               "reason": 'duplicate key value violates unique constraint "t_k_key"'}]
    # The landed first copy of A stays in the compare and the written-ids scope.
    assert _without_held_out_cells(sample, unique) == sample
    cast = [{"row": 2, "column": "v", "value": "abc", "reason": "Invalid decimal: 'abc'"}]
    assert _without_held_out_cells(sample, cast) == [sample[0], sample[2]]
    width = [{"row": 3, "column": "v", "value": "2",
              "reason": "value too long for type character varying(0)"}]
    assert _without_held_out_cells(sample, width) == [sample[0], sample[1]]
    not_null = [{"row": 9, "column": "k", "value": "C",
                 "reason": 'null value in column "k" violates not-null constraint'}]
    assert _without_held_out_cells(sample, not_null) == sample


# ── 6. ONE verdict per column (mode_holdout_kind) for every gate ──────────
@pytest.mark.parametrize("mode, good, bad, holds", [
    ("lenient", 1, 9, False), ("lenient", 6, 4, True), ("lenient", 5, 5, True),
    ("strict", 6, 4, False), ("balanced", 6, 4, False),
])
def test_every_cast_holdout_site_asks_the_one_verdict(mode, good, bad, holds):
    from services.coercion_probe import analyze_coercion
    from services.data_integrity import _check_financial_precision
    from services.transform_engine import dry_run_sample
    from services.validation_mode_contract import CAST_SHARE_RULE, mode_holdout_kind

    values = ["12.50"] * good + [f"x{i}" for i in range(bad)]
    m = {"source": "price", "target": "price", "target_type": "DECIMAL(10,2)", "transform": "decimal",
         "confidence": 1}
    rows = [{"price": v} for v in values]
    held = bad if mode == "lenient" else 0
    assert bool(mode_holdout_kind(mode, failed=bad, held=held, cast_ok=good, record_batch=True)) is holds
    fin = _check_financial_precision([m], {"price": "VARCHAR"}, rows, file_row_quarantine=True,
                                     validation_mode=mode)
    assert fin["blocks_transfer"] is (not holds), fin
    ok, errors = dry_run_sample(headers=["price"], sample_rows=[[v] for v in values], mappings=[m],
                                column_types={"price": "VARCHAR"}, file_row_quarantine=True,
                                validation_mode=mode)
    assert any(e.startswith("holdout ") for e in errors) is holds, errors
    if mode == "lenient" and not holds:
        assert any(CAST_SHARE_RULE in e for e in errors) and any(CAST_SHARE_RULE in i for i in fin["issues"])
    report = analyze_coercion(sample_rows=rows, mappings=[m], source_types={"price": "VARCHAR"},
                              dest_types={"price": "DECIMAL(10,2)"}, dest_db_type="postgresql",
                              validation_mode=mode, file_row_quarantine=True)
    col = next(c for c in report["columns"] if c["source"] == "price")
    assert bool(col.get("holdout_kind")) is holds, col
    if not holds:
        assert col["severity"] == "block", col  # no verdict, no warning


def _gates_for(client, mode, lines):
    from services.mongodb_service import get_mongodb_service

    table = f"r14g_{uuid.uuid4().hex[:6]}"
    h.execute("postgresql", f'CREATE TABLE public."{table}" (k text, d integer)')  # nosec B608
    data = {"source_kind": "file", "dest_kind": "database", "dest_table": table, "async_mode": "false",
            "validation_mode": mode, "sync_mode": "full_refresh_overwrite",
            "schema_policy": "propagate_columns", **PG_DEST}
    try:
        resp = client.post("/api/v1/transfer/run", data=data,
                           files={"file": ("i.csv", ("\n".join(lines) + "\n").encode(), "text/csv")})
        body = resp.json()
        job_id = body.get("job_id") or (body.get("detail") or {}).get("job_id")
        job = get_mongodb_service().get_job(job_id) or {}
        gates = {g.get("id"): g.get("status") for g in (job.get("preflight") or {}).get("gates") or []}
        landed = h.query("postgresql", f'SELECT count(*) FROM public."{table}"')[0][0]  # nosec B608
        return resp, gates, landed
    finally:
        h.drop_table("postgresql", table)


@pytest.mark.parametrize("mode", ["lenient", "strict"])
def test_one_castable_of_forty_into_integer_every_gate_blocks(client, mode):
    resp, gates, landed = _gates_for(client, mode, ["k,d", "a,5"] + [f"b{i},word{i}" for i in range(39)])
    assert resp.status_code == 422, resp.text[:600]
    for gate in ("g3_schema_contract", "g5_dry_run", "g6_target_ddl", "g8_reconciliation"):
        assert gates.get(gate) == "block", (mode, gate, gates)
    assert landed == 0


def test_lenient_mostly_castable_integer_column_loads_and_quarantines(client):
    lines = ["k,d"] + [f"a{i},{i}" for i in range(30)] + [f"b{i},word{i}" for i in range(10)]
    resp, gates, landed = _gates_for(client, "lenient", lines)
    assert resp.status_code == 200, resp.text[:800]
    for gate in ("g3_schema_contract", "g5_dry_run", "g6_target_ddl", "g8_reconciliation"):
        assert gates.get(gate) in ("pass", "warn"), (gate, gates)
    assert landed == 30


def test_a_mostly_unparseable_file_column_is_refused_naming_the_share_rule(client):
    from services.validation_mode_contract import CAST_SHARE_RULE

    table = f"r14sh_{uuid.uuid4().hex[:6]}"
    h.execute("postgresql", f'CREATE TABLE public."{table}" (k text, price numeric(10,2))')  # nosec B608
    lines = ["k,price", "a,12.50"] + [f"b{i},x{i}" for i in range(9)]
    data = {"source_kind": "file", "dest_kind": "database", "dest_table": table, "async_mode": "false",
            "validation_mode": "lenient", "sync_mode": "full_refresh_overwrite",
            "schema_policy": "propagate_columns", **PG_DEST}
    try:
        resp = client.post("/api/v1/transfer/run", data=data,
                           files={"file": ("p.csv", ("\n".join(lines) + "\n").encode(), "text/csv")})
        assert resp.status_code == 422, resp.text[:800]
        assert CAST_SHARE_RULE in resp.text, resp.text[:2000]
        assert h.query("postgresql", f'SELECT count(*) FROM public."{table}"') == [(0,)]  # nosec B608
    finally:
        h.drop_table("postgresql", table)


# ── 7. a kept column narrower than the data: held out or refused, never cut ──
_OVERFLOW = {
    # case: (live DDL, prior cell, fitting cell, overflowing cells)
    "varchar": ("varchar(3)", "old", "abc", {"x1": "abcdef", "x2": "abcdefgh"}),
    "numeric": ("numeric(6,2)", "1.00", "12.50", {"x1": "12345.67"}),
}


@pytest.mark.parametrize("policy", ["propagate_columns", "manual_review"])
@pytest.mark.parametrize("mode", ["strict", "lenient"])
@pytest.mark.parametrize("case", sorted(_OVERFLOW))
def test_overflow_into_a_kept_column_is_never_stored_cut(client, case, mode, policy):
    ddl, prior, fits, overflow = _OVERFLOW[case]
    table = f"r14w_{uuid.uuid4().hex[:6]}"
    h.execute("postgresql", f'CREATE TABLE public."{table}" (k text, c {ddl})')  # nosec B608
    h.execute("postgresql", f"INSERT INTO public.\"{table}\" VALUES ('old', '{prior}')")  # nosec B608
    lines = ["k,c"] + [f"r{i},{fits}" for i in range(298)] + [f"{k},{v}" for k, v in overflow.items()]
    if case == "numeric":
        lines.append("x2,9999.99")  # the column's largest value still lands
    data = {"source_kind": "file", "dest_kind": "database", "dest_table": table, "async_mode": "false",
            "validation_mode": mode, "sync_mode": "full_refresh_overwrite", "schema_policy": policy,
            **PG_DEST}
    try:
        resp = client.post("/api/v1/transfer/run", data=data,
                           files={"file": ("w.csv", ("\n".join(lines) + "\n").encode(), "text/csv")})
        rows = dict(h.query("postgresql", f'SELECT k, c::text FROM public."{table}"'))  # nosec B608
        # Whatever the verdict, an overflowing cell is never stored cut down.
        for key in overflow:
            assert key not in rows, (case, mode, policy, key, rows.get(key))
        if mode == "strict":
            assert resp.status_code == 422, resp.text[:800]
            assert rows == {"old": prior}, (case, policy, len(rows))  # the prior data survives
            return
        assert resp.status_code == 200, resp.text[:1200]
        assert "old" not in rows
        assert len(rows) == 300 - len(overflow), len(rows)
        assert set(rows.values()) == ({fits, "9999.99"} if case == "numeric" else {fits})
        job_id = resp.json()["job_id"]
        quarantine = client.get(f"/api/v1/connectors/jobs/{job_id}/quarantine")
        assert quarantine.status_code == 200, quarantine.text[:600]
        held = {(d.get("values") or {}).get("k"): d.get("value") for d in quarantine.json()["quarantine"]}
        assert held == overflow, quarantine.text[:1500]
    finally:
        h.drop_table("postgresql", table)


def test_the_overwrite_swap_never_casts_a_kept_column_down_to_its_width():
    import psycopg2

    from services.dest_overwrite_swap import pg_swap_staged_dest

    live, stage = f"r14sw_{uuid.uuid4().hex[:6]}", f"r14st_{uuid.uuid4().hex[:6]}"
    h.execute("postgresql", f'CREATE TABLE public."{live}" (k text, c varchar(3))')  # nosec B608
    h.execute("postgresql", f"INSERT INTO public.\"{live}\" VALUES ('old', 'old')")  # nosec B608
    h.execute("postgresql", f'CREATE TABLE public."{stage}" (k text, c text)')  # nosec B608
    h.execute("postgresql", f"INSERT INTO public.\"{stage}\" VALUES ('new', 'abcdef')")  # nosec B608
    conn = h.pg_conn()
    conn.autocommit = False  # the caller owns the swap's transaction
    try:
        with pytest.raises(psycopg2.Error, match="too long"):
            with conn.cursor() as cur:
                pg_swap_staged_dest(cur, schema="public", dest_table=live, staging_table=stage,
                                    quote=lambda n: f'"{n}"')
        conn.rollback()
        assert h.query("postgresql", f'SELECT k, c FROM public."{live}"') == [("old", "old")]  # nosec B608
    finally:
        conn.close()
        h.drop_table("postgresql", live)
        h.drop_table("postgresql", stage)


# ── 8. review nits ─────────────────────────────────────────────────────────
def test_an_approved_widen_is_the_carrier_not_the_kept_column():
    from src.transfer.file_stream import _kept_table_carriers

    live = {"k": "TEXT", "c": "VARCHAR(3)", "n": "NUMERIC(10,2)"}
    kept = _kept_table_carriers(live, ["k", "c", "n"], "postgresql", "full_refresh_overwrite")
    assert kept == live
    widened = _kept_table_carriers(live, ["k", "c", "n"], "postgresql", "full_refresh_overwrite", {"C"})
    assert widened == {"k": "TEXT", "n": "NUMERIC(10,2)"}


def test_a_scale_only_overflow_is_quarantined_as_rounding():
    from connectors.writer_quarantine_unfit import quarantine_unfit_decimals

    details: list[dict] = []
    out = quarantine_unfit_decimals([("1.2391",), ("123456789.5",)], ["v"], ["NUMERIC(10,2)"], details, "quarantine",
                                     dest_db="postgresql")
    assert out == []
    reasons = [d["reason"] for d in details]
    assert "would round to the column's scale, 2 decimal place(s)" in reasons[0], reasons
    assert "would truncate/overflow" in reasons[1], reasons
