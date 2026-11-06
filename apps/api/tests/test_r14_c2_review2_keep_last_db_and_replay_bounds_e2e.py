"""Second review of e84fe5af6 / c851e7c97 — live PostgreSQL, MySQL, SQL Server.

1. keep_last on a TABLE source cleared Validate and stored both copies
   (21 rows, 20 keys, "verified"). A table has no read order that picks a
   "last" copy and no table route writes it collapsed, so Validate refuses it
   with a clear 422 and offers only the routes that work there (append, or
   remove the repeats at the source) — each followed here. A FILE read whole
   (a row limit takes the buffered route) honours keep_last with the same
   collapse the file stream runs.
2. The replay's duplicate probe is bounded even outside a tool deadline.
3. Every remedy the replay 409 names is followed and the replay then lands.
4. Repeated NULL keys on a MERGE engine (SQL Server) are not a refused upsert.
5. Lenient text into an operator NUMERIC(10,2): the rows that parse load and
   'abc' is quarantined; Gate-8 hashes on the kept table's carrier ('1' is
   '1.00') and never keys the read-back on a held-out cell. A value the
   column would round ('1.239') is still held out, never stored rounded.
6. Lenient holds cells out only while at least half the sampled cells cast.
7. job_quarantine counts rows through the shared finding-row key.
"""

from __future__ import annotations

import json
import os
import time
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


ROWS = [(f"SKU-{i:05d}", f"item {i}", i) for i in range(1, 21)] + [("SKU-00007", "dup item", 99)]


def _seed(kind: str, rows) -> str:
    table = f"r14kl_{uuid.uuid4().hex[:6]}"
    if kind == "postgresql":
        return sfmod._seed_pg("sku text, name text, n int", rows)
    conn = h.mysql_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE `{table}` (sku varchar(20), name varchar(40), n int) CHARSET utf8mb4")
        cur.executemany(f"INSERT INTO `{table}` VALUES (%s, %s, %s)", rows)
    finally:
        conn.close()
    return table


def _source(kind: str) -> dict:
    if kind == "postgresql":
        return {"source_format": "postgresql", "source_schema": "public", **sfmod._side("source", sfmod.PG_ENDPOINT)}
    return {"source_format": "mysql", "source_host": "127.0.0.1", "source_port": "3306",
            "source_database": os.environ.get("MYSQL_DATABASE", "dataflow"),
            "source_username": os.environ.get("MYSQL_USER", "dataflow"),
            "source_password": os.environ.get("MYSQL_PASSWORD", "")}


def _run_table(client, kind, src, dst, *, field="", extra=None, sync="full_refresh_overwrite"):
    data = {"source_kind": "database", "dest_kind": "database", "source_table": src, "dest_table": dst,
            "async_mode": "false", "validation_mode": "lenient", "sync_mode": sync,
            **_source(kind), **PG_DEST,
            "stream_contracts_json": json.dumps([{"name": src, "sync_mode": sync, "primary_key": "sku", "selected": True}])}
    if field:
        data[field] = json.dumps(extra)
    return client.post("/api/v1/transfer/run", data=data)


def _dest_counts(dst):
    try:
        return h.query("postgresql", f'SELECT count(*), count(DISTINCT sku) FROM public."{dst}"')[0]  # nosec B608
    except Exception:
        return None


# ── 1. keep_last on a table is refused; the routes offered there work ───────
@pytest.mark.parametrize("kind", ["postgresql", "mysql"])
@pytest.mark.parametrize("field", ["source_extra_json", "dest_extra_json"])
def test_keep_last_on_a_table_source_is_refused_and_its_routes_work(client, kind, field):
    from services.data_integrity import DUPLICATE_KEY_ROUTES_TABLE, KEEP_LAST_TABLE_REFUSAL

    src = _seed(kind, ROWS)
    dst = f"r14kld_{uuid.uuid4().hex[:6]}"
    try:
        refused = _run_table(client, kind, src, dst, field=field, extra={"in_file_dedupe": "keep_last"})
        assert refused.status_code == 422, refused.text[:600]
        assert KEEP_LAST_TABLE_REFUSAL in refused.text, refused.text[:1500]
        assert _dest_counts(dst) is None  # nothing written, never 21 rows "verified"
        plain = _run_table(client, kind, src, dst)
        assert plain.status_code == 422 and DUPLICATE_KEY_ROUTES_TABLE in plain.text, plain.text[:800]
        assert "in_file_dedupe=keep_last (one row" not in plain.text  # a file route, not offered here
        # Route: duplicate_policy=append loads every copy, as it says.
        appended = _run_table(client, kind, src, dst, field="dest_extra_json",
                              extra={"duplicate_policy": "append"})
        assert appended.status_code == 200, appended.text[:800]
        assert _dest_counts(dst) == (21, 20)
        # Route: remove the repeats at the source, then the load is one row per key.
        h.execute(kind, f"DELETE FROM {h.quote(kind, src)} WHERE n = 99")  # nosec B608
        clean = _run_table(client, kind, src, dst)
        assert clean.status_code == 200, clean.text[:800]
        assert _dest_counts(dst) == (20, 20)
    finally:
        h.drop_table(kind, src)
        h.drop_table("postgresql", dst)


def _dups_csv() -> bytes:
    lines = ["sku,name,price"] + [f"SKU-{i:05d},item {i},{i}.50" for i in range(1, 21)]
    lines.append("SKU-00007,dup item,9.99")
    return ("\n".join(lines) + "\n").encode()


def test_keep_last_on_a_file_read_whole_keeps_the_last_copy(client):
    dst = f"r14klf_{uuid.uuid4().hex[:6]}"
    data = {"source_kind": "file", "dest_kind": "database", "dest_table": dst, "async_mode": "false",
            "validation_mode": "lenient", "sync_mode": "full_refresh_overwrite", **PG_DEST,
            "limit": "1000",  # a row cap reads the file whole (the buffered route)
            "stream_contracts_json": json.dumps(
                [{"name": dst, "sync_mode": "full_refresh_overwrite", "primary_key": "sku", "selected": True}]),
            "dest_extra_json": json.dumps({"in_file_dedupe": "keep_last"})}
    try:
        resp = client.post("/api/v1/transfer/run", data=data, files={"file": ("d.csv", _dups_csv(), "text/csv")})
        assert resp.status_code == 200, resp.text[:800]
        assert _dest_counts(dst) == (20, 20)
        got = h.query("postgresql", f'SELECT name FROM public."{dst}" WHERE sku = \'SKU-00007\'')  # nosec B608
        assert got == [("dup item",)], got
    finally:
        h.drop_table("postgresql", dst)


# ── 2 / 3. the replay: bounded probe, and every 409 remedy lands ────────────
def _bad_price_csv(*, dup: bool = False) -> bytes:
    lines = ["sku,name,price"] + [f"SKU-{i:05d},item {i},{i}.50" for i in range(1, 21)]
    if dup:
        lines.append("SKU-00007,dup item,9.99")
    lines.append("SKU-00030,bad price,abc")
    return ("\n".join(lines) + "\n").encode()


def _run_file(client, dst, csv: bytes, extra: dict | None = None):
    data = {"source_kind": "file", "dest_kind": "database", "dest_table": dst, "async_mode": "false",
            "validation_mode": "lenient", "sync_mode": "full_refresh_overwrite", **PG_DEST,
            "stream_contracts_json": json.dumps(
                [{"name": dst, "sync_mode": "full_refresh_overwrite", "primary_key": "sku", "selected": True}])}
    if extra:
        data["dest_extra_json"] = json.dumps(extra)
    resp = client.post("/api/v1/transfer/run", data=data, files={"file": ("p.csv", csv, "text/csv")})
    assert resp.status_code == 200, resp.text[:800]
    return str(resp.json().get("job_id"))


EDIT = [{"sku": "SKU-00030", "name": "bad price", "price": "30.50"}]


def test_replay_duplicate_probe_is_bounded_without_a_tool_deadline(client, monkeypatch):
    import services.upsert_ddl_gate as gate

    dst = f"r14rb_{uuid.uuid4().hex[:6]}"
    try:
        job_id = _run_file(client, dst, _bad_price_csv())
        monkeypatch.setattr(gate, "DEST_DUPLICATE_KEY_STEP_SECONDS", 0.5)
        real = gate.count_dest_duplicate_keys

        def slow(*a, **k):  # a destination whose GROUP BY takes 6 s
            time.sleep(6)
            return real(*a, **k)

        monkeypatch.setattr(gate, "count_dest_duplicate_keys", slow)
        started = time.monotonic()
        replay = client.post(f"/api/v1/connectors/jobs/{job_id}/quarantine/replay", json={"rows": EDIT})
        elapsed = time.monotonic() - started
        assert replay.status_code == 200, replay.text[:600]
        assert elapsed < 5.5, elapsed  # the probe gave up at its budget
        assert replay.json()["rows_written"] == 1
    finally:
        h.drop_table("postgresql", dst)


@pytest.mark.parametrize("remedy", ["delete_extra_copies", "rerun_parent_keep_last"])
def test_each_replay_409_remedy_then_lands(client, remedy):
    dst = f"r14r9_{uuid.uuid4().hex[:6]}"
    try:
        job_id = _run_file(client, dst, _bad_price_csv())
        h.execute("postgresql", f"INSERT INTO public.\"{dst}\" (sku, name, price) VALUES ('SKU-00007', 'dup item', 9.99)")  # nosec B608
        refused = client.post(f"/api/v1/connectors/jobs/{job_id}/quarantine/replay", json={"rows": EDIT})
        assert refused.status_code == 409, refused.text[:600]
        next_action = refused.json()["detail"]["next_action"]
        assert "sync_mode=upsert" not in next_action, next_action
        if remedy == "delete_extra_copies":
            assert "delete the extra copies" in next_action
            h.execute("postgresql", f"DELETE FROM public.\"{dst}\" WHERE name = 'dup item'")  # nosec B608
            replay_of = job_id
        else:
            assert "in_file_dedupe=keep_last" in next_action and "full_refresh_overwrite" in next_action
            replay_of = _run_file(client, dst, _bad_price_csv(dup=True), {"in_file_dedupe": "keep_last"})
        replay = client.post(f"/api/v1/connectors/jobs/{replay_of}/quarantine/replay", json={"rows": EDIT})
        assert replay.status_code == 200, replay.text[:600]
        assert _dest_counts(dst) == (21, 21)
    finally:
        h.drop_table("postgresql", dst)


# ── 4. NULL keys on a MERGE engine ──────────────────────────────────────────
def test_sql_server_upsert_over_repeated_null_keys_is_not_refused(client):
    h.mssql_conn().close()
    src = sfmod._seed_pg("id integer PRIMARY KEY, label text", [(1, "a"), (2, "Zoë ✓")])
    dst = f"r14nk_{uuid.uuid4().hex[:6]}"
    h.execute("sqlserver", f"CREATE TABLE [{dst}] (id int, label nvarchar(max))")
    h.execute("sqlserver", f"INSERT INTO [{dst}] VALUES (NULL, N'x'), (NULL, N'y')")
    data = {"source_kind": "database", "dest_kind": "database", "source_table": src, "dest_table": dst,
            "async_mode": "false", "validation_mode": "strict", "sync_mode": "upsert",
            "source_format": "postgresql", "source_schema": "public", **sfmod._side("source", sfmod.PG_ENDPOINT),
            "dest_format": "sqlserver", "dest_host": "127.0.0.1", "dest_port": "1433", "dest_database": "dataflow",
            "dest_username": os.environ.get("DATAFLOW_TEST_MSSQL_USER", "sa"),
            "dest_password": os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD", ""), "dest_schema": "dbo",
            "dest_extra_json": json.dumps({"trust_server_certificate": True}),
            "stream_contracts_json": json.dumps([{"name": src, "sync_mode": "upsert", "primary_key": "id", "selected": True}])}
    try:
        resp = client.post("/api/v1/transfer/run", data=data)
        assert resp.status_code == 200, resp.text[:1200]
        got = sorted(h.query("sqlserver", f"SELECT id, label FROM [{dst}] WHERE id IS NOT NULL"))
        assert got == [(1, "a"), (2, "Zoë ✓")], got
        assert len(h.query("sqlserver", f"SELECT id FROM [{dst}] WHERE id IS NULL")) == 2
    finally:
        h.drop_table("postgresql", src)
        h.drop_table("sqlserver", dst)


# ── 5. text into an operator NUMERIC(10,2) ──────────────────────────────────
def _scaled_run(client, table, lines):
    data = {"source_kind": "file", "dest_kind": "database", "dest_table": table, "async_mode": "false",
            "validation_mode": "lenient", "sync_mode": "full_refresh_overwrite",
            "schema_policy": "propagate_columns", **PG_DEST}
    return client.post("/api/v1/transfer/run", data=data,
                       files={"file": ("v.csv", ("\n".join(lines) + "\n").encode(), "text/csv")})


@pytest.mark.parametrize("layout", ["bad_first", "bad_middle", "keyless_bad_first"])
def test_text_into_scaled_numeric_loads_and_quarantines_abc(client, layout):
    table = f"r14sc_{uuid.uuid4().hex[:6]}"
    body = [f"r{i},1" for i in range(699)]
    if layout == "keyless_bad_first":
        h.execute("postgresql", f'CREATE TABLE public."{table}" (v numeric(10,2))')  # nosec B608
        lines = ["v", "abc"] + ["1"] * 699
    else:
        h.execute("postgresql", f'CREATE TABLE public."{table}" (k text, v numeric(10,2))')  # nosec B608
        lines = ["k,v", "bad,abc", *body] if layout == "bad_first" else ["k,v", *body[:300], "bad,abc", *body[300:]]
    try:
        resp = _scaled_run(client, table, lines)
        assert resp.status_code == 200, resp.text[:1000]
        rows = h.query("postgresql", f'SELECT count(*), min(v), max(v) FROM public."{table}"')  # nosec B608
        assert rows[0][0] == 699 and str(rows[0][1]) == "1.00" == str(rows[0][2]), rows
        job = h.wait_job(str(resp.json().get("job_id")))
        assert job.get("status") == "completed_with_quarantine", job.get("status")
        q = client.get(f"/api/v1/connectors/jobs/{job.get('job_id') or job.get('_id')}/quarantine").json()
        assert [d["value"] for d in q["quarantine"]] == ["abc"], q["quarantine"]
    finally:
        h.drop_table("postgresql", table)


def test_a_value_the_column_would_round_is_never_stored_rounded(client):
    table = f"r14sr_{uuid.uuid4().hex[:6]}"
    h.execute("postgresql", f'CREATE TABLE public."{table}" (k text, v numeric(10,2))')  # nosec B608
    lines = ["k,v", "bad,abc", "round,1.239", *[f"r{i},1" for i in range(300)]]
    try:
        resp = _scaled_run(client, table, lines)
        stored = h.query("postgresql", f'SELECT v FROM public."{table}" WHERE k = \'round\'')  # nosec B608
        assert stored in ([], None) or all(str(r[0]) != "1.24" for r in stored), (resp.status_code, stored)
        if resp.status_code == 200:
            job_id = str(resp.json().get("job_id"))
            q = client.get(f"/api/v1/connectors/jobs/{job_id}/quarantine").json()
            assert "1.239" in {str(d.get("value")) for d in q["quarantine"]}, q["quarantine"]
    finally:
        h.drop_table("postgresql", table)


# ── 6. lenient holds cells out only while most cells cast ───────────────────
def test_lenient_holds_out_only_while_at_least_half_the_cells_cast():
    from services.coercion_probe import samples_coerce_mapping
    from services.validation_mode_contract import LENIENT_MIN_CAST_SHARE, mode_holdout_kind

    m = {"source": "v", "target": "v", "target_type": "NUMERIC(10,2)", "transform": "decimal"}
    kw = dict(source_types={"v": "VARCHAR"}, target_types={"v": "NUMERIC(10,2)"})
    one_of_500 = [{"v": "1"}] + [{"v": f"x{i}"} for i in range(499)]
    most = [{"v": "1"}] * 6 + [{"v": "abc"}] * 4
    assert samples_coerce_mapping(m, rows=one_of_500, validation_mode="lenient", **kw) is False
    assert samples_coerce_mapping(m, rows=most, validation_mode="lenient", **kw) is True
    assert samples_coerce_mapping(m, rows=most, validation_mode="strict", **kw) is False
    assert samples_coerce_mapping(m, rows=most, **kw) is False  # no mode: every refusal fails
    assert LENIENT_MIN_CAST_SHARE == 0.5
    assert mode_holdout_kind("lenient", failed=1, held=1, cast_ok=1, record_batch=True) == "cast_failure"
    assert mode_holdout_kind("lenient", failed=499, held=499, cast_ok=1, record_batch=True) == ""


# ── 7. one row key for job_quarantine too ───────────────────────────────────
def test_job_quarantine_counts_rows_with_the_shared_key():
    from src.transfer.job_quarantine import quarantined_row_count, rows_with_findings

    same = {"id": "2", "ratio": "abc"}
    details = [{"row": same, "source_row_index": 2}, {"row": dict(same), "source_row_index": 3},
               {"row": 7, "column": "a"}, {"row": 7, "column": "b"}, {"reason": "unit refused"}]
    assert rows_with_findings(details) == 3
    assert quarantined_row_count(details) == 4
