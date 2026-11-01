"""QA round 12 (build 8f7d246d), Snowflake: D7, D8, D9, D13, D3 end to end.

Every test drives ``POST /api/v1/transfer/run`` between the live PostgreSQL and
fakesnow, with NULL, '', whitespace, unicode and decimal cells, and runs a
second time into the table the first run created.

fakesnow runs on DuckDB, which folds unquoted names to UPPER like Snowflake
but matches *quoted* names case-insensitively, so a quoted ``"id"`` against a
stored ``ID`` silently works there and fails on Snowflake (000904). The D120
``snowflake_semantics`` fixture restores Snowflake's rule, and every test also
asserts the statements actually sent (quoting and casing), because those are
what a real account judges.

- D8: the transform proof read the destination as ``SELECT "id", "qty" …`` —
  000904 on Snowflake, so every shaped Snowflake run was "unverifiable".
- D7: ``cast_column qty -> text`` re-inferred the shaped '5' as INTEGER, so
  the destination kept a NUMBER column (PostgreSQL got bigint too).
- D9: with ``on_error=divert`` the proof paired outputs to inputs by batch
  position; a diverted row shifted every later pair, the SQL oracle
  "disagreed", the job failed and the table it created was dropped.
- D13: ADD COLUMN for drifted source columns quoted the lowercase name, so
  Snowflake made ``"extra_col"`` and the MERGE's ``"EXTRA_COL"`` was 000904.
- D3: Snowflake's folded ``ID`` landed in a new PostgreSQL table as ``"ID"``.
"""

from __future__ import annotations

import json
import re
import uuid
from decimal import Decimal
from typing import Any

import pytest

pytest.importorskip("fakesnow")

from tests import r4_route_harness as h  # noqa: E402
from tests.test_d120_snowflake_strict_e2e import snowflake_semantics  # noqa: E402,F401

pytestmark = pytest.mark.timeout(600)

SENT: list[str] = []


@pytest.fixture
def client(tmp_path, monkeypatch, snowflake_semantics):  # noqa: F811
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    from fakesnow.cursor import FakeSnowflakeCursor

    strict = FakeSnowflakeCursor.execute

    def recording(self, command, *args, **kwargs):
        SENT.append(str(command))
        return strict(self, command, *args, **kwargs)

    monkeypatch.setattr(FakeSnowflakeCursor, "execute", recording)
    SENT.clear()
    from fastapi.testclient import TestClient
    from src.main import app

    return TestClient(app)


def _sf_conn():
    from connectors.snowflake_conn import get_connection

    return get_connection(
        account="localhost", username="test", password="test",
        database="dataflow", schema="PUBLIC", warehouse="", connection_string="",
    )


def _sf(sql: str) -> list[tuple]:
    conn = _sf_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [tuple(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _sf_columns(table: str) -> dict[str, str]:
    return {str(r[0]): str(r[1]) for r in _sf(f'DESC TABLE "PUBLIC"."{table.upper()}"')}


def _sf_ddl(sql: str) -> None:
    conn = _sf_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE DATABASE IF NOT EXISTS dataflow")
            cur.execute("CREATE SCHEMA IF NOT EXISTS dataflow.public")
            cur.execute(sql)
    finally:
        conn.close()


PG_ENDPOINT = {
    "host": h.PG["host"], "port": str(h.PG["port"]), "database": h.PG["database"],
    "username": h.PG["username"], "password": h.PG["password"],
}
SF_ENDPOINT = {
    "host": "localhost", "port": "443", "database": "dataflow",
    "username": "test", "password": "test", "schema": "public",
}


def _side(prefix: str, ep: dict[str, str]) -> dict[str, str]:
    return {f"{prefix}_{k}": v for k, v in ep.items()}


def _run(client, *, source: str, dest: str, to_snowflake: bool = True, **extra: Any) -> dict:
    data = {
        "source_kind": "database",
        "dest_kind": "database",
        "source_table": source,
        "dest_table": dest,
        "async_mode": "false",
        "validation_mode": "strict",
        "sync_mode": "full_refresh_overwrite",
    }
    if to_snowflake:
        data.update(source_format="postgresql", dest_format="snowflake",
                    source_schema="public", **_side("source", PG_ENDPOINT),
                    **_side("dest", SF_ENDPOINT))
    else:
        data.update(source_format="snowflake", dest_format="postgresql",
                    dest_schema="public", **_side("source", SF_ENDPOINT),
                    **_side("dest", PG_ENDPOINT))
    for key, value in extra.items():
        data[key] = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
    resp = client.post("/api/v1/transfer/run", data=data)
    if resp.status_code == 422:
        # A synchronous run that failed answers 422 with the job's error.
        detail = resp.json().get("detail") or {}
        return {"status": "failed", **(detail if isinstance(detail, dict) else {"error": detail})}
    assert resp.status_code == 200, resp.text
    body = resp.json()
    job_id = str(body.get("job_id") or (body.get("job") or {}).get("id") or "")
    return h.wait_job(job_id) if job_id else body


def _recon(job: dict) -> dict:
    result = job.get("result") or {}
    return job.get("reconciliation") or result.get("reconciliation") or {}


def _shaped(recipe: dict) -> dict[str, Any]:
    from services.shape_models import ShapeRecipe

    return {
        "shape_recipe_json": recipe,
        "approved_shape_recipe_hash": ShapeRecipe.parse(recipe).recipe_hash,
    }


def _seed_pg(ddl: str, rows: list[tuple]) -> str:
    table = f"r12sf_{uuid.uuid4().hex[:8]}"
    conn = h.pg_conn()
    try:
        cur = conn.cursor()
        cur.execute(f'CREATE TABLE public."{table}" ({ddl})')  # nosec B608
        marks = ", ".join(["%s"] * len(rows[0]))
        cur.executemany(f'INSERT INTO public."{table}" VALUES ({marks})', rows)  # nosec B608
    finally:
        conn.close()
    return table


# Quoted lower/mixed-case column references a real Snowflake rejects against
# the folded names the writer created (``AS "id"`` aliases are fine).
_LOWER_REF = re.compile(r'(?<![Aa][Ss] )"(id|qty|ratio|label|amt|extra_col|extra_num)"')


def _assert_no_lowercase_refs(table: str) -> None:
    hits = [
        s for s in SENT
        if table.upper() in s.upper() and _LOWER_REF.search(s)
    ]
    assert not hits, hits[:3]


_ROWS = [
    (1, 5, Decimal("3.00"), "plain", Decimal("10.5000")),
    (2, None, Decimal("1.50"), None, Decimal("-0.0001")),
    (3, 7, None, "", Decimal("0")),
    (4, 8, Decimal("4.25"), "   ", None),
    (5, 1, Decimal("2.00"), "Zoë 東京 ✓", Decimal("123456.7891")),
    (6, 0, Decimal("0.50"), " padded ", Decimal("1.0000")),
]
_DDL = "id integer PRIMARY KEY, qty integer, ratio numeric(10,2), label text, amt numeric(12,4)"


def test_d7_cast_to_text_lands_as_text_on_snowflake(client):
    src = _seed_pg(_DDL, _ROWS)
    dst = f"r12sf_d78_{uuid.uuid4().hex[:6]}"
    recipe = {"steps": [{"op": "cast_column", "column": "qty", "options": {"to_type": "text"}}]}
    try:
        for run in (1, 2):
            SENT.clear()
            job = _run(client, source=src, dest=dst, **_shaped(recipe))
            assert job.get("status") == "completed", (run, job.get("error"), job.get("message"))
            recon = _recon(job)
            proof = recon.get("transform_applied") or {}
            assert proof.get("status") == "verified", (run, proof, recon.get("message"))
            _assert_no_lowercase_refs(dst)
            # D7: the cast reached the DDL — QTY is text, not NUMBER.
            cols = _sf_columns(dst)
            assert cols["QTY"].upper().startswith(("VARCHAR", "TEXT")), cols
            assert cols["AMT"].upper().startswith("NUMBER(12,4)"), cols
            got = {
                int(r[0]): r[1:]
                for r in _sf(f'SELECT "ID", "QTY", "LABEL", "AMT" FROM "PUBLIC"."{dst.upper()}"')
            }
            assert got[1] == ("5", "plain", Decimal("10.5000"))
            assert got[2] == (None, None, Decimal("-0.0001"))
            assert got[3][:2] == ("7", "")
            assert got[4] == ("8", "   ", None)
            assert got[5] == ("1", "Zoë 東京 ✓", Decimal("123456.7891"))
            assert got[6][:2] == ("0", " padded ")
    finally:
        h.drop_table("postgresql", src)


def test_d8_mcp_transform_proof_reads_the_writers_folded_names(client):
    """MCP start_transfer -> confirm -> job, any transform, into a new table."""
    from conftest import spend_pilot_ack
    from services.connector_store import create_connector, delete_connector
    from src.ai.copilot.tools import get_pilot_tools

    src = _seed_pg(_DDL, _ROWS)
    sfx = uuid.uuid4().hex[:6]
    pg = create_connector({
        "name": f"pg-{sfx}", "type": "postgresql", "schema": "public",
        **{k: h.PG[k] for k in ("host", "port", "database", "username", "password")},
    })
    sf = create_connector({
        "name": f"sf-{sfx}", "type": "snowflake", "host": "localhost", "port": 443,
        "database": "dataflow", "username": "test", "password": "test",
        "schema": "public", "warehouse": "COMPUTE_WH",
    })
    dst = f"r12sf_d8_{sfx}"
    try:
        for run in (1, 2):
            SENT.clear()
            staged = get_pilot_tools().execute("start_transfer", {
                "source_connector_id": pg.id, "source_table": src,
                "dest_connector_id": sf.id, "dest_table": dst,
                "sync_mode": "full_refresh_overwrite",
                "column_transforms": {"label": ["trim", "upper"]},
            })
            assert staged.success, staged.error
            job = h.wait_job(spend_pilot_ack(staged.output["ack_id"], "r12-d8")["job_id"])
            assert job.get("status") == "completed", (run, job.get("error"))
            recon = _recon(job)
            proof = recon.get("transform_applied") or {}
            # Base: "unverifiable — 000904 invalid identifier '"id"'".
            assert proof.get("status") == "verified", (run, proof, recon.get("message"))
            assert "000904" not in str(recon.get("message") or ""), recon.get("message")
            _assert_no_lowercase_refs(dst)
            got = {
                int(r[0]): r[1]
                for r in _sf(f'SELECT "ID", "LABEL" FROM "PUBLIC"."{dst.upper()}"')
            }
            assert got == {1: "PLAIN", 2: None, 3: "", 4: "", 5: "ZOË 東京 ✓", 6: "PADDED"}, got
    finally:
        delete_connector(pg.id)
        delete_connector(sf.id)
        h.drop_table("postgresql", src)


def test_d9_cast_with_divert_loads_the_castable_rows_and_diverts_the_rest(client):
    src = _seed_pg(_DDL, _ROWS)
    dst = f"r12sf_d9_{uuid.uuid4().hex[:6]}"
    recipe = {"steps": [{
        "op": "cast_column", "column": "ratio", "options": {"to_type": "integer"},
        "on_error": "divert",
    }]}
    try:
        for run in (1, 2):
            SENT.clear()
            job = _run(client, source=src, dest=dst, **_shaped(recipe))
            # Three rows diverted to quarantine; the rest loaded and verified.
            assert job.get("status") == "completed_with_quarantine", (
                run, job.get("status"), job.get("error"), job.get("message"))
            proof = _recon(job).get("transform_applied") or {}
            assert proof.get("status") == "verified", (run, proof)
            _assert_no_lowercase_refs(dst)
            got = {
                int(r[0]): r[1:]
                for r in _sf(f'SELECT "ID", "RATIO", "LABEL" FROM "PUBLIC"."{dst.upper()}"')
            }
            # 1.50, 4.25 and 0.50 are not integers: diverted. NULL casts to NULL.
            assert sorted(got) == [1, 3, 5], got
            assert got[1] == (3, "plain") and got[3] == (None, "") and got[5] == (2, "Zoë 東京 ✓")
    finally:
        h.drop_table("postgresql", src)


def test_d9_a_failed_transform_proof_keeps_the_table_it_created(client, monkeypatch):
    import src.transfer.reconcile_step as rs

    def failed(**_kwargs):
        return {
            "status": "failed", "reason": "forced", "recipe_hash": "x",
            "culprit_steps": ["step 1 (cast_column on 'qty')"],
        }

    monkeypatch.setattr(rs, "_transform_applied_proof", failed)
    src = _seed_pg(_DDL, _ROWS)
    dst = f"r12sf_keep_{uuid.uuid4().hex[:6]}"
    recipe = {"steps": [{"op": "cast_column", "column": "qty", "options": {"to_type": "text"}}]}
    try:
        job = _run(client, source=src, dest=dst, **_shaped(recipe))
        assert job.get("status") == "failed", job
        text = f"{job.get('error')} {job.get('message')}"
        assert "Transform-applied proof failed" in text, text
        assert "Data kept at" in text and "was not dropped" in text, text
        assert "was dropped" not in text.replace("was not dropped", ""), text
        assert len(_sf(f'SELECT "ID" FROM "PUBLIC"."{dst.upper()}"')) == len(_ROWS)
    finally:
        h.drop_table("postgresql", src)


def test_d13_pg_drift_adds_snowflake_columns_with_folded_names(client):
    src = _seed_pg("id integer PRIMARY KEY, val text", [(1, "a"), (2, None), (3, "")])
    dst = f"r12sf_d13_{uuid.uuid4().hex[:6]}"
    contracts = [{"name": src, "sync_mode": "upsert", "primary_key": "id", "selected": True}]
    args = dict(sync_mode="upsert", schema_policy="propagate_columns",
                stream_contracts_json=contracts)
    try:
        job = _run(client, source=src, dest=dst, **args)
        assert job.get("status") == "completed", job.get("error")
        h.execute(
            "postgresql",
            f'ALTER TABLE public."{src}" ADD COLUMN extra_col text, '  # nosec B608
            "ADD COLUMN extra_num numeric(38,6)",
        )
        h.execute(
            "postgresql",
            f'INSERT INTO public."{src}" VALUES (4, \'d\', \'x\', 1.5), '  # nosec B608
            "(5, '', NULL, NULL), (6, ' ', '中文 ✓', -0.000001), (7, NULL, '  ', 123456.123456)",
        )
        for run in (2, 3):  # the drift run, then a run into the evolved table
            SENT.clear()
            job = _run(client, source=src, dest=dst, **args)
            assert job.get("status") == "completed", (run, job.get("error"))
            _assert_no_lowercase_refs(dst)
            if run == 2:
                adds = [s for s in SENT if "ADD COLUMN" in s.upper()]
                assert any('ADD COLUMN "EXTRA_COL" VARCHAR' in s for s in adds), adds
                assert any('ADD COLUMN "EXTRA_NUM" NUMBER(38,6)' in s for s in adds), adds
            cols = _sf_columns(dst)
            assert "extra_col" not in cols and "extra_num" not in cols, cols
            assert cols["EXTRA_NUM"].upper() == "NUMBER(38,6)", cols
            got = {
                int(r[0]): r[1:]
                for r in _sf(
                    f'SELECT "ID", "VAL", "EXTRA_COL", "EXTRA_NUM" FROM "PUBLIC"."{dst.upper()}"'
                )
            }
            assert got[1] == ("a", None, None) and got[2] == (None, None, None)
            assert got[3] == ("", None, None)
            assert got[4] == ("d", "x", Decimal("1.500000"))
            assert got[5] == ("", None, None)
            assert got[6] == (" ", "中文 ✓", Decimal("-0.000001"))
            assert got[7] == (None, "  ", Decimal("123456.123456"))
    finally:
        h.drop_table("postgresql", src)


def _seed_sf() -> str:
    table = f"R12SF_D3_{uuid.uuid4().hex[:6].upper()}"
    _sf_ddl(
        f'CREATE TABLE dataflow.public.{table} (id INT, amount NUMBER(38,6), '
        '"MixedCase" VARCHAR, label VARCHAR)'
    )
    conn = _sf_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO dataflow.public.{table} VALUES "
                "(1, 1.5, 'a', 'plain'), (2, NULL, NULL, ''), "
                "(3, -0.000001, ' ', 'Zoë 東京 ✓'), (4, 123456.123456, '  ', NULL)"
            )
    finally:
        conn.close()
    return table


def _pg_columns(table: str) -> list[str]:
    return [
        r[0] for r in h.query(
            "postgresql",
            "SELECT column_name FROM information_schema.columns WHERE table_schema = "
            f"'public' AND table_name = '{table}' ORDER BY ordinal_position",  # nosec B608
        )
    ]


def test_d3_snowflake_to_new_postgres_table_uses_lowercase_names(client):
    src = _seed_sf()
    dst = f"r12sf_d3_{uuid.uuid4().hex[:6]}"
    try:
        for run in (1, 2):
            job = _run(client, source=src, dest=dst, to_snowflake=False)
            assert job.get("status") == "completed", (run, job.get("error"))
            # Folded names land lower-case; a quoted mixed-case name keeps its case.
            assert _pg_columns(dst) == ["id", "amount", "MixedCase", "label"]
            rows = h.query(
                "postgresql",
                f'SELECT id, amount, "MixedCase", label FROM public."{dst}" ORDER BY id',  # nosec B608
            )
            assert [(int(r[0]), r[1], r[2], r[3]) for r in rows] == [
                (1, Decimal("1.500000"), "a", "plain"),
                (2, None, None, ""),
                (3, Decimal("-0.000001"), " ", "Zoë 東京 ✓"),
                (4, Decimal("123456.123456"), "  ", None),
            ]
    finally:
        h.drop_table("postgresql", dst)


def test_d3_an_existing_uppercase_postgres_table_keeps_working(client):
    src = _seed_sf()
    dst = f"r12sf_d3l_{uuid.uuid4().hex[:6]}"
    h.execute(
        "postgresql",
        f'CREATE TABLE public."{dst}" ("ID" numeric PRIMARY KEY, "AMOUNT" numeric(38,6), '  # nosec B608
        '"MixedCase" text, "LABEL" text)',
    )
    contracts = [{"name": src, "sync_mode": "upsert", "primary_key": "ID", "selected": True}]
    try:
        for run in (1, 2):
            job = _run(client, source=src, dest=dst, to_snowflake=False,
                       sync_mode="upsert", stream_contracts_json=contracts)
            assert job.get("status") == "completed", (run, job.get("error"))
            assert _pg_columns(dst) == ["ID", "AMOUNT", "MixedCase", "LABEL"]
            assert h.query("postgresql", f'SELECT count(*) FROM public."{dst}"')[0][0] == 4  # nosec B608
    finally:
        h.drop_table("postgresql", dst)
