"""QA round 11 (build 9c75ada4): a destination NUMERIC sized from a sample.

QA, every time:

* ``clean.parquet`` (500 rows, price 0.5 - 99.9) -> PostgreSQL with
  full_refresh_overwrite failed: ``price`` was created ``NUMERIC(2,1)`` and
  491 cells were rejected;
* a recipe ``derive amount*2`` over 1,000 rows (amount = id x 1.37) created
  ``NUMERIC(5,2)`` and 636 rows overflowed.

Root causes: Execute created the table from Map's 8-row automap stamp while
Validate had widened only its own copy of the mappings (two sources of truth);
a parquet column the file declares as *string* and that we profiled as numeric
was treated as a declared domain; and a derived column was re-inferred from the
shaped sample and then treated as declared, so the population scan never
widened it.

Each test drives the Studio's REST route (``/api/v1/connectors/upload`` ->
``/api/v1/transfer/run``) into a real PostgreSQL — MCP ``start_transfer`` has no
file-upload or recipe argument — with NULL, '', whitespace and unicode cells,
and a second run into the table the first run created.
"""

from __future__ import annotations

import io
import json
import uuid
from decimal import Decimal

import pytest

from tests import r4_route_harness as h
from tests import r11_pg_writer_route as r

pytestmark = pytest.mark.timeout(900)

NAMES = {1: "Zoë ✓", 2: None, 3: "", 4: "   ", 5: "日本語"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_UPLOAD_DIR", str(tmp_path / "uploads"))
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _pg_form(dst: str) -> dict[str, str]:
    cfg = r.pg_cfg()
    return {
        "dest_kind": "database", "dest_format": "postgresql", "dest_host": cfg["host"],
        "dest_port": str(cfg["port"]), "dest_database": cfg["database"],
        "dest_username": cfg["username"], "dest_password": cfg["password"],
        "dest_schema": "public", "dest_table": dst, "async_mode": "false",
        "sync_mode": "full_refresh_overwrite", "validation_mode": "strict",
    }


def _finished(client, response) -> dict:
    assert response.status_code == 200, response.text
    job = response.json()
    job_id = job.get("job_id") or (job.get("job") or {}).get("id")
    if job.get("status") not in {"completed", "failed", "partial"} and job_id:
        job = h.wait_job(str(job_id), timeout_s=300)
    return job


def _status(job: dict) -> str:
    return str(job.get("status") or ("completed" if job.get("success") else "failed"))


# --------------------------------------------------------------------------
# clean.parquet, 500 rows, price 0.5 - 99.9
# --------------------------------------------------------------------------


def _prices() -> list[Decimal | None]:
    out: list[Decimal | None] = []
    for i in range(1, 501):
        # The first 50 rows stay below 10 (what every sample sees); the rest
        # cover the whole 0.5 - 99.9 range.
        value = Decimal(5 + (i % 95)) / 10 if i <= 50 else Decimal(5 + (i * 7 % 995)) / 10
        out.append(None if i in (60, 61) else value)
    return out


def _clean_parquet(price_type: str) -> bytes:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    prices = _prices()
    ids = list(range(1, 501))
    names = [NAMES.get(i, f"n{i}") for i in ids]
    if price_type == "string":
        price_col = pa.array([None if p is None else str(p) for p in prices], type=pa.string())
    elif price_type == "double":
        price_col = pa.array([None if p is None else float(p) for p in prices], type=pa.float64())
    else:
        price_col = pa.array(prices, type=pa.decimal128(4, 1))
    table = pa.table({"id": pa.array(ids, type=pa.int64()),
                      "name": pa.array(names, type=pa.string()), "price": price_col})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


@pytest.mark.parametrize("price_type", ["string", "double", "decimal"])
def test_clean_parquet_into_pg_full_refresh_overwrite(client, price_type):
    dst = f"r11pq_{price_type}_{uuid.uuid4().hex[:8]}"
    content = _clean_parquet(price_type)
    up = client.post("/api/v1/connectors/upload",
                     files={"file": ("clean.parquet", content, "application/octet-stream")})
    assert up.status_code == 200, up.text
    want = [(i, NAMES.get(i, f"n{i}"), p) for i, p in zip(range(1, 501), _prices())]
    try:
        for run in (1, 2):
            job = _finished(client, client.post(
                "/api/v1/transfer/run",
                files={"file": ("clean.parquet", io.BytesIO(content), "application/octet-stream")},
                data={"source_kind": "file", **_pg_form(dst)},
            ))
            assert _status(job) == "completed", (run, job.get("error") or job.get("message"))
            got = r.pg(f'SELECT id, name, price FROM public."{dst}" ORDER BY id')
            assert len(got) == 500, (run, len(got))
            if price_type == "double":
                # A declared double stays a double: the values compare as floats.
                assert [(a, b, None if c is None else Decimal(str(c))) for a, b, c in got] == want
            else:
                assert [(a, b, None if c is None else Decimal(c)) for a, b, c in got] == want
            ptype = r.columns(dst)["price"][0]
            assert ptype != "numeric(2,1)", ptype
    finally:
        h.drop_table("postgresql", dst)


# --------------------------------------------------------------------------
# recipe derive amount*2 over 1,000 rows, amount = id x 1.37
# --------------------------------------------------------------------------


def test_recipe_derive_amount_times_two_is_sized_for_every_row(client):
    from services.shape_models import ShapeRecipe

    token = uuid.uuid4().hex[:8]
    src, dst = f"r11dv_s_{token}", f"r11dv_d_{token}"
    r.pg(f'CREATE TABLE public."{src}" (id integer PRIMARY KEY, name text, amount numeric(10,2))')
    r.pg(
        f'INSERT INTO public."{src}" SELECT g, \'n\' || g, round(g * 1.37, 2) '
        "FROM generate_series(1, 1000) g"
    )
    for i, name in NAMES.items():
        r.pg(f'UPDATE public."{src}" SET name = %s WHERE id = %s', (name, i))
    r.pg(f'UPDATE public."{src}" SET amount = NULL WHERE id = 7')
    recipe = {"steps": [{"op": "derive_column", "options": {"to": "amount2", "expression": "[amount] * 2"}}]}
    source = r.connector("source", r.pg_cfg())
    form = {
        **_pg_form(dst), "source_kind": "database", "source_format": "postgresql",
        "source_connector_id": source, "source_table": src,
        "shape_recipe_json": json.dumps(recipe),
        "approved_shape_recipe_hash": ShapeRecipe.parse(recipe).recipe_hash,
    }
    try:
        for run in (1, 2):
            job = _finished(client, client.post("/api/v1/transfer/run", data=form))
            assert _status(job) == "completed", (run, job.get("error") or job.get("message"))
            got = r.pg(f'SELECT id, name, amount, amount2 FROM public."{dst}" ORDER BY id')
            assert len(got) == 1000
            names = {**NAMES, 8: "ñ"} if run == 2 else NAMES
            for row_id, name, amount, amount2 in got:
                assert name == names.get(row_id, f"n{row_id}")
                assert amount2 == (None if amount is None else amount * 2), (row_id, amount, amount2)
            top = Decimal("2740.00") if run == 1 else Decimal("199999999.98")
            assert max(a for _i, _n, _a, a in got if a is not None) == top
            # DECIMAL(10,2) * 2 needs at most one more integer digit.
            assert r.columns(dst)["amount2"][0] in {"numeric(11,2)", "numeric"}, r.columns(dst)
            r.pg(f'UPDATE public."{src}" SET name = \'ñ\', amount = 99999999.99 WHERE id = 8')
    finally:
        h.drop_table("postgresql", src)
        h.drop_table("postgresql", dst)
