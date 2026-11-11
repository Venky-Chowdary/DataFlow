"""D333 — a query-mode source's transform proof re-runs the query, not the base table.

QA: SQL Server ``source_query`` ``SELECT id, name AS customer_name, …, qty*2 AS
qty_x2 … FROM dbo.q35_src WHERE …`` with ``{"customer_name": ["trim", "upper"],
"note": "default:NA", "payload": "redact"}``. The data landed exactly (25/25),
but the proof re-read ``dbo.q35_src`` selecting the query's output names and
reported "(transform proof unverifiable — expected-output recompute failed:
(pymssql…) Invalid column name 'customer_name' …)". On SQLite the same base-table
read silently selected the string literal 'customer_name'.

The proof now re-reads a query / procedure extract through the engine's own
reader for it (``read_callable_batch``: the statement the write ran), and the
SQL oracle, which recomputes on the base table, stays off for it.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

pytestmark = pytest.mark.timeout(600)

_ROWS = [
    (1, " ada ", 1.5, 60, "", "p1"),
    (2, "bob", 2.0, None, None, "p2"),
    (7, "x", 1.0, 100, "n", "p7"),  # filtered out by the query
    (8, "Zoë", 3.25, 10, "n8", None),  # filtered out by the query
    (9, " eve", 4.0, 55, "z", None),
]
_QUERY = (
    "SELECT id, name AS customer_name, amount, qty, qty*2 AS qty_x2, note, payload "
    "FROM {table} WHERE id <> 7 AND (qty IS NULL OR qty >= 50)"
)
_COLS = ["id", "customer_name", "amount", "qty", "qty_x2", "note", "payload"]
_TRANSFORMS = {"customer_name": ["trim", "upper"], "note": "default:NA", "payload": "redact"}


def _request(source: EndpointConfig, dest: EndpointConfig) -> TransferRequest:
    from services.column_transform_chain import compile_column_chains, split_column_transforms
    from services.shape_models import ShapeRecipe

    singles, chains = split_column_transforms(_TRANSFORMS)
    recipe, refusal = compile_column_chains(chains, _COLS)
    assert not refusal, refusal
    mappings = []
    for c in _COLS:
        m = {"source": c, "target": c, "approved": True, "confidence": 0.99}
        if c in singles:
            m.update(transform=singles[c], transform_override=True)
        mappings.append(m)
    return TransferRequest(
        source=source,
        destination=dest,
        mappings=mappings,
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(recipe).recipe_hash,
        skip_preflight=True,
    )


@pytest.fixture()
def fake_jobs(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(fake, request: TransferRequest):
    job_id = "d333" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _assert_proven(result) -> None:
    assert result.success, result.error
    proof = (result.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "verified", proof
    assert proof.get("rows") == 3, proof
    message = str((result.reconciliation or {}).get("message") or "")
    for leaked in ("Invalid column name", "pymssql", "pyodbc", "recompute failed"):
        assert leaked not in message, message


def test_sqlite_query_with_aliases_is_proven_against_the_query(tmp_path, fake_jobs):
    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute(
            "CREATE TABLE q35_src (id INTEGER PRIMARY KEY, name TEXT, amount NUMERIC, "
            "qty INTEGER, note TEXT, payload TEXT)"
        )
        conn.executemany("INSERT INTO q35_src VALUES (?, ?, ?, ?, ?, ?)", _ROWS)
    conn.close()
    source = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(src),
        table="q35_src",
        extra={"source_read_mode": "query", "source_query": _QUERY.format(table="q35_src")},
    )
    result = _run(
        fake_jobs,
        _request(source, EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t")),
    )
    _assert_proven(result)
    got = sqlite3.connect(str(dst)).execute(
        "SELECT id, customer_name, qty_x2, note FROM t ORDER BY id"
    ).fetchall()
    assert got == [(1, "ADA", 120, "NA"), (2, "BOB", None, "NA"), (9, "EVE", 110, "z")]


def test_sqlserver_query_with_aliases_into_postgres_is_proven(fake_jobs):
    """QA's pair: SQL Server query mode → PostgreSQL."""
    from tests import r4_route_harness as h
    from tests.typed_fidelity_helpers import pg_endpoint, sqlserver_endpoint

    h.dest_config("postgresql")  # skips when PostgreSQL is absent
    table = f"d333_src_{uuid.uuid4().hex[:8]}"
    dst = f"d333_dst_{uuid.uuid4().hex[:8]}"
    conn = h.mssql_conn()  # skips when SQL Server is absent
    try:
        cur = conn.cursor()
        cur.execute(
            f"CREATE TABLE dbo.[{table}] (id int PRIMARY KEY, name nvarchar(40), "  # nosec B608
            "amount decimal(12,2), qty int, note nvarchar(40), payload nvarchar(40))"
        )
        cur.executemany(f"INSERT INTO dbo.[{table}] VALUES (?, ?, ?, ?, ?, ?)", _ROWS)  # nosec B608
    finally:
        conn.close()
    source = sqlserver_endpoint(table)
    source.extra = {
        **(source.extra or {}),
        "source_read_mode": "query",
        "source_query": _QUERY.format(table=f"dbo.[{table}]"),
    }
    try:
        result = _run(fake_jobs, _request(source, pg_endpoint(dst)))
        _assert_proven(result)
    finally:
        h.drop_table("sqlserver", table)
        h.drop_table("postgresql", dst)
