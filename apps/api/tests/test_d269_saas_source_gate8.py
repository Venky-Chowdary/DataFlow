"""D269 / D276 — Stripe (SaaS) transfers verify against the API source, not a SQL dialect.

Prod repro (Stripe ``customers`` -> PG / MSSQL / Mongo / SQLite,
full_refresh_overwrite, ``column_transforms created=[datetime, assume_timezone:UTC]``):
every job FAILED "Row count mismatch: source 29, rejected 1 ... expected target
28 vs target 29 (transform proof unverifiable — expected-output recompute
failed: SQLAlchemy dialect/driver for 'stripe' is not available)", and MSSQL
added "column metadata_r10 source=None target='22'". The data was correct.

Root causes, each covered below on a real engine run (mocked Stripe HTTP,
SQLite destination file):

1. the transform proof re-read every non-Mongo source through SQLAlchemy;
2. the writer stamps ``source_row_count`` as the rows it *accepted*, so the
   quarantined row was subtracted twice;
3. SQL writers store ``metadata.r10`` as ``metadata_r10``: the writer sample,
   the read-back digest and the proof's destination SELECT each crossed the
   two spellings (SQLite answers an unknown double-quoted column with its
   literal text);
4. a nested key one record lacks was filled with '' instead of NULL, so every
   such record failed an INTEGER carrier and was quarantined.

D276: the "Data kept at <table>" note names the SQLite file and host, so the
claim is checkable where it is true.
"""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from typing import Any
from unittest import mock

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.test_property2_golden_path_never_blocked import _FakeMongo  # noqa: E402

NAMES = ["Alice", "Zoë", "東京 太郎", "محمد", "🚀 Rocket", "", "  spaced  ", None, "Bob"] + [
    f"cust {i}" for i in range(21)
]
COLS = ["id", "object", "name", "email", "created", "metadata.r10"]


def _customers(metadata) -> list[dict[str, Any]]:
    out = []
    for i, name in enumerate(NAMES, 1):
        rec = {
            "id": f"cus_{i:03d}", "object": "customer", "name": name,
            "email": None if i % 7 == 0 else f"u{i}@x.io", "created": 1730620800 + i,
        }
        meta = metadata(i)
        if meta is not None:
            rec["metadata"] = meta
        out.append(rec)
    return out


def _run(tmp_path, data, *, recipe: bool = True, validation_mode: str = "strict",
         mappings: list[dict] | None = None, table: str = "saas11_st_customers"):
    from services.column_transform_chain import compile_column_chains

    payload = {"data": data, "has_more": False}

    def _respond(method, url, **kw):
        m = mock.MagicMock()
        m.status_code, m.headers = 200, {}
        m.json.return_value = payload
        m.text = json.dumps(payload)
        m.content = m.text.encode()
        return m

    shape = {}
    if recipe:
        source_cols = sorted({k for rec in data for k in rec})
        shape, refusal = compile_column_chains({"created": ["datetime", "assume_timezone:UTC"]}, source_cols)
        assert not refusal, refusal
    fake = _FakeMongo()
    db = str(tmp_path / "QA_E2E_sqlite_dst.db")
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="stripe", host="api.stripe.com", api_key="sk_test_x", table="customers"),
        destination=EndpointConfig(kind="database", format="sqlite", database=db, table=table),
        sync_mode="full_refresh_overwrite", validation_mode=validation_mode,
        shape_recipe=shape, mappings=mappings or [], skip_preflight=True,
    )
    with mock.patch.object(engine_mod, "get_mongodb_service", lambda: fake), \
            mock.patch("requests.request", side_effect=_respond), \
            mock.patch("requests.Session.request", side_effect=_respond):
        res = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
    conn = sqlite3.connect(db)
    try:
        rows = conn.execute(f'SELECT * FROM "{table}" ORDER BY id').fetchall()
        cols = [c[1] for c in conn.execute(f'PRAGMA table_info("{table}")')]
    finally:
        conn.close()
    return res, cols, rows


def test_dotted_metadata_with_recipe_completes_and_proof_reads_the_api(tmp_path):
    data = _customers(lambda i: {"r10": "22" if i == 5 else "r10"})
    res, cols, rows = _run(tmp_path, data)
    assert res.success, res.error
    assert "dialect" not in (res.error or "")
    rec = res.reconciliation or {}
    assert rec.get("passed") is True and rec.get("source_rows") == 30 and rec.get("target_rows") == 30
    proof = rec.get("transform_applied") or {}
    assert proof.get("status") == "verified", proof
    assert len(rows) == 30
    by_id = {r[0]: dict(zip(cols, r)) for r in rows}
    assert by_id["cus_005"]["metadata_r10"] == "22"
    # NULL, '', whitespace and unicode kept exactly.
    assert by_id["cus_008"]["name"] is None and by_id["cus_006"]["name"] == ""
    assert by_id["cus_007"]["name"] == "  spaced  " and by_id["cus_003"]["name"] == "東京 太郎"
    assert by_id["cus_007"]["email"] is None
    assert by_id["cus_001"]["created"].startswith("2024-11-03T08:00:01")
    # Second run into the table DataFlow created.
    res2, _, rows2 = _run(tmp_path, data)
    assert res2.success, res2.error
    assert len(rows2) == 30


def test_writer_quarantine_is_subtracted_once(tmp_path):
    data = _customers(lambda i: {"r10": "abc" if i == 4 else str(i)})
    maps = [{"source": c, "target": c, **({"target_type": "INTEGER"} if c == "metadata.r10" else {})} for c in COLS]
    res, cols, rows = _run(tmp_path, data, recipe=False, validation_mode="lenient", mappings=maps)
    assert res.success, res.error
    rec = res.reconciliation or {}
    assert (rec.get("source_rows"), rec.get("target_rows")) == (30, 29), rec
    assert "Row count mismatch" not in (res.error or "")
    assert len(rows) == 29 and "cus_004" not in {r[0] for r in rows}


def test_sparse_nested_key_lands_as_null_not_quarantined(tmp_path):
    data = _customers(lambda i: {"r10": "22"} if i == 5 else {})
    res, cols, rows = _run(tmp_path, data, validation_mode="lenient")
    assert res.success, res.error
    assert len(rows) == 30
    meta = {r[0]: dict(zip(cols, r))["metadata_r10"] for r in rows}
    assert str(meta["cus_005"]) == "22"
    assert all(v is None for k, v in meta.items() if k != "cus_005")


def test_rows_and_schema_from_saas_marks_absent_key_null():
    from connectors.saas_typed_schema import rows_and_schema_from_saas
    from services.value_serializer import SQL_NULL_SENTINEL

    keys, rows, _ = rows_and_schema_from_saas(
        "stripe", [{"id": "a", "metadata": {"r10": "1"}}, {"id": "b", "metadata": {}}]
    )
    assert rows[1][keys.index("metadata.r10")] == SQL_NULL_SENTINEL


def test_readback_projection_resolves_writer_spelling():
    from services.readback_projection import project_readback

    cols, rows = project_readback(["id", "metadata_r10"], ["id", "metadata.r10"], [("a", "x")])
    assert cols == ["id", "metadata.r10"] and list(rows) == [("a", "x")]
    cols, rows = project_readback(["id", "metadata_r10"], ["id", "metadata.r10"], [{"id": "a", "metadata_r10": "x"}])
    assert list(rows) == [{"id": "a", "metadata.r10": "x"}]
    # A column truly absent still keeps the full row (mismatch stays visible).
    cols, _ = project_readback(["id", "other"], ["id", "metadata.r10"], [("a", "x")])
    assert cols == ["id", "other"]


def test_writer_sample_rekeyed_to_map_spelling():
    from src.transfer.writer_column_spelling import physical_select_list, rekey_writer_sample

    out = rekey_writer_sample([{"id": "a", "metadata_r10": "22"}], [{"source": "metadata.r10", "target": "metadata.r10"}])
    assert out[0]["metadata.r10"] == "22"
    assert physical_select_list(["id", "metadata.r10"], '"') == 'id, metadata_r10 AS "metadata.r10"' or \
        physical_select_list(["id", "metadata.r10"], '"') == '"id", "metadata_r10" AS "metadata.r10"'


def test_api_sources_are_not_read_through_sqlalchemy():
    from src.transfer.proof_api_source import is_api_source

    assert is_api_source("stripe") and is_api_source("hubspot")
    assert not is_api_source("postgresql") and not is_api_source("sqlite")


def test_d276_kept_data_note_names_file_and_host(tmp_path):
    import socket

    from services.batch_undo import keep_failed_validation_for_inspection

    db = tmp_path / "QA_E2E_sqlite_dst.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE saas11_st_customers (id TEXT)")
    conn.executemany("INSERT INTO saas11_st_customers VALUES (?)", [(str(i),) for i in range(31)])
    conn.commit()
    conn.close()
    dest = EndpointConfig(kind="database", format="sqlite", database=str(db), table="saas11_st_customers")
    summary = {"table": "saas11_st_customers", "dest_table_existed_before": False, "dest_rows_before": 0}
    from services.dest_precount import PRECOUNT_KEY

    summary[PRECOUNT_KEY] = 0
    note = keep_failed_validation_for_inspection(destination=dest, dest_summary=summary, sync_mode="full_refresh_overwrite")
    assert "Data kept at saas11_st_customers (31 row(s))" in note
    assert str(db) in note and socket.gethostname() in note
    kept = summary["validation_failed_data_kept"]
    assert kept["rows"] == 31 and kept["file"] == str(db)
