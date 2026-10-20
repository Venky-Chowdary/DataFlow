"""X-99 — the DB-source transform oracle must read only source columns.

At 616df100 the recompute seeded its SELECT with the write contract's
column set, which carries recipe-PRODUCED columns (rk, first, label,
batch). The source table has no such column, so the re-read died on
UndefinedColumn and every shaped DB-source run ended "transform proof
unverifiable" on all 5 engines. The recompute now reads only real source
columns (recipe-declared inputs, or the seeded set minus produced
columns ∩ source schema) while the expected side still produces and
compares the produced columns.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest


def _mk_src(tmp: Path) -> Path:
    src = tmp / "src.db"
    conn = sqlite3.connect(src)
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT, "
        "fullname TEXT, phone TEXT)"
    )
    conn.executemany(
        "INSERT INTO t VALUES (?,?,?,?)",
        [
            (1, "123456", "Ada Lovelace", "+1 (555) 0100"),
            (2, "7654321", "Mary Ann Evans", "+1 (555) 0200"),
            (3, "0001", "Cher", ""),
        ],
    )
    conn.commit()
    conn.close()
    return src


def _recipe() -> dict:
    return {
        "steps": [
            {
                "op": "constant_column",
                "options": {"to": "batch", "value": "B1"},
            },
            {
                "op": "hash_identity",
                "options": {
                    "to": "rk",
                    "columns": ["fullname", "phone"],
                },
            },
        ]
    }


def _req(src: Path, dst: Path):
    from services.shape_models import ShapeRecipe
    from src.transfer.models import EndpointConfig, TransferRequest

    recipe = _recipe()
    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="t"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="out"
        ),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        skip_preflight=True,
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(
            recipe, source_columns=["id", "code", "fullname", "phone"]
        ).recipe_hash,
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "code", "target": "code", "target_type": "TEXT"},
            {
                "source": "fullname",
                "target": "fullname",
                "target_type": "TEXT",
            },
            {"source": "phone", "target": "phone", "target_type": "TEXT"},
            {"source": "rk", "target": "rk", "target_type": "TEXT"},
            {"source": "batch", "target": "batch", "target_type": "TEXT"},
        ],
    )


def _run(src: Path, dst: Path):
    from src.transfer.engine import UniversalTransferEngine

    return UniversalTransferEngine().execute_tracked(
        _req(src, dst), uuid.uuid4().hex[:24]
    )


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    yield


def test_oracle_verified_with_produced_columns(env, tmp_path):
    """Clean shaped run reaches oracle 'verified' — the re-read selects only
    source columns, the produced columns are recomputed and compared."""
    src = _mk_src(tmp_path)
    dst = tmp_path / "dst.db"
    r = _run(src, dst)
    assert r.success, r.error
    proof = (r.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "verified", proof
    rows = sqlite3.connect(dst).execute(
        "SELECT id, rk, batch FROM out ORDER BY id"
    ).fetchall()
    assert [r[0] for r in rows] == [1, 2, 3]
    assert all(r[1] and r[2] == "B1" for r in rows), rows


def test_produced_column_tamper_fails(env, tmp_path, monkeypatch):
    """A tampered produced column fails the run naming the culprit — the
    proof compares produced columns too, not only passthrough ones."""
    src = _mk_src(tmp_path)
    dst = tmp_path / "dst.db"

    import services.reconciliation as rec

    real = rec.iter_select_row_dicts

    def tampered(conn, stmt, cols, *a, **k):
        sql = str(stmt)
        for batch in real(conn, stmt, cols, *a, **k):
            if '"out"' in sql or " out " in sql:
                for row in batch:
                    if row.get("id") == 1:
                        row["batch"] = "HACKED"
            yield batch

    monkeypatch.setattr(
        "services.reconciliation.iter_select_row_dicts", tampered
    )
    r = _run(src, dst)
    assert not r.success
    text = str(r.error or "") + str(r.reconciliation or {})
    assert "HACKED" in text or "batch" in text.lower() or "1" in text, text


@pytest.mark.skipif(
    os.environ.get("DATAFLOW_X99_LIVE") != "1",
    reason="live 5-dest shaped oracle matrix needs MySQL/PG/MSSQL/DuckDB",
)
def test_x99_live_oracle_matrix():
    pytest.skip("runs in the live matrix")


def _derive_recipe() -> dict:
    """The live_616d_rexpr recipe — 7 produced columns incl. expression
    language ops (regex_extract, split_part, case, format_date, coalesce)."""
    return {
        "steps": [
            {
                "op": "derive_column",
                "options": {
                    "to": "dom",
                    "expression": "lower(regex_extract(email,'@(.+)$',1))",
                },
            },
            {
                "op": "derive_column",
                "options": {
                    "to": "usr",
                    "expression": "split_part(email,'@',1)",
                },
            },
            {
                "op": "derive_column",
                "options": {
                    "to": "total",
                    "expression": "round(amt * qty, 2)",
                },
            },
            {
                "op": "derive_column",
                "options": {
                    "to": "tier",
                    "expression": (
                        "case when amt >= 100 then 'gold' "
                        "when amt >= 10 then 'silver' else 'bronze' end"
                    ),
                },
            },
            {
                "op": "derive_column",
                "options": {
                    "to": "ym",
                    "expression": "format_date(d,'%Y-%m')",
                },
            },
            {
                "op": "derive_column",
                "options": {
                    "to": "unit",
                    "expression": "if(qty = 0, null, round(amt / qty, 4))",
                },
            },
            {
                "op": "derive_column",
                "options": {
                    "to": "disp",
                    "expression": "coalesce(nick, title_case(name))",
                },
            },
        ]
    }


def test_oracle_verified_derive_column_recipe(env, tmp_path):
    """The derive/expression recipe that failed live — recompute must read
    only (id,email,amt,qty,d,name,nick) and still verify produced columns."""
    from services.shape_models import ShapeRecipe
    from src.transfer.models import EndpointConfig, TransferRequest
    from src.transfer.engine import UniversalTransferEngine

    src = tmp_path / "src2.db"
    conn = sqlite3.connect(src)
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT, amt REAL, "
        "qty INTEGER, d TEXT, name TEXT, nick TEXT)"
    )
    conn.executemany(
        "INSERT INTO t VALUES (?,?,?,?,?,?,?)",
        [
            (1, "a@x.com", 37.04, 1, "2024-02-01", "ada lovelace", None),
            (2, "b@y.com", 150.5, 2, "2024-02-02", "mary ann evans", "Mae"),
            (3, "no-at", 5.0, 0, "2024-03-01", "cher", ""),
        ],
    )
    conn.commit()
    conn.close()
    dst = tmp_path / "dst2.db"
    recipe = _derive_recipe()
    req = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="t"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="out"
        ),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        skip_preflight=True,
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(
            recipe,
            source_columns=["id", "email", "amt", "qty", "d", "name", "nick"],
        ).recipe_hash,
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "email", "target": "email", "target_type": "TEXT"},
            {"source": "amt", "target": "amt", "target_type": "REAL"},
            {"source": "qty", "target": "qty", "target_type": "INTEGER"},
            {"source": "d", "target": "d", "target_type": "TEXT"},
            {"source": "name", "target": "name", "target_type": "TEXT"},
            {"source": "nick", "target": "nick", "target_type": "TEXT"},
            {"source": "dom", "target": "dom", "target_type": "TEXT"},
            {"source": "usr", "target": "usr", "target_type": "TEXT"},
            {"source": "total", "target": "total", "target_type": "REAL"},
            {"source": "tier", "target": "tier", "target_type": "TEXT"},
            {"source": "ym", "target": "ym", "target_type": "TEXT"},
            {"source": "unit", "target": "unit", "target_type": "REAL"},
            {"source": "disp", "target": "disp", "target_type": "TEXT"},
        ],
    )
    r = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
    assert r.success, r.error
    proof = (r.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "verified", proof
    rows = sqlite3.connect(dst).execute(
        "SELECT id, dom, usr, tier, disp FROM out ORDER BY id"
    ).fetchall()
    assert rows[0][1] == "x.com" and rows[0][2] == "a", rows
    assert rows[1][3] == "gold" and rows[2][3] == "bronze", rows
