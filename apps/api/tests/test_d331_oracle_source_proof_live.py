"""D331 live: the transform proof over a real Oracle source (CI oracle-live).

QA r61-r75: every shaped run from an Oracle source said "transform proof
unverifiable - N of N keys do not hold the recipe output" over exact data, and
a strict Oracle -> Oracle upsert deleted its correct rows. The proof re-read
the source with a raw ``SELECT``: python-oracledb returns TIMESTAMP WITH TIME
ZONE as a naive wall clock, while the write's reader keeps the instant.

The local reproduction is ``test_d331_oracle_source_proof_reread.py`` (fakes of
the Oracle side). This file is the same scenario on Oracle itself: a seed with
a zone-bearing column at +05:30 and a row that is NULL except its key and
cursor, a recipe, an upsert into PostgreSQL and a strict upsert into Oracle.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import TransferRequest  # noqa: E402

pytestmark = pytest.mark.timeout(900)

_COLS = ["ID", "NOTE", "UPDATED_AT", "SEEN_AT"]


def _oracle_conn():
    from services.brand_env import getenv_brand
    from tests import typed_fidelity_helpers as tf

    tf.require_oracle_env()
    oracledb = pytest.importorskip("oracledb")
    return oracledb.connect(
        user=getenv_brand("ORACLE_USER", "dataflow"),
        password=getenv_brand("ORACLE_PASSWORD", "dataflow"),
        dsn=f"{getenv_brand('ORACLE_HOST', 'localhost')}:"
        f"{getenv_brand('ORACLE_PORT', '1521')}/"
        f"{getenv_brand('ORACLE_SERVICE') or getenv_brand('ORACLE_DATABASE', 'ORCLPDB1')}",
    )


def _owner() -> str:
    from tests import typed_fidelity_helpers as tf

    return str(tf.oracle_endpoint("x").schema or "").upper()


def _seed_oracle(table: str) -> None:
    conn = _oracle_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f'CREATE TABLE "{_owner()}"."{table}" ('  # nosec B608
            "ID NUMBER(10) PRIMARY KEY, NOTE VARCHAR2(40), "
            "UPDATED_AT TIMESTAMP(6) WITH TIME ZONE NOT NULL, "
            "SEEN_AT TIMESTAMP(6) WITH TIME ZONE)"
        )
        for i in range(1, 31):
            note = None if i in (3, 30) else f"n{i}"
            seen = None if i == 30 else f"2026-11-24 08:{i:02d}:07.250000 +05:30"
            cur.execute(
                f'INSERT INTO "{_owner()}"."{table}" VALUES (:1, :2, '  # nosec B608
                "TO_TIMESTAMP_TZ(:3, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'), "
                "TO_TIMESTAMP_TZ(:4, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'))",
                [i, note, f"2026-11-24 10:{i:02d}:00.000000 +00:00", seen],
            )
        conn.commit()
    finally:
        conn.close()


def _drop_oracle(table: str) -> None:
    conn = _oracle_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT table_name FROM all_tables WHERE owner = :o AND UPPER(table_name) = :t",
            o=_owner(),
            t=table.upper(),
        )
        for (stored,) in cur.fetchall():
            cur.execute(f'DROP TABLE "{_owner()}"."{stored}" PURGE')  # nosec B608
    finally:
        conn.close()


def _oracle_count(table: str) -> int:
    conn = _oracle_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT table_name FROM all_tables WHERE owner = :o AND UPPER(table_name) = :t",
            o=_owner(),
            t=table.upper(),
        )
        found = cur.fetchone()
        if not found:
            return 0
        cur.execute(f'SELECT COUNT(*) FROM "{_owner()}"."{found[0]}"')  # nosec B608
        return int(cur.fetchone()[0])
    finally:
        conn.close()


@pytest.fixture()
def fake_jobs(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields, **_k):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    store = _Store()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return store


def _request(source, dest, *, lower_targets: bool) -> TransferRequest:
    from services.column_transform_chain import compile_column_chains, split_column_transforms
    from services.shape_models import ShapeRecipe

    _s, chains = split_column_transforms({"NOTE": ["upper"]})
    recipe, refusal = compile_column_chains(chains, _COLS)
    assert not refusal, refusal
    return TransferRequest(
        source=source,
        destination=dest,
        mappings=[
            {
                "source": c,
                "target": c.lower() if lower_targets else c,
                "approved": True,
                "confidence": 0.99,
            }
            for c in _COLS
        ],
        sync_mode="incremental_upsert",
        validation_mode="strict",
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(recipe).recipe_hash,
        skip_preflight=True,
        stream_contracts=[
            {"name": dest.table, "primary_key": ["ID"], "sync_mode": "incremental_upsert", "selected": True}
        ],
    )


def _run(store, request):
    job_id = "d331l" + uuid.uuid4().hex[:12]
    store.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def test_live_oracle_source_to_postgres_upsert_proof_verifies(fake_jobs):
    from tests import r4_route_harness as h
    from tests import typed_fidelity_helpers as tf

    _oracle_conn().close()
    src = f"D331S_{uuid.uuid4().hex[:8]}".upper()
    dst = f"d331d_{uuid.uuid4().hex[:8]}"
    _seed_oracle(src)
    try:
        result = _run(
            fake_jobs,
            _request(tf.oracle_endpoint(src), tf.pg_endpoint(dst), lower_targets=True),
        )
        proof = (result.reconciliation or {}).get("transform_applied") or {}
        assert "do not hold the recipe output" not in str(proof.get("reason") or ""), proof
        assert proof.get("status") == "verified", proof
        assert result.success, result.error
        conn = h.pg_conn()
        try:
            cur = conn.cursor()
            cur.execute(f'SELECT id, note FROM public."{dst}" ORDER BY id')  # nosec B608
            rows = cur.fetchall()
        finally:
            conn.close()
        assert len(rows) == 30 and rows[0][1] == "N1" and rows[2][1] is None
    finally:
        _drop_oracle(src)
        h.drop_table("postgresql", dst)


def test_live_oracle_to_oracle_strict_upsert_keeps_and_verifies_its_rows(fake_jobs):
    """QA r67-r72: strict Oracle -> Oracle FAILED and left the table empty."""
    from tests import typed_fidelity_helpers as tf

    _oracle_conn().close()
    src = f"D331S_{uuid.uuid4().hex[:8]}".upper()
    dst = f"D331D_{uuid.uuid4().hex[:8]}".upper()
    _seed_oracle(src)
    try:
        result = _run(
            fake_jobs,
            _request(tf.oracle_endpoint(src), tf.oracle_endpoint(dst), lower_targets=False),
        )
        proof = (result.reconciliation or {}).get("transform_applied") or {}
        assert "do not hold the recipe output" not in str(proof.get("reason") or ""), proof
        assert _oracle_count(dst) == 30, "correct rows were removed"
        assert proof.get("status") == "verified", proof
        assert result.success, result.error
    finally:
        _drop_oracle(src)
        _drop_oracle(dst)
