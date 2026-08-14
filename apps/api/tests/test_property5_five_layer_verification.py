"""PROPERTY 5 — five-layer population verification (not sample screening).

Inject a single-cell drift and assert:
  L1 passes (counts balance)
  L2 fails on the drifted column (sum)
  L3 fails (table checksum)
  L4 localizes to that column only
  L5 localizes to the exact PK + source/target values
"""

from __future__ import annotations

import os
import socket
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.verification_ladder import (
    DEFAULT_SCREENING_LIMIT,
    attach_ladder_to_reconcile_report,
    run_five_layer_verification,
)
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _pg_up() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=0.4):
            return True
    except OSError:
        return False


def _pg_creds() -> dict:
    return {
        "host": os.environ.get("P5_PG_HOST", os.environ.get("P2_PG_HOST", "127.0.0.1")),
        "port": int(os.environ.get("P5_PG_PORT", os.environ.get("P2_PG_PORT", "5432"))),
        "database": os.environ.get("P5_PG_DB", os.environ.get("P2_PG_DB", "postgres")),
        "username": os.environ.get("P5_PG_USER", os.environ.get("P2_PG_USER", "postgres")),
        "password": os.environ.get("P5_PG_PASSWORD", os.environ.get("P2_PG_PASSWORD", "admin")),
    }


def _build_population(n: int = 500) -> list[dict]:
    return [{"id": i, "nm": f"r{i}", "amount": i * 10} for i in range(1, n + 1)]


def test_screening_limit_is_not_population_proof_constant():
    assert DEFAULT_SCREENING_LIMIT == 500


def test_five_layer_localizes_injected_cell_drift():
    source = _build_population(800)
    target = [dict(r) for r in source]
    # Inject one silent drift — the exact failure mode L4/L5 must catch.
    bad_pk = 424
    target[bad_pk - 1]["amount"] = 10_000_000

    cols = ["id", "nm", "amount"]
    ladder = run_five_layer_verification(
        source_rows=source,
        target_rows=target,
        columns=cols,
        pk_column="id",
        dest_db_type="sqlite",
        dest_types={"id": "BIGINT", "nm": "TEXT", "amount": "BIGINT"},
        always_localize=True,
    )

    assert ladder["layers"]["L1"]["passed"] is True
    assert ladder["layers"]["L2"]["passed"] is False
    assert "amount" in ladder["layers"]["L2"]["details"]["mismatched_columns"]
    assert ladder["layers"]["L3"]["passed"] is False
    assert ladder["layers"]["L4"]["passed"] is False
    assert ladder["layers"]["L4"]["details"]["mismatched_columns"] == ["amount"]
    assert ladder["layers"]["L5"]["passed"] is False
    mismatches = ladder["layers"]["L5"]["details"]["mismatches"]
    assert len(mismatches) >= 1
    hit = next(m for m in mismatches if str(m.get("pk")) == str(bad_pk))
    assert hit["column"] == "amount"
    assert int(hit["source_value"]) == bad_pk * 10
    assert int(hit["target_value"]) == 10_000_000
    assert "amount" in ladder["localization_summary"]
    assert ladder["population_proof"] is False  # RI not claimed
    assert "screening" in ladder["screening_note"].lower()


def test_five_layer_green_on_identical_populations():
    rows = _build_population(200)
    ladder = run_five_layer_verification(
        source_rows=rows,
        target_rows=[dict(r) for r in rows],
        columns=["id", "nm", "amount"],
        pk_column="id",
        dest_db_type="sqlite",
        always_localize=True,
    )
    assert ladder["passed"] is True
    assert ladder["layers"]["L1"]["passed"] is True
    assert ladder["layers"]["L2"]["passed"] is True
    assert ladder["layers"]["L3"]["passed"] is True
    assert ladder["layers"]["L4"]["passed"] is True
    assert ladder["layers"]["L5"]["passed"] is True
    assert ladder["assurance_level"] == "five_layer"


def test_attach_ladder_enriches_failed_message():
    report = {
        "passed": False,
        "message": "Checksum mismatch (strict): source aaa vs target bbb.",
    }
    ladder = {
        "localization_summary": "L4/L5 localized: row id='424' column 'amount' "
        "source=4240 target=10000000",
        "population_checksum_proof": False,
    }
    out = attach_ladder_to_reconcile_report(report, ladder)
    assert "L4/L5 localized" in out["message"]
    assert out["verification_ladder"] is ladder


def test_a_failed_column_profile_vetoes_a_green_checksum():
    """Checksum match is not full fidelity when L2 names a silently-nulled column."""
    from services.signed_proof_pack import classify_post_write_assurance

    report = {
        "passed": True,
        "source_checksum": "aaa",
        "target_checksum": "aaa",
        "checksum_match": True,
        "phase": "post_write_verified",
        "coverage": "full_checksum",
        "message": "Row fidelity verified",
    }
    ladder = {
        "passed": False,
        "skipped": False,
        "localization_summary": "Engine column profile diverged on amount",
        "assurance_level": "engine_column_profile",
        "engine_profile": True,
    }
    out = attach_ladder_to_reconcile_report(report, ladder)
    assert out["passed"] is False
    assert out["phase"] == "post_write_failed"
    assert out["migration_proven"] is False
    claim = classify_post_write_assurance(out)
    assert claim["migration_proven"] is False
    assert claim["claim_level"] == "failed"


def test_sqlite_transfer_ladder_localizes_post_write_drift(tmp_path: Path):
    """End-to-end: transfer succeeds, then dest cell is corrupted → ladder localizes."""
    src = tmp_path / "p5_src.sqlite"
    dst = tmp_path / "p5_dst.sqlite"
    conn = sqlite3.connect(str(src))
    try:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, nm TEXT, amount INTEGER)")
        conn.executemany(
            "INSERT INTO t VALUES (?, ?, ?)",
            [(i, f"r{i}", i * 10) for i in range(1, 121)],
        )
        conn.commit()
    finally:
        conn.close()

    req = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="t"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="t"
        ),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        skip_preflight=True,
        mappings=[
            {"source": "id", "target": "id", "target_type": "BIGINT", "confidence": 0.99},
            {"source": "nm", "target": "nm", "target_type": "TEXT", "confidence": 0.99},
            {
                "source": "amount",
                "target": "amount",
                "target_type": "BIGINT",
                "confidence": 0.99,
            },
        ],
        stream_contracts=[
            {
                "name": "t",
                "primary_key": "id",
                "sync_mode": "full_refresh_overwrite",
                "selected": True,
            }
        ],
    )
    result = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
    assert result.success, result.error
    ladder = (result.reconciliation or {}).get("verification_ladder") or {}
    # Clean transfer may or may not attach ladder (L3 pass); corruption path below
    # is the localization proof.

    conn = sqlite3.connect(str(dst))
    try:
        conn.execute("UPDATE t SET amount = 999999 WHERE id = 55")
        conn.commit()
    finally:
        conn.close()

    from services.verification_ladder import read_sqlite_rows, run_five_layer_verification

    source_rows = read_sqlite_rows(database=str(src), table="t")
    target_rows = read_sqlite_rows(database=str(dst), table="t")
    ladder = run_five_layer_verification(
        source_rows=source_rows,
        target_rows=target_rows,
        columns=["id", "nm", "amount"],
        pk_column="id",
        dest_db_type="sqlite",
        always_localize=True,
    )
    assert ladder["layers"]["L4"]["details"]["mismatched_columns"] == ["amount"]
    hit = ladder["layers"]["L5"]["details"]["mismatches"][0]
    assert str(hit["pk"]) == "55"
    assert hit["column"] == "amount"
    assert int(hit["source_value"]) == 550
    assert int(hit["target_value"]) == 999999


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not reachable")
def test_pg_five_layer_localizes_injected_drift():
    import psycopg2

    creds = _pg_creds()
    src_table = f"p5_src_{uuid.uuid4().hex[:8]}"
    dst_table = f"p5_dst_{uuid.uuid4().hex[:8]}"
    conn = psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["database"],
        user=creds["username"],
        password=creds["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE public."{src_table}" '
                "(id bigint PRIMARY KEY, nm text, amount bigint)"
            )
            cur.execute(
                f'CREATE TABLE public."{dst_table}" '
                "(id bigint PRIMARY KEY, nm text, amount bigint)"
            )
            for i in range(1, 201):
                cur.execute(
                    f'INSERT INTO public."{src_table}" VALUES (%s, %s, %s)',
                    (i, f"r{i}", i * 10),
                )
                amount = 10_000_000 if i == 77 else i * 10
                cur.execute(
                    f'INSERT INTO public."{dst_table}" VALUES (%s, %s, %s)',
                    (i, f"r{i}", amount),
                )
        conn.commit()

        from services.verification_ladder import (
            read_postgres_rows,
            run_five_layer_verification,
        )

        source_rows = read_postgres_rows(
            host=creds["host"],
            port=creds["port"],
            database=creds["database"],
            username=creds["username"],
            password=creds["password"],
            schema="public",
            table=src_table,
        )
        target_rows = read_postgres_rows(
            host=creds["host"],
            port=creds["port"],
            database=creds["database"],
            username=creds["username"],
            password=creds["password"],
            schema="public",
            table=dst_table,
        )
        ladder = run_five_layer_verification(
            source_rows=source_rows,
            target_rows=target_rows,
            columns=["id", "nm", "amount"],
            pk_column="id",
            dest_db_type="postgresql",
            always_localize=True,
        )
        assert ladder["layers"]["L4"]["details"]["mismatched_columns"] == ["amount"]
        hit = next(
            m
            for m in ladder["layers"]["L5"]["details"]["mismatches"]
            if str(m.get("pk")) == "77"
        )
        assert hit["column"] == "amount"
        assert int(hit["source_value"]) == 770
        assert int(hit["target_value"]) == 10_000_000
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{dst_table}"')
        conn.commit()
        conn.close()


def test_l1_append_uses_dest_before_delta():
    from services.verification_ladder import layer_l1_row_balance

    # 200 written into a table that already held 100 → dest=300.
    ok = layer_l1_row_balance(
        source_rows=200,
        target_rows=300,
        allow_extra_rows=True,
        target_rows_before=100,
    )
    assert ok.passed is True
    assert ok.details["equation"] == "target - target_rows_before == expected"

    short = layer_l1_row_balance(
        source_rows=200,
        target_rows=250,
        allow_extra_rows=True,
        target_rows_before=100,
    )
    assert short.passed is False


def test_ladder_does_not_fail_incomparable_append_hashes():
    """Whole-table hashes after Full Append are not L3 cell proof."""
    from services.reconcile_coverage import WHOLE_TABLE_NOT_COMPARABLE
    from services.verification_ladder import run_five_layer_verification

    source = [{"id": 1, "nm": "a"}, {"id": 2, "nm": "b"}]
    dest = source + [{"id": 9, "nm": "seed"}, {"id": 10, "nm": "seed"}]
    ladder = run_five_layer_verification(
        source_rows=source,
        target_rows=dest,
        columns=["id", "nm"],
        pk_column="id",
        source_row_count=2,
        target_row_count=4,
        source_checksum="aaa",
        target_checksum="bbb",
        allow_extra_rows=True,
        checksum_scope=WHOLE_TABLE_NOT_COMPARABLE,
        target_rows_before=2,
    )
    assert ladder["layers"]["L1"]["passed"] is True
    assert ladder["layers"]["L3"]["details"]["skipped"] is True
    assert ladder["passed"] is True
    assert ladder["population_checksum_proof"] is False
    assert ladder["assurance_level"] == "row_count"


def test_attach_ladder_does_not_veto_dest_before_pass():
    from services.reconcile_coverage import WHOLE_TABLE_NOT_COMPARABLE
    from services.verification_ladder import attach_ladder_to_reconcile_report

    report = {
        "passed": True,
        "message": "Append delta verified (2 row(s) appended: 2 → 4).",
        "phase": "post_write_row_count",
        "coverage": "row_count",
        "assurance_level": "row_count",
        "checksum_scope": WHOLE_TABLE_NOT_COMPARABLE,
        "checksum_match": False,
        "source_checksum": "aaa",
        "target_checksum": "bbb",
        "migration_proven": False,
    }
    ladder = {
        "passed": False,
        "skipped": False,
        "assurance_level": "failed",
        "population_checksum_proof": False,
        "layers": {
            "L1": {"passed": True},
            "L3": {"passed": False},
        },
        "localization_summary": "",
    }
    out = attach_ladder_to_reconcile_report(report, ladder)
    assert out["passed"] is True
    assert out["phase"] == "post_write_row_count"
    assert out["migration_proven"] is not True
