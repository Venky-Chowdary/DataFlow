"""D267 — a signed QUARANTINE_ROW holdout is honoured at write time.

QA: Neo4j -> new PostgreSQL, full_refresh_overwrite strict, row 30 empty. The
plan signed QUARANTINE_ROW on the column that needed a contract; the other
empty cells of row 30 sat on unsigned columns. The writer judged those under
the strict job posture and failed the job "abort policy blocks partial write
(FAIL_JOB)" with 0 rows, although the signed contract already held the whole
row out. Expected: 29 rows written, row 30 quarantined, completed_with_quarantine.

A row whose failing cells are all unsigned still refuses under strict, and that
refusal no longer blames a Risk Contract.
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
from tests.test_property2_golden_path_never_blocked import _FakeMongo  # noqa: E402


class _Mongo(_FakeMongo):
    def update_job_fields(self, job_id, fields):
        self.jobs.setdefault(job_id, {}).update(fields or {})
        return True


@pytest.fixture()
def mongo(monkeypatch):
    fake = _Mongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


def _seed(path: Path, *, bad_amount: str = "") -> None:
    con = sqlite3.connect(str(path))
    con.execute("CREATE TABLE seed (id INTEGER PRIMARY KEY, name TEXT, amount TEXT, flag TEXT, d TEXT)")
    rows = [
        (i, n, f"{i}.5", "true" if i % 2 else "false", f"2024-01-{(i % 28) + 1:02d}")
        for i, n in zip(range(1, 30), ["", "  ", "東京 ü", None] + [f"n{i}" for i in range(5, 30)])
    ]
    rows.append((30, "n30", bad_amount, "", ""))
    con.executemany("INSERT INTO seed VALUES (?,?,?,?,?)", rows)
    con.commit()
    con.close()


def _contract(column: str, src_t: str, dst_t: str) -> dict:
    from services.migration_risk_contract import create_migration_risk_contract

    return create_migration_risk_contract(
        column=column,
        source_type=src_t,
        destination_type=dst_t,
        approved_by="qa@dataflow.app",
        reason="hold bad rows out",
        execution_policy="QUARANTINE_ROW",
        target=column,
        quarantine_policy="holdout_rejected_rows",
    ).to_dict()


def _mappings(*, sign: str) -> list[dict]:
    maps = [
        {"source": "id", "target": "id", "target_type": "INTEGER", "source_type": "INTEGER"},
        {"source": "name", "target": "name", "target_type": "TEXT", "source_type": "TEXT"},
        {"source": "amount", "target": "amount", "target_type": "NUMERIC(12,2)", "source_type": "TEXT"},
        {"source": "flag", "target": "flag", "target_type": "BOOLEAN", "source_type": "TEXT"},
        {"source": "d", "target": "d", "target_type": "DATE", "source_type": "TEXT"},
    ]
    for m in maps:
        if m["source"] == sign:
            m["risk_contract"] = _contract(sign, m["source_type"], m["target_type"])
    return maps


def _run(src: Path, dest: EndpointConfig, mongo, *, sign: str):
    job_id = uuid.uuid4().hex[:24]
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="seed"),
        destination=dest,
        mappings=_mappings(sign=sign),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
    )
    result = UniversalTransferEngine().execute_tracked(req, job_id)
    return result, mongo.jobs.get(job_id, {})


def _assert_holdout(result, job, failing=("amount", "flag", "d"), *, signed: str) -> None:
    assert result.success is True, result.error
    assert result.records_transferred == 29
    assert job.get("status") == "completed_with_quarantine"
    details = job.get("rejected_details") or []
    assert {d.get("column") for d in details} == set(failing)
    assert {d.get("row") for d in details} == {30}
    assert all(d.get("policy") == "quarantine" for d in details)
    by_col = {d["column"]: d for d in details}
    signed_finding = by_col[signed]
    assert signed_finding.get("execution_policy") == "QUARANTINE_ROW"
    assert signed_finding.get("risk_id")
    # Honest attribution: the contract covers its own column only. The other
    # cells rode out with the row and say so, without borrowing the contract.
    for col, d in by_col.items():
        if col == signed:
            continue
        assert not d.get("execution_policy"), d
        assert not d.get("risk_id"), d
        assert d.get("held_with_row") is True
        assert d.get("held_by_policy") == "QUARANTINE_ROW"
        assert d.get("held_by_risk_id") == signed_finding["risk_id"]
        assert d.get("held_by_column") == signed


def test_signed_holdout_covers_the_unsigned_cells_of_the_same_row(tmp_path, mongo):
    src = tmp_path / "src.db"
    dst = tmp_path / "dst.db"
    # SQLite's NUMERIC carrier takes any text, so the signed column here is
    # the BOOLEAN one; DATE stays unsigned and fails on the same row.
    _seed(src, bad_amount="7.5")
    dest = EndpointConfig(kind="database", format="sqlite", database=str(dst), table="r9_neo")
    for _run_no in (1, 2):  # second run into the table DataFlow created
        result, job = _run(src, dest, mongo, sign="flag")
        _assert_holdout(result, job, failing=("flag", "d"), signed="flag")
        con = sqlite3.connect(str(dst))
        rows = con.execute("SELECT id, name FROM r9_neo ORDER BY id").fetchall()
        con.close()
        assert [r[0] for r in rows] == list(range(1, 30))
        assert rows[0][1] == "" and rows[1][1] == "  " and rows[2][1] == "東京 ü" and rows[3][1] is None


def test_unsigned_failures_alone_still_refuse_and_do_not_blame_a_contract(tmp_path, mongo):
    src = tmp_path / "src.db"
    dst = tmp_path / "dst.db"
    _seed(src, bad_amount="7.5")  # only the unsigned flag/d cells fail on row 30
    dest = EndpointConfig(kind="database", format="sqlite", database=str(dst), table="r9_neo")
    result, _job = _run(src, dest, mongo, sign="amount")
    assert result.success is False
    err = result.error or ""
    assert "strict error policy" in err
    assert "Risk Contract abort policy" not in err and "(FAIL_JOB)" not in err


def _pg_endpoint(table: str) -> EndpointConfig | None:
    try:
        import psycopg2

        psycopg2.connect(
            host="localhost", port=5432, database="dataflow", user="dataflow",
            password="dataflow", connect_timeout=3,
        ).close()
    except Exception:
        return None
    return EndpointConfig(
        kind="database", format="postgresql", host="localhost", port=5432, database="dataflow",
        username="dataflow", password="dataflow", schema="public", table=table,
    )


def test_signed_holdout_into_new_postgres_table(tmp_path, mongo):
    table = f"r13_d267_{uuid.uuid4().hex[:8]}"
    dest = _pg_endpoint(table)
    if dest is None:
        pytest.skip("PostgreSQL unavailable")
    import psycopg2

    src = tmp_path / "src.db"
    _seed(src)
    try:
        result, job = _run(src, dest, mongo, sign="amount")
        _assert_holdout(result, job, signed="amount")
        con = psycopg2.connect(host="localhost", port=5432, database="dataflow", user="dataflow", password="dataflow")
        with con.cursor() as cur:
            cur.execute(f'SELECT count(*), count(*) FILTER (WHERE id = 30) FROM public."{table}"')
            assert cur.fetchone() == (29, 0)
        con.close()
    finally:
        con = psycopg2.connect(host="localhost", port=5432, database="dataflow", user="dataflow", password="dataflow")
        con.autocommit = True
        with con.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{table}_df_quarantine"')
        con.close()


def test_a_signed_fail_closed_cast_contract_is_named_as_the_contract_abort():
    """CAST_AND_CONTINUE with quarantine_policy FAIL resolves to ``fail``; the
    refusal must name the contract, not claim the column is unsigned."""
    from connectors.writer_common import reject_on_strict_policy

    details = [{"row": 3, "column": "amount", "policy": "fail", "reason": "bad cast",
                "execution_policy": "CAST_AND_CONTINUE", "risk_id": "mrc-1"}]
    msg = reject_on_strict_policy("fail", details, "PostgreSQL") or ""
    assert "Migration Risk Contract abort policy" in msg
    assert "no signed Risk Contract covers" not in msg


def test_held_with_row_findings_are_continue_not_abort():
    from services.migration_risk_contract import (
        rejected_details_are_continue_contract_only,
        rejected_details_require_job_abort,
    )

    details = [
        {"row": 30, "column": "flag", "policy": "quarantine", "execution_policy": "QUARANTINE_ROW", "risk_id": "mrc-9"},
        {"row": 30, "column": "d", "policy": "quarantine", "held_with_row": True,
         "held_by_policy": "QUARANTINE_ROW", "held_by_risk_id": "mrc-9"},
    ]
    assert rejected_details_require_job_abort(details) is False
    assert rejected_details_are_continue_contract_only(details) is True
