"""D31 — a create-new DECIMAL is sized from the full scan, not the 100-row peek.

300 rows of ``1.25`` followed by ``123456789.12345``: the peek saw scale 2, so
the CREATE was ``DECIMAL(38,2)`` and the population-fit gate then blocked the
load asking the operator to approve DECIMAL(38,5) for a type nobody chose. The
fit scan already proved the wider carrier over the same rows, but its widen
needed a replayable population (only DB-table sources had one) and stamped the
preflight's private copy of the mappings, so the CREATE never saw it.

Rule: DataFlow resizes only a type DataFlow inferred, for a table it creates.
An operator-locked mapping (``user_override``) keeps block-and-approve.
"""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

import pytest

from src.transfer.models import EndpointConfig
from tests.test_cr_d125_file_quoted_empty_fidelity import (
    assert_verified,
    file_source,
    mysql_dest,
    pg_dest,
    put_s3_object,
    run_transfer,
    s3_source,
)

WIDE = "123456789.12345"


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc
    import src.transfer.engine as engine_mod

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)


def _body(small: int = 300) -> str:
    lines = ["id,amt"] + [f"{i},1.25" for i in range(small)]
    lines += [f"{small + i},{WIDE}" for i in range(5)]
    return "\n".join(lines) + "\n"


def _destination(dest: str, tmp_path: Path, table: str) -> EndpointConfig:
    if dest == "sqlite":
        return EndpointConfig(
            kind="database", format="sqlite", database=str(tmp_path / "dst.db"), table="t"
        )
    return pg_dest(table) if dest == "postgresql" else mysql_dest(table)


def _landed(dest: str, tmp_path: Path, table: str) -> tuple[int, str, str]:
    """(row count, max amt as text, the amt column's DDL type)."""
    if dest == "sqlite":
        conn = sqlite3.connect(str(tmp_path / "dst.db"))
        try:
            rows = conn.execute("SELECT amt FROM t").fetchall()
            ddl = dict((r[1], r[2]) for r in conn.execute("PRAGMA table_info(t)"))
        finally:
            conn.close()
        return len(rows), max(rows, key=lambda r: float(r[0]))[0], ddl["amt"]
    if dest == "postgresql":
        import psycopg2

        conn = psycopg2.connect(
            host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*), MAX(amt)::text FROM public."{table}"')
                count, top = cur.fetchone()
                cur.execute(
                    "SELECT data_type, numeric_precision, numeric_scale FROM "
                    "information_schema.columns WHERE table_name=%s AND column_name='amt'",
                    (table,),
                )
                ddl = "/".join(str(x) for x in cur.fetchone())
        finally:
            conn.rollback()
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
            conn.close()
        return int(count), top, ddl
    import pymysql

    conn = pymysql.connect(
        host="127.0.0.1", port=3306, user="dataflow", password="dataflow",
        database="dataflow", autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*), MAX(amt) FROM `{table}`")
            count, top = cur.fetchone()
            cur.execute(
                "SELECT column_type FROM information_schema.columns "
                "WHERE table_schema='dataflow' AND table_name=%s AND column_name='amt'",
                (table,),
            )
            ddl = cur.fetchone()[0]
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        conn.close()
    return int(count), str(top), ddl


@pytest.mark.parametrize("source", ["file", "s3"])
@pytest.mark.parametrize("dest", ["sqlite", "postgresql", "mysql"])
def test_create_new_decimal_is_sized_from_the_full_scan(tmp_path, dest, source):
    import moto

    table = f"d31_{uuid.uuid4().hex[:8]}"
    destination = _destination(dest, tmp_path, table)
    if source == "file":
        result = run_transfer(destination=destination, **file_source("t.csv", _body()))
    else:
        bucket = f"d31-{uuid.uuid4().hex[:8]}"
        with moto.mock_aws():
            put_s3_object(bucket, "in/t.csv", _body())
            result = run_transfer(source=s3_source(bucket, "in/t.csv"), destination=destination)
    count, top, ddl = _landed(dest, tmp_path, table)
    assert_verified(result)
    assert count == 305
    assert top.rstrip("0") == WIDE
    if dest == "mysql":
        assert ddl == "decimal(38,5)", ddl


def test_the_plan_says_the_type_was_sized_from_the_full_scan(tmp_path):
    from services.population_fit_scan import build_population_fit_gate, scan_population_fit

    rows = [{"amt": "1.25"}] * 300 + [{"amt": WIDE}] * 5
    mapping = [{"source": "amt", "target": "amt", "target_type": "DECIMAL(38,2)"}]
    kwargs = dict(
        dest_types={}, source_types={}, dest_db="mysql", dialect_label="mysql",
        job_error_policy="fail", rows_are_population=True, source_kind="file",
        source_format="csv", sync_mode="full_refresh_overwrite", dest_table_exists=False,
    )
    from services.population_fit_scan import create_new_population_widen

    report = scan_population_fit(iter(rows), mapping, rows_total=len(rows), **kwargs)
    widen = create_new_population_widen(
        report, mapping, lambda: iter(rows), scan_kwargs=dict(rows_total=len(rows), **kwargs)
    )
    assert widen is not None
    gate = build_population_fit_gate(widen.report)
    assert gate["status"] == "pass"
    assert "CREATE type sized from the full scan: amt DECIMAL(38,2) → DECIMAL(38,5)" in gate["message"]


def test_an_operator_declared_type_keeps_block_and_approve():
    from services.population_fit_scan import (
        build_population_fit_gate,
        create_new_population_widen,
        scan_population_fit,
    )

    rows = [{"amt": "1.25"}] * 300 + [{"amt": WIDE}] * 5
    mapping = [
        {
            "source": "amt", "target": "amt", "target_type": "DECIMAL(38,2)",
            "user_override": True,
        }
    ]
    kwargs = dict(
        dest_types={}, source_types={}, dest_db="mysql", dialect_label="mysql",
        job_error_policy="fail", rows_are_population=True, source_kind="file",
        source_format="csv", sync_mode="full_refresh_overwrite", dest_table_exists=False,
    )
    report = scan_population_fit(iter(rows), mapping, rows_total=len(rows), **kwargs)
    assert create_new_population_widen(
        report, mapping, lambda: iter(rows), scan_kwargs=dict(rows_total=len(rows), **kwargs)
    ) is None
    assert mapping[0]["target_type"] == "DECIMAL(38,2)"
    assert build_population_fit_gate(report)["status"] == "block"
