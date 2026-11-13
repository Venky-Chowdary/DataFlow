"""G7-F2 review M: a shaped job's transform proof reverses the formula prefix.

New Studio file_export jobs write ``formula_protection: "prefix"``. Gate-8's
cell read-back (``_file_export_cell_readback``) restores the reversible ``'``,
but the transform proof's file read-back (``_proof_file_dest_batches``) did
not: a recipe whose output starts with ``+`` / ``=`` (``'+1 555 0101'`` after
trim) compared ``'+1 555 0101`` to ``+1 555 0101`` and the proof went
"unverifiable". It reads the exact values now, as Gate-8 does.
"""

from __future__ import annotations

import uuid

import pytest

from src.transfer.models import EndpointConfig
from tests.test_x5_same_run_artifact_readback import COLS, PG, _assert_clean, _local, _run


@pytest.fixture
def pg_formula_source():
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(
            host=PG["host"], port=int(PG["port"]), user=PG["username"],
            password=PG["password"], dbname=PG["database"], connect_timeout=3,
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    table = "g7f2tp_" + uuid.uuid4().hex[:8]
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT, amt TEXT, note TEXT, "  # nosec B608
        "flag BOOLEAN, ts TIMESTAMPTZ, big NUMERIC(38,0))"
    )
    rows = [
        (1, "  +1 555 0101 ", "10.50", "=sum(a1)", True, "2026-10-01 12:00:00+00", 10**30 + 1),
        (2, " @handle ", "-2.25", "-2", False, "2026-10-02 12:00:00+00", 10**30 + 2),
        (3, " plain ", "3.25", "note 3", True, "2026-10-03 12:00:00+00", 10**30 + 3),
    ]
    for r in rows:
        cur.execute(f"INSERT INTO {table} VALUES (%s,%s,%s,%s,%s,%s,%s)", r)  # nosec B608
    yield EndpointConfig(
        kind="database", format="postgresql", host=PG["host"], port=int(PG["port"]),
        username=PG["username"], password=PG["password"], database=PG["database"], table=table,
    )
    cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
    conn.close()


@pytest.mark.parametrize("protection", ["off", "prefix"])
def test_shaped_csv_file_export_transform_proof_verified(protection, tmp_path, monkeypatch, pg_formula_source, request):
    dest, artifact = _local(tmp_path, monkeypatch, "csv", request)
    dest.extra = {**(dest.extra or {}), "formula_protection": protection}
    result, seen = _run(monkeypatch, pg_formula_source, dest, artifact, tamper=False)
    _assert_clean(result, seen)
    body = seen["final"].decode("utf-8")
    if protection == "prefix":
        assert "'+1 555 0101" in body and "'=SUM(A1)" in body and "'@handle" in body
        assert ",-2," in body or ",-2\r" in body or ",-2\n" in body  # numerals untouched
    else:
        assert "'+1" not in body
    assert COLS[0] == "id"


def test_proof_reader_restores_prefix_on_keys_and_values():
    from src.transfer.reconcile_step import _restore_formula_prefix

    rows = [{"'=h": "'=1+1", "n": "-2", "q": "''=x", "v": 5}]
    assert _restore_formula_prefix(rows) == [{"=h": "=1+1", "n": "-2", "q": "'=x", "v": 5}]
