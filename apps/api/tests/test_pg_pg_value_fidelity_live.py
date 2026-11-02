"""PG -> PG value fidelity for types the row path used to rewrite.

Each case copies one column PostgreSQL -> PostgreSQL through the engine with
preflight ON, on both the binary COPY fast path and the row path, and requires
the destination's text image (``col::text``) and type to equal the source's.

Live PostgreSQL.
"""

from __future__ import annotations

import os
import socket
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

PG = dict(host="127.0.0.1", port=5432, database="dataflow", username="dataflow", password="dataflow")


def _up() -> bool:
    try:
        with socket.create_connection((PG["host"], PG["port"]), timeout=1.5):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _up(), reason="PostgreSQL not reachable")


def _pg(sql: str) -> list[tuple]:
    import psycopg2

    conn = psycopg2.connect(
        host=PG["host"], port=PG["port"], dbname=PG["database"],
        user=PG["username"], password=PG["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return list(cur.fetchall()) if cur.description else []
    finally:
        conn.close()


#: defect id -> (column type, two literal values); a NULL row is always added.
CASES = {
    # D50 — timetz was planned as TIME, refused as an offset-dropping collapse.
    "D50_timetz": ("timetz", "'10:00:00+05:30'", "'23:59:59-04'"),
    # D51 — NaN / ±Infinity were quarantined as "non-finite float" invents.
    "D51_real_nonfinite": ("real", "'NaN'", "'Infinity'"),
    "D51_double_nonfinite": ("double precision", "'-Infinity'", "1.5"),
    # D80 — the read-back rendered unbounded ranges as ``[1, None)``.
    "D80_unbounded_range": ("int4range", "'[1,)'", "'(,5]'"),
    # D79 — varbit was validated as hex/base64 bytes; odd-length bits refused.
    "D79_varbit": ("varbit", "B'101'", "B'1'"),
    # D75 — the checksum read-back decoded JSON ``null`` as SQL NULL.
    "D75_json_null": ("jsonb", "'null'", "'[]'"),
    # D74 — a high-precision JSON number was rewritten as a JSON string.
    "D74_json_big_number": (
        "jsonb", "'{\"a\": 12345678901234567890.12345678901234567890}'", "'[1.5, 2]'"
    ),
    # D68 — a bare numeric was created as the NUMERIC(p,s) its rows measured.
    "D68_unconstrained_numeric": ("numeric", "1.5", "12345678901234567890.123456789"),
}


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


@pytest.mark.parametrize("copy_path", ["1", "0"], ids=["copy", "row_path"])
@pytest.mark.parametrize("case", list(CASES))
def test_pg_to_pg_value_survives(isolated, monkeypatch, case, copy_path):
    monkeypatch.setenv("DATAFLOW_PG_PG_COPY", copy_path)
    col_type, first, second = CASES[case]
    src = f"pgfid_s_{uuid.uuid4().hex[:8]}"
    dst = f"pgfid_d_{uuid.uuid4().hex[:8]}"
    _pg(
        f"CREATE TABLE public.{src} (id BIGINT PRIMARY KEY, c {col_type}); "
        f"INSERT INTO public.{src} VALUES (1, {first}), (2, {second}), (3, NULL)"
    )
    try:
        request = TransferRequest(
            source=EndpointConfig(kind="database", format="postgresql", schema="public", table=src, **PG),
            destination=EndpointConfig(kind="database", format="postgresql", schema="public", table=dst, **PG),
            sync_mode="full_refresh_overwrite",
            validation_mode="strict",
        )
        job_id = "pgfid" + uuid.uuid4().hex[:12]
        isolated.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(request, job_id)
        assert result.success, result.error

        image = "SELECT id, c::text, pg_typeof(c)::text FROM public.{} ORDER BY id"
        assert _pg(image.format(dst)) == _pg(image.format(src))
        if case.startswith("D68"):
            # Still unbounded: the next, larger value must fit the new table.
            assert _pg(
                "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
                f"WHERE attrelid = 'public.{dst}'::regclass AND attname = 'c'"
            ) == [("numeric",)]
    finally:
        _pg(f"DROP TABLE IF EXISTS public.{src}; DROP TABLE IF EXISTS public.{dst}")
