"""Review defect (b): keep_last on a CSV whose key header is "Customer ID".

A spaced header lands as ``Customer_ID`` (the destination writer's identifier
rule). ``in_file_dedupe=keep_last`` sends the file down the row path (the COPY
route lands every row), where Gate-8 read the destination back by the *raw*
header: SQLite answered the unknown quoted name ``"Customer ID"`` with the
literal string on every row, PostgreSQL / MySQL could not read it back, and an
exact, correctly de-duplicated load failed the strict checksum. Gate-8 now
reads back the names the writer created.

Real engine (``execute_tracked``) from an uploaded CSV into SQLite, live
PostgreSQL and live MySQL/MariaDB.
"""

from __future__ import annotations

import csv
import io
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
from tests import value_fidelity_route as r  # noqa: E402

_DEDUPE = {"in_file_dedupe": "keep_last", "dedupe_key": "Customer ID"}


@pytest.fixture()
def store(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _csv() -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["Customer ID", "Order Note", "amount"])
    notes = {7: 'q"uo,te', 11: "汉字ß"}
    for i in range(1, 31):
        if i == 2:
            buf.write('2,"",2.50\n')  # R12: quoted "" is the empty string
        elif i == 4:
            buf.write("4,,4.50\n")  # unquoted empty is NULL
        else:
            writer.writerow([i, notes.get(i, f"note {i}"), f"{i}.50"])
    writer.writerow([3, "note 3 LATEST", "3.75"])  # the copy keep_last must keep
    return buf.getvalue().encode("utf-8")


def _destination(kind: str, table: str, tmp_path: Path) -> EndpointConfig:
    if kind == "sqlite":
        return EndpointConfig(kind="database", format="sqlite", database=str(tmp_path / "d.sqlite"),
                              table=table, extra=dict(_DEDUPE))
    cfg = r.dest_config(kind)
    return EndpointConfig(kind="database", format=kind, table=table, host=cfg["host"],
                          port=cfg["port"], database=cfg["database"], username=cfg["username"],
                          password=cfg["password"], schema=str(cfg.get("schema") or ""),
                          extra=dict(_DEDUPE))


def _rows(kind: str, table: str, tmp_path: Path) -> dict[int, tuple]:
    sql = 'SELECT {k}, {n}, amount FROM {t}'
    if kind == "sqlite":
        conn = sqlite3.connect(str(tmp_path / "d.sqlite"))
        try:
            got = conn.execute(sql.format(k='"Customer_ID"', n='"Order_Note"', t=f'"{table}"')).fetchall()
        finally:
            conn.close()
    elif kind == "mysql":
        got = r.query(kind, sql.format(k="`Customer_ID`", n="`Order_Note`", t=f"`{table}`"))
    else:
        got = r.query(kind, sql.format(k='"Customer_ID"', n='"Order_Note"', t=f'public."{table}"'))
    return {int(a): (b, str(c)) for a, b, c in got}


def _run(store, kind: str, tmp_path: Path):
    table = "t2b_" + uuid.uuid4().hex[:8]
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv", extra=dict(_DEDUPE)),
        source_filename="customers.csv", source_content=_csv(),
        destination=_destination(kind, table, tmp_path),
        sync_mode="full_refresh_overwrite", schema_policy="propagate_columns",
        validation_mode="strict",
    )
    job_id = "t2b" + uuid.uuid4().hex[:16]
    store.update_job_status(job_id, "pending", transfer_request={})
    return table, UniversalTransferEngine().execute_tracked(request, job_id), store.get_job(job_id) or {}


@pytest.mark.parametrize("kind", ["sqlite", "postgresql", "mysql"])
def test_keep_last_on_a_spaced_key_header_verifies_green(store, tmp_path, kind):
    if kind != "sqlite":
        r.connect(kind).close()  # skips when the engine is not reachable
    table, result, job = _run(store, kind, tmp_path)
    try:
        assert result.success, result.error
        recon = job.get("reconciliation") or {}
        assert recon.get("checksum_match") is True, recon.get("message")
        rows = _rows(kind, table, tmp_path)
        assert len(rows) == 30
        assert rows[3] == ("note 3 LATEST", "3.75")  # the last copy in file order
        assert rows[2][0] == "" and rows[4][0] is None and rows[11][0] == "汉字ß"
    finally:
        if kind != "sqlite":
            r.drop(kind, table)


def test_a_wrong_cell_still_fails_the_strict_checksum(store, tmp_path, monkeypatch):
    real = engine_mod.run_reconciliation

    def _corrupt_then_verify(**kwargs):
        conn = sqlite3.connect(str(tmp_path / "d.sqlite"))
        with conn:
            table = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 't2b_%'").fetchone()[0]
            conn.execute(f'UPDATE "{table}" SET "Order_Note" = \'WRONG\' WHERE "Customer_ID" = 9')
        conn.close()
        return real(**kwargs)

    monkeypatch.setattr(engine_mod, "run_reconciliation", _corrupt_then_verify)
    _table, result, job = _run(store, "sqlite", tmp_path)
    assert not result.success
    assert (job.get("reconciliation") or {}).get("checksum_match") is False
