"""D34 — a multi-file pickup under full-refresh overwrite is one source.

``load_picked_files`` loaded every picked file with the run's overwrite, so
each file replaced the one before it: an S3 glob of two files landed only the
last, Gate-8 then failed the row count, and the destination's previous rows
were gone too. The set now loads as one source — one staged write and one
swap per engine — so both files land, and a failure on the second file leaves
the pre-run table exactly as it was.
"""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

import pytest

import src.transfer.engine as engine_mod
from src.transfer.models import EndpointConfig
from tests.test_cr_d125_file_quoted_empty_fidelity import (
    assert_verified,
    mysql_dest,
    mysql_rows_and_drop,
    pg_dest,
    pg_rows_and_drop,
    put_s3_object,
    run_transfer,
    s3_source,
)

SEED = [(100, "old"), (101, "old")]
BOTH = [(1, "a"), (2, "b"), (3, "c"), (4, "d")]


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.file_load_ledger as ledger
    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setattr(ledger, "STORE_PATH", tmp_path / "file_load_ledger.json")


def _destination(dest: str, tmp_path: Path, table: str) -> EndpointConfig:
    if dest == "sqlite":
        return EndpointConfig(
            kind="database", format="sqlite", database=str(tmp_path / "dst.db"), table="t"
        )
    return pg_dest(table) if dest == "postgresql" else mysql_dest(table)


def _rows(dest: str, tmp_path: Path, table: str) -> list[tuple]:
    if dest == "sqlite":
        conn = sqlite3.connect(str(tmp_path / "dst.db"))
        try:
            return [tuple(r) for r in conn.execute("SELECT id, name FROM t ORDER BY id")]
        finally:
            conn.close()
    return pg_rows_and_drop(table) if dest == "postgresql" else mysql_rows_and_drop(table)


def _run_glob_overwrite(dest: str, tmp_path: Path, table: str):
    """Seed the destination, then overwrite it from a two-file glob."""
    import moto

    bucket = f"d34-{uuid.uuid4().hex[:8]}"
    destination = _destination(dest, tmp_path, table)
    with moto.mock_aws():
        put_s3_object(bucket, "seed/s.csv", "id,name\n100,old\n101,old\n")
        seeded = run_transfer(source=s3_source(bucket, "seed/s.csv"), destination=destination)
        assert_verified(seeded)
        put_s3_object(bucket, "in/a_1.csv", "id,name\n1,a\n2,b\n")
        put_s3_object(bucket, "in/b_2.csv", "id,name\n3,c\n4,d\n")
        return run_transfer(source=s3_source(bucket, "in/*.csv"), destination=destination)


@pytest.mark.parametrize("dest", ["sqlite", "postgresql", "mysql"])
def test_glob_overwrite_lands_every_file(tmp_path, dest):
    table = f"d34_{uuid.uuid4().hex[:8]}"
    result = _run_glob_overwrite(dest, tmp_path, table)
    rows = _rows(dest, tmp_path, table)
    assert_verified(result)
    assert rows == BOTH


@pytest.mark.parametrize("dest", ["sqlite", "postgresql", "mysql"])
def test_failure_on_the_second_file_leaves_the_pre_run_table(tmp_path, monkeypatch, dest):
    """File 1's batch is written before file 2's fails: nothing may commit."""
    import src.transfer.file_stream as file_stream

    monkeypatch.setenv("DATAFLOW_CSV_LOCAL_COPY", "0")
    table = f"d34_{uuid.uuid4().hex[:8]}"
    real_write = file_stream._write_batch
    calls = {"set": 0}
    armed = {"on": False}

    def _write(*args, **kwargs):
        if armed["on"]:
            calls["set"] += 1
            if calls["set"] >= 2:
                raise RuntimeError("injected failure writing the second file")
        return real_write(*args, **kwargs)

    import services.file_pickup_load as pickup

    real_load = pickup.load_picked_files

    def _armed_load(**kwargs):
        # Two rows per file: one write batch per file. Arm only the glob run.
        armed["on"] = str(kwargs.get("key") or "").endswith("*.csv")
        return real_load(**kwargs)

    monkeypatch.setattr(file_stream, "_write_batch", _write)
    monkeypatch.setattr(
        file_stream, "file_dest_batch_size", lambda *_a, **_k: 2
    )
    monkeypatch.setattr(pickup, "load_picked_files", _armed_load)
    import src.transfer.stream as stream_mod

    if hasattr(stream_mod, "load_picked_files"):
        monkeypatch.setattr(stream_mod, "load_picked_files", _armed_load)

    result = _run_glob_overwrite(dest, tmp_path, table)
    rows = _rows(dest, tmp_path, table)
    assert result.success is False
    assert calls["set"] >= 2, "the injected failure never fired"
    assert rows == SEED


def test_a_later_overwrite_with_one_new_file_still_loads_the_whole_set(tmp_path):
    """The manifest picks new files; an overwrite's source is every match."""
    import moto

    bucket = f"d34-{uuid.uuid4().hex[:8]}"
    destination = _destination("sqlite", tmp_path, "t")
    with moto.mock_aws():
        put_s3_object(bucket, "in/a_1.csv", "id,name\n1,a\n2,b\n")
        put_s3_object(bucket, "in/b_2.csv", "id,name\n3,c\n4,d\n")
        first = run_transfer(source=s3_source(bucket, "in/*.csv"), destination=destination)
        assert_verified(first)
        put_s3_object(bucket, "in/c_3.csv", "id,name\n5,e\n")
        second = run_transfer(source=s3_source(bucket, "in/*.csv"), destination=destination)
    assert_verified(second)
    assert _rows("sqlite", tmp_path, "t") == BOTH + [(5, "e")]


def test_combine_file_set_keeps_cells_and_refuses_what_it_cannot_join(tmp_path):
    import io

    from services.csv_codec import iter_csv_records
    from src.transfer.file_stream import combine_file_set

    a = tmp_path / "a.csv"
    a.write_text('id,name\n1,""\n2,\n', encoding="utf-8")
    b = tmp_path / "b.csv"
    b.write_text("id;name\n3;x\n", encoding="utf-8")
    name, counts = combine_file_set([(a, "in/a.csv"), (b, "in/b.csv")], tmp_path / "set")
    assert (name, counts) == ("a.csv", [2, 1])
    body = (tmp_path / "set").read_text(encoding="utf-8")
    assert list(iter_csv_records(io.StringIO(body, newline=""), ",")) == [
        ["id", "name"], ["1", ""], ["2", None], ["3", "x"],
    ]

    j1 = tmp_path / "a.jsonl"
    j1.write_text('{"id":1}\n\n{"id":2}', encoding="utf-8")
    j2 = tmp_path / "b.jsonl"
    j2.write_text('{"id":3}\n', encoding="utf-8")
    name, counts = combine_file_set([(j1, "a.jsonl"), (j2, "b.jsonl")], tmp_path / "jset")
    assert (name, counts) == ("a.jsonl", [2, 1])
    assert (tmp_path / "jset").read_text(encoding="utf-8").splitlines() == [
        '{"id":1}', '{"id":2}', '{"id":3}',
    ]

    with pytest.raises(ValueError, match="one source"):
        combine_file_set([(a, "a.parquet"), (b, "b.parquet")], tmp_path / "pset")
    with pytest.raises(ValueError, match="one source"):
        combine_file_set([(a, "a.csv"), (j1, "b.jsonl")], tmp_path / "mixed")
