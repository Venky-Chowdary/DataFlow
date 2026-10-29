"""X-5/X-140: a file destination is verified against the file it finally holds.

Same-run repro: the artifact is changed after the write and before
reconciliation (row 1 ``amt`` 10.50 -> 999.99). The run must fail naming the
row and column, the checked file must be the final file, and no file or Mongo
route may print SQLAlchemy dialect/driver text.
"""

from __future__ import annotations

import io
import json
import shutil
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from services.export_paths import exports_dir

import src.transfer.engine as engine_module
from services.shape_models import ShapeRecipe
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests.sftp_test_server import start_sftp_server
from tests.test_x97_append_continuity import MONGO_URI, PG

FORMATS = ["csv", "jsonl", "parquet"]
COLS = ["id", "name", "amt", "note", "flag", "ts", "big"]
RECIPE = {
    "steps": [
        {"op": "trim", "column": "name"},
        {"op": "case", "column": "note", "options": {"mode": "upper"}},
        {"op": "cast_column", "column": "amt", "options": {"to_type": "decimal"}},
    ]
}
SQL_DRIVER_TEXT = ("SQLAlchemy", "dialect/driver")


def _rows() -> list[dict[str, Any]]:
    return [
        {
            "id": i,
            "name": f"  n{i} ",
            "amt": "10.50" if i == 1 else f"{i}.25",
            "note": f"note {i}",
            "flag": i % 2 == 0,
            "ts": f"2026-10-0{i} 12:00:00+00",
            "big": 10**30 + i,
        }
        for i in range(1, 6)
    ]


@pytest.fixture(autouse=True)
def _loopback(monkeypatch):
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8")


@pytest.fixture
def pg_source():
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(
            host=PG["host"], port=int(PG["port"]), user=PG["username"],
            password=PG["password"], dbname=PG["database"], connect_timeout=3,
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    table = "x5sr_" + uuid.uuid4().hex[:8]
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT, amt TEXT, note TEXT, "
        "flag BOOLEAN, ts TIMESTAMPTZ, big NUMERIC(38,0))"
    )
    for r in _rows():
        cur.execute(f"INSERT INTO {table} VALUES (%s,%s,%s,%s,%s,%s,%s)", tuple(r[c] for c in COLS))  # nosec B608
    yield EndpointConfig(
        kind="database", format="postgresql", host=PG["host"], port=int(PG["port"]),
        username=PG["username"], password=PG["password"], database=PG["database"], table=table,
    )
    cur.execute(f"DROP TABLE IF EXISTS {table}")
    conn.close()


@pytest.fixture
def mongo_source():
    pymongo = pytest.importorskip("pymongo")
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    try:
        client.admin.command("ping")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"MongoDB not reachable: {exc}")
    coll_name = "x5sr_" + uuid.uuid4().hex[:8]
    coll = client["x5sr"][coll_name]
    coll.insert_many(
        [{**r, "ts": r["ts"].replace(" ", "T").replace("+00", "Z"), "big": str(r["big"])} for r in _rows()]
    )
    yield EndpointConfig(
        kind="database", format="mongodb", connection_string=MONGO_URI,
        database="x5sr", table=coll_name,
    )
    coll.drop()
    client.close()


def _tamper(path: Path) -> None:
    if path.suffix == ".parquet":
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pq.read_table(path)
        rows = table.to_pylist()
        for r in rows:
            if int(r["id"]) == 1:
                v = r["amt"]
                r["amt"] = Decimal("999.99") if isinstance(v, Decimal) else (999.99 if isinstance(v, float) else "999.99")
        pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path)
        return
    text = path.read_text()
    out = text.replace("10.50", "999.99", 1)
    if out == text:
        out = text.replace("10.5,", "999.99,", 1).replace('"amt": 10.5,', '"amt": 999.99,', 1)
    assert out != text, "tamper did not change the artifact"
    path.write_text(out)


def _amt_of_row1(data: bytes, fmt: str) -> str:
    if fmt == "csv":
        lines = data.decode().splitlines()
        head = lines[0].split(",")
        for line in lines[1:]:
            cells = line.split(",")
            if cells[head.index("id")] == "1":
                return cells[head.index("amt")]
    elif fmt == "jsonl":
        for line in data.decode().splitlines():
            rec = json.loads(line)
            if int(rec["id"]) == 1:
                return str(rec["amt"])
    else:
        import pyarrow.parquet as pq

        for rec in pq.read_table(io.BytesIO(data)).to_pylist():
            if int(rec["id"]) == 1:
                return str(rec["amt"])
    raise AssertionError("row id=1 not found")


def _run(monkeypatch, source: EndpointConfig, dest: EndpointConfig, artifact, *, tamper: bool, recipe: bool = True):
    seen: dict[str, Any] = {}
    real = engine_module.run_reconciliation

    def patched(*a, **k):
        path = artifact()
        assert path.exists(), f"artifact {path} not written before reconciliation"
        if tamper:
            _tamper(path)
        seen["checked"] = path.read_bytes()
        return real(*a, **k)

    monkeypatch.setattr(engine_module, "run_reconciliation", patched)
    req = TransferRequest(
        source=source,
        destination=dest,
        sync_mode="full_refresh_overwrite",
        shape_recipe=RECIPE if recipe else None,
        approved_shape_recipe_hash=(
            ShapeRecipe.parse(RECIPE, source_columns=COLS).recipe_hash if recipe else None
        ),
        validation_mode="strict",
        stream_contracts=[{
            "name": source.table, "selected": True, "primary_key": ["id"],
            "sync_mode": "full_refresh_overwrite",
        }],
    )
    result = UniversalTransferEngine().execute_tracked(req, "x5sr-" + uuid.uuid4().hex[:10])
    monkeypatch.setattr(engine_module, "run_reconciliation", real)
    final = artifact()
    # A failed run into an empty destination is rolled back, which removes the
    # artifact; say why instead of dying on a bare FileNotFoundError.
    assert final.exists(), (
        f"run left no artifact: success={result.success} error={getattr(result, 'error', None)!r}"
    )
    seen["final"] = final.read_bytes()
    return result, seen


def _dump(result) -> str:
    data = result.model_dump() if hasattr(result, "model_dump") else result.__dict__
    return json.dumps(data, default=str)


_EXPORTS = exports_dir()


def _local(tmp_path, monkeypatch, fmt, request):
    # file_export output_path is confined to apps/api/exports (services/export_paths.py);
    # a relative name resolves under that folder.
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "data"))
    run_dir = f"x5sr_test/{uuid.uuid4().hex[:8]}"
    request.addfinalizer(lambda: shutil.rmtree(_EXPORTS / run_dir, ignore_errors=True))
    dest = EndpointConfig(
        kind="file_export", format=fmt, table="x_out", output_path=f"{run_dir}/x_out.{fmt}"
    )
    return dest, lambda: _EXPORTS / run_dir / f"x_out.{fmt}"


def _assert_tamper_caught(result, seen, fmt):
    assert not result.success, "a tampered artifact must fail the run"
    err = result.error or ""
    dump = _dump(result)
    assert "amt" in err and "999.99" in dump, err
    assert "'1'" in dump or '"1"' in dump or "row 1" in err or "id=1" in dump, err
    for text in SQL_DRIVER_TEXT:
        assert text not in dump, f"file route printed SQL driver text: {text}"
    # The artifact that was checked is the one left at rest.
    assert seen["final"] == seen["checked"]
    assert _amt_of_row1(seen["final"], fmt) in {"999.99", "999.990000000000000000"} or "999.99" in _amt_of_row1(seen["final"], fmt)


def _assert_clean(result, seen):
    assert result.success, result.error
    dump = _dump(result)
    for text in SQL_DRIVER_TEXT:
        assert text not in dump, f"file route printed SQL driver text: {text}"
    recon = (result.model_dump() if hasattr(result, "model_dump") else result.__dict__)
    proof = json.loads(json.dumps(recon, default=str)).get("reconciliation", {}).get("transform_applied", {})
    assert proof.get("status") == "verified", proof
    assert seen["final"] == seen["checked"]


@pytest.mark.parametrize("fmt", FORMATS)
def test_local_file_same_run_tamper_fails_naming_row_and_column(fmt, tmp_path, monkeypatch, pg_source, request):
    dest, artifact = _local(tmp_path, monkeypatch, fmt, request)
    result, seen = _run(monkeypatch, pg_source, dest, artifact, tamper=True)
    _assert_tamper_caught(result, seen, fmt)
    assert "row 1 column amt" in (result.error or "")


@pytest.mark.parametrize("fmt", FORMATS)
def test_local_file_clean_shaped_run_is_verified_without_sql_text(fmt, tmp_path, monkeypatch, pg_source, request):
    dest, artifact = _local(tmp_path, monkeypatch, fmt, request)
    result, seen = _run(monkeypatch, pg_source, dest, artifact, tamper=False)
    _assert_clean(result, seen)


@pytest.mark.parametrize("tamper", [False, True], ids=["clean", "tamper"])
def test_mongo_source_to_file_has_no_sql_text(tamper, tmp_path, monkeypatch, mongo_source, request):
    dest, artifact = _local(tmp_path, monkeypatch, "csv", request)
    result, seen = _run(monkeypatch, mongo_source, dest, artifact, tamper=tamper)
    if tamper:
        _assert_tamper_caught(result, seen, "csv")
    else:
        _assert_clean(result, seen)


@pytest.fixture
def sftp_server(tmp_path):
    root = tmp_path / "srv"
    root.mkdir()
    server, runner = start_sftp_server(str(root))
    yield server, root
    runner.stop()


def _sftp(server, root: Path, fmt: str):
    name = f"/x_out_{uuid.uuid4().hex[:6]}.{fmt}"
    cfg = server.endpoint_config(name)
    dest = EndpointConfig(
        kind="database", format="sftp", host=cfg["host"], port=cfg["port"],
        username=cfg["username"], password=cfg["password"], database=cfg["database"],
        table=cfg["table"], extra={"host_key": cfg["host_key"]},
    )
    return dest, lambda: root / name.lstrip("/")


@pytest.mark.parametrize("fmt", FORMATS)
def test_sftp_same_run_tamper_fails_without_sql_text(fmt, sftp_server, monkeypatch, pg_source):
    server, root = sftp_server
    dest, artifact = _sftp(server, root, fmt)
    result, seen = _run(monkeypatch, pg_source, dest, artifact, tamper=True)
    _assert_tamper_caught(result, seen, fmt)


@pytest.mark.parametrize("fmt", FORMATS)
def test_sftp_clean_shaped_run_is_verified_without_sql_text(fmt, sftp_server, monkeypatch, pg_source):
    server, root = sftp_server
    dest, artifact = _sftp(server, root, fmt)
    result, seen = _run(monkeypatch, pg_source, dest, artifact, tamper=False)
    _assert_clean(result, seen)


CENSUS_CLAUSE = "culprit keys could not be named"


@pytest.mark.parametrize("fmt", FORMATS)
def test_local_tamper_message_omits_census_clause_when_row_is_named(fmt, tmp_path, monkeypatch, pg_source, request):
    dest, artifact = _local(tmp_path, monkeypatch, fmt, request)
    result, _seen = _run(monkeypatch, pg_source, dest, artifact, tamper=True, recipe=False)
    err = result.error or ""
    assert "row 1 column amt" in err and "999.99" in err, err
    assert CENSUS_CLAUSE not in err, err


@pytest.mark.parametrize("fmt", FORMATS)
def test_sftp_tamper_message_omits_census_clause_when_row_is_named(fmt, sftp_server, monkeypatch, pg_source):
    server, root = sftp_server
    dest, artifact = _sftp(server, root, fmt)
    result, _seen = _run(monkeypatch, pg_source, dest, artifact, tamper=True, recipe=False)
    err = result.error or ""
    assert "row 1 column amt" in err and "999.99" in err, err
    assert CENSUS_CLAUSE not in err, err
