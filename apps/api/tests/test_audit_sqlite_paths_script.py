"""scripts/audit_sqlite_paths.py: where saved SQLite/DuckDB connectors point vs the proposed root."""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import audit_sqlite_paths  # noqa: E402


def _connector(cid, name, ctype, **fields):
    return {"id": cid, "name": name, "type": ctype, "role": "both", "workspace_id": "ws-1", **fields}


@pytest.fixture
def world(tmp_path, monkeypatch):
    for var in (
        "DATAFLOW_SQLITE_ROOT", "DATAWRAP_SQLITE_ROOT", "DATAFLOW_DUCKDB_ROOT", "DATAWRAP_DUCKDB_ROOT",
        "DATAWRAP_CONNECTOR_STORE", "DATAWRAP_CONNECTOR_STORE_BACKEND", "DATAFLOW_CONFINE_LOCAL_FILES",
    ):
        monkeypatch.delenv(var, raising=False)
    # The scripts export their store/data-dir choice into os.environ; let
    # monkeypatch restore whatever the process had.
    for var in ("DATAFLOW_CONNECTOR_STORE", "DATAFLOW_CONNECTOR_STORE_BACKEND", "DATAFLOW_DATA_DIR", "MONGODB_URI"):
        monkeypatch.setenv(var, "")  # recorded, so teardown restores the original
        monkeypatch.delenv(var)
    import services.connector_store as connector_store

    monkeypatch.setattr(connector_store, "_backend_choice", None)
    data = tmp_path / "data"
    cwd = tmp_path / "cwd"
    elsewhere = tmp_path / "elsewhere"
    other = tmp_path / "other"
    for d in (cwd, elsewhere, other, data / "sqlite"):
        d.mkdir(parents=True)
    for path in (data / "sqlite" / "inside.db", cwd / "rel.db", elsewhere / "orders.db", other / "orders.db", data / "jobs.db"):
        sqlite3.connect(path).close()
    (elsewhere / "lake.duckdb").write_bytes(b"duck")
    store = tmp_path / "connectors.json"
    store.write_text(
        json.dumps(
            {
                "connectors": [
                    _connector("c-inside", "inside", "sqlite", database=str(data / "sqlite" / "inside.db")),
                    _connector("c-rel", "relative", "sqlite", database="rel.db"),
                    _connector("c-out1", "outside-1", "sqlite", database=str(elsewhere / "orders.db")),
                    _connector("c-out2", "outside-2", "sqlite", connection_string=f"sqlite:///{other}/orders.db"),
                    _connector("c-state", "state", "sqlite", database=str(data / "jobs.db")),
                    _connector("c-mem", "memory", "sqlite", database=":memory:"),
                    _connector("c-duck", "duck", "duckdb", database=str(elsewhere / "lake.duckdb")),
                    _connector("c-md", "motherduck", "duckdb", database="md:analytics"),
                    _connector("c-pg", "postgres", "postgresql", host="db.example.com"),
                ]
            }
        ),
        encoding="utf-8",
    )
    return {"tmp": tmp_path, "data": data, "cwd": cwd, "elsewhere": elsewhere, "other": other, "store": store}


def _run(world, *extra, capsys):
    argv = ["--file", str(world["store"]), "--data-dir", str(world["data"]), "--cwd", str(world["cwd"]), *extra]
    assert audit_sqlite_paths.main(argv) == 0
    return capsys.readouterr().out


def _snapshot(root: Path) -> dict[str, bytes]:
    return {str(p): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}


def test_verdicts_per_connector(world, capsys):
    out = json.loads(_run(world, "--json", capsys=capsys))
    rows = {r["name"]: r for r in out["rows"]}
    assert set(rows) == {"inside", "relative", "outside-1", "outside-2", "state", "memory", "duck", "motherduck"}
    assert rows["inside"]["verdict"] == "inside root" and not rows["inside"]["needs_copy"]
    assert rows["relative"]["verdict"].startswith("relative")
    assert rows["relative"]["today_path"] == str((world["cwd"] / "rel.db").resolve())
    assert rows["relative"]["under_flag_path"] == str((world["data"] / "sqlite" / "rel.db").resolve())
    assert rows["relative"]["needs_copy"] is True
    assert rows["outside-1"]["verdict"] == "outside root" and rows["outside-1"]["needs_copy"]
    assert rows["outside-2"]["today_path"] == str(world["other"] / "orders.db")
    assert "service data directory" in rows["state"]["verdict"] and not rows["state"]["needs_copy"]
    assert rows["memory"]["verdict"] == "in-memory"
    assert rows["duck"]["verdict"] == "outside root"
    assert rows["motherduck"]["verdict"].startswith("MotherDuck")
    assert out["local_file_connectors"] == 8 and out["needs_copy"] == 4


def test_explicit_root_is_the_reference(world, capsys, monkeypatch):
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(world["elsewhere"]))
    rows = {r["name"]: r for r in json.loads(_run(world, "--json", capsys=capsys))["rows"]}
    assert rows["outside-1"]["verdict"] == "inside root"
    assert rows["inside"]["verdict"] == "outside root"
    assert rows["duck"]["verdict"] == "inside root"  # DuckDB follows the SQLite root under the flag


def test_text_report_counts(world, capsys):
    out = _run(world, capsys=capsys)
    assert "8 local-file connectors; 4 need their file copied" in out
    assert "postgres" not in out


def test_copy_plan_is_text_with_distinct_names_and_runs_nothing(world, capsys):
    snapshot = _snapshot(world["tmp"])
    plan = _run(world, "--copy-plan", capsys=capsys)
    sqlite_root = world["data"] / "sqlite"
    assert f"mkdir -p {sqlite_root}" in plan
    assert f".backup {sqlite_root}/rel.db" in plan
    assert f".backup {sqlite_root}/orders.db" in plan
    # two different orders.db files get distinct targets; the later one is renamed
    assert f"{sqlite_root}/orders-c-out2.db" in plan
    assert "set database to 'orders-c-out2.db'" in plan
    assert f"cp -p -n {world['elsewhere']}/lake.duckdb {world['data']}/duckdb/lake.duckdb" in plan
    assert "jobs.db" not in plan and "inside.db" not in plan
    assert _snapshot(world["tmp"]) == snapshot  # nothing copied, nothing written
    assert not (world["data"] / "duckdb").exists()
