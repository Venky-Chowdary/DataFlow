"""Business-rule guard: append keeps existing rows, overwrite replaces them."""

from __future__ import annotations

import csv
import io
import json
import sqlite3
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


class _FakeMongo:
    def __init__(self):
        self.jobs: dict[str, dict] = {}

    def get_job(self, job_id: str) -> dict | None:
        return self.jobs.get(job_id)

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        self.jobs.setdefault(job_id, {})
        self.jobs[job_id].update(kwargs)
        self.jobs[job_id]["status"] = status
        return True


@pytest.fixture(autouse=True)
def _patch_mongodb_service(monkeypatch):
    fake_mongo = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake_mongo)


def _csv_content(rows: list[dict]) -> bytes:
    cols = list(rows[0].keys())
    lines = [",".join(cols)]
    for r in rows:
        lines.append(",".join(str(r[c]) for c in cols))
    return "\n".join(lines).encode("utf-8")


def _seed_products(db_path: Path, rows: list[tuple]) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE products (id INTEGER, name TEXT)")
    conn.executemany("INSERT INTO products VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


def _read_products(db_path: Path) -> list[tuple]:
    conn = sqlite3.connect(str(db_path))
    rows = conn.execute("SELECT id, name FROM products ORDER BY id").fetchall()
    conn.close()
    return rows


@pytest.mark.parametrize("sync_mode", ["append", "full_refresh_append", "insert", "full_append"])
def test_append_preserves_existing_rows(sync_mode: str):
    """Append aliases must add rows without deleting previously loaded data."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "append.db"
        _seed_products(db_path, [(1, "existing-1"), (2, "existing-2")])

        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            source_filename="products.csv",
            source_content=_csv_content([
                {"id": "3", "name": "new-1"},
                {"id": "4", "name": "new-2"},
            ]),
            destination=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(db_path),
                table="products",
            ),
            sync_mode=sync_mode,
            skip_preflight=True,
        )

        engine = UniversalTransferEngine()
        result = engine.execute_tracked(request, uuid.uuid4().hex)
        assert result.success is True, result.error
        assert result.records_transferred == 2
        assert _read_products(db_path) == [
            (1, "existing-1"),
            (2, "existing-2"),
            (3, "new-1"),
            (4, "new-2"),
        ]


def test_append_with_empty_stream_contract_does_not_drop(monkeypatch):
    """UI often sends stream_contracts without sync_mode — must not wipe on append."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "append_contract.db"
        _seed_products(db_path, [(1, "keep-me")])

        drop_calls: list[str] = []
        real_drop = engine_mod._drop_destination_table

        def _tracking_drop(dest, **kwargs):
            drop_calls.append(dest.table or "")
            return real_drop(dest, **kwargs)

        monkeypatch.setattr(engine_mod, "_drop_destination_table", _tracking_drop)

        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            source_filename="products.csv",
            source_content=_csv_content([{"id": "2", "name": "added"}]),
            destination=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(db_path),
                table="products",
            ),
            sync_mode="full_refresh_append",
            stream_contracts=[{"name": "products", "selected": True}],
            skip_preflight=True,
        )

        engine = UniversalTransferEngine()
        result = engine.execute_tracked(request, uuid.uuid4().hex)
        assert result.success is True, result.error
        assert drop_calls == []
        assert _read_products(db_path) == [(1, "keep-me"), (2, "added")]


def test_overwrite_calls_drop_then_replaces(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "overwrite_drop.db"
        _seed_products(db_path, [(1, "stale"), (99, "orphan")])

        drop_calls: list[str] = []
        real_drop = engine_mod._drop_destination_table

        def _tracking_drop(dest, **kwargs):
            drop_calls.append(dest.table or "")
            return real_drop(dest, **kwargs)

        monkeypatch.setattr(engine_mod, "_drop_destination_table", _tracking_drop)

        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            source_filename="products.csv",
            source_content=_csv_content([
                {"id": "1", "name": "fresh-1"},
                {"id": "2", "name": "fresh-2"},
            ]),
            destination=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(db_path),
                table="products",
            ),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
        )

        engine = UniversalTransferEngine()
        result = engine.execute_tracked(request, uuid.uuid4().hex)
        assert result.success is True, result.error
        assert drop_calls == ["products"]
        assert _read_products(db_path) == [(1, "fresh-1"), (2, "fresh-2")]


def test_full_refresh_overwrite_replaces_existing_rows():
    """Full-refresh overwrite should replace the entire table, not merge."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "overwrite.db"

        # Pre-populate destination with stale rows.
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE products (id INTEGER, name TEXT)")
        conn.execute("INSERT INTO products VALUES (1, 'stale'), (99, 'orphan')")
        conn.commit()
        conn.close()

        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            source_filename="products.csv",
            source_content=_csv_content([
                {"id": "1", "name": "fresh-1"},
                {"id": "2", "name": "fresh-2"},
            ]),
            destination=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(db_path),
                table="products",
            ),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
        )

        engine = UniversalTransferEngine()
        result = engine.execute_tracked(request, uuid.uuid4().hex)
        assert result.success is True, result.error
        assert result.records_transferred == 2

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT id, name FROM products ORDER BY id").fetchall()
        conn.close()
        assert rows == [(1, "fresh-1"), (2, "fresh-2")]


@pytest.fixture
def two_run_world(tmp_path, monkeypatch):
    import services.mongodb_service as stores
    import services.sync_cursor as cursors

    store = stores.MemoryMongoDBService()
    monkeypatch.setattr(stores, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(stores, "_mongodb_service", store)
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(cursors, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(cursors, "_mongo_cursors", lambda: None)
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    return store


def _write_two_run_source(path: Path, source_format: str, rows: list[dict]) -> None:
    if source_format == "csv":
        output = io.StringIO(newline="")
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
        path.write_text(output.getvalue(), encoding="utf-8")
    else:
        path.write_text(
            "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n",
            encoding="utf-8",
        )


def _two_run_connection(destination_format: str, db_path: Path):
    if destination_format == "sqlite":
        return sqlite3.connect(str(db_path))
    duckdb = pytest.importorskip("duckdb", reason="DuckDB two-run driver is optional")
    return duckdb.connect(str(db_path))


def _read_two_run_rows(destination_format: str, db_path: Path) -> list[tuple]:
    connection = _two_run_connection(destination_format, db_path)
    try:
        return connection.execute(
            "SELECT id, name, amount, note FROM products ORDER BY id, name, amount"
        ).fetchall()
    finally:
        connection.close()


@pytest.mark.parametrize("source_format", ["csv", "jsonl"])
@pytest.mark.parametrize("destination_format", ["sqlite", "duckdb"])
@pytest.mark.parametrize("validation_mode", ["balanced", "strict"])
@pytest.mark.parametrize("sync_mode", [
    "full_refresh_overwrite", "full_refresh_append", "incremental_append", "upsert",
])
def test_file_two_run_sync_modes_fullflow(
    tmp_path, two_run_world, source_format, destination_format, validation_mode, sync_mode,
):
    """Real parsing, preflight and writes must preserve the projected population."""
    from tests.sync_mode_probe import stream_contract

    db_path = tmp_path / f"destination.{destination_format}"
    connection = _two_run_connection(destination_format, db_path)
    try:
        identity = "INTEGER PRIMARY KEY" if sync_mode == "upsert" else "INTEGER"
        connection.execute(
            f"CREATE TABLE products (id {identity}, name TEXT, amount INTEGER, note TEXT)"
        )
        connection.commit()
    finally:
        connection.close()

    first = [
        {
            "id": number,
            "source_name": f"caf\u00e9, \u6771\u4eac {number}",
            "source_amount": number * 10,
            "source_note": None if source_format == "jsonl" and number % 2 == 0
            else f'quote "{number}"',
        }
        for number in range(1, 7)
    ]
    second = [dict(row) for row in first] + [
        {
            "id": number,
            "source_name": f"na\u00efve \u03a9 {number}",
            "source_amount": number * 10,
            "source_note": None if source_format == "jsonl" else "new row",
        }
        for number in (7, 8)
    ]
    if sync_mode == "upsert":
        second[1].update(source_name="updated \u00e9", source_amount=425,
                         source_note="changed")

    def projected(rows):
        return [
            (row["id"], row["source_name"], row["source_amount"], row["source_note"])
            for row in rows
        ]

    contract = stream_contract("products", sync_mode)
    if sync_mode == "full_refresh_append":
        contract["primary_key"] = ""
    if sync_mode == "incremental_append":
        contract["cursor_semantics"] = "insert_only"

    source_path = tmp_path / f"products.{source_format}"
    engine = UniversalTransferEngine()
    for attempt, population in enumerate((first, second), start=1):
        _write_two_run_source(source_path, source_format, population)
        request = TransferRequest(
            source=EndpointConfig(kind="file", format=source_format),
            source_filename=source_path.name,
            source_path=str(source_path),
            source_content=source_path.read_bytes(),
            destination=EndpointConfig(
                kind="database", format=destination_format,
                database=str(db_path), table="products",
            ),
            mappings=[
                {"source": "id", "target": "id", "confidence": 1.0},
                {"source": "source_name", "target": "name", "confidence": 1.0},
                {"source": "source_amount", "target": "amount", "confidence": 1.0},
                {"source": "source_note", "target": "note", "confidence": 1.0},
            ],
            column_types={"id": "INTEGER", "name": "TEXT", "amount": "INTEGER",
                          "note": "TEXT"},
            sync_mode=sync_mode,
            validation_mode=validation_mode,
            stream_contracts=[contract],
            skip_preflight=False,
        )
        result = engine.execute_tracked(request, uuid.uuid4().hex)
        landed = _read_two_run_rows(destination_format, db_path)
        evidence = {
            "route": f"{source_format}->{destination_format}", "mode": sync_mode,
            "run": attempt, "error": result.error, "landed": landed,
            "validation_plan": result.validation_plan,
            "destination_summary": result.destination_summary,
        }
        assert result.success is True, evidence
        assert result.validation_plan, evidence
        assert result.reconciliation.get("passed") is True, evidence
        expected = projected(population)
        if attempt == 2 and sync_mode == "full_refresh_append":
            expected = sorted(projected(first) + projected(second))
        assert landed == expected, evidence
        assert all(type(row[0]) is int and type(row[2]) is int for row in landed), evidence
        assert all(type(row[1]) is str for row in landed), evidence
        assert all(row[3] is None or type(row[3]) is str for row in landed), evidence
        assert result.records_transferred == (
            2 if attempt == 2 and sync_mode == "incremental_append" else len(population)
        ), evidence
