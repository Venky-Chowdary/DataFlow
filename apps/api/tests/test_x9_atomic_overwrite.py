"""X-9: a failed run must never leave a changed destination.

Invariant (production contract): after a failed run the destination is
byte-identical to before it — row count AND content — on every writer, for
every failure path: mid-run transform refusal, writer reject, cancel.

These cells drive a file (CSV) source into a live SQLite destination across
multiple chunks with a strict cast_column refusal on the last row.
"""

from __future__ import annotations

import sqlite3
import sys
import uuid
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from services.shape_models import ShapeRecipe  # noqa: E402

N_ROWS = 50000


def _csv(n: int = N_ROWS) -> bytes:
    lines = [f"{i},{i}.50" for i in range(1, n)]
    lines.append(f"{n},abc")  # last row: strict cast_column refusal
    return ("id,amt\n" + "\n".join(lines) + "\n").encode()


def _engine_env(monkeypatch):
    import src.transfer.engine as engine_mod
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128,localhost")
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


def _recipe() -> dict:
    return {
        "steps": [
            {"op": "cast_column", "column": "amt", "options": {"to_type": "decimal"}},
        ]
    }


def _request(tmp_path: Path, table: str) -> TransferRequest:
    recipe = _recipe()
    return TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            host="",
            port=0,
            database=str(tmp_path / "dest.sqlite"),
            username="",
            password="",
            table=table,
        ),
        source_filename="amt.csv",
        source_content=_csv(),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        skip_preflight=True,
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(
            recipe, source_columns=["id", "amt"]
        ).recipe_hash,
        mappings=[
            {"source": "id", "target": "id", "confidence": 1.0, "target_type": "integer"},
            {"source": "amt", "target": "amt", "confidence": 1.0, "target_type": "DECIMAL(10,2)"},
        ],
    )


def _dest_snapshot(dbfile: str, table: str):
    conn = sqlite3.connect(dbfile)
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if not exists:
            return None
        rows = conn.execute(f'SELECT id, amt FROM "{table}" ORDER BY id').fetchall()
        ddl = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()[0]
        return {"ddl": ddl, "rows": rows}
    finally:
        conn.close()


def _make_prior(dbfile: str, table: str) -> None:
    conn = sqlite3.connect(dbfile)
    try:
        conn.execute(
            f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY, amt NUMERIC CHECK (amt < 100))'
        )
        conn.executemany(
            f'INSERT INTO "{table}" (id, amt) VALUES (?, ?)',
            [(900001, 10.0), (900002, 5.0), (900003, 3.0)],
        )
        conn.commit()
    finally:
        conn.close()


def test_x9_transform_refusal_empty_dest_leaves_nothing(tmp_path, monkeypatch):
    """Empty dest + mid-run transform refusal → destination stays absent."""
    _engine_env(monkeypatch)
    table = f"x9e_{uuid.uuid4().hex[:8]}"
    dbfile = str(tmp_path / "dest.sqlite")
    result = UniversalTransferEngine().execute(_request(tmp_path, table))
    assert not result.success
    snap = _dest_snapshot(dbfile, table)
    assert snap is None or snap["rows"] == [], snap


def test_x9_transform_refusal_preserves_prior_rows(tmp_path, monkeypatch):
    """Dest with 3 prior rows + PK + CHECK: a failed overwrite must leave the
    destination byte-identical (DDL + rows)."""
    _engine_env(monkeypatch)
    table = f"x9p_{uuid.uuid4().hex[:8]}"
    dbfile = str(tmp_path / "dest.sqlite")
    _make_prior(dbfile, table)
    before = _dest_snapshot(dbfile, table)
    result = UniversalTransferEngine().execute(_request(tmp_path, table))
    assert not result.success
    after = _dest_snapshot(dbfile, table)
    assert after == before, (
        f"destination changed by a failed run: before={before['rows']} "
        f"after={after and after['rows']} ddl_same={after and after['ddl'] == before['ddl']}"
    )


def _make_db_source(dbfile: str, table: str, n: int = N_ROWS) -> None:
    conn = sqlite3.connect(dbfile)
    try:
        conn.execute(f'CREATE TABLE "{table}" (id INTEGER, amt TEXT)')
        conn.executemany(
            f'INSERT INTO "{table}" (id, amt) VALUES (?, ?)',
            [(i, f"{i}.50") for i in range(1, n)] + [(n, "abc")],
        )
        conn.commit()
    finally:
        conn.close()


def _db_request(src_db: str, src_table: str, dst_db: str, dst_table: str) -> TransferRequest:
    recipe = _recipe()
    return TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="sqlite",
            host="",
            port=0,
            database=src_db,
            username="",
            password="",
            table=src_table,
        ),
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            host="",
            port=0,
            database=dst_db,
            username="",
            password="",
            table=dst_table,
        ),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        skip_preflight=True,
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(
            recipe, source_columns=["id", "amt"]
        ).recipe_hash,
        mappings=[
            {"source": "id", "target": "id", "confidence": 1.0, "target_type": "integer"},
            {"source": "amt", "target": "amt", "confidence": 1.0, "target_type": "DECIMAL(10,2)"},
        ],
    )


def test_x9_db_source_refusal_preserves_prior_rows(tmp_path, monkeypatch):
    """DB->DB path: dest with 3 prior rows must be byte-identical after a
    mid-run transform refusal (same undo as the file path)."""
    _engine_env(monkeypatch)
    src_table = f"x9s_{uuid.uuid4().hex[:8]}"
    dst_table = f"x9d_{uuid.uuid4().hex[:8]}"
    src_db = str(tmp_path / "src.sqlite")
    dst_db = str(tmp_path / "dst.sqlite")
    _make_db_source(src_db, src_table)
    _make_prior(dst_db, dst_table)
    before = _dest_snapshot(dst_db, dst_table)
    result = UniversalTransferEngine().execute(
        _db_request(src_db, src_table, dst_db, dst_table)
    )
    assert not result.success
    after = _dest_snapshot(dst_db, dst_table)
    assert after == before, (
        f"destination changed by a failed run: before={before['rows']} "
        f"after={after and after['rows']}"
    )


def test_x9_db_source_refusal_empty_dest_leaves_nothing(tmp_path, monkeypatch):
    _engine_env(monkeypatch)
    src_table = f"x9s_{uuid.uuid4().hex[:8]}"
    dst_table = f"x9d_{uuid.uuid4().hex[:8]}"
    src_db = str(tmp_path / "src.sqlite")
    dst_db = str(tmp_path / "dst.sqlite")
    _make_db_source(src_db, src_table)
    result = UniversalTransferEngine().execute(
        _db_request(src_db, src_table, dst_db, dst_table)
    )
    assert not result.success
    snap = _dest_snapshot(dst_db, dst_table)
    assert snap is None or snap["rows"] == [], snap
