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


def _reject_request(tmp_path: Path, table: str) -> TransferRequest:
    """Writer-reject shape: values cast fine but violate dest CHECK(amt<100)."""
    lines = [f"{i},{i}.50" for i in range(1, 30000)]
    lines.append("30000,500.00")
    content = ("id,amt\n" + "\n".join(lines) + "\n").encode()
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
        source_content=content,
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        skip_preflight=True,
        shape_recipe={"steps": [{"op": "cast_column", "column": "amt", "options": {"to_type": "decimal"}}]},
        approved_shape_recipe_hash=ShapeRecipe.parse(
            {"steps": [{"op": "cast_column", "column": "amt", "options": {"to_type": "decimal"}}]},
            source_columns=["id", "amt"],
        ).recipe_hash,
        mappings=[
            {"source": "id", "target": "id", "confidence": 1.0, "target_type": "integer"},
            {"source": "amt", "target": "amt", "confidence": 1.0, "target_type": "DECIMAL(10,2)"},
        ],
    )


def test_x9_writer_reject_preserves_prior_rows(tmp_path, monkeypatch):
    """CHECK violation mid-run: prior rows + DDL must stay byte-identical."""
    _engine_env(monkeypatch)
    table = f"x9r_{uuid.uuid4().hex[:8]}"
    dbfile = str(tmp_path / "dest.sqlite")
    _make_prior(dbfile, table)
    before = _dest_snapshot(dbfile, table)
    result = UniversalTransferEngine().execute(_reject_request(tmp_path, table))
    assert not result.success
    after = _dest_snapshot(dbfile, table)
    assert after == before, (
        f"destination changed by a writer reject: before={before['rows']} "
        f"after={after and after['rows']} ddl_same={after and after['ddl'] == before['ddl']}"
    )


def _clean_csv(n: int = 3000) -> bytes:
    # amt = i/100 keeps every row under the dest CHECK (amt < 100).
    lines = [f"{i},{i/100:.2f}" for i in range(1, n)]
    return ("id,amt\n" + "\n".join(lines) + "\n").encode()


def _clean_request(tmp_path: Path, table: str) -> TransferRequest:
    recipe = {"steps": [{"op": "cast_column", "column": "amt", "options": {"to_type": "decimal"}}]}
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
        source_content=_clean_csv(),
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


def test_x12_successful_overwrite_preserves_ddl(tmp_path, monkeypatch):
    """X-12: a successful overwrite on a customer's existing table must keep
    its declared DDL byte-identical — PK, CHECK, NOT NULL, types. A
    stage+publish that renames a fresh table over the live one silently
    drops the customer's constraints."""
    _engine_env(monkeypatch)
    table = f"x12_{uuid.uuid4().hex[:8]}"
    dbfile = str(tmp_path / "dest.sqlite")
    _make_prior(dbfile, table)
    before = _dest_snapshot(dbfile, table)
    result = UniversalTransferEngine().execute(_clean_request(tmp_path, table))
    assert result.success
    after = _dest_snapshot(dbfile, table)
    assert after is not None
    assert len(after["rows"]) == 2999
    assert after["ddl"] == before["ddl"], (
        f"customer DDL changed by a successful overwrite:\nbefore={before['ddl']}\nafter ={after['ddl']}"
    )


def test_x9_db_source_refusal_empty_dest_leaves_nothing(tmp_path, monkeypatch):
    """DB->DB create-new drop: the run-created table must be absent, not just
    empty — byte-identical means the destination had no such table before."""
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
    assert snap is None, f"table this run created must be dropped, got {snap}"
