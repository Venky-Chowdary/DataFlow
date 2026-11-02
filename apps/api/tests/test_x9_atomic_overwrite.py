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


class _CancelAfterFirstChunk:
    """Job store that answers "cancel requested" once the first batch has
    reported written rows — a mid-run operator cancel, not a pre-run one."""

    def __init__(self, inner):
        self._inner = inner

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def get_job(self, job_id):
        job = dict(self._inner.get_job(job_id) or {})
        if job.get("records_processed"):
            job["cancel_requested"] = True
        return job


def test_x9_cancel_mid_run_leaves_nothing(tmp_path, monkeypatch):
    """Cancel landing after committed batches must still leave the destination
    byte-identical (absent for a create-new run)."""
    import src.transfer.engine as engine_mod

    fake = _engine_env(monkeypatch)
    monkeypatch.setattr(
        engine_mod, "get_mongodb_service", lambda: _CancelAfterFirstChunk(fake)
    )
    table = f"x9c_{uuid.uuid4().hex[:8]}"
    dbfile = str(tmp_path / "dest.sqlite")
    result = UniversalTransferEngine().execute(_request(tmp_path, table))
    assert not result.success
    snap = _dest_snapshot(dbfile, table)
    assert snap is None or snap["rows"] == [], (
        f"cancelled run left {len(snap['rows']) if snap else 'table'} committed"
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


_MYSQL = dict(
    host="127.0.0.1", port=3306, database="dataflow",
    username="dataflow", password="dataflow",
)


def _mysql_conn():
    import pymysql

    return pymysql.connect(
        host=_MYSQL["host"], port=_MYSQL["port"], user=_MYSQL["username"],
        password=_MYSQL["password"], database=_MYSQL["database"],
        connect_timeout=5, autocommit=True,
    )


def _mysql_available() -> bool:
    try:
        conn = _mysql_conn()
        conn.close()
        return True
    except Exception:
        return False


def _mysql_drop(table: str) -> None:
    conn = _mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
    finally:
        conn.close()


def _mysql_make_prior(table: str) -> None:
    conn = _mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE `{table}` ("
                " id INT PRIMARY KEY,"
                " amt DECIMAL(10,2) NOT NULL CHECK (amt < 100))"
            )
            cur.executemany(
                f"INSERT INTO `{table}` (id, amt) VALUES (%s, %s)",
                [(1, "9.00"), (2, "8.00"), (3, "7.00")],
            )
    finally:
        conn.close()


def _mysql_snapshot(table: str):
    conn = _mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s",
                (table,),
            )
            if not cur.fetchone()[0]:
                return None
            cur.execute(f"SELECT * FROM `{table}` ORDER BY 1")
            rows = [tuple(r) for r in cur.fetchall()]
            cur.execute(
                "SELECT column_name, data_type, is_nullable, column_key "
                "FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = %s "
                "ORDER BY ordinal_position",
                (table,),
            )
            cols = cur.fetchall()
            cur.execute(
                "SELECT con.constraint_name, con.constraint_type "
                "FROM information_schema.table_constraints con "
                "WHERE con.table_schema = DATABASE() AND con.table_name = %s "
                "ORDER BY con.constraint_name",
                (table,),
            )
            constraints = cur.fetchall()
            return {"rows": rows, "cols": cols, "constraints": constraints}
    finally:
        conn.close()


def _mysql_request(tmp_path: Path, table: str, content: bytes) -> TransferRequest:
    recipe = _recipe()
    return TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database", format="mysql", host=_MYSQL["host"], port=_MYSQL["port"],
            database=_MYSQL["database"], username=_MYSQL["username"],
            password=_MYSQL["password"], table=table,
        ),
        source_filename="amt.csv",
        source_content=content,
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


def test_x9_mysql_refusal_preserves_prior_rows(tmp_path, monkeypatch):
    """Live MySQL cell: PK + CHECK + prior rows byte-identical after a
    mid-run transform refusal."""
    if not _mysql_available():
        import pytest

        pytest.skip("mysql not reachable")
    _engine_env(monkeypatch)
    table = f"x9my_{uuid.uuid4().hex[:8]}"
    _mysql_drop(table)
    _mysql_make_prior(table)
    before = _mysql_snapshot(table)
    try:
        result = UniversalTransferEngine().execute(
            _mysql_request(tmp_path, table, _csv())
        )
        assert not result.success
        after = _mysql_snapshot(table)
        assert after == before, (
            f"mysql destination changed by a failed run: before={len(before['rows'])} "
            f"after={after and len(after['rows'])}"
        )
    finally:
        _mysql_drop(table)


def test_x9_mysql_refusal_empty_dest_leaves_nothing(tmp_path, monkeypatch):
    if not _mysql_available():
        import pytest

        pytest.skip("mysql not reachable")
    _engine_env(monkeypatch)
    table = f"x9my_{uuid.uuid4().hex[:8]}"
    _mysql_drop(table)
    try:
        result = UniversalTransferEngine().execute(
            _mysql_request(tmp_path, table, _csv())
        )
        assert not result.success
        snap = _mysql_snapshot(table)
        assert snap is None, f"table this run created must be dropped, got {snap}"
    finally:
        _mysql_drop(table)


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


def test_x9_failed_run_reports_undo_and_readback_matches(tmp_path, monkeypatch):
    """Committed==read-back: a failed run that rolled its partial batch back
    must (a) say so on the job's error message — an operator must not read a
    silent failure and believe rows are committed — and (b) leave the
    destination read-back byte-equal to the pre-run state."""
    _engine_env(monkeypatch)
    table = f"x9rb_{uuid.uuid4().hex[:8]}"
    dbfile = str(tmp_path / "dest.sqlite")
    result = UniversalTransferEngine().execute(_request(tmp_path, table))
    assert not result.success
    error_text = str(getattr(result, "error", "") or "")
    assert (
        "partial batch" in error_text
        or "byte-identical" in error_text
        or "dropped" in error_text
    ), f"failure message must surface the batch undo, got: {error_text!r}"
    snap = _dest_snapshot(dbfile, table)
    readback_rows = 0 if snap is None else len(snap["rows"])
    assert readback_rows == 0, (
        f"read-back shows {readback_rows} row(s) the failure report did not account for"
    )
