"""D118 — one NULL in a non-text column failed the whole transfer.

Database readers wire-encode every cell at extract (``SQL_NULL_SENTINEL`` for
NULL, ``__DF_ESC__`` for a colliding literal). The preflight dry-run
(``project_row_cells``) and the automap / overwrite sample paths re-serialised
those already-wire cells through ``cell_to_string(..., preserve_sql_null=True)``,
which escaped the NULL sentinel a second time. ``apply_transform`` then missed
the sentinel, unescaped it back to the literal and refused it: "Null sentinel
'__DF_SQL_NULL__' cannot coerce to integer" — 0 rows, after a green plan.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from services.value_serializer import (  # noqa: E402
    SQL_NULL_SENTINEL,
    cell_to_string,
    escape_wire_text,
    project_row_cells,
)
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def test_project_row_cells_is_idempotent_on_wire_cells():
    raw = {
        "n": None,
        "i": 7,
        "d": Decimal("1.50"),
        "dt": date(2024, 1, 2),
        "lit": "__DF_SQL_NULL__",
        "txt": "plain",
    }
    headers = list(raw)
    wire = {k: cell_to_string(v, preserve_sql_null=True) for k, v in raw.items()}
    once = project_row_cells(wire, headers, preserve_sql_null=True)
    assert once == [wire[h] for h in headers]
    assert once[0] == SQL_NULL_SENTINEL
    assert once[4] == escape_wire_text("__DF_SQL_NULL__")
    # A second projection of its own output changes nothing.
    assert project_row_cells(dict(zip(headers, once)), headers, preserve_sql_null=True) == once
    # Typed (not yet wire) values still get the canonical wire spelling.
    assert project_row_cells(raw, headers, preserve_sql_null=True)[:4] == once[:4]


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


_COLUMNS = (
    "id INTEGER PRIMARY KEY, qty INTEGER, price DECIMAL(10,2), born DATE, "
    "seen TIMESTAMP, active BOOLEAN, doc JSON, note TEXT"
)


def test_nulls_in_every_typed_column_land_as_null_with_preflight_on(tmp_path, isolated):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute(f"CREATE TABLE t ({_COLUMNS})")
        conn.executemany(
            "INSERT INTO t VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (1, 3, "9.99", "2024-01-02", "2024-01-02 03:04:05", 1, '{"a": 1}', "x"),
                (2, None, None, None, None, None, None, None),
                (3, 5, "1.25", "2023-12-31", "2023-12-31 23:59:59", 0, '{"b": [1, 2]}', "__DF_SQL_NULL__"),
            ],
        )
    conn.close()

    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
    )
    job_id = "d118" + uuid.uuid4().hex[:16]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert "cannot coerce" not in str(result.error or ""), result.error
    assert result.success, result.error

    conn = sqlite3.connect(str(dst))
    try:
        rows = conn.execute(
            "SELECT id, qty, price, born, seen, active, doc, note FROM t ORDER BY id"
        ).fetchall()
    finally:
        conn.close()
    assert len(rows) == 3
    assert rows[1] == (2, None, None, None, None, None, None, None)
    # A literal that spells the sentinel is text, never NULL (D109).
    assert rows[2][7] == "__DF_SQL_NULL__"
