"""D97 guard — incremental sync out of a DataFlow-created SQLite table (TEXT timestamp cursor).

QA: "every incremental sync out of a DataFlow-created SQLite table fails at run
time with 'must be real number, not str' … the trigger is a timestamp column
stored as SQLite TEXT"; only the from-zero retry succeeded.

Not reproduced on 3e258e90f: DataFlow creates ``updated_at TEXT`` and run 2
(saved watermark, one new row) completes. The current cursor bind raises a
typed ``CursorBindError`` on an unparsable watermark, never a TypeError. This
live guard pins the route in CI: create the table with DataFlow, run the
incremental twice, the second run lands exactly the new rows.
"""

from __future__ import annotations

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
from tests.test_property2_golden_path_never_blocked import _FakeMongo  # noqa: E402


class _Mongo(_FakeMongo):
    def update_job_fields(self, job_id, fields):
        self.jobs.setdefault(job_id, {}).update(fields or {})
        return True


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    import services.sync_cursor as sc

    fake = _Mongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _req(src: str, dst: str, mode: str, cursor: str | None) -> TransferRequest:
    contracts = (
        [{"name": "ev", "cursor_field": cursor, "primary_key": "id",
          "cursor_semantics": "modification_timestamp", "sync_mode": mode, "selected": True}]
        if cursor else None
    )
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=src, table="ev"),
        destination=EndpointConfig(kind="database", format="sqlite", database=dst, table="ev"),
        sync_mode=mode, stream_contracts=contracts, skip_preflight=True, validation_mode="strict",
    )


def _run(req: TransferRequest):
    return UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])


def test_incremental_run2_out_of_a_dataflow_created_sqlite_table(tmp_path: Path, isolated):
    origin, mid, dst = (str(tmp_path / f"{n}.db") for n in ("origin", "mid", "dst"))
    con = sqlite3.connect(origin)
    con.execute("CREATE TABLE ev (id INTEGER PRIMARY KEY, name TEXT, updated_at TIMESTAMP)")
    con.executemany(
        "INSERT INTO ev VALUES (?,?,?)",
        [(i, ["n", "", "  ", None, "東京"][i % 5], f"2026-01-{i:02d} 10:00:00") for i in range(1, 6)],
    )
    con.commit()
    con.close()
    # DataFlow creates ``mid`` (the timestamp lands as SQLite TEXT).
    assert _run(_req(origin, mid, "full_refresh_overwrite", None)).success
    ddl = sqlite3.connect(mid).execute("SELECT sql FROM sqlite_master WHERE name='ev'").fetchone()[0]
    assert '"updated_at" TEXT' in ddl, ddl
    first = _run(_req(mid, dst, "incremental_append", "updated_at"))
    assert first.success is True, first.error
    assert first.records_transferred == 5
    # New source rows, then the created table is refreshed and read incrementally again.
    con = sqlite3.connect(origin)
    con.executemany("INSERT INTO ev VALUES (?,?,?)",
                    [(6, "n6", "2026-01-06 10:00:00"), (7, "ß", "2026-01-07 10:00:00.123456")])
    con.commit()
    con.close()
    assert _run(_req(origin, mid, "full_refresh_overwrite", None)).success
    second = _run(_req(mid, dst, "incremental_append", "updated_at"))
    assert second.success is True, second.error
    assert second.records_transferred == 2
    rows = sqlite3.connect(dst).execute("SELECT id, name FROM ev ORDER BY id").fetchall()
    assert [r[0] for r in rows] == [1, 2, 3, 4, 5, 6, 7]
    assert rows[1][1] == "  " and rows[2][1] is None and rows[3][1] == "東京" and rows[6][1] == "ß"
