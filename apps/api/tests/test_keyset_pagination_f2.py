"""Phase F2 — shared keyset pagination (composite PK, SQL Server–safe OR/AND)."""

from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.keyset_pagination import (  # noqa: E402
    KEYSET_SEP,
    decode_keyset_bookmark,
    encode_keyset_bookmark,
    keyset_successor_predicate,
    max_keyset_bookmark,
)


def test_encode_decode_roundtrip_unit_sep():
    bm = encode_keyset_bookmark(["ord-1", "line-2", "3"])
    assert KEYSET_SEP in bm
    assert decode_keyset_bookmark(bm, expected_parts=3) == ["ord-1", "line-2", "3"]


def test_decode_legacy_pipe_two_col():
    parts = decode_keyset_bookmark("2024-01-01T00:00:00|42", expected_parts=2)
    assert parts == ["2024-01-01T00:00:00", "42"]


def test_arity_mismatch_fail_closed():
    with pytest.raises(ValueError, match="arity mismatch"):
        decode_keyset_bookmark("only-one", expected_parts=2)


def test_successor_predicate_three_col_portable():
    where, params = keyset_successor_predicate(
        ['"a"', '"b"', '"c"'],
        encode_keyset_bookmark(["1", "2", "3"]),
    )
    assert where.count(">") == 3
    assert "OR" in where
    assert params == ["1", "1", "2", "1", "2", "3"]


def test_max_keyset_bookmark_picks_lex_max():
    headers = ["a", "b", "payload"]
    rows = [
        ["1", "9", "x"],
        ["2", "1", "y"],
        ["1", "10", "z"],
    ]
    bm = max_keyset_bookmark(rows, headers, ["a", "b"])
    assert bm == encode_keyset_bookmark(["2", "1"])


def test_cdc_reexport_still_works():
    from services.cdc_snapshot_window import keyset_successor_predicate as cdc_pred

    where, params = cdc_pred(['"a"', '"b"'], encode_keyset_bookmark(["x", "y"]))
    assert "OR" in where
    assert params == ["x", "x", "y"]


def test_generic_sql_cursor_composite_sqlite():
    """SQLAlchemy path used by SQL Server/Oracle — exercise via sqlite dialect."""
    from connectors.generic_sql import read_table_cursor_batch

    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "k.db"
        conn = sqlite3.connect(db)
        conn.execute(
            "CREATE TABLE lines (order_id TEXT, line_id INTEGER, qty INTEGER, "
            "PRIMARY KEY (order_id, line_id))"
        )
        for oid, lid in [("A", 1), ("A", 2), ("A", 3), ("B", 1), ("B", 2)]:
            conn.execute("INSERT INTO lines VALUES (?, ?, ?)", (oid, lid, lid * 10))
        conn.commit()
        conn.close()

        first = read_table_cursor_batch(
            host="",
            port=0,
            database=str(db),
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            type="sqlite",
            table="lines",
            cursor_column="order_id",
            cursor_after=None,
            columns=["order_id", "line_id", "qty"],
            limit=2,
            cursor_key_columns=["order_id", "line_id"],
        )
        assert len(first.rows) == 2
        assert first.rows[0][:2] == ["A", "1"]
        assert first.rows[1][:2] == ["A", "2"]

        after = encode_keyset_bookmark(["A", "2"])
        second = read_table_cursor_batch(
            host="",
            port=0,
            database=str(db),
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            type="sqlite",
            table="lines",
            cursor_column="order_id",
            cursor_after=after,
            columns=["order_id", "line_id", "qty"],
            limit=10,
            cursor_key_columns=["order_id", "line_id"],
        )
        # Must not skip A/3 (same leading key as bookmark).
        got = [(r[0], str(r[1])) for r in second.rows]
        assert ("A", "3") in got
        assert ("B", "1") in got
        assert ("A", "1") not in got
        assert ("A", "2") not in got


def test_stream_sqlite_composite_pk_keyset_mode():
    from services.checkpoint_service import CheckpointService
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    class _FakeMongo:
        def __init__(self):
            self.jobs: dict = {}

        def get_job(self, job_id: str):
            return self.jobs.get(job_id)

        def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
            self.jobs.setdefault(job_id, {})
            self.jobs[job_id].update(kwargs)
            self.jobs[job_id]["status"] = status
            return True

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        src = tmp_path / "src.db"
        dst = tmp_path / "dst.db"
        conn = sqlite3.connect(src)
        conn.execute(
            "CREATE TABLE lines (order_id TEXT, line_id INTEGER, qty INTEGER, "
            "PRIMARY KEY (order_id, line_id))"
        )
        for i in range(40):
            conn.execute(
                "INSERT INTO lines VALUES (?, ?, ?)",
                (f"O{i // 5}", i % 5, i),
            )
        conn.commit()
        conn.close()

        rows_written, _ddl, summary, _cols = stream_database_transfer(
            EndpointConfig(
                kind="database", format="sqlite", database=str(src), table="lines"
            ),
            EndpointConfig(
                kind="database", format="sqlite", database=str(dst), table="lines_out"
            ),
            [
                {"source": "order_id", "target": "order_id"},
                {"source": "line_id", "target": "line_id"},
                {"source": "qty", "target": "qty"},
            ],
            {"order_id": "string", "line_id": "integer", "qty": "integer"},
            job_id="000000000000000000000001",
            checkpoint_service=CheckpointService(_FakeMongo()),
            stream_contracts=[
                {
                    "selected": True,
                    "sync_mode": "full_refresh_overwrite",
                    "primary_key": "order_id,line_id",
                }
            ],
        )
        assert rows_written == 40
        assert summary.get("pagination_mode") == "keyset"
        assert summary.get("pagination_key_columns") == ["order_id", "line_id"]
        out = sqlite3.connect(dst)
        assert out.execute("SELECT count(*) FROM lines_out").fetchone()[0] == 40
        out.close()
