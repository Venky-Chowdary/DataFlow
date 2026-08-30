"""Wave 48: Athena Iceberg MERGE routing + append-only fallback (no delete invent)."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_upsert_batch_routes_athena_to_trino_merge():
    from connectors import generic_sql as gs

    calls: list[str] = []

    def _tr(*_a, **_k):  # noqa: ANN001
        calls.append("merge")
        return 4

    class _Col:
        def __eq__(self, other):  # noqa: ANN001
            return ("eq", other)

    class _Table:
        name = "orders"
        schema = "db"
        c = {"id": _Col(), "v": _Col()}

        def insert(self):
            return "INSERT"

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            return MagicMock(rowcount=1)

        def rollback(self):
            pass

    orig = gs._trino_merge_upsert
    gs._trino_merge_upsert = _tr  # type: ignore[assignment]
    try:
        for dialect in ("athena", "awsathena"):
            n = gs._upsert_batch(
                _Conn(),
                _Table(),
                [{"id": 1, "v": "x"}],
                ["id"],
                ["id", "v"],
                dialect,
            )
            assert n == 4
    finally:
        gs._trino_merge_upsert = orig  # type: ignore[assignment]

    assert calls == ["merge", "merge"]


def test_upsert_batch_athena_skips_delete_insert_fallback():
    from connectors import generic_sql as gs

    calls: list[str] = []

    class _Result:
        rowcount = 1

    class _Col:
        def __eq__(self, other):  # noqa: ANN001
            return ("eq", other)

    class _Table:
        name = "t"
        schema = None
        c = {"id": _Col(), "name": _Col()}

        def insert(self):
            calls.append("insert")
            return "INSERT"

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            text = str(getattr(stmt, "text", stmt))
            # A DELETE *statement* — not the mirror-lattice probe
            # `SELECT "_deleted" FROM t WHERE 1=0`, which merely contains it.
            if re.match(r"\s*DELETE\b", text, flags=re.IGNORECASE):
                calls.append("delete")
            elif stmt == "INSERT" or "INSERT" in text.upper():
                calls.append("insert_exec")
            else:
                calls.append(text[:40])
            return _Result()

        def rollback(self):
            calls.append("rollback")

    def _boom(*_a, **_k):  # noqa: ANN001
        import sqlalchemy as sa

        raise sa.exc.SQLAlchemyError("MERGE only for Iceberg")

    orig = gs._trino_merge_upsert
    gs._trino_merge_upsert = _boom  # type: ignore[assignment]
    try:
        n = gs._upsert_batch(
            _Conn(),
            _Table(),
            [{"id": 1, "name": "x"}],
            ["id"],
            ["id", "name"],
            "athena",
        )
    finally:
        gs._trino_merge_upsert = orig  # type: ignore[assignment]

    assert n == 1
    assert "insert" in calls or "insert_exec" in calls
    assert "delete" not in calls


def test_trino_merge_doc_mentions_athena():
    from connectors.generic_sql import _trino_merge_upsert

    assert "Athena" in (_trino_merge_upsert.__doc__ or "")
