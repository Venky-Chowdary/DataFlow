"""Redshift upsert delete+insert honors ``_df_lsn`` (stale redelivery skip)."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.postgresql_writer import _redshift_delete_by_keys  # noqa: E402
from connectors.writer_common import DF_LSN_COL  # noqa: E402


class _Frag:
    def __init__(self, text: str = ""):
        self.text = text

    def format(self, *args: object) -> "_Frag":
        return _Frag(" ".join([self.text, *[str(a) for a in args]]))

    def join(self, parts: object) -> "_Frag":
        return _Frag(self.text.join(str(p) for p in parts))

    def __str__(self) -> str:
        return self.text


class _SQL:
    @staticmethod
    def SQL(text: str) -> _Frag:
        return _Frag(text)

    @staticmethod
    def Identifier(name: str) -> str:
        return name

    @staticmethod
    def Placeholder() -> str:
        return "%s"


class _Cursor:
    def __init__(self, existing_lsn: str | None):
        self.existing_lsn = existing_lsn
        self.deleted = 0
        self.last_was_select = False

    def execute(self, query: object, params: object = None) -> None:
        q = str(query).upper()
        self.last_was_select = "SELECT" in q and "DELETE" not in q
        if "DELETE" in q:
            self.deleted += 1

    def fetchone(self):
        if self.existing_lsn is None:
            return None
        return (self.existing_lsn,)


def test_redshift_upsert_skips_stale_lsn():
    cols = ["id", "v", DF_LSN_COL]
    cur = _Cursor(existing_lsn="0/300")
    out = _redshift_delete_by_keys(
        cur,
        _SQL,
        schema="public",
        table_name="orders",
        target_cols=cols,
        conflict_cols=["id"],
        batch=[("1", "stale", "0/100")],
    )
    assert out == []
    assert cur.deleted == 0

    cur2 = _Cursor(existing_lsn="0/100")
    out2 = _redshift_delete_by_keys(
        cur2,
        _SQL,
        schema="public",
        table_name="orders",
        target_cols=cols,
        conflict_cols=["id"],
        batch=[("1", "new", "0/200")],
    )
    assert len(out2) == 1 and out2[0][1] == "new"
    assert cur2.deleted == 1


def test_redshift_upsert_inserts_when_no_existing_row():
    cols = ["id", "v", DF_LSN_COL]
    cur = _Cursor(existing_lsn=None)
    out = _redshift_delete_by_keys(
        cur,
        _SQL,
        schema="public",
        table_name="orders",
        target_cols=cols,
        conflict_cols=["id"],
        batch=[("1", "first", "0/10")],
    )
    assert len(out) == 1
    assert cur.deleted == 1
