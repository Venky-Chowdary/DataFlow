"""CDC-LIT-1: source text that spells an internal marker lands byte-exact.

Before the fix a source text ``'__DF_MISSING__'`` landed as ``''`` through PG
CDC (snapshot and stream) into PG and MySQL with Gate-8 passing, and PG→SQLite
failed Gate-8 on the marker literals.
"""

from __future__ import annotations

import sqlite3

import pytest

from tests import w2dat_live as live

VALUES = [
    (1, "__DF_SQL_NULL__"),
    (2, "__DF_ESC__x"),
    (3, "__df_sql_null__"),
    (4, "__DF_MISSING__"),
    (5, None),
    (6, "__DF_SQL_NULL__ "),
    (7, "plain"),
    (8, "__DF_NULL__"),
]


def _dest(kind: str, name: str, tmp_path):
    if kind == "mysql":
        live.need_mysql()
        return live.ep_my(name)
    if kind == "sqlite":
        return live.ep_sqlite(tmp_path / "lit.db", name)
    return live.ep_pg(name)


def _rows(kind: str, name: str, tmp_path) -> list[tuple]:
    if kind == "pg":
        return [tuple(r) for r in live.pg(f'SELECT id, name FROM public."{name}" ORDER BY id')]
    if kind == "mysql":
        return [tuple(r) for r in live.my(f"SELECT id, name FROM `{name}` ORDER BY id")]
    with sqlite3.connect(tmp_path / "lit.db") as conn:
        return [(int(i), n) for i, n in conn.execute(f'SELECT id, name FROM "{name}" ORDER BY id')]


def _expected(kind: str, values: list[tuple]) -> list[tuple]:
    # MySQL VARCHAR keeps trailing spaces; PAD SPACE only affects comparison.
    return [(i, v) for i, v in values]


@pytest.mark.parametrize("mode", ["full_refresh_overwrite", "cdc"])
@pytest.mark.parametrize("kind", ["pg", "mysql", "sqlite"])
def test_marker_literals_land_exactly(kind, mode, tmp_path):
    if mode == "cdc":
        live.need_logical_pg()
    else:
        live.need_pg()
    u = live.uid()
    src = f"w2dat_lit_{u}"
    dst = f"w2dat_litd_{u}"
    dest = _dest(kind, dst, tmp_path)
    try:
        live.pg(f'CREATE TABLE public."{src}" (id int primary key, name text)')
        for i, v in VALUES[:4]:
            live.pg(f'INSERT INTO public."{src}" VALUES (%s, %s)', (i, v))
        first = live.run(live.ep_pg(src), dest, mode, key="id", cols=["id", "name"])
        assert first.success, first.error
        assert _rows(kind, dst, tmp_path) == _expected(kind, VALUES[:4])
        # The rest arrive through the change stream on CDC (or a second load).
        for i, v in VALUES[4:]:
            live.pg(f'INSERT INTO public."{src}" VALUES (%s, %s)', (i, v))
        live.pg(f'UPDATE public."{src}" SET name = %s WHERE id = 7', ("__DF_MISSING__",))
        second = live.run(live.ep_pg(src), dest, mode, key="id", cols=["id", "name"])
        assert second.success, second.error
        want = [(i, "__DF_MISSING__" if i == 7 else v) for i, v in VALUES]
        assert _rows(kind, dst, tmp_path) == _expected(kind, want)
        if mode == "cdc":
            # A delete keyed on a marker-spelled row must find it.
            live.pg(f'DELETE FROM public."{src}" WHERE id IN (4, 7)')
            third = live.run(live.ep_pg(src), dest, mode, key="id", cols=["id", "name"])
            assert third.success, third.error
            assert [r[0] for r in _rows(kind, dst, tmp_path)] == [1, 2, 3, 5, 6, 8]
    finally:
        live.drop_pg(src, dst)
        if kind == "mysql":
            live.drop_my(dst)
        if mode == "cdc":
            live.drop_cdc_artifacts(src)
