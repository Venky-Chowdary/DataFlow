"""GSQL-G8-1: a generic SQL destination verifies through a sample reader.

Before the fix ``read_target_sample`` had no branch for a ``generic_sql``
destination (a SQLAlchemy URL to an engine with no dedicated reader), so
Gate-8 failed every run after a correct write and the undo emptied the table.
"""

from __future__ import annotations

import pytest

from tests import w2dat_live as live


def _generic(table: str):
    from src.transfer.models import EndpointConfig

    c = live.PG
    url = f"postgresql+psycopg2://{c['username']}:{c['password']}@{c['host']}:{c['port']}/{c['database']}"
    return EndpointConfig(kind="database", format="generic_sql", connection_string=url,
                          schema="public", table=table)


def test_generic_sql_sample_reader_reads_keys():
    live.need_pg()
    from services.target_sample import read_target_sample

    t = f"w2dat_gs_{live.uid()}"
    try:
        live.pg(f'CREATE TABLE public."{t}" (id int primary key, name text)')
        live.pg(f'INSERT INTO public."{t}" VALUES (1, \'a\'), (2, NULL), (3, \'c\')')
        from src.transfer.adapters import resolve_connector_config

        cfg = resolve_connector_config(_generic(t))
        rows = read_target_sample(
            "generic_sql", cfg, schema="public", table_name=t, columns=["id", "name"],
            limit=10, sort_key="id", key_values=["3", "1"],
        )
        assert sorted((int(r["id"]), r["name"]) for r in rows) == [(1, "a"), (3, "c")]
    finally:
        live.drop_pg(t)


@pytest.mark.parametrize("mode", ["full_refresh_overwrite", "full_refresh_append", "upsert"])
def test_generic_sql_destination_run_verifies_and_keeps_rows(mode):
    live.need_pg()
    u = live.uid()
    src, dst = f"w2dat_gsrc_{u}", f"w2dat_gdst_{u}"
    try:
        live.pg(f'CREATE TABLE public."{src}" (id int primary key, name text)')
        live.pg(f'INSERT INTO public."{src}" VALUES (1, \'plain\'), (2, NULL), (3, \'x\')')
        live.pg(f'CREATE TABLE public."{dst}" (id int primary key, name text)')
        live.pg(f'INSERT INTO public."{dst}" VALUES (10, \'kept\')')
        res = live.run(live.ep_pg(src), _generic(dst), mode, key="id", cols=["id", "name"])
        rows = live.pg(f'SELECT id, name FROM public."{dst}" ORDER BY id')
        want = [(1, "plain"), (2, None), (3, "x")]
        if mode != "full_refresh_overwrite":
            want = want + [(10, "kept")]
        assert rows == want, (rows, res.error)
        assert res.success, res.error
    finally:
        live.drop_pg(src, dst)
