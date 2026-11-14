"""DATA-8: PostgreSQL double/real NaN and ±Infinity copy PG → PG.

Before the fix the copy was refused with "Null sentinel 'NaN' cannot coerce
to decimal": ``nan`` is in ``NULL_SENTINELS`` and only json/vector were
exempt. Parquet → PG already landed them.
"""

from __future__ import annotations

import pytest

from tests import w2dat_live as live


def test_nan_is_a_value_for_float_and_decimal_transforms():
    from services.transform_engine import apply_transform

    for transform in ("decimal",):
        for token in ("NaN", "Infinity", "-Infinity"):
            value, err = apply_transform(token, transform)
            assert err is None, (transform, token, err)
            assert value is not None, (transform, token)


@pytest.mark.parametrize("mode", ["full_refresh_overwrite", "upsert"])
def test_pg_double_and_real_nonfinite_values_copy_exactly(mode):
    live.need_pg()
    u = live.uid()
    src, dst = f"w2dat_d8_{u}", f"w2dat_d8d_{u}"
    try:
        live.pg(f'CREATE TABLE public."{src}" (id int primary key, d double precision, r real)')
        live.pg(
            f"INSERT INTO public.\"{src}\" VALUES (1, 'NaN', 'NaN'), "
            f"(2, 'Infinity', 'Infinity'), (3, '-Infinity', '-Infinity'), (4, 1.25, NULL)"
        )
        res = live.run(live.ep_pg(src), live.ep_pg(dst), mode, key="id", cols=["id", "d", "r"])
        assert res.success, res.error
        got = live.pg(f'SELECT id, d::text, r::text FROM public."{dst}" ORDER BY id')
        want = live.pg(f'SELECT id, d::text, r::text FROM public."{src}" ORDER BY id')
        assert got == want
    finally:
        live.drop_pg(src, dst)


def test_pg_unbounded_numeric_nan_copies():
    live.need_pg()
    u = live.uid()
    src, dst = f"w2dat_d8n_{u}", f"w2dat_d8nd_{u}"
    try:
        live.pg(f'CREATE TABLE public."{src}" (id int primary key, n numeric)')
        live.pg(f"INSERT INTO public.\"{src}\" VALUES (1, 'NaN'), (2, 1.5), (3, -2), (4, NULL)")
        res = live.run(live.ep_pg(src), live.ep_pg(dst), "full_refresh_overwrite", key="id", cols=["id", "n"])
        assert res.success, res.error
        assert live.pg(f'SELECT id, n::text FROM public."{dst}" ORDER BY id') == live.pg(
            f'SELECT id, n::text FROM public."{src}" ORDER BY id')
    finally:
        live.drop_pg(src, dst)
