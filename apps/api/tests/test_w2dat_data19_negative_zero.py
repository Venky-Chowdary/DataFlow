"""DATA-19: Parquet -0.0 lands as -0.0 and Gate-8 tells -0.0 from 0.0.

Before the fix -0.0 landed as 0.0 in a PostgreSQL double column (psycopg2's
-0.0 literal is numeric negation) and the run verified under full_checksum,
because the fingerprint folded -0 into 0.
"""

from __future__ import annotations

import io
import math

import pytest

from tests import w2dat_live as live


def test_fingerprint_keeps_the_sign_of_zero_on_float_columns():
    from services.reconciliation import normalize_cell

    assert normalize_cell(-0.0, ddl_type="DOUBLE PRECISION") != normalize_cell(0.0, ddl_type="DOUBLE PRECISION")
    assert normalize_cell("-0.0", ddl_type="REAL") == normalize_cell(-0.0, ddl_type="REAL")
    # A decimal carrier has no negative zero: it still folds.
    assert normalize_cell("-0.0", ddl_type="NUMERIC(5,1)") == normalize_cell("0.0", ddl_type="NUMERIC(5,1)")


def test_parquet_negative_zero_lands_in_pg():
    live.need_pg()
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq
    from src.transfer.models import EndpointConfig

    buf = io.BytesIO()
    pq.write_table(pa.table({"id": pa.array([1, 2], pa.int64()), "f": pa.array([-0.0, 1.5], pa.float64())}), buf)
    dst = f"w2dat_d19_{live.uid()}"
    try:
        res = live.run(EndpointConfig(kind="file", format="parquet"), live.ep_pg(dst), "full_refresh_overwrite",
                       key="id", extra_req={"source_content": buf.getvalue(), "source_filename": "f.parquet"})
        assert res.success, res.error
        (f,) = live.pg(f'SELECT f FROM public."{dst}" WHERE id = 1')[0]
        assert f == 0.0 and math.copysign(1.0, f) < 0
    finally:
        live.drop_pg(dst)
