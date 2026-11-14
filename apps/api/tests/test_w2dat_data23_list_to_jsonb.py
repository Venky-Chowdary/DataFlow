"""DATA-23: a Parquet list into PostgreSQL JSONB is graded by its elements.

Before the fix ``list<int64>`` -> JSONB was refused as "ARRAY -> JSONB
lossy" although every element round-trips. The PostgreSQL typed-array
collapse (``INTEGER[]`` bounds and dimensions) stays pinned in
test_mongo_mysql_array_json_fidelity; no BIGINT[] is invented.
"""

from __future__ import annotations

import io

from services.source_engine_scope import bind_source_engine
from services.type_coercion import is_lossy_coercion
from tests import w2dat_live as live


def _lossy(src: str, population=None, engine: str = "parquet") -> bool:
    with bind_source_engine(engine):
        return is_lossy_coercion(src, "JSONB", dest_db="postgresql", population=population)


def test_exact_elements_grade_lossless_from_a_list_source():
    for t in ("ARRAY<BIGINT>", "ARRAY<TEXT>", "ARRAY<BOOLEAN>", "ARRAY<ARRAY<INTEGER>>"):
        assert not _lossy(t), t


def test_floats_need_finite_evidence():
    assert _lossy("ARRAY<DOUBLE>")
    assert not _lossy("ARRAY<DOUBLE>", ["[1.5, -0.25]", "[]", None])
    assert _lossy("ARRAY<DOUBLE>", ["[1.5]", "[NaN]"])
    assert _lossy("ARRAY<DOUBLE>", [[float("inf")]])


def test_postgres_typed_arrays_and_other_elements_stay_lossy():
    assert _lossy("ARRAY<INTEGER>", engine="postgresql")
    assert _lossy("ARRAY<INTEGER>", engine="")
    assert _lossy("ARRAY<BYTEA>")


def test_parquet_lists_land_in_postgres_jsonb():
    live.need_pg()
    import pyarrow as pa
    import pyarrow.parquet as pq

    from src.transfer.models import EndpointConfig

    t = pa.table({
        "id": pa.array([1, 2, 3], pa.int64()),
        "li": pa.array([[1, 2], [], None], pa.list_(pa.int64())),
        "ls": pa.array([["a", "é"], [], None], pa.list_(pa.string())),
        "lf": pa.array([[1.5, -0.25], [], None], pa.list_(pa.float64())),
        "lb": pa.array([[True, False], [], None], pa.list_(pa.bool_())),
    })
    buf = io.BytesIO()
    pq.write_table(t, buf)
    dst = f"w2dat_d23_{live.uid()}"
    try:
        res = live.run(
            EndpointConfig(kind="file", format="parquet"), live.ep_pg(dst), key="id",
            extra_req={"source_content": buf.getvalue(), "source_filename": "l.parquet"},
        )
        assert res.success, res.error
        got = live.pg(f'SELECT id, li::text, ls::text, lf::text, lb::text FROM "{dst}" ORDER BY id')
        assert got == [
            (1, "[1, 2]", '["a", "é"]', "[1.5, -0.25]", "[true, false]"),
            (2, "[]", "[]", "[]", "[]"),
            (3, None, None, None, None),
        ]
    finally:
        live.drop_pg(dst)
