"""DATA-4 / DATA-12: CDC snapshot JSON fidelity and committed-row accounting.

Before the fix the CDC snapshot let psycopg2 decode jsonb: JSON ``null`` landed
as SQL NULL, the JSON string ``"null"`` as JSON null, and ``""`` was refused
under strict ("blocks partial write", records=0) after 7 of 8 rows had
already been committed.
"""

from __future__ import annotations

import pytest

from tests import w2dat_live as live

ROWS = [
    (1, "null"), (2, None), (3, '"str"'), (4, '""'), (5, "0"), (6, "false"),
    (7, '{"k": null}'), (8, '"null"'),
]


@pytest.mark.parametrize("kind", ["pg", "mysql"])
def test_cdc_snapshot_and_stream_keep_json_null_apart_from_sql_null(kind):
    live.need_logical_pg()
    u = live.uid()
    src, dst = f"w2dat_d4_{u}", f"w2dat_d4d_{u}"
    dest = live.ep_pg(dst) if kind == "pg" else (live.need_mysql() or live.ep_my(dst))
    try:
        live.pg(f'CREATE TABLE public."{src}" (id int primary key, j jsonb)')
        for i, j in ROWS:
            live.pg(f'INSERT INTO public."{src}" VALUES (%s, %s::jsonb)', (i, j))
        first = live.run(live.ep_pg(src), dest, "cdc", key="id", cols=["id", "j"])
        assert first.success, first.error
        assert first.records_transferred == len(ROWS)
        live.pg(f'INSERT INTO public."{src}" VALUES (11, \'null\'::jsonb), (18, \'"null"\'::jsonb)')
        second = live.run(live.ep_pg(src), dest, "cdc", key="id", cols=["id", "j"])
        assert second.success, second.error
        if kind == "pg":
            got = {i: j for i, j in live.pg(f'SELECT id, j::text FROM public."{dst}"')}
        else:
            got = {i: j for i, j in live.my(f"SELECT id, CAST(j AS CHAR) FROM `{dst}`")}
        import json

        want = {i: j for i, j in ROWS + [(11, "null"), (18, '"null"')]}
        for i, j in want.items():
            if j is None:
                assert got[i] is None, (i, got[i])
            else:
                assert got[i] is not None and json.loads(got[i]) == json.loads(j), (i, got[i])
                assert type(json.loads(got[i])) is type(json.loads(j)), (i, got[i])
    finally:
        live.drop_pg(src, dst)
        if kind == "mysql":
            live.drop_my(dst)
        live.drop_cdc_artifacts(src)


def test_cdc_strict_stop_after_a_write_reports_the_rows_that_stay():
    from src.transfer.adapters import WriteBatchBlocked
    from src.transfer.cdc_transfer import _refuse_cdc_advance_on_abort

    summary = {"rejected_details": [
        {"row": 4, "column": "j", "policy": "fail", "reason": "bad value"},
    ]}
    with pytest.raises(WriteBatchBlocked) as info:
        _refuse_cdc_advance_on_abort(summary, "strict", rows_committed=7)
    assert info.value.rows_written == 7
    assert "7 row(s)" in str(info.value) and "stay on" in str(info.value)
    assert "blocks partial write" not in str(info.value)
    # Nothing written yet: the plain refusal is still accurate.
    with pytest.raises(ValueError) as plain:
        _refuse_cdc_advance_on_abort(summary, "strict", rows_committed=0)
    assert not isinstance(plain.value, WriteBatchBlocked)
