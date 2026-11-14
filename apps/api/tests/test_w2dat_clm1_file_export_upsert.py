"""CLM-1: upsert ("Incremental deduped") into a file export keeps earlier rows.

Before the fix run 2 opened the export ``wb``: after the source changed (2
updated, 3 deleted, 4 inserted) the file held only rows 2 and 4 — row 1 was
lost — and the run reported completed.
"""

from __future__ import annotations

import csv
import io
import json
import os

import pytest

from tests import w2dat_live as live


def _export_rows(path: str, fmt: str) -> dict[int, str]:
    with open(path, "rb") as fh:
        raw = fh.read().decode("utf-8-sig")
    if fmt == "csv":
        rows = list(csv.DictReader(io.StringIO(raw)))
    elif fmt == "jsonl":
        rows = [json.loads(line) for line in raw.splitlines() if line.strip()]
    else:
        rows = json.loads(raw)
    return {int(r["id"]): str(r["v"]) for r in rows}


@pytest.mark.parametrize("fmt", ["csv", "jsonl", "json"])
@pytest.mark.parametrize("mode", ["incremental_deduped", "upsert"])
def test_two_run_upsert_into_a_file_export_merges_by_key(fmt, mode, tmp_path, monkeypatch):
    live.need_pg()
    from src.transfer.models import EndpointConfig

    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path))
    u = live.uid()
    src = f"w2dat_c1_{u}"
    name = f"w2dat_c1_{u}.{fmt}"
    dest = EndpointConfig(kind="file_export", format=fmt, output_path=name)
    extra = {"cursor_field": "u", "cursor_semantics": "modification_timestamp"}
    try:
        live.pg(f'CREATE TABLE public."{src}" (id int primary key, v text, u int)')
        live.pg(f'INSERT INTO public."{src}" VALUES (1, \'a\', 1), (2, \'b\', 1), (3, \'c\', 1)')
        first = live.run(live.ep_pg(src), dest, mode, key="id", cols=["id", "v", "u"],
                         contract_extra=extra)
        assert first.success, first.error
        path = (first.destination_summary or {}).get("path")
        assert path and os.path.exists(path)
        assert _export_rows(path, fmt) == {1: "a", 2: "b", 3: "c"}
        live.pg(f'UPDATE public."{src}" SET v = \'b2\', u = 2 WHERE id = 2')
        live.pg(f'DELETE FROM public."{src}" WHERE id = 3')
        live.pg(f'INSERT INTO public."{src}" VALUES (4, \'d\', 2)')
        second = live.run(live.ep_pg(src), dest, mode, key="id", cols=["id", "v", "u"],
                          contract_extra=extra)
        assert _export_rows(path, fmt) == {1: "a", 2: "b2", 3: "c", 4: "d"}, second.error
        assert second.success, second.error
    finally:
        live.drop_pg(src)
