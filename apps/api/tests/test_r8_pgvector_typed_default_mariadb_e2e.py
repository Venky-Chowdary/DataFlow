"""Owner decision 1 (QA D209), MariaDB source: no layout given -> typed table.

QA on 557f556c: MariaDB -> pgvector with no layout put every column only
inside ``metadata``. With the owner's decision a new table is the typed table
the plan shows — every MariaDB column with its real type, NULL kept — plus
``embedding vector(n)``. Route: ``start_transfer`` -> Confirm -> job, run 1
and run 2, from a real MariaDB (:3307) into PostgreSQL with pgvector.
"""

from __future__ import annotations

import json

from tests.test_d209_pgvector_destination_contract_e2e import (  # noqa: F401
    _connector,
    _physical,
    _run,
    _sql,
    client,
)
from tests.test_r6_d209_mariadb_into_pgvector_e2e import (  # noqa: F401
    _COLS,
    _assert_plan_is_the_table,
    _maria_sql,
    _same,
    _source_rows,
    tables,
)

DIM = 32


def _check_cells(src: str, dst: str) -> None:
    physical = [name for name, _t in _physical(dst)]
    assert physical[-1] == "embedding"
    for col in _COLS + ("rev",):
        assert col in physical, physical
    got = _sql(f"SELECT {', '.join(_COLS)}, rev, embedding::text FROM {dst} ORDER BY id")
    want = _source_rows(src)
    assert len(got) == len(want)
    for g, w in zip(got, want):
        for col, gv, wv in zip(_COLS + ("rev",), g, w):
            assert _same(gv, wv), (w[0], col, gv, wv)  # NULL is NULL, '' is ''
        assert g[-1] is not None and len(json.loads(g[-1])) == DIM, g


def test_mariadb_into_pgvector_without_layout_is_the_typed_table_with_embedding(client, tables):
    src, dst = tables["src"], tables["dst"]
    dest = _connector("pgvector", "destination")
    args = dict(source_connector_id=tables["source"], source_table=src,
                dest_connector_id=dest, dest_table=dst)

    plan, job = _run(client, sync_mode="full_refresh_overwrite", **args)
    contract = _assert_plan_is_the_table(plan, dst)  # promised == created, name and type
    assert (contract["layout"], contract["layout_reason"]) == ("table", "typed_default")
    assert contract["columns"][-1] == {
        "name": "embedding", "type": f"vector({DIM})", "role": "embedding"
    }
    assert [c["name"] for c in contract["columns"][:-1]] == list(_COLS) + ["rev"]
    _check_cells(src, dst)
    recon = job.get("reconciliation") or {}
    assert recon.get("passed") is True, recon

    _maria_sql(f"UPDATE `{src}` SET long_txt = NULL, amount = 3.10, rev = 2 WHERE id = 1")
    _maria_sql(f"UPDATE `{src}` SET name = 'ñ', note = NULL, rev = 2 WHERE id = 2")
    _maria_sql(f"INSERT INTO `{src}` (id, rev) VALUES (5, 2)")
    plan2, job2 = _run(client, sync_mode="incremental_upsert", primary_key="id",
                       cursor_column="rev", cursor_semantics="monotonic_sequence", **args)
    contract2 = _assert_plan_is_the_table(plan2, dst)
    assert contract2["layout_reason"] == "existing_typed_table"
    _check_cells(src, dst)
    assert (job2.get("reconciliation") or {}).get("passed") is True
